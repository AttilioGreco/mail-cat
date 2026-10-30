//! Server-side INBOX watcher: IMAP IDLE / JMAP event stream.
//!
//! Each ProviderNotification is mapped to a domain command so that all state
//! changes flow through the single InProcessEventBus:
//!
//!   NewMessage / MailboxCountChanged → debounced sync (Semaphore 1)
//!       └→ SyncService emits MessageReceived + MailboxCountsUpdated
//!
//!   FlagsChanged  → Command::ApplyRemoteFlagChange
//!       └→ emits MessageFlagged on the bus
//!
//!   (Expunges arrive as MailboxCountChanged → sync → reconcile detects the
//!    missing UID and emits MessageDeleted; plain IMAP IDLE gives no reliable
//!    UID for an EXPUNGE.)

use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use mc_application::{Command, CommandBus, Query, QueryBus, QueryResult};
use mc_domain::{
    AccountEventSource, AccountId, Mailbox, MailboxId, MailboxPath, MailboxType,
    ProviderNotification,
};
use mc_infrastructure::{AnyAccountEventSource, AnyProvider};

use super::AccountRuntime;
use crate::wiring::{CommandBusImpl, QueryBusImpl, SyncServiceImpl};

impl AccountRuntime {
    pub(super) fn spawn_watcher(
        &self,
        id: AccountId,
        name: String,
        provider: Arc<AnyProvider>,
        (stop, tasks): (CancellationToken, TaskTracker),
        first_sync: Arc<Notify>,
    ) {
        let sync = self.deps.sync.clone();
        let cmd_bus = self.deps.command_bus.clone();
        let query_bus = self.deps.query_bus.clone();
        let spawner = tasks.clone();
        tasks.spawn(async move {
            let mut mailboxes = local_mailboxes(&query_bus, id).await;
            // A brand-new account has no mailboxes until its first sync, and
            // the INBOX path (with the server's real delimiter) comes from there.
            if !mailboxes
                .iter()
                .any(|m| m.mailbox_type == MailboxType::Inbox)
            {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    () = first_sync.notified() => {}
                }
                mailboxes = local_mailboxes(&query_bus, id).await;
            }
            // The stored path matches what's in the read model, whatever delimiter
            // the server uses.
            let inbox_path = mailboxes
                .iter()
                .find(|mb| mb.mailbox_type == MailboxType::Inbox)
                .map(|mb| mb.path.clone())
                .unwrap_or_else(MailboxPath::inbox);
            let path_to_id: HashMap<String, MailboxId> = mailboxes
                .into_iter()
                .map(|mb| (mb.path.full_path().to_string(), mb.id))
                .collect();

            let mut stream =
                AnyAccountEventSource::new(provider).watch(std::slice::from_ref(&inbox_path));
            // At most one triggered sync in flight per account.
            let sync_gate = Arc::new(Semaphore::new(1));
            tracing::info!("[Watcher] Started INBOX IDLE for '{name}'");
            loop {
                let item = tokio::select! {
                    _ = stop.cancelled() => break,
                    item = stream.next() => match item { Some(v) => v, None => break },
                };
                match item {
                    Ok(notification) => {
                        let ctx = Triggered {
                            id,
                            sync: &sync,
                            sync_gate: &sync_gate,
                            stop: &stop,
                            tasks: &spawner,
                        };
                        handle(notification, ctx, &cmd_bus, &path_to_id).await
                    }
                    Err(e) => tracing::warn!("[Watcher] IDLE error for '{name}': {e}"),
                }
            }
            tracing::info!("[Watcher] INBOX IDLE stopped for '{name}'");
        });
    }
}

async fn local_mailboxes(query_bus: &QueryBusImpl, account_id: AccountId) -> Vec<Mailbox> {
    match query_bus
        .dispatch(Query::ListMailboxes { account_id })
        .await
    {
        Ok(QueryResult::Mailboxes(m)) => m,
        _ => vec![],
    }
}

/// What a notification handler needs to start a sync that stopping the account
/// can cancel and wait for.
struct Triggered<'a> {
    id: AccountId,
    sync: &'a Arc<SyncServiceImpl>,
    sync_gate: &'a Arc<Semaphore>,
    stop: &'a CancellationToken,
    tasks: &'a TaskTracker,
}

async fn handle(
    notification: ProviderNotification,
    Triggered {
        id,
        sync,
        sync_gate,
        stop,
        tasks,
    }: Triggered<'_>,
    cmd_bus: &CommandBusImpl,
    path_to_id: &HashMap<String, MailboxId>,
) {
    match notification {
        // Targeted sync of this mailbox from its cursor. Emits MessageReceived
        // + MailboxCountsUpdated and advances the cursor so start_sync won't
        // re-fetch on the next periodic run.
        ProviderNotification::NewMessage { mailbox, .. }
        | ProviderNotification::MailboxCountChanged { mailbox, .. } => {
            let Ok(permit) = sync_gate.clone().try_acquire_owned() else {
                return;
            };
            let sync = sync.clone();
            let stop = stop.clone();
            tasks.spawn(async move {
                let _permit = permit;
                let synced = tokio::select! {
                    _ = stop.cancelled() => return,
                    synced = sync.sync_mailbox(&id, &mailbox) => synced,
                };
                if let Err(e) = synced {
                    tracing::warn!("[Watcher] Partial sync failed: {e}");
                }
            });
        }
        // Mapped immediately so the UI gets a MessageFlagged event without
        // waiting for the next full sync.
        ProviderNotification::FlagsChanged {
            mailbox,
            remote_id,
            flags,
        } => {
            let Some(mailbox_id) = path_to_id.get(&mailbox.full_path()) else {
                return;
            };
            let cmd = Command::ApplyRemoteFlagChange {
                mailbox_id: *mailbox_id,
                remote_id: remote_id.clone(),
                new_flags: flags,
            };
            if let Err(e) = cmd_bus.dispatch(cmd).await {
                tracing::warn!("[Watcher] ApplyRemoteFlagChange id={remote_id}: {e}");
            }
        }
    }
}
