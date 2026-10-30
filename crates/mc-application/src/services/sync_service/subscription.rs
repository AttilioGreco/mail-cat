//! Which mailboxes the user sees and syncs. The server's own subscription
//! decides until the user does (`Mailbox::subscription_explicit`).

use std::sync::Arc;

use tracing::{info, warn};

use mc_domain::{AggregateType, EmailProvider, EventStore, Mailbox, MailboxId, ProviderRegistry};

use super::events::sync_failed;
use super::mailbox::left_mailbox;
use super::{SyncProgressEvent, SyncService, SyncState, SyncStore};

use crate::error::ApplicationError;

/// Messages dropped per read of the store; progress is published after each.
const DROP_BATCH: u32 = 500;

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Follow the server's subscription for a mailbox the user has not
    /// decided on, as listed in `remote`.
    pub(super) async fn follow_server_subscription(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &mut Mailbox,
        remote: &Mailbox,
    ) -> Result<(), ApplicationError> {
        if !mailbox.subscription_explicit && mailbox.subscribed != remote.subscribed {
            info!(path = %remote.path, subscribed = remote.subscribed, "Subscription changed on the server");
            let event = mailbox.set_subscribed(remote.subscribed, false);
            self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
                .await?;
        }
        // Also finishes a drop a restart interrupted.
        if !mailbox.subscribed {
            self.drop_local_messages(provider, mailbox).await?;
        }
        Ok(())
    }

    /// Whether `mailbox` is still subscribed now: a pass that began before
    /// an unsubscribe must not go on downloading.
    pub(super) async fn is_subscribed(&self, mailbox: &Mailbox) -> Result<bool, ApplicationError> {
        Ok(self
            .projector
            .get_mailbox(&mailbox.id)
            .await
            .map_err(sync_failed("get_mailbox"))?
            .is_some_and(|m| m.subscribed))
    }

    /// The user's choice. Applied locally whatever the server says; `Some`
    /// is the warning when the server could not be told.
    pub async fn set_mailbox_subscribed(
        &self,
        mailbox_id: &MailboxId,
        subscribed: bool,
    ) -> Result<Option<String>, ApplicationError> {
        let mut mailbox = self
            .projector
            .get_mailbox(mailbox_id)
            .await
            .map_err(sync_failed("get_mailbox"))?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", mailbox_id.to_string()))?;
        if !subscribed && mailbox.mailbox_type.is_mandatory() {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{} ({:?}) is always subscribed: it holds your own mail",
                mailbox.display_path, mailbox.mailbox_type
            )));
        }
        let provider = self.provider_registry.get_provider(&mailbox.account_id)?;

        // The event goes first and without the account lock: a sync of this
        // mailbox that is running sees it and stops, which releases the lock
        // the background job waits for.
        let event = mailbox.set_subscribed(subscribed, true);
        self.append_and_project(&mailbox.id.to_string(), AggregateType::Mailbox, event)
            .await?;
        // Closed only on shutdown, when a restart redoes it (see
        // `follow_server_subscription`).
        let _ = self.subscription_jobs.send(mailbox.id);

        let warning = match provider.set_subscribed(&mailbox.path, subscribed).await {
            Ok(()) => None,
            Err(e) => {
                warn!(path = %mailbox.path, error = %e, "Server not told of the subscription");
                Some(format!(
                    "{}: the server was not updated ({e}); the choice applies here only",
                    mailbox.display_path
                ))
            }
        };
        Ok(warning)
    }

    /// Run the jobs [`Self::set_mailbox_subscribed`] queues until the service
    /// is dropped. Call once; later calls return at once.
    pub async fn serve_subscription_jobs(&self) {
        let rx = self.subscription_jobs_rx.lock().unwrap().take();
        let Some(mut rx) = rx else { return };
        while let Some(mailbox_id) = rx.recv().await {
            self.apply_subscription(&mailbox_id).await;
        }
    }

    /// The queued jobs, run now, for tests that do not run the server loop.
    #[cfg(test)]
    pub(super) async fn run_subscription_jobs(&self) {
        let mut rx = self.subscription_jobs_rx.lock().unwrap().take().unwrap();
        while let Ok(id) = rx.try_recv() {
            self.apply_subscription(&id).await;
        }
        *self.subscription_jobs_rx.lock().unwrap() = Some(rx);
    }

    /// Bring the local store in line with the mailbox's current flag. Reads
    /// it at the time it runs, so queued toggles of one mailbox collapse into
    /// the last one.
    pub(super) async fn apply_subscription(&self, mailbox_id: &MailboxId) {
        let Ok(Some(mailbox)) = self.projector.get_mailbox(mailbox_id).await else {
            return;
        };
        if mailbox.subscribed {
            if let Err(e) = self.sync_mailbox(&mailbox.account_id, &mailbox.path).await {
                warn!(path = %mailbox.path, error = %e, "First sync of a subscribed mailbox failed");
            }
            return;
        }
        let _exclusive = self.exclusive(&mailbox.account_id).await;
        let result = match self.provider_registry.get_provider(&mailbox.account_id) {
            Ok(provider) => self.drop_local_messages(&provider, &mailbox).await,
            Err(e) => Err(e.into()),
        };
        if let Err(e) = result {
            warn!(path = %mailbox.path, error = %e, "Dropping the messages of an unsubscribed mailbox failed, the next sync resumes it");
        }
    }

    /// Take the messages that live only in `mailbox` out of the store, and the
    /// memberships of the rest, one event each so the search index follows.
    /// A Gmail label (a view over another mailbox) holds nothing of its own.
    async fn drop_local_messages(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &Mailbox,
    ) -> Result<(), ApplicationError> {
        if provider.backed_by(&mailbox.path).is_some() {
            return Ok(());
        }
        let views = self.views_of(provider, mailbox).await?;
        let batch = mc_shared::pagination::Pagination {
            page: 1,
            page_size: DROP_BATCH,
        };
        let mut dropped = 0u32;
        let mut total = None;
        loop {
            // Always the first page: what was dropped no longer lists.
            let page = self
                .projector
                .list_messages(&mailbox.id, &batch)
                .await
                .map_err(sync_failed("list_messages"))?;
            if page.items.is_empty() {
                break;
            }
            // Subscribed again meanwhile: the new job syncs it back.
            if self.is_subscribed(mailbox).await? {
                return Ok(());
            }
            let total = *total.get_or_insert(page.total_count as u32);
            if dropped == 0 {
                info!(path = %mailbox.path, count = total, "Unsubscribed, dropping local messages");
            }
            for summary in &page.items {
                self.append_and_project(
                    &summary.id.to_string(),
                    AggregateType::Message,
                    left_mailbox(summary, mailbox.id, &views),
                )
                .await?;
            }
            dropped += page.items.len() as u32;
            self.publish(SyncProgressEvent {
                account_id: mailbox.account_id,
                state: SyncState::FetchingEnvelopes {
                    mailbox_id: mailbox.id,
                },
                processed: dropped,
                total: total.max(dropped),
                message: format!("Removing {}...", mailbox.display_path),
            });
        }
        if dropped > 0 {
            self.publish(SyncProgressEvent {
                account_id: mailbox.account_id,
                state: SyncState::Completed,
                processed: dropped,
                total: dropped,
                message: format!("Removed {}", mailbox.display_path),
            });
        }
        Ok(())
    }
}
