//! Command handler for Message aggregate operations.
//!
//! Message handlers orchestrate local event-sourced state changes.
//! For operations that need remote provider interaction (set flags,
//! move, delete on IMAP), the handler calls the EmailProvider first,
//! then produces domain events on success.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use mc_domain::{
    Account, AccountId, AggregateType, Attachment, AttachmentId, DomainError, DomainEvent,
    EmailProvider, Envelope, EventEnvelope, EventStore, Flag, Mailbox, MailboxId, MailboxPath,
    MailboxReadRepository, MailboxType, Membership, Message, MessageBodyStore, MessageId,
    MessageReadRepository, OutgoingMessage, ProviderRegistry, ReadModelProjector, SentCopy,
    value_objects::remote_id::RemoteId,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

use crate::bus::EventPublisher;
use crate::commands::CommandResult;
use crate::compose::{self, ComposeInput, ComposeMode, ComposeView, ParsedSource};
use crate::error::ApplicationError;

/// Handles Message-related commands.
///
/// Works directly with the EventStore for message aggregate persistence
/// (pure ES pattern). Uses ProviderRegistry to resolve the correct
/// EmailProvider per account for remote operations.
pub struct MessageCommandHandler<
    ES: EventStore,
    PR: ProviderRegistry,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore,
    B: EventPublisher,
> {
    event_store: Arc<ES>,
    provider_registry: Arc<PR>,
    message_read_repo: Arc<MR>,
    event_bus: Arc<B>,
    /// One server-side change (move, delete, flag store) at a time per
    /// account, each acting on the message as it is when its turn comes:
    /// two quick actions on one message must not both send its old remote id.
    /// Separate from the sync lock, which a full sync holds for minutes.
    account_queues: Arc<std::sync::Mutex<HashMap<AccountId, Arc<tokio::sync::Mutex<()>>>>>,
    /// Mailboxes to sync after the server changed them in a way the local
    /// state could not follow; the sync repairs it.
    resync: Option<UnboundedSender<(AccountId, MailboxPath)>>,
}

impl<
    ES: EventStore,
    PR: ProviderRegistry,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore,
    B: EventPublisher,
> Clone for MessageCommandHandler<ES, PR, MR, B>
{
    fn clone(&self) -> Self {
        Self {
            event_store: Arc::clone(&self.event_store),
            provider_registry: Arc::clone(&self.provider_registry),
            message_read_repo: Arc::clone(&self.message_read_repo),
            event_bus: Arc::clone(&self.event_bus),
            account_queues: Arc::clone(&self.account_queues),
            resync: self.resync.clone(),
        }
    }
}

/// Messages per provider call in a bulk move or delete, each chunk recorded
/// locally as soon as the server confirms it. Matches the IMAP adapter's
/// own command chunk.
const SERVER_CHUNK: usize = 500;

/// Tries at recording a change the server already made, when another writer
/// (a sync append, a flag change) keeps landing first.
const RECORD_ATTEMPTS: u32 = 3;

impl<
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static,
    B: EventPublisher + 'static,
> MessageCommandHandler<ES, PR, MR, B>
{
    pub fn new(
        event_store: Arc<ES>,
        provider_registry: Arc<PR>,
        message_read_repo: Arc<MR>,
        event_bus: Arc<B>,
    ) -> Self {
        Self {
            event_store,
            provider_registry,
            message_read_repo,
            event_bus,
            account_queues: Arc::default(),
            resync: None,
        }
    }

    /// Where to ask for a mailbox sync when local state fell behind the server.
    #[must_use]
    pub fn with_resync_requests(
        mut self,
        resync: UnboundedSender<(AccountId, MailboxPath)>,
    ) -> Self {
        self.resync = Some(resync);
        self
    }

    pub async fn handle_fetch_body(
        &self,
        message_id: MessageId,
        mailbox_id: MailboxId,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, mut seq) = self.load_message(&message_id).await?;
        require_membership(&message, mailbox_id)?;
        // Another copy (Gmail: the same mail under another label) may already
        // hold the whole text.
        if let Some(rfc_id) = message.message_rfc_id.clone() {
            let shared = self
                .message_read_repo
                .share_bodies(&message.account_id, &[rfc_id])
                .await?;
            for (copy, _) in &shared {
                self.record(copy, |m| Some(m.set_body(None, None, vec![])))
                    .await?;
            }
            if shared.contains(&(message_id, false)) {
                info!(%message_id, "message body taken from another copy");
                return Ok(CommandResult::MessageBodyFetched);
            }
            if !shared.is_empty() {
                // Recording a truncated body here moved the sequence.
                (message, seq) = self.load_message(&message_id).await?;
            }
        }
        let (mailbox_path, remote_id) = self
            .readable_copy(&mut message, &mut seq, Some(mailbox_id))
            .await?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        let detail = provider
            .fetch_message_detail(&mailbox_path, &remote_id)
            .await?;

        // Reload after the round trip: opening a message marks it read at the
        // same moment, and that flag event lands while we wait on the server,
        // so the sequence read before the fetch is already stale.
        let (mut message, seq) = self.load_message(&message_id).await?;
        let event = message.set_body(
            detail.body_text.clone(),
            detail.body_html.clone(),
            detail.attachments.clone(),
        );

        // Body before the event, as in SyncService::store_body.
        self.message_read_repo
            .put_body(
                &message_id,
                detail.body_text.clone(),
                detail.body_html.clone(),
                false,
            )
            .await?;

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message body fetched");
        Ok(CommandResult::MessageBodyFetched)
    }

    pub async fn handle_set_flags(
        &self,
        message_id: MessageId,
        flags_to_add: Vec<Flag>,
        flags_to_remove: Vec<Flag>,
    ) -> Result<CommandResult, ApplicationError> {
        let (message, _) = self.load_message(&message_id).await?;

        // Optimistic update: persist locally first for instant UI feedback,
        // then sync to IMAP in background. Next sync reconciles if IMAP fails.
        self.record(&message_id, |m| {
            Some(m.change_flags(flags_to_add.clone(), flags_to_remove.clone()))
        })
        .await?;

        info!(%message_id, "message flags updated locally, syncing to IMAP in background");
        self.push_flags(
            message.account_id,
            vec![message_id],
            flags_to_add,
            flags_to_remove,
        );
        Ok(CommandResult::FlagsSet)
    }

    /// Mark every unread message in a mailbox as read. The server gets one
    /// `set_flags_bulk` per batch as the batches are recorded, so a failure
    /// late in a big mailbox loses nothing already marked here.
    pub async fn handle_mark_mailbox_read(
        &self,
        mailbox_id: MailboxId,
        mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let unread = self
            .message_read_repo
            .find_unread_by_mailbox(&mailbox_id)
            .await?;
        let Some(account_id) = unread.first().map(|m| m.account_id) else {
            return Ok(CommandResult::MailboxMarkedRead { count: 0 });
        };

        let mut count = 0;
        for batch in unread.chunks(SERVER_CHUNK) {
            let mut marked = Vec::with_capacity(batch.len());
            for summary in batch {
                match self.mark_seen(&summary.id, &mailbox_path).await {
                    Ok(()) => marked.push(summary.id),
                    Err(e) => warn!(message_id = %summary.id, %e, "not marked read"),
                }
            }
            count += marked.len() as u32;
            self.push_flags(account_id, marked, vec![Flag::Seen], vec![]);
        }

        info!(%mailbox_id, count, "mailbox marked read locally, syncing to IMAP in background");
        Ok(CommandResult::MailboxMarkedRead { count })
    }

    /// Record `\Seen` on one message, finding its remote id on the server first
    /// when it has none so the flag can reach the server too.
    async fn mark_seen(
        &self,
        message_id: &MessageId,
        mailbox_path: &MailboxPath,
    ) -> Result<(), ApplicationError> {
        let (mut message, mut seq) = self.load_message(message_id).await?;
        if let Err(e) = self.remote_id(&mut message, &mut seq, mailbox_path).await {
            warn!(%message_id, %e, "marked read locally only");
        }
        self.record(message_id, |m| {
            Some(m.change_flags(vec![Flag::Seen], vec![]))
        })
        .await
        .map(drop)
    }

    /// Store a flag change on the server in the background, in the account's
    /// turn: each message's mailbox and remote id are read when the store is sent,
    /// so a move queued ahead of it (open, mark read, archive) is followed
    /// rather than racing it with an id that no longer exists.
    fn push_flags(
        &self,
        account_id: AccountId,
        message_ids: Vec<MessageId>,
        add: Vec<Flag>,
        remove: Vec<Flag>,
    ) {
        if message_ids.is_empty() {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let _turn = this.turn(account_id).await;
            let mut by_mailbox: HashMap<MailboxId, Vec<RemoteId>> = HashMap::new();
            for id in &message_ids {
                match this.server_copies(id).await {
                    Ok(copies) if copies.is_empty() => {
                        warn!(message_id = %id, "no remote id to store flags on, next sync will reconcile");
                    }
                    Ok(copies) => {
                        for (mailbox_id, remote_id) in copies {
                            by_mailbox.entry(mailbox_id).or_default().push(remote_id);
                        }
                    }
                    Err(e) => warn!(message_id = %id, %e, "flags not stored on the server"),
                }
            }
            let Ok(provider) = this.provider_registry.get_provider(&account_id) else {
                return;
            };
            for (mailbox_id, ids) in by_mailbox {
                let Ok(Some(mailbox)) = this.message_read_repo.get_mailbox(&mailbox_id).await
                else {
                    continue;
                };
                if let Err(e) = provider
                    .set_flags_bulk(&mailbox.path, &ids, &add, &remove)
                    .await
                {
                    warn!(%e, "Background flag store failed — next sync will reconcile");
                }
            }
        });
    }

    /// Every copy of the message the server has, as (mailbox, remote id).
    /// A copy without an id is looked up first, as for any other action; one
    /// that cannot be found is left out.
    async fn server_copies(
        &self,
        id: &MessageId,
    ) -> Result<Vec<(MailboxId, RemoteId)>, ApplicationError> {
        let (mut message, mut seq) = self.load_message(id).await?;
        let mut copies = Vec::with_capacity(message.memberships.len());
        for membership in message.memberships.clone() {
            if let Some(remote_id) = membership.remote_id {
                copies.push((membership.mailbox_id, remote_id));
                continue;
            }
            let Some(mailbox) = self
                .message_read_repo
                .get_mailbox(&membership.mailbox_id)
                .await?
            else {
                continue;
            };
            match self.remote_id(&mut message, &mut seq, &mailbox.path).await {
                Ok(remote_id) => copies.push((mailbox.id, remote_id)),
                Err(e) => {
                    warn!(message_id = %id, %e, "not found in {}, flags not stored there", mailbox.path)
                }
            }
        }
        Ok(copies)
    }

    /// Apply a flag change that arrived from a provider push notification.
    ///
    /// Unlike `handle_set_flags`, this does NOT call the provider (flags are
    /// already changed on the server). It only updates the local projection
    /// by emitting a `MessageFlagged` domain event.
    pub async fn handle_apply_remote_flag_change(
        &self,
        mailbox_id: MailboxId,
        remote_id: RemoteId,
        new_flags: Vec<Flag>,
    ) -> Result<CommandResult, ApplicationError> {
        info!(mailbox_id = %mailbox_id, %remote_id, ?new_flags,
            "handle_apply_remote_flag_change: looking up message");

        // Look up the local message by remote id
        let summary = self
            .message_read_repo
            .find_by_remote_id(&mailbox_id, &remote_id)
            .await?
            .ok_or_else(|| {
                ApplicationError::NotFound(
                    "Message",
                    format!("remote_id={remote_id} mailbox_id={mailbox_id}"),
                )
            })?;

        let message_id = summary.id;
        info!(%message_id, %remote_id, "message found, loading aggregate");

        let (mut message, seq) = self.load_message(&message_id).await?;

        // Compute diff vs current flags
        let flags_to_add: Vec<Flag> = new_flags
            .iter()
            .filter(|f| !message.flags.contains(f))
            .cloned()
            .collect();
        let flags_to_remove: Vec<Flag> = message
            .flags
            .iter()
            .filter(|f| !new_flags.contains(f))
            .cloned()
            .collect();

        info!(%message_id, ?flags_to_add, ?flags_to_remove, "computed flag diff");

        if flags_to_add.is_empty() && flags_to_remove.is_empty() {
            info!(%message_id, "no-op: flags already match");
            return Ok(CommandResult::RemoteFlagApplied);
        }

        let event = message.change_flags(flags_to_add, flags_to_remove);
        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, %remote_id, "remote flag change applied");
        Ok(CommandResult::RemoteFlagApplied)
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /// The account's turn for a server-side change; see `account_queues`.
    async fn turn(&self, account_id: AccountId) -> tokio::sync::OwnedMutexGuard<()> {
        let queue = Arc::clone(
            self.account_queues
                .lock()
                .unwrap()
                .entry(account_id)
                .or_default(),
        );
        queue.lock_owned().await
    }

    /// Append what `change` makes of the message's current state, reloading
    /// and retrying when another writer (a sync, a flag change) appended
    /// first. `false` when `change` had nothing to record.
    async fn record(
        &self,
        message_id: &MessageId,
        change: impl Fn(&mut Message) -> Option<DomainEvent>,
    ) -> Result<bool, ApplicationError> {
        let mut attempt = 1;
        loop {
            let (mut message, seq) = self.load_message(message_id).await?;
            let Some(event) = change(&mut message) else {
                return Ok(false);
            };
            match self
                .append_and_publish(message_id, AggregateType::Message, seq, event)
                .await
            {
                Err(ApplicationError::Domain(DomainError::ConcurrencyConflict { .. }))
                    if attempt < RECORD_ATTEMPTS =>
                {
                    attempt += 1;
                }
                recorded => return recorded.map(|()| true),
            }
        }
    }

    /// The remote id `message` has in `mailbox` (at `path`), found by Message-ID:
    /// the newest copy no other local message already holds.
    async fn locate(
        &self,
        provider: &PR::Provider,
        message: &Message,
        mailbox_id: &MailboxId,
        path: &MailboxPath,
    ) -> Result<Option<RemoteId>, ApplicationError> {
        let Some(rfc_id) = message.message_rfc_id.as_deref() else {
            return Ok(None);
        };
        let found = provider.find_by_message_id(path, rfc_id).await?;
        let held = self.message_read_repo.held_ids(mailbox_id, &found).await?;
        Ok(found.into_iter().rev().find(|id| !held.contains(id)))
    }

    /// A copy of the message to read from (a body, the raw source), as its
    /// mailbox path and remote id: the one in `preferred` if given — which must
    /// hold the message — else the lowest-numbered mailbox that has an id.
    /// Reading is harmless on any copy, so when the preferred one has no id
    /// and the server can't find it, another copy that has one stands in; the
    /// order is fixed so that the same message always reads from the same place.
    async fn readable_copy(
        &self,
        message: &mut Message,
        seq: &mut u64,
        preferred: Option<MailboxId>,
    ) -> Result<(MailboxPath, RemoteId), ApplicationError> {
        if let Some(preferred) = preferred {
            require_membership(message, preferred)?;
        }
        let mut copies = message.memberships.clone();
        copies.sort_by_key(|m| {
            (
                Some(m.mailbox_id) != preferred,
                m.remote_id.is_none(),
                *m.mailbox_id.as_uuid(),
            )
        });
        let mut first_error = None;
        for (rank, copy) in copies.iter().enumerate() {
            // Past the first, only copies already known on the server: no
            // lookup in a mailbox nobody asked about.
            if rank > 0 && copy.remote_id.is_none() {
                break;
            }
            let mailbox = self
                .message_read_repo
                .get_mailbox(&copy.mailbox_id)
                .await?
                .ok_or_else(|| {
                    ApplicationError::NotFound("Mailbox", copy.mailbox_id.to_string())
                })?;
            match self.remote_id(message, seq, &mailbox.path).await {
                Ok(remote_id) => return Ok((mailbox.path, remote_id)),
                Err(e) => {
                    first_error.get_or_insert(e);
                }
            }
        }
        Err(first_error.unwrap_or_else(|| {
            ApplicationError::PreconditionFailed(
                "il messaggio non è più su nessuna cartella".into(),
            )
        }))
    }

    /// The message's remote id in its mailbox, at `mailbox_path`. One without (a
    /// mailbox whose ids were invalidated, a move the server confirmed without its new id)
    /// is looked up on the server by Message-ID and recorded, so every
    /// action on it can go ahead. `seq` follows the recording event.
    async fn remote_id(
        &self,
        message: &mut Message,
        seq: &mut u64,
        mailbox_path: &MailboxPath,
    ) -> Result<RemoteId, ApplicationError> {
        // A remote id is valid only in its own mailbox: sending it to another, or
        // searching another, would act on some other message.
        let mut elsewhere = None;
        let mut here = None;
        for membership in &message.memberships {
            let mailbox = self
                .message_read_repo
                .get_mailbox(&membership.mailbox_id)
                .await?
                .ok_or_else(|| {
                    ApplicationError::NotFound("Mailbox", membership.mailbox_id.to_string())
                })?;
            if mailbox.path.full_path() == mailbox_path.full_path() {
                here = Some((membership.clone(), mailbox));
                break;
            }
            elsewhere.get_or_insert(mailbox.path);
        }
        let Some((membership, mailbox)) = here else {
            return Err(ApplicationError::PreconditionFailed(match elsewhere {
                Some(path) => format!("message is in {path}, not {mailbox_path}"),
                None => format!("message is in no mailbox, not {mailbox_path}"),
            }));
        };
        if let Some(remote_id) = membership.remote_id {
            return Ok(remote_id);
        }
        let provider = self.provider_registry.get_provider(&message.account_id)?;
        let remote_id = self
            .locate(&provider, message, &mailbox.id, &mailbox.path)
            .await?
            .ok_or_else(|| {
                ApplicationError::NotFound(
                    "Message on the server",
                    format!(
                        "{} in {mailbox_path}",
                        message
                            .message_rfc_id
                            .as_deref()
                            .unwrap_or("(no Message-ID)")
                    ),
                )
            })?;
        if let Some(event) = message.attach_remote_id(mailbox.id, remote_id.clone()) {
            self.append_and_publish(&message.id, AggregateType::Message, *seq, event)
                .await?;
            *seq += 1;
        }
        Ok(remote_id)
    }

    /// Load a message aggregate from the event store.
    /// Returns the rehydrated Message and the current event count (for OCC).
    async fn load_message(&self, id: &MessageId) -> Result<(Message, u64), ApplicationError> {
        let events = self.event_store.load_events(&id.to_string(), 0).await?;
        let event_count = events.len() as u64;

        let domain_events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();

        let message = Message::rehydrate(&domain_events)
            .filter(|m| !m.is_deleted())
            .ok_or_else(|| ApplicationError::NotFound("Message", id.to_string()))?;

        Ok((message, event_count))
    }

    async fn append_and_publish(
        &self,
        message_id: &MessageId,
        aggregate_type: AggregateType,
        expected_sequence: u64,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        let new_seq = self
            .event_store
            .append(
                &message_id.to_string(),
                aggregate_type,
                expected_sequence,
                vec![event.clone()],
            )
            .await?;

        let envelope = EventEnvelope::new(message_id.to_string(), aggregate_type, new_seq, event);

        // Project inline so the read model is updated BEFORE we return.
        // This ensures that any re-query after the command sees fresh data.
        if let Err(e) = self.message_read_repo.project(&envelope).await {
            tracing::warn!(
                "Inline projection failed for {}: {}",
                envelope.payload.event_name(),
                e
            );
        }

        self.event_bus.publish(envelope);

        Ok(())
    }
}

/// The message is in `mailbox_id`, or an action naming that mailbox is an
/// error: falling back to another of its mailboxes would act on a copy nobody
/// asked about.
fn require_membership(
    message: &Message,
    mailbox_id: MailboxId,
) -> Result<&Membership, ApplicationError> {
    message.membership(mailbox_id).ok_or_else(|| {
        ApplicationError::NotFound("Message", format!("{} in mailbox {mailbox_id}", message.id))
    })
}

mod labels;
mod outgoing;
mod relocate;

#[cfg(test)]
mod tests;
