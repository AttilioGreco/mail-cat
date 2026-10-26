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
    Account, AccountId, AggregateType, Attachment, AttachmentId, DomainError, DomainEvent, EmailProvider,
    Envelope, EventEnvelope, EventStore, Flag, Mailbox, MailboxId, MailboxPath, MailboxReadRepository,
    MailboxType, Message, MessageId, MessageBodyStore, MessageReadRepository, OutgoingMessage,
    ProviderRegistry, ReadModelProjector, value_objects::uid::ProviderUid,
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
pub struct MessageCommandHandler<ES: EventStore, PR: ProviderRegistry, MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore, B: EventPublisher> {
    event_store: Arc<ES>,
    provider_registry: Arc<PR>,
    message_read_repo: Arc<MR>,
    event_bus: Arc<B>,
    /// One server-side change (move, delete, flag store) at a time per
    /// account, each acting on the message as it is when its turn comes:
    /// two quick actions on one message must not both send its old UID.
    /// Separate from the sync lock, which a full sync holds for minutes.
    account_queues: Arc<std::sync::Mutex<HashMap<AccountId, Arc<tokio::sync::Mutex<()>>>>>,
    /// Mailboxes to sync after the server changed them in a way the local
    /// state could not follow; the sync repairs it.
    resync: Option<UnboundedSender<(AccountId, MailboxPath)>>,
}

impl<ES: EventStore, PR: ProviderRegistry, MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore, B: EventPublisher> Clone
    for MessageCommandHandler<ES, PR, MR, B>
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

impl<ES: EventStore + 'static, PR: ProviderRegistry + 'static, MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static, B: EventPublisher + 'static>
    MessageCommandHandler<ES, PR, MR, B>
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
    pub fn with_resync_requests(mut self, resync: UnboundedSender<(AccountId, MailboxPath)>) -> Self {
        self.resync = Some(resync);
        self
    }

    pub async fn handle_fetch_body(
        &self,
        message_id: MessageId,
        mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, mut seq) = self.load_message(&message_id).await?;
        // Another copy (Gmail: the same mail under another label) may already
        // hold the whole text.
        if let Some(rfc_id) = message.message_rfc_id.clone() {
            let shared = self.message_read_repo.share_bodies(&message.account_id, &[rfc_id]).await?;
            for (copy, _) in &shared {
                self.record(copy, |m| Some(m.set_body(None, None, vec![]))).await?;
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
        let provider_uid = self.remote_uid(&mut message, &mut seq, &mailbox_path).await?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        let detail = provider
            .fetch_message_detail(&mailbox_path, provider_uid)
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
            .put_body(&message_id, detail.body_text.clone(), detail.body_html.clone(), false)
            .await?;

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message body fetched");
        Ok(CommandResult::MessageBodyFetched)
    }

    pub async fn handle_set_flags(
        &self,
        message_id: MessageId,
        mailbox_path: MailboxPath,
        flags_to_add: Vec<Flag>,
        flags_to_remove: Vec<Flag>,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, mut seq) = self.load_message(&message_id).await?;
        self.remote_uid(&mut message, &mut seq, &mailbox_path).await?;

        // Optimistic update: persist locally first for instant UI feedback,
        // then sync to IMAP in background. Next sync reconciles if IMAP fails.
        self.record(&message_id, |m| Some(m.change_flags(flags_to_add.clone(), flags_to_remove.clone())))
            .await?;

        info!(%message_id, "message flags updated locally, syncing to IMAP in background");
        self.push_flags(message.account_id, vec![message_id], flags_to_add, flags_to_remove);
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

    /// Record `\Seen` on one message, finding its UID on the server first
    /// when it has none so the flag can reach the server too.
    async fn mark_seen(&self, message_id: &MessageId, mailbox_path: &MailboxPath) -> Result<(), ApplicationError> {
        let (mut message, mut seq) = self.load_message(message_id).await?;
        if let Err(e) = self.remote_uid(&mut message, &mut seq, mailbox_path).await {
            warn!(%message_id, %e, "marked read locally only");
        }
        self.record(message_id, |m| Some(m.change_flags(vec![Flag::Seen], vec![]))).await.map(drop)
    }

    /// Store a flag change on the server in the background, in the account's
    /// turn: each message's mailbox and UID are read when the store is sent,
    /// so a move queued ahead of it (open, mark read, archive) is followed
    /// rather than racing it with a UID that no longer exists.
    fn push_flags(&self, account_id: AccountId, message_ids: Vec<MessageId>, add: Vec<Flag>, remove: Vec<Flag>) {
        if message_ids.is_empty() {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let _turn = this.turn(account_id).await;
            let mut by_mailbox: HashMap<MailboxId, Vec<ProviderUid>> = HashMap::new();
            for id in &message_ids {
                match this.load_message(id).await {
                    Ok((message, _)) => {
                        let mut stored = false;
                        for m in &message.memberships {
                            if let Some(uid) = m.provider_uid {
                                by_mailbox.entry(m.mailbox_id).or_default().push(uid);
                                stored = true;
                            }
                        }
                        if !stored {
                            warn!(message_id = %id, "no UID to store flags on, next sync will reconcile");
                        }
                    }
                    Err(e) => warn!(message_id = %id, %e, "flags not stored on the server"),
                }
            }
            let Ok(provider) = this.provider_registry.get_provider(&account_id) else {
                return;
            };
            for (mailbox_id, uids) in by_mailbox {
                let Ok(Some(mailbox)) = this.message_read_repo.get_mailbox(&mailbox_id).await else {
                    continue;
                };
                if let Err(e) = provider.set_flags_bulk(&mailbox.path, &uids, &add, &remove).await {
                    warn!(%e, "Background IMAP flag store failed — next sync will reconcile");
                }
            }
        });
    }

    /// Apply a flag change that arrived from a remote IMAP IDLE notification.
    ///
    /// Unlike `handle_set_flags`, this does NOT call the provider (flags are
    /// already changed on the server). It only updates the local projection
    /// by emitting a `MessageFlagged` domain event.
    pub async fn handle_apply_remote_flag_change(
        &self,
        mailbox_id: MailboxId,
        provider_uid: ProviderUid,
        new_flags: Vec<Flag>,
    ) -> Result<CommandResult, ApplicationError> {
        info!(mailbox_id = %mailbox_id, uid = provider_uid.value(), ?new_flags,
            "handle_apply_remote_flag_change: looking up message");

        // Look up the local message by provider UID
        let summary = self
            .message_read_repo
            .find_by_provider_uid(&mailbox_id, provider_uid.value())
            .await?
            .ok_or_else(|| {
                ApplicationError::NotFound(
                    "Message",
                    format!("provider_uid={} mailbox_id={}", provider_uid.value(), mailbox_id),
                )
            })?;

        let message_id = summary.id;
        info!(%message_id, uid = provider_uid.value(), "message found, loading aggregate");

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

        info!(%message_id, uid = provider_uid.value(), "remote flag change applied");
        Ok(CommandResult::RemoteFlagApplied)
    }

    /// Move the message, from wherever it is when the account's turn comes:
    /// an action queued ahead may already have moved it. One already in
    /// `to_mailbox_id` stays put.
    pub async fn handle_move(
        &self,
        message_id: MessageId,
        from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        _from_mailbox_path: MailboxPath,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_one(message_id, from_mailbox_id, to_mailbox(to_mailbox_id, to_mailbox_path)).await?;
        info!(%message_id, "message moved");
        Ok(CommandResult::MessageMoved)
    }

    /// Standard client behaviour: a plain delete moves the message to the
    /// Trash folder (Thunderbird-style). Only a permanent delete, a delete of
    /// a message already in Trash, or the absence of a Trash folder actually
    /// expunges it. Like a move, it acts on the message where it is when the
    /// account's turn comes, except that a message an earlier delete put in
    /// Trash meanwhile stays there: a double click must not purge it.
    pub async fn handle_delete(
        &self,
        message_id: MessageId,
        mailbox_id: MailboxId,
        _mailbox_path: MailboxPath,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_one(message_id, mailbox_id, to_trash_or_expunged(permanently)).await?;
        info!(%message_id, "message deleted");
        Ok(CommandResult::MessageDeleted)
    }

    /// One message's move/delete from `requested_in`; `destination` as in
    /// [`Self::apply_many`]. A message (or its mailbox) gone by its turn is
    /// nothing left to do, as when the same delete is sent twice.
    async fn apply_one(
        &self,
        message_id: MessageId,
        requested_in: MailboxId,
        destination: impl Fn(MailboxId, &Mailbox, &[Mailbox]) -> Destination,
    ) -> Result<(), ApplicationError> {
        let account_id = match self.load_message(&message_id).await {
            Ok((message, _)) => message.account_id,
            Err(ApplicationError::NotFound(..)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let _turn = self.turn(account_id).await;
        let (mut message, mut seq) = match self.load_message(&message_id).await {
            Err(ApplicationError::NotFound(..)) => return Ok(()),
            loaded => loaded?,
        };
        let mailboxes = self.message_read_repo.list_mailboxes(&account_id).await?;
        let Some(source) = source_mailbox(&message, requested_in, &mailboxes) else {
            return Ok(());
        };
        let Some(dest) = destination(requested_in, source, &mailboxes) else {
            return Ok(());
        };
        let uid = self.remote_uid(&mut message, &mut seq, &source.path).await?;
        let dest = dest.as_ref().map(|(id, path)| (*id, path));
        match self.apply_on_server(account_id, source, dest, vec![(message_id, uid)]).await {
            (_, Some(refused)) => Err(refused),
            _ => Ok(()),
        }
    }

    /// Bulk delete. Applies the same Trash-vs-expunge rules as [`Self::handle_delete`]
    /// per message, grouped by source mailbox: one provider call per group
    /// (per [`SERVER_CHUNK`]). The per-message path would open a connection
    /// each, and deleting a search result set of 800 would mean 800 of them.
    ///
    /// A message that can't be loaded, has no provider UID, or whose mailbox has
    /// vanished is counted in `failed` and skipped — a stale search index must not
    /// abort the rest of the batch halfway through. So is every message of a
    /// chunk the provider refused, and of the chunks after it.
    pub async fn handle_delete_many(
        &self,
        message_ids: Vec<MessageId>,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_many(message_ids, to_trash_or_expunged(permanently)).await
    }

    /// Bulk move to a single destination, grouped like [`Self::handle_delete_many`].
    pub async fn handle_move_many(
        &self,
        message_ids: Vec<MessageId>,
        to_mailbox_id: MailboxId,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_many(message_ids, to_mailbox(to_mailbox_id, to_mailbox_path)).await
    }

    /// Bulk move/delete. `destination` picks, from a message's source mailbox
    /// (and its account's mailboxes), where it goes: `Some(None)` expunges,
    /// `None` skips it. The source is wherever the message is when its
    /// account's turn comes, not when the command was sent.
    async fn apply_many(
        &self,
        message_ids: Vec<MessageId>,
        destination: impl Fn(MailboxId, &Mailbox, &[Mailbox]) -> Destination,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut failed, mut succeeded) = (0u32, 0u32);
        let mut by_account: HashMap<AccountId, Vec<(MessageId, MailboxId)>> = HashMap::new();
        for message_id in message_ids {
            match self.load_message(&message_id).await {
                Ok((message, _)) => match message.memberships.first() {
                    Some(home) => by_account.entry(message.account_id).or_default().push((message_id, home.mailbox_id)),
                    None => failed += 1,
                },
                Err(_) => failed += 1,
            }
        }

        type Group = (MailboxId, Option<(MailboxId, MailboxPath)>);
        for (account_id, ids) in by_account {
            let _turn = self.turn(account_id).await;
            let mailboxes = self.message_read_repo.list_mailboxes(&account_id).await?;
            let mut groups: HashMap<Group, Vec<(MessageId, ProviderUid)>> = HashMap::new();
            for (message_id, requested_in) in ids {
                // Gone while it waited for its turn: an earlier action took it.
                let Ok((mut message, mut seq)) = self.load_message(&message_id).await else {
                    continue;
                };
                let Some(source) = source_mailbox(&message, requested_in, &mailboxes) else {
                    failed += 1;
                    continue;
                };
                let Some(dest) = destination(requested_in, source, &mailboxes) else {
                    continue;
                };
                let Ok(provider_uid) = self.remote_uid(&mut message, &mut seq, &source.path).await else {
                    failed += 1;
                    continue;
                };
                groups.entry((source.id, dest)).or_default().push((message_id, provider_uid));
            }

            for ((from, dest), members) in groups {
                let Some(from) = mailboxes.iter().find(|m| m.id == from) else {
                    continue;
                };
                let count = members.len() as u32;
                let (done, refused) =
                    self.apply_on_server(account_id, from, dest.as_ref().map(|(id, path)| (*id, path)), members).await;
                if let Some(e) = refused {
                    warn!(%e, from = %from.path, applied = done, "bulk change refused by the server part-way");
                }
                succeeded += done;
                failed += count - done;
            }
        }

        info!(succeeded, failed, "bulk change applied");
        Ok(CommandResult::MessagesBulkChanged { succeeded, failed })
    }

    // ── Compose ────────────────────────────────────────────────────────

    pub async fn handle_prepare_compose(
        &self,
        accounts: &[Account],
        mode: ComposeMode,
        message_id: Option<MessageId>,
        account_id: Option<AccountId>,
    ) -> Result<CommandResult, ApplicationError> {
        let first = accounts
            .first()
            .ok_or_else(|| ApplicationError::PreconditionFailed("Nessun account configurato".into()))?;
        let mut view = ComposeView {
            draft_id: None,
            account_id: account_id
                .filter(|id| accounts.iter().any(|a| a.id == *id))
                .unwrap_or(first.id),
            identities: compose::identities(accounts),
            to: vec![],
            cc: vec![],
            bcc: vec![],
            subject: String::new(),
            body_md: String::new(),
            quoted_html: None,
            signature_html: None,
            forwarded_attachments: vec![],
            inline_images: vec![],
            mode,
            source_message_id: None,
        };

        if mode != ComposeMode::New {
            let id = message_id.ok_or_else(|| {
                ApplicationError::PreconditionFailed(format!("{}: manca il messaggio di partenza", mode.as_str()))
            })?;
            if mode == ComposeMode::Draft {
                self.restore_draft(&mut view, id).await?;
            } else {
                let (source, parsed) = self.load_source(id, mode == ComposeMode::Forward).await?;
                let envelope = &source.envelope;
                view.account_id = compose::reply_sender(accounts, envelope, source.account_id);
                view.source_message_id = Some(id);
                if mode == ComposeMode::Forward {
                    view.subject = compose::prefixed_subject("Fwd:", &envelope.subject);
                    if let Some(parsed) = &parsed {
                        let html = source.body_html.as_deref().unwrap_or_default();
                        view.forwarded_attachments = compose::listable_parts(id, parsed, html);
                    }
                } else {
                    let own: Vec<_> = accounts.iter().map(|a| a.identity.email.clone()).collect();
                    (view.to, view.cc) =
                        compose::reply_recipients(envelope, &own, mode == ComposeMode::ReplyAll);
                    view.subject = compose::prefixed_subject("Re:", &envelope.subject);
                }
                view.quoted_html = compose::quote_for(mode, &source).map(|q| q.html);
            }
        }

        view.signature_html = accounts
            .iter()
            .find(|a| a.id == view.account_id)
            .and_then(|a| a.identity.signature_html.clone());
        Ok(CommandResult::ComposePrepared(Box::new(view)))
    }

    async fn restore_draft(&self, view: &mut ComposeView, id: MessageId) -> Result<(), ApplicationError> {
        let draft = self
            .message_read_repo
            .get_message(&id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Message", id.to_string()))?;
        let parsed = compose::parse_source(&self.fetch_raw(&draft).await?)?;
        let meta = parsed.draft.clone();

        view.draft_id = Some(id);
        view.account_id = draft.account_id;
        view.to = parsed.to.clone();
        view.cc = parsed.cc.clone();
        view.bcc = parsed.bcc.clone();
        view.subject = parsed.subject.clone();
        view.body_md = meta
            .as_ref()
            .and_then(|m| m.body_md.clone())
            .or_else(|| parsed.text.clone())
            .unwrap_or_default();
        // Quote and threading are rebuilt from the original; without it the
        // draft carries on as a plain new message.
        view.mode = ComposeMode::New;
        let mut shown_html = compose::markdown_to_html(&view.body_md);
        if let Some(meta) = meta.filter(|m| m.mode.quotes_source())
            && let Some(source_id) = meta.source_message_id
        {
            match self.load_source(source_id, false).await {
                Ok((source, _)) => {
                    view.mode = meta.mode;
                    view.source_message_id = Some(source_id);
                    if let Some(quote) = compose::quote_for(meta.mode, &source) {
                        shown_html.push_str(&quote.html);
                        view.quoted_html = Some(quote.html);
                    }
                }
                Err(e) => tracing::warn!(%e, %source_id, "draft's original message is gone"),
            }
        }
        view.forwarded_attachments = compose::listable_parts(id, &parsed, &shown_html);
        view.inline_images =
            parsed.parts.into_iter().filter(|p| compose::referenced_inline(p, &shown_html)).collect();
        Ok(())
    }

    pub async fn handle_send_compose(
        &self,
        accounts: &[Account],
        input: ComposeInput,
    ) -> Result<CommandResult, ApplicationError> {
        let (account, source, outgoing) = self.build_outgoing(accounts, &input, false).await?;
        let provider = self.provider_registry.get_provider(&account.id)?;
        provider.send_message(&outgoing).await?;

        let mailboxes = self.message_read_repo.list_mailboxes(&account.id).await?;
        let sent = mailboxes.iter().find(|m| m.mailbox_type == MailboxType::Sent);

        // Shown in Sent right away instead of after the next sync. Falls back
        // to a placeholder id before the first sync has discovered Sent.
        // Projected before the Sent APPEND starts, so a sync storing the
        // server copy always finds the placeholder to supersede.
        let message_id = MessageId::new();
        let event = DomainEvent::MessageSent {
            message_id,
            account_id: account.id,
            mailbox_id: sent.map_or_else(MailboxId::new, |m| m.id),
            envelope: outgoing_envelope(&outgoing),
            message_rfc_id: outgoing.message_rfc_id.clone(),
            in_reply_to: outgoing.in_reply_to.clone(),
            references: outgoing.references.clone(),
            body_text: outgoing.body_text.clone(),
            body_html: outgoing.body_html.clone(),
            sent_at: chrono::Utc::now(),
        };
        // The message is out either way: still file the Sent copy and clean
        // up before reporting a failed record.
        let recorded = self.append_and_publish(&message_id, AggregateType::Message, 0, event).await;

        if let Some(sent) = sent.filter(|_| !compose::saves_sent_copy_itself(&account)) {
            let (provider, path, copy) = (Arc::clone(&provider), sent.path.clone(), outgoing.clone());
            tokio::spawn(async move {
                if let Err(e) = provider.append_message(&path, &copy, &[Flag::Seen]).await {
                    tracing::warn!(%e, "message sent, but storing the Sent copy failed");
                }
            });
        }

        if input.mode.is_reply()
            && let Some(source) = &source
            && let Some(home) = source.memberships.first()
            && let Some(mailbox) = mailboxes.iter().find(|m| m.id == home.mailbox_id)
            && let Err(e) = self
                .handle_set_flags(source.id, mailbox.path.clone(), vec![Flag::Answered], vec![])
                .await
        {
            tracing::warn!(%e, "Failed to set \\Answered on replied-to message");
        }

        if let Some(draft_id) = input.draft_id
            && let Err(e) = self.remove_draft(draft_id).await
        {
            tracing::warn!(%e, %draft_id, "message sent, but deleting its draft failed");
        }

        recorded?;
        info!(%message_id, "message sent");
        Ok(CommandResult::MessageSent { message_id })
    }

    /// Store the draft in the server's Drafts folder (and locally), then drop
    /// the version it replaces.
    pub async fn handle_save_draft(
        &self,
        accounts: &[Account],
        input: ComposeInput,
    ) -> Result<CommandResult, ApplicationError> {
        let (account, _, outgoing) = self.build_outgoing(accounts, &input, true).await?;
        let drafts = self
            .message_read_repo
            .list_mailboxes(&account.id)
            .await?
            .into_iter()
            .find(|m| m.mailbox_type == MailboxType::Drafts)
            .ok_or_else(|| {
                ApplicationError::PreconditionFailed("Cartella Bozze non trovata: sincronizza l'account".into())
            })?;

        let uid = self
            .provider_registry
            .get_provider(&account.id)?
            .append_message(&drafts.path, &outgoing, &[Flag::Draft, Flag::Seen])
            .await?
            .ok_or_else(|| ApplicationError::Internal("bozza salvata ma non ritrovata sul server".into()))?;

        let message_id = match self
            .message_read_repo
            .find_by_provider_uid(&drafts.id, uid.value())
            .await?
        {
            // A Drafts sync stored it first.
            Some(existing) => existing.id,
            None => self.store_draft_locally(account.id, drafts.id, uid, &outgoing).await?,
        };

        if let Some(old) = input.draft_id.filter(|old| *old != message_id)
            && let Err(e) = self.remove_draft(old).await
        {
            tracing::warn!(%e, draft_id = %old, "new draft saved, but deleting the previous version failed");
        }

        info!(%message_id, "draft saved");
        Ok(CommandResult::DraftSaved { message_id })
    }

    pub async fn handle_discard_draft(
        &self,
        message_id: MessageId,
    ) -> Result<CommandResult, ApplicationError> {
        self.remove_draft(message_id).await?;
        info!(%message_id, "draft discarded");
        Ok(CommandResult::DraftDiscarded)
    }

    async fn build_outgoing(
        &self,
        accounts: &[Account],
        input: &ComposeInput,
        draft: bool,
    ) -> Result<(Account, Option<Message>, OutgoingMessage), ApplicationError> {
        let account = accounts
            .iter()
            .find(|a| a.id == input.account_id)
            .ok_or_else(|| ApplicationError::NotFound("Account", input.account_id.to_string()))?;
        let to = compose::parse_addresses(&input.to)?;
        let cc = compose::parse_addresses(&input.cc)?;
        let bcc = compose::parse_addresses(&input.bcc)?;
        if !draft && to.is_empty() && cc.is_empty() && bcc.is_empty() {
            return Err(ApplicationError::PreconditionFailed("Aggiungi almeno un destinatario".into()));
        }

        let mode = input.mode;
        let mut sources: Vec<(MessageId, ParsedSource)> = Vec::new();
        let source = match input.source_message_id.filter(|_| mode.quotes_source()) {
            Some(id) => {
                let (message, parsed) = self.load_source(id, mode == ComposeMode::Forward).await?;
                sources.extend(parsed.map(|p| (id, p)));
                Some(message)
            }
            None => None,
        };
        let quote = source.as_ref().and_then(|s| compose::quote_for(mode, s));
        let body_html =
            compose::assemble_html(&input.body_md, account.identity.signature_html.as_deref(), quote.as_ref());
        let body_text =
            compose::assemble_text(&input.body_md, compose::signature_text(account).as_deref(), quote.as_ref());

        // The user's files, the parts of existing messages they kept, and every
        // inline image the HTML shows (forwarded original, restored draft).
        let kept = input
            .forwarded_attachment_ids
            .iter()
            .map(|id| compose::parse_attachment_id(id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut needed: Vec<MessageId> = kept.iter().map(|(id, _)| *id).collect();
        if !kept.is_empty() || body_html.contains("cid:") {
            needed.extend(input.draft_id);
        }
        for id in needed {
            if sources.iter().any(|(s, _)| *s == id) {
                continue;
            }
            // A kept part may point at a draft version an earlier save already
            // replaced; the part lives on, identical, in the current draft.
            let Some(message) = self.message_read_repo.get_message(&id).await? else {
                continue;
            };
            sources.push((id, compose::parse_source(&self.fetch_raw(&message).await?)?));
        }

        let mut attachments = input.attachments.clone();
        for (_, fingerprint) in kept {
            let part = sources
                .iter()
                .flat_map(|(_, parsed)| &parsed.parts)
                .find(|p| compose::part_fingerprint(p) == fingerprint)
                .ok_or_else(|| {
                    ApplicationError::PreconditionFailed("un allegato inoltrato non è più disponibile".into())
                })?;
            attachments.push(part.clone());
        }
        for (_, parsed) in &sources {
            for part in &parsed.parts {
                if compose::referenced_inline(part, &body_html)
                    && !attachments.iter().any(|a| a.content_id == part.content_id)
                {
                    attachments.push(part.clone());
                }
            }
        }

        let (in_reply_to, references) = match &source {
            Some(s) if mode.is_reply() => {
                let mut refs = s.references.clone();
                if refs.is_empty() {
                    refs.extend(s.in_reply_to.clone());
                }
                refs.extend(s.message_rfc_id.clone());
                (s.message_rfc_id.clone(), refs)
            }
            _ => (None, vec![]),
        };

        let from = compose::sender_address(account);
        let outgoing = OutgoingMessage {
            message_rfc_id: Some(compose::new_message_rfc_id(&from.email)),
            from,
            to,
            cc,
            bcc,
            subject: input.subject.clone(),
            body_text: Some(body_text),
            body_html: Some(body_html),
            in_reply_to,
            references,
            attachments,
            extra_headers: if draft {
                compose::draft_headers(mode, input.source_message_id, &input.body_md)
            } else {
                vec![]
            },
        };
        Ok((account.clone(), source, outgoing))
    }

    /// The message being replied to / forwarded, with its body. The raw MIME
    /// is fetched when asked for (attachments) or when no body is stored yet.
    async fn load_source(
        &self,
        id: MessageId,
        with_parts: bool,
    ) -> Result<(Message, Option<ParsedSource>), ApplicationError> {
        let mut message = self
            .message_read_repo
            .get_message(&id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Message", id.to_string()))?;
        if !with_parts && (message.body_text.is_some() || message.body_html.is_some()) {
            return Ok((message, None));
        }
        let parsed = compose::parse_source(&self.fetch_raw(&message).await?)?;
        message.body_text = message.body_text.or_else(|| parsed.text.clone());
        message.body_html = message.body_html.or_else(|| parsed.html.clone());
        Ok((message, Some(parsed)))
    }

    async fn fetch_raw(&self, message: &Message) -> Result<Vec<u8>, ApplicationError> {
        let home = message
            .memberships
            .first()
            .ok_or_else(|| ApplicationError::PreconditionFailed("il messaggio non è più su nessuna cartella".into()))?;
        let mailbox = self
            .message_read_repo
            .get_mailbox(&home.mailbox_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", home.mailbox_id.to_string()))?;
        let uid = match self.load_message(&message.id).await {
            Ok((mut aggregate, mut seq)) => self.remote_uid(&mut aggregate, &mut seq, &mailbox.path).await?,
            // Only received mail rehydrates; one sent from here has no server
            // copy until Sent syncs.
            Err(ApplicationError::NotFound(..)) => {
                return Err(ApplicationError::PreconditionFailed("il messaggio non è ancora sul server".into()));
            }
            Err(e) => return Err(e),
        };
        Ok(self
            .provider_registry
            .get_provider(&message.account_id)?
            .fetch_raw_message(&mailbox.path, uid)
            .await?)
    }

    async fn store_draft_locally(
        &self,
        account_id: AccountId,
        drafts_id: MailboxId,
        uid: ProviderUid,
        outgoing: &OutgoingMessage,
    ) -> Result<MessageId, ApplicationError> {
        let attachments: Vec<Attachment> = outgoing
            .attachments
            .iter()
            .map(|a| Attachment {
                id: AttachmentId::new(),
                filename: Some(a.filename.clone()),
                mime_type: a.mime_type.clone(),
                size_bytes: a.data.len() as u64,
                content_id: a.content_id.clone(),
                is_inline: a.content_id.is_some(),
            })
            .collect();
        let (mut message, received) = Message::receive(
            account_id,
            drafts_id,
            uid,
            outgoing.message_rfc_id.clone(),
            outgoing.in_reply_to.clone(),
            outgoing.references.clone(),
            outgoing_envelope(outgoing),
            vec![Flag::Draft, Flag::Seen],
            0,
            !attachments.is_empty(),
            chrono::Utc::now(),
        );
        let id = message.id;
        self.append_and_publish(&id, AggregateType::Message, 0, received).await?;

        let fetched = message.set_body(outgoing.body_text.clone(), outgoing.body_html.clone(), attachments);
        // Body before the event, as in SyncService::store_body.
        self.message_read_repo
            .put_body(&id, outgoing.body_text.clone(), outgoing.body_html.clone(), false)
            .await?;
        self.append_and_publish(&id, AggregateType::Message, 1, fetched).await?;
        Ok(id)
    }

    /// Delete a draft locally now and on the server in the background.
    async fn remove_draft(&self, id: MessageId) -> Result<(), ApplicationError> {
        let (mut message, mut seq) = self.load_message(&id).await?;
        let home = message.memberships.first().map(|m| m.mailbox_id);
        let mailbox = match home {
            Some(home) => self.message_read_repo.get_mailbox(&home).await?,
            None => None,
        };
        // No server copy to find is no reason to keep the local one.
        let uid = match &mailbox {
            Some(mailbox) => self.remote_uid(&mut message, &mut seq, &mailbox.path).await.ok(),
            None => None,
        };
        self.append_and_publish(&id, AggregateType::Message, seq, message.delete())
            .await?;

        if let (Some(uid), Some(mailbox)) = (uid, mailbox) {
            let provider = self.provider_registry.get_provider(&message.account_id)?;
            let this = self.clone();
            tokio::spawn(async move {
                let _turn = this.turn(message.account_id).await;
                if let Err(e) = provider.delete_message(&mailbox.path, uid, true).await {
                    tracing::warn!(%e, "Background IMAP draft delete failed — next sync will reconcile");
                }
            });
        }
        Ok(())
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /// The account's turn for a server-side change; see `account_queues`.
    async fn turn(&self, account_id: AccountId) -> tokio::sync::OwnedMutexGuard<()> {
        let queue = Arc::clone(self.account_queues.lock().unwrap().entry(account_id).or_default());
        queue.lock_owned().await
    }

    /// Move `members` (message, UID) from `from` to `dest`, or expunge them
    /// when `dest` is `None`, on the server first, a chunk at a time, each
    /// chunk recorded here as soon as the server confirms it. Returns how
    /// many the server applied and, if it refused a chunk, why: that chunk
    /// and the ones after it changed nowhere.
    async fn apply_on_server(
        &self,
        account_id: AccountId,
        from: &Mailbox,
        dest: Option<(MailboxId, &MailboxPath)>,
        members: Vec<(MessageId, ProviderUid)>,
    ) -> (u32, Option<ApplicationError>) {
        let provider = match self.provider_registry.get_provider(&account_id) {
            Ok(provider) => provider,
            Err(e) => return (0, Some(e.into())),
        };
        let mut done = 0;
        let mut out_of_step = false;
        for chunk in members.chunks(SERVER_CHUNK) {
            let uids: Vec<ProviderUid> = chunk.iter().map(|&(_, uid)| uid).collect();
            let applied = match dest {
                Some((_, to)) => provider.move_messages(&uids, &from.path, to).await,
                None => provider.delete_messages(&from.path, &uids, true).await.map(|()| HashMap::new()),
            };
            let new_uids = match applied {
                Ok(new_uids) => new_uids,
                Err(e) => {
                    // A timeout or a dropped connection may come after the
                    // server applied it: let a sync tell.
                    self.request_resync(account_id, &from.path, dest.map(|(_, to)| to));
                    return (done, Some(e.into()));
                }
            };
            for &(message_id, uid) in chunk {
                if let Err(e) = self.record_applied(&provider, message_id, from.id, dest, new_uids.get(&uid).copied()).await {
                    warn!(%message_id, %e, "done on the server but not recorded here, resyncing");
                    out_of_step = true;
                }
            }
            done += chunk.len() as u32;
        }
        if out_of_step {
            self.request_resync(account_id, &from.path, dest.map(|(_, to)| to));
        }
        (done, None)
    }

    /// Record a move or expunge the server confirmed. A moved message is
    /// recorded in `dest` only with evidence of its UID there — reported by
    /// the server, else found by Message-ID. With neither, the server had
    /// nothing under that UID any more (another client moved it) or the
    /// destination copy already belongs to another row: either way the real
    /// copy is, or will be, synced as a row of its own, so this one goes.
    /// `Err` means local state is out of step and needs a resync.
    async fn record_applied(
        &self,
        provider: &PR::Provider,
        message_id: MessageId,
        from: MailboxId,
        dest: Option<(MailboxId, &MailboxPath)>,
        reported_uid: Option<ProviderUid>,
    ) -> Result<(), ApplicationError> {
        let Some((to_id, to_path)) = dest else {
            return self.record(&message_id, |m| m.remove_from(from)).await.map(drop);
        };
        let new_uid = match reported_uid {
            Some(uid) => Some(uid),
            None => {
                let (message, _) = self.load_message(&message_id).await?;
                self.locate(provider, &message, &to_id, to_path).await.unwrap_or_else(|e| {
                    warn!(%message_id, %e, "moved, but its new UID could not be looked up");
                    None
                })
            }
        };
        let Some(uid) = new_uid else {
            self.record(&message_id, |m| m.remove_from(from)).await?;
            return Err(ApplicationError::Internal(format!("server confirmed the move to {to_path} without the new UID")));
        };
        // The destination already had that copy as a row of its own: a
        // Gmail label the message already carried, All Mail as the archive.
        // Recording the move too would make two rows of one server message.
        if self.message_read_repo.held_uids(&to_id, &[uid]).await?.contains(&uid) {
            return self.record(&message_id, |m| m.remove_from(from)).await.map(drop);
        }
        self.record(&message_id, |m| m.move_to(from, to_id, Some(uid))).await.map(drop)
    }

    fn request_resync(&self, account_id: AccountId, from: &MailboxPath, to: Option<&MailboxPath>) {
        let Some(resync) = &self.resync else {
            return;
        };
        for path in std::iter::once(from).chain(to) {
            // Closed only on shutdown.
            let _ = resync.send((account_id, path.clone()));
        }
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
            match self.append_and_publish(message_id, AggregateType::Message, seq, event).await {
                Err(ApplicationError::Domain(DomainError::ConcurrencyConflict { .. })) if attempt < RECORD_ATTEMPTS => {
                    attempt += 1;
                }
                recorded => return recorded.map(|()| true),
            }
        }
    }

    /// The UID `message` has in `mailbox` (at `path`), found by Message-ID:
    /// the newest copy no other local message already holds.
    async fn locate(
        &self,
        provider: &PR::Provider,
        message: &Message,
        mailbox_id: &MailboxId,
        path: &MailboxPath,
    ) -> Result<Option<ProviderUid>, ApplicationError> {
        let Some(rfc_id) = message.message_rfc_id.as_deref() else {
            return Ok(None);
        };
        let found = provider.find_message(path, rfc_id).await?;
        let held = self.message_read_repo.held_uids(mailbox_id, &found).await?;
        Ok(found.into_iter().rev().find(|uid| !held.contains(uid)))
    }

    /// The message's UID in its mailbox, at `mailbox_path`. One without (a
    /// renumbered mailbox, a move the server confirmed without its new UID)
    /// is looked up on the server by Message-ID and recorded, so every
    /// action on it can go ahead. `seq` follows the recording event.
    async fn remote_uid(
        &self,
        message: &mut Message,
        seq: &mut u64,
        mailbox_path: &MailboxPath,
    ) -> Result<ProviderUid, ApplicationError> {
        // A UID is valid only in its own mailbox: sending it to another, or
        // searching another, would act on some other message.
        let mut elsewhere = None;
        let mut here = None;
        for membership in &message.memberships {
            let mailbox = self
                .message_read_repo
                .get_mailbox(&membership.mailbox_id)
                .await?
                .ok_or_else(|| ApplicationError::NotFound("Mailbox", membership.mailbox_id.to_string()))?;
            if mailbox.path.full_path() == mailbox_path.full_path() {
                here = Some((*membership, mailbox));
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
        if let Some(uid) = membership.provider_uid {
            return Ok(uid);
        }
        let provider = self.provider_registry.get_provider(&message.account_id)?;
        let uid = self.locate(&provider, message, &mailbox.id, &mailbox.path).await?.ok_or_else(|| {
            ApplicationError::NotFound(
                "Message on the server",
                format!("{} in {mailbox_path}", message.message_rfc_id.as_deref().unwrap_or("(no Message-ID)")),
            )
        })?;
        if let Some(event) = message.attach_uid(mailbox.id, uid) {
            self.append_and_publish(&message.id, AggregateType::Message, *seq, event).await?;
            *seq += 1;
        }
        Ok(uid)
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

        let envelope =
            EventEnvelope::new(message_id.to_string(), aggregate_type, new_seq, event);

        // Project inline so the read model is updated BEFORE we return.
        // This ensures that any re-query after the command sees fresh data.
        if let Err(e) = self.message_read_repo.project(&envelope).await {
            tracing::warn!("Inline projection failed for {}: {}", envelope.payload.event_name(), e);
        }

        self.event_bus.publish(envelope);

        Ok(())
    }
}

/// Where a move/delete sends a message from its source mailbox (given the
/// account's mailboxes): `Some(Some(_))` a mailbox, `Some(None)` expunged,
/// `None` nowhere — it stays put.
type Destination = Option<Option<(MailboxId, MailboxPath)>>;

/// The mailbox a move/delete acts from: the one it was asked for if the
/// message is still there, else (an earlier action moved it) the one it is
/// in now.
fn source_mailbox<'a>(message: &Message, requested_in: MailboxId, mailboxes: &'a [Mailbox]) -> Option<&'a Mailbox> {
    let home = message.membership(requested_in).or_else(|| message.memberships.first())?;
    mailboxes.iter().find(|m| m.id == home.mailbox_id)
}

/// Moving a message to the folder it already sits in is a no-op, not a copy.
fn to_mailbox(id: MailboxId, path: MailboxPath) -> impl Fn(MailboxId, &Mailbox, &[Mailbox]) -> Destination {
    move |_, source, _| (source.id != id).then(|| Some((id, path.clone())))
}

/// `requested_in`: where the message was when the delete was asked for. A
/// delete expunges from Trash only when asked there: one that finds the
/// message moved to Trash since (the first of a double delete) is done.
fn to_trash_or_expunged(permanently: bool) -> impl Fn(MailboxId, &Mailbox, &[Mailbox]) -> Destination {
    move |requested_in, source, mailboxes| {
        if permanently {
            return Some(None);
        }
        if source.mailbox_type == MailboxType::Trash {
            return (source.id == requested_in).then_some(None);
        }
        let trash = mailboxes.iter().find(|m| m.mailbox_type == MailboxType::Trash);
        Some(trash.map(|t| (t.id, t.path.clone())))
    }
}

fn outgoing_envelope(outgoing: &OutgoingMessage) -> Envelope {
    Envelope {
        from: vec![outgoing.from.clone()],
        to: outgoing.to.clone(),
        cc: outgoing.cc.clone(),
        bcc: outgoing.bcc.clone(),
        reply_to: vec![],
        subject: outgoing.subject.clone(),
        date: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests;
