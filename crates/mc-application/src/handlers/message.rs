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
            .put_body(&message_id, detail.body_text.clone(), detail.body_html.clone())
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
                    Ok((Message { mailbox_id, provider_uid: Some(uid), .. }, _)) => by_mailbox.entry(mailbox_id).or_default().push(uid),
                    Ok(_) => warn!(message_id = %id, "no UID to store flags on, next sync will reconcile"),
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
        _from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        _from_mailbox_path: MailboxPath,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_one(message_id, to_mailbox(to_mailbox_id, to_mailbox_path)).await?;
        info!(%message_id, "message moved");
        Ok(CommandResult::MessageMoved)
    }

    /// Standard client behaviour: a plain delete moves the message to the
    /// Trash folder (Thunderbird-style). Only a permanent delete, a delete of
    /// a message already in Trash, or the absence of a Trash folder actually
    /// expunges it. Like a move, it acts on the message where it is when the
    /// account's turn comes.
    pub async fn handle_delete(
        &self,
        message_id: MessageId,
        _mailbox_id: MailboxId,
        _mailbox_path: MailboxPath,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        self.apply_one(message_id, to_trash_or_expunged(permanently)).await?;
        info!(%message_id, "message deleted");
        Ok(CommandResult::MessageDeleted)
    }

    /// One message's move/delete; `destination` as in [`Self::apply_many`].
    /// A message (or its mailbox) gone by its turn is nothing left to do,
    /// as when the same delete is sent twice.
    async fn apply_one(&self, message_id: MessageId, destination: impl Fn(&Mailbox, &[Mailbox]) -> Destination) -> Result<(), ApplicationError> {
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
        let Some(source) = mailboxes.iter().find(|m| m.id == message.mailbox_id) else {
            return Ok(());
        };
        let Some(dest) = destination(source, &mailboxes) else {
            return Ok(());
        };
        let uid = self.remote_uid(&mut message, &mut seq, &source.path).await?;
        let dest = dest.as_ref().map(|(id, path)| (*id, path));
        match self.apply_on_server(account_id, &source.path, dest, vec![(message_id, uid)]).await {
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
        destination: impl Fn(&Mailbox, &[Mailbox]) -> Destination,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut failed, mut succeeded) = (0u32, 0u32);
        let mut by_account: HashMap<AccountId, Vec<MessageId>> = HashMap::new();
        for message_id in message_ids {
            match self.load_message(&message_id).await {
                Ok((message, _)) => by_account.entry(message.account_id).or_default().push(message_id),
                Err(_) => failed += 1,
            }
        }

        type Group = (MailboxPath, Option<(MailboxId, MailboxPath)>);
        for (account_id, ids) in by_account {
            let _turn = self.turn(account_id).await;
            let mailboxes = self.message_read_repo.list_mailboxes(&account_id).await?;
            let mut groups: HashMap<Group, Vec<(MessageId, ProviderUid)>> = HashMap::new();
            for message_id in ids {
                // Gone while it waited for its turn: an earlier action took it.
                let Ok((mut message, mut seq)) = self.load_message(&message_id).await else {
                    continue;
                };
                let Some(source) = mailboxes.iter().find(|m| m.id == message.mailbox_id) else {
                    failed += 1;
                    continue;
                };
                let Some(dest) = destination(source, &mailboxes) else {
                    continue;
                };
                let Ok(provider_uid) = self.remote_uid(&mut message, &mut seq, &source.path).await else {
                    failed += 1;
                    continue;
                };
                groups.entry((source.path.clone(), dest)).or_default().push((message_id, provider_uid));
            }

            for ((from, dest), members) in groups {
                let count = members.len() as u32;
                let (done, refused) =
                    self.apply_on_server(account_id, &from, dest.as_ref().map(|(id, path)| (*id, path)), members).await;
                if let Some(e) = refused {
                    warn!(%e, %from, applied = done, "bulk change refused by the server part-way");
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
            && let Some(mailbox) = mailboxes.iter().find(|m| m.id == source.mailbox_id)
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
        let mailbox = self
            .message_read_repo
            .get_mailbox(&message.mailbox_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", message.mailbox_id.to_string()))?;
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
            .put_body(&id, outgoing.body_text.clone(), outgoing.body_html.clone())
            .await?;
        self.append_and_publish(&id, AggregateType::Message, 1, fetched).await?;
        Ok(id)
    }

    /// Delete a draft locally now and on the server in the background.
    async fn remove_draft(&self, id: MessageId) -> Result<(), ApplicationError> {
        let (mut message, mut seq) = self.load_message(&id).await?;
        let mailbox = self.message_read_repo.get_mailbox(&message.mailbox_id).await?;
        // No server copy to find is no reason to keep the local one.
        let uid = match &mailbox {
            Some(mailbox) => self.remote_uid(&mut message, &mut seq, &mailbox.path).await.ok(),
            None => None,
        };
        self.append_and_publish(&id, AggregateType::Message, seq, message.delete(true))
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
        from: &MailboxPath,
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
                Some((_, to)) => provider.move_messages(&uids, from, to).await,
                None => provider.delete_messages(from, &uids, true).await.map(|()| HashMap::new()),
            };
            let new_uids = match applied {
                Ok(new_uids) => new_uids,
                Err(e) => {
                    // A timeout or a dropped connection may come after the
                    // server applied it: let a sync tell.
                    self.request_resync(account_id, from, dest.map(|(_, to)| to));
                    return (done, Some(e.into()));
                }
            };
            for &(message_id, uid) in chunk {
                if let Err(e) = self.record_applied(&provider, message_id, dest, new_uids.get(&uid).copied()).await {
                    warn!(%message_id, %e, "done on the server but not recorded here, resyncing");
                    out_of_step = true;
                }
            }
            done += chunk.len() as u32;
        }
        if out_of_step {
            self.request_resync(account_id, from, dest.map(|(_, to)| to));
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
        dest: Option<(MailboxId, &MailboxPath)>,
        reported_uid: Option<ProviderUid>,
    ) -> Result<(), ApplicationError> {
        let Some((to_id, to_path)) = dest else {
            return self.record(&message_id, |m| Some(m.delete(true))).await.map(drop);
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
            self.record(&message_id, |m| Some(m.delete(false))).await?;
            return Err(ApplicationError::Internal(format!("server confirmed the move to {to_path} without the new UID")));
        };
        // The destination already had that copy as a row of its own: a
        // Gmail label the message already carried, All Mail as the archive.
        // Recording the move too would make two rows of one server message.
        if self.message_read_repo.held_uids(&to_id, &[uid]).await?.contains(&uid) {
            return self.record(&message_id, |m| Some(m.delete(false))).await.map(drop);
        }
        self.record(&message_id, |m| Some(m.move_to(to_id, Some(uid)))).await.map(drop)
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
        let mailbox = self
            .message_read_repo
            .get_mailbox(&message.mailbox_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", message.mailbox_id.to_string()))?;
        // A UID is valid only in its own mailbox: sending it to another, or
        // searching another, would act on some other message.
        if mailbox.path.full_path() != mailbox_path.full_path() {
            return Err(ApplicationError::PreconditionFailed(format!(
                "message is in {}, not {mailbox_path}",
                mailbox.path
            )));
        }
        if let Some(uid) = message.provider_uid {
            return Ok(uid);
        }
        let provider = self.provider_registry.get_provider(&message.account_id)?;
        let uid = self.locate(&provider, message, &mailbox.id, &mailbox.path).await?.ok_or_else(|| {
            ApplicationError::NotFound(
                "Message on the server",
                format!("{} in {mailbox_path}", message.message_rfc_id.as_deref().unwrap_or("(no Message-ID)")),
            )
        })?;
        if let Some(event) = message.attach_uid(message.mailbox_id, uid) {
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

        // A deleted message still rehydrates, in its last mailbox.
        let message = Message::rehydrate(&domain_events)
            .filter(|_| !domain_events.iter().any(|e| matches!(e, DomainEvent::MessageDeleted { .. })))
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

/// Moving a message to the folder it already sits in is a no-op, not a copy.
fn to_mailbox(id: MailboxId, path: MailboxPath) -> impl Fn(&Mailbox, &[Mailbox]) -> Destination {
    move |source, _| (source.id != id).then(|| Some((id, path.clone())))
}

fn to_trash_or_expunged(permanently: bool) -> impl Fn(&Mailbox, &[Mailbox]) -> Destination {
    move |source, mailboxes| {
        let trash = mailboxes.iter().find(|m| m.mailbox_type == MailboxType::Trash);
        let to_trash = !permanently && source.mailbox_type != MailboxType::Trash;
        Some(to_trash.then_some(trash).flatten().map(|t| (t.id, t.path.clone())))
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
mod tests {
    use super::*;
    use mc_domain::{
        AggregateType, DomainError, EventEnvelope, FetchRange, Mailbox, MailboxStatus,
        MessageEnvelopeSummary, ProviderNotification, ProviderUid,
    };
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::RwLock;

    // ── Mock EventStore ────────────────────────────────────────────────

    struct MockEventStore {
        events: RwLock<HashMap<String, Vec<EventEnvelope>>>,
        /// Appends that lose to another writer, as a sync appending first.
        conflicts: std::sync::Mutex<u32>,
    }

    impl MockEventStore {
        fn new() -> Self {
            Self {
                events: RwLock::new(HashMap::new()),
                conflicts: Default::default(),
            }
        }

        fn with_message(message: &Message) -> Self {
            let store = Self::new();
            store.add(message);
            store
        }

        fn add(&self, message: &Message) {
            // Seed with a MessageReceived event.
            let event = DomainEvent::MessageReceived {
                message_id: message.id,
                account_id: message.account_id,
                mailbox_id: message.mailbox_id,
                provider_uid: message.provider_uid.unwrap(),
                message_rfc_id: message.message_rfc_id.clone(),
                in_reply_to: message.in_reply_to.clone(),
                references: message.references.clone(),
                envelope: message.envelope.clone(),
                flags: message.flags.clone(),
                size_bytes: message.size_bytes,
                has_attachments: message.has_attachments,
                received_at: message.received_at,
            };
            let env = EventEnvelope::new(message.id.to_string(), AggregateType::Message, 1, event);
            self.events.write().unwrap().insert(message.id.to_string(), vec![env]);
        }
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            aggregate_id: &str,
            _aggregate_type: AggregateType,
            expected_sequence: u64,
            events: Vec<DomainEvent>,
        ) -> Result<u64, DomainError> {
            let mut store = self.events.write().unwrap();
            let list = store.entry(aggregate_id.to_string()).or_default();
            let mut seq = list.len() as u64;
            let mut conflicts = self.conflicts.lock().unwrap();
            if *conflicts > 0 {
                *conflicts -= 1;
                list.push(EventEnvelope::new(
                    aggregate_id.to_string(),
                    _aggregate_type,
                    seq + 1,
                    DomainEvent::MessageFlagged {
                        // Only the flags matter to the aggregate.
                        message_id: MessageId::new(),
                        mailbox_id: MailboxId::new(),
                        flags_added: vec![Flag::Flagged],
                        flags_removed: vec![],
                    },
                ));
                return Err(DomainError::ConcurrencyConflict { expected: expected_sequence, actual: seq + 1 });
            }
            if seq != expected_sequence {
                return Err(DomainError::ConcurrencyConflict { expected: expected_sequence, actual: seq });
            }
            for event in events {
                seq += 1;
                let env = EventEnvelope::new(aggregate_id.to_string(), _aggregate_type, seq, event);
                list.push(env);
            }
            Ok(seq)
        }

        async fn load_events(
            &self,
            aggregate_id: &str,
            _from_sequence: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            Ok(self
                .events
                .read()
                .unwrap()
                .get(aggregate_id)
                .cloned()
                .unwrap_or_default())
        }

        fn load_all_events(
            &self,
            _aggregate_type: Option<AggregateType>,
            _from_global_position: u64,
        ) -> Pin<Box<dyn futures::Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>> {
            Box::pin(futures::stream::empty())
        }

        fn subscribe(&self) -> Pin<Box<dyn futures::Stream<Item = EventEnvelope> + Send + '_>> {
            Box::pin(futures::stream::empty())
        }
    }

    // ── Mock EmailProvider ─────────────────────────────────────────────

    #[derive(Default)]
    struct MockEmailProvider {
        /// Message to return from fetch_message_detail.
        detail_response: RwLock<Option<Message>>,
        sent: RwLock<Vec<OutgoingMessage>>,
        appended: RwLock<Vec<(MailboxPath, OutgoingMessage, Vec<Flag>)>>,
        raw: RwLock<Vec<u8>>,
        /// Runs while the body fetch is "on the wire".
        during_fetch: RwLock<Option<Box<dyn Fn() + Send + Sync>>>,
        /// UID reported for the moved message in its destination.
        moved_uid: RwLock<Option<ProviderUid>>,
        /// What `find_message` finds on the server.
        found: RwLock<Vec<ProviderUid>>,
        /// UIDs `set_flags_bulk` was asked to flag.
        flagged_bulk: RwLock<Vec<ProviderUid>>,
        /// UIDs `fetch_message_detail` was asked for.
        fetched: RwLock<Vec<ProviderUid>>,
        /// UIDs `move_message` was asked to move.
        moved: RwLock<Vec<ProviderUid>>,
        /// Makes `move_message` fail, as a server refusing it.
        refuse: RwLock<bool>,
        /// `move_message` moves, then times out before the server's answer.
        time_out: RwLock<bool>,
        /// Makes `move_message` fail once this many messages moved.
        refuse_after: RwLock<Option<usize>>,
        /// Mailbox of each `set_flags_bulk` call.
        flagged_in: RwLock<Vec<MailboxPath>>,
    }

    impl MockEmailProvider {
        fn new() -> Self {
            Self::default()
        }

        /// One whose moves report `uid` as the destination UID.
        fn moving_to(uid: u32) -> Self {
            Self { moved_uid: RwLock::new(Some(ProviderUid::new(uid))), ..Self::default() }
        }
    }

    #[async_trait]
    impl EmailProvider for MockEmailProvider {
        async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_message_envelopes(
            &self,
            _mailbox: &MailboxPath,
            _range: FetchRange,
        ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_message_detail(
            &self,
            _mailbox: &MailboxPath,
            uid: ProviderUid,
        ) -> Result<Message, DomainError> {
            self.fetched.write().unwrap().push(uid);
            if let Some(during) = self.during_fetch.read().unwrap().as_ref() {
                during();
            }
            self.detail_response
                .read()
                .unwrap()
                .clone()
                .ok_or_else(|| DomainError::Provider("no detail configured".into()))
        }
        async fn find_message(
            &self,
            _mailbox: &MailboxPath,
            _rfc_id: &str,
        ) -> Result<Vec<ProviderUid>, DomainError> {
            Ok(self.found.read().unwrap().clone())
        }
        async fn uids_from(&self, _mailbox: &MailboxPath, _from: ProviderUid) -> Result<Vec<ProviderUid>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_raw_message(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Vec<u8>, DomainError> {
            Ok(self.raw.read().unwrap().clone())
        }
        async fn set_flags(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
            _add: &[Flag],
            _remove: &[Flag],
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn set_flags_bulk(
            &self,
            mailbox: &MailboxPath,
            uids: &[ProviderUid],
            _add: &[Flag],
            _remove: &[Flag],
        ) -> Result<(), DomainError> {
            self.flagged_in.write().unwrap().push(mailbox.clone());
            self.flagged_bulk.write().unwrap().extend_from_slice(uids);
            Ok(())
        }
        async fn move_message(
            &self,
            uid: ProviderUid,
            _from: &MailboxPath,
            _to: &MailboxPath,
        ) -> Result<Option<ProviderUid>, DomainError> {
            let moved = self.moved.read().unwrap().len();
            if *self.refuse.read().unwrap() || self.refuse_after.read().unwrap().is_some_and(|n| moved >= n) {
                return Err(DomainError::Provider("refused".into()));
            }
            self.moved.write().unwrap().push(uid);
            if *self.time_out.read().unwrap() {
                return Err(DomainError::ConnectionTimeout("move".into()));
            }
            Ok(*self.moved_uid.read().unwrap())
        }
        async fn delete_message(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
            _permanently: bool,
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn send_message(&self, message: &OutgoingMessage) -> Result<(), DomainError> {
            self.sent.write().unwrap().push(message.clone());
            Ok(())
        }
        async fn append_message(
            &self,
            mailbox: &MailboxPath,
            message: &OutgoingMessage,
            flags: &[Flag],
        ) -> Result<Option<ProviderUid>, DomainError> {
            self.appended
                .write()
                .unwrap()
                .push((mailbox.clone(), message.clone(), flags.to_vec()));
            Ok(Some(ProviderUid::new(7)))
        }
        async fn mailbox_status(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<MailboxStatus, DomainError> {
            Ok(MailboxStatus {
                uid_validity: 1,
                uid_next: 1,
                total_messages: 0,
                unseen_messages: 0,
            })
        }
        async fn fetch_uid_flags(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<Vec<mc_domain::traits::email_provider::UidFlagEntry>, DomainError> {
            Ok(vec![])
        }
        fn watch_mailbox(
            &self,
            _mailbox: &MailboxPath,
        ) -> Pin<
            Box<dyn futures::Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>,
        > {
            Box::pin(futures::stream::empty())
        }
        async fn fetch_bodies_bulk(
            &self,
            _mailbox: &MailboxPath,
            _uids: &[ProviderUid],
        ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError> {
            Ok(vec![])
        }
    }

    // ── Mock ProviderRegistry ──────────────────────────────────────────

    struct MockProviderRegistry {
        provider: Arc<MockEmailProvider>,
    }

    impl MockProviderRegistry {
        fn new(provider: Arc<MockEmailProvider>) -> Self {
            Self { provider }
        }
    }

    impl ProviderRegistry for MockProviderRegistry {
        type Provider = MockEmailProvider;

        fn get_provider(
            &self,
            _account_id: &AccountId,
        ) -> Result<Arc<MockEmailProvider>, DomainError> {
            Ok(self.provider.clone())
        }

        fn register_provider(&self, _account_id: AccountId, _provider: Arc<MockEmailProvider>) {}

        fn remove_provider(&self, _account_id: &AccountId) {}
    }

    // ── Mock MessageReadRepository ─────────────────────────────────────

    /// (message_id, body_text, body_html) as handed to `MessageBodyStore::put_body`.
    type StoredBody = (MessageId, Option<String>, Option<String>);

    #[derive(Default)]
    struct MockMessageReadRepo {
        mailboxes: Vec<mc_domain::Mailbox>,
        messages: Vec<Message>,
        stored_bodies: std::sync::Mutex<Vec<StoredBody>>,
        unread: Vec<mc_domain::MessageSummary>,
        /// UIDs other local rows already hold.
        held: Vec<ProviderUid>,
    }

    impl MockMessageReadRepo {
        fn new() -> Self {
            Self::default()
        }

        fn with_mailboxes(mailboxes: Vec<mc_domain::Mailbox>) -> Self {
            Self { mailboxes, ..Self::default() }
        }
    }

    #[async_trait]
    impl MessageBodyStore for MockMessageReadRepo {
        async fn put_body(
            &self,
            message_id: &MessageId,
            body_text: Option<String>,
            body_html: Option<String>,
        ) -> Result<(), DomainError> {
            self.stored_bodies.lock().unwrap().push((*message_id, body_text, body_html));
            Ok(())
        }
    }

    #[async_trait]
    impl mc_domain::MailboxReadRepository for MockMessageReadRepo {
        async fn get_mailbox(
            &self,
            id: &MailboxId,
        ) -> Result<Option<mc_domain::Mailbox>, DomainError> {
            Ok(self.mailboxes.iter().find(|m| m.id == *id).cloned())
        }
        async fn list_mailboxes(
            &self,
            account_id: &AccountId,
        ) -> Result<Vec<mc_domain::Mailbox>, DomainError> {
            Ok(self
                .mailboxes
                .iter()
                .filter(|m| m.account_id == *account_id)
                .cloned()
                .collect())
        }
    }

    #[async_trait]
    impl MessageReadRepository for MockMessageReadRepo {
        async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
            Ok(self.messages.iter().find(|m| m.id == *id).cloned())
        }
        async fn list_messages(
            &self,
            _mailbox_id: &MailboxId,
            _pagination: &mc_shared::pagination::Pagination,
        ) -> Result<mc_shared::pagination::PagedResult<mc_domain::MessageSummary>, DomainError>
        {
            Ok(mc_shared::pagination::PagedResult {
                items: vec![],
                total_count: 0,
                page: 1,
                page_size: 20,
            })
        }
        async fn get_thread(&self, _id: &MessageId) -> Result<Vec<Message>, DomainError> {
            Ok(vec![])
        }
        async fn find_by_provider_uid(
            &self,
            _mailbox_id: &MailboxId,
            _provider_uid: u32,
        ) -> Result<Option<mc_domain::MessageSummary>, DomainError> {
            Ok(None)
        }

        async fn held_uids(
            &self,
            _mailbox_id: &MailboxId,
            uids: &[ProviderUid],
        ) -> Result<std::collections::HashSet<ProviderUid>, DomainError> {
            Ok(self.held.iter().copied().filter(|uid| uids.contains(uid)).collect())
        }
        async fn uidless_messages(&self, _mailbox_id: &MailboxId) -> Result<Vec<MessageId>, DomainError> {
            Ok(vec![])
        }
        async fn find_by_rfc_id(
            &self,
            _account_id: &AccountId,
            _rfc_id: &str,
        ) -> Result<Vec<mc_domain::MessageSummary>, DomainError> {
            Ok(vec![])
        }

        async fn find_unread_by_mailbox(
            &self,
            _mailbox_id: &MailboxId,
        ) -> Result<Vec<mc_domain::MessageSummary>, DomainError> {
            Ok(self.unread.clone())
        }

        async fn scan_all_messages(
            &self,
            _account_id: Option<&mc_domain::AccountId>,
        ) -> Result<Vec<mc_domain::Message>, DomainError> {
            Ok(vec![])
        }

        async fn suggest(
            &self,
            _prefix: &str,
            _scope: Option<mc_domain::SuggestScope>,
            _max_per_field: usize,
        ) -> Result<Vec<mc_domain::Suggestion>, DomainError> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl ReadModelProjector for MockMessageReadRepo {
        async fn project(&self, _envelope: &EventEnvelope) -> Result<(), DomainError> {
            Ok(()) // no-op in tests
        }
    }

    // ── Mock EventBus ──────────────────────────────────────────────────

    struct MockEventBus {
        published: RwLock<Vec<EventEnvelope>>,
    }

    impl MockEventBus {
        fn new() -> Self {
            Self {
                published: RwLock::new(Vec::new()),
            }
        }

        fn last_event_name(&self) -> Option<String> {
            self.published
                .read()
                .unwrap()
                .last()
                .map(|e| e.payload.event_name().to_string())
        }
    }

    impl EventPublisher for MockEventBus {
        fn publish(&self, envelope: EventEnvelope) {
            self.published.write().unwrap().push(envelope);
        }
    }

    // ── Test helpers ───────────────────────────────────────────────────

    use mc_domain::{Address, EmailAddress};

    fn test_envelope() -> Envelope {
        Envelope {
            from: vec![Address {
                name: Some("Sender".into()),
                email: EmailAddress::new("sender@example.com").unwrap(),
            }],
            to: vec![Address {
                name: None,
                email: EmailAddress::new("recipient@example.com").unwrap(),
            }],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "Test".into(),
            date: chrono::Utc::now(),
        }
    }

    fn make_test_message() -> Message {
        let (msg, _) = Message::receive(
            AccountId::new(),
            MailboxId::new(),
            ProviderUid::new(42),
            None,
            None,
            vec![],
            test_envelope(),
            vec![],
            2048,
            false,
            chrono::Utc::now(),
        );
        msg
    }

    /// The INBOX `msg` sits in.
    fn inbox_of(msg: &Message) -> mc_domain::Mailbox {
        mc_domain::Mailbox { id: msg.mailbox_id, ..test_mailbox(msg.account_id, "INBOX", MailboxType::Inbox) }
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_set_flags_marks_as_read() {
        let msg = make_test_message();
        let msg_id = msg.id;

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_set_flags(msg_id, MailboxPath::inbox(), vec![Flag::Seen], vec![])
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::FlagsSet));
        assert_eq!(bus.last_event_name().unwrap(), "MessageFlagged");
    }

    #[tokio::test]
    async fn handle_mark_mailbox_read_with_no_unread_is_noop() {
        let msg = make_test_message();
        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new()); // find_unread_by_mailbox -> []
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_mark_mailbox_read(MailboxId::new(), MailboxPath::inbox())
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MailboxMarkedRead { count: 0 }));
        assert!(bus.last_event_name().is_none());
    }

    #[tokio::test]
    async fn handle_move_produces_event() {
        let msg = make_test_message();
        let msg_id = msg.id;

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::moving_to(900)),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_move(
                msg_id,
                MailboxId::new(),
                MailboxId::new(),
                MailboxPath::inbox(),
                MailboxPath::parse("Archive", '/').unwrap(),
            )
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageMoved));
        assert_eq!(bus.last_event_name().unwrap(), "MessageMoved");
    }

    #[tokio::test]
    async fn handle_delete_produces_event() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let mbx_id = msg.mailbox_id;

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::moving_to(900)),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageDeleted));
        assert_eq!(bus.last_event_name().unwrap(), "MessageDeleted");
    }

    #[tokio::test]
    async fn handle_delete_moves_to_trash_when_trash_exists() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let mbx_id = msg.mailbox_id;

        // A Trash folder exists for this account and the message is not in it,
        // so a plain delete must move-to-Trash (MessageMoved), not expunge.
        let trash = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id: msg.account_id,
            path: MailboxPath::parse("Trash", '/').unwrap(),
            display_name: "Trash".into(),
            mailbox_type: MailboxType::Trash,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::moving_to(900))));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg), trash]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        let result = handler
            .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), false)
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageDeleted));
        assert_eq!(bus.last_event_name().unwrap(), "MessageMoved");
    }

    /// The bulk path must survive ids it cannot resolve: a search index that is
    /// behind the mailbox will hand it messages that no longer exist, and one of
    /// those must not abort the rest of the batch.
    #[tokio::test]
    async fn handle_delete_many_counts_partial_success() {
        let msg = make_test_message();
        let msg_id = msg.id;

        let source = mc_domain::Mailbox {
            id: msg.mailbox_id,
            account_id: msg.account_id,
            path: MailboxPath::inbox(),
            display_name: "INBOX".into(),
            mailbox_type: MailboxType::Inbox,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };
        let trash = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id: msg.account_id,
            path: MailboxPath::parse("Trash", '/').unwrap(),
            display_name: "Trash".into(),
            mailbox_type: MailboxType::Trash,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::moving_to(900))));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![source, trash]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        // One real message, one id that isn't in the event store.
        let result = handler
            .handle_delete_many(vec![msg_id, MessageId::new()], false)
            .await
            .unwrap();

        assert!(
            matches!(result, CommandResult::MessagesBulkChanged { succeeded: 1, failed: 1 }),
            "expected 1 ok / 1 skipped, got {result:?}"
        );
        // Trash exists and the message wasn't in it, so a plain bulk delete moves.
        assert_eq!(bus.last_event_name().unwrap(), "MessageMoved");
    }

    #[tokio::test]
    async fn handle_delete_permanent_expunges_even_with_trash() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let mbx_id = msg.mailbox_id;

        let trash = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id: msg.account_id,
            path: MailboxPath::parse("Trash", '/').unwrap(),
            display_name: "Trash".into(),
            mailbox_type: MailboxType::Trash,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg), trash]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus.clone());

        // permanently = true → expunge, MessageDeleted, even though Trash exists.
        let result = handler
            .handle_delete(msg_id, mbx_id, MailboxPath::inbox(), true)
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessageDeleted));
        assert_eq!(bus.last_event_name().unwrap(), "MessageDeleted");
    }

    #[tokio::test]
    async fn handle_nonexistent_message_returns_not_found() {
        let es = Arc::new(MockEventStore::new());
        let registry = Arc::new(MockProviderRegistry::new(
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo, bus);

        let err = handler
            .handle_set_flags(
                MessageId::new(),
                MailboxPath::inbox(),
                vec![Flag::Seen],
                vec![],
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ApplicationError::NotFound(..)));
    }

    fn test_account(email: &str, imap_host: &str) -> Account {
        let email = EmailAddress::new(email).unwrap();
        Account::create(
            "Lavoro".into(),
            email.clone(),
            mc_domain::ProviderType::Imap,
            mc_domain::ConnectionConfig {
                imap_host: Some(imap_host.into()),
                imap_port: Some(993),
                use_tls: true,
                auth: mc_domain::AuthConfig::Plain { username: "u".into(), password: String::new() },
                smtp: None,
                smtp_host: None,
                smtp_port: None,
            },
            mc_domain::Identity {
                display_name: "Mario Rossi".into(),
                email,
                signature_html: None,
                signature_text: Some("Mario".into()),
            },
        )
        .0
    }

    fn test_mailbox(account_id: AccountId, name: &str, kind: MailboxType) -> mc_domain::Mailbox {
        mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id,
            path: MailboxPath::parse(name, '/').unwrap(),
            display_name: name.into(),
            mailbox_type: kind,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        }
    }

    fn compose_input(account_id: AccountId) -> ComposeInput {
        ComposeInput {
            draft_id: None,
            account_id,
            to: vec!["Anna <anna@example.com>".into()],
            cc: vec![],
            bcc: vec![],
            subject: "Ciao".into(),
            body_md: "**ciao**".into(),
            mode: ComposeMode::New,
            source_message_id: None,
            attachments: vec![mc_domain::OutgoingAttachment {
                filename: "a.pdf".into(),
                mime_type: "application/pdf".into(),
                content_id: None,
                data: vec![1, 2],
            }],
            forwarded_attachment_ids: vec![],
        }
    }

    /// Let spawned fire-and-forget provider calls run.
    async fn drain_spawned() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    struct Fixture {
        account: Account,
        provider: Arc<MockEmailProvider>,
        bus: Arc<MockEventBus>,
        sent_box: mc_domain::Mailbox,
        drafts_box: mc_domain::Mailbox,
        /// A message stored in INBOX (event store + read model).
        seeded: Option<Message>,
        handler: MessageCommandHandler<MockEventStore, MockProviderRegistry, MockMessageReadRepo, MockEventBus>,
    }

    fn fixture(imap_host: &str, seed: impl FnOnce(AccountId, MailboxId) -> Option<Message>) -> Fixture {
        let account = test_account("mario@example.com", imap_host);
        let inbox = test_mailbox(account.id, "INBOX", MailboxType::Inbox);
        let seeded = seed(account.id, inbox.id);
        let sent_box = test_mailbox(account.id, "Sent", MailboxType::Sent);
        let drafts_box = test_mailbox(account.id, "Drafts", MailboxType::Drafts);
        let provider = Arc::new(MockEmailProvider::new());
        let bus = Arc::new(MockEventBus::new());
        let es = Arc::new(match &seeded {
            Some(m) => MockEventStore::with_message(m),
            None => MockEventStore::new(),
        });
        let mut repo = MockMessageReadRepo::with_mailboxes(vec![inbox, sent_box.clone(), drafts_box.clone()]);
        repo.messages.extend(seeded.clone());
        let handler = MessageCommandHandler::new(
            es,
            Arc::new(MockProviderRegistry::new(provider.clone())),
            Arc::new(repo),
            bus.clone(),
        );
        Fixture { account, provider, bus, sent_box, drafts_box, seeded, handler }
    }

    #[tokio::test]
    async fn send_compose_sends_appends_to_sent_and_records_it() {
        let f = fixture("imap.example.com", |_, _| None);
        let result = f
            .handler
            .handle_send_compose(std::slice::from_ref(&f.account), compose_input(f.account.id))
            .await
            .unwrap();
        assert!(matches!(result, CommandResult::MessageSent { .. }));
        drain_spawned().await;

        let sent = f.provider.sent.read().unwrap();
        let out = &sent[0];
        assert_eq!(out.from.name.as_deref(), Some("Mario Rossi"));
        assert_eq!(out.to[0].name.as_deref(), Some("Anna"));
        assert_eq!(out.attachments.len(), 1, "attachments must reach SMTP");
        assert!(out.body_html.as_deref().unwrap().contains("<strong>ciao</strong>"));
        assert_eq!(out.body_text.as_deref(), Some("**ciao**\n\n-- \nMario"));

        let appended = f.provider.appended.read().unwrap();
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].0, f.sent_box.path);
        assert_eq!(appended[0].1.message_rfc_id, out.message_rfc_id);

        // The sent event carries the body, or a refetch of the sent message comes back empty.
        let published = f.bus.published.read().unwrap();
        let DomainEvent::MessageSent { body_text, mailbox_id, .. } = &published.last().unwrap().payload
        else {
            panic!("expected MessageSent event");
        };
        assert_eq!(body_text, &out.body_text);
        assert_eq!(*mailbox_id, f.sent_box.id);
    }

    #[tokio::test]
    async fn gmail_sent_copy_is_not_appended() {
        let f = fixture("imap.gmail.com", |_, _| None);
        f.handler
            .handle_send_compose(std::slice::from_ref(&f.account), compose_input(f.account.id))
            .await
            .unwrap();
        drain_spawned().await;
        assert!(f.provider.appended.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn send_compose_rejects_bad_or_missing_recipients() {
        let f = fixture("imap.example.com", |_, _| None);
        let accounts = std::slice::from_ref(&f.account);

        let mut input = compose_input(f.account.id);
        input.cc = vec!["anna@".into()];
        let err = f.handler.handle_send_compose(accounts, input).await.unwrap_err();
        assert!(err.to_string().contains("anna@"), "{err}");

        let mut input = compose_input(f.account.id);
        input.to.clear();
        assert!(f.handler.handle_send_compose(accounts, input).await.is_err());
        assert!(f.provider.sent.read().unwrap().is_empty());
    }

    fn source_message(account_id: AccountId, mailbox_id: MailboxId) -> Message {
        let (mut msg, _) = Message::receive(
            account_id,
            mailbox_id,
            ProviderUid::new(42),
            Some("<orig@example.com>".into()),
            Some("<parent@example.com>".into()),
            vec!["<root@example.com>".into(), "<parent@example.com>".into()],
            Envelope {
                from: vec![Address { name: Some("Anna".into()), email: EmailAddress::new("anna@example.com").unwrap() }],
                to: vec![Address { name: None, email: EmailAddress::new("mario@example.com").unwrap() }],
                cc: vec![Address { name: None, email: EmailAddress::new("bob@example.com").unwrap() }],
                bcc: vec![],
                reply_to: vec![],
                subject: "R: Budget".into(),
                date: chrono::Utc::now(),
            },
            vec![],
            100,
            false,
            chrono::Utc::now(),
        );
        msg.body_text = Some("testo originale".into());
        msg
    }

    #[tokio::test]
    async fn reply_all_is_prepared_and_sent_threaded_with_quote() {
        let f = fixture("imap.example.com", |account, inbox| Some(source_message(account, inbox)));
        let source = f.seeded.clone().unwrap();
        let accounts = std::slice::from_ref(&f.account);

        let CommandResult::ComposePrepared(view) = f
            .handler
            .handle_prepare_compose(accounts, ComposeMode::ReplyAll, Some(source.id), None)
            .await
            .unwrap()
        else {
            panic!("expected ComposePrepared");
        };
        assert_eq!(view.subject, "Re: Budget");
        assert_eq!(view.to, ["Anna <anna@example.com>"]);
        assert_eq!(view.cc, ["bob@example.com"]);
        assert!(view.quoted_html.as_deref().unwrap().contains("testo originale"));

        let input = ComposeInput {
            to: view.to.clone(),
            cc: view.cc.clone(),
            subject: view.subject.clone(),
            mode: view.mode,
            source_message_id: view.source_message_id,
            attachments: vec![],
            ..compose_input(view.account_id)
        };
        f.handler.handle_send_compose(accounts, input).await.unwrap();

        let sent = f.provider.sent.read().unwrap();
        assert_eq!(sent[0].in_reply_to.as_deref(), Some("<orig@example.com>"));
        assert_eq!(sent[0].references, ["<root@example.com>", "<parent@example.com>", "<orig@example.com>"]);
        assert!(sent[0].body_text.as_deref().unwrap().ends_with("ha scritto:\n> testo originale"));

        let published = f.bus.published.read().unwrap();
        assert!(published.iter().any(|e| matches!(
            &e.payload,
            DomainEvent::MessageFlagged { message_id, flags_added, .. }
                if *message_id == source.id && flags_added == &[Flag::Answered]
        )));
    }

    #[tokio::test]
    async fn forward_carries_kept_attachments_and_inline_images() {
        let f = fixture("imap.example.com", |account, inbox| Some(source_message(account, inbox)));
        let source = f.seeded.clone().unwrap();
        *f.provider.raw.write().unwrap() = b"Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: multipart/related; boundary=\"R\"\r\n\r\n\
--R\r\nContent-Type: text/html\r\n\r\n<p>Ecco <img src=\"cid:logo@x\"></p>\r\n\
--R\r\nContent-Type: image/png\r\nContent-ID: <logo@x>\r\nContent-Transfer-Encoding: base64\r\n\r\nAQID\r\n\
--R--\r\n\
--M\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nBAUG\r\n\
--M--\r\n"
            .to_vec();
        let accounts = std::slice::from_ref(&f.account);

        let CommandResult::ComposePrepared(view) = f
            .handler
            .handle_prepare_compose(accounts, ComposeMode::Forward, Some(source.id), None)
            .await
            .unwrap()
        else {
            panic!("expected ComposePrepared");
        };
        assert_eq!(view.subject, "Fwd: Budget");
        assert!(view.to.is_empty());
        let names: Vec<_> = view.forwarded_attachments.iter().map(|a| a.filename.as_str()).collect();
        assert_eq!(names, ["a.pdf"]);

        let input = ComposeInput {
            subject: view.subject.clone(),
            mode: view.mode,
            source_message_id: view.source_message_id,
            attachments: vec![],
            forwarded_attachment_ids: vec![view.forwarded_attachments[0].id.clone()],
            ..compose_input(view.account_id)
        };
        f.handler.handle_send_compose(accounts, input).await.unwrap();

        let sent = f.provider.sent.read().unwrap();
        let parts: Vec<_> = sent[0]
            .attachments
            .iter()
            .map(|a| (a.filename.as_str(), a.content_id.as_deref(), a.data.as_slice()))
            .collect();
        assert_eq!(parts, [("a.pdf", None, &[4u8, 5, 6][..]), ("allegato", Some("logo@x"), &[1, 2, 3][..])]);
        assert!(sent[0].body_html.as_deref().unwrap().contains("Inizio messaggio inoltrato"));
        assert!(sent[0].in_reply_to.is_none());
    }

    #[tokio::test]
    async fn draft_restores_markdown_recipients_and_attachments() {
        let f = fixture("imap.example.com", |account, inbox| Some(source_message(account, inbox)));
        let draft = f.seeded.clone().unwrap();
        let headers: String = compose::draft_headers(ComposeMode::Forward, None, "**ciao**\n![](cid:img@x)")
            .into_iter()
            .map(|(k, v)| format!("{k}: {v}\r\n"))
            .collect();
        *f.provider.raw.write().unwrap() = format!(
            "{headers}To: Anna <anna@example.com>\r\nBcc: b@example.com\r\nSubject: Bozza\r\n\
Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: text/plain\r\n\r\nciao\r\n\
--M\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\n\r\nPDF\r\n\
--M\r\nContent-Type: image/png\r\nContent-ID: <img@x>\r\n\r\nPNG\r\n\
--M--\r\n"
        )
        .into_bytes();

        let CommandResult::ComposePrepared(view) = f
            .handler
            .handle_prepare_compose(std::slice::from_ref(&f.account), ComposeMode::Draft, Some(draft.id), None)
            .await
            .unwrap()
        else {
            panic!("expected ComposePrepared");
        };
        assert_eq!(view.draft_id, Some(draft.id));
        assert_eq!(view.body_md, "**ciao**\n![](cid:img@x)");
        assert_eq!(view.to, ["Anna <anna@example.com>"]);
        assert_eq!(view.bcc, ["b@example.com"]);
        assert_eq!(view.subject, "Bozza");
        // A forward whose original is unknown carries on as a new message.
        assert_eq!(view.mode, ComposeMode::New);
        assert_eq!(view.forwarded_attachments.len(), 1);
        let inline: Vec<_> =
            view.inline_images.iter().map(|i| (i.content_id.as_deref(), i.data.as_slice())).collect();
        assert_eq!(inline, [(Some("img@x"), &b"PNG"[..])]);
    }

    #[tokio::test]
    async fn save_draft_appends_stores_locally_and_replaces_previous() {
        let f = fixture("imap.example.com", |account, inbox| Some(source_message(account, inbox)));
        let old = f.seeded.clone().unwrap();
        let mut input = compose_input(f.account.id);
        input.draft_id = Some(old.id);
        input.to.clear();

        let CommandResult::DraftSaved { message_id } =
            f.handler.handle_save_draft(std::slice::from_ref(&f.account), input).await.unwrap()
        else {
            panic!("expected DraftSaved");
        };
        assert_ne!(message_id, old.id);

        let appended = f.provider.appended.read().unwrap();
        let (path, draft, flags) = &appended[0];
        assert_eq!(*path, f.drafts_box.path);
        assert!(flags.contains(&Flag::Draft));
        assert!(draft.extra_headers.iter().any(|(k, v)| k == "X-Mail-Cat-Mode" && v == "new"));
        assert!(f.provider.sent.read().unwrap().is_empty());

        let events: Vec<_> = f
            .bus
            .published
            .read()
            .unwrap()
            .iter()
            .map(|e| (e.aggregate_id.clone(), e.payload.event_name()))
            .collect();
        assert!(events.contains(&(message_id.to_string(), "MessageReceived")));
        assert!(events.contains(&(message_id.to_string(), "MessageBodyFetched")));
        assert!(events.contains(&(old.id.to_string(), "MessageDeleted")));
    }

    #[tokio::test]
    async fn handle_fetch_body_stores_body_through_body_store() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let provider = MockEmailProvider::new();
        *provider.detail_response.write().unwrap() = Some(Message {
            body_text: Some("fetched text".into()),
            body_html: Some("<p>fetched</p>".into()),
            ..msg.clone()
        });

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)]));
        let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

        handler.handle_fetch_body(msg_id, MailboxPath::inbox()).await.unwrap();

        assert_eq!(
            read_repo.stored_bodies.lock().unwrap().as_slice(),
            &[(msg_id, Some("fetched text".to_string()), Some("<p>fetched</p>".to_string()))]
        );
        assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
    }

    /// Opening an unread message marks it read at the same moment, so the
    /// `MessageFlagged` lands while the body is still being downloaded. The
    /// fetch used to append at the sequence it read before the download and
    /// lose the race — leaving the reading pane blank.
    #[tokio::test]
    async fn handle_fetch_body_survives_a_flag_change_during_the_download() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let es = Arc::new(MockEventStore::with_message(&msg));
        let provider = MockEmailProvider::new();
        *provider.detail_response.write().unwrap() = Some(Message {
            body_text: Some("fetched text".into()),
            ..msg.clone()
        });
        let store = Arc::clone(&es);
        *provider.during_fetch.write().unwrap() = Some(Box::new(move || {
            let mut events = store.events.write().unwrap();
            let list = events.get_mut(&msg_id.to_string()).unwrap();
            let seq = list.len() as u64 + 1;
            list.push(EventEnvelope::new(
                msg_id.to_string(),
                AggregateType::Message,
                seq,
                DomainEvent::MessageFlagged {
                    message_id: msg_id,
                    mailbox_id: MailboxId::new(),
                    flags_added: vec![Flag::Seen],
                    flags_removed: vec![],
                },
            ));
        }));

        let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
        let bus = Arc::new(MockEventBus::new());
        let handler = MessageCommandHandler::new(es, registry, Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])), bus.clone());

        handler.handle_fetch_body(msg_id, MailboxPath::inbox()).await.unwrap();
        assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
    }

    /// A message moved into a new "Archive" mailbox whose UID there is not
    /// known — as after a move on a server without UIDPLUS, or any move
    /// stored before moves carried the UID.
    fn uidless_in_archive() -> (Message, mc_domain::Mailbox, Arc<MockEventStore>) {
        let (msg, _) = Message::receive(
            AccountId::new(),
            MailboxId::new(),
            ProviderUid::new(42),
            Some("<moved@example.com>".into()),
            None,
            vec![],
            test_envelope(),
            vec![],
            2048,
            false,
            chrono::Utc::now(),
        );
        let archive = mc_domain::Mailbox {
            id: MailboxId::new(),
            account_id: msg.account_id,
            path: MailboxPath::parse("Archive", '/').unwrap(),
            display_name: "Archive".into(),
            mailbox_type: MailboxType::Archive,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        };
        let es = Arc::new(MockEventStore::with_message(&msg));
        es.events.write().unwrap().get_mut(&msg.id.to_string()).unwrap().push(EventEnvelope::new(
            msg.id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageMoved {
                message_id: msg.id,
                from_mailbox_id: msg.mailbox_id,
                to_mailbox_id: archive.id,
                provider_uid: None,
            },
        ));
        (msg, archive, es)
    }

    #[tokio::test]
    async fn a_message_without_uid_is_found_on_the_server_before_acting() {
        let (msg, archive, es) = uidless_in_archive();
        let provider = Arc::new(MockEmailProvider::new());
        *provider.found.write().unwrap() = vec![ProviderUid::new(55)];
        *provider.detail_response.write().unwrap() = Some(Message { body_text: Some("text".into()), ..msg.clone() });
        let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive.clone()]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

        handler.handle_fetch_body(msg.id, archive.path.clone()).await.unwrap();

        assert_eq!(*provider.fetched.read().unwrap(), [ProviderUid::new(55)]);
        let (stored, _) = handler.load_message(&msg.id).await.unwrap();
        assert_eq!(stored.provider_uid, Some(ProviderUid::new(55)), "found UID is recorded");
    }

    #[tokio::test]
    async fn a_message_gone_from_the_server_is_a_clear_error() {
        let (msg, archive, es) = uidless_in_archive();
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive.clone()]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

        let err = handler
            .handle_set_flags(msg.id, archive.path, vec![Flag::Seen], vec![])
            .await
            .unwrap_err();
        assert!(matches!(err, ApplicationError::NotFound(..)), "{err:?}");
    }

    #[tokio::test]
    async fn marking_a_mailbox_read_reaches_messages_without_uid() {
        let (msg, archive, es) = uidless_in_archive();
        let provider = Arc::new(MockEmailProvider::new());
        *provider.found.write().unwrap() = vec![ProviderUid::new(55)];
        let unread = mc_domain::MessageSummary {
            id: msg.id,
            account_id: msg.account_id,
            mailbox_id: archive.id,
            provider_uid: None,
            from_name: None,
            from_email: "sender@example.com".into(),
            to: vec![],
            cc: vec![],
            subject: "Test".into(),
            flags: vec![],
            size_bytes: 0,
            has_attachments: false,
            received_at: chrono::Utc::now(),
        };
        let read_repo = Arc::new(MockMessageReadRepo { unread: vec![unread], ..MockMessageReadRepo::with_mailboxes(vec![archive.clone()]) });
        let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
        let handler = MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

        handler.handle_mark_mailbox_read(archive.id, archive.path).await.unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        assert_eq!(*provider.flagged_bulk.read().unwrap(), [ProviderUid::new(55)]);
    }

    /// Two copies of one Message-ID in the mailbox: the one another row
    /// already holds is not this message's.
    #[tokio::test]
    async fn the_uid_found_skips_copies_other_rows_hold() {
        let (msg, archive, es) = uidless_in_archive();
        let provider = Arc::new(MockEmailProvider::new());
        *provider.found.write().unwrap() = vec![ProviderUid::new(55), ProviderUid::new(60)];
        *provider.detail_response.write().unwrap() = Some(msg.clone());
        let read_repo = Arc::new(MockMessageReadRepo { held: vec![ProviderUid::new(60)], ..MockMessageReadRepo::with_mailboxes(vec![archive.clone()]) });
        let handler = MessageCommandHandler::new(es, Arc::new(MockProviderRegistry::new(provider.clone())), read_repo, Arc::new(MockEventBus::new()));

        handler.handle_fetch_body(msg.id, archive.path.clone()).await.unwrap();

        assert_eq!(*provider.fetched.read().unwrap(), [ProviderUid::new(55)]);
    }

    #[tokio::test]
    async fn a_uid_is_never_looked_up_in_another_mailbox() {
        let (msg, archive, es) = uidless_in_archive();
        let provider = Arc::new(MockEmailProvider::new());
        *provider.found.write().unwrap() = vec![ProviderUid::new(55)];
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive]));
        let handler = MessageCommandHandler::new(es, Arc::new(MockProviderRegistry::new(provider)), read_repo, Arc::new(MockEventBus::new()));

        let err = handler.handle_fetch_body(msg.id, MailboxPath::inbox()).await.unwrap_err();
        assert!(matches!(err, ApplicationError::PreconditionFailed(_)), "{err:?}");
    }

    #[tokio::test]
    async fn bulk_move_finds_messages_without_uid() {
        let (msg, archive, es) = uidless_in_archive();
        let provider = Arc::new(MockEmailProvider::new());
        *provider.found.write().unwrap() = vec![ProviderUid::new(55)];
        let registry = Arc::new(MockProviderRegistry::new(provider.clone()));
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![archive]));
        let handler = MessageCommandHandler::new(es, registry, read_repo, Arc::new(MockEventBus::new()));

        let result = handler
            .handle_move_many(vec![msg.id], MailboxId::new(), MailboxPath::parse("Trash", '/').unwrap())
            .await
            .unwrap();

        assert!(matches!(result, CommandResult::MessagesBulkChanged { succeeded: 1, failed: 0 }));
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(*provider.moved.read().unwrap(), [ProviderUid::new(55)]);
    }

    #[tokio::test]
    async fn handle_move_records_the_uid_the_provider_reports() {
        let msg = make_test_message();
        let msg_id = msg.id;
        let archive = MailboxId::new();
        let provider = MockEmailProvider::new();
        *provider.moved_uid.write().unwrap() = Some(ProviderUid::new(900));

        let es = Arc::new(MockEventStore::with_message(&msg));
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(provider)));
        let handler = MessageCommandHandler::new(
            es.clone(),
            registry,
            Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
            Arc::new(MockEventBus::new()),
        );

        handler
            .handle_move(msg_id, msg.mailbox_id, archive, MailboxPath::inbox(), MailboxPath::parse("Archive", '/').unwrap())
            .await
            .unwrap();

        // The server moved it before anything changed here; the inbox UID
        // would address some other message in Archive.
        let (moved, _) = handler.load_message(&msg_id).await.unwrap();
        assert_eq!(moved.mailbox_id, archive);
        assert_eq!(moved.provider_uid, Some(ProviderUid::new(900)));
    }

    /// A move the server refuses changes nothing locally.
    #[tokio::test]
    async fn a_refused_move_changes_nothing() {
        let msg = make_test_message();
        let provider = MockEmailProvider::new();
        *provider.refuse.write().unwrap() = true;
        let es = Arc::new(MockEventStore::with_message(&msg));
        let handler = MessageCommandHandler::new(
            es,
            Arc::new(MockProviderRegistry::new(Arc::new(provider))),
            Arc::new(MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])),
            Arc::new(MockEventBus::new()),
        );

        let result = handler
            .handle_move(msg.id, msg.mailbox_id, MailboxId::new(), MailboxPath::inbox(), MailboxPath::parse("Archive", '/').unwrap())
            .await;

        assert!(result.is_err());
        let (stored, _) = handler.load_message(&msg.id).await.unwrap();
        assert_eq!((stored.mailbox_id, stored.provider_uid), (msg.mailbox_id, msg.provider_uid));
    }

    struct Moving {
        handler: MessageCommandHandler<MockEventStore, MockProviderRegistry, MockMessageReadRepo, MockEventBus>,
        provider: Arc<MockEmailProvider>,
        es: Arc<MockEventStore>,
        archive: mc_domain::Mailbox,
        trash: mc_domain::Mailbox,
        resync: tokio::sync::mpsc::UnboundedReceiver<(AccountId, MailboxPath)>,
    }

    impl Moving {
        fn resync_requests(&mut self) -> Vec<MailboxPath> {
            std::iter::from_fn(|| self.resync.try_recv().ok().map(|(_, path)| path)).collect()
        }

        async fn location(&self, id: MessageId) -> (MailboxId, Option<ProviderUid>) {
            let (stored, _) = self.handler.load_message(&id).await.unwrap();
            (stored.mailbox_id, stored.provider_uid)
        }
    }

    /// INBOX, Archive and Trash of `msg`'s account, and a handler over them
    /// that reports resync requests.
    fn moving(msg: &Message, provider: MockEmailProvider) -> Moving {
        moving_with(msg, provider, MockMessageReadRepo::default())
    }

    fn moving_with(msg: &Message, provider: MockEmailProvider, repo: MockMessageReadRepo) -> Moving {
        let archive = test_mailbox(msg.account_id, "Archive", MailboxType::Archive);
        let trash = test_mailbox(msg.account_id, "Trash", MailboxType::Trash);
        let provider = Arc::new(provider);
        let es = Arc::new(MockEventStore::with_message(msg));
        let (tx, resync) = tokio::sync::mpsc::unbounded_channel();
        let repo = MockMessageReadRepo { mailboxes: vec![inbox_of(msg), archive.clone(), trash.clone()], ..repo };
        let handler = MessageCommandHandler::new(
            es.clone(),
            Arc::new(MockProviderRegistry::new(provider.clone())),
            Arc::new(repo),
            Arc::new(MockEventBus::new()),
        )
        .with_resync_requests(tx);
        Moving { handler, provider, es, archive, trash, resync }
    }

    /// Archive, then delete before the list caught up: the delete waits its
    /// turn and acts on the message where the archive put it, with the UID
    /// it has there — never the INBOX UID again.
    #[tokio::test]
    async fn a_delete_queued_behind_a_move_follows_it_to_trash() {
        let msg = make_test_message();
        let m = moving(&msg, MockEmailProvider::moving_to(900));
        let archive = m.archive.clone();

        m.handler.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive.path.clone()).await.unwrap();
        let result = m.handler.handle_delete(msg.id, msg.mailbox_id, MailboxPath::inbox(), false).await.unwrap();

        assert!(matches!(result, CommandResult::MessageDeleted));
        assert_eq!(*m.provider.moved.read().unwrap(), [ProviderUid::new(42), ProviderUid::new(900)]);
        assert_eq!(m.location(msg.id).await, (m.trash.id, Some(ProviderUid::new(900))));

        // Moving it where it already is changes nothing.
        m.handler.handle_move(msg.id, msg.mailbox_id, m.trash.id, MailboxPath::inbox(), m.trash.path.clone()).await.unwrap();
        assert_eq!(m.provider.moved.read().unwrap().len(), 2);
    }

    fn with_rfc_id(rfc_id: &str) -> Message {
        Message::receive(
            AccountId::new(),
            MailboxId::new(),
            ProviderUid::new(42),
            Some(rfc_id.into()),
            None,
            vec![],
            test_envelope(),
            vec![],
            0,
            false,
            chrono::Utc::now(),
        )
        .0
    }

    /// The server said OK without COPYUID and the destination has no copy
    /// to find: nothing may claim the message sits there. The row goes —
    /// the real copy syncs as its own — and both mailboxes resync.
    #[tokio::test]
    async fn a_move_confirmed_without_evidence_records_no_destination() {
        let msg = with_rfc_id("<ghost@example.com>");
        let mut m = moving(&msg, MockEmailProvider::new());
        let archive = m.archive.clone();

        m.handler.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive.path.clone()).await.unwrap();

        assert!(matches!(m.handler.load_message(&msg.id).await, Err(ApplicationError::NotFound(..))));
        assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
    }

    /// A phone moved it to Archive; the Archive sync already stored that copy
    /// as a row of its own while this one kept its stale INBOX UID. Archiving
    /// it here: the server has nothing at that UID (OK, no COPYUID) and the
    /// Archive copy is the other row's. This row goes rather than lingering
    /// UID-less in INBOX, and nothing is duplicated.
    #[tokio::test]
    async fn archiving_a_message_another_client_already_moved_leaves_one_row() {
        let msg = with_rfc_id("<phone@example.com>");
        let provider = MockEmailProvider::new();
        *provider.found.write().unwrap() = vec![ProviderUid::new(900)];
        let mut m = moving_with(&msg, provider, MockMessageReadRepo { held: vec![ProviderUid::new(900)], ..Default::default() });
        let archive = m.archive.clone();

        m.handler.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive.path.clone()).await.unwrap();

        assert!(matches!(m.handler.load_message(&msg.id).await, Err(ApplicationError::NotFound(..))), "no UID-less row left");
        let created = m.es.events.read().unwrap().len();
        assert_eq!(created, 1, "no new row");
        assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
    }

    /// Gmail: archiving to All Mail, whose COPYUID names the UID the synced
    /// All Mail row already holds. That row is the message there; this one
    /// goes instead of becoming a second row of the same server message.
    #[tokio::test]
    async fn a_move_onto_a_uid_another_row_holds_leaves_that_row_alone() {
        let msg = make_test_message();
        let m = moving_with(&msg, MockEmailProvider::moving_to(900), MockMessageReadRepo { held: vec![ProviderUid::new(900)], ..Default::default() });
        let archive = m.archive.clone();

        m.handler.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive.path.clone()).await.unwrap();

        assert!(matches!(m.handler.load_message(&msg.id).await, Err(ApplicationError::NotFound(..))), "no second row in Archive");
        assert_eq!(m.es.events.read().unwrap().len(), 1, "and no new one");
    }

    #[tokio::test]
    async fn the_same_delete_sent_twice_is_done_once() {
        let msg = make_test_message();
        let m = moving(&msg, MockEmailProvider::new());

        m.handler.handle_delete(msg.id, msg.mailbox_id, MailboxPath::inbox(), true).await.unwrap();
        let again = m.handler.handle_delete(msg.id, msg.mailbox_id, MailboxPath::inbox(), true).await;

        assert!(matches!(again, Ok(CommandResult::MessageDeleted)), "{again:?}");
    }

    /// The connection dropped after the MOVE went out: it may have been
    /// applied. The user sees the error, and both mailboxes resync.
    #[tokio::test]
    async fn a_timeout_after_a_move_resyncs_both_mailboxes() {
        let msg = make_test_message();
        let provider = MockEmailProvider::moving_to(900);
        *provider.time_out.write().unwrap() = true;
        let mut m = moving(&msg, provider);
        let archive = m.archive.clone();

        let result =
            m.handler.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive.path.clone()).await;

        assert!(result.is_err());
        assert_eq!(m.location(msg.id).await, (msg.mailbox_id, Some(ProviderUid::new(42))));
        assert_eq!(m.resync_requests(), [MailboxPath::inbox(), archive.path]);
    }

    /// The server moved it; recording that loses to a sync appending first.
    /// It is reloaded and recorded, not reported as a failure the server
    /// never had.
    #[tokio::test]
    async fn recording_a_done_move_retries_past_a_concurrent_append() {
        let msg = make_test_message();
        let mut m = moving(&msg, MockEmailProvider::moving_to(900));
        let archive = m.archive.clone();
        *m.es.conflicts.lock().unwrap() = 1;

        m.handler.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive.path.clone()).await.unwrap();

        let (stored, _) = m.handler.load_message(&msg.id).await.unwrap();
        assert_eq!((stored.mailbox_id, stored.provider_uid), (archive.id, Some(ProviderUid::new(900))));
        assert!(stored.flags.contains(&Flag::Flagged), "the other writer's change is kept");
        assert!(m.resync_requests().is_empty());

        // Losing every time is still no error to the user: a resync repairs it.
        let msg = make_test_message();
        let mut m = moving(&msg, MockEmailProvider::moving_to(900));
        let archive = m.archive.clone();
        *m.es.conflicts.lock().unwrap() = RECORD_ATTEMPTS;
        m.handler.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive.path.clone()).await.unwrap();
        assert!(!m.resync_requests().is_empty());
    }

    /// The server applied the first chunk and refused the second: the first
    /// is recorded, only the rest counts as failed.
    #[tokio::test]
    async fn a_refused_chunk_keeps_the_chunks_before_it() {
        let first = make_test_message();
        let provider = MockEmailProvider::moving_to(900);
        *provider.refuse_after.write().unwrap() = Some(SERVER_CHUNK);
        let Moving { handler, es, archive, .. } = moving(&first, provider);
        let mut ids = vec![first.id];
        for _ in 1..=SERVER_CHUNK {
            let other = Message { id: MessageId::new(), ..first.clone() };
            es.add(&other);
            ids.push(other.id);
        }

        let result = handler.handle_move_many(ids.clone(), archive.id, archive.path.clone()).await.unwrap();

        assert!(
            matches!(result, CommandResult::MessagesBulkChanged { succeeded: 500, failed: 1 }),
            "{result:?}"
        );
        let mut in_archive = 0;
        for id in &ids {
            if handler.load_message(id).await.unwrap().0.mailbox_id == archive.id {
                in_archive += 1;
            }
        }
        assert_eq!(in_archive, SERVER_CHUNK);
    }

    /// A message that can't be marked must not stop the others, here or on
    /// the server.
    #[tokio::test]
    async fn marking_a_mailbox_read_continues_past_a_failing_message() {
        let msg = make_test_message();
        let summary = |id| mc_domain::MessageSummary {
            id,
            account_id: msg.account_id,
            mailbox_id: msg.mailbox_id,
            provider_uid: None,
            from_name: None,
            from_email: "sender@example.com".into(),
            to: vec![],
            cc: vec![],
            subject: "Test".into(),
            flags: vec![],
            size_bytes: 0,
            has_attachments: false,
            received_at: chrono::Utc::now(),
        };
        let provider = Arc::new(MockEmailProvider::new());
        let read_repo = MockMessageReadRepo {
            unread: vec![summary(MessageId::new()), summary(msg.id)],
            ..MockMessageReadRepo::with_mailboxes(vec![inbox_of(&msg)])
        };
        let handler = MessageCommandHandler::new(
            Arc::new(MockEventStore::with_message(&msg)),
            Arc::new(MockProviderRegistry::new(provider.clone())),
            Arc::new(read_repo),
            Arc::new(MockEventBus::new()),
        );

        let result = handler.handle_mark_mailbox_read(msg.mailbox_id, MailboxPath::inbox()).await.unwrap();
        drain_spawned().await;

        assert!(matches!(result, CommandResult::MailboxMarkedRead { count: 1 }), "{result:?}");
        assert!(handler.load_message(&msg.id).await.unwrap().0.is_read());
        assert_eq!(*provider.flagged_bulk.read().unwrap(), [ProviderUid::new(42)]);
    }

    /// Open (mark read), then archive at once: the flag store waits for the
    /// move and goes to the message where it now is.
    #[tokio::test]
    async fn a_flag_store_follows_a_move_queued_ahead_of_it() {
        let msg = make_test_message();
        let Moving { handler, provider, archive, .. } = moving(&msg, MockEmailProvider::moving_to(900));
        let turn = handler.turn(msg.account_id).await;

        let mover = handler.clone();
        let archive_path = archive.path.clone();
        let moved = tokio::spawn(async move {
            mover.handle_move(msg.id, msg.mailbox_id, archive.id, MailboxPath::inbox(), archive_path).await
        });
        drain_spawned().await;
        handler.handle_set_flags(msg.id, MailboxPath::inbox(), vec![Flag::Seen], vec![]).await.unwrap();
        drop(turn);
        moved.await.unwrap().unwrap();
        drain_spawned().await;

        assert_eq!(*provider.flagged_bulk.read().unwrap(), [ProviderUid::new(900)]);
        assert_eq!(*provider.flagged_in.read().unwrap(), [archive.path]);
    }
}
