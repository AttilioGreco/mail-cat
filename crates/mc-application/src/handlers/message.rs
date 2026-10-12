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
    Account, AccountId, AggregateType, Attachment, AttachmentId, DomainEvent, EmailProvider,
    Envelope, EventEnvelope, EventStore, Flag, MailboxId, MailboxPath, MailboxReadRepository,
    MailboxType, Message, MessageId, MessageBodyStore, MessageReadRepository, OutgoingMessage,
    ProviderRegistry, ReadModelProjector, value_objects::uid::ProviderUid,
};
use tracing::info;

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
}

impl<ES: EventStore, PR: ProviderRegistry, MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore, B: EventPublisher>
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
        }
    }

    pub async fn handle_fetch_body(
        &self,
        message_id: MessageId,
        mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Fetch full message from the remote provider.
        let detail = provider
            .fetch_message_detail(&mailbox_path, provider_uid)
            .await?;

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
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Optimistic update: persist locally first for instant UI feedback,
        // then sync to IMAP in background. Next sync reconciles if IMAP fails.
        let event = message.change_flags(flags_to_add.clone(), flags_to_remove.clone());

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message flags updated locally, syncing to IMAP in background");

        // Fire-and-forget IMAP update
        tokio::spawn(async move {
            if let Err(e) = provider
                .set_flags(&mailbox_path, provider_uid, &flags_to_add, &flags_to_remove)
                .await
            {
                tracing::warn!(%e, "Background IMAP set_flags failed — next sync will reconcile");
            }
        });

        Ok(CommandResult::FlagsSet)
    }

    /// Mark every unread message in a mailbox as read, in a single provider
    /// round-trip (one `set_flags_bulk` call instead of one `set_flags` per
    /// message).
    pub async fn handle_mark_mailbox_read(
        &self,
        mailbox_id: MailboxId,
        mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let unread = self
            .message_read_repo
            .find_unread_by_mailbox(&mailbox_id)
            .await?;

        if unread.is_empty() {
            return Ok(CommandResult::MailboxMarkedRead { count: 0 });
        }

        let mut provider_uids = Vec::new();
        let account_id = unread[0].account_id.clone();

        for summary in &unread {
            let (mut message, seq) = self.load_message(&summary.id).await?;
            let event = message.change_flags(vec![Flag::Seen], vec![]);
            self.append_and_publish(&summary.id, AggregateType::Message, seq, event)
                .await?;

            if let Some(provider_uid) = summary.provider_uid {
                provider_uids.push(provider_uid);
            }
        }

        info!(%mailbox_id, count = unread.len(), "mailbox marked read locally, syncing to IMAP in background");

        if !provider_uids.is_empty() {
            let provider = self.provider_registry.get_provider(&account_id)?;

            // Fire-and-forget IMAP update, single round-trip for the whole batch.
            tokio::spawn(async move {
                if let Err(e) = provider
                    .set_flags_bulk(&mailbox_path, &provider_uids, &[Flag::Seen], &[])
                    .await
                {
                    tracing::warn!(%e, "Background IMAP set_flags_bulk failed — next sync will reconcile");
                }
            });
        }

        Ok(CommandResult::MailboxMarkedRead {
            count: unread.len() as u32,
        })
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

    pub async fn handle_move(
        &self,
        message_id: MessageId,
        _from_mailbox_id: MailboxId,
        to_mailbox_id: MailboxId,
        from_mailbox_path: MailboxPath,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Optimistic: update locally first, sync to IMAP in background.
        let event = message.move_to(to_mailbox_id);

        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message moved locally, syncing to IMAP in background");

        tokio::spawn(async move {
            if let Err(e) = provider
                .move_message(provider_uid, &from_mailbox_path, &to_mailbox_path)
                .await
            {
                tracing::warn!(%e, "Background IMAP move failed — next sync will reconcile");
            }
        });

        Ok(CommandResult::MessageMoved)
    }

    pub async fn handle_delete(
        &self,
        message_id: MessageId,
        _mailbox_id: MailboxId,
        mailbox_path: MailboxPath,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        let (mut message, seq) = self.load_message(&message_id).await?;

        let provider_uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("message has no provider UID".into())
        })?;

        let provider = self.provider_registry.get_provider(&message.account_id)?;

        // Standard client behaviour: a plain delete moves the message to the
        // Trash folder (Thunderbird-style). Only a permanent delete, a delete of
        // a message already in Trash, or the absence of a Trash folder actually
        // expunges it.
        let mailboxes = self.message_read_repo.list_mailboxes(&message.account_id).await?;
        let trash = mailboxes.iter().find(|m| m.mailbox_type == MailboxType::Trash);
        let source_is_trash = mailboxes
            .iter()
            .find(|m| m.id == message.mailbox_id)
            .map(|m| m.mailbox_type == MailboxType::Trash)
            .unwrap_or(false);

        if !permanently
            && !source_is_trash
            && let Some(trash) = trash
        {
            let to_mailbox_id = trash.id;
            let to_path = trash.path.clone();
            let from_path = mailbox_path.clone();

            // Optimistic: move locally first, sync to IMAP in background.
            let event = message.move_to(to_mailbox_id);
            self.append_and_publish(&message_id, AggregateType::Message, seq, event)
                .await?;

            info!(%message_id, "message moved to Trash, syncing to IMAP in background");

            tokio::spawn(async move {
                if let Err(e) = provider
                    .move_message(provider_uid, &from_path, &to_path)
                    .await
                {
                    tracing::warn!(%e, "Background IMAP move-to-Trash failed — next sync will reconcile");
                }
            });

            return Ok(CommandResult::MessageDeleted);
        }

        // Permanent delete (or already in Trash / no Trash folder): expunge on the
        // server and drop from the local read model.
        let event = message.delete(true);
        self.append_and_publish(&message_id, AggregateType::Message, seq, event)
            .await?;

        info!(%message_id, "message permanently deleted, syncing to IMAP in background");

        tokio::spawn(async move {
            if let Err(e) = provider
                .delete_message(&mailbox_path, provider_uid, true)
                .await
            {
                tracing::warn!(%e, "Background IMAP delete failed — next sync will reconcile");
            }
        });

        Ok(CommandResult::MessageDeleted)
    }

    /// Bulk delete. Applies the same Trash-vs-expunge rules as [`Self::handle_delete`]
    /// per message, then groups the survivors by source mailbox and fires **one**
    /// background provider call per group.
    ///
    /// The grouping is the whole point: the per-message path spawns a task each,
    /// so deleting a search result set of 800 would open 800 concurrent IMAP
    /// connections. One `UID STORE` over a compressed UID set is what IMAP is for.
    ///
    /// A message that can't be loaded, has no provider UID, or whose mailbox has
    /// vanished is counted in `failed` and skipped — a stale search index must not
    /// abort the rest of the batch halfway through.
    pub async fn handle_delete_many(
        &self,
        message_ids: Vec<MessageId>,
        permanently: bool,
    ) -> Result<CommandResult, ApplicationError> {
        let mut failed: u32 = 0;
        let mut succeeded: u32 = 0;
        // (account, source path, Some(trash dest) | None = expunge) -> uids
        let mut groups: HashMap<(AccountId, MailboxPath, Option<MailboxPath>), Vec<ProviderUid>> =
            HashMap::new();
        // list_mailboxes hits the read model — once per account, not once per message.
        let mut mailbox_cache: HashMap<AccountId, Vec<mc_domain::Mailbox>> = HashMap::new();

        for message_id in message_ids {
            let Ok((mut message, seq)) = self.load_message(&message_id).await else {
                failed += 1;
                continue;
            };
            let Some(provider_uid) = message.provider_uid else {
                failed += 1;
                continue;
            };

            let account_id = message.account_id;
            if !mailbox_cache.contains_key(&account_id) {
                let boxes = self.message_read_repo.list_mailboxes(&account_id).await?;
                mailbox_cache.insert(account_id, boxes);
            }
            let mailboxes = &mailbox_cache[&account_id];

            let Some(source) = mailboxes.iter().find(|m| m.id == message.mailbox_id) else {
                failed += 1;
                continue;
            };
            let source_path = source.path.clone();
            let source_is_trash = source.mailbox_type == MailboxType::Trash;
            let trash = mailboxes.iter().find(|m| m.mailbox_type == MailboxType::Trash);

            let (event, dest) = if !permanently
                && !source_is_trash
                && let Some(trash) = trash
            {
                (message.move_to(trash.id), Some(trash.path.clone()))
            } else {
                (message.delete(true), None)
            };

            if self
                .append_and_publish(&message_id, AggregateType::Message, seq, event)
                .await
                .is_err()
            {
                failed += 1;
                continue;
            }
            succeeded += 1;

            groups
                .entry((account_id, source_path, dest))
                .or_default()
                .push(provider_uid);
        }

        info!(succeeded, failed, "bulk delete applied locally, syncing to IMAP in background");

        for ((account_id, from_path, dest), uids) in groups {
            let provider = match self.provider_registry.get_provider(&account_id) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%e, "Bulk delete: no provider for account — next sync will reconcile");
                    continue;
                }
            };
            tokio::spawn(async move {
                let result = match &dest {
                    Some(to) => provider.move_messages(&uids, &from_path, to).await,
                    None => provider.delete_messages(&from_path, &uids, true).await,
                };
                if let Err(e) = result {
                    tracing::warn!(%e, "Background IMAP bulk delete failed — next sync will reconcile");
                }
            });
        }

        Ok(CommandResult::MessagesBulkChanged { succeeded, failed })
    }

    /// Bulk move to a single destination. Same grouping rationale as
    /// [`Self::handle_delete_many`]: one provider call per source mailbox.
    pub async fn handle_move_many(
        &self,
        message_ids: Vec<MessageId>,
        to_mailbox_id: MailboxId,
        to_mailbox_path: MailboxPath,
    ) -> Result<CommandResult, ApplicationError> {
        let mut failed: u32 = 0;
        let mut succeeded: u32 = 0;
        let mut groups: HashMap<(AccountId, MailboxPath), Vec<ProviderUid>> = HashMap::new();
        let mut mailbox_cache: HashMap<AccountId, Vec<mc_domain::Mailbox>> = HashMap::new();

        for message_id in message_ids {
            let Ok((mut message, seq)) = self.load_message(&message_id).await else {
                failed += 1;
                continue;
            };
            let Some(provider_uid) = message.provider_uid else {
                failed += 1;
                continue;
            };
            // Moving a message to the folder it already sits in is a no-op, not
            // a copy — skip it before it reaches IMAP.
            if message.mailbox_id == to_mailbox_id {
                continue;
            }

            let account_id = message.account_id;
            if !mailbox_cache.contains_key(&account_id) {
                let boxes = self.message_read_repo.list_mailboxes(&account_id).await?;
                mailbox_cache.insert(account_id, boxes);
            }
            let Some(source) = mailbox_cache[&account_id]
                .iter()
                .find(|m| m.id == message.mailbox_id)
            else {
                failed += 1;
                continue;
            };
            let source_path = source.path.clone();

            let event = message.move_to(to_mailbox_id);
            if self
                .append_and_publish(&message_id, AggregateType::Message, seq, event)
                .await
                .is_err()
            {
                failed += 1;
                continue;
            }
            succeeded += 1;

            groups
                .entry((account_id, source_path))
                .or_default()
                .push(provider_uid);
        }

        info!(succeeded, failed, "bulk move applied locally, syncing to IMAP in background");

        for ((account_id, from_path), uids) in groups {
            let provider = match self.provider_registry.get_provider(&account_id) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%e, "Bulk move: no provider for account — next sync will reconcile");
                    continue;
                }
            };
            let to = to_mailbox_path.clone();
            tokio::spawn(async move {
                if let Err(e) = provider.move_messages(&uids, &from_path, &to).await {
                    tracing::warn!(%e, "Background IMAP bulk move failed — next sync will reconcile");
                }
            });
        }

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

        // Shown in Sent right away instead of after the next sync. Falls back
        // to a placeholder id before the first sync has discovered Sent.
        let message_id = MessageId::new();
        let event = DomainEvent::MessageSent {
            message_id,
            account_id: account.id,
            mailbox_id: sent.map_or_else(MailboxId::new, |m| m.id),
            envelope: outgoing_envelope(&outgoing),
            in_reply_to: outgoing.in_reply_to.clone(),
            references: outgoing.references.clone(),
            body_text: outgoing.body_text.clone(),
            body_html: outgoing.body_html.clone(),
            sent_at: chrono::Utc::now(),
        };
        self.append_and_publish(&message_id, AggregateType::Message, 0, event)
            .await?;

        if let Some(draft_id) = input.draft_id
            && let Err(e) = self.remove_draft(draft_id).await
        {
            tracing::warn!(%e, %draft_id, "message sent, but deleting its draft failed");
        }

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
        let uid = message.provider_uid.ok_or_else(|| {
            ApplicationError::PreconditionFailed("il messaggio non è ancora sul server".into())
        })?;
        let mailbox = self
            .message_read_repo
            .get_mailbox(&message.mailbox_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Mailbox", message.mailbox_id.to_string()))?;
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
        let (message, seq) = self.load_message(&id).await?;
        let mailbox = self.message_read_repo.get_mailbox(&message.mailbox_id).await?;
        self.append_and_publish(&id, AggregateType::Message, seq, message.delete(true))
            .await?;

        if let (Some(uid), Some(mailbox)) = (message.provider_uid, mailbox) {
            let provider = self.provider_registry.get_provider(&message.account_id)?;
            tokio::spawn(async move {
                if let Err(e) = provider.delete_message(&mailbox.path, uid, true).await {
                    tracing::warn!(%e, "Background IMAP draft delete failed — next sync will reconcile");
                }
            });
        }
        Ok(())
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /// Load a message aggregate from the event store.
    /// Returns the rehydrated Message and the current event count (for OCC).
    async fn load_message(&self, id: &MessageId) -> Result<(Message, u64), ApplicationError> {
        let events = self.event_store.load_events(&id.to_string(), 0).await?;
        let event_count = events.len() as u64;

        let domain_events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();

        let message = Message::rehydrate(&domain_events)
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
    }

    impl MockEventStore {
        fn new() -> Self {
            Self {
                events: RwLock::new(HashMap::new()),
            }
        }

        fn with_message(message: &Message) -> Self {
            let store = Self::new();
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
            store
                .events
                .write()
                .unwrap()
                .insert(message.id.to_string(), vec![env]);
            store
        }
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            aggregate_id: &str,
            _aggregate_type: AggregateType,
            _expected_sequence: u64,
            events: Vec<DomainEvent>,
        ) -> Result<u64, DomainError> {
            let mut store = self.events.write().unwrap();
            let list = store.entry(aggregate_id.to_string()).or_default();
            let mut seq = list.len() as u64;
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
    }

    impl MockEmailProvider {
        fn new() -> Self {
            Self::default()
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
            _uid: ProviderUid,
        ) -> Result<Message, DomainError> {
            self.detail_response
                .read()
                .unwrap()
                .clone()
                .ok_or_else(|| DomainError::Provider("no detail configured".into()))
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
            _mailbox: &MailboxPath,
            _uids: &[ProviderUid],
            _add: &[Flag],
            _remove: &[Flag],
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn move_message(
            &self,
            _uid: ProviderUid,
            _from: &MailboxPath,
            _to: &MailboxPath,
        ) -> Result<Option<ProviderUid>, DomainError> {
            Ok(None)
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

        async fn find_unread_by_mailbox(
            &self,
            _mailbox_id: &MailboxId,
        ) -> Result<Vec<mc_domain::MessageSummary>, DomainError> {
            Ok(vec![])
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
        let read_repo = Arc::new(MockMessageReadRepo::new());
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
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
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
            Arc::new(MockEmailProvider::new()),
        ));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::new());
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
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
        let bus = Arc::new(MockEventBus::new());
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![trash]));
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
        let registry = Arc::new(MockProviderRegistry::new(Arc::new(MockEmailProvider::new())));
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
        let read_repo = Arc::new(MockMessageReadRepo::with_mailboxes(vec![trash]));
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
        let headers: String = compose::draft_headers(ComposeMode::Forward, None, "**ciao**\nriga")
            .into_iter()
            .map(|(k, v)| format!("{k}: {v}\r\n"))
            .collect();
        *f.provider.raw.write().unwrap() = format!(
            "{headers}To: Anna <anna@example.com>\r\nBcc: b@example.com\r\nSubject: Bozza\r\n\
Content-Type: multipart/mixed; boundary=\"M\"\r\n\r\n\
--M\r\nContent-Type: text/plain\r\n\r\nciao\r\n\
--M\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\n\r\nPDF\r\n\
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
        assert_eq!(view.body_md, "**ciao**\nriga");
        assert_eq!(view.to, ["Anna <anna@example.com>"]);
        assert_eq!(view.bcc, ["b@example.com"]);
        assert_eq!(view.subject, "Bozza");
        // A forward whose original is unknown carries on as a new message.
        assert_eq!(view.mode, ComposeMode::New);
        assert_eq!(view.forwarded_attachments.len(), 1);
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
        let read_repo = Arc::new(MockMessageReadRepo::new());
        let handler = MessageCommandHandler::new(es, registry, read_repo.clone(), bus.clone());

        handler.handle_fetch_body(msg_id, MailboxPath::inbox()).await.unwrap();

        assert_eq!(
            read_repo.stored_bodies.lock().unwrap().as_slice(),
            &[(msg_id, Some("fetched text".to_string()), Some("<p>fetched</p>".to_string()))]
        );
        assert_eq!(bus.last_event_name().unwrap(), "MessageBodyFetched");
    }
}
