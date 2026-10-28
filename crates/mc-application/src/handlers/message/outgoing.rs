//! Composing, sending and drafts.

use super::*;

impl<
    ES: EventStore + 'static,
    PR: ProviderRegistry + 'static,
    MR: MessageReadRepository + ReadModelProjector + MailboxReadRepository + MessageBodyStore + 'static,
    B: EventPublisher + 'static,
> MessageCommandHandler<ES, PR, MR, B>
{
    pub async fn handle_prepare_compose(
        &self,
        accounts: &[Account],
        mode: ComposeMode,
        message_id: Option<MessageId>,
        account_id: Option<AccountId>,
    ) -> Result<CommandResult, ApplicationError> {
        let first = accounts.first().ok_or_else(|| {
            ApplicationError::PreconditionFailed("Nessun account configurato".into())
        })?;
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
                ApplicationError::PreconditionFailed(format!(
                    "{}: manca il messaggio di partenza",
                    mode.as_str()
                ))
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

    async fn restore_draft(
        &self,
        view: &mut ComposeView,
        id: MessageId,
    ) -> Result<(), ApplicationError> {
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
        view.inline_images = parsed
            .parts
            .into_iter()
            .filter(|p| compose::referenced_inline(p, &shown_html))
            .collect();
        Ok(())
    }

    pub async fn handle_send_compose(
        &self,
        accounts: &[Account],
        input: ComposeInput,
    ) -> Result<CommandResult, ApplicationError> {
        let (account, source, outgoing) = self.build_outgoing(accounts, &input, false).await?;
        let provider = self.provider_registry.get_provider(&account.id)?;
        let sent = self
            .message_read_repo
            .list_mailboxes(&account.id)
            .await?
            .into_iter()
            .find(|m| m.mailbox_type == MailboxType::Sent);

        // Shown in Sent right away instead of after the next sync. Falls back
        // to a placeholder id before the first sync has discovered Sent.
        // Recorded before the provider is called: any Sent sync that sees the
        // server copy while the send is still in flight must find the
        // placeholder to supersede, or the message would show twice.
        let message_id = MessageId::new();
        let event = DomainEvent::MessageSent {
            message_id,
            account_id: account.id,
            mailbox_id: sent.as_ref().map_or_else(MailboxId::new, |m| m.id),
            envelope: outgoing_envelope(&outgoing),
            message_rfc_id: outgoing.message_rfc_id.clone(),
            in_reply_to: outgoing.in_reply_to.clone(),
            references: outgoing.references.clone(),
            body_text: outgoing.body_text.clone(),
            body_html: outgoing.body_html.clone(),
            sent_at: chrono::Utc::now(),
        };
        self.append_and_publish(&message_id, AggregateType::Message, 0, event)
            .await?;

        let outcome = match provider
            .send_message(&outgoing, sent.as_ref().map(|s| &s.path))
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => {
                if let Err(undo) = self.record(&message_id, |m| Some(m.delete())).await {
                    warn!(%undo, %message_id, "send failed, and so did dropping its Sent placeholder");
                }
                return Err(e.into());
            }
        };
        // `Stored { None }` and `ServerFiled` leave the placeholder for the
        // Sent sync to supersede by Message-ID; `NotStored` keeps it as the
        // only local trace of the message.
        if let (
            SentCopy::Stored {
                remote_id: Some(remote_id),
            },
            Some(sent),
        ) = (outcome.sent_copy, &sent)
        {
            self.attach_sent_id(message_id, sent.id, remote_id).await;
        }

        if input.mode.is_reply()
            && let Some(source) = &source
            && let Err(e) = self
                .handle_set_flags(source.id, vec![Flag::Answered], vec![])
                .await
        {
            tracing::warn!(%e, "Failed to set \\Answered on replied-to message");
        }

        if let Some(draft_id) = input.draft_id
            && let Err(e) = self.remove_draft(draft_id).await
        {
            tracing::warn!(%e, %draft_id, "message sent, but deleting its draft failed");
        }

        info!(%message_id, "message sent");
        Ok(CommandResult::MessageSent { message_id })
    }

    /// The provider filed the sent copy as `remote_id`: the placeholder becomes
    /// that message. Left alone when a Sent sync got there first, which means
    /// a row already holds the id or superseded the placeholder.
    async fn attach_sent_id(&self, message_id: MessageId, sent: MailboxId, remote_id: RemoteId) {
        let attached = async {
            if self
                .message_read_repo
                .held_ids(&sent, std::slice::from_ref(&remote_id))
                .await?
                .contains(&remote_id)
            {
                return Ok(());
            }
            self.record(&message_id, |m| m.attach_remote_id(sent, remote_id.clone()))
                .await
                .map(drop)
        }
        .await;
        match attached {
            Ok(()) | Err(ApplicationError::NotFound(..)) => {}
            Err(e) => {
                warn!(%e, %message_id, "Sent copy stored, but its id was not recorded; the next sync will match it")
            }
        }
    }

    /// Store the draft in the server's Drafts folder (and locally), replacing
    /// the version it supersedes.
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
                ApplicationError::PreconditionFailed(
                    "Cartella Bozze non trovata: sincronizza l'account".into(),
                )
            })?;

        let replacing = match input.draft_id {
            Some(old) => self.draft_remote_id(old, &drafts).await,
            None => None,
        };
        let remote_id = self
            .provider_registry
            .get_provider(&account.id)?
            .save_draft(&drafts.path, &outgoing, replacing.as_ref())
            .await?;

        let message_id = match self
            .message_read_repo
            .find_by_remote_id(&drafts.id, &remote_id)
            .await?
        {
            // A Drafts sync stored it first.
            Some(existing) => existing.id,
            None => {
                self.store_draft_locally(account.id, drafts.id, remote_id, &outgoing)
                    .await?
            }
        };

        // The provider already dropped the old version from the server.
        if let Some(old) = input.draft_id.filter(|old| *old != message_id)
            && let Err(e) = self.record(&old, |m| Some(m.delete())).await
        {
            tracing::warn!(%e, draft_id = %old, "new draft saved, but deleting the previous version failed");
        }

        info!(%message_id, "draft saved");
        Ok(CommandResult::DraftSaved { message_id })
    }

    /// Where an earlier version of a draft sits on the server, if it can be found.
    async fn draft_remote_id(&self, id: MessageId, drafts: &Mailbox) -> Option<RemoteId> {
        let (mut message, mut seq) = self.load_message(&id).await.ok()?;
        message.membership(drafts.id)?;
        self.remote_id(&mut message, &mut seq, &drafts.path)
            .await
            .ok()
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
            return Err(ApplicationError::PreconditionFailed(
                "Aggiungi almeno un destinatario".into(),
            ));
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
        let body_html = compose::assemble_html(
            &input.body_md,
            account.identity.signature_html.as_deref(),
            quote.as_ref(),
        );
        let body_text = compose::assemble_text(
            &input.body_md,
            compose::signature_text(account).as_deref(),
            quote.as_ref(),
        );

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
                    ApplicationError::PreconditionFailed(
                        "un allegato inoltrato non è più disponibile".into(),
                    )
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
        let (mut aggregate, mut seq) = match self.load_message(&message.id).await {
            Ok(loaded) => loaded,
            Err(ApplicationError::NotFound(..)) => {
                return Err(ApplicationError::PreconditionFailed(
                    "il messaggio non esiste più".into(),
                ));
            }
            Err(e) => return Err(e),
        };
        let (path, remote_id) = self.readable_copy(&mut aggregate, &mut seq, None).await?;
        Ok(self
            .provider_registry
            .get_provider(&message.account_id)?
            .fetch_raw_message(&path, &remote_id)
            .await?)
    }

    async fn store_draft_locally(
        &self,
        account_id: AccountId,
        drafts_id: MailboxId,
        remote_id: RemoteId,
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
            remote_id,
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
        self.append_and_publish(&id, AggregateType::Message, 0, received)
            .await?;

        let fetched = message.set_body(
            outgoing.body_text.clone(),
            outgoing.body_html.clone(),
            attachments,
        );
        // Body before the event, as in SyncService::store_body.
        self.message_read_repo
            .put_body(
                &id,
                outgoing.body_text.clone(),
                outgoing.body_html.clone(),
                false,
            )
            .await?;
        self.append_and_publish(&id, AggregateType::Message, 1, fetched)
            .await?;
        Ok(id)
    }

    /// Delete a draft locally now and on the server in the background. A draft
    /// lives in the Drafts mailbox, whatever else it may be in.
    async fn remove_draft(&self, id: MessageId) -> Result<(), ApplicationError> {
        let (mut message, mut seq) = self.load_message(&id).await?;
        let drafts = self
            .message_read_repo
            .list_mailboxes(&message.account_id)
            .await?
            .into_iter()
            .find(|m| m.mailbox_type == MailboxType::Drafts && message.membership(m.id).is_some())
            .ok_or_else(|| {
                ApplicationError::PreconditionFailed("il messaggio non è una bozza".into())
            })?;
        // No server copy to find is no reason to keep the local one.
        let remote_id = self
            .remote_id(&mut message, &mut seq, &drafts.path)
            .await
            .ok();
        self.append_and_publish(&id, AggregateType::Message, seq, message.delete())
            .await?;

        if let Some(remote_id) = remote_id {
            let provider = self.provider_registry.get_provider(&message.account_id)?;
            let this = self.clone();
            tokio::spawn(async move {
                let _turn = this.turn(message.account_id).await;
                if let Err(e) = provider
                    .delete_messages(&drafts.path, &[remote_id], true)
                    .await
                {
                    tracing::warn!(%e, "Background draft delete failed — next sync will reconcile");
                }
            });
        }
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
