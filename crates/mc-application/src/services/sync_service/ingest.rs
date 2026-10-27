//! Storing fetched envelopes as messages, and matching them with the local
//! rows that already stand for them (our own sent mail and drafts).

use std::collections::HashMap;
use std::sync::Arc;

use tracing::warn;

use mc_domain::{
    AggregateType, DomainEvent, EmailProvider, EventStore, Mailbox, MailboxType, Message,
    MessageEnvelopeSummary, MessageId, MessageSummary, ProviderRegistry, RemoteId,
};

use super::events::sync_failed;
use super::{SyncService, SyncStore};
use crate::error::ApplicationError;

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Store one batch of envelopes, then their bodies when eager.
    /// `restored`: messages brought back, not new (see `MessageReceived`).
    pub(super) async fn ingest(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        envelopes: &[MessageEnvelopeSummary],
        restored: bool,
    ) -> Result<u32, ApplicationError> {
        let awaiting_remote_id = self.any_pending(local).await?;
        let mut new_messages: Vec<(MessageId, RemoteId)> = Vec::new();
        for env in envelopes {
            if awaiting_remote_id && self.reattach(provider, local, env).await? {
                continue;
            }
            let placeholder = match self.local_copy(local, env).await {
                Some(stored)
                    if stored
                        .membership(local.id)
                        .is_some_and(|m| m.remote_id.is_some()) =>
                {
                    continue;
                }
                other => other,
            };

            let (_message, mut event) = Message::receive(
                local.account_id,
                local.id,
                env.remote_id.clone(),
                env.message_rfc_id.clone(),
                env.in_reply_to.clone(),
                env.references.clone(),
                env.envelope.clone(),
                env.flags.clone(),
                env.size_bytes,
                env.has_attachments,
                env.internal_date,
            );
            if let DomainEvent::MessageReceived {
                message_id,
                restored: quiet,
                ..
            } = &mut event
            {
                *quiet = restored;
                let message_id = *message_id;
                new_messages.push((message_id, env.remote_id.clone()));
                self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
                    .await?;
                self.supersede(placeholder).await?;
            }
        }

        if !new_messages.is_empty() {
            self.backfill_wake.notify_one();
        }
        if self.config.eager_body_fetch && !new_messages.is_empty() {
            self.fetch_eager_bodies(provider, local, &new_messages)
                .await;
        }
        Ok(new_messages.len() as u32)
    }

    async fn fetch_eager_bodies(
        &self,
        provider: &Arc<PR::Provider>,
        local: &Mailbox,
        new_messages: &[(MessageId, RemoteId)],
    ) {
        let ids: Vec<RemoteId> = new_messages.iter().map(|(_, id)| id.clone()).collect();
        let by_remote_id: HashMap<&RemoteId, MessageId> = new_messages
            .iter()
            .map(|(message, id)| (id, *message))
            .collect();
        match provider.fetch_bodies_bulk(&local.path, &ids).await {
            Ok(bodies) => {
                for body in bodies {
                    // Search indexing is owned by the search-index projection.
                    if let Some(message_id) = by_remote_id.get(&body.remote_id)
                        && let Err(e) = self
                            .store_body(
                                message_id,
                                body.body_text,
                                body.body_html,
                                body.truncated,
                                vec![],
                            )
                            .await
                    {
                        warn!(%message_id, error = %e, "Failed to project body");
                    }
                }
            }
            Err(e) => warn!(path = %local.path, error = %e, "Bulk body fetch failed, skipping"),
        }
    }

    /// Which of `ids` no message in `mailbox` holds yet: the ones worth
    /// fetching. A sync cut short and redone skips what it already stored.
    pub(super) async fn unheld(
        &self,
        mailbox: &Mailbox,
        ids: &[RemoteId],
    ) -> Result<Vec<RemoteId>, ApplicationError> {
        let held = self
            .projector
            .held_ids(&mailbox.id, ids)
            .await
            .map_err(sync_failed("held_ids"))?;
        Ok(ids
            .iter()
            .filter(|id| !held.contains(id))
            .cloned()
            .collect())
    }

    /// Mail we put on the server ourselves is stored locally before its
    /// server copy syncs back: a saved draft with the id APPEND returned, a
    /// sent message with none (`MessageSent`). Scoped to Drafts and Sent: a
    /// per-envelope lookup would be a needless query per message on a big
    /// Inbox sync. Only this mailbox's rows count, and one without an id only
    /// if it really is a `MessageSent` placeholder: anything else without an
    /// id is a received message waiting for it.
    async fn local_copy(
        &self,
        mailbox: &Mailbox,
        env: &MessageEnvelopeSummary,
    ) -> Option<MessageSummary> {
        if !matches!(
            mailbox.mailbox_type,
            MailboxType::Drafts | MailboxType::Sent
        ) {
            return None;
        }
        let rfc_id = env.message_rfc_id.as_deref()?;
        let copies = self
            .projector
            .find_by_rfc_id(&mailbox.account_id, rfc_id)
            .await
            .ok()?;
        for copy in copies {
            let Some(here) = copy.membership(mailbox.id) else {
                continue;
            };
            let ours = match &here.remote_id {
                Some(id) => *id == env.remote_id,
                None => {
                    mailbox.mailbox_type == MailboxType::Sent
                        && self.is_sent_placeholder(&copy.id).await
                }
            };
            if ours {
                return Some(copy);
            }
        }
        None
    }

    pub(super) async fn is_sent_placeholder(&self, message_id: &MessageId) -> bool {
        self.event_store
            .load_events(&message_id.to_string(), 0)
            .await
            .is_ok_and(|events| {
                matches!(
                    events.first().map(|e| &e.payload),
                    Some(DomainEvent::MessageSent { .. })
                )
            })
    }

    /// The server copy just stored replaces the one shown at send time. The
    /// projector counts recipients once per Message-ID, so the copy is not
    /// counted again.
    async fn supersede(&self, placeholder: Option<MessageSummary>) -> Result<(), ApplicationError> {
        let Some(sent) = placeholder else {
            return Ok(());
        };
        let event = DomainEvent::MessageDeleted {
            message_id: sent.id,
            mailbox_ids: sent.memberships.iter().map(|m| m.mailbox_id).collect(),
        };
        self.append_and_project(&sent.id.to_string(), AggregateType::Message, event)
            .await
    }
}
