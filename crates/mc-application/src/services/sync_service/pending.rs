//! Messages waiting for a remote id in some mailbox: after the provider
//! invalidated a mailbox's ids, or a move it confirmed without saying where
//! the message went. Their ids are found again by Message-ID.

use std::sync::Arc;

use tracing::{info, warn};

use mc_domain::{
    DomainEvent, EmailProvider, EventStore, Mailbox, MailboxId, MailboxType, Message,
    MessageEnvelopeSummary, MessageId, ProviderRegistry,
};

use super::events::sync_failed;
use super::{SyncService, SyncStore};
use crate::error::ApplicationError;

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Whether any message of the account is waiting for a remote id.
    pub(super) async fn any_pending(&self, mailbox: &Mailbox) -> Result<bool, ApplicationError> {
        let mailboxes = self
            .projector
            .list_mailboxes(&mailbox.account_id)
            .await
            .map_err(sync_failed("list_mailboxes"))?;
        for id in std::iter::once(mailbox.id).chain(
            mailboxes
                .iter()
                .map(|m| m.id)
                .filter(|id| *id != mailbox.id),
        ) {
            let waiting = self
                .projector
                .pending_messages(&id)
                .await
                .map_err(sync_failed("pending_messages"))?;
            if !waiting.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The server copy with a Message-ID the account already has keeps that
    /// message's id instead of arriving as a duplicate: a message waiting for
    /// its remote id here gets it, else one waiting elsewhere that the server
    /// no longer has there moves here. A copy still present in its own mailbox
    /// is a genuine second copy (self-sent mail in Sent and INBOX) and stays.
    pub(super) async fn reattach(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &Mailbox,
        env: &MessageEnvelopeSummary,
    ) -> Result<bool, ApplicationError> {
        let Some(rfc_id) = env.message_rfc_id.as_deref() else {
            return Ok(false);
        };
        let copies = self
            .projector
            .find_by_rfc_id(&mailbox.account_id, rfc_id)
            .await
            .map_err(sync_failed("find_by_rfc_id"))?;
        // (message, mailbox it waits for a remote id in)
        let (here, elsewhere): (Vec<_>, Vec<_>) = copies
            .iter()
            .flat_map(|c| {
                c.memberships
                    .iter()
                    .filter(|m| m.remote_id.is_none())
                    .map(|m| (c.id, m.mailbox_id))
            })
            .partition(|&(_, in_mailbox)| in_mailbox == mailbox.id);
        for (waiting, _) in here {
            if self
                .update_message(&waiting, |m| {
                    m.attach_remote_id(mailbox.id, env.remote_id.clone())
                })
                .await?
            {
                return Ok(true);
            }
        }
        for (orphan, from) in elsewhere {
            if self.is_sent_placeholder(&orphan).await
                || !self.gone_from_server(provider, from, rfc_id).await
            {
                continue;
            }
            let moved = self
                .update_message(&orphan, |m| {
                    m.membership(from).filter(|x| x.remote_id.is_none())?;
                    m.move_to(from, mailbox.id, Some(env.remote_id.clone()))
                })
                .await?;
            if moved {
                info!(message_id = %orphan, "Found again in another mailbox, moved there");
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether the server has no copy of a message waiting for a remote id in
    /// `mailbox_id` left there that another local row doesn't already hold.
    /// Unsure counts as present: a wrong "gone" would steal the message from
    /// its mailbox.
    async fn gone_from_server(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox_id: MailboxId,
        rfc_id: &str,
    ) -> bool {
        let Ok(Some(own)) = self.projector.get_mailbox(&mailbox_id).await else {
            return false;
        };
        let Ok(found) = provider.find_by_message_id(&own.path, rfc_id).await else {
            return false;
        };
        self.projector
            .held_ids(&own.id, &found)
            .await
            .is_ok_and(|held| found.iter().all(|id| held.contains(id)))
    }

    /// The provider invalidated this mailbox's ids: every message it holds
    /// goes back to waiting for one. A stale id would hide the new message
    /// that reuses it, so none is kept; the mailbox is then ingested whole
    /// and each message re-attaches by Message-ID.
    pub(super) async fn forget_remote_ids(&self, local: &Mailbox) -> Result<(), ApplicationError> {
        warn!(path = %local.path, "Remote ids invalidated, matching the mailbox again");
        let all = mc_shared::pagination::Pagination {
            page: 1,
            page_size: u32::MAX,
        };
        let held = self
            .projector
            .list_messages(&local.id, &all)
            .await
            .map_err(sync_failed("list_messages"))?;
        for message in &held.items {
            self.update_message(&message.id, |m| m.forget_remote_id(local.id))
                .await?;
        }
        Ok(())
    }

    /// Rows already waiting for a remote id before this sync, and still
    /// waiting after its ingest: reconciliation goes by id and skips them, so
    /// nothing else would ever resolve them. The server copy found by
    /// Message-ID (one no other row holds) gives the id back; with none the
    /// message is gone and is deleted here. Sent placeholders and drafts are
    /// ours and wait for their server copy instead. A server that can't answer
    /// leaves them for the next sync. Returns how many were deleted.
    pub(super) async fn settle_pending(
        &self,
        provider: &Arc<PR::Provider>,
        mailbox: &Mailbox,
        waiting: Vec<MessageId>,
    ) -> Result<u32, ApplicationError> {
        if waiting.is_empty() || mailbox.mailbox_type == MailboxType::Drafts {
            return Ok(0);
        }
        let mut deleted = 0;
        for message_id in waiting {
            if self.is_sent_placeholder(&message_id).await {
                continue;
            }
            let events = self
                .event_store
                .load_events(&message_id.to_string(), 0)
                .await
                .map_err(sync_failed("load_events"))?;
            let events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
            let Some(message) = Message::rehydrate(&events).filter(|m| !m.is_deleted()) else {
                continue;
            };
            if message
                .membership(mailbox.id)
                .is_none_or(|m| m.remote_id.is_some())
            {
                continue;
            }
            let found = match message.message_rfc_id.as_deref() {
                Some(rfc_id) => match provider.find_by_message_id(&mailbox.path, rfc_id).await {
                    Ok(found) => found,
                    Err(e) => {
                        warn!(%message_id, error = %e, "Could not look for a message waiting for its id, keeping it");
                        continue;
                    }
                },
                None => Vec::new(),
            };
            let held = self
                .projector
                .held_ids(&mailbox.id, &found)
                .await
                .map_err(sync_failed("held_ids"))?;
            let free = found.into_iter().rev().find(|id| !held.contains(id));
            let changed = self
                .update_message(&message_id, |m| {
                    if m.membership(mailbox.id)
                        .is_none_or(|x| x.remote_id.is_some())
                    {
                        return None;
                    }
                    match free.clone() {
                        Some(id) => m.attach_remote_id(mailbox.id, id),
                        None => m.remove_from(mailbox.id),
                    }
                })
                .await?;
            if changed && free.is_none() {
                info!(%message_id, "Waiting for an id and not on the server, deleted locally");
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// After a mailbox whose ids were invalidated was ingested whole, the
    /// received messages there still without an id are gone from the server.
    /// Sent placeholders are not received messages, and stay.
    pub(super) async fn drop_unclaimed(&self, mailbox: &Mailbox) -> Result<(), ApplicationError> {
        let waiting = self
            .projector
            .pending_messages(&mailbox.id)
            .await
            .map_err(sync_failed("pending_messages"))?;
        for message_id in waiting {
            if self.is_sent_placeholder(&message_id).await {
                continue;
            }
            let dropped = self
                .update_message(&message_id, |m| {
                    m.membership(mailbox.id).filter(|x| x.remote_id.is_none())?;
                    m.remove_from(mailbox.id)
                })
                .await?;
            if dropped {
                info!(%message_id, "Not in the mailbox after its ids changed, deleted locally");
            }
        }
        Ok(())
    }
}
