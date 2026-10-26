use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::events::DomainEvent;
use crate::model::account::AccountId;
use crate::model::attachment::Attachment;
use crate::model::mailbox::MailboxId;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::flag::Flag;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::uid::ProviderUid;

/// An address with optional display name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub name: Option<String>,
    pub email: EmailAddress,
}

/// Message envelope (header summary).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub reply_to: Vec<Address>,
    pub subject: String,
    pub date: DateTime<Utc>,
}

/// Message aggregate root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub account_id: AccountId,
    pub mailbox_id: MailboxId,
    pub provider_uid: Option<ProviderUid>,
    pub message_rfc_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub envelope: Envelope,
    pub flags: Vec<Flag>,
    pub size_bytes: u64,
    pub has_attachments: bool,
    pub attachments: Vec<Attachment>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    /// The stored body is only the start of a long text part (the backfill
    /// caps what it downloads); opening the message fetches the rest.
    #[serde(default)]
    pub body_truncated: bool,
    pub received_at: DateTime<Utc>,
    pub internal_date: DateTime<Utc>,
}

impl Message {
    /// Create a message from provider data (received during sync) and produce the event.
    pub fn receive(
        account_id: AccountId,
        mailbox_id: MailboxId,
        provider_uid: ProviderUid,
        message_rfc_id: Option<String>,
        in_reply_to: Option<String>,
        references: Vec<String>,
        envelope: Envelope,
        flags: Vec<Flag>,
        size_bytes: u64,
        has_attachments: bool,
        internal_date: DateTime<Utc>,
    ) -> (Self, DomainEvent) {
        let id = MessageId::new();
        let received_at = internal_date;

        let event = DomainEvent::MessageReceived {
            message_id: id,
            account_id,
            mailbox_id,
            provider_uid,
            message_rfc_id: message_rfc_id.clone(),
            in_reply_to: in_reply_to.clone(),
            references: references.clone(),
            envelope: envelope.clone(),
            flags: flags.clone(),
            size_bytes,
            has_attachments,
            received_at,
            restored: false,
        };

        let message = Self {
            id,
            account_id,
            mailbox_id,
            provider_uid: Some(provider_uid),
            message_rfc_id,
            in_reply_to,
            references,
            envelope,
            flags,
            size_bytes,
            has_attachments,
            attachments: Vec::new(),
            body_text: None,
            body_html: None,
            body_truncated: false,
            received_at,
            internal_date: received_at,
        };

        (message, event)
    }

    /// Attach the body content after fetching it from the provider.
    pub fn set_body(
        &mut self,
        body_text: Option<String>,
        body_html: Option<String>,
        attachments: Vec<Attachment>,
    ) -> DomainEvent {
        self.body_text = body_text.clone();
        self.body_html = body_html.clone();
        self.attachments = attachments.clone();
        self.has_attachments = !self.attachments.is_empty();

        DomainEvent::MessageBodyFetched {
            message_id: self.id,
            attachments,
        }
    }

    /// Change flags on the message and produce the event.
    pub fn change_flags(
        &mut self,
        flags_to_add: Vec<Flag>,
        flags_to_remove: Vec<Flag>,
    ) -> DomainEvent {
        for flag in &flags_to_remove {
            self.flags.retain(|f| f != flag);
        }
        for flag in &flags_to_add {
            if !self.flags.contains(flag) {
                self.flags.push(flag.clone());
            }
        }

        DomainEvent::MessageFlagged {
            message_id: self.id,
            mailbox_id: self.mailbox_id,
            flags_added: flags_to_add,
            flags_removed: flags_to_remove,
        }
    }

    /// Mark as read (add Seen flag).
    pub fn mark_as_read(&mut self) -> DomainEvent {
        self.change_flags(vec![Flag::Seen], vec![])
    }

    /// Mark as unread (remove Seen flag).
    pub fn mark_as_unread(&mut self) -> DomainEvent {
        self.change_flags(vec![], vec![Flag::Seen])
    }

    /// Toggle the Flagged (starred) flag.
    pub fn toggle_flagged(&mut self) -> DomainEvent {
        if self.flags.contains(&Flag::Flagged) {
            self.change_flags(vec![], vec![Flag::Flagged])
        } else {
            self.change_flags(vec![Flag::Flagged], vec![])
        }
    }

    /// Move the message to another mailbox and produce the event.
    /// `provider_uid` is its UID there (`None` only when the provider could
    /// not tell): UIDs are per-mailbox, so the old one must not follow.
    pub fn move_to(&mut self, to_mailbox_id: MailboxId, provider_uid: Option<ProviderUid>) -> DomainEvent {
        let from = self.mailbox_id;
        self.mailbox_id = to_mailbox_id;
        self.provider_uid = provider_uid;

        DomainEvent::MessageMoved {
            message_id: self.id,
            from_mailbox_id: from,
            to_mailbox_id,
            provider_uid,
        }
    }

    /// Record the UID the message has in `mailbox_id`, learned after the move
    /// that cleared it. `None` when the message has since left that mailbox or
    /// already has a UID: the information arrived late and no longer applies.
    pub fn attach_uid(&mut self, mailbox_id: MailboxId, provider_uid: ProviderUid) -> Option<DomainEvent> {
        (self.mailbox_id == mailbox_id && self.provider_uid.is_none())
            .then(|| self.set_uid(Some(provider_uid)))
    }

    /// Drop the UID the message had in `mailbox_id`, after the server
    /// renumbered that mailbox (UIDVALIDITY change). `None` when the message
    /// is no longer there or has no UID.
    pub fn forget_uid(&mut self, mailbox_id: MailboxId) -> Option<DomainEvent> {
        (self.mailbox_id == mailbox_id && self.provider_uid.is_some()).then(|| self.set_uid(None))
    }

    fn set_uid(&mut self, provider_uid: Option<ProviderUid>) -> DomainEvent {
        self.provider_uid = provider_uid;

        DomainEvent::MessageMoved {
            message_id: self.id,
            from_mailbox_id: self.mailbox_id,
            to_mailbox_id: self.mailbox_id,
            provider_uid,
        }
    }

    /// Produce a deletion event.
    pub fn delete(&self, permanently: bool) -> DomainEvent {
        DomainEvent::MessageDeleted {
            message_id: self.id,
            mailbox_id: self.mailbox_id,
            permanently,
        }
    }

    /// Whether the message has been read.
    pub fn is_read(&self) -> bool {
        self.flags.contains(&Flag::Seen)
    }

    /// Whether the message is flagged/starred.
    pub fn is_flagged(&self) -> bool {
        self.flags.contains(&Flag::Flagged)
    }

    // ── Event Sourcing rehydration ─────────────────────────────────────

    /// Apply a single domain event to mutate aggregate state.
    pub fn apply(&mut self, event: &DomainEvent) {
        match event {
            DomainEvent::MessageReceived { .. } => {
                // Initial state is set by the constructor.
            }
            // A text-only fetch carries no attachment info: keep the envelope's.
            DomainEvent::MessageBodyFetched { attachments, .. } if !attachments.is_empty() => {
                self.attachments = attachments.clone();
                self.has_attachments = true;
            }
            DomainEvent::MessageBodyFetched { .. } => {}
            DomainEvent::MessageFlagged {
                flags_added,
                flags_removed,
                ..
            } => {
                for flag in flags_removed {
                    self.flags.retain(|f| f != flag);
                }
                for flag in flags_added {
                    if !self.flags.contains(flag) {
                        self.flags.push(flag.clone());
                    }
                }
            }
            DomainEvent::MessageMoved { to_mailbox_id, provider_uid, .. } => {
                self.mailbox_id = *to_mailbox_id;
                self.provider_uid = *provider_uid;
            }
            DomainEvent::MessageDeleted { .. }
            | DomainEvent::MessageSent { .. }
            | DomainEvent::DraftSaved { .. }
            | DomainEvent::DraftDiscarded { .. } => {
                // These don't further mutate in-memory message state.
            }
            _ => {} // Ignore events belonging to other aggregates.
        }
    }

    /// Rehydrate a Message from a sequence of domain events.
    ///
    /// The first event must be `MessageReceived`. Returns `None` if the
    /// events slice is empty or doesn't start with a creation event.
    pub fn rehydrate(events: &[DomainEvent]) -> Option<Self> {
        let first = events.first()?;
        let DomainEvent::MessageReceived {
            message_id,
            account_id,
            mailbox_id,
            provider_uid,
            message_rfc_id,
            in_reply_to,
            references,
            envelope,
            flags,
            size_bytes,
            has_attachments,
            received_at,
            ..
        } = first
        else {
            return None;
        };

        let mut message = Self {
            id: *message_id,
            account_id: *account_id,
            mailbox_id: *mailbox_id,
            provider_uid: Some(*provider_uid),
            message_rfc_id: message_rfc_id.clone(),
            in_reply_to: in_reply_to.clone(),
            references: references.clone(),
            envelope: envelope.clone(),
            flags: flags.clone(),
            size_bytes: *size_bytes,
            has_attachments: *has_attachments,
            attachments: Vec::new(),
            body_text: None,
            body_html: None,
            body_truncated: false,
            received_at: *received_at,
            internal_date: *received_at,
        };

        for event in &events[1..] {
            message.apply(event);
        }

        Some(message)
    }
}

/// Outgoing message for sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutgoingMessage {
    pub from: Address,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub subject: String,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub attachments: Vec<OutgoingAttachment>,
    /// RFC Message-ID (`<…>`). Set by the caller so the SMTP copy and the one
    /// appended to Sent/Drafts share it; `None` lets the transport pick one.
    pub message_rfc_id: Option<String>,
    /// Extra `X-…` headers (e.g. draft metadata). Values must be ASCII.
    pub extra_headers: Vec<(String, String)>,
}

/// Attachment for outgoing messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutgoingAttachment {
    pub filename: String,
    pub mime_type: String,
    /// Set for inline images referenced from the HTML body as `cid:<content_id>`.
    pub content_id: Option<String>,
    pub data: Vec<u8>,
}

#[cfg(test)]
mod tests;
