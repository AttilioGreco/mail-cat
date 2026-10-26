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

/// One mailbox a message is in. Provider ids are per mailbox (IMAP UIDs), so
/// the id lives here; `None` while unknown (a move the server confirmed
/// without saying where the message went, a renumbered mailbox).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub mailbox_id: MailboxId,
    pub provider_uid: Option<ProviderUid>,
}

/// Message aggregate root. A live message is in at least one mailbox, never
/// twice in the same one; leaving the last deletes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub account_id: AccountId,
    pub memberships: Vec<Membership>,
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
            memberships: vec![Membership { mailbox_id, provider_uid: Some(provider_uid) }],
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

    /// The membership in `mailbox_id`, if the message is there.
    pub fn membership(&self, mailbox_id: MailboxId) -> Option<&Membership> {
        self.memberships.iter().find(|m| m.mailbox_id == mailbox_id)
    }

    /// Whether the message is gone (its last membership was removed).
    pub fn is_deleted(&self) -> bool {
        self.memberships.is_empty()
    }

    /// Move the message from one mailbox to another in a single event.
    /// `provider_uid` is its UID in `to` (`None` only when the provider could
    /// not tell): UIDs are per-mailbox, so the old one must not follow.
    /// `None` when the message is not in `from`: the move no longer applies.
    pub fn move_to(
        &mut self,
        from: MailboxId,
        to: MailboxId,
        provider_uid: Option<ProviderUid>,
    ) -> Option<DomainEvent> {
        self.membership(from)?;
        Some(self.change_mailboxes(vec![Membership { mailbox_id: to, provider_uid }], vec![from]))
    }

    /// Put the message in one more mailbox. `None` when it is already there.
    pub fn add_to(&mut self, mailbox_id: MailboxId, provider_uid: Option<ProviderUid>) -> Option<DomainEvent> {
        self.membership(mailbox_id).is_none().then(|| {
            self.change_mailboxes(vec![Membership { mailbox_id, provider_uid }], vec![])
        })
    }

    /// Take the message out of `mailbox_id`; leaving the last one deletes it.
    /// `None` when it is not there.
    pub fn remove_from(&mut self, mailbox_id: MailboxId) -> Option<DomainEvent> {
        self.membership(mailbox_id)?;
        if self.memberships.len() == 1 {
            return Some(self.delete());
        }
        Some(self.change_mailboxes(vec![], vec![mailbox_id]))
    }

    /// Record the UID the message has in `mailbox_id`, learned after the move
    /// that cleared it. `None` when the message has since left that mailbox or
    /// already has a UID: the information arrived late and no longer applies.
    pub fn attach_uid(&mut self, mailbox_id: MailboxId, provider_uid: ProviderUid) -> Option<DomainEvent> {
        self.membership(mailbox_id)
            .is_some_and(|m| m.provider_uid.is_none())
            .then(|| self.set_uid(mailbox_id, Some(provider_uid)))
    }

    /// Drop the UID the message had in `mailbox_id`, after the server
    /// renumbered that mailbox (UIDVALIDITY change). `None` when the message
    /// is no longer there or has no UID.
    pub fn forget_uid(&mut self, mailbox_id: MailboxId) -> Option<DomainEvent> {
        self.membership(mailbox_id)
            .is_some_and(|m| m.provider_uid.is_some())
            .then(|| self.set_uid(mailbox_id, None))
    }

    fn set_uid(&mut self, mailbox_id: MailboxId, provider_uid: Option<ProviderUid>) -> DomainEvent {
        self.change_mailboxes(vec![Membership { mailbox_id, provider_uid }], vec![])
    }

    fn change_mailboxes(&mut self, added: Vec<Membership>, removed: Vec<MailboxId>) -> DomainEvent {
        self.apply_mailbox_change(&added, &removed);
        DomainEvent::MessageMailboxesChanged { message_id: self.id, added, removed }
    }

    /// Remove the message everywhere.
    pub fn delete(&mut self) -> DomainEvent {
        let mailbox_ids = self.memberships.drain(..).map(|m| m.mailbox_id).collect();
        DomainEvent::MessageDeleted { message_id: self.id, mailbox_ids }
    }

    /// `added` upserts: an entry for a mailbox the message is already in
    /// replaces its UID.
    fn apply_mailbox_change(&mut self, added: &[Membership], removed: &[MailboxId]) {
        self.memberships.retain(|m| !removed.contains(&m.mailbox_id));
        for new in added {
            match self.memberships.iter_mut().find(|m| m.mailbox_id == new.mailbox_id) {
                Some(existing) => *existing = *new,
                None => self.memberships.push(*new),
            }
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
            DomainEvent::MessageMailboxesChanged { added, removed, .. } => {
                self.apply_mailbox_change(added, removed);
            }
            DomainEvent::MessageDeleted { .. } => self.memberships.clear(),
            DomainEvent::MessageSent { .. }
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
            memberships: vec![Membership { mailbox_id: *mailbox_id, provider_uid: Some(*provider_uid) }],
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
