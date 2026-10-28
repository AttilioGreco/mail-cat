//! Contact aggregate — represents a person or organization in the address book.
//!
//! A contact can have multiple email addresses (with labels), phone numbers,
//! and can originate from different providers (local, Google, CardDAV, etc.).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::DomainError;
use crate::events::DomainEvent;
use crate::model::account::AccountId;
use crate::value_objects::contact_id::ContactId;
use crate::value_objects::email_address::EmailAddress;

// ── ContactEmailLabel ────────────────────────────────────────────────────────

/// Label for a contact email address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ContactEmailLabel {
    Work,
    Home,
    #[default]
    Other,
    Custom(String),
}

// ── ContactEmail ─────────────────────────────────────────────────────────────

/// A single email address entry for a contact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactEmail {
    pub address: EmailAddress,
    pub label: ContactEmailLabel,
    pub is_primary: bool,
}

impl ContactEmail {
    pub fn new(address: EmailAddress, label: ContactEmailLabel, is_primary: bool) -> Self {
        Self {
            address,
            label,
            is_primary,
        }
    }

    pub fn primary(address: EmailAddress) -> Self {
        Self {
            address,
            label: ContactEmailLabel::Work,
            is_primary: true,
        }
    }
}

// ── ContactSource ─────────────────────────────────────────────────────────────

/// Where a contact was imported from / is synced with.
///
/// Only `Local` and `LocalEmail` are currently implemented.
/// Future backends (Google, CardDAV, etc.) are planned but not yet active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContactSource {
    /// Manually added by the user inside mail-cat.
    Local,
    /// Auto-created from observed email activity (sender / recipients).
    LocalEmail,
    // Future (not yet implemented):
    // GoogleContacts,
    // CardDav { server_url: String },
    // MicrosoftGraph,
}

// ── Contact aggregate ─────────────────────────────────────────────────────────

/// Contact aggregate root.
///
/// Represents a person or organization that can receive email.
/// Supports multiple email addresses and multiple provider sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contact {
    pub id: ContactId,
    /// Display name — may be empty for auto-created contacts with no known name.
    pub display_name: String,
    pub emails: Vec<ContactEmail>,
    pub phones: Vec<String>,
    pub organization: Option<String>,
    pub notes: Option<String>,
    pub source: ContactSource,
    /// Provider-assigned identifier (e.g. Google resourceName, CardDAV UID).
    /// `None` for locally created contacts.
    pub external_id: Option<String>,
    /// The account this contact belongs to.
    /// `None` means the contact is shared across all accounts.
    pub account_scope: Option<AccountId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Contact {
    // ── Factory ──────────────────────────────────────────────────────────────

    /// Create a new contact and produce the corresponding domain event.
    ///
    /// `display_name` may be empty — contacts auto-created from email headers
    /// often have no known display name; search falls back to email address.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        display_name: String,
        emails: Vec<ContactEmail>,
        phones: Vec<String>,
        organization: Option<String>,
        notes: Option<String>,
        source: ContactSource,
        external_id: Option<String>,
        account_scope: Option<AccountId>,
    ) -> Result<(Self, DomainEvent), DomainError> {
        if emails.is_empty() {
            return Err(DomainError::Validation(
                "a contact must have at least one email address".into(),
            ));
        }

        let now = Utc::now();
        let id = ContactId::new();

        let event = DomainEvent::ContactAdded {
            contact_id: id,
            display_name: display_name.clone(),
            emails: emails.clone(),
            source: source.clone(),
            external_id: external_id.clone(),
            account_scope,
        };

        let contact = Self {
            id,
            display_name,
            emails,
            phones,
            organization,
            notes,
            source,
            external_id,
            account_scope,
            created_at: now,
            updated_at: now,
        };

        Ok((contact, event))
    }

    /// Apply an update and produce the corresponding domain event.
    pub fn update(
        &mut self,
        display_name: Option<String>,
        emails: Option<Vec<ContactEmail>>,
        phones: Option<Vec<String>>,
        organization: Option<Option<String>>,
        notes: Option<Option<String>>,
    ) -> Result<DomainEvent, DomainError> {
        if let Some(ref name) = display_name {
            if name.trim().is_empty() {
                return Err(DomainError::Validation(
                    "display_name cannot be empty".into(),
                ));
            }
            self.display_name = name.clone();
        }
        if let Some(ref new_emails) = emails {
            if new_emails.is_empty() {
                return Err(DomainError::Validation(
                    "a contact must have at least one email address".into(),
                ));
            }
            self.emails = new_emails.clone();
        }
        if let Some(ref new_phones) = phones {
            self.phones = new_phones.clone();
        }
        if let Some(ref org) = organization {
            self.organization = org.clone();
        }
        if let Some(ref n) = notes {
            self.notes = n.clone();
        }
        self.updated_at = Utc::now();

        Ok(DomainEvent::ContactUpdated {
            contact_id: self.id,
            display_name,
            emails,
            phones,
            organization,
            notes,
        })
    }

    /// Mark the contact as removed and produce the event.
    pub fn remove(&self) -> DomainEvent {
        DomainEvent::ContactRemoved {
            contact_id: self.id,
        }
    }

    /// Absorb `other` into `self`, producing a `ContactMerged` event.
    ///
    /// Merging unions the email lists (deduplicating by address), preserving
    /// `self`'s primary designation. Phone numbers and organization are
    /// updated only if `self` has no value. Returns the event to persist.
    pub fn merge_from(&mut self, other: &Contact) -> DomainEvent {
        let existing: std::collections::HashSet<String> = self
            .emails
            .iter()
            .map(|e| e.address.as_str().to_lowercase())
            .collect();

        for email in &other.emails {
            if !existing.contains(&email.address.as_str().to_lowercase()) {
                let mut absorbed = email.clone();
                absorbed.is_primary = false; // never override self's primary
                self.emails.push(absorbed);
            }
        }
        if self.phones.is_empty() && !other.phones.is_empty() {
            self.phones = other.phones.clone();
        }
        if self.organization.is_none() && other.organization.is_some() {
            self.organization = other.organization.clone();
        }
        self.updated_at = Utc::now();

        DomainEvent::ContactMerged {
            kept_id: self.id,
            discarded_id: other.id,
            merged_emails: self.emails.clone(),
        }
    }

    // ── Event Sourcing rehydration ────────────────────────────────────────────

    /// Apply a single domain event to mutate state (used during rehydration).
    pub fn apply(&mut self, event: &DomainEvent) {
        match event {
            DomainEvent::ContactAdded { .. } => {
                // Initial state set by constructor.
            }
            DomainEvent::ContactUpdated {
                display_name,
                emails,
                phones,
                organization,
                notes,
                ..
            } => {
                if let Some(name) = display_name {
                    self.display_name = name.clone();
                }
                if let Some(new_emails) = emails {
                    self.emails = new_emails.clone();
                }
                if let Some(new_phones) = phones {
                    self.phones = new_phones.clone();
                }
                if let Some(org) = organization {
                    self.organization = org.clone();
                }
                if let Some(n) = notes {
                    self.notes = n.clone();
                }
                self.updated_at = Utc::now();
            }
            DomainEvent::ContactRemoved { .. } => {
                // No state to mutate; removal is tracked externally.
            }
            DomainEvent::ContactMerged { merged_emails, .. } => {
                self.emails = merged_emails.clone();
                self.updated_at = Utc::now();
            }
            _ => {} // ignore events belonging to other aggregates
        }
    }

    /// Rehydrate a Contact from a sequence of domain events.
    ///
    /// The first event must be `ContactAdded`. Returns `None` if the
    /// events slice is empty or doesn't start with a creation event.
    pub fn rehydrate(events: &[DomainEvent]) -> Option<Self> {
        let first = events.first()?;
        let DomainEvent::ContactAdded {
            contact_id,
            display_name,
            emails,
            source,
            external_id,
            account_scope,
        } = first
        else {
            return None;
        };

        let now = Utc::now();
        let mut contact = Self {
            id: *contact_id,
            display_name: display_name.clone(),
            emails: emails.clone(),
            phones: Vec::new(),
            organization: None,
            notes: None,
            source: source.clone(),
            external_id: external_id.clone(),
            account_scope: *account_scope,
            created_at: now,
            updated_at: now,
        };

        for event in &events[1..] {
            contact.apply(event);
        }

        Some(contact)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Returns the primary email address, or the first one if none is marked primary.
    pub fn primary_email(&self) -> Option<&EmailAddress> {
        self.emails
            .iter()
            .find(|e| e.is_primary)
            .or_else(|| self.emails.first())
            .map(|e| &e.address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_email(addr: &str) -> ContactEmail {
        ContactEmail::primary(EmailAddress::new(addr).unwrap())
    }

    fn create(name: &str, email: &str) -> (Contact, DomainEvent) {
        Contact::create(
            name.into(),
            vec![make_email(email)],
            vec![],
            None,
            None,
            ContactSource::Local,
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn create_contact_produces_event() {
        let (contact, event) = create("Alice Rossi", "alice@example.com");
        assert_eq!(contact.display_name, "Alice Rossi");
        assert!(matches!(event, DomainEvent::ContactAdded { .. }));
        assert_eq!(
            contact.primary_email().unwrap().as_str(),
            "alice@example.com"
        );
    }

    #[test]
    fn create_empty_name_is_allowed() {
        // Empty display_name is valid — auto-created contacts may have no known name.
        let (contact, _) = create("", "x@example.com");
        assert_eq!(contact.display_name, "");
    }

    #[test]
    fn create_no_emails_fails() {
        let result = Contact::create(
            "Bob".into(),
            vec![],
            vec![],
            None,
            None,
            ContactSource::Local,
            None,
            None,
        );
        assert!(matches!(result, Err(DomainError::Validation(_))));
    }

    #[test]
    fn update_changes_name() {
        let (mut contact, _) = create("Old Name", "old@example.com");
        let event = contact
            .update(Some("New Name".into()), None, None, None, None)
            .unwrap();
        assert_eq!(contact.display_name, "New Name");
        assert!(matches!(event, DomainEvent::ContactUpdated { .. }));
    }

    #[test]
    fn merge_absorbs_emails() {
        let (mut kept, _) = create("Alice", "alice@work.com");
        let (other, _) = create("Alice Personal", "alice@home.com");
        kept.merge_from(&other);
        assert_eq!(kept.emails.len(), 2);
        assert!(
            kept.emails
                .iter()
                .any(|e| e.address.as_str() == "alice@home.com")
        );
    }

    #[test]
    fn rehydrate_from_events() {
        let (contact, added_event) = create("Carlo", "carlo@example.com");

        let updated_event = {
            let mut c = contact.clone();
            c.update(Some("Carlo Bianchi".into()), None, None, None, None)
                .unwrap()
        };

        let rehydrated = Contact::rehydrate(&[added_event, updated_event]).unwrap();

        assert_eq!(rehydrated.display_name, "Carlo Bianchi");
        assert_eq!(rehydrated.id, contact.id);
    }
}
