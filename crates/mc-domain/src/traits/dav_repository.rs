use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::DomainError;
use crate::model::account::AccountId;
use crate::model::collection::{AddressBook, Calendar, CalendarEvent, EventFields};
use crate::value_objects::contact_id::ContactId;
use crate::value_objects::email_address::EmailAddress;
use crate::value_objects::remote::{CollectionId, Etag, Href, RemoteBinding};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingChange {
    Put {
        raw: String,
    },
    /// The user deleted a resource the server still has.
    Delete,
}

/// A local change the server has not acknowledged: derived from the events
/// (the last `Local` one has no later `PushedToServer`), so it survives a
/// restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPush {
    /// What to append the outcome events to: a contact's id, or the event's
    /// [`AggregateType::path_id`](crate::AggregateType::path_id).
    pub aggregate_id: String,
    pub binding: RemoteBinding,
    pub change: PendingChange,
    /// When the last local edit happened: the local side of last-write-wins.
    pub edited_at: DateTime<Utc>,
    /// Why the server refused the last push, if it did.
    pub failure: Option<String>,
    /// Sequence of the local event that made this the pending state: what a
    /// `PushedToServer` acknowledging it must carry.
    pub sequence: u64,
}

/// One occurrence of an event in a requested range: `event` holds that
/// occurrence's own times and fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Occurrence {
    pub calendar: CollectionId,
    pub href: Href,
    pub event: EventFields,
    pub sync: EventSync,
}

/// Where an event stands with the server, like a contact's `dirty` and
/// `push_error`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventSync {
    pub dirty: bool,
    pub push_failed: Option<String>,
}

/// A contact bound to a server resource, with the state of its sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteContact {
    pub id: ContactId,
    pub binding: RemoteBinding,
    pub raw: String,
    /// A local edit the server has not acknowledged.
    pub dirty: bool,
    /// Why the server refused the last push, if it did.
    pub push_error: Option<String>,
}

/// Read side of the DAV projections: what the sync service diffs against and
/// what the API lists.
#[async_trait]
pub trait DavReadRepository: Send + Sync {
    async fn list_address_books(
        &self,
        account: &AccountId,
    ) -> Result<Vec<AddressBook>, DomainError>;

    async fn list_calendars(&self, account: &AccountId) -> Result<Vec<Calendar>, DomainError>;

    /// Dirty contacts and deletions of one address book. Pending creates
    /// have no etag in their binding.
    async fn pending_contacts(
        &self,
        account: &AccountId,
        book: &CollectionId,
    ) -> Result<Vec<PendingPush>, DomainError>;

    async fn pending_events(
        &self,
        account: &AccountId,
        calendar: &CollectionId,
    ) -> Result<Vec<PendingPush>, DomainError>;

    /// The etag each local resource of the collection is based on, to fetch
    /// only what differs. Never-pushed resources are absent.
    async fn contact_etags(
        &self,
        account: &AccountId,
        book: &CollectionId,
    ) -> Result<HashMap<Href, Etag>, DomainError>;

    async fn event_etags(
        &self,
        account: &AccountId,
        calendar: &CollectionId,
    ) -> Result<HashMap<Href, Etag>, DomainError>;

    /// Contacts bound to a server, of one address book or of all the account's.
    async fn remote_contacts(
        &self,
        account: &AccountId,
        book: Option<&CollectionId>,
    ) -> Result<Vec<RemoteContact>, DomainError>;

    async fn remote_contact(&self, id: &ContactId) -> Result<Option<RemoteContact>, DomainError>;

    /// Contacts bound to a server that have this address, of any account.
    async fn remote_contacts_by_email(
        &self,
        email: &EmailAddress,
    ) -> Result<Vec<RemoteContact>, DomainError>;

    /// The contact bound to a server resource.
    async fn contact_at(
        &self,
        account: &AccountId,
        href: &Href,
    ) -> Result<Option<ContactId>, DomainError>;

    async fn get_event(
        &self,
        account: &AccountId,
        href: &Href,
    ) -> Result<Option<CalendarEvent>, DomainError>;

    async fn event_sync(&self, account: &AccountId, href: &Href) -> Result<EventSync, DomainError>;

    /// Occurrences overlapping `[from, to)`, recurrence expanded at read time,
    /// ordered by start. `calendars` empty means every calendar of the account.
    async fn events_in_range(
        &self,
        account: &AccountId,
        calendars: &[CollectionId],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<Occurrence>, DomainError>;

    /// Local edits discarded by last-write-wins, ever: never invisible.
    async fn conflicts_lost(&self, account: &AccountId) -> Result<u64, DomainError>;
}
