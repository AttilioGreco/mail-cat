use async_trait::async_trait;

use crate::error::DomainError;
use crate::model::collection::{AddressBook, Calendar};
use crate::value_objects::remote::{CollectionId, Etag, Href};
use crate::value_objects::sync_cursor::SyncCursor;

/// What a provider reports for one collection since a cursor.
#[derive(Debug, Clone)]
pub struct CollectionChanges {
    /// To store once the changes are fully applied, and to pass back next time.
    pub cursor: SyncCursor,
    /// Resources added or modified since the cursor. When `reset` is set it is
    /// every resource the collection has, and `removed` is empty: the caller
    /// diffs it against what it holds. An unchanged collection yields nothing.
    pub changed: Vec<(Href, Etag)>,
    pub removed: Vec<Href>,
    /// The cursor passed in was missing, expired or unsupported.
    pub reset: bool,
}

/// A resource as the server holds it: the payload is vCard or iCalendar text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteResource {
    pub href: Href,
    pub etag: Etag,
    pub raw: String,
}

#[derive(Debug, Clone)]
pub struct ResourcePut {
    /// Chosen by the caller even for a create: the aggregate is keyed by it.
    pub href: Href,
    pub raw: String,
    /// The version the edit is based on; `None` is a create and must not
    /// overwrite anything.
    pub if_match: Option<Etag>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutOutcome {
    Stored {
        href: Href,
        etag: Etag,
        /// The body, when the server rewrote it (Google does): that copy
        /// replaces the local one.
        raw: Option<String>,
    },
    /// The precondition failed (412): someone else changed the resource.
    PreconditionFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    PreconditionFailed,
    /// Already gone from the server.
    Gone,
}

/// Address books of one account, on whatever protocol the provider speaks.
///
/// Implementors live in `mc-infrastructure`. A conflict is an outcome the
/// caller resolves, not an error; auth, connection and throttling failures are
/// errors and stop the pass, like in [`EmailProvider`](super::email_provider::EmailProvider).
#[async_trait]
pub trait ContactsProvider: Send + Sync {
    /// Creates are POSTed and the server picks the href (Google): a create
    /// that was stored but never acknowledged cannot be recognised by a
    /// refused retry, so the retry may leave a duplicate.
    fn creates_by_post(&self) -> bool {
        false
    }

    async fn list_address_books(&self) -> Result<Vec<AddressBook>, DomainError>;

    async fn address_book_changes(
        &self,
        book: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError>;

    /// Resources the server no longer has are simply absent.
    async fn fetch_contacts(
        &self,
        book: &CollectionId,
        hrefs: &[Href],
    ) -> Result<Vec<RemoteResource>, DomainError>;

    async fn put_contact(
        &self,
        book: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError>;

    async fn delete_contact(
        &self,
        book: &CollectionId,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError>;
}

/// Calendars of one account; same contract as [`ContactsProvider`].
#[async_trait]
pub trait CalendarProvider: Send + Sync {
    async fn list_calendars(&self) -> Result<Vec<Calendar>, DomainError>;

    async fn calendar_changes(
        &self,
        calendar: &CollectionId,
        since: Option<&SyncCursor>,
    ) -> Result<CollectionChanges, DomainError>;

    async fn fetch_events(
        &self,
        calendar: &CollectionId,
        hrefs: &[Href],
    ) -> Result<Vec<RemoteResource>, DomainError>;

    async fn put_event(
        &self,
        calendar: &CollectionId,
        put: ResourcePut,
    ) -> Result<PutOutcome, DomainError>;

    async fn delete_event(
        &self,
        calendar: &CollectionId,
        href: &Href,
        if_match: &Etag,
    ) -> Result<DeleteOutcome, DomainError>;
}
