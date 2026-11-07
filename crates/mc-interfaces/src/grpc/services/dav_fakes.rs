//! Fakes shared by the contact and calendar service tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use mc_application::{
    ApplicationError,
    bus::CommandBus,
    commands::{Command, CommandResult},
};
use mc_domain::{
    AccountId, AddressBook, Calendar, CalendarEvent, CollectionId, ContactId,
    ContactReadRepository, ContactSummary, DavReadRepository, DomainError, EmailAddress, Etag,
    EventSync, Href, Occurrence, PendingPush, RemoteContact,
};

#[derive(Default)]
pub struct FakeDav {
    pub books: Vec<AddressBook>,
    pub calendars: Vec<Calendar>,
    pub contacts: Vec<RemoteContact>,
    pub occurrences: Vec<Occurrence>,
    pub event: Option<CalendarEvent>,
    pub sync: EventSync,
    pub pending_contacts: Vec<PendingPush>,
    pub conflicts: u64,
    /// What `events_in_range` was asked: the calendars and the window start.
    pub asked_calendars: Mutex<Vec<CollectionId>>,
    pub asked_from: Mutex<Option<DateTime<Utc>>>,
}

#[async_trait]
impl DavReadRepository for FakeDav {
    async fn list_address_books(&self, _: &AccountId) -> Result<Vec<AddressBook>, DomainError> {
        Ok(self.books.clone())
    }

    async fn list_calendars(&self, _: &AccountId) -> Result<Vec<Calendar>, DomainError> {
        Ok(self.calendars.clone())
    }

    async fn pending_contacts(
        &self,
        _: &AccountId,
        _: &CollectionId,
    ) -> Result<Vec<PendingPush>, DomainError> {
        Ok(self.pending_contacts.clone())
    }

    async fn pending_events(
        &self,
        _: &AccountId,
        _: &CollectionId,
    ) -> Result<Vec<PendingPush>, DomainError> {
        Ok(vec![])
    }

    async fn contact_etags(
        &self,
        _: &AccountId,
        _: &CollectionId,
    ) -> Result<HashMap<Href, Etag>, DomainError> {
        unreachable!()
    }

    async fn event_etags(
        &self,
        _: &AccountId,
        _: &CollectionId,
    ) -> Result<HashMap<Href, Etag>, DomainError> {
        unreachable!()
    }

    async fn remote_contacts(
        &self,
        _: &AccountId,
        book: Option<&CollectionId>,
    ) -> Result<Vec<RemoteContact>, DomainError> {
        Ok(self
            .contacts
            .iter()
            .filter(|c| book.is_none_or(|b| *b == c.binding.collection))
            .cloned()
            .collect())
    }

    async fn remote_contact(&self, id: &ContactId) -> Result<Option<RemoteContact>, DomainError> {
        Ok(self.contacts.iter().find(|c| c.id == *id).cloned())
    }

    async fn remote_contacts_by_email(
        &self,
        email: &EmailAddress,
    ) -> Result<Vec<RemoteContact>, DomainError> {
        Ok(self
            .contacts
            .iter()
            .filter(|c| c.raw.contains(email.as_str()))
            .cloned()
            .collect())
    }

    async fn contact_at(&self, _: &AccountId, _: &Href) -> Result<Option<ContactId>, DomainError> {
        unreachable!()
    }

    async fn get_event(
        &self,
        _: &AccountId,
        _: &Href,
    ) -> Result<Option<CalendarEvent>, DomainError> {
        Ok(self.event.clone())
    }

    async fn event_sync(&self, _: &AccountId, _: &Href) -> Result<EventSync, DomainError> {
        Ok(self.sync.clone())
    }

    async fn events_in_range(
        &self,
        _: &AccountId,
        calendars: &[CollectionId],
        from: DateTime<Utc>,
        _: DateTime<Utc>,
    ) -> Result<Vec<Occurrence>, DomainError> {
        *self.asked_calendars.lock().unwrap() = calendars.to_vec();
        *self.asked_from.lock().unwrap() = Some(from);
        Ok(self.occurrences.clone())
    }

    async fn conflicts_lost(&self, _: &AccountId) -> Result<u64, DomainError> {
        Ok(self.conflicts)
    }
}

/// Records the commands it gets and answers each with `result`.
pub struct RecordingBus {
    pub seen: Mutex<Vec<Command>>,
    pub result: CommandResult,
}

impl RecordingBus {
    pub fn answering(result: CommandResult) -> Self {
        Self {
            seen: Mutex::default(),
            result,
        }
    }
}

#[async_trait]
impl CommandBus for RecordingBus {
    async fn dispatch(&self, command: Command) -> Result<CommandResult, ApplicationError> {
        self.seen.lock().unwrap().push(command);
        Ok(self.result.clone())
    }
}

/// For services whose DAV calls never read through the query bus.
pub struct NoQueries;

#[async_trait]
impl mc_application::bus::QueryBus for NoQueries {
    async fn dispatch(
        &self,
        _: mc_application::queries::Query,
    ) -> Result<mc_application::queries::QueryResult, ApplicationError> {
        unreachable!()
    }
}

/// Harvested contacts, found by address.
#[derive(Default)]
pub struct FakeContacts {
    pub contacts: Vec<ContactSummary>,
}

#[async_trait]
impl ContactReadRepository for FakeContacts {
    async fn get_contact(&self, _: &ContactId) -> Result<Option<ContactSummary>, DomainError> {
        unreachable!()
    }

    async fn list_contacts(
        &self,
        _: &mc_shared::pagination::Pagination,
    ) -> Result<mc_shared::pagination::PagedResult<ContactSummary>, DomainError> {
        unreachable!()
    }

    async fn list_unsynced_contacts(&self) -> Result<Vec<ContactSummary>, DomainError> {
        Ok(self.contacts.clone())
    }

    async fn find_by_email(
        &self,
        email: &EmailAddress,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        Ok(self
            .contacts
            .iter()
            .filter(|c| c.emails.iter().any(|e| e.address == *email))
            .cloned()
            .collect())
    }

    async fn find_by_email_in_account(
        &self,
        _: &EmailAddress,
        _: &AccountId,
    ) -> Result<Vec<ContactSummary>, DomainError> {
        unreachable!()
    }

    async fn search_contacts(&self, _: &str, _: usize) -> Result<Vec<ContactSummary>, DomainError> {
        unreachable!()
    }

    async fn find_duplicate_candidates(
        &self,
        _: Option<&AccountId>,
    ) -> Result<Vec<(ContactSummary, ContactSummary)>, DomainError> {
        unreachable!()
    }
}
