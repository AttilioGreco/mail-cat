//! Command handler for contacts and calendar events synced with a server.
//!
//! An edit is stored as a `Local` event, which is what makes the resource
//! dirty, and then pushed at once on a spawned task. Whether that push works
//! or not, the next sync pass pushes whatever is still dirty.

use std::sync::Arc;

use chrono::Utc;
use tracing::{info, warn};

use mc_domain::formats::{self, ContactEdit, EventEdit};
use mc_domain::{
    AccountId, CollectionId, Contact, ContactEmail, ContactId, DomainEvent, Href, Origin,
    RemoteBinding,
};
use uuid::Uuid;

use crate::commands::CommandResult;
use crate::error::ApplicationError;
use crate::services::dav_sync::{DavSyncService, Key, Kind, Target, resource_events};

/// Fields of a contact edit; `None` leaves the field as it is.
#[derive(Debug, Clone, Default)]
pub struct ContactFields {
    pub display_name: Option<String>,
    pub emails: Option<Vec<ContactEmail>>,
    pub phones: Option<Vec<String>>,
    /// `Some(None)` clears it.
    pub organization: Option<Option<String>>,
    pub notes: Option<Option<String>>,
}

pub struct DavCommandHandler {
    dav: Arc<DavSyncService>,
}

impl DavCommandHandler {
    pub fn new(dav: Arc<DavSyncService>) -> Self {
        Self { dav }
    }

    pub async fn handle_add_contact(
        &self,
        account: AccountId,
        book: CollectionId,
        fields: ContactFields,
    ) -> Result<CommandResult, ApplicationError> {
        self.require_writable(account, Kind::Contacts, &book)
            .await?;
        let name = fields
            .display_name
            .clone()
            .filter(|n| !n.trim().is_empty())
            .or_else(|| {
                fields
                    .emails
                    .iter()
                    .flatten()
                    .next()
                    .map(|e| e.address.to_string())
            })
            .ok_or_else(|| {
                ApplicationError::PreconditionFailed("a contact needs a name or an email".into())
            })?;
        let uid = Uuid::now_v7();
        let blank = format!("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:{uid}\r\nFN:\r\nEND:VCARD\r\n");
        let edit = ContactEdit {
            display_name: Some(name),
            ..contact_edit(fields)
        };
        let raw = formats::edit_vcard(&blank, &edit, Utc::now())?;
        let binding = new_binding(account, &book, &format!("{uid}.vcf"));
        let (key, events) = resource_events(Kind::Contacts, None, binding, &raw, Origin::Local)?;
        self.dav.append(&key, None, events).await?;

        let contact_id = key.contact.expect("a contact key");
        info!(%contact_id, "remote contact added");
        self.push_soon(account, Kind::Contacts, book, &key);
        Ok(CommandResult::ContactAdded { contact_id })
    }

    pub async fn handle_update_contact(
        &self,
        contact_id: ContactId,
        fields: ContactFields,
    ) -> Result<CommandResult, ApplicationError> {
        let (contact, sequence) = self.load_contact(&contact_id).await?;
        let (Some(binding), Some(raw)) = (contact.remote, contact.raw) else {
            return Err(not_remote());
        };
        self.require_writable(binding.account, Kind::Contacts, &binding.collection)
            .await?;
        let raw = formats::edit_vcard(&raw, &contact_edit(fields), Utc::now())?;
        let (account, book) = (binding.account, binding.collection.clone());
        let key = Key::contact(contact_id);
        let (_, events) =
            resource_events(Kind::Contacts, Some(&key), binding, &raw, Origin::Local)?;
        self.dav.append(&key, Some(sequence), events).await?;

        info!(%contact_id, "remote contact updated");
        self.push_soon(account, Kind::Contacts, book, &key);
        Ok(CommandResult::ContactUpdated)
    }

    pub async fn handle_remove_contact(
        &self,
        contact_id: ContactId,
    ) -> Result<CommandResult, ApplicationError> {
        let (contact, sequence) = self.load_contact(&contact_id).await?;
        let Some(binding) = contact.remote else {
            return Err(not_remote());
        };
        self.require_writable(binding.account, Kind::Contacts, &binding.collection)
            .await?;
        let key = Key::contact(contact_id);
        let removed = DomainEvent::ContactRemoved {
            contact_id,
            origin: Origin::Local,
        };
        self.dav.append(&key, Some(sequence), vec![removed]).await?;

        info!(%contact_id, "remote contact removed");
        self.push_soon(binding.account, Kind::Contacts, binding.collection, &key);
        Ok(CommandResult::ContactRemoved)
    }

    pub async fn handle_add_event(
        &self,
        account: AccountId,
        calendar: CollectionId,
        summary: String,
        when: (formats::EventWhen, formats::EventWhen),
        location: Option<String>,
    ) -> Result<CommandResult, ApplicationError> {
        self.require_writable(account, Kind::Calendar, &calendar)
            .await?;
        let uid = Uuid::now_v7();
        let now = Utc::now();
        let blank = format!(
            "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//mail-cat//EN\r\nBEGIN:VEVENT\r\n\
             UID:{uid}\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
        );
        let edit = EventEdit {
            summary: Some(summary),
            location: Some(location),
            start: Some(when.0),
            end: Some(when.1),
        };
        let raw = formats::edit_event(&blank, &edit, now)?;
        let href = format!("{uid}.ics");
        let binding = new_binding(account, &calendar, &href);
        let href = binding.href.clone();
        let (key, events) = resource_events(Kind::Calendar, None, binding, &raw, Origin::Local)?;
        self.dav.append(&key, None, events).await?;

        info!(%href, "event added");
        self.push_soon(account, Kind::Calendar, calendar, &key);
        Ok(CommandResult::CalendarEventAdded { href })
    }

    pub async fn handle_update_event(
        &self,
        account: AccountId,
        href: Href,
        edit: EventEdit,
    ) -> Result<CommandResult, ApplicationError> {
        let key = Key::event(account, &href);
        // Before the read: an edit that lands in between fails the append.
        let sequence = self.dav.events.current_sequence(&key.aggregate_id).await?;
        let event = self.load_event(account, &href).await?;
        let calendar = event.binding.collection.clone();
        self.require_writable(account, Kind::Calendar, &calendar)
            .await?;
        let raw = formats::edit_event(&event.raw, &edit, Utc::now())?;
        let (_, events) = resource_events(
            Kind::Calendar,
            Some(&key),
            event.binding,
            &raw,
            Origin::Local,
        )?;
        self.dav.append(&key, Some(sequence), events).await?;

        info!(%href, "event updated");
        self.push_soon(account, Kind::Calendar, calendar, &key);
        Ok(CommandResult::CalendarEventUpdated)
    }

    pub async fn handle_remove_event(
        &self,
        account: AccountId,
        href: Href,
    ) -> Result<CommandResult, ApplicationError> {
        let key = Key::event(account, &href);
        let sequence = self.dav.events.current_sequence(&key.aggregate_id).await?;
        let event = self.load_event(account, &href).await?;
        let calendar = event.binding.collection;
        self.require_writable(account, Kind::Calendar, &calendar)
            .await?;
        let removed = DomainEvent::CalendarEventRemoved {
            account_id: account,
            href: href.clone(),
            origin: Origin::Local,
        };
        self.dav.append(&key, Some(sequence), vec![removed]).await?;

        info!(%href, "event removed");
        self.push_soon(account, Kind::Calendar, calendar, &key);
        Ok(CommandResult::CalendarEventRemoved)
    }

    /// The contact as its stream leaves it, and that stream's last sequence.
    async fn load_contact(&self, id: &ContactId) -> Result<(Contact, u64), ApplicationError> {
        let history = self.dav.events.load_events(&id.to_string(), 0).await?;
        let removed = history
            .iter()
            .any(|e| matches!(e.payload, DomainEvent::ContactRemoved { .. }));
        let payloads: Vec<_> = history.iter().map(|e| e.payload.clone()).collect();
        let contact = Contact::rehydrate(&payloads)
            .filter(|_| !removed)
            .ok_or_else(|| ApplicationError::NotFound("Contact", id.to_string()))?;
        Ok((contact, history.last().map_or(0, |e| e.sequence_number)))
    }

    async fn load_event(
        &self,
        account: AccountId,
        href: &Href,
    ) -> Result<mc_domain::CalendarEvent, ApplicationError> {
        self.dav
            .store
            .get_event(&account, href)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Event", href.to_string()))
    }

    /// A read-only collection is refused before any event: the push could
    /// never succeed, and the edit would only be lost on the next pass.
    async fn require_writable(
        &self,
        account: AccountId,
        kind: Kind,
        collection: &CollectionId,
    ) -> Result<(), ApplicationError> {
        let store = &self.dav.store;
        let read_only = match kind {
            Kind::Contacts => store
                .list_address_books(&account)
                .await?
                .iter()
                .find(|b| b.collection == *collection)
                .map(|b| b.read_only),
            Kind::Calendar => store
                .list_calendars(&account)
                .await?
                .iter()
                .find(|c| c.collection == *collection)
                .map(|c| c.read_only),
        };
        match read_only {
            None => Err(ApplicationError::NotFound(
                "Collection",
                collection.to_string(),
            )),
            Some(true) => Err(ApplicationError::PreconditionFailed(format!(
                "{collection} is read-only"
            ))),
            Some(false) => Ok(()),
        }
    }

    fn push_soon(&self, account: AccountId, kind: Kind, collection: CollectionId, key: &Key) {
        let dav = Arc::clone(&self.dav);
        let aggregate_id = key.aggregate_id.clone();
        tokio::spawn(async move {
            let target = Target::new(kind, collection);
            if let Err(e) = dav.push_now(&account, &target, &aggregate_id).await {
                warn!(%aggregate_id, "push after a local edit failed, the next pass retries: {e}");
            }
        });
    }
}

fn contact_edit(fields: ContactFields) -> ContactEdit {
    ContactEdit {
        display_name: fields.display_name,
        emails: fields.emails,
        phones: fields.phones,
        organization: fields.organization,
        notes: fields.notes,
    }
}

fn not_remote() -> ApplicationError {
    ApplicationError::PreconditionFailed("the contact is not synced with a server".into())
}

/// A resource made here: no etag yet, so its first push is a create. The
/// href is chosen here because the aggregate is keyed by it.
fn new_binding(account: AccountId, collection: &CollectionId, name: &str) -> RemoteBinding {
    let dir = collection.as_str();
    let slash = if dir.ends_with('/') { "" } else { "/" };
    RemoteBinding {
        account,
        collection: collection.clone(),
        href: Href::new(format!("{dir}{slash}{name}")),
        etag: None,
        remote_modified: None,
    }
}
