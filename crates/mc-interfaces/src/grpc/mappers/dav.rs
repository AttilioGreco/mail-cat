//! Contacts and calendars synced with a server, between domain and proto.

use chrono::{DateTime, NaiveDate, Utc};
use tonic::Status;

use super::datetime_to_timestamp;
use crate::grpc::proto;
use mc_application::commands::ContactFields;
use mc_domain::formats::{EventEdit, EventWhen, parse_event, parse_vcard};
use mc_domain::{
    AccountId, AddressBook, Calendar, CalendarEvent, ContactEmail, ContactEmailLabel,
    ContactSummary, EmailAddress, EventFields, EventSync, Occurrence, Ownership, RemoteContact,
};

pub fn parse_account_id(text: &str) -> Result<AccountId, Status> {
    uuid::Uuid::parse_str(text)
        .map(AccountId::from_uuid)
        .map_err(|_| Status::invalid_argument(format!("invalid account id: {text}")))
}

pub fn address_book_to_proto(book: &AddressBook) -> proto::AddressBook {
    proto::AddressBook {
        collection: book.collection.to_string(),
        name: book.name.clone(),
        description: book.description.clone(),
        read_only: book.read_only,
    }
}

pub fn remote_contact_to_proto(contact: &RemoteContact) -> proto::RemoteContact {
    // A card the parser refuses is still listed, with its raw text, so that
    // it can be inspected and deleted.
    let card = parse_vcard(&contact.raw).ok();
    let emails = card.iter().flat_map(|c| &c.emails);
    proto::RemoteContact {
        id: contact.id.to_string(),
        account_id: contact.binding.account.to_string(),
        address_book: contact.binding.collection.to_string(),
        href: contact.binding.href.to_string(),
        display_name: card
            .as_ref()
            .map(|c| c.display_name.clone())
            .unwrap_or_default(),
        emails: emails.map(|e| e.address.to_string()).collect(),
        phones: card.iter().flat_map(|c| c.phones.clone()).collect(),
        organization: card.as_ref().and_then(|c| c.organization.clone()),
        notes: card.as_ref().and_then(|c| c.notes.clone()),
        raw: contact.raw.clone(),
        dirty: contact.dirty,
        push_failed: contact.push_error.clone(),
    }
}

pub fn remote_contact_to_card(
    contact: &RemoteContact,
    address_book_name: Option<String>,
) -> proto::ContactCard {
    let remote = remote_contact_to_proto(contact);
    proto::ContactCard {
        id: remote.id,
        display_name: remote.display_name,
        organization: remote.organization,
        emails: remote.emails,
        phones: remote.phones,
        address_book: Some(remote.address_book),
        address_book_name,
        account_id: remote.account_id,
        notes: remote.notes,
        source: proto::ContactSource::Remote as i32,
        raw: Some(remote.raw),
        dirty: remote.dirty,
        push_failed: remote.push_failed,
    }
}

pub fn harvested_contact_to_card(contact: &ContactSummary) -> proto::ContactCard {
    proto::ContactCard {
        id: contact.id.to_string(),
        display_name: contact.display_name.clone(),
        organization: contact.organization.clone(),
        emails: contact
            .emails
            .iter()
            .map(|e| e.address.to_string())
            .collect(),
        account_id: contact
            .account_scope
            .map(|a| a.to_string())
            .unwrap_or_default(),
        source: proto::ContactSource::Harvested as i32,
        ..Default::default()
    }
}

pub fn contact_fields_from_proto(fields: proto::ContactFields) -> Result<ContactFields, String> {
    let emails = fields
        .emails
        .map(|list| {
            list.items
                .iter()
                .enumerate()
                .map(|(i, address)| {
                    let address = EmailAddress::new(address.trim())
                        .map_err(|_| format!("invalid email address: {address}"))?;
                    Ok(ContactEmail::new(address, ContactEmailLabel::Other, i == 0))
                })
                .collect::<Result<Vec<_>, String>>()
        })
        .transpose()?;
    let clearable = |text: Option<String>| text.map(|t| Some(t).filter(|t| !t.is_empty()));
    Ok(ContactFields {
        display_name: fields.display_name,
        emails,
        phones: fields.phones.map(|list| list.items),
        organization: clearable(fields.organization),
        notes: clearable(fields.notes),
    })
}

pub fn calendar_to_proto(calendar: &Calendar) -> proto::Calendar {
    proto::Calendar {
        collection: calendar.collection.to_string(),
        name: calendar.name.clone(),
        color: calendar.color.clone(),
        timezone: calendar.timezone.clone(),
        read_only: calendar.read_only,
        ownership: match calendar.ownership {
            Ownership::Own => proto::CalendarOwnership::Own,
            Ownership::Shared => proto::CalendarOwnership::Shared,
            Ownership::Subscription => proto::CalendarOwnership::Subscription,
        } as i32,
        holds_events: calendar.holds_events(),
    }
}

fn occurrence_parts(
    calendar: String,
    href: String,
    fields: &EventFields,
    sync: &EventSync,
) -> proto::EventOccurrence {
    proto::EventOccurrence {
        calendar,
        href,
        summary: fields.summary.clone(),
        start: Some(datetime_to_timestamp(&fields.start)),
        end: Some(datetime_to_timestamp(&fields.end)),
        all_day: fields.all_day,
        recurring: fields.recurring,
        location: fields.location.clone(),
        status: fields
            .status
            .map(|status| format!("{status:?}").to_lowercase()),
        description: fields.description.clone(),
        dirty: sync.dirty,
        push_failed: sync.push_failed.clone(),
    }
}

pub fn occurrence_to_proto(occurrence: &Occurrence) -> proto::EventOccurrence {
    occurrence_parts(
        occurrence.calendar.to_string(),
        occurrence.href.to_string(),
        &occurrence.event,
        &occurrence.sync,
    )
}

pub fn event_to_proto(event: &CalendarEvent, sync: &EventSync) -> proto::EventDetail {
    let calendar = event.binding.collection.to_string();
    let href = event.binding.href.to_string();
    // The projection keeps the master's fields; a resource it could not read
    // anymore still has its raw text to show.
    let fields = event
        .fields
        .clone()
        .or_else(|| parse_event(&event.raw).ok()?.fields);
    proto::EventDetail {
        first: fields
            .as_ref()
            .map(|f| occurrence_parts(calendar.clone(), href.clone(), f, sync)),
        calendar,
        href,
        raw: event.raw.clone(),
    }
}

/// A date is an all-day time, an RFC 3339 instant is a point in time.
pub fn event_when_from_str(text: &str) -> Result<EventWhen, String> {
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(EventWhen::Date(date));
    }
    DateTime::parse_from_rfc3339(text)
        .map(|at| EventWhen::At(at.with_timezone(&Utc)))
        .map_err(|_| format!("{text:?} is neither a YYYY-MM-DD date nor an RFC 3339 time"))
}

pub fn event_edit_from_proto(req: &proto::UpdateEventRequest) -> Result<EventEdit, String> {
    let when = |text: &Option<String>| text.as_deref().map(event_when_from_str).transpose();
    Ok(EventEdit {
        summary: req.summary.clone(),
        location: req
            .location
            .clone()
            .map(|l| Some(l).filter(|l| !l.is_empty())),
        description: req
            .description
            .clone()
            .map(|d| Some(d).filter(|d| !d.is_empty())),
        start: when(&req.start)?,
        end: when(&req.end)?,
    })
}

#[cfg(test)]
mod tests;
