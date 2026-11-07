//! Contacts and calendars synced with a server: address books, calendars,
//! events in a range and their edits.

use chrono::{DateTime, Utc};
use tonic::transport::Channel;

use crate::client::MailCatClient;
use crate::error::Result;
use crate::proto::mail_cat::v1::{
    self as pb, calendar_service_client::CalendarServiceClient,
    contact_service_client::ContactServiceClient,
};

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressBook {
    pub collection: String,
    pub name: String,
    pub description: Option<String>,
    pub read_only: bool,
}

/// A contact synced with a server. `dirty` is a local edit not yet
/// acknowledged; `push_failed` is why the server refused it.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteContact {
    pub id: String,
    pub account_id: String,
    pub address_book: String,
    pub href: String,
    pub display_name: String,
    pub emails: Vec<String>,
    pub phones: Vec<String>,
    pub organization: Option<String>,
    pub notes: Option<String>,
    pub raw: String,
    pub dirty: bool,
    pub push_failed: Option<String>,
}

/// Fields of a contact edit; `None` leaves the field as it is, and an empty
/// organization or notes clears it.
#[derive(Debug, Clone, Default)]
pub struct ContactFields {
    pub display_name: Option<String>,
    pub emails: Option<Vec<String>>,
    pub phones: Option<Vec<String>>,
    pub organization: Option<String>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Calendar {
    pub collection: String,
    pub name: String,
    pub color: Option<String>,
    pub timezone: Option<String>,
    pub read_only: bool,
    /// `own`, `shared` or `subscription`.
    pub ownership: &'static str,
    pub holds_events: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventOccurrence {
    pub calendar: String,
    pub href: String,
    pub summary: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub all_day: bool,
    pub recurring: bool,
    pub location: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventDetail {
    pub calendar: String,
    pub href: String,
    pub raw: String,
    pub first: Option<EventOccurrence>,
}

/// Times are RFC 3339 instants, or `YYYY-MM-DD` for an all-day event.
#[derive(Debug, Clone)]
pub struct NewEvent {
    pub calendar: String,
    pub summary: String,
    pub start: String,
    pub end: String,
    pub location: Option<String>,
}

/// `None` leaves the field as it is; an empty location clears it.
#[derive(Debug, Clone, Default)]
pub struct EventChanges {
    pub summary: Option<String>,
    pub location: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DavStatus {
    pub conflicts_lost: u64,
    pub pending_changes: u32,
    pub failed_changes: u32,
}

fn timestamp(at: DateTime<Utc>) -> pb::Timestamp {
    pb::Timestamp {
        seconds: at.timestamp(),
        nanos: at.timestamp_subsec_nanos() as i32,
    }
}

fn instant(ts: Option<pb::Timestamp>) -> DateTime<Utc> {
    let ts = ts.unwrap_or_default();
    DateTime::from_timestamp(ts.seconds, ts.nanos as u32).unwrap_or_default()
}

fn occurrence(o: pb::EventOccurrence) -> EventOccurrence {
    EventOccurrence {
        calendar: o.calendar,
        href: o.href,
        summary: o.summary,
        start: instant(o.start),
        end: instant(o.end),
        all_day: o.all_day,
        recurring: o.recurring,
        location: o.location,
        status: o.status,
    }
}

fn contact_fields(f: ContactFields) -> pb::ContactFields {
    let list = |items| pb::StringList { items };
    pb::ContactFields {
        display_name: f.display_name,
        emails: f.emails.map(list),
        phones: f.phones.map(list),
        organization: f.organization,
        notes: f.notes,
    }
}

impl MailCatClient {
    fn contacts(&self) -> ContactServiceClient<Channel> {
        ContactServiceClient::new(self.channel())
    }

    fn calendars(&self) -> CalendarServiceClient<Channel> {
        CalendarServiceClient::new(self.channel())
    }

    pub async fn list_address_books(&self, account_id: &str) -> Result<Vec<AddressBook>> {
        let resp = self
            .contacts()
            .list_address_books(pb::ListAddressBooksRequest {
                account_id: account_id.to_owned(),
            })
            .await?
            .into_inner();
        Ok(resp
            .address_books
            .into_iter()
            .map(|b| AddressBook {
                collection: b.collection,
                name: b.name,
                description: b.description,
                read_only: b.read_only,
            })
            .collect())
    }

    pub async fn list_remote_contacts(
        &self,
        account_id: &str,
        address_book: Option<String>,
    ) -> Result<Vec<RemoteContact>> {
        let resp = self
            .contacts()
            .list_remote_contacts(pb::ListRemoteContactsRequest {
                account_id: account_id.to_owned(),
                address_book,
            })
            .await?
            .into_inner();
        Ok(resp.contacts.into_iter().map(remote_contact).collect())
    }

    pub async fn get_remote_contact(&self, contact_id: &str) -> Result<RemoteContact> {
        let resp = self
            .contacts()
            .get_remote_contact(pb::GetRemoteContactRequest {
                contact_id: contact_id.to_owned(),
            })
            .await?
            .into_inner();
        Ok(remote_contact(resp))
    }

    /// Returns the new contact's id.
    pub async fn create_remote_contact(
        &self,
        account_id: &str,
        address_book: &str,
        fields: ContactFields,
    ) -> Result<String> {
        let resp = self
            .contacts()
            .create_remote_contact(pb::CreateRemoteContactRequest {
                account_id: account_id.to_owned(),
                address_book: address_book.to_owned(),
                fields: Some(contact_fields(fields)),
            })
            .await?
            .into_inner();
        Ok(resp.contact_id)
    }

    pub async fn update_remote_contact(
        &self,
        contact_id: &str,
        fields: ContactFields,
    ) -> Result<()> {
        self.contacts()
            .update_remote_contact(pb::UpdateRemoteContactRequest {
                contact_id: contact_id.to_owned(),
                fields: Some(contact_fields(fields)),
            })
            .await?;
        Ok(())
    }

    pub async fn delete_remote_contact(&self, contact_id: &str) -> Result<()> {
        self.contacts()
            .delete_remote_contact(pb::DeleteRemoteContactRequest {
                contact_id: contact_id.to_owned(),
            })
            .await?;
        Ok(())
    }

    pub async fn list_calendars(&self, account_id: &str) -> Result<Vec<Calendar>> {
        let resp = self
            .calendars()
            .list_calendars(pb::ListCalendarsRequest {
                account_id: account_id.to_owned(),
            })
            .await?
            .into_inner();
        Ok(resp
            .calendars
            .into_iter()
            .map(|c| Calendar {
                ownership: match pb::CalendarOwnership::try_from(c.ownership) {
                    Ok(pb::CalendarOwnership::Shared) => "shared",
                    Ok(pb::CalendarOwnership::Subscription) => "subscription",
                    _ => "own",
                },
                collection: c.collection,
                name: c.name,
                color: c.color,
                timezone: c.timezone,
                read_only: c.read_only,
                holds_events: c.holds_events,
            })
            .collect())
    }

    /// Occurrences overlapping `[from, to)`; no calendars means all of them.
    pub async fn list_events(
        &self,
        account_id: &str,
        calendars: Vec<String>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<EventOccurrence>> {
        let resp = self
            .calendars()
            .list_events(pb::ListEventsRequest {
                account_id: account_id.to_owned(),
                calendars,
                from: Some(timestamp(from)),
                to: Some(timestamp(to)),
            })
            .await?
            .into_inner();
        Ok(resp.occurrences.into_iter().map(occurrence).collect())
    }

    pub async fn get_event(&self, account_id: &str, href: &str) -> Result<EventDetail> {
        let resp = self
            .calendars()
            .get_event(pb::GetEventRequest {
                account_id: account_id.to_owned(),
                href: href.to_owned(),
            })
            .await?
            .into_inner();
        Ok(EventDetail {
            calendar: resp.calendar,
            href: resp.href,
            raw: resp.raw,
            first: resp.first.map(occurrence),
        })
    }

    /// Returns the new event's href.
    pub async fn create_event(&self, account_id: &str, event: NewEvent) -> Result<String> {
        let resp = self
            .calendars()
            .create_event(pb::CreateEventRequest {
                account_id: account_id.to_owned(),
                calendar: event.calendar,
                summary: event.summary,
                start: event.start,
                end: event.end,
                location: event.location,
            })
            .await?
            .into_inner();
        Ok(resp.href)
    }

    pub async fn update_event(
        &self,
        account_id: &str,
        href: &str,
        changes: EventChanges,
    ) -> Result<()> {
        self.calendars()
            .update_event(pb::UpdateEventRequest {
                account_id: account_id.to_owned(),
                href: href.to_owned(),
                summary: changes.summary,
                location: changes.location,
                start: changes.start,
                end: changes.end,
            })
            .await?;
        Ok(())
    }

    pub async fn delete_event(&self, account_id: &str, href: &str) -> Result<()> {
        self.calendars()
            .delete_event(pb::DeleteEventRequest {
                account_id: account_id.to_owned(),
                href: href.to_owned(),
            })
            .await?;
        Ok(())
    }

    pub async fn dav_status(&self, account_id: &str) -> Result<DavStatus> {
        let resp = self
            .calendars()
            .get_dav_status(pb::GetDavStatusRequest {
                account_id: account_id.to_owned(),
            })
            .await?
            .into_inner();
        Ok(DavStatus {
            conflicts_lost: resp.conflicts_lost,
            pending_changes: resp.pending_changes,
            failed_changes: resp.failed_changes,
        })
    }
}

fn remote_contact(c: pb::RemoteContact) -> RemoteContact {
    RemoteContact {
        id: c.id,
        account_id: c.account_id,
        address_book: c.address_book,
        href: c.href,
        display_name: c.display_name,
        emails: c.emails,
        phones: c.phones,
        organization: c.organization,
        notes: c.notes,
        raw: c.raw,
        dirty: c.dirty,
        push_failed: c.push_failed,
    }
}
