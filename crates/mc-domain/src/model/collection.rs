//! Server-side collections (address books, calendars) and the calendar
//! resources inside them.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::model::account::AccountId;
use crate::value_objects::remote::{CollectionId, RemoteBinding};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddressBook {
    pub account: AccountId,
    pub collection: CollectionId,
    pub name: String,
    pub description: Option<String>,
    pub read_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Ownership {
    Own,
    Shared,
    Subscription,
}

/// What a calendar may hold: the sync handles events, the rest is opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CalendarComponent {
    Event,
    Todo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Calendar {
    pub account: AccountId,
    pub collection: CollectionId,
    pub name: String,
    pub color: Option<String>,
    pub timezone: Option<String>,
    pub read_only: bool,
    pub ownership: Ownership,
    pub components: Vec<CalendarComponent>,
}

impl Calendar {
    /// A collection holding only tasks is listed but not synced.
    pub fn holds_events(&self) -> bool {
        self.components.is_empty() || self.components.contains(&CalendarComponent::Event)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventStatus {
    Tentative,
    Confirmed,
    Cancelled,
}

/// What queries need from an event, copied out of the raw payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventFields {
    pub summary: String,
    /// First occurrence. All-day events are midnight UTC of their date.
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub all_day: bool,
    pub recurring: bool,
    pub location: Option<String>,
    pub status: Option<EventStatus>,
}

/// One iCalendar resource: a master and its overrides (a single `UID`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarEvent {
    pub binding: RemoteBinding,
    /// Source of truth: written back with edits applied, never regenerated.
    pub raw: String,
    /// `None` for VTODO/VJOURNAL, which round-trip opaquely.
    pub fields: Option<EventFields>,
}
