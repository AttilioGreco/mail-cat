use std::ops::Range;
use std::str::FromStr;

use calcard::common::timezone::Tz;
use calcard::icalendar::dates::TimeOrDelta;
use calcard::icalendar::{
    ICalendar, ICalendarComponent, ICalendarComponentType, ICalendarProperty, ICalendarStatus,
};
use chrono::{DateTime, NaiveDate, Utc};

use super::{Doc, content_line, escape, invalid, timestamp};
use crate::error::DomainError;
use crate::model::collection::{EventFields, EventStatus};

/// Occurrences produced per resource: the ceiling for a rule with no end.
/// Daily since 2010 is ~6000, so this covers any real calendar.
// ponytail: raise, or seed the expansion near `from`, if someone has a minutely rule.
const EXPAND_LIMIT: usize = 20_000;
/// Enough to get past a series' leading EXDATEs and overrides.
const FIRST_OCCURRENCES: usize = 64;

/// What queries need from an iCalendar resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedEvent {
    pub uid: Option<String>,
    /// `None` when the resource holds no VEVENT (VTODO, VJOURNAL): opaque.
    pub fields: Option<EventFields>,
    /// `LAST-MODIFIED` of the master: the remote side of last-write-wins.
    pub last_modified: Option<DateTime<Utc>>,
}

fn text(comp: &ICalendarComponent, property: &ICalendarProperty) -> Option<String> {
    comp.property(property)?
        .values
        .first()?
        .as_text()
        .map(str::to_owned)
}

/// The VEVENT that is not an override of another: the series itself.
fn master(ical: &ICalendar) -> Option<(usize, &ICalendarComponent)> {
    let mut events = ical
        .components
        .iter()
        .enumerate()
        .filter(|(_, c)| c.component_type == ICalendarComponentType::VEvent);
    let first = events.next()?;
    Some(
        std::iter::once(first)
            .chain(events)
            .find(|(_, c)| !c.is_recurrence_override())
            .unwrap_or(first),
    )
}

fn fields(comp: &ICalendarComponent, start: DateTime<Utc>, end: DateTime<Utc>) -> EventFields {
    let all_day = comp
        .property(&ICalendarProperty::Dtstart)
        .and_then(|e| e.values.first())
        .and_then(|v| v.as_partial_date_time())
        .is_some_and(|dt| !dt.has_time());
    EventFields {
        summary: text(comp, &ICalendarProperty::Summary).unwrap_or_default(),
        start,
        end,
        all_day,
        recurring: comp.is_recurrent_or_override(),
        location: text(comp, &ICalendarProperty::Location).filter(|l| !l.is_empty()),
        status: comp.status().and_then(|s| match s {
            ICalendarStatus::Tentative => Some(EventStatus::Tentative),
            ICalendarStatus::Confirmed => Some(EventStatus::Confirmed),
            ICalendarStatus::Cancelled => Some(EventStatus::Cancelled),
            _ => None,
        }),
        description: text(comp, &ICalendarProperty::Description).filter(|d| !d.is_empty()),
    }
}

fn span(
    start: DateTime<Tz>,
    end: TimeOrDelta<DateTime<Tz>, chrono::TimeDelta>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let start = start.with_timezone(&Utc);
    let end = match end {
        TimeOrDelta::Time(end) => end.with_timezone(&Utc),
        TimeOrDelta::Delta(length) => start + length,
    };
    (start, end)
}

/// calcard pairs an override with its occurrence only when their SEQUENCEs
/// are equal, and clients do not keep them equal (an override often has none).
/// Expansion has no use for SEQUENCE, so it is dropped before it can get in the way.
fn parse_for_expansion(raw: &str) -> Result<ICalendar, DomainError> {
    let mut ical = ICalendar::parse(raw).map_err(|_| invalid("iCalendar"))?;
    for comp in &mut ical.components {
        comp.entries
            .retain(|e| e.name != ICalendarProperty::Sequence);
    }
    Ok(ical)
}

pub fn parse_event(raw: &str) -> Result<ParsedEvent, DomainError> {
    let ical = parse_for_expansion(raw)?;
    let uid = ical.uids().next().map(str::to_owned);
    let Some((id, comp)) = master(&ical) else {
        return Ok(ParsedEvent {
            uid,
            fields: None,
            last_modified: None,
        });
    };

    // The master's own first start is not always an occurrence: Google's
    // "this and following" splits leave series whose first instance is an
    // EXDATE or an override. The earliest occurrence of any of its events
    // stands in; a series with none left is kept, without fields.
    let expanded = ical.expand_dates(Utc, FIRST_OCCURRENCES).events;
    let first = expanded
        .iter()
        .filter(|e| e.comp_id as usize == id)
        .min_by_key(|e| e.start)
        .or_else(|| {
            expanded
                .iter()
                .filter(|e| {
                    ical.components
                        .get(e.comp_id as usize)
                        .is_some_and(|c| c.component_type == ICalendarComponentType::VEvent)
                })
                .min_by_key(|e| e.start)
        });
    let Some(first) = first else {
        return Ok(ParsedEvent {
            uid,
            fields: None,
            last_modified: None,
        });
    };
    let (start, end) = span(first.start, first.end.clone());

    Ok(ParsedEvent {
        uid,
        fields: Some(fields(comp, start, end)),
        last_modified: comp
            .property(&ICalendarProperty::LastModified)
            .and_then(|e| e.values.first()?.as_partial_date_time()?.to_timestamp())
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
    })
}

/// The occurrences of the resource's events overlapping `[from, to)`, by
/// start: the master's recurrences minus EXDATEs, with each RECURRENCE-ID
/// override standing in for the occurrence it replaces.
pub fn occurrences(
    raw: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<EventFields>, DomainError> {
    let ical = parse_for_expansion(raw)?;
    let mut found: Vec<EventFields> = ical
        .expand_dates(Utc, EXPAND_LIMIT)
        .events
        .into_iter()
        .filter_map(|event| {
            let comp = ical.components.get(event.comp_id as usize)?;
            if comp.component_type != ICalendarComponentType::VEvent {
                return None;
            }
            let (start, end) = span(event.start, event.end);
            // An instant (start == end) sits inside the range it starts in.
            let overlaps = start < to && (end > from || start >= from);
            overlaps.then(|| fields(comp, start, end))
        })
        .collect();
    found.sort_by_key(|f| f.start);
    Ok(found)
}

/// A new start or end, in the terms a caller has: a date or an instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventWhen {
    Date(NaiveDate),
    At(DateTime<Utc>),
}

#[derive(Debug, Clone, Default)]
pub struct EventEdit {
    pub summary: Option<String>,
    /// `Some(None)` clears it.
    pub location: Option<Option<String>>,
    /// `Some(None)` clears it.
    pub description: Option<Option<String>>,
    pub start: Option<EventWhen>,
    pub end: Option<EventWhen>,
}

/// Applies `edit` to the series' master and stamps `DTSTAMP`,
/// `LAST-MODIFIED` and `SEQUENCE` so other clients see a newer version.
/// Everything else, overrides and unknown properties included, is kept as
/// written.
pub fn edit_event(raw: &str, edit: &EventEdit, now: DateTime<Utc>) -> Result<String, DomainError> {
    let mut doc = Doc::parse(raw);
    if master_scope(&doc).is_none() {
        return Err(DomainError::Validation(
            "the resource holds no event".into(),
        ));
    }
    let set = |doc: &mut Doc, name: &str, build: &dyn Fn(&[super::Line]) -> Vec<String>| {
        let scope = master_scope(doc).expect("checked above");
        doc.replace(&scope, name, build);
    };

    if let Some(summary) = &edit.summary {
        set(&mut doc, "SUMMARY", &|_| {
            vec![content_line("SUMMARY", "", &escape(summary))]
        });
    }
    if let Some(location) = &edit.location {
        set(&mut doc, "LOCATION", &|_| {
            location
                .iter()
                .map(|l| content_line("LOCATION", "", &escape(l)))
                .collect()
        });
    }
    if let Some(description) = &edit.description {
        set(&mut doc, "DESCRIPTION", &|_| {
            description
                .iter()
                .map(|d| content_line("DESCRIPTION", "", &escape(d)))
                .collect()
        });
    }
    for (name, when) in [("DTSTART", edit.start), ("DTEND", edit.end)] {
        if let Some(when) = when {
            set(&mut doc, name, &|old| {
                vec![when_line(name, old.first(), when)]
            });
        }
    }
    if edit.end.is_some() {
        // DTEND and DURATION exclude each other.
        set(&mut doc, "DURATION", &|_| vec![]);
    }

    let scope = master_scope(&doc).expect("checked above");
    let sequence = doc
        .first(&scope, "SEQUENCE")
        .and_then(|l| l.value().trim().parse::<u32>().ok())
        .map_or(1, |n| n + 1);
    for (name, value) in [
        ("DTSTAMP", timestamp(now)),
        ("LAST-MODIFIED", timestamp(now)),
        ("SEQUENCE", sequence.to_string()),
    ] {
        set(&mut doc, name, &|_| vec![content_line(name, "", &value)]);
    }
    Ok(doc.render())
}

fn master_scope(doc: &Doc) -> Option<Range<usize>> {
    let events = doc.components("VEVENT");
    events
        .iter()
        .find(|scope| doc.first(scope, "RECURRENCE-ID").is_none())
        .or(events.first())
        .cloned()
}

/// The line for a DTSTART/DTEND, in the zone the old one used: rewriting it
/// in UTC would pin a recurring event to one UTC offset across DST changes.
fn when_line(name: &str, old: Option<&super::Line>, when: EventWhen) -> String {
    let zone = old
        .map(super::Line::params)
        .and_then(|params| {
            let tzid = params.split(';').find_map(|p| p.strip_prefix("TZID="))?;
            Some(tzid.trim_matches('"').to_owned())
        })
        .and_then(|tzid| Some((Tz::from_str(&tzid).ok()?, tzid)));
    match (when, zone) {
        (EventWhen::Date(date), _) => {
            content_line(name, ";VALUE=DATE", &date.format("%Y%m%d").to_string())
        }
        (EventWhen::At(at), Some((tz, tzid))) => content_line(
            name,
            &format!(";TZID={tzid}"),
            &at.with_timezone(&tz).format("%Y%m%dT%H%M%S").to_string(),
        ),
        (EventWhen::At(at), None) => content_line(name, "", &timestamp(at)),
    }
}
