//! Reminders: a VEVENT's VALARMs, read as the instants they ring and
//! rewritten from the editor's list.

use calcard::icalendar::{
    ICalendar, ICalendarComponent, ICalendarComponentType, ICalendarParameterValue,
    ICalendarProperty, ICalendarRelated, ICalendarValue,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use url::Url;
use uuid::Uuid;

use super::icalendar::{
    EXPAND_LIMIT, is_all_day, master, overlaps, parse_for_expansion, span, text,
};
use super::{Doc, Line, content_line};
use crate::error::DomainError;

/// Hosts whose links in a LOCATION or DESCRIPTION are taken for a video call.
const MEETING_HOSTS: [&str; 5] = [
    "meet.google.com",
    "zoom.us",
    "teams.microsoft.com",
    "teams.live.com",
    "webex.com",
];

/// A reminder of one occurrence, resolved to the instant it rings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alarm {
    pub fire_at: DateTime<Utc>,
    /// Identifies the trigger across syncs, so a reminder changed from 10 to
    /// 30 minutes is a new one that rings again.
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccurrenceAlarms {
    pub start: DateTime<Utc>,
    /// When the occurrence is over; an all-day one ends at midnight in the
    /// zone it was resolved in, not at midnight UTC.
    pub end: DateTime<Utc>,
    pub alarms: Vec<Alarm>,
    pub meeting_url: Option<String>,
}

enum Trigger {
    /// Negative is before the anchor, as in the TRIGGER text.
    Relative {
        offset: Duration,
        from_end: bool,
    },
    At(DateTime<Utc>),
}

fn trigger(alarm: &ICalendarComponent) -> Option<Trigger> {
    let entry = alarm.property(&ICalendarProperty::Trigger)?;
    match entry.values.first()? {
        ICalendarValue::Duration(d) => {
            let seconds = (i64::from(d.weeks) * 7 + i64::from(d.days)) * 86_400
                + i64::from(d.hours) * 3_600
                + i64::from(d.minutes) * 60
                + i64::from(d.seconds);
            Some(Trigger::Relative {
                offset: Duration::seconds(if d.neg { -seconds } else { seconds }),
                from_end: entry
                    .params
                    .iter()
                    .any(|p| p.value == ICalendarParameterValue::Related(ICalendarRelated::End)),
            })
        }
        ICalendarValue::PartialDateTime(at) => {
            DateTime::from_timestamp(at.to_timestamp()?, 0).map(Trigger::At)
        }
        _ => None,
    }
}

fn triggers(ical: &ICalendar, event: &ICalendarComponent) -> Vec<Trigger> {
    event
        .component_ids
        .iter()
        .filter_map(|&id| ical.components.get(id as usize))
        .filter(|c| c.component_type == ICalendarComponentType::VAlarm)
        .filter_map(|alarm| {
            let found = trigger(alarm);
            if found.is_none() {
                tracing::debug!("skipping a VALARM with an unreadable TRIGGER");
            }
            found
        })
        .collect()
}

/// The reminders of the series' master that the editor shows: relative to the
/// start, in the sign of the TRIGGER (negative is before).
pub fn reminders(raw: &str) -> Result<Vec<Duration>, DomainError> {
    let ical = parse_for_expansion(raw)?;
    Ok(master(&ical)
        .map(|(_, event)| triggers(&ical, event))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|t| match t {
            Trigger::Relative {
                offset,
                from_end: false,
            } => Some(offset),
            _ => None,
        })
        .collect())
}

/// The reminders of the occurrences overlapping `[from, to)`. An all-day
/// event starts at midnight in `zone`, not in UTC, so that Google's default
/// "9:00 the day before" is 9:00 for the person who set it.
pub fn alarms<Z: TimeZone>(
    raw: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    zone: &Z,
) -> Result<Vec<OccurrenceAlarms>, DomainError> {
    let ical = parse_for_expansion(raw)?;
    let master_triggers = master(&ical).map(|(_, event)| triggers(&ical, event));
    let all: Vec<_> = ical
        .expand_dates(Utc, EXPAND_LIMIT)
        .events
        .into_iter()
        .filter_map(|event| {
            let comp = ical.components.get(event.comp_id as usize)?;
            let (start, end) = span(event.start, event.end);
            (comp.component_type == ICalendarComponentType::VEvent).then_some((comp, start, end))
        })
        .collect();

    Ok(all
        .iter()
        .filter(|&&(_, start, end)| overlaps(start, end, from, to))
        .map(|&(comp, start, end)| {
            let own = triggers(&ical, comp);
            // RFC 5545: an override without alarms of its own has the master's.
            let set = if own.is_empty() {
                master_triggers.as_deref().unwrap_or_default()
            } else {
                &own
            };
            let all_day = is_all_day(comp);
            let alarms = set
                .iter()
                .filter_map(|t| match *t {
                    Trigger::Relative { offset, from_end } => {
                        let anchor = if from_end { end } else { start };
                        let fire_at = if all_day {
                            // Local wall-clock arithmetic keeps "09:00" on a DST day.
                            read_in_zone(zone, anchor + offset)?
                        } else {
                            anchor + offset
                        };
                        Some(Alarm {
                            fire_at,
                            key: format!(
                                "{}{}",
                                offset.num_seconds(),
                                if from_end { "E" } else { "S" }
                            ),
                        })
                    }
                    // A one-shot: it belongs to the first occurrence it precedes.
                    Trigger::At(at) => {
                        let first = all.iter().map(|&(_, s, _)| s).filter(|&s| s >= at).min();
                        (first == Some(start)).then(|| Alarm {
                            fire_at: at,
                            key: format!("@{}", at.timestamp()),
                        })
                    }
                })
                .collect();
            OccurrenceAlarms {
                start,
                end: if all_day {
                    read_in_zone(zone, end).unwrap_or(end)
                } else {
                    end
                },
                alarms,
                meeting_url: meeting_url(comp),
            }
        })
        .collect())
}

/// The wall-clock time `at` reads in UTC (all-day events are stored as
/// midnight UTC of their date), taken as a time in `zone`.
fn read_in_zone<Z: TimeZone>(zone: &Z, at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    Some(
        zone.from_local_datetime(&at.naive_utc())
            .earliest()?
            .with_timezone(&Utc),
    )
}

/// A web link to join the call: the conference property, the event's URL, or
/// a known video-call link written in its location or description.
fn meeting_url(event: &ICalendarComponent) -> Option<String> {
    let conference = ICalendarProperty::Other("X-GOOGLE-CONFERENCE".into());
    [conference, ICalendarProperty::Url]
        .iter()
        .filter_map(|p| text(event, p))
        .find(|url| is_web(url))
        .or_else(|| {
            [ICalendarProperty::Location, ICalendarProperty::Description]
                .iter()
                .filter_map(|p| text(event, p))
                .find_map(|t| meeting_link(&t))
        })
}

fn is_web(url: &str) -> bool {
    Url::parse(url).is_ok_and(|u| matches!(u.scheme(), "https" | "http"))
}

fn meeting_link(text: &str) -> Option<String> {
    text.split(|c: char| c.is_whitespace() || "<>\"'()".contains(c))
        .filter(|word| word.starts_with("https://"))
        .map(|word| word.trim_end_matches(['.', ',', ';']))
        .find(|word| {
            Url::parse(word).is_ok_and(|u| {
                u.host_str().is_some_and(|host| {
                    MEETING_HOSTS
                        .iter()
                        .any(|h| host == *h || host.ends_with(&format!(".{h}")))
                })
            })
        })
        .map(str::to_owned)
}

/// Replaces the master's relative-to-start VALARMs with `reminders`. Absolute
/// and end-relative ones, and the overrides' own, are not the editor's and
/// stay as written.
pub(super) fn set_reminders(doc: &mut Doc, scope: &std::ops::Range<usize>, reminders: &[Duration]) {
    let editable: Vec<_> = doc
        .components("VALARM")
        .into_iter()
        .filter(|alarm| alarm.start > scope.start && alarm.end <= scope.end)
        .filter(|alarm| doc.first(alarm, "TRIGGER").is_some_and(relative_to_start))
        .collect();
    // Back to front, so the ranges still to remove are not shifted.
    for alarm in editable.iter().rev() {
        doc.0.drain(alarm.start - 1..=alarm.end);
    }
    let end = scope.end
        - editable
            .iter()
            .map(|a| a.end + 1 - (a.start - 1))
            .sum::<usize>();
    let lines = reminders
        .iter()
        .flat_map(|r| alarm_lines(*r))
        .map(Line::parse);
    doc.0.splice(end..end, lines);
}

fn relative_to_start(trigger: &Line) -> bool {
    let value = trigger.value();
    value.trim_start_matches(['+', '-']).starts_with(['P', 'p'])
        && !trigger
            .params()
            .to_ascii_uppercase()
            .contains("RELATED=END")
}

fn alarm_lines(before: Duration) -> [String; 6] {
    let seconds = before.num_seconds().unsigned_abs();
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    let time: String = [
        (rest / 3_600, 'H'),
        (rest % 3_600 / 60, 'M'),
        (rest % 60, 'S'),
    ]
    .into_iter()
    .filter(|&(n, _)| n > 0)
    .map(|(n, unit)| format!("{n}{unit}"))
    .collect();
    let text = format!(
        "{}P{}{}",
        if before < Duration::zero() { "-" } else { "" },
        if days > 0 {
            format!("{days}D")
        } else {
            String::new()
        },
        match (days, time.is_empty()) {
            (_, false) => format!("T{time}"),
            (0, true) => "T0S".to_owned(),
            _ => String::new(),
        }
    );
    [
        "BEGIN:VALARM".into(),
        content_line("UID", "", &Uuid::now_v7().to_string()),
        content_line("TRIGGER", "", &text),
        "ACTION:DISPLAY".into(),
        "DESCRIPTION:Reminder".into(),
        "END:VALARM".into(),
    ]
}
