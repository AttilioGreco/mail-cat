use chrono::{DateTime, NaiveDate, TimeZone, Utc};

use super::*;
use crate::model::contact::{ContactEmail, ContactEmailLabel};
use crate::value_objects::email_address::EmailAddress;

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

fn unfolded(text: &str) -> String {
    text.replace("\r\n ", "")
}

const CARD: &str = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Mario Rossi\r\nN:Rossi;Mario;;;\r\n\
    item1.EMAIL;TYPE=INTERNET,WORK,pref:mario@work.it\r\nEMAIL;TYPE=HOME:mario@home.it\r\n\
    TEL;TYPE=CELL:+39 333 1\r\nORG:Acme\\, Inc;Sales\r\nNOTE:one\\ntwo\r\n\
    X-CUSTOM;X-PARAM=1:kept verbatim\r\n\
    PHOTO;ENCODING=b;TYPE=JPEG:/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgF\r\n \
    BQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/\r\n\
    REV:20200101T000000Z\r\nEND:VCARD\r\n";

#[test]
fn vcard_fields_are_extracted() {
    let card = parse_vcard(CARD).unwrap();
    assert_eq!(card.display_name, "Mario Rossi");
    assert_eq!(card.organization.as_deref(), Some("Acme, Inc"));
    assert_eq!(card.notes.as_deref(), Some("one\ntwo"));
    assert_eq!(card.phones, ["+39 333 1"]);
    assert_eq!(card.revised, Some(at("2020-01-01T00:00:00Z")));
    let emails: Vec<_> = card
        .emails
        .iter()
        .map(|e| (e.address.as_str(), e.label.clone(), e.is_primary))
        .collect();
    assert_eq!(
        emails,
        [
            ("mario@work.it", ContactEmailLabel::Work, true),
            ("mario@home.it", ContactEmailLabel::Home, false)
        ]
    );
}

#[test]
fn vcard_edit_keeps_unknown_properties_and_stamps_rev() {
    let now = at("2026-11-07T10:00:00Z");
    let edit = ContactEdit {
        display_name: Some("Mario Bianchi".into()),
        emails: Some(vec![
            ContactEmail::new(
                EmailAddress::new("mario@work.it").unwrap(),
                ContactEmailLabel::Work,
                true,
            ),
            ContactEmail::new(
                EmailAddress::new("new@example.com").unwrap(),
                ContactEmailLabel::Home,
                false,
            ),
        ]),
        organization: Some(None),
        ..ContactEdit::default()
    };
    let edited = edit_vcard(CARD, &edit, now).unwrap();

    // Untouched lines come back byte for byte, the folded PHOTO included.
    for kept in [
        "N:Rossi;Mario;;;\r\n",
        "item1.EMAIL;TYPE=INTERNET,WORK,pref:mario@work.it\r\n",
        "TEL;TYPE=CELL:+39 333 1\r\n",
        "X-CUSTOM;X-PARAM=1:kept verbatim\r\n",
        "NOTE:one\\ntwo\r\n",
        "PHOTO;ENCODING=b;TYPE=JPEG:/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgF\r\n \
         BQQEBQoHBwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/\r\n",
    ] {
        assert!(edited.contains(kept), "lost {kept:?} in {edited}");
    }
    assert!(edited.contains("FN:Mario Bianchi\r\n"));
    assert!(!edited.contains("mario@home.it") && !edited.contains("ORG:"));
    assert!(edited.contains("EMAIL;TYPE=home:new@example.com\r\n"));
    assert!(edited.contains("REV:20261107T100000Z\r\n"));
    assert!(!edited.contains("20200101"));

    let card = parse_vcard(&edited).unwrap();
    assert_eq!(card.display_name, "Mario Bianchi");
    assert_eq!(card.organization, None);
    assert_eq!(card.revised, Some(now));
}

#[test]
fn vcard_without_an_edit_changes_only_rev() {
    let edited = edit_vcard(CARD, &ContactEdit::default(), at("2026-11-07T10:00:00Z")).unwrap();
    assert_eq!(edited.replace("20261107T100000Z", "20200101T000000Z"), CARD);
}

#[test]
fn long_values_are_folded_at_75_octets() {
    let note = "è".repeat(80);
    let edit = ContactEdit {
        notes: Some(Some(note.clone())),
        ..ContactEdit::default()
    };
    let card = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:A\r\nEND:VCARD\r\n";
    let edited = edit_vcard(card, &edit, Utc::now()).unwrap();
    assert!(edited.lines().all(|l| l.len() <= 75));
    assert_eq!(parse_vcard(&edited).unwrap().notes, Some(note));
}

const SERIES: &str = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//t//\r\n\
    BEGIN:VTIMEZONE\r\nTZID:Europe/Rome\r\nBEGIN:STANDARD\r\nDTSTART:19701025T030000\r\n\
    TZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nRRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\n\
    END:STANDARD\r\nEND:VTIMEZONE\r\n\
    BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSEQUENCE:2\r\n\
    DTSTART;TZID=Europe/Rome:20261024T100000\r\nDTEND;TZID=Europe/Rome:20261024T110000\r\n\
    RRULE:FREQ=DAILY;COUNT=5\r\nEXDATE;TZID=Europe/Rome:20261026T100000\r\n\
    SUMMARY:Standup\r\nLOCATION:Room 1\r\nX-APPLE-STRUCTURED:keep me\r\n\
    BEGIN:VALARM\r\nACTION:DISPLAY\r\nSUMMARY:alarm\r\nTRIGGER:-PT10M\r\nEND:VALARM\r\n\
    END:VEVENT\r\n\
    BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\n\
    RECURRENCE-ID;TZID=Europe/Rome:20261025T100000\r\n\
    DTSTART;TZID=Europe/Rome:20261025T140000\r\nDTEND;TZID=Europe/Rome:20261025T150000\r\n\
    SUMMARY:Standup (moved)\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

#[test]
fn recurrence_with_override_and_exdate_across_a_dst_change() {
    // Rome leaves DST at 03:00 on 2026-10-25: 10:00 local is 08:00Z on the
    // 24th (CEST) and 09:00Z from the 25th (CET).
    let found = occurrences(
        SERIES,
        at("2026-10-23T00:00:00Z"),
        at("2026-11-30T00:00:00Z"),
    )
    .unwrap();
    let seen: Vec<_> = found
        .iter()
        .map(|o| (o.start, o.end, o.summary.as_str()))
        .collect();
    assert_eq!(
        seen,
        [
            (
                at("2026-10-24T08:00:00Z"),
                at("2026-10-24T09:00:00Z"),
                "Standup"
            ),
            // Overridden to 14:00 CET; the rule's own 25th is replaced.
            (
                at("2026-10-25T13:00:00Z"),
                at("2026-10-25T14:00:00Z"),
                "Standup (moved)"
            ),
            // The 26th is an EXDATE.
            (
                at("2026-10-27T09:00:00Z"),
                at("2026-10-27T10:00:00Z"),
                "Standup"
            ),
            (
                at("2026-10-28T09:00:00Z"),
                at("2026-10-28T10:00:00Z"),
                "Standup"
            ),
        ]
    );
    assert!(found.iter().all(|o| o.recurring));
}

#[test]
fn occurrences_are_limited_to_the_range() {
    let found = occurrences(
        SERIES,
        at("2026-10-27T00:00:00Z"),
        at("2026-10-28T00:00:00Z"),
    )
    .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].start, at("2026-10-27T09:00:00Z"));
}

#[test]
fn event_fields_come_from_the_master() {
    let parsed = parse_event(SERIES).unwrap();
    let fields = parsed.fields.unwrap();
    assert_eq!(parsed.uid.as_deref(), Some("series"));
    assert_eq!(fields.summary, "Standup");
    assert_eq!(fields.location.as_deref(), Some("Room 1"));
    assert_eq!(fields.start, at("2026-10-24T08:00:00Z"));
    assert_eq!(fields.end, at("2026-10-24T09:00:00Z"));
    assert!(fields.recurring && !fields.all_day);
}

#[test]
fn all_day_events_start_at_midnight_utc() {
    let raw = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:d\r\n\
        DTSTART;VALUE=DATE:20261101\r\nSUMMARY:Holiday\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
    let fields = parse_event(raw).unwrap().fields.unwrap();
    assert!(fields.all_day);
    assert_eq!(fields.start, at("2026-11-01T00:00:00Z"));
    assert_eq!(fields.end, at("2026-11-02T00:00:00Z"));
}

#[test]
fn todos_are_opaque() {
    let raw = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTODO\r\nUID:t\r\nSUMMARY:Buy milk\r\n\
        END:VTODO\r\nEND:VCALENDAR\r\n";
    let parsed = parse_event(raw).unwrap();
    assert_eq!(parsed.fields, None);
    assert!(
        occurrences(raw, at("2026-01-01T00:00:00Z"), at("2027-01-01T00:00:00Z"))
            .unwrap()
            .is_empty()
    );
    assert!(edit_event(raw, &EventEdit::default(), Utc::now()).is_err());
}

#[test]
fn event_edit_changes_the_master_and_keeps_everything_else() {
    let now = at("2026-11-07T10:00:00Z");
    let edit = EventEdit {
        summary: Some("Daily sync, v2".into()),
        location: Some(None),
        // 15:00 CET: the zone of the old DTSTART is kept, not pinned to UTC.
        start: Some(EventWhen::At(at("2026-10-24T13:00:00Z"))),
        end: Some(EventWhen::At(at("2026-10-24T14:00:00Z"))),
    };
    let edited = unfolded(&edit_event(SERIES, &edit, now).unwrap());

    for kept in [
        "X-APPLE-STRUCTURED:keep me\r\n",
        "BEGIN:VTIMEZONE\r\nTZID:Europe/Rome\r\n",
        "RRULE:FREQ=DAILY;COUNT=5\r\n",
        "EXDATE;TZID=Europe/Rome:20261026T100000\r\n",
        "BEGIN:VALARM\r\nACTION:DISPLAY\r\nSUMMARY:alarm\r\n",
        "SUMMARY:Standup (moved)\r\nEND:VEVENT\r\n",
    ] {
        assert!(edited.contains(kept), "lost {kept:?} in {edited}");
    }
    assert!(edited.contains("SUMMARY:Daily sync\\, v2\r\n"));
    assert!(!edited.contains("LOCATION"));
    assert!(edited.contains("DTSTART;TZID=Europe/Rome:20261024T150000\r\n"));
    assert!(edited.contains("DTEND;TZID=Europe/Rome:20261024T160000\r\n"));
    assert!(edited.contains("SEQUENCE:3\r\n"));
    assert!(edited.contains("LAST-MODIFIED:20261107T100000Z\r\n"));
    // Stamped on the master only: the override keeps its own DTSTAMP.
    assert_eq!(edited.matches("DTSTAMP:20261107T100000Z").count(), 1);
    assert_eq!(edited.matches("DTSTAMP:20260101T000000Z").count(), 1);

    let parsed = parse_event(&edited).unwrap();
    assert_eq!(parsed.last_modified, Some(now));
    assert_eq!(parsed.fields.unwrap().start, at("2026-10-24T13:00:00Z"));
}

#[test]
fn event_edit_can_make_an_event_all_day() {
    let raw = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:d\r\n\
        DTSTART:20261101T090000Z\r\nDURATION:PT1H\r\nSUMMARY:x\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
    let day = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
    let edit = EventEdit {
        start: Some(EventWhen::Date(day)),
        end: Some(EventWhen::Date(day.succ_opt().unwrap())),
        ..EventEdit::default()
    };
    let edited = edit_event(raw, &edit, Utc.timestamp_opt(0, 0).unwrap()).unwrap();
    assert!(!edited.contains("DURATION"));
    assert!(edited.contains("DTSTART;VALUE=DATE:20261101\r\n"));
    assert!(edited.contains("SEQUENCE:1\r\n"));
    assert!(parse_event(&edited).unwrap().fields.unwrap().all_day);
}

#[test]
fn a_series_whose_first_instance_is_excluded_starts_at_its_next_one() {
    let raw = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:split\r\n\
        DTSTART:20251111T140000Z\r\nDTEND:20251111T150000Z\r\n\
        RRULE:FREQ=WEEKLY;COUNT=3\r\nEXDATE:20251111T140000Z\r\n\
        SUMMARY:Weekly\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
    let fields = parse_event(raw).unwrap().fields.unwrap();
    assert_eq!(fields.start, at("2025-11-18T14:00:00Z"));
}

#[test]
fn a_series_with_no_occurrence_left_is_kept_without_fields() {
    let raw = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:gone\r\n\
        DTSTART:20251111T140000Z\r\nDTEND:20251111T150000Z\r\n\
        RRULE:FREQ=WEEKLY;COUNT=1\r\nEXDATE:20251111T140000Z\r\n\
        SUMMARY:Gone\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
    let parsed = parse_event(raw).unwrap();
    assert_eq!(parsed.uid.as_deref(), Some("gone"));
    assert!(parsed.fields.is_none());
}
