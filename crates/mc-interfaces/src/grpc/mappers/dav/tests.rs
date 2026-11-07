use super::*;
use chrono::TimeZone;
use mc_domain::{AccountId, CollectionId, ContactId, Etag, Href, RemoteBinding};

fn binding() -> RemoteBinding {
    RemoteBinding {
        account: AccountId::new(),
        collection: CollectionId::new("/book/"),
        href: Href::new("/book/a.vcf"),
        etag: Some(Etag::new("e1")),
        remote_modified: None,
    }
}

#[test]
fn remote_contact_carries_extracted_fields_raw_and_sync_state() {
    let raw = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:u\r\nFN:Ada Lovelace\r\n\
               EMAIL:ada@example.com\r\nTEL:+39 055 1\r\nORG:Analytical\r\nEND:VCARD\r\n";
    let contact = RemoteContact {
        id: ContactId::new(),
        binding: binding(),
        raw: raw.into(),
        dirty: true,
        push_error: Some("403".into()),
    };

    let proto = remote_contact_to_proto(&contact);

    assert_eq!(proto.display_name, "Ada Lovelace");
    assert_eq!(proto.emails, ["ada@example.com"]);
    assert_eq!(proto.phones, ["+39 055 1"]);
    assert_eq!(proto.organization.as_deref(), Some("Analytical"));
    assert_eq!(proto.raw, raw);
    assert_eq!(proto.address_book, "/book/");
    assert!(proto.dirty);
    assert_eq!(proto.push_failed.as_deref(), Some("403"));
}

#[test]
fn unparsable_card_is_still_listed_with_its_raw_text() {
    let contact = RemoteContact {
        id: ContactId::new(),
        binding: binding(),
        raw: "garbage".into(),
        dirty: false,
        push_error: None,
    };
    let proto = remote_contact_to_proto(&contact);
    assert_eq!(proto.raw, "garbage");
    assert!(proto.emails.is_empty());
}

#[test]
fn contact_fields_unset_stay_unset_and_empty_clears() {
    let fields = contact_fields_from_proto(proto::ContactFields {
        display_name: Some("Ada".into()),
        emails: Some(proto::StringList {
            items: vec!["ada@example.com".into(), "a@b.org".into()],
        }),
        phones: None,
        organization: Some(String::new()),
        notes: None,
    })
    .unwrap();

    assert_eq!(fields.display_name.as_deref(), Some("Ada"));
    let emails = fields.emails.unwrap();
    assert!(emails[0].is_primary && !emails[1].is_primary);
    assert_eq!(fields.phones, None);
    assert_eq!(fields.organization, Some(None));
    assert_eq!(fields.notes, None);
}

#[test]
fn contact_fields_refuse_a_bad_address() {
    let bad = proto::ContactFields {
        emails: Some(proto::StringList {
            items: vec!["not an address".into()],
        }),
        ..Default::default()
    };
    assert!(contact_fields_from_proto(bad).is_err());
}

#[test]
fn event_times_are_dates_or_instants() {
    assert_eq!(
        event_when_from_str("2026-11-10").unwrap(),
        EventWhen::Date(NaiveDate::from_ymd_opt(2026, 11, 10).unwrap())
    );
    assert_eq!(
        event_when_from_str("2026-11-10T10:00:00+01:00").unwrap(),
        EventWhen::At(Utc.with_ymd_and_hms(2026, 11, 10, 9, 0, 0).unwrap())
    );
    assert!(event_when_from_str("tomorrow").is_err());
}

#[test]
fn event_edit_empty_location_clears_and_unset_keeps() {
    let edit = event_edit_from_proto(&proto::UpdateEventRequest {
        location: Some(String::new()),
        description: Some("Agenda".into()),
        start: Some("2026-11-10".into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(edit.location, Some(None));
    assert_eq!(edit.description, Some(Some("Agenda".into())));
    assert_eq!(edit.summary, None);
    assert!(edit.start.is_some() && edit.end.is_none());
}

#[test]
fn occurrence_and_calendar_map_their_flags() {
    let start = Utc.with_ymd_and_hms(2026, 11, 10, 9, 0, 0).unwrap();
    let occurrence = Occurrence {
        calendar: CollectionId::new("/cal/"),
        href: Href::new("/cal/e.ics"),
        event: EventFields {
            summary: "Standup".into(),
            start,
            end: start + chrono::Duration::hours(1),
            all_day: false,
            recurring: true,
            location: None,
            status: Some(mc_domain::EventStatus::Confirmed),
            description: Some("Daily".into()),
        },
        sync: mc_domain::EventSync {
            dirty: true,
            push_failed: Some("412".into()),
        },
    };
    let proto = occurrence_to_proto(&occurrence);
    assert_eq!(proto.start.unwrap().seconds, start.timestamp());
    assert!(proto.recurring);
    assert_eq!(proto.status.as_deref(), Some("confirmed"));
    assert_eq!(proto.description.as_deref(), Some("Daily"));
    assert!(proto.dirty);
    assert_eq!(proto.push_failed.as_deref(), Some("412"));

    let calendar = Calendar {
        account: AccountId::new(),
        collection: CollectionId::new("/cal/"),
        name: "Work".into(),
        color: None,
        timezone: None,
        read_only: true,
        ownership: Ownership::Shared,
        components: vec![mc_domain::CalendarComponent::Todo],
    };
    let proto = calendar_to_proto(&calendar);
    assert_eq!(proto.ownership, proto::CalendarOwnership::Shared as i32);
    assert!(proto.read_only && !proto.holds_events);
}
