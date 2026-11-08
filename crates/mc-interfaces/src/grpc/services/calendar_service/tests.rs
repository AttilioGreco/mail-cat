use super::*;
use crate::grpc::mappers::datetime_to_timestamp;
use crate::grpc::proto::{Reminder, ReminderList};
use crate::grpc::services::dav_fakes::{FakeDav, RecordingBus};
use chrono::{TimeZone, Utc};
use mc_application::bus::EventPublisher;
use mc_domain::DomainEvent;
use mc_domain::formats::EventWhen;
use mc_domain::{
    Calendar, CalendarComponent, CalendarEvent, EventFields, Occurrence, Ownership, RemoteBinding,
};

fn fields(summary: &str) -> EventFields {
    let start = Utc.with_ymd_and_hms(2026, 11, 10, 9, 0, 0).unwrap();
    EventFields {
        summary: summary.into(),
        start,
        end: start + chrono::Duration::hours(1),
        all_day: false,
        recurring: false,
        location: None,
        status: None,
        description: None,
    }
}

fn events() -> Arc<InProcessEventBus> {
    Arc::new(InProcessEventBus::new(64))
}

fn service(
    dav: FakeDav,
    result: CommandResult,
) -> (CalendarServiceImpl<RecordingBus>, Arc<RecordingBus>) {
    let bus = Arc::new(RecordingBus::answering(result));
    (
        CalendarServiceImpl::new(bus.clone(), Arc::new(dav), events(), Default::default()),
        bus,
    )
}

#[tokio::test]
async fn lists_calendars_with_their_flags() {
    let dav = FakeDav {
        calendars: vec![Calendar {
            account: AccountId::new(),
            collection: CollectionId::new("/cal/"),
            name: "Work".into(),
            color: Some("#ff0000".into()),
            timezone: Some("Europe/Rome".into()),
            read_only: false,
            ownership: Ownership::Own,
            components: vec![CalendarComponent::Event],
        }],
        ..Default::default()
    };
    let (svc, _) = service(dav, CommandResult::CalendarEventRemoved);
    let listed = svc
        .list_calendars(Request::new(ListCalendarsRequest {
            account_id: AccountId::new().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    let cal = &listed.calendars[0];
    assert_eq!(cal.name, "Work");
    assert_eq!(cal.timezone.as_deref(), Some("Europe/Rome"));
    assert!(cal.holds_events);
}

#[tokio::test]
async fn events_in_range_pass_the_window_and_calendars_through() {
    let dav = FakeDav {
        occurrences: vec![Occurrence {
            calendar: CollectionId::new("/cal/"),
            href: Href::new("/cal/e.ics"),
            event: fields("Standup"),
            sync: Default::default(),
        }],
        ..Default::default()
    };
    let dav = Arc::new(dav);
    let svc = CalendarServiceImpl::new(
        Arc::new(RecordingBus::answering(CommandResult::CalendarEventRemoved)),
        dav.clone(),
        events(),
        Default::default(),
    );
    let (from, to) = (
        Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2026, 12, 1, 0, 0, 0).unwrap(),
    );
    let listed = svc
        .list_events(Request::new(ListEventsRequest {
            account_id: AccountId::new().to_string(),
            calendars: vec!["/cal/".into()],
            from: Some(datetime_to_timestamp(&from)),
            to: Some(datetime_to_timestamp(&to)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.occurrences[0].summary, "Standup");
    assert_eq!(listed.occurrences[0].href, "/cal/e.ics");
    assert_eq!(
        *dav.asked_calendars.lock().unwrap(),
        [CollectionId::new("/cal/")]
    );
    assert_eq!(*dav.asked_from.lock().unwrap(), Some(from));

    let missing_window = svc
        .list_events(Request::new(ListEventsRequest::default()))
        .await;
    assert!(missing_window.is_err());
}

#[tokio::test]
async fn get_event_returns_raw_and_unknown_is_not_found() {
    let event = CalendarEvent {
        binding: RemoteBinding {
            account: AccountId::new(),
            collection: CollectionId::new("/cal/"),
            href: Href::new("/cal/e.ics"),
            etag: None,
            remote_modified: None,
        },
        raw: "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n".into(),
        fields: Some(fields("Lunch")),
    };
    let dav = FakeDav {
        event: Some(event),
        sync: mc_domain::EventSync {
            dirty: true,
            push_failed: Some("403".into()),
        },
        ..Default::default()
    };
    let (svc, _) = service(dav, CommandResult::CalendarEventRemoved);
    let request = || GetEventRequest {
        account_id: AccountId::new().to_string(),
        href: "/cal/e.ics".into(),
    };
    let detail = svc
        .get_event(Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(detail.raw, "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n");
    let first = detail.first.unwrap();
    assert_eq!(first.summary, "Lunch");
    assert!(first.dirty);
    assert_eq!(first.push_failed.as_deref(), Some("403"));

    let (empty, _) = service(FakeDav::default(), CommandResult::CalendarEventRemoved);
    let err = empty.get_event(Request::new(request())).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn create_update_and_delete_dispatch_the_commands() {
    let href = Href::new("/cal/new.ics");
    let (svc, bus) = service(
        FakeDav::default(),
        CommandResult::CalendarEventAdded { href: href.clone() },
    );
    let account_id = AccountId::new().to_string();

    let created = svc
        .create_event(Request::new(CreateEventRequest {
            description: None,
            account_id: account_id.clone(),
            calendar: "/cal/".into(),
            summary: "Dentist".into(),
            start: "2026-11-10".into(),
            end: "2026-11-11".into(),
            location: Some(String::new()),
            reminders: Some(ReminderList {
                reminders: vec![Reminder {
                    before_seconds: -600,
                }],
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.href, "/cal/new.ics");

    svc.update_event(Request::new(UpdateEventRequest {
        account_id: account_id.clone(),
        href: "/cal/new.ics".into(),
        summary: Some("Dentist 2".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    svc.delete_event(Request::new(DeleteEventRequest {
        account_id,
        href: "/cal/new.ics".into(),
    }))
    .await
    .unwrap();

    let seen = bus.seen.lock().unwrap();
    assert!(matches!(
        &seen[0],
        Command::AddCalendarEvent { calendar, summary, start: EventWhen::Date(_), location: None, reminders, .. }
            if calendar.as_str() == "/cal/" && summary == "Dentist"
                && *reminders == [chrono::Duration::minutes(-10)]
    ));
    assert!(matches!(
        &seen[1],
        Command::UpdateCalendarEvent { edit, .. }
            if edit.summary.as_deref() == Some("Dentist 2") && edit.reminders.is_none()
    ));
    assert!(matches!(&seen[2], Command::RemoveCalendarEvent { href: h, .. } if *h == href));
}

#[tokio::test]
async fn bad_event_time_is_refused_before_any_command() {
    let (svc, bus) = service(FakeDav::default(), CommandResult::CalendarEventRemoved);
    let err = svc
        .create_event(Request::new(CreateEventRequest {
            description: None,
            account_id: AccountId::new().to_string(),
            calendar: "/cal/".into(),
            summary: "x".into(),
            start: "tomorrow".into(),
            end: "2026-11-11".into(),
            location: None,
            reminders: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(bus.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn dav_status_counts_lost_conflicts_and_pending_changes() {
    let failed = |failure: Option<&str>| PendingPush {
        aggregate_id: "a".into(),
        binding: RemoteBinding {
            account: AccountId::new(),
            collection: CollectionId::new("/book/"),
            href: Href::new("/book/a.vcf"),
            etag: None,
            remote_modified: None,
        },
        change: mc_domain::PendingChange::Delete,
        edited_at: Utc::now(),
        failure: failure.map(Into::into),
        sequence: 1,
    };
    let dav = FakeDav {
        books: vec![mc_domain::AddressBook {
            account: AccountId::new(),
            collection: CollectionId::new("/book/"),
            name: "b".into(),
            description: None,
            read_only: false,
        }],
        pending_contacts: vec![failed(None), failed(Some("403"))],
        conflicts: 3,
        ..Default::default()
    };
    let (svc, _) = service(dav, CommandResult::CalendarEventRemoved);
    let status = svc
        .get_dav_status(Request::new(GetDavStatusRequest {
            account_id: AccountId::new().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (
            status.conflicts_lost,
            status.pending_changes,
            status.failed_changes
        ),
        (3, 2, 1)
    );
}

fn envelope(event: DomainEvent) -> EventEnvelope {
    EventEnvelope::new("agg".into(), event.aggregate_type(), 1, event)
}

#[tokio::test]
async fn dav_changes_are_coalesced_per_account_and_kind() {
    let bus = events();
    let svc = CalendarServiceImpl::new(
        Arc::new(RecordingBus::answering(CommandResult::CalendarEventRemoved)),
        Arc::new(FakeDav::default()),
        bus.clone(),
        Default::default(),
    );
    let mut stream = svc
        .watch_dav_changes(Request::new(WatchDavChangesRequest {}))
        .await
        .unwrap()
        .into_inner();
    let account = AccountId::new();
    for i in 0..40 {
        bus.publish(envelope(DomainEvent::AddressBookRemoved {
            account_id: account,
            collection: CollectionId::new(format!("/book{i}/")),
        }));
        bus.publish(envelope(DomainEvent::CalendarEventRemoved {
            account_id: account,
            href: Href::new(format!("/cal/{i}.ics")),
            origin: mc_domain::Origin::Remote,
        }));
    }
    // Not a dav event: ignored.
    bus.publish(envelope(DomainEvent::AccountSyncStarted {
        account_id: account,
    }));

    let mut seen = Vec::new();
    while let Ok(Some(change)) = tokio::time::timeout(
        Duration::from_millis(1500),
        tokio_stream::StreamExt::next(&mut stream),
    )
    .await
    {
        let change = change.unwrap();
        assert_eq!(change.account_id, account.to_string());
        seen.push(change.kind);
    }
    seen.sort();
    assert_eq!(seen, [DavKind::Contacts as i32, DavKind::Calendar as i32]);
}
