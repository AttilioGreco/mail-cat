use super::*;
use crate::commands::CommandResult;
use mc_domain::formats::{EventEdit, EventWhen};

const CAL: &str = "/cal/";

fn ics(uid: &str, summary: &str, last_modified: Option<&str>) -> String {
    let modified = last_modified
        .map(|m| format!("LAST-MODIFIED:{m}\r\n"))
        .unwrap_or_default();
    format!(
        "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//t//\r\nBEGIN:VEVENT\r\nUID:{uid}\r\n\
         DTSTAMP:20260101T000000Z\r\nDTSTART:20261110T090000Z\r\nDTEND:20261110T100000Z\r\n\
         SUMMARY:{summary}\r\n{modified}END:VEVENT\r\nEND:VCALENDAR\r\n"
    )
}

fn env_with_calendar() -> Env {
    let env = env();
    env.fake
        .add_calendar(calendar(CAL, vec![CalendarComponent::Event]));
    env.fake
        .add_calendar(calendar("/todo/", vec![CalendarComponent::Todo]));
    env
}

async fn pending(env: &Env) -> usize {
    env.repo
        .pending_events(&env.account, &CollectionId::new(CAL))
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn events_sync_both_ways_and_task_calendars_are_left_alone() {
    let env = env_with_calendar();
    env.fake.write("/cal/e.ics", &ics("e", "Standup", None));
    env.sync().await;
    let href = Href::new("/cal/e.ics");
    let event = env
        .repo
        .get_event(&env.account, &href)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.fields.unwrap().summary, "Standup");
    assert_eq!(env.fake.calls_to("changes /todo/"), 0);
    assert!(env.cursor("caldav:/cal/").await.is_some());

    env.handler
        .handle_update_event(
            env.account,
            href.clone(),
            EventEdit {
                summary: Some("Retro".into()),
                ..EventEdit::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..200 {
        if pending(&env).await == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let sent = env.fake.raw("/cal/e.ics").unwrap();
    assert!(
        sent.contains("SUMMARY:Retro") && sent.contains("SEQUENCE:1"),
        "{sent}"
    );

    let when = EventWhen::At("2026-11-12T09:00:00Z".parse().unwrap());
    let end = EventWhen::At("2026-11-12T10:00:00Z".parse().unwrap());
    let CommandResult::CalendarEventAdded { href: made } = env
        .handler
        .handle_add_event(
            env.account,
            CollectionId::new(CAL),
            "Lunch".into(),
            (when, end),
            (None, None),
        )
        .await
        .unwrap()
    else {
        panic!("expected the href");
    };
    for _ in 0..200 {
        if env.fake.raw(made.as_str()).is_some() && pending(&env).await == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        env.fake
            .raw(made.as_str())
            .unwrap()
            .contains("SUMMARY:Lunch")
    );

    env.handler
        .handle_remove_event(env.account, href)
        .await
        .unwrap();
    for _ in 0..200 {
        if env.fake.raw("/cal/e.ics").is_none() && pending(&env).await == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(env.fake.hrefs(), [made.to_string()]);
    env.sync().await;
    assert_eq!(env.fake.hrefs(), [made.to_string()]);
}

#[tokio::test]
async fn a_newer_server_event_wins_and_the_local_edit_is_counted() {
    let env = env_with_calendar();
    env.fake.write("/cal/e.ics", &ics("e", "Standup", None));
    env.sync().await;
    env.fake
        .write("/cal/e.ics", &ics("e", "Remote", Some("20990101T000000Z")));

    // Stored by hand, not pushed: the pass does that.
    let href = Href::new("/cal/e.ics");
    let key = Key::event(env.account, &href);
    let event = env
        .repo
        .get_event(&env.account, &href)
        .await
        .unwrap()
        .unwrap();
    let raw = formats::edit_event(
        &event.raw,
        &EventEdit {
            summary: Some("Local".into()),
            ..EventEdit::default()
        },
        chrono::Utc::now(),
    )
    .unwrap();
    let (_, events) = resource_events(
        Kind::Calendar,
        Some(&key),
        event.binding,
        &raw,
        Origin::Local,
    )
    .unwrap();
    env.svc.append(&key, None, events).await.unwrap();
    env.sync().await;

    assert!(
        env.fake
            .raw("/cal/e.ics")
            .unwrap()
            .contains("SUMMARY:Remote")
    );
    assert_eq!(env.lost().await, 1);
    let held = env
        .repo
        .get_event(&env.account, &href)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.fields.unwrap().summary, "Remote");
    assert_eq!(pending(&env).await, 0);
}
