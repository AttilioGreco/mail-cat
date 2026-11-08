use chrono::TimeZone;
use mc_domain::DomainError;
use mc_infrastructure::{SqliteDb, SqliteEventStore, SqliteRepository};
use rusqlite::params;

use super::*;

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

fn ics(start: &str, end: &str, alarms: &[&str]) -> String {
    let alarms: String = alarms
        .iter()
        .map(|t| format!("BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:{t}\r\nEND:VALARM\r\n"))
        .collect();
    format!(
        "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:u\r\nDTSTART:{start}\r\n\
         DTEND:{end}\r\nSUMMARY:Standup\r\nLOCATION:Room 1\r\n{alarms}END:VEVENT\r\nEND:VCALENDAR\r\n"
    )
}

struct Env {
    reminders: Reminders,
    db: Arc<SqliteDb>,
    account: AccountId,
    _dir: tempfile::TempDir,
}

impl Env {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("t.db")).unwrap();
        let repository =
            SqliteRepository::new(db.clone(), Arc::new(SqliteEventStore::new(db.clone())));
        let env = Self {
            reminders: Reminders::new(Arc::new(repository), ReminderLog::new(db.clone())),
            db,
            account: AccountId::new(),
            _dir: dir,
        };
        env.calendar("/own/", "\"Own\"").await;
        env.calendar("/colleague/", "\"Subscription\"").await;
        env
    }

    async fn calendar(&self, collection: &'static str, ownership: &'static str) {
        let account = *self.account.as_uuid();
        self.db
            .write(move |conn| {
                conn.execute(
                    "INSERT INTO calendars (account_id, collection, name, read_only, ownership, components) \
                     VALUES (?1, ?2, 'Work', 0, ?3, '[\"Event\"]')",
                    params![account, collection, ownership],
                )
                .map(drop)
                .map_err(|e| DomainError::Repository(e.to_string()))
            })
            .await
            .unwrap();
    }

    async fn event(&self, calendar: &'static str, href: &'static str, raw: String) {
        let parsed = formats::parse_event(&raw).unwrap().fields.unwrap();
        let account = *self.account.as_uuid();
        let nanos = |t: DateTime<Utc>| t.timestamp_nanos_opt().unwrap();
        self.db
            .write(move |conn| {
                conn.execute(
                    "INSERT INTO calendar_events (account_id, href, collection, raw, summary, \
                     start_at, end_at, all_day, recurring) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0)",
                    params![account, href, calendar, raw, parsed.summary, nanos(parsed.start), nanos(parsed.end)],
                )
                .map(drop)
                .map_err(|e| DomainError::Repository(e.to_string()))
            })
            .await
            .unwrap();
    }

    async fn due(&self, now: &str) -> Vec<Due> {
        self.reminders.scan(self.account, at(now)).await.unwrap()
    }
}

#[tokio::test]
async fn a_reminder_rings_when_due_and_only_once() {
    let env = Env::new().await;
    env.event(
        "/own/",
        "/own/a.ics",
        ics("20261110T140000Z", "20261110T150000Z", &["-PT10M"]),
    )
    .await;

    assert!(env.due("2026-11-10T13:49:00Z").await.is_empty());
    let due = env.due("2026-11-10T13:50:30Z").await;
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].event.summary, "Standup");
    assert_eq!(due[0].calendar_name, "Work");
    // The next tick, and a later one, see it as already rung.
    assert!(env.due("2026-11-10T13:51:30Z").await.is_empty());
    assert!(env.due("2026-11-10T14:30:00Z").await.is_empty());
}

#[tokio::test]
async fn a_restart_does_not_ring_it_again() {
    let env = Env::new().await;
    env.event(
        "/own/",
        "/own/a.ics",
        ics("20261110T140000Z", "20261110T150000Z", &["-PT10M"]),
    )
    .await;
    assert_eq!(env.due("2026-11-10T13:55:00Z").await.len(), 1);

    let restarted = Reminders::new(env.reminders.dav.clone(), ReminderLog::new(env.db.clone()));
    assert!(
        restarted
            .scan(env.account, at("2026-11-10T13:56:00Z"))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_missed_reminder_rings_while_the_event_has_not_ended() {
    let env = Env::new().await;
    env.event(
        "/own/",
        "/own/running.ics",
        ics("20261110T140000Z", "20261110T150000Z", &["-PT10M"]),
    )
    .await;
    env.event(
        "/own/",
        "/own/over.ics",
        ics("20261110T120000Z", "20261110T133000Z", &["-PT10M"]),
    )
    .await;

    // The server was off from 13:00; it starts at 14:20.
    let due = env.due("2026-11-10T14:20:00Z").await;
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].href.as_str(), "/own/running.ics");
}

#[tokio::test]
async fn each_trigger_of_an_event_rings_and_an_edited_one_rings_again() {
    let env = Env::new().await;
    env.event(
        "/own/",
        "/own/a.ics",
        ics("20261110T140000Z", "20261110T150000Z", &["-PT10M", "-PT1H"]),
    )
    .await;
    assert_eq!(env.due("2026-11-10T13:30:00Z").await.len(), 1);
    assert_eq!(env.due("2026-11-10T13:55:00Z").await.len(), 1);

    let account = *env.account.as_uuid();
    let raw = ics(
        "20261110T140000Z",
        "20261110T150000Z",
        &["-PT10M", "-PT30M"],
    );
    env.db
        .write(move |conn| {
            conn.execute(
                "UPDATE calendar_events SET raw = ?2 WHERE account_id = ?1",
                params![account, raw],
            )
            .map(drop)
            .map_err(|e| DomainError::Repository(e.to_string()))
        })
        .await
        .unwrap();
    // -PT30M is new; the -PT10M that already rang stays quiet.
    assert_eq!(env.due("2026-11-10T13:56:00Z").await.len(), 1);
}

#[tokio::test]
async fn only_own_calendars_and_confirmed_events_ring() {
    let env = Env::new().await;
    env.event(
        "/colleague/",
        "/colleague/a.ics",
        ics("20261110T140000Z", "20261110T150000Z", &["-PT10M"]),
    )
    .await;
    let cancelled = ics("20261110T140000Z", "20261110T150000Z", &["-PT10M"])
        .replace("SUMMARY:", "STATUS:CANCELLED\r\nSUMMARY:");
    env.event("/own/", "/own/b.ics", cancelled).await;
    let account = *env.account.as_uuid();
    env.db
        .write(move |conn| {
            conn.execute(
                "UPDATE calendar_events SET status = '\"Cancelled\"' WHERE account_id = ?1",
                [account],
            )
            .map(drop)
            .map_err(|e| DomainError::Repository(e.to_string()))
        })
        .await
        .unwrap();

    assert!(env.due("2026-11-10T13:55:00Z").await.is_empty());
}

#[test]
fn the_body_says_when_and_where() {
    let due = |all_day, start, end| Due {
        account: AccountId::new(),
        calendar: CollectionId::new("/c/"),
        calendar_name: "Work".into(),
        href: Href::new("/c/a.ics"),
        event: EventFields {
            summary: "x".into(),
            start,
            end,
            all_day,
            recurring: false,
            location: Some("Room 1".into()),
            status: None,
            description: None,
        },
        meeting_url: None,
    };
    let now = Local
        .with_ymd_and_hms(2026, 11, 10, 9, 0, 0)
        .unwrap()
        .with_timezone(&Utc);
    let local = |h, m| {
        Local
            .with_ymd_and_hms(2026, 11, 10, h, m, 0)
            .unwrap()
            .with_timezone(&Utc)
    };
    assert_eq!(
        body(&due(false, local(14, 30), local(15, 0)), now),
        "Oggi 14:30–15:00\nRoom 1"
    );
    let tomorrow = at("2026-11-11T00:00:00Z");
    assert_eq!(
        body(&due(true, tomorrow, tomorrow), now),
        "Domani, tutto il giorno\nRoom 1"
    );
}
