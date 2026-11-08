//! Calendar reminders: the VALARMs of the user's own events, shown as desktop
//! notifications beside the mail ones, even with the UI closed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Local, TimeDelta, Utc};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use tokio_util::sync::CancellationToken;

use mc_application::AuthGates;
use mc_domain::{
    AccountId, CollectionId, DavReadRepository, DomainError, EventFields, EventStatus, Href,
    Occurrence, Ownership, formats,
};
use mc_infrastructure::ReminderLog;

use crate::notifications::{NotificationCenter, NotifiedEvent, run_event_action};

const TICK: Duration = Duration::from_secs(60);
/// A `-P1W` trigger rings a week ahead of its event.
const LOOKAHEAD: TimeDelta = TimeDelta::days(8);
/// An all-day event of a zone behind UTC is still on after its UTC midnight.
const LOOKBACK: TimeDelta = TimeDelta::days(1);

/// A reminder that rang now: what the notification shows and opens.
#[derive(Debug, Clone)]
pub struct Due {
    pub account: AccountId,
    pub calendar: CollectionId,
    /// The notification's subtitle, where the platform has one.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub calendar_name: String,
    pub href: Href,
    pub event: EventFields,
    pub meeting_url: Option<String>,
}

pub struct Reminders {
    dav: Arc<dyn DavReadRepository>,
    log: ReminderLog,
}

impl Reminders {
    pub fn new(dav: Arc<dyn DavReadRepository>, log: ReminderLog) -> Self {
        Self { dav, log }
    }

    /// Rings every reminder of the account that is due at `now`, once. One
    /// whose time passed while the server was off still rings if its event
    /// has not ended. `now` is a parameter so tests can drive the clock.
    pub async fn scan(
        &self,
        account: AccountId,
        now: DateTime<Utc>,
    ) -> Result<Vec<Due>, DomainError> {
        let calendars: HashMap<CollectionId, String> = self
            .dav
            .list_calendars(&account)
            .await?
            .into_iter()
            .filter(|c| c.ownership == Ownership::Own && c.holds_events())
            .map(|c| (c.collection, c.name))
            .collect();
        if calendars.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<_> = calendars.keys().cloned().collect();
        let (from, to) = (now - LOOKBACK, now + LOOKAHEAD);
        let mut by_href: HashMap<Href, Vec<Occurrence>> = HashMap::new();
        for occurrence in self.dav.events_in_range(&account, &ids, from, to).await? {
            by_href
                .entry(occurrence.href.clone())
                .or_default()
                .push(occurrence);
        }

        let mut due = Vec::new();
        for (href, occurrences) in by_href {
            let Some(event) = self.dav.get_event(&account, &href).await? else {
                continue;
            };
            // Expanding a years-old daily series every tick is the whole cost
            // of a scan; most events carry no alarm at all.
            if !event.raw.contains("BEGIN:VALARM") {
                continue;
            }
            let rings = match formats::alarms(&event.raw, from, to, &Local) {
                Ok(rings) => rings,
                Err(e) => {
                    tracing::warn!(%href, "skipping the reminders of an unreadable event: {e}");
                    continue;
                }
            };
            for occurrence in occurrences {
                if occurrence.event.status == Some(EventStatus::Cancelled) {
                    continue;
                }
                let Some(ring) = rings.iter().find(|r| r.start == occurrence.event.start) else {
                    continue;
                };
                if ring.end <= now {
                    continue;
                }
                for alarm in ring.alarms.iter().filter(|a| a.fire_at <= now) {
                    let span = (ring.start, ring.end);
                    // Recorded before it is shown: a crash in between loses one
                    // reminder instead of ringing it again at every restart.
                    if !self
                        .log
                        .record(&account, &href, span, &alarm.key, now)
                        .await?
                    {
                        continue;
                    }
                    due.push(Due {
                        account,
                        calendar_name: calendars[&occurrence.calendar].clone(),
                        calendar: occurrence.calendar.clone(),
                        href: href.clone(),
                        event: occurrence.event.clone(),
                        meeting_url: ring.meeting_url.clone(),
                    });
                }
            }
        }
        Ok(due)
    }

    /// Scans every tick, the first one at start-up for what was missed. Only
    /// accounts whose calendar sync is enabled are looked at, so a mail-only
    /// account costs nothing.
    pub fn spawn(
        self: Arc<Self>,
        gates: Arc<AuthGates>,
        center: Arc<NotificationCenter>,
        ui_bin: String,
        shutdown: CancellationToken,
    ) {
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(TICK);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = ticks.tick() => {}
                }
                let now = Utc::now();
                for status in gates
                    .statuses()
                    .iter()
                    .filter(|s| s.dav.calendar.is_enabled())
                {
                    match self.scan(status.account_id, now).await {
                        Ok(due) => due.into_iter().for_each(|d| show(d, now, &center, &ui_bin)),
                        Err(e) => tracing::warn!("Calendar reminders scan failed: {e}"),
                    }
                }
                if let Err(e) = self.log.prune(now).await {
                    tracing::warn!("Pruning the fired reminders failed: {e}");
                }
            }
        });
    }
}

/// `Oggi 14:30–15:00`, `Domani, tutto il giorno`, then the location.
fn body(due: &Due, now: DateTime<Utc>) -> String {
    let event = &due.event;
    let (start, end) = if event.all_day {
        // Stored as midnight UTC of the date, which is the date itself.
        let date = event.start.date_naive();
        (
            date.and_time(Default::default()),
            date.and_time(Default::default()),
        )
    } else {
        (
            event.start.with_timezone(&Local).naive_local(),
            event.end.with_timezone(&Local).naive_local(),
        )
    };
    let today = now.with_timezone(&Local).date_naive();
    let day = match (start.date() - today).num_days() {
        0 => "Oggi".to_owned(),
        1 => "Domani".to_owned(),
        _ => start.format("%d/%m").to_string(),
    };
    let when = if event.all_day {
        format!("{day}, tutto il giorno")
    } else {
        format!("{day} {}–{}", start.format("%H:%M"), end.format("%H:%M"))
    };
    match &event.location {
        Some(location) => format!("{when}\n{location}"),
        None => when,
    }
}

fn show(due: Due, now: DateTime<Utc>, center: &Arc<NotificationCenter>, ui_bin: &str) {
    let encode = |text: &str| utf8_percent_encode(text, NON_ALPHANUMERIC).to_string();
    let target = NotifiedEvent {
        deep_link: format!(
            "{}/{}/{}/{}",
            due.account.as_uuid(),
            encode(due.calendar.as_str()),
            encode(due.href.as_str()),
            due.event.start.timestamp()
        ),
        meeting_url: due.meeting_url.clone(),
    };
    let (center, ui_bin) = (center.clone(), ui_bin.to_owned());
    tokio::task::spawn_blocking(move || {
        let mut notification = notify_rust::Notification::new();
        notification
            .summary(&due.event.summary)
            .body(&body(&due, now))
            .icon(&crate::wiring::app_icon_path())
            // The body click, which carries no button label.
            .action("default", "Apri")
            .action("open-event", "Apri evento");
        if target.meeting_url.is_some() {
            notification.action("join", "Unisciti");
        }
        #[cfg(target_os = "macos")]
        notification.subtitle(&due.calendar_name);
        let handle = match notification.show() {
            Ok(handle) => handle,
            Err(e) => {
                tracing::warn!("Desktop notification failed: {e}");
                return;
            }
        };
        #[cfg(target_os = "linux")]
        if center.is_listening() {
            center.track(handle.id(), crate::notifications::Notified::Event(target));
            return;
        }
        handle.wait_for_action(move |action| run_event_action(action, &target, &ui_bin));
    });
}

#[cfg(test)]
mod tests;
