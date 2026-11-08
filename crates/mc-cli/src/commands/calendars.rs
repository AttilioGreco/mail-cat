use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use clap::Subcommand;
use colored::Colorize;
use mc_client::{EventChanges, EventOccurrence, MailCatClient, NewEvent};

use super::accounts::find_account;
use super::{Format, print_json};
use crate::toon::{Toon, n, s};

#[derive(Subcommand)]
pub enum CalendarsCmd {
    /// List the calendars the server holds for an account
    List {
        /// Account name, email or ID
        account: String,
    },

    /// Add the calendars a colleague shares with the account
    Subscribe {
        /// Account name, email or ID
        account: String,
        /// The colleague's email address
        email: String,
    },

    /// Remove a calendar added with `subscribe`
    Unsubscribe {
        /// Account name, email or ID
        account: String,
        /// Calendar collection, as `list` prints it
        collection: String,
    },

    /// Sync health of an account's contacts and calendars: the DAV error (a
    /// Google token without the DAV scopes), local edits waiting for the
    /// server, and edits discarded by last-write-wins
    Status {
        /// Account name, email or ID
        account: String,
    },

    /// Sync contacts and calendars now, without waiting for the next period
    /// or the failure backoff; returns before the pass finishes
    Sync {
        /// Account name, email or ID (default: every account)
        account: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum EventsCmd {
    /// List event occurrences in a range, recurrence expanded
    List {
        /// Account name, email or ID
        account: String,
        /// Only this calendar collection (see `calendars list`); repeatable
        #[arg(long = "calendar")]
        calendars: Vec<String>,
        /// Start of the range: RFC 3339 or YYYY-MM-DD (default: now)
        #[arg(long)]
        from: Option<String>,
        /// End of the range (default: 30 days after --from)
        #[arg(long)]
        to: Option<String>,
    },

    /// Show an event with its raw iCalendar
    Show {
        /// Account name, email or ID
        account: String,
        /// Event href, as `list` prints it
        href: String,
    },

    /// Create an event in a calendar of the account
    Add {
        /// Account name, email or ID
        account: String,
        /// Calendar collection (see `calendars list`); there is no default
        #[arg(long)]
        calendar: String,
        #[arg(long)]
        summary: String,
        /// RFC 3339 instant, or YYYY-MM-DD for an all-day event
        #[arg(long)]
        start: String,
        /// Same format as --start; an all-day end is exclusive
        #[arg(long)]
        end: String,
        #[arg(long)]
        location: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },

    /// Edit an event; other properties and the recurrence are kept
    Update {
        /// Account name, email or ID
        account: String,
        href: String,
        #[arg(long)]
        summary: Option<String>,
        /// An empty location clears it
        #[arg(long)]
        location: Option<String>,
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        /// An empty description clears it
        #[arg(long)]
        description: Option<String>,
    },

    /// Delete an event (on the server too)
    Delete {
        /// Account name, email or ID
        account: String,
        href: String,
    },
}

pub async fn run_calendars(cmd: CalendarsCmd, client: MailCatClient, format: Format) -> Result<()> {
    match cmd {
        CalendarsCmd::List { account } => {
            let account = find_account(&client, &account).await?;
            let calendars = client.list_calendars(&account.id).await?;
            if format == Format::Json {
                return print_json(&calendars);
            }
            let rows: Vec<_> = calendars
                .iter()
                .map(|c| {
                    vec![
                        s(&c.collection),
                        s(&c.name),
                        s(c.color.as_deref().unwrap_or_default()),
                        s(c.timezone.as_deref().unwrap_or_default()),
                        n(c.read_only),
                        s(c.ownership),
                        n(c.holds_events),
                    ]
                })
                .collect();
            let fields = [
                "collection",
                "name",
                "color",
                "tz",
                "read_only",
                "ownership",
                "holds_events",
            ];
            Toon::default().table("calendars", &fields, &rows).print();
            Ok(())
        }
        CalendarsCmd::Subscribe { account, email } => {
            let account = find_account(&client, &account).await?;
            client.subscribe_calendar(&account.id, &email).await?;
            println!("Subscribed; the calendars appear after the next sync");
            Ok(())
        }
        CalendarsCmd::Unsubscribe {
            account,
            collection,
        } => {
            let account = find_account(&client, &account).await?;
            client
                .unsubscribe_calendar(&account.id, &collection)
                .await?;
            println!("Unsubscribed");
            Ok(())
        }
        CalendarsCmd::Sync { account } => {
            let id = match account {
                Some(wanted) => Some(find_account(&client, &wanted).await?.id),
                None => None,
            };
            client.sync_dav_now(id.as_deref()).await?;
            println!("Sync requested");
            Ok(())
        }
        CalendarsCmd::Status { account } => {
            let account = find_account(&client, &account).await?;
            let status = client.dav_status(&account.id).await?;
            let (contacts, calendar) = account
                .auth
                .map_or((None, None), |a| (Some(a.contacts), Some(a.calendar)));
            if format == Format::Json {
                return print_json(
                    &serde_json::json!({ "contacts": contacts, "calendar": calendar, "status": status }),
                );
            }
            let describe = |f: &Option<mc_client::FeatureStatus>| match f {
                Some(f) if !f.missing_scopes.is_empty() => {
                    format!("{} ({})", f.state.as_str(), f.missing_scopes.join(", "))
                }
                Some(f) => f.state.as_str().to_owned(),
                None => String::new(),
            };
            let mut t = Toon::default();
            t.field("contacts", s(&describe(&contacts)))
                .field("calendar", s(&describe(&calendar)))
                .field("pending_changes", n(status.pending_changes))
                .field("failed_changes", n(status.failed_changes))
                .field("conflicts_lost", n(status.conflicts_lost))
                .print();
            Ok(())
        }
    }
}

pub async fn run_events(cmd: EventsCmd, client: MailCatClient, format: Format) -> Result<()> {
    match cmd {
        EventsCmd::List {
            account,
            calendars,
            from,
            to,
        } => {
            let account = find_account(&client, &account).await?;
            let from = from.as_deref().map_or_else(|| Ok(Utc::now()), parse_time)?;
            let to = to
                .as_deref()
                .map_or_else(|| Ok(from + Duration::days(30)), parse_time)?;
            let found = client.list_events(&account.id, calendars, from, to).await?;
            if format == Format::Json {
                return print_json(&found);
            }
            print_occurrences(&found);
            Ok(())
        }
        EventsCmd::Show { account, href } => {
            let account = find_account(&client, &account).await?;
            let event = client.get_event(&account.id, &href).await?;
            if format == Format::Json {
                return print_json(&event);
            }
            let mut t = Toon::default();
            t.field("calendar", s(&event.calendar))
                .field("href", s(&event.href));
            if let Some(first) = &event.first {
                t.field("summary", s(&first.summary))
                    .field("start", s(&when(first, first.start)))
                    .field("end", s(&when(first, first.end)))
                    .field("recurring", n(first.recurring))
                    .field("location", s(first.location.as_deref().unwrap_or_default()))
                    .field(
                        "description",
                        s(first.description.as_deref().unwrap_or_default()),
                    )
                    .field("dirty", n(first.dirty));
            }
            t.field("raw", s(&event.raw)).print();
            Ok(())
        }
        EventsCmd::Add {
            account,
            calendar,
            summary,
            start,
            end,
            location,
            description,
        } => {
            let account = find_account(&client, &account).await?;
            let event = NewEvent {
                calendar,
                summary,
                start,
                end,
                location,
                description,
                reminders: vec![],
            };
            let href = client.create_event(&account.id, event).await?;
            println!("{} created {href}", "✓".green());
            Ok(())
        }
        EventsCmd::Update {
            account,
            href,
            summary,
            location,
            start,
            end,
            description,
        } => {
            if [&summary, &location, &start, &end, &description]
                .iter()
                .all(|f| f.is_none())
            {
                bail!(
                    "nothing to change: pass --summary, --location, --description, --start or --end"
                );
            }
            let account = find_account(&client, &account).await?;
            let changes = EventChanges {
                summary,
                location,
                start,
                end,
                description,
                reminders: None,
            };
            client.update_event(&account.id, &href, changes).await?;
            println!("{} updated {href}", "✓".green());
            Ok(())
        }
        EventsCmd::Delete { account, href } => {
            let account = find_account(&client, &account).await?;
            client.delete_event(&account.id, &href).await?;
            println!("{} deleted {href}", "✓".green());
            Ok(())
        }
    }
}

/// A date is midnight UTC, so a range given in days covers whole days.
fn parse_time(text: &str) -> Result<DateTime<Utc>> {
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(date
            .and_hms_opt(0, 0, 0)
            .expect("midnight exists")
            .and_utc());
    }
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .with_context(|| format!("{text:?} is neither YYYY-MM-DD nor an RFC 3339 time"))
}

/// An all-day event is shown as its date: its instants are midnight UTC.
fn when(event: &EventOccurrence, at: DateTime<Utc>) -> String {
    if event.all_day {
        at.format("%Y-%m-%d").to_string()
    } else {
        at.to_rfc3339()
    }
}

fn print_occurrences(found: &[EventOccurrence]) {
    let rows: Vec<_> = found
        .iter()
        .map(|e| {
            vec![
                s(&when(e, e.start)),
                s(&when(e, e.end)),
                s(&e.summary),
                s(e.location.as_deref().unwrap_or_default()),
                n(e.recurring),
                s(&e.calendar),
                s(&e.href),
            ]
        })
        .collect();
    let fields = [
        "start",
        "end",
        "summary",
        "location",
        "recurring",
        "calendar",
        "href",
    ];
    Toon::default().table("events", &fields, &rows).print();
}
