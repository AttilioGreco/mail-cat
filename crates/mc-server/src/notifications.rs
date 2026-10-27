//! Desktop-notification action routing and dismissal.
//!
//! notify-rust's `wait_for_action` listens on a private connection and stops at
//! the *first* `NotificationClosed` carrying its id. Most shells fire that as
//! soon as the banner leaves the screen, while the notification itself lives on
//! in the tray with its buttons still clickable — so "Archivia" pressed from the
//! notification panel used to reach nobody and silently do nothing.
//!
//! Hence a single long-lived session-bus listener plus a registry of live
//! notifications, which also gives us the id needed to *close* a notification
//! when its message gets read, moved or deleted somewhere else.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mc_application::bus::{CommandBus, QueryBus};
use mc_domain::{AccountId, MailboxId, MessageId};

/// What a still-visible "new message" notification refers to.
#[derive(Clone, Copy)]
pub struct NotifiedMessage {
    pub message_id: MessageId,
    pub account_id: AccountId,
    pub mailbox_id: MailboxId,
}

/// Runs the action a user picked on a new-mail notification.
///
/// Shared by the D-Bus listener and the notify-rust fallback so both platforms
/// behave identically.
pub async fn run_action(
    action: &str,
    target: NotifiedMessage,
    command_bus: &Arc<dyn CommandBus>,
    query_bus: &Arc<dyn QueryBus>,
    ui_bin: &str,
) {
    let NotifiedMessage {
        message_id,
        account_id,
        mailbox_id,
    } = target;

    match action {
        "archive" => {
            // Fetch all mailboxes for the account so we can resolve both the
            // source path and the Archive target.
            let mailboxes = match query_bus
                .dispatch(mc_application::Query::ListMailboxes { account_id })
                .await
            {
                Ok(mc_application::QueryResult::Mailboxes(m)) => m,
                _ => return,
            };
            let from_mb = mailboxes.iter().find(|m| m.id == mailbox_id);
            // Gmail ships no \Archive folder — archiving there means moving to
            // \All ("All Mail"), which drops the Inbox label. Prefer a real
            // Archive when one exists.
            let archive_mb = mailboxes
                .iter()
                .find(|m| m.mailbox_type == mc_domain::MailboxType::Archive)
                .or_else(|| {
                    mailboxes
                        .iter()
                        .find(|m| m.mailbox_type == mc_domain::MailboxType::All)
                });
            match (from_mb, archive_mb) {
                (Some(src), Some(dst)) => {
                    if let Err(e) = command_bus
                        .dispatch(mc_application::Command::MoveMessage {
                            message_id,
                            from_mailbox_id: src.id,
                            to_mailbox_id: dst.id,
                            to_mailbox_path: dst.path.clone(),
                        })
                        .await
                    {
                        tracing::warn!("Archive from notification failed: {e}");
                    }
                }
                _ => tracing::warn!(
                    "Archive mailbox not found for account {}",
                    account_id.as_uuid()
                ),
            }
        }
        // Body click and the explicit button do the same thing: open (or focus)
        // the client on this message. The UI's single-instance plugin forwards
        // these args to an already-running window instead of opening a second one.
        "default" | "open" => {
            let deep_link = format!("{}/{}/{}", account_id.as_uuid(), mailbox_id, message_id);
            if let Err(e) = std::process::Command::new(ui_bin)
                .arg("--open-message")
                .arg(&deep_link)
                .spawn()
            {
                tracing::warn!("Failed to open Mail Cat (ui_bin='{ui_bin}'): {e}");
            }
        }
        _ => {}
    }
}

/// Registry of on-screen new-mail notifications, keyed by the freedesktop
/// notification id the daemon handed back.
#[derive(Default)]
pub struct NotificationCenter {
    live: Mutex<HashMap<u32, NotifiedMessage>>,
    #[cfg(target_os = "linux")]
    conn: Option<zbus::Connection>,
}

impl NotificationCenter {
    #[cfg(target_os = "linux")]
    pub async fn connect() -> Arc<Self> {
        let conn = match zbus::Connection::session().await {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!("No session bus for notification actions: {e}");
                None
            }
        };
        Arc::new(Self {
            live: Mutex::new(HashMap::new()),
            conn,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn connect() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// True when the shared listener is running, i.e. notifications must *not*
    /// also be waited on individually via notify-rust.
    pub fn is_listening(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.conn.is_some()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    pub fn track(&self, notification_id: u32, target: NotifiedMessage) {
        self.live
            .lock()
            .expect("notification registry poisoned")
            .insert(notification_id, target);
    }

    /// Dismisses the notification for `message_id`, if one is still up.
    ///
    /// Called when the message is read (here or in another client), archived or
    /// deleted — a notification for mail the user has already dealt with is just
    /// noise.
    pub async fn close_for_message(&self, message_id: &MessageId) {
        let id = {
            let mut live = self.live.lock().expect("notification registry poisoned");
            let Some((&id, _)) = live.iter().find(|(_, t)| t.message_id == *message_id) else {
                return;
            };
            live.remove(&id);
            id
        };
        self.close(id).await;
    }

    #[cfg(target_os = "linux")]
    async fn close(&self, id: u32) {
        let Some(conn) = &self.conn else { return };
        if let Err(e) = conn
            .call_method(
                Some("org.freedesktop.Notifications"),
                "/org/freedesktop/Notifications",
                Some("org.freedesktop.Notifications"),
                "CloseNotification",
                &(id,),
            )
            .await
        {
            tracing::debug!("CloseNotification({id}) failed: {e}");
        }
    }

    #[cfg(not(target_os = "linux"))]
    async fn close(&self, _id: u32) {}

    /// Listens for `ActionInvoked` / `NotificationClosed` for the whole process
    /// lifetime and routes each action to [`run_action`].
    #[cfg(target_os = "linux")]
    pub fn spawn_action_loop(
        self: Arc<Self>,
        command_bus: Arc<dyn CommandBus>,
        query_bus: Arc<dyn QueryBus>,
        ui_bin: String,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        use futures::StreamExt;

        let Some(conn) = self.conn.clone() else { return };
        tokio::spawn(async move {
            if let Err(e) = add_notification_matches(&conn).await {
                tracing::warn!("Notification action listener not started: {e}");
                return;
            }
            let mut stream = zbus::MessageStream::from(&conn);
            loop {
                let msg = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    m = stream.next() => match m {
                        Some(Ok(m)) => m,
                        Some(Err(_)) => continue,
                        None => break,
                    },
                };
                let header = msg.header();
                if header.message_type() != zbus::message::Type::Signal {
                    continue;
                }
                match header.member().map(|m| m.as_str()) {
                    Some("ActionInvoked") => {
                        let Ok((id, action)) = msg.body().deserialize::<(u32, String)>() else {
                            continue;
                        };
                        // Leave the entry in place: the daemon sends
                        // NotificationClosed right after, which cleans up.
                        let target = self
                            .live
                            .lock()
                            .expect("notification registry poisoned")
                            .get(&id)
                            .copied();
                        if let Some(target) = target {
                            run_action(&action, target, &command_bus, &query_bus, &ui_bin).await;
                        }
                    }
                    Some("NotificationClosed") => {
                        let Ok((id, _reason)) = msg.body().deserialize::<(u32, u32)>() else {
                            continue;
                        };
                        self.live
                            .lock()
                            .expect("notification registry poisoned")
                            .remove(&id);
                    }
                    _ => {}
                }
            }
            tracing::info!("Notification action listener exited");
        });
    }

    #[cfg(not(target_os = "linux"))]
    pub fn spawn_action_loop(
        self: Arc<Self>,
        _command_bus: Arc<dyn CommandBus>,
        _query_bus: Arc<dyn QueryBus>,
        _ui_bin: String,
        _shutdown: tokio_util::sync::CancellationToken,
    ) {
    }
}

#[cfg(target_os = "linux")]
async fn add_notification_matches(conn: &zbus::Connection) -> zbus::Result<()> {
    let proxy = zbus::fdo::DBusProxy::new(conn).await?;
    for member in ["ActionInvoked", "NotificationClosed"] {
        let rule = zbus::MatchRule::builder()
            .msg_type(zbus::message::Type::Signal)
            .interface("org.freedesktop.Notifications")?
            .member(member)?
            .build();
        proxy.add_match_rule(rule).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(message_id: MessageId) -> NotifiedMessage {
        NotifiedMessage {
            message_id,
            account_id: AccountId::new(),
            mailbox_id: MailboxId::new(),
        }
    }

    /// Closing by message id must dismiss exactly the matching notification.
    /// Without a session bus `close` is a no-op, but the bookkeeping is the part
    /// that decides *which* notification the daemon is told to drop.
    #[tokio::test]
    async fn close_for_message_drops_only_that_notification() {
        let center = NotificationCenter::default();
        let (a, b) = (MessageId::new(), MessageId::new());
        center.track(1, target(a));
        center.track(2, target(b));

        center.close_for_message(&a).await;

        let live = center.live.lock().unwrap();
        assert_eq!(live.len(), 1);
        assert!(live.contains_key(&2));
    }

    #[tokio::test]
    async fn close_for_unknown_message_is_a_noop() {
        let center = NotificationCenter::default();
        center.track(1, target(MessageId::new()));

        center.close_for_message(&MessageId::new()).await;

        assert_eq!(center.live.lock().unwrap().len(), 1);
    }
}
