//! Push: the RFC 8620 §7.3 event source behind `watch_mailbox`.

use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use futures::Stream;
use serde_json::Value;
use tracing::{debug, warn};

use mc_domain::error::DomainError;
use mc_domain::{MailboxPath, ProviderNotification};

use super::provider::JmapEmailProvider;

/// Waits between reconnects, the last one repeating.
const BACKOFF: [Duration; 4] = [
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(30),
];
/// The server pings every 60 s; silence for much longer means a dead socket.
const SILENCE: Duration = Duration::from_secs(150);

/// Accumulates stream bytes into complete `(event, data)` pairs.
#[derive(Default)]
pub(super) struct EventParser(Vec<u8>);

impl EventParser {
    pub(super) fn feed(&mut self, chunk: &[u8]) -> Vec<(String, String)> {
        self.0.extend(chunk.iter().filter(|b| **b != b'\r'));
        let mut events = Vec::new();
        while let Some(end) = self.0.windows(2).position(|w| w == b"\n\n") {
            let block: Vec<u8> = self.0.drain(..end + 2).collect();
            let (mut name, mut data) = (String::new(), String::new());
            for line in String::from_utf8_lossy(&block).lines() {
                if let Some(value) = line.strip_prefix("event:") {
                    name = value.trim().to_owned();
                } else if let Some(value) = line.strip_prefix("data:") {
                    data.push_str(value.strip_prefix(' ').unwrap_or(value));
                }
            }
            events.push((name, data));
        }
        events
    }
}

/// Records the Email and Mailbox states a `state` event carries for the
/// account; true when one differs from the last seen.
fn state_moved(seen: &mut HashMap<&'static str, String>, data: &str, account_id: &str) -> bool {
    let Ok(data) = serde_json::from_str::<Value>(data) else {
        return false;
    };
    let mut moved = false;
    for kind in ["Email", "Mailbox"] {
        if let Some(state) = data["changed"][account_id][kind].as_str() {
            moved |= seen.insert(kind, state.to_owned()).as_deref() != Some(state);
        }
    }
    moved
}

impl JmapEmailProvider {
    /// Never ends: a dropped or refused connection is retried with backoff,
    /// and an account the gate paused waits there for the user.
    pub(super) fn watch(
        &self,
        mailbox: &MailboxPath,
    ) -> Pin<Box<dyn Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>> {
        let mut target = mailbox.clone();
        let mut resolved = false;
        Box::pin(async_stream::stream! {
            let mut seen = HashMap::new();
            let mut failures = 0usize;
            loop {
                self.client.ready().await;
                match self.client.event_stream().await {
                    Ok((mut response, account_id)) => {
                        // Booted before the first sync, the caller can only guess the
                        // inbox's path (IMAP's "INBOX"); a JMAP path is the server's id.
                        if !resolved && let Ok(Some(id)) = self.role_mailbox("inbox").await
                            && let Ok(path) = MailboxPath::new(vec![id], '/')
                        {
                            (target, resolved) = (path, true);
                        }
                        let mut parser = EventParser::default();
                        loop {
                        let chunk = match tokio::time::timeout(SILENCE, response.chunk()).await {
                            Ok(Ok(Some(chunk))) => chunk,
                            Ok(Ok(None)) => break,
                            Ok(Err(e)) => {
                                warn!(%e, "JMAP push stream broke");
                                break;
                            }
                            Err(_) => {
                                warn!("JMAP push stream went silent");
                                break;
                            }
                        };
                        failures = 0;
                        for (name, data) in parser.feed(&chunk) {
                            if name == "state" && state_moved(&mut seen, &data, &account_id) {
                                yield Ok(ProviderNotification::MailboxCountChanged {
                                    mailbox: target.clone(),
                                    total: 0,
                                    unseen: 0,
                                });
                            }
                        }
                        }
                    }
                    Err(e) => warn!(%e, "JMAP push connection failed"),
                }
                let wait = BACKOFF[failures.min(BACKOFF.len() - 1)];
                debug!(?wait, "JMAP push reconnecting");
                failures += 1;
                tokio::time::sleep(wait).await;
            }
        })
    }
}
