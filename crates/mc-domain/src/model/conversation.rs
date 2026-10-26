use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::model::account::AccountId;
use crate::value_objects::flag::Flag;
use crate::value_objects::message_id::MessageId;
use crate::value_objects::thread_id::ThreadId;

/// A conversation groups the messages of one connected component of the
/// Message-ID graph (own id, In-Reply-To, References).
///
/// This is a **read model** projected from `MessageReceived` (and flag-change)
/// events. It is NOT an aggregate — its state is entirely derived from messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub thread_id: ThreadId,
    pub account_id: AccountId,
    /// Subject normalised (Re:/Fwd: stripped).
    pub subject: String,
    /// Internal message IDs in chronological order.
    pub message_ids: Vec<MessageId>,
    /// Unread count (messages without Flag::Seen).
    pub unread_count: u32,
    /// Timestamp of the most recent message.
    pub last_message_at: DateTime<Utc>,
    /// From-display of the most recent sender.
    pub latest_sender: String,
}

impl Conversation {
    /// Normalise a subject line by stripping Re:/Fwd:/Fw: prefixes.
    pub fn normalize_subject(subject: &str) -> String {
        let mut s = subject.trim();
        loop {
            let lower = s.to_lowercase();
            if lower.starts_with("re:") {
                s = s[3..].trim();
            } else if lower.starts_with("fwd:") {
                s = s[4..].trim();
            } else if lower.starts_with("fw:") {
                s = s[3..].trim();
            } else {
                break;
            }
        }
        s.to_string()
    }

    /// True if the flags list does NOT contain `Seen` (i.e. message is unread).
    pub fn is_unread(flags: &[Flag]) -> bool {
        !flags.contains(&Flag::Seen)
    }
}
