//! Incremental mailbox sync: what arrived and what changed since a cursor.
//!
//! The cursor is `imap1:{UIDVALIDITY}:{UIDNEXT}` as of the last sync. Without
//! CONDSTORE/QRESYNC an IMAP server cannot say what changed among the messages
//! already known, so `existing` is always a full UID + FLAGS listing.

use std::collections::HashMap;

use tracing::debug;

use mc_domain::MessageKey;
use mc_domain::error::DomainError;
use mc_domain::traits::email_provider::{Existing, MailboxChanges, MailboxCounts, Reset};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::remote_id::RemoteId;
use mc_domain::value_objects::sync_cursor::SyncCursor;

use super::super::mapper::{imap_flag_to_domain, mailbox_path_to_string, visible_flags};
use super::commands::{exchange, uid_flags, uid_of};
use super::{COMMAND_TIMEOUT, ImapEmailProvider};

const CURSOR_TAG: &str = "imap1";

/// Where a mailbox stood at one sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Position {
    uid_validity: u32,
    uid_next: u32,
}

impl Position {
    fn parse(cursor: &SyncCursor) -> Option<Self> {
        let mut parts = cursor.as_str().split(':');
        (parts.next()? == CURSOR_TAG).then_some(())?;
        let position = Self {
            uid_validity: parts.next()?.parse().ok()?,
            uid_next: parts.next()?.parse().ok()?,
        };
        parts.next().is_none().then_some(position)
    }

    fn cursor(self) -> SyncCursor {
        SyncCursor::new(format!(
            "{CURSOR_TAG}:{}:{}",
            self.uid_validity, self.uid_next
        ))
    }
}

/// A message's UID and its flags as the server lists them.
type Listing = Vec<(u32, Vec<Flag>)>;

/// Turn one UID + FLAGS listing into changes since `since`.
///
/// `added` and the snapshot come from the same listing, and the new cursor
/// from its highest UID, not from STATUS: a message that lands between the
/// two commands must be in `added` and the cursor must move past it, and a
/// server that omits UIDNEXT must not empty the snapshot. The application
/// never sees IMAP's own markers (see [`visible_flags`]).
fn changes_from(
    since: Option<&SyncCursor>,
    uid_validity: u32,
    counts: MailboxCounts,
    listing: Listing,
) -> MailboxChanges {
    let (reset, last_next) = match since.map(Position::parse) {
        None => (None, 1),
        Some(Some(last)) if last.uid_validity == uid_validity => (None, last.uid_next),
        Some(_) => (Some(Reset::IdsInvalidated), 1),
    };
    let uid_next = listing
        .iter()
        .map(|(uid, _)| uid + 1)
        .fold(last_next, u32::max);
    let mut live: Listing = listing
        .into_iter()
        .filter_map(|(uid, flags)| Some((uid, visible_flags(flags)?)))
        .collect();
    live.sort_unstable_by_key(|(uid, _)| std::cmp::Reverse(*uid));
    let added = live
        .iter()
        .filter(|(uid, _)| *uid >= last_next)
        .map(|(uid, _)| RemoteId::from(*uid))
        .collect();
    let snapshot = live
        .into_iter()
        .map(|(uid, flags)| (RemoteId::from(uid), flags))
        .collect();
    MailboxChanges {
        cursor: Position {
            uid_validity,
            uid_next,
        }
        .cursor(),
        reset,
        added,
        keys: HashMap::new(),
        labels: HashMap::new(),
        existing: Existing::Snapshot(snapshot),
        counts,
    }
}

/// Without it no cursor can say whether ids are still valid, and guessing
/// would either wipe or keep a mailbox wrongly.
fn require_uid_validity(uid_validity: Option<u32>, mailbox: &str) -> Result<u32, DomainError> {
    uid_validity
        .ok_or_else(|| DomainError::Provider(format!("Server sent no UIDVALIDITY for {mailbox}")))
}

impl ImapEmailProvider {
    pub(super) async fn changes(
        &self,
        mailbox: &MailboxPath,
        since: Option<&SyncCursor>,
    ) -> Result<MailboxChanges, DomainError> {
        let name = mailbox_path_to_string(mailbox);

        // STATUS, not SELECT's numbers: SELECT's "unseen" is the first unseen
        // sequence number (RFC 3501), not a count.
        let mut session = self.session().await?;
        let gmail_all = self
            .layout_on(&mut session)
            .await?
            .is_some_and(|l| l.is_all_mail(&name));
        let items = if gmail_all {
            "(MESSAGES UNSEEN UIDVALIDITY HIGHESTMODSEQ)"
        } else {
            "(MESSAGES UNSEEN UIDVALIDITY)"
        };
        let status = exchange(
            COMMAND_TIMEOUT,
            "Mailbox STATUS",
            session.status(&name, items),
        )
        .await?;
        self.release(session);
        let uid_validity = require_uid_validity(status.uid_validity, &name)?;
        let counts = MailboxCounts {
            total: status.exists,
            unread: status.unseen.unwrap_or(0),
        };

        if gmail_all {
            let modseq = status.highest_modseq.unwrap_or(0);
            return self
                .all_mail_changes(&name, since, uid_validity, modseq, counts)
                .await;
        }
        let mut session = self.selected(&name).await?;
        let fetches = uid_flags(&mut session, self.has_gmail_ext()).await?;
        self.release(session);
        self.record_downloads().await;
        let listing: Listing = fetches
            .iter()
            .filter_map(|f| {
                Some((
                    f.uid?,
                    f.flags()
                        .filter_map(|flag| imap_flag_to_domain(&flag))
                        .collect(),
                ))
            })
            .collect();

        let mut changes = changes_from(since, uid_validity, counts, listing);
        let keys: HashMap<u32, MessageKey> = fetches
            .iter()
            .filter_map(|f| Some((f.uid?, MessageKey::new(f.gmail_msg_id()?.to_string()))))
            .collect();
        changes.keys = changes
            .added
            .iter()
            .filter_map(|id| Some((id.clone(), keys.get(&uid_of(id).ok()?)?.clone())))
            .collect();
        debug!(
            mailbox = %name,
            added = changes.added.len(),
            reset = ?changes.reset,
            cursor = %changes.cursor,
            "Mailbox changes"
        );
        Ok(changes)
    }
}

#[cfg(test)]
mod tests;
