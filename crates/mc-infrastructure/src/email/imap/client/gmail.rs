//! Gmail's X-GM-EXT-1. Gmail keeps every message once, in All Mail, and shows
//! it under its labels; as IMAP folders each label has UIDs of its own. Only
//! All Mail, Spam and Trash are synced (All Mail holds neither of the last
//! two). Every other mailbox is a *view*: its messages and their ids are All
//! Mail's, and its membership is read off `X-GM-LABELS`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use async_imap::types::Fetch;
use futures::TryStreamExt;
use tracing::debug;

use mc_domain::MessageKey;
use mc_domain::error::DomainError;
use mc_domain::model::mailbox::MailboxType;
use mc_domain::traits::email_provider::{Existing, MailboxChanges, MailboxCounts, Reset};
use mc_domain::value_objects::flag::Flag;
use mc_domain::value_objects::mailbox_path::MailboxPath;
use mc_domain::value_objects::remote_id::RemoteId;
use mc_domain::value_objects::sync_cursor::SyncCursor;

use super::super::mapper::{
    decode_utf7, imap_flag_to_domain, mailbox_path_to_string, parse_mailbox_path, unescape_quoted,
    visible_flags,
};
use super::commands::{
    ServerCaps, delete_uids, exchange, move_uids, quoted, store, timed, uids_of,
};
use super::mailbox::{Listed, list_on};
use super::{BODY_FETCH_TIMEOUT, COMMAND_TIMEOUT, ImapEmailProvider, ImapSession};

const CURSOR_TAG: &str = "gm1";
const ITEMS: &str = "(FLAGS X-GM-LABELS X-GM-MSGID)";

/// How a LIST entry reads to [`Layout::from_list`].
pub(super) struct Entry<'a> {
    pub(super) name: &'a str,
    pub(super) delimiter: char,
    pub(super) kind: MailboxType,
}

/// Which mailboxes of a Gmail account are real and which are views.
#[derive(Debug)]
pub(super) struct Layout {
    all: String,
    all_path: MailboxPath,
    trash: Option<String>,
    /// Spam and Trash: ids of their own.
    outside: HashSet<String>,
    /// View name -> its label as `X-GM-LABELS` spells it.
    views: HashMap<String, String>,
    /// Lowercased decoded label -> the view's path.
    by_label: HashMap<String, MailboxPath>,
    /// Changes when a label appears, disappears or is renamed: messages must
    /// then be read again, since labels are only reported when they change.
    fingerprint: u64,
}

impl Layout {
    /// `None` without `\All`: Gmail lets users hide All Mail from IMAP, and
    /// then labels cannot be read off it.
    pub(super) fn from_list<'a>(entries: impl IntoIterator<Item = Entry<'a>>) -> Option<Self> {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let all = entries.iter().find(|e| e.kind == MailboxType::All)?;
        let mut layout = Self {
            all: all.name.to_owned(),
            all_path: parse_mailbox_path(all.name, all.delimiter).ok()?,
            trash: None,
            outside: HashSet::new(),
            views: HashMap::new(),
            by_label: HashMap::new(),
            fingerprint: 0,
        };
        for e in &entries {
            let label = match e.kind {
                MailboxType::All => continue,
                MailboxType::Trash => {
                    layout.trash = Some(e.name.to_owned());
                    layout.outside.insert(e.name.to_owned());
                    continue;
                }
                MailboxType::Spam => {
                    layout.outside.insert(e.name.to_owned());
                    continue;
                }
                MailboxType::Inbox => "\\Inbox",
                MailboxType::Sent => "\\Sent",
                MailboxType::Drafts => "\\Draft",
                MailboxType::Important => "\\Important",
                MailboxType::Starred => "\\Starred",
                _ => e.name,
            };
            let Ok(path) = parse_mailbox_path(e.name, e.delimiter) else {
                continue;
            };
            layout.views.insert(e.name.to_owned(), label.to_owned());
            layout.by_label.insert(label_key(label), path);
        }
        let mut names: Vec<&String> = layout.views.keys().collect();
        names.sort_unstable();
        // FNV-1a: persisted in cursors, so it must not change between Rust releases.
        layout.fingerprint = names.iter().fold(0xcbf2_9ce4_8422_2325, |h, name| {
            name.bytes()
                .chain([0])
                .fold(h, |h, b| (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3))
        });
        Some(layout)
    }

    pub(super) fn is_all_mail(&self, name: &str) -> bool {
        self.all == name
    }

    pub(super) fn is_view(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    /// Where `name`'s messages are read and addressed.
    pub(super) fn backing<'a>(&'a self, name: &'a str) -> &'a str {
        if self.is_view(name) { &self.all } else { name }
    }

    /// Whether ids in `name` are All Mail's (not Spam's or Trash's).
    pub(super) fn holds_all_mail_ids(&self, name: &str) -> bool {
        self.is_view(name) || self.is_all_mail(name)
    }

    pub(super) fn backed_by(&self, path: &MailboxPath) -> Option<MailboxPath> {
        self.is_view(&mailbox_path_to_string(path))
            .then(|| self.all_path.clone())
    }

    /// The views named by `labels`; labels no view has (Gmail's own, or
    /// created since LIST) are dropped.
    fn paths_of(&self, labels: &[String]) -> Vec<MailboxPath> {
        labels
            .iter()
            .filter_map(|l| self.by_label.get(&label_key(l)).cloned())
            .collect()
    }

    /// User labels the rows carry that the last LIST did not (a rename or
    /// creation not yet visible to IMAP): their messages lose them silently.
    fn unlisted(&self, rows: &[Row]) -> BTreeSet<String> {
        rows.iter()
            .flat_map(|r| &r.labels)
            .filter(|l| !l.starts_with('\\') && !self.by_label.contains_key(&label_key(l)))
            .cloned()
            .collect()
    }

    /// `view` as `STORE` names it.
    fn store_label(&self, view: &str) -> Option<String> {
        let label = self.views.get(view)?;
        Some(if label.starts_with('\\') {
            label.clone()
        } else {
            quoted(label).ok()?
        })
    }

    fn store_labels(&self, paths: &[MailboxPath]) -> Result<Vec<String>, DomainError> {
        paths
            .iter()
            .map(|p| {
                self.store_label(&mailbox_path_to_string(p))
                    .ok_or_else(|| DomainError::NotSupported(format!("{p} is not a Gmail label")))
            })
            .collect()
    }
}

fn label_key(label: &str) -> String {
    decode_utf7(label).to_lowercase()
}

/// Where a mailbox stood at one sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Position {
    pub(super) uid_validity: u32,
    pub(super) uid_next: u32,
    pub(super) modseq: u64,
    pub(super) views: u64,
}

impl Position {
    fn parse(cursor: &SyncCursor) -> Option<Self> {
        let mut parts = cursor.as_str().split(':');
        (parts.next()? == CURSOR_TAG).then_some(())?;
        let position = Self {
            uid_validity: parts.next()?.parse().ok()?,
            uid_next: parts.next()?.parse().ok()?,
            modseq: parts.next()?.parse().ok()?,
            views: parts.next()?.parse().ok()?,
        };
        parts.next().is_none().then_some(position)
    }

    fn cursor(self) -> SyncCursor {
        SyncCursor::new(format!(
            "{CURSOR_TAG}:{}:{}:{}:{}",
            self.uid_validity, self.uid_next, self.modseq, self.views
        ))
    }
}

/// What to ask the server for, given the cursor.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Plan {
    pub(super) reset: Option<Reset>,
    pub(super) last_next: u32,
    /// `Some`: only messages changed after this mod-sequence. `None`: all of
    /// them (first sync, a reset, no CONDSTORE, or labels that changed meaning).
    pub(super) changed_since: Option<u64>,
}

pub(super) fn plan(since: Option<&SyncCursor>, uid_validity: u32, modseq: u64, views: u64) -> Plan {
    match since.map(Position::parse) {
        None => Plan {
            reset: None,
            last_next: 1,
            changed_since: None,
        },
        Some(Some(last)) if last.uid_validity == uid_validity => Plan {
            reset: None,
            last_next: last.uid_next,
            changed_since: (last.modseq > 0 && modseq > 0 && last.views == views)
                .then_some(last.modseq),
        },
        Some(_) => Plan {
            reset: Some(Reset::IdsInvalidated),
            last_next: 1,
            changed_since: None,
        },
    }
}

/// One message of the FETCH, as All Mail reports it.
#[derive(Debug, Clone)]
pub(super) struct Row {
    pub(super) uid: u32,
    pub(super) flags: Vec<Flag>,
    pub(super) labels: Vec<String>,
    pub(super) key: Option<u64>,
}

/// `None` for an unsolicited flags update, which carries no labels: reading
/// that as "no labels" would strip the message of all of them.
fn row_of(fetch: &Fetch) -> Option<Row> {
    Some(Row {
        uid: fetch.uid?,
        flags: fetch
            .flags()
            .filter_map(|f| imap_flag_to_domain(&f))
            .collect(),
        labels: fetch
            .gmail_labels()?
            .iter()
            .map(|l| unescape_quoted(l))
            .collect(),
        key: fetch.gmail_msg_id().copied(),
    })
}

/// Turn the UIDs still there (`present`) and the rows fetched per `plan` into
/// changes. A row is a message that is new or whose flags or labels changed
/// (every message when `plan.changed_since` is `None`).
pub(super) fn changes_from(
    layout: &Layout,
    plan: &Plan,
    uid_validity: u32,
    modseq: u64,
    counts: MailboxCounts,
    mut present: Vec<u32>,
    rows: Vec<Row>,
) -> MailboxChanges {
    let mut deleted = HashSet::new();
    let mut live = Vec::with_capacity(rows.len());
    for mut row in rows {
        match visible_flags(std::mem::take(&mut row.flags)) {
            Some(flags) => live.push(Row { flags, ..row }),
            None => {
                deleted.insert(row.uid);
            }
        }
    }
    present.retain(|uid| !deleted.contains(uid));
    present.sort_unstable_by_key(|uid| std::cmp::Reverse(*uid));
    let uid_next = present
        .first()
        .map_or(plan.last_next, |uid| (uid + 1).max(plan.last_next));
    let added: Vec<RemoteId> = present
        .iter()
        .take_while(|uid| **uid >= plan.last_next)
        .map(|uid| RemoteId::from(*uid))
        .collect();
    let keys = live
        .iter()
        .filter(|r| r.uid >= plan.last_next)
        .filter_map(|r| Some((RemoteId::from(r.uid), MessageKey::new(r.key?.to_string()))))
        .collect();
    let labels = live
        .iter()
        .map(|r| (RemoteId::from(r.uid), layout.paths_of(&r.labels)))
        .collect();
    let changed = live
        .into_iter()
        .map(|r| (RemoteId::from(r.uid), r.flags))
        .collect();
    MailboxChanges {
        cursor: Position {
            uid_validity,
            uid_next,
            modseq,
            views: layout.fingerprint,
        }
        .cursor(),
        reset: plan.reset,
        added,
        keys,
        labels,
        existing: Existing::Listed {
            present: present.into_iter().map(RemoteId::from).collect(),
            changed,
        },
        counts,
    }
}

/// The rows to read for `present`: one `CHANGEDSINCE` fetch, or all of them
/// in bounded batches so a big mailbox is no single huge reply.
pub(super) async fn labelled_rows<T>(
    session: &mut async_imap::Session<T>,
    present: &[u32],
    changed_since: Option<u64>,
) -> Result<Vec<Row>, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let requests: Vec<(String, String)> = match changed_since {
        _ if present.is_empty() => Vec::new(),
        Some(modseq) => vec![("1:*".to_owned(), format!("{ITEMS} (CHANGEDSINCE {modseq})"))],
        None => present
            .chunks(ImapEmailProvider::FLAGS_BATCH_SIZE)
            .map(|c| (ImapEmailProvider::compress_uid_ranges(c), ITEMS.to_owned()))
            .collect(),
    };
    let mut rows = Vec::new();
    for (set, query) in requests {
        let batch: Vec<Fetch> = exchange(BODY_FETCH_TIMEOUT, "UID FETCH LABELS", async {
            session.uid_fetch(&set, &query).await?.try_collect().await
        })
        .await?;
        rows.extend(batch.iter().filter_map(row_of));
    }
    Ok(rows)
}

/// `UID STORE` adding (`+`) or removing (`-`) `labels` on `uids` (ascending).
pub(super) async fn store_labels<T>(
    session: &mut async_imap::Session<T>,
    uids: &[u32],
    sign: char,
    labels: &[String],
) -> Result<(), DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    if labels.is_empty() {
        return Ok(());
    }
    for chunk in uids.chunks(ImapEmailProvider::FETCH_BATCH_SIZE) {
        let set = ImapEmailProvider::compress_uid_ranges(chunk);
        let change = format!("{sign}X-GM-LABELS ({})", labels.join(" "));
        store(session, &set, &change, "Labels").await?;
    }
    Ok(())
}

impl ImapEmailProvider {
    pub(super) fn layout(&self) -> Option<Arc<Layout>> {
        self.layout.lock().expect("layout lock poisoned").clone()?
    }

    pub(super) fn learn_layout(&self, listed: &[Listed]) {
        let layout = Layout::from_list(listed.iter().map(|l| Entry {
            name: &l.name,
            delimiter: l.mailbox.path.delimiter(),
            kind: l.mailbox.mailbox_type,
        }));
        *self.layout.lock().expect("layout lock poisoned") = Some(layout.map(Arc::new));
    }

    /// The Gmail layout, listing the mailboxes once to learn it; `None` for
    /// any other server, or a Gmail without All Mail.
    pub(super) async fn layout_on(
        &self,
        session: &mut ImapSession,
    ) -> Result<Option<Arc<Layout>>, DomainError> {
        if !self.has_gmail_ext() {
            return Ok(None);
        }
        let known = self.layout.lock().expect("layout lock poisoned").is_some();
        if !known {
            self.learn_layout(&list_on(session, true).await?);
        }
        Ok(self.layout())
    }

    pub(super) async fn all_mail_changes(
        &self,
        name: &str,
        since: Option<&SyncCursor>,
        uid_validity: u32,
        modseq: u64,
        counts: MailboxCounts,
    ) -> Result<MailboxChanges, DomainError> {
        let mut session = self.selected(name).await?;
        let layout = self
            .layout()
            .ok_or_else(|| DomainError::Provider("Gmail layout unknown".into()))?;
        let plan = plan(since, uid_validity, modseq, layout.fingerprint);
        let found = exchange(
            COMMAND_TIMEOUT,
            "UID SEARCH UNDELETED",
            session.uid_search("UNDELETED"),
        )
        .await?;
        let mut present: Vec<u32> = found.into_iter().collect();
        present.sort_unstable();
        let rows = labelled_rows(&mut session, &present, plan.changed_since).await?;
        self.release(session);
        self.record_downloads().await;
        let unlisted = layout.unlisted(&rows);
        let changes = changes_from(&layout, &plan, uid_validity, modseq, counts, present, rows);
        debug!(
            added = changes.added.len(),
            delta = plan.changed_since.is_some(),
            ?unlisted,
            reset = ?changes.reset,
            cursor = %changes.cursor,
            "All Mail changes"
        );
        Ok(changes)
    }

    /// Add and remove labels on All Mail messages (`ids` are in `in_mailbox`).
    pub(super) async fn relabel(
        &self,
        ids: &[RemoteId],
        in_mailbox: &MailboxPath,
        add: &[MailboxPath],
        remove: &[MailboxPath],
    ) -> Result<(), DomainError> {
        let name = mailbox_path_to_string(in_mailbox);
        let mut session = self.selected(&name).await?;
        let Some(layout) = self.layout() else {
            return Err(DomainError::NotSupported(
                "this account's mailboxes are folders, not labels".into(),
            ));
        };
        if !layout.holds_all_mail_ids(&name) {
            return Err(DomainError::NotSupported(
                "labels of a message in Spam or Trash cannot be changed".into(),
            ));
        }
        let (add, remove) = (layout.store_labels(add)?, layout.store_labels(remove)?);
        let mut uids = uids_of(ids)?;
        uids.sort_unstable();
        store_labels(&mut session, &uids, '+', &add).await?;
        store_labels(&mut session, &uids, '-', &remove).await?;
        self.release(session);
        Ok(())
    }

    /// `move_messages` between two mailboxes that share All Mail's ids: the
    /// message stays put and `to` gains, `from` loses, its label.
    pub(super) async fn move_by_labels<T>(
        session: &mut async_imap::Session<T>,
        layout: &Layout,
        uids: &[u32],
        from: &str,
        to: &str,
    ) -> Result<(), DomainError>
    where
        T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
    {
        let add: Vec<String> = layout.store_label(to).into_iter().collect();
        let remove: Vec<String> = layout
            .store_label(from)
            .filter(|_| from != to)
            .into_iter()
            .collect();
        store_labels(session, uids, '+', &add).await?;
        store_labels(session, uids, '-', &remove).await
    }

    /// Permanently delete All Mail messages, which a `\Deleted` + expunge in
    /// All Mail is not documented to do: move them to Trash, then expunge
    /// exactly those copies there.
    pub(super) async fn expunge_via_trash<T>(
        session: &mut async_imap::Session<T>,
        layout: &Layout,
        caps: ServerCaps,
        uids: &[u32],
    ) -> Result<(), DomainError>
    where
        T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
    {
        let Some(trash) = layout.trash.as_deref() else {
            return Err(DomainError::NotSupported("this Gmail has no Trash".into()));
        };
        let mut moved = Vec::with_capacity(uids.len());
        for chunk in uids.chunks(Self::FETCH_BATCH_SIZE) {
            let pairs = timed(
                COMMAND_TIMEOUT,
                "Move",
                move_uids(session, caps, chunk, trash),
            )
            .await?;
            moved.extend(pairs.into_iter().map(|(_, new)| new));
        }
        if moved.is_empty() {
            return Ok(());
        }
        moved.sort_unstable();
        exchange(COMMAND_TIMEOUT, "Mailbox select", session.select(trash)).await?;
        for chunk in moved.chunks(Self::FETCH_BATCH_SIZE) {
            timed(
                COMMAND_TIMEOUT,
                "Delete",
                delete_uids(session, caps, chunk, true),
            )
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
