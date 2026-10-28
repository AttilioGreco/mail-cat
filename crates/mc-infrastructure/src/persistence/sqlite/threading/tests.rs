use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use mc_domain::{
    AccountId, ConversationReadRepository, DomainEvent, Envelope, EventEnvelope, Flag, MailboxId,
    MailboxPath, MailboxType, Membership, MessageId, ReadModelProjector, RemoteId,
};
use mc_shared::pagination::Pagination;
use uuid::Uuid;

use crate::persistence::sqlite::SqliteDb;
use crate::persistence::sqlite::event_store::SqliteEventStore;
use crate::persistence::sqlite::repository::SqliteRepository;

/// Components of the Message-ID graph recomputed from scratch with a recursive
/// CTE, as the label (smallest id) of each message's component. Independent of
/// the write-time union-find it checks.
const COMPONENTS: &str = "
WITH RECURSIVE raw_ids(acct, msg, raw) AS (
    SELECT account_id, id, rfc_message_id FROM messages WHERE rfc_message_id IS NOT NULL
    UNION SELECT account_id, id, in_reply_to FROM messages WHERE in_reply_to IS NOT NULL
    UNION SELECT m.account_id, m.id, j.value FROM messages m, json_each(m.refs) j
),
ids(acct, msg, id) AS (SELECT acct, msg, '<' || trim(raw, ' <>') || '>' FROM raw_ids
),
edge(acct, a, b) AS (SELECT x.acct, x.id, y.id FROM ids x JOIN ids y ON x.msg = y.msg AND x.acct = y.acct),
reach(acct, root, id) AS (
    SELECT acct, id, id FROM ids
    UNION SELECT r.acct, r.root, e.b FROM reach r JOIN edge e ON e.acct = r.acct AND e.a = r.id
)
SELECT m.id, m.thread_id,
    (SELECT MIN(r.root) FROM ids i JOIN reach r ON r.acct = i.acct AND r.id = i.id WHERE i.msg = m.id)
FROM messages m WHERE m.thread_id IS NOT NULL";

struct Mail {
    repo: SqliteRepository,
    _dir: tempfile::TempDir,
    account: AccountId,
    inbox: MailboxId,
    trash: MailboxId,
    events: Vec<EventEnvelope>,
    uid: u32,
}

impl Mail {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
        let store = Arc::new(SqliteEventStore::new(db.clone()));
        let mut mail = Self {
            repo: SqliteRepository::new(db, store),
            _dir: dir,
            account: AccountId::new(),
            inbox: MailboxId::new(),
            trash: MailboxId::new(),
            events: vec![],
            uid: 0,
        };
        for (id, path, mailbox_type) in [
            (mail.inbox, "INBOX", MailboxType::Inbox),
            (mail.trash, "Trash", MailboxType::Trash),
        ] {
            let path = MailboxPath::parse(path, '/').unwrap();
            mail.project(DomainEvent::MailboxDiscovered {
                mailbox_id: id,
                account_id: mail.account,
                path,
                mailbox_type,
            })
            .await;
        }
        mail
    }

    async fn project(&mut self, event: DomainEvent) {
        let mut envelope = EventEnvelope::new("agg".into(), event.aggregate_type(), 1, event);
        envelope.global_position = self.events.len() as u64 + 1;
        self.repo.project(&envelope).await.unwrap();
        self.events.push(envelope);
        self.assert_partition_matches_graph().await;
    }

    async fn receive(
        &mut self,
        rfc: Option<&str>,
        in_reply_to: Option<&str>,
        refs: &[&str],
    ) -> MessageId {
        let inbox = self.inbox;
        self.receive_in(inbox, rfc, in_reply_to, refs).await
    }

    async fn receive_in(
        &mut self,
        mailbox_id: MailboxId,
        rfc: Option<&str>,
        in_reply_to: Option<&str>,
        refs: &[&str],
    ) -> MessageId {
        self.uid += 1;
        let message_id = MessageId::new();
        let at = DateTime::from_timestamp(1_760_000_000 + i64::from(self.uid) * 60, 0).unwrap();
        self.project(DomainEvent::MessageReceived {
            message_id,
            account_id: self.account,
            mailbox_id,
            remote_id: RemoteId::from(self.uid),
            message_rfc_id: rfc.map(str::to_owned),
            in_reply_to: in_reply_to.map(str::to_owned),
            references: refs.iter().map(|r| r.to_string()).collect(),
            envelope: Envelope {
                from: vec![],
                to: vec![],
                cc: vec![],
                bcc: vec![],
                reply_to: vec![],
                subject: "Hello".into(),
                date: at,
            },
            flags: vec![],
            size_bytes: 1,
            has_attachments: false,
            received_at: at,
            restored: false,
        })
        .await;
        message_id
    }

    async fn move_message(&mut self, id: MessageId, from: MailboxId, to: MailboxId) {
        self.project(DomainEvent::MessageMailboxesChanged {
            message_id: id,
            added: vec![Membership {
                mailbox_id: to,
                remote_id: Some(RemoteId::from(900)),
            }],
            removed: vec![from],
        })
        .await;
    }

    async fn thread_of(&self, id: MessageId) -> Uuid {
        self.repo
            .db
            .read(move |c| {
                c.query_row(
                    "SELECT thread_id FROM messages WHERE id = ?1",
                    [id.as_uuid()],
                    |r| r.get(0),
                )
                .map_err(crate::persistence::sqlite::repo_err)
            })
            .await
            .unwrap()
    }

    /// (message_count, unread_count) of every conversation, newest first.
    async fn conversations(&self) -> Vec<(u32, u32)> {
        let page = Pagination::new(1, 200);
        let list = self
            .repo
            .list_conversations(&self.account, None, &page)
            .await
            .unwrap();
        list.items
            .iter()
            .map(|c| (c.message_count, c.unread_count))
            .collect()
    }

    async fn conversation_rows(&self) -> i64 {
        self.repo
            .db
            .read(|c| {
                c.query_row("SELECT COUNT(*) FROM conversations", [], |r| r.get(0))
                    .map_err(crate::persistence::sqlite::repo_err)
            })
            .await
            .unwrap()
    }

    /// Same partition as the CTE: equal thread ids exactly when equal labels.
    async fn assert_partition_matches_graph(&self) {
        let rows: Vec<(Uuid, Uuid, Option<String>)> = self
            .repo
            .db
            .read(|c| {
                let mut stmt = c
                    .prepare(COMPONENTS)
                    .map_err(crate::persistence::sqlite::repo_err)?;
                stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .and_then(|rows| rows.collect())
                    .map_err(crate::persistence::sqlite::repo_err)
            })
            .await
            .unwrap();
        let (mut by_thread, mut by_label) = (HashMap::new(), HashMap::new());
        for (msg, thread, label) in rows {
            // A message without any id is its own component.
            let label = label.unwrap_or_else(|| msg.to_string());
            assert_eq!(
                by_thread.entry(thread).or_insert(label.clone()),
                &label,
                "thread {thread} spans components"
            );
            assert_eq!(
                by_label.entry(label.clone()).or_insert(thread),
                &thread,
                "component {label} is split"
            );
        }
    }
}

#[tokio::test]
async fn in_reply_to_only_chain_is_one_thread() {
    let mut mail = Mail::new().await;
    let root = mail.receive(Some("<root@x>"), None, &[]).await;
    let r1 = mail.receive(Some("<r1@x>"), Some("<root@x>"), &[]).await;
    let r2 = mail.receive(Some("<r2@x>"), Some("<r1@x>"), &[]).await;
    let r3 = mail.receive(Some("<r3@x>"), Some("<r2@x>"), &[]).await;

    let thread = mail.thread_of(root).await;
    for id in [r1, r2, r3] {
        assert_eq!(mail.thread_of(id).await, thread);
    }
    assert_eq!(mail.conversations().await, [(4, 4)]);
}

#[tokio::test]
async fn root_arriving_last_joins_the_replies() {
    let mut mail = Mail::new().await;
    let r2 = mail.receive(Some("<r2@x>"), Some("<r1@x>"), &[]).await;
    let r1 = mail.receive(Some("<r1@x>"), Some("<root@x>"), &[]).await;
    let root = mail.receive(Some("<root@x>"), None, &[]).await;

    assert_eq!(mail.thread_of(r2).await, mail.thread_of(root).await);
    assert_eq!(mail.thread_of(r1).await, mail.thread_of(root).await);
    assert_eq!(mail.conversations().await, [(3, 3)]);
}

#[tokio::test]
async fn bridge_message_merges_two_threads() {
    let mut mail = Mail::new().await;
    let a = mail.receive(Some("<a@x>"), None, &[]).await;
    let b = mail.receive(Some("<b@x>"), None, &[]).await;
    assert_ne!(mail.thread_of(a).await, mail.thread_of(b).await);
    assert_eq!(mail.conversation_rows().await, 2);

    let c = mail
        .receive(Some("<c@x>"), Some("<b@x>"), &["<a@x>", "<b@x>"])
        .await;

    let thread = mail.thread_of(a).await;
    assert_eq!(mail.thread_of(b).await, thread);
    assert_eq!(mail.thread_of(c).await, thread);
    assert_eq!(mail.conversations().await, [(3, 3)]);
    assert_eq!(mail.conversation_rows().await, 1, "the loser's row is gone");
}

#[tokio::test]
async fn merge_keeps_the_oldest_thread_id() {
    let mut mail = Mail::new().await;
    let a = mail.receive(Some("<a@x>"), None, &[]).await;
    let b = mail.receive(Some("<b@x>"), None, &[]).await;
    let oldest = mail.thread_of(a).await.min(mail.thread_of(b).await);
    mail.receive(Some("<c@x>"), None, &["<b@x>", "<a@x>"]).await;
    assert_eq!(mail.thread_of(a).await, oldest);
    assert_eq!(mail.thread_of(b).await, oldest);
}

#[tokio::test]
async fn replies_to_a_parent_we_never_received_share_a_thread() {
    let mut mail = Mail::new().await;
    let r1 = mail
        .receive(Some("<r1@x>"), Some("<ghost@x>"), &["<ghost@x>"])
        .await;
    let r2 = mail
        .receive(Some("<r2@x>"), Some("<ghost@x>"), &["<ghost@x>"])
        .await;
    let other = mail.receive(Some("<other@x>"), None, &[]).await;

    assert_eq!(mail.thread_of(r1).await, mail.thread_of(r2).await);
    assert_ne!(mail.thread_of(r1).await, mail.thread_of(other).await);
}

#[tokio::test]
async fn bracketless_ids_link_like_bracketed_ones() {
    let mut mail = Mail::new().await;
    let a = mail.receive(Some("<a@x>"), None, &[]).await;
    let b = mail.receive(Some("<b@x>"), Some(" a@x "), &[]).await;
    assert_eq!(mail.thread_of(a).await, mail.thread_of(b).await);
}

#[tokio::test]
async fn message_without_any_id_has_its_own_thread() {
    let mut mail = Mail::new().await;
    let a = mail.receive(None, None, &[]).await;
    let b = mail.receive(None, None, &[]).await;
    assert_ne!(mail.thread_of(a).await, mail.thread_of(b).await);
    assert_eq!(mail.conversations().await, [(1, 1), (1, 1)]);
}

#[tokio::test]
async fn a_message_in_three_mailboxes_counts_once() {
    let mut mail = Mail::new().await;
    let trash = mail.trash;
    let id = mail.receive(Some("<a@x>"), None, &[]).await;
    let extra = |mailbox_id| Membership {
        mailbox_id,
        remote_id: Some(RemoteId::from(77)),
    };
    mail.project(DomainEvent::MessageMailboxesChanged {
        message_id: id,
        added: vec![extra(MailboxId::new()), extra(MailboxId::new())],
        removed: vec![],
    })
    .await;
    assert_eq!(mail.conversations().await, [(1, 1)]);

    // Still in the inbox and elsewhere: a copy in Trash doesn't uncount it.
    mail.project(DomainEvent::MessageMailboxesChanged {
        message_id: id,
        added: vec![extra(trash)],
        removed: vec![],
    })
    .await;
    assert_eq!(mail.conversations().await, [(1, 1)]);
}

#[tokio::test]
async fn trashing_uncounts_and_restoring_recounts() {
    let mut mail = Mail::new().await;
    let (inbox, trash) = (mail.inbox, mail.trash);
    let ids = [
        mail.receive(Some("<a@x>"), None, &[]).await,
        mail.receive(Some("<b@x>"), Some("<a@x>"), &[]).await,
        mail.receive(Some("<c@x>"), Some("<b@x>"), &[]).await,
    ];
    assert_eq!(mail.conversations().await, [(3, 3)]);

    mail.move_message(ids[1], inbox, trash).await;
    assert_eq!(mail.conversations().await, [(2, 2)]);
    mail.move_message(ids[1], trash, inbox).await;
    assert_eq!(mail.conversations().await, [(3, 3)]);

    for id in ids {
        mail.move_message(id, inbox, trash).await;
    }
    assert_eq!(
        mail.conversation_rows().await,
        0,
        "nothing countable, no row"
    );
    mail.move_message(ids[0], trash, inbox).await;
    assert_eq!(mail.conversations().await, [(1, 1)]);
}

#[tokio::test]
async fn get_thread_hides_trashed_messages_unless_asked_for_directly() {
    use mc_domain::MessageReadRepository;
    let mut mail = Mail::new().await;
    let (inbox, trash) = (mail.inbox, mail.trash);
    let a = mail.receive(Some("<a@x>"), None, &[]).await;
    let b = mail.receive(Some("<b@x>"), Some("<a@x>"), &[]).await;
    mail.move_message(b, inbox, trash).await;

    assert_eq!(mail.repo.get_thread(&a).await.unwrap().len(), 1);
    assert_eq!(mail.repo.get_thread(&b).await.unwrap().len(), 2);
}

#[tokio::test]
async fn unread_count_follows_flags() {
    let mut mail = Mail::new().await;
    let a = mail.receive(Some("<a@x>"), None, &[]).await;
    mail.receive(Some("<b@x>"), Some("<a@x>"), &[]).await;
    assert_eq!(mail.conversations().await, [(2, 2)]);

    let flag = |add, remove| DomainEvent::MessageFlagged {
        message_id: a,
        flags_added: add,
        flags_removed: remove,
    };
    mail.project(flag(vec![Flag::Seen], vec![])).await;
    assert_eq!(mail.conversations().await, [(2, 1)]);
    mail.project(flag(vec![], vec![Flag::Seen])).await;
    assert_eq!(mail.conversations().await, [(2, 2)]);
}

#[tokio::test]
async fn replaying_the_stream_changes_nothing() {
    let mut mail = Mail::new().await;
    let (inbox, trash) = (mail.inbox, mail.trash);
    mail.receive(Some("<r2@x>"), Some("<r1@x>"), &[]).await;
    mail.receive(Some("<a@x>"), None, &[]).await;
    mail.receive(Some("<b@x>"), None, &[]).await;
    mail.receive(Some("<c@x>"), None, &["<a@x>", "<b@x>"]).await;
    let r1 = mail.receive(Some("<r1@x>"), Some("<root@x>"), &[]).await;
    mail.move_message(r1, inbox, trash).await;
    let before = (mail.conversations().await, mail.conversation_rows().await);

    for event in mail.events.clone() {
        mail.repo.project(&event).await.unwrap();
    }
    mail.assert_partition_matches_graph().await;
    assert_eq!(
        (mail.conversations().await, mail.conversation_rows().await),
        before
    );
}

#[tokio::test]
async fn replay_into_a_fresh_database_gives_the_same_partition() {
    let mut mail = Mail::new().await;
    mail.receive(Some("<r2@x>"), Some("<r1@x>"), &[]).await;
    mail.receive(Some("<a@x>"), None, &[]).await;
    mail.receive(Some("<c@x>"), None, &["<a@x>", "<r1@x>"])
        .await;
    let before = mail.conversations().await;

    let mut fresh = Mail::new().await;
    fresh.account = mail.account;
    for event in mail.events.iter().skip(2) {
        fresh.repo.project(event).await.unwrap();
    }
    fresh.assert_partition_matches_graph().await;
    assert_eq!(fresh.conversations().await, before);
}
