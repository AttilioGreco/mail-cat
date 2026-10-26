use super::backfill::{BACKFILL_BATCH, BACKFILL_CAP_WAIT, backfill_pause};
use super::*;
use async_trait::async_trait;
use futures::Stream;
use mc_domain::{
    Address, AggregateType, BackfillEntry, BodyBackfillQueue, DomainError, EmailAddress, Envelope,
    EventEnvelope, Existing, Flag, LoginGate, LoginSecret, MailboxChanges, MailboxCounts,
    MailboxType, Membership, Message, MessageBodyResult, MessageBodyStore, MessageEnvelopeSummary,
    MessageId, MessageSummary, OutgoingMessage, ProviderNotification, RemoteId, Reset, SyncCursor,
};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use tokio_util::sync::CancellationToken;

mod mocks;
use mocks::*;
mod mocks_store;
use mocks_store::*;

// ── Test helpers ──────────────────────────────────────────────────

fn test_envelope() -> Envelope {
    Envelope {
        from: vec![Address {
            name: Some("Sender".into()),
            email: EmailAddress::new("sender@example.com").unwrap(),
        }],
        to: vec![Address {
            name: None,
            email: EmailAddress::new("recipient@example.com").unwrap(),
        }],
        cc: vec![],
        bcc: vec![],
        reply_to: vec![],
        subject: "Test Subject".into(),
        date: chrono::Utc::now(),
    }
}

fn make_envelope_summary(uid: u32) -> MessageEnvelopeSummary {
    MessageEnvelopeSummary {
        remote_id: RemoteId::from(uid),
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        envelope: test_envelope(),
        flags: vec![],
        size_bytes: 1024,
        has_attachments: false,
        internal_date: chrono::Utc::now(),
    }
}

fn make_service(
    provider: MockEmailProvider,
    event_store: Arc<MockEventStore>,
    projector: Arc<MockProjector>,
) -> SyncService<MockProviderRegistry, MockEventStore, MockProjector> {
    make_shared_service(Arc::new(provider), event_store, projector)
}

/// For tests that change the provider between two syncs.
fn make_shared_service(
    provider: Arc<MockEmailProvider>,
    event_store: Arc<MockEventStore>,
    projector: Arc<MockProjector>,
) -> SyncService<MockProviderRegistry, MockEventStore, MockProjector> {
    let registry = Arc::new(MockProviderRegistry::new(provider));
    SyncService::new(registry, event_store, projector, SyncConfig::default())
}

async fn cursor_of(projector: &MockProjector, mailbox: &Mailbox) -> Option<SyncCursor> {
    projector
        .get_cursor(&mailbox.account_id, &mailbox.id.to_string())
        .await
        .unwrap()
}

fn archived(
    account_id: AccountId,
    mailbox_id: MailboxId,
    remote_id: Option<RemoteId>,
) -> MessageSummary {
    MessageSummary {
        id: MessageId::new(),
        account_id,
        memberships: vec![Membership {
            mailbox_id,
            remote_id,
        }],
        from_name: None,
        from_email: "sender@example.com".into(),
        to: vec![],
        cc: vec![],
        subject: "Test Subject".into(),
        flags: vec![Flag::Seen],
        size_bytes: 0,
        has_attachments: false,
        received_at: chrono::Utc::now(),
    }
}

fn archive_mailbox(account_id: AccountId) -> Mailbox {
    Mailbox::discover(
        account_id,
        MailboxPath::new(vec!["Archive".to_string()], '/').unwrap(),
        MailboxType::Archive,
    )
    .0
}

fn received(projector: &MockProjector) -> usize {
    projector
        .projected_events()
        .iter()
        .filter(|e| matches!(e, DomainEvent::MessageReceived { .. }))
        .count()
}

/// A message in `mailbox` holding `uid` there (`None`: waiting for one,
/// as after a move), the same in the log and in the read model.
fn seed(
    event_store: &MockEventStore,
    projector: &MockProjector,
    mailbox: &Mailbox,
    uid: Option<u32>,
    rfc_id: &str,
) -> MessageId {
    let (msg, received) = Message::receive(
        mailbox.account_id,
        mailbox.id,
        RemoteId::from(uid.unwrap_or(1)),
        Some(rfc_id.into()),
        None,
        vec![],
        test_envelope(),
        vec![Flag::Seen],
        0,
        false,
        chrono::Utc::now(),
    );
    let mut events = vec![received];
    if uid.is_none() {
        events.push(DomainEvent::MessageMailboxesChanged {
            message_id: msg.id,
            added: vec![Membership {
                mailbox_id: mailbox.id,
                remote_id: None,
            }],
            removed: vec![MailboxId::new()],
        });
    }
    event_store
        .events
        .lock()
        .unwrap()
        .extend(events.into_iter().map(|e| (msg.id.to_string(), e)));
    let mut row = archived(mailbox.account_id, mailbox.id, uid.map(RemoteId::from));
    row.id = msg.id;
    projector.stored.lock().unwrap().push(row);
    projector
        .rfc_ids
        .lock()
        .unwrap()
        .insert(msg.id, rfc_id.into());
    msg.id
}

fn envelope(uid: u32, rfc_id: &str) -> MessageEnvelopeSummary {
    let mut env = make_envelope_summary(uid);
    env.message_rfc_id = Some(rfc_id.into());
    env
}

fn row_uid(projector: &MockProjector, id: MessageId) -> Option<Option<u32>> {
    projector
        .stored
        .lock()
        .unwrap()
        .iter()
        .find(|m| m.id == id)
        .map(|m| m.memberships[0].remote_id.as_ref().map(numeric))
}

fn batch_of(count: u32) -> Vec<MessageEnvelopeSummary> {
    (1..=count)
        .map(|uid| envelope(uid, &format!("<m{uid}@example.com>")))
        .collect()
}

/// A projector for `mailbox` whose cursor is already at `next_id`: every
/// server message is below it, where only reconciliation looks.
async fn synced_up_to(mailbox: &Mailbox, next_id: u32) -> Arc<MockProjector> {
    let projector = Arc::new(MockProjector::new());
    projector.mailboxes.lock().unwrap().push(mailbox.clone());
    projector
        .put_cursor(
            &mailbox.account_id,
            &mailbox.id.to_string(),
            &cursor_at(1, next_id),
        )
        .await
        .unwrap();
    projector
}

fn stored_uids(projector: &MockProjector) -> Vec<u32> {
    let mut uids: Vec<_> = projector
        .stored
        .lock()
        .unwrap()
        .iter()
        .filter_map(|m| m.memberships[0].remote_id.as_ref().map(numeric))
        .collect();
    uids.sort_unstable();
    uids
}

fn inbox_mailbox(account_id: AccountId) -> Mailbox {
    Mailbox::discover(account_id, MailboxPath::inbox(), MailboxType::Inbox).0
}

mod backfill;
mod cursor;
mod lifecycle;
mod pending;
mod reconcile;
