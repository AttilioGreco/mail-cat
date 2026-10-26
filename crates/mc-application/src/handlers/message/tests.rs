use super::*;
use mc_domain::{
    AggregateType, DomainError, EventEnvelope, Existing, Mailbox, MailboxChanges, MailboxCounts,
    Membership, MessageEnvelopeSummary, ProviderNotification, RemoteId, SyncCursor,
};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::RwLock;

/// The only mailbox a freshly received message is in.
fn mailbox_of(message: &Message) -> MailboxId {
    message.memberships[0].mailbox_id
}

fn uid_of(message: &Message) -> Option<RemoteId> {
    message.memberships[0].remote_id.clone()
}
mod delete;
mod fetch_body;
mod flags;
mod mocks;
mod moves;
mod send_compose;
mod uid_lookup;
use mocks::*;

use mc_domain::{Address, EmailAddress};

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
        subject: "Test".into(),
        date: chrono::Utc::now(),
    }
}

fn make_test_message() -> Message {
    let (msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        RemoteId::from(42),
        None,
        None,
        vec![],
        test_envelope(),
        vec![],
        2048,
        false,
        chrono::Utc::now(),
    );
    msg
}

/// The INBOX `msg` sits in.
fn inbox_of(msg: &Message) -> mc_domain::Mailbox {
    mc_domain::Mailbox {
        id: mailbox_of(msg),
        ..test_mailbox(msg.account_id, "INBOX", MailboxType::Inbox)
    }
}

fn test_account(email: &str, host: &str) -> Account {
    let email = EmailAddress::new(email).unwrap();
    Account::create(
        "Lavoro".into(),
        email.clone(),
        mc_domain::ProviderType::Imap,
        mc_domain::ConnectionConfig {
            incoming: mc_domain::IncomingServer::Imap {
                host: host.into(),
                port: 993,
                use_tls: true,
            },
            auth: mc_domain::AuthConfig::Plain {
                username: "u".into(),
                password: String::new(),
            },
            smtp: None,
        },
        mc_domain::Identity {
            display_name: "Mario Rossi".into(),
            email,
            signature_html: None,
            signature_text: Some("Mario".into()),
        },
    )
    .0
}

fn test_mailbox(account_id: AccountId, name: &str, kind: MailboxType) -> mc_domain::Mailbox {
    mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id,
        path: MailboxPath::parse(name, '/').unwrap(),
        display_name: name.into(),
        mailbox_type: kind,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    }
}

fn compose_input(account_id: AccountId) -> ComposeInput {
    ComposeInput {
        draft_id: None,
        account_id,
        to: vec!["Anna <anna@example.com>".into()],
        cc: vec![],
        bcc: vec![],
        subject: "Ciao".into(),
        body_md: "**ciao**".into(),
        mode: ComposeMode::New,
        source_message_id: None,
        attachments: vec![mc_domain::OutgoingAttachment {
            filename: "a.pdf".into(),
            mime_type: "application/pdf".into(),
            content_id: None,
            data: vec![1, 2],
        }],
        forwarded_attachment_ids: vec![],
    }
}

/// Let spawned fire-and-forget provider calls run.
async fn drain_spawned() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

struct Fixture {
    account: Account,
    provider: Arc<MockEmailProvider>,
    bus: Arc<MockEventBus>,
    sent_box: mc_domain::Mailbox,
    drafts_box: mc_domain::Mailbox,
    /// A message stored in INBOX (event store + read model).
    seeded: Option<Message>,
    handler: MessageCommandHandler<
        MockEventStore,
        MockProviderRegistry,
        MockMessageReadRepo,
        MockEventBus,
    >,
}

fn fixture(host: &str, seed: impl FnOnce(AccountId, MailboxId) -> Option<Message>) -> Fixture {
    let account = test_account("mario@example.com", host);
    let inbox = test_mailbox(account.id, "INBOX", MailboxType::Inbox);
    let seeded = seed(account.id, inbox.id);
    let sent_box = test_mailbox(account.id, "Sent", MailboxType::Sent);
    let drafts_box = test_mailbox(account.id, "Drafts", MailboxType::Drafts);
    let provider = Arc::new(MockEmailProvider::new());
    let bus = Arc::new(MockEventBus::new());
    let es = Arc::new(match &seeded {
        Some(m) => MockEventStore::with_message(m),
        None => MockEventStore::new(),
    });
    let mut repo =
        MockMessageReadRepo::with_mailboxes(vec![inbox, sent_box.clone(), drafts_box.clone()]);
    repo.messages.extend(seeded.clone());
    let handler = MessageCommandHandler::new(
        es,
        Arc::new(MockProviderRegistry::new(provider.clone())),
        Arc::new(repo),
        bus.clone(),
    );
    Fixture {
        account,
        provider,
        bus,
        sent_box,
        drafts_box,
        seeded,
        handler,
    }
}

fn source_message(account_id: AccountId, mailbox_id: MailboxId) -> Message {
    let (mut msg, _) = Message::receive(
        account_id,
        mailbox_id,
        RemoteId::from(42),
        Some("<orig@example.com>".into()),
        Some("<parent@example.com>".into()),
        vec!["<root@example.com>".into(), "<parent@example.com>".into()],
        Envelope {
            from: vec![Address {
                name: Some("Anna".into()),
                email: EmailAddress::new("anna@example.com").unwrap(),
            }],
            to: vec![Address {
                name: None,
                email: EmailAddress::new("mario@example.com").unwrap(),
            }],
            cc: vec![Address {
                name: None,
                email: EmailAddress::new("bob@example.com").unwrap(),
            }],
            bcc: vec![],
            reply_to: vec![],
            subject: "R: Budget".into(),
            date: chrono::Utc::now(),
        },
        vec![],
        100,
        false,
        chrono::Utc::now(),
    );
    msg.body_text = Some("testo originale".into());
    msg
}

fn uidless_in_archive() -> (Message, mc_domain::Mailbox, Arc<MockEventStore>) {
    let (msg, _) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        RemoteId::from(42),
        Some("<moved@example.com>".into()),
        None,
        vec![],
        test_envelope(),
        vec![],
        2048,
        false,
        chrono::Utc::now(),
    );
    let archive = mc_domain::Mailbox {
        id: MailboxId::new(),
        account_id: msg.account_id,
        path: MailboxPath::parse("Archive", '/').unwrap(),
        display_name: "Archive".into(),
        mailbox_type: MailboxType::Archive,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    };
    let es = Arc::new(MockEventStore::with_message(&msg));
    es.events
        .write()
        .unwrap()
        .get_mut(&msg.id.to_string())
        .unwrap()
        .push(EventEnvelope::new(
            msg.id.to_string(),
            AggregateType::Message,
            2,
            DomainEvent::MessageMailboxesChanged {
                message_id: msg.id,
                added: vec![Membership {
                    mailbox_id: archive.id,
                    remote_id: None,
                }],
                removed: vec![mailbox_of(&msg)],
            },
        ));
    (msg, archive, es)
}

struct Moving {
    handler: MessageCommandHandler<
        MockEventStore,
        MockProviderRegistry,
        MockMessageReadRepo,
        MockEventBus,
    >,
    provider: Arc<MockEmailProvider>,
    es: Arc<MockEventStore>,
    archive: mc_domain::Mailbox,
    trash: mc_domain::Mailbox,
    resync: tokio::sync::mpsc::UnboundedReceiver<(AccountId, MailboxPath)>,
}

impl Moving {
    fn resync_requests(&mut self) -> Vec<MailboxPath> {
        std::iter::from_fn(|| self.resync.try_recv().ok().map(|(_, path)| path)).collect()
    }

    async fn location(&self, id: MessageId) -> (MailboxId, Option<RemoteId>) {
        let (stored, _) = self.handler.load_message(&id).await.unwrap();
        (mailbox_of(&stored), uid_of(&stored))
    }
}

/// INBOX, Archive and Trash of `msg`'s account, and a handler over them
/// that reports resync requests.
fn moving(msg: &Message, provider: MockEmailProvider) -> Moving {
    moving_with(msg, provider, MockMessageReadRepo::default())
}

fn moving_with(msg: &Message, provider: MockEmailProvider, repo: MockMessageReadRepo) -> Moving {
    let archive = test_mailbox(msg.account_id, "Archive", MailboxType::Archive);
    let trash = test_mailbox(msg.account_id, "Trash", MailboxType::Trash);
    let provider = Arc::new(provider);
    let es = Arc::new(MockEventStore::with_message(msg));
    let (tx, resync) = tokio::sync::mpsc::unbounded_channel();
    let repo = MockMessageReadRepo {
        mailboxes: vec![inbox_of(msg), archive.clone(), trash.clone()],
        ..repo
    };
    let handler = MessageCommandHandler::new(
        es.clone(),
        Arc::new(MockProviderRegistry::new(provider.clone())),
        Arc::new(repo),
        Arc::new(MockEventBus::new()),
    )
    .with_resync_requests(tx);
    Moving {
        handler,
        provider,
        es,
        archive,
        trash,
        resync,
    }
}

fn with_rfc_id(rfc_id: &str) -> Message {
    Message::receive(
        AccountId::new(),
        MailboxId::new(),
        RemoteId::from(42),
        Some(rfc_id.into()),
        None,
        vec![],
        test_envelope(),
        vec![],
        0,
        false,
        chrono::Utc::now(),
    )
    .0
}
