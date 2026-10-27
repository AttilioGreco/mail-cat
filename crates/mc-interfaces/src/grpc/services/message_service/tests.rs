use super::*;

use std::sync::Arc;
use tokio::time::{Duration, timeout};
use tokio_stream::StreamExt as _;
use tonic::Request;

use mc_application::{
    ApplicationError, EventPublisher, InProcessEventBus,
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::{Query, QueryResult},
};
use mc_domain::{
    AccountId, AggregateType, DomainEvent, EventEnvelope, Flag, Mailbox, MailboxId, MailboxPath,
    MailboxType, MessageId, RemoteId,
};

// ── Minimal mock buses ────────────────────────────────────────────────────

struct NopCommandBus;

#[async_trait::async_trait]
impl CommandBus for NopCommandBus {
    async fn dispatch(&self, _: Command) -> Result<CommandResult, ApplicationError> {
        unreachable!()
    }
}

struct SingleMailboxQueryBus(Mailbox);

#[async_trait::async_trait]
impl QueryBus for SingleMailboxQueryBus {
    async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
        match query {
            Query::GetMailbox { .. } => Ok(QueryResult::Mailbox(Some(self.0.clone()))),
            _ => Err(ApplicationError::Internal("unexpected".into())),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn fake_mailbox(account_id: AccountId, mailbox_id: MailboxId) -> Mailbox {
    Mailbox {
        id: mailbox_id,
        account_id,
        path: MailboxPath::inbox(),
        display_name: "INBOX".to_string(),
        mailbox_type: MailboxType::Inbox,
        flags: vec![],
        total_messages: 0,
        unread_messages: 0,
    }
}

fn make_svc(
    bus: Arc<InProcessEventBus>,
    mailbox: Mailbox,
) -> MessageServiceImpl<NopCommandBus, SingleMailboxQueryBus> {
    MessageServiceImpl::new(
        Arc::new(NopCommandBus),
        Arc::new(SingleMailboxQueryBus(mailbox)),
        bus,
    )
}

fn envelope(event: DomainEvent) -> EventEnvelope {
    EventEnvelope::new("test".into(), AggregateType::Message, 1, event)
}

/// Answers with one message and its mailbox; counts body fetches.
struct OneMessage {
    message: mc_domain::Message,
    mailbox: Mailbox,
    fetches: std::sync::Mutex<u32>,
}

#[async_trait::async_trait]
impl QueryBus for OneMessage {
    async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
        match query {
            Query::GetMessage { .. } => {
                Ok(QueryResult::Message(Box::new(Some(self.message.clone()))))
            }
            Query::GetMailbox { .. } => Ok(QueryResult::Mailbox(Some(self.mailbox.clone()))),
            _ => Err(ApplicationError::Internal("unexpected".into())),
        }
    }
}

#[async_trait::async_trait]
impl CommandBus for OneMessage {
    async fn dispatch(&self, command: Command) -> Result<CommandResult, ApplicationError> {
        assert!(matches!(command, Command::FetchMessageBody { .. }));
        *self.fetches.lock().unwrap() += 1;
        Ok(CommandResult::MessageBodyFetched)
    }
}

/// Body fetches opening a message with `body_text` and `truncated` dispatches.
async fn fetches_on_open(body_text: Option<&str>, truncated: bool) -> u32 {
    let (account_id, mailbox_id) = (AccountId::new(), MailboxId::new());
    let (message, _) = mc_domain::Message::receive(
        account_id,
        mailbox_id,
        RemoteId::from(1),
        None,
        None,
        vec![],
        mc_domain::Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "s".into(),
            date: chrono::Utc::now(),
        },
        vec![],
        1,
        false,
        chrono::Utc::now(),
    );
    let message = mc_domain::Message {
        body_text: body_text.map(Into::into),
        body_truncated: truncated,
        ..message
    };
    let buses = Arc::new(OneMessage {
        message: message.clone(),
        mailbox: fake_mailbox(account_id, mailbox_id),
        fetches: Default::default(),
    });
    let svc = MessageServiceImpl::new(
        Arc::clone(&buses),
        Arc::clone(&buses),
        Arc::new(InProcessEventBus::new(16)),
    );
    svc.get_message(Request::new(GetMessageRequest {
        message_id: message.id.to_string(),
        mailbox_id: mailbox_id.to_string(),
    }))
    .await
    .unwrap();
    *buses.fetches.lock().unwrap()
}

#[tokio::test]
async fn opening_a_truncated_body_fetches_the_rest() {
    assert_eq!(fetches_on_open(Some("start"), true).await, 1);
    assert_eq!(fetches_on_open(None, false).await, 1);
    assert_eq!(fetches_on_open(Some("whole"), false).await, 0);
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// MessageReceived domain event reaches the gRPC stream as NewMessage.
#[tokio::test]
async fn new_message_reaches_watch_stream() {
    let bus = Arc::new(InProcessEventBus::new(16));
    let account_id = AccountId::new();
    let mailbox_id = MailboxId::new();
    let message_id = MessageId::new();

    let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
    let mut stream = svc
        .watch_mailbox(Request::new(WatchMailboxRequest {
            mailbox_id: mailbox_id.as_uuid().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    bus.publish(envelope(DomainEvent::MessageReceived {
        message_id,
        account_id,
        mailbox_id,
        remote_id: RemoteId::from(42),
        envelope: mc_domain::Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "Hi".into(),
            date: chrono::Utc::now(),
        },
        flags: vec![],
        size_bytes: 0,
        has_attachments: false,
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        received_at: chrono::Utc::now(),
        restored: false,
    }));

    let event = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("timeout")
        .expect("ended")
        .expect("error");

    assert_eq!(event.mailbox_id, mailbox_id.as_uuid().to_string());
    assert!(matches!(
        event.event_type,
        Some(mailbox_event::EventType::NewMessage(ref n)) if n.remote_id == "42"
    ));
}

/// Events from a different account are filtered out.
#[tokio::test]
async fn events_from_other_account_are_filtered() {
    let bus = Arc::new(InProcessEventBus::new(16));
    let account_id = AccountId::new();
    let other_account = AccountId::new();
    let mailbox_id = MailboxId::new();

    let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
    let mut stream = svc
        .watch_mailbox(Request::new(WatchMailboxRequest {
            mailbox_id: mailbox_id.as_uuid().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    // Wrong account → filtered
    bus.publish(envelope(DomainEvent::MessageReceived {
        message_id: MessageId::new(),
        account_id: other_account,
        mailbox_id,
        remote_id: RemoteId::from(1),
        envelope: mc_domain::Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "".into(),
            date: chrono::Utc::now(),
        },
        flags: vec![],
        size_bytes: 0,
        has_attachments: false,
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        received_at: chrono::Utc::now(),
        restored: false,
    }));
    // Correct account → arrives
    bus.publish(envelope(DomainEvent::MessageReceived {
        message_id: MessageId::new(),
        account_id,
        mailbox_id,
        remote_id: RemoteId::from(2),
        envelope: mc_domain::Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "".into(),
            date: chrono::Utc::now(),
        },
        flags: vec![],
        size_bytes: 0,
        has_attachments: false,
        message_rfc_id: None,
        in_reply_to: None,
        references: vec![],
        received_at: chrono::Utc::now(),
        restored: false,
    }));

    let event = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("timeout")
        .unwrap()
        .unwrap();
    match event.event_type {
        Some(mailbox_event::EventType::NewMessage(n)) => assert_eq!(n.remote_id, "2"),
        other => panic!("unexpected: {:?}", other),
    }
}

/// MailboxCountsUpdated reaches the stream as CountsChanged.
#[tokio::test]
async fn counts_updated_reaches_watch_stream() {
    let bus = Arc::new(InProcessEventBus::new(16));
    let account_id = AccountId::new();
    let mailbox_id = MailboxId::new();

    let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
    let mut stream = svc
        .watch_mailbox(Request::new(WatchMailboxRequest {
            mailbox_id: mailbox_id.as_uuid().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    bus.publish(EventEnvelope::new(
        "mbx".into(),
        AggregateType::Mailbox,
        1,
        DomainEvent::MailboxCountsUpdated {
            mailbox_id,
            total_messages: 100,
            unread_messages: 5,
        },
    ));

    let event = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("timeout")
        .unwrap()
        .unwrap();
    assert!(matches!(
        event.event_type,
        Some(mailbox_event::EventType::CountsChanged(ref c)) if c.total == 100 && c.unseen == 5
    ));
}

/// A body-less message whose UID is unknown is still fetched on open:
/// the handler finds it by Message-ID.
#[tokio::test]
async fn opening_a_message_without_uid_fetches_its_body() {
    struct Recording(std::sync::Mutex<Vec<Command>>);
    #[async_trait::async_trait]
    impl CommandBus for Recording {
        async fn dispatch(&self, command: Command) -> Result<CommandResult, ApplicationError> {
            self.0.lock().unwrap().push(command);
            Ok(CommandResult::MessageBodyFetched)
        }
    }
    struct MessageAndMailbox(mc_domain::Message, Mailbox);
    #[async_trait::async_trait]
    impl QueryBus for MessageAndMailbox {
        async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
            match query {
                Query::GetMessage { .. } => {
                    Ok(QueryResult::Message(Box::new(Some(self.0.clone()))))
                }
                Query::GetMailbox { .. } => Ok(QueryResult::Mailbox(Some(self.1.clone()))),
                _ => Err(ApplicationError::Internal("unexpected".into())),
            }
        }
    }

    let (account_id, mailbox_id) = (AccountId::new(), MailboxId::new());
    let (mut message, _) = mc_domain::Message::receive(
        account_id,
        mailbox_id,
        RemoteId::from(1),
        Some("<x@example.com>".into()),
        None,
        vec![],
        mc_domain::Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "".into(),
            date: chrono::Utc::now(),
        },
        vec![],
        0,
        false,
        chrono::Utc::now(),
    );
    message.memberships[0].remote_id = None;
    let commands = Arc::new(Recording(std::sync::Mutex::new(vec![])));
    let svc = MessageServiceImpl::new(
        Arc::clone(&commands),
        Arc::new(MessageAndMailbox(
            message.clone(),
            fake_mailbox(account_id, mailbox_id),
        )),
        Arc::new(InProcessEventBus::new(16)),
    );

    svc.get_message(Request::new(GetMessageRequest {
        message_id: message.id.to_string(),
        mailbox_id: mailbox_id.to_string(),
    }))
    .await
    .unwrap();

    assert!(matches!(
        commands.0.lock().unwrap().as_slice(),
        [Command::FetchMessageBody { .. }]
    ));
}

/// A request names the mailbox it acts from; a message that is not in it is
/// not found, whatever other mailbox it is in.
#[tokio::test]
async fn acting_from_a_mailbox_the_message_is_not_in_is_not_found() {
    let (account_id, mailbox_id, elsewhere) =
        (AccountId::new(), MailboxId::new(), MailboxId::new());
    let message = message_in(account_id, mailbox_id);
    let svc = MessageServiceImpl::new(
        Arc::new(NopCommandBus),
        Arc::new(MessageInMailbox(
            message.clone(),
            fake_mailbox(account_id, elsewhere),
        )),
        Arc::new(InProcessEventBus::new(16)),
    );
    let (message_id, elsewhere) = (message.id.to_string(), elsewhere.to_string());

    let opened = svc
        .get_message(Request::new(GetMessageRequest {
            message_id: message_id.clone(),
            mailbox_id: elsewhere.clone(),
        }))
        .await
        .unwrap_err();
    let moved = svc
        .move_message(Request::new(MoveMessageRequest {
            message_id: message_id.clone(),
            from_mailbox_id: elsewhere.clone(),
            to_mailbox_id: MailboxId::new().to_string(),
        }))
        .await
        .unwrap_err();
    let deleted = svc
        .delete_message(Request::new(DeleteMessageRequest {
            message_id,
            mailbox_id: elsewhere,
            permanently: true,
        }))
        .await
        .unwrap_err();

    for status in [opened, moved, deleted] {
        assert_eq!(status.code(), tonic::Code::NotFound, "{status:?}");
    }
}

/// Query bus whose one message is in `mailbox` only.
struct MessageInMailbox(mc_domain::Message, Mailbox);

#[async_trait::async_trait]
impl QueryBus for MessageInMailbox {
    async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
        match query {
            Query::GetMessage { message_id } if message_id == self.0.id => {
                Ok(QueryResult::Message(Box::new(Some(self.0.clone()))))
            }
            Query::GetMessage { .. } => Ok(QueryResult::Message(Box::new(None))),
            Query::GetMailbox { .. } => Ok(QueryResult::Mailbox(Some(self.1.clone()))),
            _ => Err(ApplicationError::Internal("unexpected".into())),
        }
    }
}

fn message_in(account_id: AccountId, mailbox_id: MailboxId) -> mc_domain::Message {
    mc_domain::Message::receive(
        account_id,
        mailbox_id,
        mc_domain::RemoteId::from(1),
        None,
        None,
        vec![],
        mc_domain::Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "".into(),
            date: chrono::Utc::now(),
        },
        vec![],
        0,
        false,
        chrono::Utc::now(),
    )
    .0
}

/// Flags belong to the message: a watcher hears of the ones in its mailbox.
#[tokio::test]
async fn flags_changed_reaches_only_the_watchers_of_the_messages_mailbox() {
    let bus = Arc::new(InProcessEventBus::new(16));
    let account_id = AccountId::new();
    let (watched, elsewhere) = (MailboxId::new(), MailboxId::new());
    let (inside, outside) = (
        message_in(account_id, watched),
        message_in(account_id, elsewhere),
    );

    let svc = MessageServiceImpl::new(
        Arc::new(NopCommandBus),
        Arc::new(MessageInMailbox(
            inside.clone(),
            fake_mailbox(account_id, watched),
        )),
        Arc::clone(&bus),
    );
    let mut stream = svc
        .watch_mailbox(Request::new(WatchMailboxRequest {
            mailbox_id: watched.as_uuid().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    for message_id in [outside.id, inside.id] {
        bus.publish(envelope(DomainEvent::MessageFlagged {
            message_id,
            flags_added: vec![Flag::Seen],
            flags_removed: vec![],
        }));
    }

    let event = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("timeout")
        .unwrap()
        .unwrap();
    assert!(matches!(
        event.event_type,
        Some(mailbox_event::EventType::FlagsChanged(ref f)) if f.message_id == inside.id.to_string()
    ));
}

/// Leaving a mailbox, by move or by deletion, reaches its watchers as an expunge.
#[tokio::test]
async fn leaving_a_mailbox_reaches_its_watchers_as_an_expunge() {
    let bus = Arc::new(InProcessEventBus::new(16));
    let account_id = AccountId::new();
    let mailbox_id = MailboxId::new();
    let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
    let mut stream = svc
        .watch_mailbox(Request::new(WatchMailboxRequest {
            mailbox_id: mailbox_id.as_uuid().to_string(),
        }))
        .await
        .unwrap()
        .into_inner();

    let (moved, deleted) = (MessageId::new(), MessageId::new());
    bus.publish(envelope(DomainEvent::MessageMailboxesChanged {
        message_id: moved,
        added: vec![mc_domain::Membership {
            mailbox_id: MailboxId::new(),
            remote_id: None,
        }],
        removed: vec![mailbox_id],
    }));
    bus.publish(envelope(DomainEvent::MessageDeleted {
        message_id: deleted,
        mailbox_ids: vec![mailbox_id],
    }));

    for expected in [moved, deleted] {
        let event = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("timeout")
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.event_type,
            Some(mailbox_event::EventType::MessageExpunged(ref e)) if e.message_id == expected.to_string()
        ));
    }
}
