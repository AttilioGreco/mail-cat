//! The label classifier end to end on a real database: existing labels train
//! it, it suggests, takes feedback, and applies a label by itself on arrival.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tokio_util::sync::CancellationToken;

use mc_application::{
    ApplicationError, Command, CommandBus, CommandResult, EventPublisher, InProcessEventBus,
    TaggingService,
};
use mc_domain::{
    AccountId, Address, AggregateType, DomainEvent, EmailAddress, Envelope, EventStore, Flag,
    GroupId, LabelMode, MailboxId, MailboxModel, MailboxPath, MailboxType, Membership, MessageId,
    ProviderType, ReadModelProjector, RemoteId,
};
mod curation;
mod discovery;
mod embeddings;

use mc_infrastructure::{SqliteDb, SqliteEventStore, SqliteRepository, SqliteTaggingStore};

#[derive(Default)]
/// The commands seen, and the mailbox to answer label creations with.
struct Recording(Mutex<Vec<Command>>, Mutex<Option<MailboxId>>);

#[async_trait]
impl CommandBus for Recording {
    async fn dispatch(&self, command: Command) -> Result<CommandResult, ApplicationError> {
        let created = matches!(command, Command::CreateLabel { .. });
        self.0.lock().unwrap().push(command);
        if created {
            return Ok(CommandResult::LabelCreated {
                mailbox_id: self.1.lock().unwrap().unwrap_or_default(),
            });
        }
        Ok(CommandResult::MessagesBulkChanged {
            succeeded: 1,
            failed: 0,
        })
    }
}

struct World {
    _dir: tempfile::TempDir,
    db: Arc<SqliteDb>,
    events: Arc<SqliteEventStore>,
    repo: SqliteRepository,
    account: AccountId,
    inbox: MailboxId,
    label: MailboxId,
}

impl World {
    async fn record(
        &self,
        aggregate: String,
        kind: AggregateType,
        event: DomainEvent,
    ) -> mc_domain::EventEnvelope {
        let sequence = self.events.current_sequence(&aggregate).await.unwrap();
        self.events
            .append(&aggregate, kind, sequence, vec![event])
            .await
            .unwrap();
        let envelope = self
            .events
            .load_events(&aggregate, sequence + 1)
            .await
            .unwrap()
            .remove(0);
        self.repo.project(&envelope).await.unwrap();
        envelope
    }

    async fn open() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
        let events = Arc::new(SqliteEventStore::new(db.clone()));
        let mut world = Self {
            repo: SqliteRepository::new(db.clone(), events.clone()),
            db,
            events,
            account: AccountId::new(),
            inbox: MailboxId::new(),
            label: MailboxId::new(),
            _dir: dir,
        };
        let account = world.account;
        world
            .record(
                account.to_string(),
                AggregateType::Account,
                DomainEvent::AccountAdded {
                    account_id: account,
                    name: "a".into(),
                    email: EmailAddress::new("a@x.example").unwrap(),
                    provider_type: ProviderType::Jmap,
                },
            )
            .await;
        world
            .record(
                account.to_string(),
                AggregateType::Account,
                DomainEvent::AccountMailboxModelChanged {
                    account_id: account,
                    model: MailboxModel::Labels,
                },
            )
            .await;
        for (id, name, kind) in [
            (world.inbox, "INBOX", MailboxType::Inbox),
            (world.label, "Newsletter", MailboxType::Custom),
        ] {
            world
                .record(
                    id.to_string(),
                    AggregateType::Mailbox,
                    DomainEvent::MailboxDiscovered {
                        mailbox_id: id,
                        account_id: account,
                        path: MailboxPath::new(vec![name.into()], '/').unwrap(),
                        display_path: name.into(),
                        mailbox_type: kind,
                    },
                )
                .await;
        }
        world.account = account;
        world
    }

    /// A message in the Inbox (and under the label when `labelled`); returns
    /// its arrival event too.
    async fn message(
        &self,
        from: &str,
        subject: &str,
        labelled: bool,
    ) -> (MessageId, mc_domain::EventEnvelope) {
        let (account, inbox) = (self.account, self.inbox);
        let label = labelled.then_some(self.label);
        self.message_in(account, inbox, label, vec![], from, subject)
            .await
    }

    /// Another account of the same group, with just an Inbox.
    async fn other_account(
        &self,
        email: &str,
        model: MailboxModel,
        tags: bool,
    ) -> (AccountId, MailboxId) {
        let (account, inbox) = (AccountId::new(), MailboxId::new());
        for event in [
            DomainEvent::AccountAdded {
                account_id: account,
                name: email.into(),
                email: EmailAddress::new(email).unwrap(),
                provider_type: ProviderType::Imap,
            },
            DomainEvent::AccountMailboxModelChanged {
                account_id: account,
                model,
            },
            DomainEvent::AccountTagsDetected {
                account_id: account,
                supported: tags,
            },
        ] {
            self.record(account.to_string(), AggregateType::Account, event)
                .await;
        }
        let discovered = DomainEvent::MailboxDiscovered {
            mailbox_id: inbox,
            account_id: account,
            path: MailboxPath::new(vec!["INBOX".into()], '/').unwrap(),
            display_path: "INBOX".into(),
            mailbox_type: MailboxType::Inbox,
        };
        self.record(inbox.to_string(), AggregateType::Mailbox, discovered)
            .await;
        (account, inbox)
    }

    async fn message_in(
        &self,
        account: AccountId,
        inbox: MailboxId,
        label: Option<MailboxId>,
        flags: Vec<Flag>,
        from: &str,
        subject: &str,
    ) -> (MessageId, mc_domain::EventEnvelope) {
        self.message_at(account, inbox, label, flags, from, subject, Utc::now())
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn message_at(
        &self,
        account: AccountId,
        inbox: MailboxId,
        label: Option<MailboxId>,
        flags: Vec<Flag>,
        from: &str,
        subject: &str,
        received_at: chrono::DateTime<Utc>,
    ) -> (MessageId, mc_domain::EventEnvelope) {
        let id = MessageId::new();
        let arrival = self
            .record(
                id.to_string(),
                AggregateType::Message,
                DomainEvent::MessageReceived {
                    message_id: id,
                    account_id: account,
                    mailbox_id: inbox,
                    remote_id: RemoteId::from(1),
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    envelope: Envelope {
                        from: vec![Address {
                            name: None,
                            email: EmailAddress::new(from).unwrap(),
                        }],
                        to: vec![],
                        cc: vec![],
                        bcc: vec![],
                        reply_to: vec![],
                        subject: subject.into(),
                        date: Utc::now(),
                    },
                    flags,
                    size_bytes: 1,
                    has_attachments: false,
                    received_at,
                    restored: false,
                    provider_key: None,
                    list_headers: Default::default(),
                },
            )
            .await;
        if let Some(label) = label {
            self.record(
                id.to_string(),
                AggregateType::Message,
                DomainEvent::MessageMailboxesChanged {
                    message_id: id,
                    added: vec![Membership {
                        mailbox_id: label,
                        remote_id: Some(RemoteId::from(2)),
                    }],
                    removed: vec![],
                },
            )
            .await;
        }
        (id, arrival)
    }
}

type Service = TaggingService<SqliteTaggingStore, SqliteRepository, Recording>;

#[tokio::test]
async fn learns_from_labels_suggests_takes_feedback_and_applies() {
    let world = World::open().await;
    let db = world.db.clone();
    let commands = Arc::new(Recording::default());
    let service = Arc::new(Service::new(
        SqliteTaggingStore::new(db),
        world.repo.clone(),
        commands.clone(),
    ));

    for i in 0..8 {
        world
            .message(
                "news@shop.example",
                &format!("offerte della settimana {i}"),
                true,
            )
            .await;
        world
            .message("anna@family.example", &format!("cena domenica {i}"), false)
            .await;
    }
    // Moving the account out and back is how a group is retrained from scratch.
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();

    let status = service
        .label_status(world.account, "NEWSLETTER")
        .await
        .unwrap();
    assert_eq!(
        (status.settings.label.as_str(), status.positives),
        ("newsletter", 8)
    );

    let (probe, _) = world
        .message("news@shop.example", "offerte speciali", false)
        .await;
    assert!(
        service
            .tags_for(probe)
            .await
            .unwrap()
            .suggestions
            .is_empty(),
        "off by default"
    );

    service
        .set_label(world.account, "Newsletter", LabelMode::Suggest, 0.9)
        .await
        .unwrap();
    let tags = service.tags_for(probe).await.unwrap();
    assert_eq!(tags.suggestions.len(), 1);
    assert_eq!(tags.suggestions[0].mailbox_id, Some(world.label));

    let (stranger, _) = world.message("anna@family.example", "cena", false).await;
    assert!(
        service
            .tags_for(stranger)
            .await
            .unwrap()
            .suggestions
            .is_empty()
    );

    service.reject(probe, "newsletter").await.unwrap();
    assert!(
        service
            .tags_for(probe)
            .await
            .unwrap()
            .suggestions
            .is_empty(),
        "a rejected suggestion stays away"
    );

    service
        .set_label(world.account, "newsletter", LabelMode::ApplyArchive, 0.9)
        .await
        .unwrap();
    let bus = InProcessEventBus::new(16);
    let shutdown = CancellationToken::new();
    service.spawn(bus.subscribe(), shutdown.clone());
    let (fresh, arrival) = world
        .message("news@shop.example", "offerte della settimana nove", false)
        .await;
    bus.publish(arrival);

    let mut applied = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(Command::ChangeLabels {
            message_ids,
            add,
            remove,
        }) = commands.0.lock().unwrap().first().cloned()
        {
            applied = Some((message_ids, add, remove));
            break;
        }
    }
    shutdown.cancel();
    let (ids, add, remove) = applied.expect("the label was applied by itself");
    assert_eq!(
        (ids, add, remove),
        (vec![fresh], vec![world.label], vec![world.inbox])
    );
}

#[tokio::test]
async fn a_label_needs_examples_before_it_can_act() {
    let world = World::open().await;
    let service = Service::new(
        SqliteTaggingStore::new(world.db.clone()),
        world.repo.clone(),
        Arc::new(Recording::default()),
    );
    world.message("news@shop.example", "offerte", true).await;
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();
    let refused = service
        .set_label(world.account, "newsletter", LabelMode::Apply, 0.9)
        .await;
    assert!(
        matches!(refused, Err(ApplicationError::PreconditionFailed(m)) if m.contains("esempi"))
    );
}

/// A group with a labelled account and a second one, trained on `news@shop`.
async fn group_with_taught_label() -> (World, Arc<Service>, Arc<Recording>) {
    let world = World::open().await;
    let commands = Arc::new(Recording::default());
    let service = Arc::new(
        Service::new(
            SqliteTaggingStore::new(world.db.clone()),
            world.repo.clone(),
            commands.clone(),
        )
        .with_page_size(2),
    );
    for i in 0..8 {
        world
            .message("news@shop.example", &format!("offerte {i}"), true)
            .await;
        world
            .message("anna@family.example", &format!("cena {i}"), false)
            .await;
    }
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();
    (world, service, commands)
}

async fn commands_after_apply(commands: &Recording, count: usize) -> Vec<Command> {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let seen = commands.0.lock().unwrap().clone();
        if seen.len() >= count {
            return seen;
        }
    }
    panic!("expected {count} commands");
}

#[tokio::test]
async fn a_label_the_account_lacks_is_created_before_it_is_applied() {
    let (world, service, commands) = group_with_taught_label().await;
    let (other, inbox) = world
        .other_account("b@x.example", MailboxModel::Labels, true)
        .await;
    let (stray, _) = world
        .message_in(other, inbox, None, vec![], "news@shop.example", "offerte 9")
        .await;

    let preview = service
        .preview_apply(world.account, "newsletter", LabelMode::Apply, 0.9)
        .await
        .unwrap();
    assert_eq!(
        preview.to_label, 1,
        "the other account counts, label or not"
    );
    // The same result whatever the page size: one read per message or many.
    let whole = Service::new(
        SqliteTaggingStore::new(world.db.clone()),
        world.repo.clone(),
        Arc::new(Recording::default()),
    );
    let one_page = whole
        .preview_apply(world.account, "newsletter", LabelMode::Apply, 0.9)
        .await
        .unwrap();
    assert_eq!(one_page.to_label, preview.to_label);
    assert_eq!(one_page.sample[0].message_id, preview.sample[0].message_id);

    service
        .confirm_apply(world.account, "newsletter", LabelMode::Apply, 0.9)
        .await
        .unwrap();
    let seen = commands_after_apply(&commands, 2).await;
    assert!(
        matches!(&seen[0], Command::CreateLabel { account_id, name } if *account_id == other && name == "Newsletter")
    );
    assert!(
        matches!(&seen[1], Command::ChangeLabels { message_ids, .. } if *message_ids == [stray])
    );
}

#[tokio::test]
async fn a_folders_account_takes_part_through_its_keywords() {
    let (world, service, commands) = group_with_taught_label().await;
    let (folders, inbox) = world
        .other_account("c@x.example", MailboxModel::Folders, true)
        .await;
    for i in 0..3 {
        let tagged = vec![Flag::Custom("Newsletter".into())];
        let subject = format!("offerte k{i}");
        let args = (folders, inbox, None, tagged);
        world
            .message_in(
                args.0,
                args.1,
                args.2,
                args.3,
                "news@shop.example",
                &subject,
            )
            .await;
    }
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();
    let status = service
        .label_status(world.account, "newsletter")
        .await
        .unwrap();
    assert_eq!(status.positives, 11, "keywords teach like mailbox labels");

    let (stray, _) = world
        .message_in(
            folders,
            inbox,
            None,
            vec![],
            "news@shop.example",
            "offerte 9",
        )
        .await;
    service
        .confirm_apply(world.account, "newsletter", LabelMode::ApplyArchive, 0.9)
        .await
        .unwrap();
    let seen = commands_after_apply(&commands, 2).await;
    assert!(matches!(
        &seen[0],
        Command::SetMessageFlags { message_id, flags_to_add, .. }
            if *message_id == stray && *flags_to_add == [Flag::Custom("newsletter".into())]
    ));
    assert!(
        matches!(&seen[1], Command::ArchiveMessages { message_ids } if *message_ids == [stray])
    );
}

/// A mailbox of `kind` on the world's account.
async fn mailbox_of(world: &World, name: &str, kind: MailboxType) -> MailboxId {
    let id = MailboxId::new();
    let discovered = DomainEvent::MailboxDiscovered {
        mailbox_id: id,
        account_id: world.account,
        path: MailboxPath::new(vec![name.into()], '/').unwrap(),
        display_path: name.into(),
        mailbox_type: kind,
    };
    world
        .record(id.to_string(), AggregateType::Mailbox, discovered)
        .await;
    id
}

/// Whether anything is done by itself to a news message that arrives in
/// `mailbox` `age` old, with the label on `mode` (`None`: left off).
async fn acts_on_arrival(
    mode: Option<LabelMode>,
    age: chrono::Duration,
    kind: MailboxType,
) -> bool {
    let (world, service, commands) = group_with_taught_label().await;
    if let Some(mode) = mode {
        service
            .set_label(world.account, "newsletter", mode, 0.9)
            .await
            .unwrap();
    }
    let mailbox = match kind {
        MailboxType::Inbox => world.inbox,
        kind => mailbox_of(&world, "Elsewhere", kind).await,
    };
    let bus = InProcessEventBus::new(16);
    let shutdown = CancellationToken::new();
    service.spawn(bus.subscribe(), shutdown.clone());
    let (_, arrival) = world
        .message_at(
            world.account,
            mailbox,
            None,
            vec![],
            "news@shop.example",
            "offerte della settimana nove",
            Utc::now() - age,
        )
        .await;
    bus.publish(arrival);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let acted = !commands.0.lock().unwrap().is_empty();
    shutdown.cancel();
    acted
}

#[tokio::test]
async fn live_auto_apply_waits_for_the_apply_mode_and_fresh_readable_mail() {
    let fresh = chrono::Duration::zero();
    let apply = Some(LabelMode::Apply);
    assert!(acts_on_arrival(apply, fresh, MailboxType::Inbox).await);
    assert!(
        !acts_on_arrival(None, fresh, MailboxType::Inbox).await,
        "off"
    );
    assert!(
        !acts_on_arrival(Some(LabelMode::Suggest), fresh, MailboxType::Inbox).await,
        "suggest"
    );
    let old = chrono::Duration::days(15);
    assert!(
        !acts_on_arrival(apply, old, MailboxType::Inbox).await,
        "old"
    );
    for kind in [MailboxType::Trash, MailboxType::Spam, MailboxType::Drafts] {
        assert!(!acts_on_arrival(apply, fresh, kind).await, "{kind:?}");
    }
}

#[tokio::test]
async fn a_rejection_survives_retraining() {
    let (world, service, _) = group_with_taught_label().await;
    service
        .set_label(world.account, "newsletter", LabelMode::Suggest, 0.9)
        .await
        .unwrap();
    let (probe, _) = world
        .message("news@shop.example", "offerte speciali", false)
        .await;
    assert_eq!(service.tags_for(probe).await.unwrap().suggestions.len(), 1);
    service.reject(probe, "newsletter").await.unwrap();

    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();
    assert!(
        service
            .tags_for(probe)
            .await
            .unwrap()
            .suggestions
            .is_empty()
    );
    let status = service
        .label_status(world.account, "newsletter")
        .await
        .unwrap();
    assert_eq!(status.positives, 8, "the counts are rebuilt, not doubled");
}

#[tokio::test]
async fn removing_an_account_takes_its_lessons_out_of_the_group() {
    let (world, service, _) = group_with_taught_label().await;
    let (folders, inbox) = world
        .other_account("c@x.example", MailboxModel::Folders, true)
        .await;
    for i in 0..3 {
        let tagged = vec![Flag::Custom("Newsletter".into())];
        let subject = format!("offerte k{i}");
        world
            .message_in(folders, inbox, None, tagged, "news@shop.example", &subject)
            .await;
    }
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();
    let positives = |service: Arc<Service>| async move {
        service
            .label_status(world.account, "newsletter")
            .await
            .unwrap()
            .positives
    };
    assert_eq!(positives(service.clone()).await, 11);

    world.repo.purge_account(folders, None).await.unwrap();
    service.retrain_pending().await.unwrap();
    assert_eq!(positives(service.clone()).await, 8);
}

#[tokio::test]
async fn a_folders_account_that_drops_keywords_is_not_labelled() {
    let (world, service, commands) = group_with_taught_label().await;
    let (folders, inbox) = world
        .other_account("c@x.example", MailboxModel::Folders, false)
        .await;
    let (stray, _) = world
        .message_in(
            folders,
            inbox,
            None,
            vec![],
            "news@shop.example",
            "offerte 9",
        )
        .await;
    let preview = service
        .preview_apply(world.account, "newsletter", LabelMode::Apply, 0.9)
        .await
        .unwrap();
    assert_eq!(preview.to_label, 0);
    assert!(service.accept(stray, "newsletter").await.is_err());
    assert!(commands.0.lock().unwrap().is_empty());
}
