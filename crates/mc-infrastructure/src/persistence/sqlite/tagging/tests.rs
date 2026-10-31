use chrono::Utc;
use mc_domain::{
    AccountId, AggregateType, DomainEvent, EmailAddress, Envelope, EventStore, Flag, GroupId,
    LabelMode, LabelSettings, Lesson, ListHeaders, MailboxId, MailboxModel, MailboxPath,
    MailboxType, Membership, MessageId, ProviderType, ReadModelProjector, RemoteId, Scope,
    SourceFilter, TaggingStore,
};

use super::*;
use crate::persistence::sqlite::event_store::SqliteEventStore;
use crate::persistence::sqlite::repository::SqliteRepository;

struct Fixture {
    _dir: tempfile::TempDir,
    events: Arc<SqliteEventStore>,
    repo: SqliteRepository,
    store: SqliteTaggingStore,
}

fn open() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
    let events = Arc::new(SqliteEventStore::new(db.clone()));
    Fixture {
        repo: SqliteRepository::new(db.clone(), events.clone()),
        store: SqliteTaggingStore::new(db),
        events,
        _dir: dir,
    }
}

impl Fixture {
    async fn record(&self, aggregate: String, kind: AggregateType, event: DomainEvent) {
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
    }

    async fn mailbox(&self, account: AccountId, name: &str, kind: MailboxType) -> MailboxId {
        let id = MailboxId::new();
        self.record(
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
        id
    }

    async fn account(&self, address: &str, model: MailboxModel) -> AccountId {
        let account = AccountId::new();
        self.record(
            account.to_string(),
            AggregateType::Account,
            DomainEvent::AccountAdded {
                account_id: account,
                name: address.into(),
                email: EmailAddress::new(address).unwrap(),
                provider_type: ProviderType::Imap,
            },
        )
        .await;
        self.record(
            account.to_string(),
            AggregateType::Account,
            DomainEvent::AccountMailboxModelChanged {
                account_id: account,
                model,
            },
        )
        .await;
        account
    }

    /// A message in `inbox`, and in each of `labels` too.
    async fn message(
        &self,
        account: AccountId,
        inbox: MailboxId,
        labels: &[MailboxId],
        list_headers: ListHeaders,
    ) -> MessageId {
        let id = MessageId::new();
        self.record(
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
                    from: vec![],
                    to: vec![],
                    cc: vec![],
                    bcc: vec![],
                    reply_to: vec![],
                    subject: "offerte".into(),
                    date: Utc::now(),
                },
                flags: vec![],
                size_bytes: 1,
                has_attachments: false,
                received_at: Utc::now(),
                restored: false,
                provider_key: None,
                list_headers,
            },
        )
        .await;
        if !labels.is_empty() {
            self.record(
                id.to_string(),
                AggregateType::Message,
                DomainEvent::MessageMailboxesChanged {
                    message_id: id,
                    added: labels
                        .iter()
                        .map(|&mailbox_id| Membership {
                            mailbox_id,
                            remote_id: Some(RemoteId::from(2)),
                        })
                        .collect(),
                    removed: vec![],
                },
            )
            .await;
        }
        id
    }
}

#[tokio::test]
async fn accounts_start_in_the_default_group_and_can_leave_it() {
    let f = open();
    let a = f.account("a@x.example", MailboxModel::Labels).await;
    let b = f.account("b@x.example", MailboxModel::Labels).await;
    assert_eq!(
        f.store.scope_of(a).await.unwrap(),
        Scope::Group(GroupId::DEFAULT)
    );
    let groups = f.store.list_groups().await.unwrap();
    assert_eq!(
        (groups[0].name.as_str(), groups[0].accounts.len()),
        ("Tutti", 2)
    );

    f.store.assign_account(b, None).await.unwrap();
    assert_eq!(f.store.scope_of(b).await.unwrap(), Scope::Alone(b));
    let work = f.store.create_group("Lavoro").await.unwrap();
    f.store.assign_account(a, Some(work.id)).await.unwrap();
    assert_eq!(
        f.store.scope_accounts(Scope::Group(work.id)).await.unwrap(),
        [a]
    );
    assert!(
        f.store
            .scope_accounts(Scope::Group(GroupId::DEFAULT))
            .await
            .unwrap()
            .is_empty()
    );

    f.store.delete_group(work.id).await.unwrap();
    assert_eq!(
        f.store.scope_of(a).await.unwrap(),
        Scope::Group(GroupId::DEFAULT)
    );
    assert!(f.store.delete_group(GroupId::DEFAULT).await.is_err());
}

#[tokio::test]
async fn labels_of_one_name_are_one_label_across_a_groups_accounts() {
    let f = open();
    let (a, b) = (
        f.account("a@x.example", MailboxModel::Labels).await,
        f.account("b@x.example", MailboxModel::Labels).await,
    );
    let mut messages = Vec::new();
    for (account, name) in [(a, "Newsletter"), (b, "newsletter")] {
        let inbox = f.mailbox(account, "INBOX", MailboxType::Inbox).await;
        let label = f.mailbox(account, name, MailboxType::Custom).await;
        let headers = ListHeaders {
            unsubscribe: true,
            ..ListHeaders::default()
        };
        messages.push(f.message(account, inbox, &[label], headers).await);
    }
    let accounts = f
        .store
        .scope_accounts(Scope::Group(GroupId::DEFAULT))
        .await
        .unwrap();
    let page = f
        .store
        .source_page(&accounts, SourceFilter::Labelled, None, false, 10)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert!(
        page.iter()
            .all(|s| s.labels == ["newsletter"] && s.list_headers.unsubscribe && s.in_inbox)
    );
    assert!(
        f.store
            .source_page(&accounts, SourceFilter::Unlabelled, None, true, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_folders_accounts_labels_are_its_keywords() {
    let f = open();
    let a = f.account("a@x.example", MailboxModel::Folders).await;
    let inbox = f.mailbox(a, "INBOX", MailboxType::Inbox).await;
    f.mailbox(a, "Receipts", MailboxType::Custom).await;
    let tagged = f.message(a, inbox, &[], ListHeaders::default()).await;
    let plain = f.message(a, inbox, &[], ListHeaders::default()).await;
    let system = f.message(a, inbox, &[], ListHeaders::default()).await;
    for (id, flags) in [
        (tagged, vec![Flag::Seen, Flag::Custom("My_Label".into())]),
        (system, vec![Flag::Custom("$Forwarded".into())]),
    ] {
        f.record(
            id.to_string(),
            AggregateType::Message,
            DomainEvent::MessageFlagged {
                message_id: id,
                flags_added: flags,
                flags_removed: vec![],
            },
        )
        .await;
    }

    let sources = f.store.sources(&[tagged, plain, system]).await.unwrap();
    let by = |id| sources.iter().find(|s| s.id == id).unwrap();
    assert_eq!(by(tagged).labels, ["my label"]);
    assert!(by(tagged).keywords && by(tagged).folder == Some(inbox));
    assert!(by(plain).labels.is_empty() && by(system).labels.is_empty());

    let accounts = [a];
    let page = |filter| f.store.source_page(&accounts, filter, None, false, 10);
    let labelled = page(SourceFilter::Labelled).await.unwrap();
    assert_eq!(labelled.iter().map(|s| s.id).collect::<Vec<_>>(), [tagged]);
    assert_eq!(page(SourceFilter::Unlabelled).await.unwrap().len(), 2);
}

#[tokio::test]
async fn teaching_adds_and_withdraws_counts() {
    let f = open();
    let scope = Scope::Group(GroupId::DEFAULT);
    let tokens = vec!["f:a@x".to_string(), "s:offerte".to_string()];
    let labels = vec!["newsletter".to_string()];
    let lesson = |weight| Lesson {
        tokens: &tokens,
        positives: &labels,
        negatives: &[],
        weight,
    };
    f.store.teach(scope, lesson(1.0)).await.unwrap();
    f.store.teach(scope, lesson(1.0)).await.unwrap();

    let stats = f.store.stats(scope, &tokens).await.unwrap();
    assert_eq!(stats.documents, 2.0);
    assert_eq!(stats.corpus["s:offerte"], 2.0);
    assert_eq!(stats.labels["newsletter"].tokens["f:a@x"], (2.0, 0.0));
    assert_eq!(
        f.store.positives(scope).await.unwrap(),
        [("newsletter".to_string(), 2)]
    );

    f.store.teach(scope, lesson(-1.0)).await.unwrap();
    assert_eq!(f.store.stats(scope, &tokens).await.unwrap().documents, 1.0);

    f.store.reset(scope).await.unwrap();
    assert_eq!(f.store.stats(scope, &tokens).await.unwrap().documents, 0.0);
}

#[tokio::test]
async fn settings_and_auto_records_round_trip() {
    let f = open();
    let scope = Scope::Group(GroupId::DEFAULT);
    let s = LabelSettings {
        label: "newsletter".into(),
        mode: LabelMode::ApplyArchive,
        threshold: 0.8,
    };
    f.store.put_settings(scope, &s).await.unwrap();
    assert_eq!(f.store.settings(scope).await.unwrap(), [s]);

    let message = MessageId::new();
    let applied = AutoApplied {
        message_id: message,
        label: "newsletter".into(),
        score: 0.95,
        archived: true,
        at: Utc::now(),
    };
    f.store.record_auto(&applied).await.unwrap();
    assert_eq!(f.store.autos(message).await.unwrap().len(), 1);
    assert!(f.store.take_auto(message, "newsletter").await.unwrap());
    assert!(!f.store.take_auto(message, "newsletter").await.unwrap());
}
