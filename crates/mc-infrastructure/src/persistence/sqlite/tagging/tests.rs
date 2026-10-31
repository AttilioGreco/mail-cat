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
async fn only_user_mailboxes_are_labels() {
    let f = open();
    let a = f.account("a@x.example", MailboxModel::Labels).await;
    let inbox = f.mailbox(a, "INBOX", MailboxType::Inbox).await;
    let mut boxes = vec![inbox];
    for (name, kind) in [
        ("Posta inviata", MailboxType::Sent),
        ("Cestino", MailboxType::Trash),
        ("Tutti i messaggi", MailboxType::All),
        ("Sent Mail", MailboxType::Custom),
        ("Fatture", MailboxType::Custom),
    ] {
        boxes.push(f.mailbox(a, name, kind).await);
    }
    let id = f
        .message(a, inbox, &boxes[1..], ListHeaders::default())
        .await;

    let source = f.store.sources(&[id]).await.unwrap().pop();

    // A Trash copy is unreadable, so the message is not seen at all.
    assert!(source.is_none());
    let id = f
        .message(
            a,
            inbox,
            &[boxes[1], boxes[3], boxes[4], boxes[5]],
            ListHeaders::default(),
        )
        .await;
    let source = f.store.sources(&[id]).await.unwrap().pop().unwrap();
    assert_eq!(source.labels, ["fatture"]);
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

const MODEL: &str = "test-model@1";

async fn with_body(f: &Fixture, id: MessageId) {
    mc_domain::MessageBodyStore::put_body(&f.repo, &id, Some("corpo".into()), None, false)
        .await
        .unwrap();
}

#[test]
fn vectors_round_trip_as_little_endian_floats() {
    let v = vec![0.0, -1.5, 0.25, f32::MIN_POSITIVE, 1e10];
    let blob = vectors::encode(&v);
    assert_eq!(blob.len(), 20);
    assert_eq!(&blob[4..8], (-1.5f32).to_le_bytes());
    assert_eq!(vectors::decode(&blob), Some(v));
    assert_eq!(
        vectors::decode(&blob[..7]),
        None,
        "a torn blob is not a vector"
    );
}

#[tokio::test]
async fn a_vector_of_another_model_is_not_seen_and_is_replaced() {
    let f = open();
    let account = f.account("a@x.example", MailboxModel::Labels).await;
    let inbox = f.mailbox(account, "INBOX", MailboxType::Inbox).await;
    let id = f.message(account, inbox, &[], ListHeaders::default()).await;
    with_body(&f, id).await;

    assert_eq!(f.store.unembedded(MODEL, None, 10).await.unwrap().len(), 1);
    f.store
        .put_vectors("old-model", &[(id, vec![1.0, 0.0])])
        .await
        .unwrap();
    assert!(f.store.vectors(MODEL, &[id]).await.unwrap().is_empty());
    assert_eq!(
        f.store.unembedded(MODEL, None, 10).await.unwrap().len(),
        1,
        "stale vectors leave the message to be embedded again"
    );

    f.store
        .put_vectors(MODEL, &[(id, vec![0.0, 1.0])])
        .await
        .unwrap();
    assert_eq!(
        f.store.vectors(MODEL, &[id]).await.unwrap()[&id],
        vec![0.0, 1.0]
    );
    assert!(
        f.store
            .vectors("old-model", &[id])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        f.store
            .unembedded(MODEL, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn only_readable_mail_with_a_body_is_left_to_embed() {
    let f = open();
    let account = f.account("a@x.example", MailboxModel::Labels).await;
    let inbox = f.mailbox(account, "INBOX", MailboxType::Inbox).await;
    let trash = f.mailbox(account, "Trash", MailboxType::Trash).await;
    let bare = f.message(account, inbox, &[], ListHeaders::default()).await;
    let binned = f.message(account, trash, &[], ListHeaders::default()).await;
    let wanted = f.message(account, inbox, &[], ListHeaders::default()).await;
    for id in [binned, wanted] {
        with_body(&f, id).await;
    }
    let left = f.store.unembedded(MODEL, None, 10).await.unwrap();
    assert_eq!(left.iter().map(|s| s.id).collect::<Vec<_>>(), vec![wanted]);
    assert_ne!(bare, wanted);
    assert!(
        f.store
            .unembedded(MODEL, Some(wanted), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_scopes_vectors_come_with_what_its_model_was_taught() {
    let f = open();
    let account = f.account("a@x.example", MailboxModel::Labels).await;
    let inbox = f.mailbox(account, "INBOX", MailboxType::Inbox).await;
    let (taught, untaught) = (
        f.message(account, inbox, &[], ListHeaders::default()).await,
        f.message(account, inbox, &[], ListHeaders::default()).await,
    );
    let scope = f.store.scope_of(account).await.unwrap();
    f.store
        .put_example(&Example {
            message_id: taught,
            account_id: account,
            scope: scope.key(),
            positives: vec!["newsletter".into()],
            negatives: vec!["lavoro".into()],
            with_body: true,
            weight: 1.0,
        })
        .await
        .unwrap();
    f.store
        .put_vectors(MODEL, &[(taught, vec![1.0]), (untaught, vec![0.5])])
        .await
        .unwrap();
    let found = f.store.scope_vectors(scope, MODEL).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].message_id, taught);
    assert_eq!(found[0].positives, vec!["newsletter"]);
    assert_eq!(found[0].negatives, vec!["lavoro"]);
    assert!(
        f.store
            .scope_vectors(scope, "other")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn embeddings_are_off_until_the_user_agrees() {
    let f = open();
    assert!(!f.store.embeddings_enabled().await.unwrap());
    f.store.set_embeddings_enabled(true).await.unwrap();
    assert!(f.store.embeddings_enabled().await.unwrap());
    f.store.set_embeddings_enabled(false).await.unwrap();
    assert!(!f.store.embeddings_enabled().await.unwrap());
}
