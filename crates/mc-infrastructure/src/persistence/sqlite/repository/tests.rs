use super::*;
use mc_domain::{AccountStatus, EventEnvelope, MailboxType, ProviderType, ReadModelProjector};

fn open() -> (tempfile::TempDir, SqliteRepository) {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteDb::open(&dir.path().join("mail-cat.db")).unwrap();
    let store = Arc::new(SqliteEventStore::new(db.clone()));
    (dir, SqliteRepository::new(db, store))
}

fn at_position(position: u64, event: DomainEvent) -> EventEnvelope {
    let mut envelope = EventEnvelope::new("agg".into(), event.aggregate_type(), 1, event);
    envelope.global_position = position;
    envelope
}

fn account_added() -> DomainEvent {
    DomainEvent::AccountAdded {
        account_id: AccountId::new(),
        name: "Work".into(),
        email: EmailAddress::new("me@example.com").unwrap(),
        provider_type: ProviderType::Imap,
    }
}

fn received(account_id: AccountId, mailbox_id: MailboxId, rfc: &str) -> (MessageId, DomainEvent) {
    let message_id = MessageId::new();
    let event = DomainEvent::MessageReceived {
        message_id,
        account_id,
        mailbox_id,
        provider_uid: ProviderUid::new(1),
        message_rfc_id: Some(rfc.into()),
        in_reply_to: None,
        references: vec![],
        envelope: Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "s".into(),
            date: Utc::now(),
        },
        flags: vec![],
        size_bytes: 1,
        has_attachments: false,
        received_at: Utc::now(),
        restored: false,
    };
    (message_id, event)
}

#[tokio::test]
async fn bodies_are_stored_compressed() {
    let (_dir, repo) = open();
    let (id, event) = received(AccountId::new(), MailboxId::new(), "<a@x>");
    repo.project(&at_position(1, event)).await.unwrap();
    let text = "the same line again\n".repeat(1000);

    repo.put_body(&id, Some(text.clone()), None, false)
        .await
        .unwrap();

    let stored: i64 = repo
        .db
        .read(move |c| {
            c.query_row(
                "SELECT length(text_zstd) FROM message_bodies WHERE message_id = ?1",
                [id.as_uuid()],
                |r| r.get(0),
            )
            .map_err(repo_err)
        })
        .await
        .unwrap();
    assert!(stored < 1000, "{stored} bytes for {} of text", text.len());
    let message = repo.get_message(&id).await.unwrap().unwrap();
    assert_eq!(message.body_text, Some(text));
}

#[tokio::test]
async fn an_auth_status_keeps_its_reason_and_since_until_fixed() {
    let (_dir, repo) = open();
    let added = account_added();
    let DomainEvent::AccountAdded { account_id, .. } = added else {
        unreachable!()
    };
    repo.project(&at_position(1, added)).await.unwrap();

    let rejected = at_position(
        2,
        DomainEvent::AccountCredentialsRejected {
            account_id,
            error: "the server rejected the password".into(),
        },
    );
    repo.project(&rejected).await.unwrap();
    let account = repo.get_account(&account_id).await.unwrap().unwrap();
    assert_eq!(account.status, AccountStatus::CredentialsRejected);
    assert_eq!(
        account.status_error.as_deref(),
        Some("the server rejected the password")
    );
    assert_eq!(account.status_since, Some(rejected.occurred_at));

    repo.project(&at_position(
        3,
        DomainEvent::AccountReauthenticated { account_id },
    ))
    .await
    .unwrap();
    let account = repo.get_account(&account_id).await.unwrap().unwrap();
    assert_eq!(account.status, AccountStatus::Active);
    assert_eq!(account.status_error, None);
}

#[tokio::test]
async fn downloads_count_per_account_within_the_window() {
    let (_dir, repo) = open();
    let added = account_added();
    let DomainEvent::AccountAdded { account_id, .. } = added else {
        unreachable!()
    };
    repo.project(&at_position(1, added)).await.unwrap();
    let meter = DownloadMeter::new(repo.db.clone(), " Me@Example.com");
    let stranger = DownloadMeter::new(repo.db.clone(), "other@example.com");

    let before = Utc::now();
    meter.record(700).await;
    meter.record(300).await;
    stranger.record(5_000).await;

    let day_ago = before - chrono::TimeDelta::hours(24);
    assert_eq!(
        repo.downloaded_since(&account_id, day_ago).await.unwrap(),
        1_000
    );
    let later = Utc::now() + chrono::TimeDelta::seconds(1);
    assert_eq!(repo.downloaded_since(&account_id, later).await.unwrap(), 0);
}

#[tokio::test]
async fn deleting_a_message_takes_its_body_along() {
    let (_dir, repo) = open();
    let (id, event) = received(AccountId::new(), MailboxId::new(), "<a@x>");
    repo.project(&at_position(1, event)).await.unwrap();
    repo.put_body(&id, Some("x".into()), None, false)
        .await
        .unwrap();

    let deleted = DomainEvent::MessageDeleted {
        message_id: id,
        mailbox_id: MailboxId::new(),
        permanently: true,
    };
    repo.project(&at_position(2, deleted)).await.unwrap();

    let bodies: i64 = repo
        .db
        .read(|c| {
            c.query_row("SELECT COUNT(*) FROM message_bodies", [], |r| r.get(0))
                .map_err(repo_err)
        })
        .await
        .unwrap();
    assert_eq!(bodies, 0);
}

#[tokio::test]
async fn project_batch_commits_projection_and_checkpoint_together() {
    let (_dir, repo) = open();

    let committed = repo
        .project_batch(
            "read_model",
            vec![
                at_position(7, account_added()),
                at_position(8, account_added()),
            ],
        )
        .await
        .unwrap();

    assert!(committed);
    assert_eq!(repo.get_checkpoint("read_model").await.unwrap(), 8);
    assert_eq!(repo.list_accounts().await.unwrap().len(), 2);
}

#[tokio::test]
async fn failed_batch_rolls_back_projection_and_checkpoint() {
    let (_dir, repo) = open();
    // Make the second event's arm fail inside the transaction.
    repo.db
        .write(|c| {
            c.execute_batch("DROP TABLE contact_emails; DROP TABLE contacts;")
                .map_err(repo_err)
        })
        .await
        .unwrap();
    let contact = DomainEvent::ContactAdded {
        contact_id: ContactId::new(),
        display_name: "Ada".into(),
        emails: vec![mc_domain::ContactEmail::primary(
            EmailAddress::new("ada@example.com").unwrap(),
        )],
        source: mc_domain::ContactSource::Local,
        external_id: None,
        account_scope: None,
    };

    let result = repo
        .project_batch(
            "read_model",
            vec![at_position(1, account_added()), at_position(2, contact)],
        )
        .await;

    assert!(result.is_err());
    assert!(repo.list_accounts().await.unwrap().is_empty());
    assert_eq!(repo.get_checkpoint("read_model").await.unwrap(), 0);
}

#[tokio::test]
async fn deleting_one_rfc_copy_keeps_the_conversation() {
    let (_dir, repo) = open();
    let account = AccountId::new();
    let (inbox_copy, a) = received(account, MailboxId::new(), "<same@x>");
    let (_label_copy, b) = received(account, MailboxId::new(), "<same@x>");
    for (pos, event) in [a, b].into_iter().enumerate() {
        repo.project(&at_position(pos as u64 + 1, event))
            .await
            .unwrap();
    }

    let deleted = DomainEvent::MessageDeleted {
        message_id: inbox_copy,
        mailbox_id: MailboxId::new(),
        permanently: false,
    };
    repo.project(&at_position(3, deleted)).await.unwrap();

    let conv = repo
        .get_conversation(&ThreadId::new("<same@x>"), &account)
        .await
        .unwrap()
        .expect("the other copy still holds the thread");
    assert_eq!(conv.message_count, 1);
}

#[tokio::test]
async fn uid_lookup_breaks_received_at_ties_by_newest_id() {
    let (_dir, repo) = open();
    let (account, inbox, received_at) = (AccountId::new(), MailboxId::new(), Utc::now());
    let mut ids = Vec::new();
    for pos in 1..=2 {
        let (id, mut event) = received(account, inbox, &format!("<{pos}@x>"));
        if let DomainEvent::MessageReceived {
            received_at: at, ..
        } = &mut event
        {
            *at = received_at;
        }
        repo.project(&at_position(pos, event)).await.unwrap();
        ids.push(id);
    }

    let found = repo.find_by_provider_uid(&inbox, 1).await.unwrap().unwrap();
    assert_eq!(found.id, ids[1]);
}

#[tokio::test]
async fn saved_contact_events_reach_the_read_model_without_the_bus() {
    let (_dir, repo) = open();
    let (contact, added) = Contact::create(
        "Ada".into(),
        vec![mc_domain::ContactEmail::primary(
            EmailAddress::new("ada@example.com").unwrap(),
        )],
        vec![],
        None,
        None,
        mc_domain::ContactSource::Local,
        None,
        None,
    )
    .unwrap();

    ContactWriteRepository::save(&repo, &contact, vec![added])
        .await
        .unwrap();
    assert!(repo.get_contact(&contact.id).await.unwrap().is_some());

    ContactWriteRepository::save(&repo, &contact, vec![contact.remove()])
        .await
        .unwrap();
    assert!(repo.get_contact(&contact.id).await.unwrap().is_none());
}

#[tokio::test]
async fn address_usage_migration_seeds_from_stored_messages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mail-cat.db");
    let db = SqliteDb::open(&path).unwrap();
    let repo = SqliteRepository::new(db.clone(), Arc::new(SqliteEventStore::new(db)));
    let (account, inbox, sent) = (AccountId::new(), MailboxId::new(), MailboxId::new());
    let addr = |raw: &str| {
        vec![Address {
            name: None,
            email: EmailAddress::new(raw).unwrap(),
        }]
    };
    let mut events = vec![];
    for (mailbox_id, mailbox_type) in [(inbox, MailboxType::Inbox), (sent, MailboxType::Sent)] {
        events.push(DomainEvent::MailboxDiscovered {
            mailbox_id,
            account_id: account,
            path: MailboxPath::parse(&format!("{mailbox_type:?}"), '/').unwrap(),
            mailbox_type,
        });
    }
    for (mailbox, from, to) in [
        (sent, "me@example.com", "ada@example.com"),
        (sent, "me@example.com", "ada@example.com"),
        (inbox, "bob@example.com", "me@example.com"),
    ] {
        let (_, mut event) = received(account, mailbox, "<x@y>");
        if let DomainEvent::MessageReceived { envelope, .. } = &mut event {
            envelope.from = addr(from);
            envelope.to = addr(to);
            envelope.cc = addr(to);
        }
        events.push(event);
    }
    for (pos, event) in (1..).zip(events) {
        repo.project(&at_position(pos, event)).await.unwrap();
    }
    // Back to a pre-usage database holding the same messages.
    repo.db
        .write(|c| c.execute_batch(
            "DROP TABLE address_usage; DROP TABLE sent_counted; DROP TABLE downloads; \
             ALTER TABLE accounts DROP COLUMN status_error; ALTER TABLE accounts DROP COLUMN status_since; \
             ALTER TABLE message_bodies DROP COLUMN truncated; \
             PRAGMA user_version = 2;",
        ).map_err(repo_err))
        .await
        .unwrap();
    drop(repo);

    let usage: Vec<(String, i64, bool, bool)> = SqliteDb::open(&path)
        .unwrap()
        .read(|c| {
            query_all(
                c,
                "SELECT email, sent_count, last_sent_at IS NOT NULL, last_received_at IS NOT NULL \
                 FROM address_usage ORDER BY email",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
        })
        .await
        .unwrap();
    assert_eq!(
        usage,
        [
            ("ada@example.com".into(), 2, true, false),
            ("bob@example.com".into(), 0, false, true)
        ]
    );
}

#[tokio::test]
async fn sent_message_syncing_back_is_one_message_counted_once() {
    let (_dir, repo) = open();
    let (account, sent) = (AccountId::new(), MailboxId::new());
    let rfc = "<sent@example.com>";
    let ada = vec![Address {
        name: None,
        email: EmailAddress::new("ada@example.com").unwrap(),
    }];
    let placeholder = MessageId::new();
    let (server_copy, mut synced) = received(account, sent, rfc);
    if let DomainEvent::MessageReceived { envelope, .. } = &mut synced {
        envelope.to = ada.clone();
    }
    let events = [
        DomainEvent::MailboxDiscovered {
            mailbox_id: sent,
            account_id: account,
            path: MailboxPath::parse("Sent", '/').unwrap(),
            mailbox_type: MailboxType::Sent,
        },
        DomainEvent::MessageSent {
            message_id: placeholder,
            account_id: account,
            mailbox_id: sent,
            envelope: Envelope {
                from: vec![],
                to: ada,
                cc: vec![],
                bcc: vec![],
                reply_to: vec![],
                subject: "s".into(),
                date: Utc::now(),
            },
            message_rfc_id: Some(rfc.into()),
            in_reply_to: None,
            references: vec![],
            body_text: Some("hi".into()),
            body_html: None,
            sent_at: Utc::now(),
        },
        // What SyncService emits when the server copy reaches Sent.
        synced,
        DomainEvent::MessageDeleted {
            message_id: placeholder,
            mailbox_id: sent,
            permanently: true,
        },
    ];
    let events: Vec<_> = (1..)
        .zip(events)
        .map(|(pos, event)| at_position(pos, event))
        .collect();
    for event in &events {
        repo.project(event).await.unwrap();
    }
    assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));

    // Boot catch-up replays what the live path already projected.
    repo.project_batch("read_model", events).await.unwrap();
    assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));
}

#[tokio::test]
async fn sent_placeholder_arriving_after_its_server_copy_is_not_shown() {
    let (_dir, repo) = open();
    let (account, sent) = (AccountId::new(), MailboxId::new());
    let rfc = "<gmail@example.com>";
    let ada = vec![Address {
        name: None,
        email: EmailAddress::new("ada@example.com").unwrap(),
    }];
    let (server_copy, mut synced) = received(account, sent, rfc);
    if let DomainEvent::MessageReceived { envelope, .. } = &mut synced {
        envelope.to = ada.clone();
    }
    let placeholder = MessageId::new();
    let sent_event = |message_id| DomainEvent::MessageSent {
        message_id,
        account_id: account,
        mailbox_id: sent,
        envelope: Envelope {
            from: vec![],
            to: ada.clone(),
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "s".into(),
            date: Utc::now(),
        },
        message_rfc_id: Some(rfc.into()),
        in_reply_to: None,
        references: vec![],
        body_text: Some("hi".into()),
        body_html: None,
        sent_at: Utc::now(),
    };
    let discovered = DomainEvent::MailboxDiscovered {
        mailbox_id: sent,
        account_id: account,
        path: MailboxPath::parse("Sent", '/').unwrap(),
        mailbox_type: MailboxType::Sent,
    };

    // Gmail files the copy during SMTP; a sync can store it first.
    let events = [discovered.clone(), synced.clone(), sent_event(placeholder)];
    for (pos, event) in (1..).zip(events) {
        repo.project(&at_position(pos, event)).await.unwrap();
    }
    assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));

    // A sync that looked before the placeholder existed stores the copy after it.
    let (_dir, repo) = open();
    let events = [discovered, sent_event(placeholder), synced];
    for (pos, event) in (1..).zip(events) {
        repo.project(&at_position(pos, event)).await.unwrap();
    }
    assert_eq!(sent_state(&repo, sent).await, (vec![server_copy], 1, 1));
}

/// Messages listed in Sent, ada's sent_count, total conversation messages.
async fn sent_state(repo: &SqliteRepository, sent: MailboxId) -> (Vec<MessageId>, i64, i64) {
    let listed = repo
        .list_messages(
            &sent,
            &Pagination {
                page: 1,
                page_size: 10,
            },
        )
        .await
        .unwrap();
    let (sent_count, conversations) = repo
        .db
        .read(|c| {
            Ok((
                c.query_row(
                    "SELECT sent_count FROM address_usage WHERE email = 'ada@example.com'",
                    [],
                    |r| r.get(0),
                )
                .map_err(repo_err)?,
                c.query_row("SELECT SUM(message_count) FROM conversations", [], |r| {
                    r.get(0)
                })
                .map_err(repo_err)?,
            ))
        })
        .await
        .unwrap();
    (
        listed.items.iter().map(|m| m.id).collect(),
        sent_count,
        conversations,
    )
}

#[tokio::test]
async fn mailbox_paths_round_trip() {
    let (_dir, repo) = open();
    let (account, mailbox) = (AccountId::new(), MailboxId::new());
    let path = MailboxPath::parse("Work.Projects.2026", '.').unwrap();
    let discovered = DomainEvent::MailboxDiscovered {
        mailbox_id: mailbox,
        account_id: account,
        path: path.clone(),
        mailbox_type: MailboxType::Custom,
    };
    repo.project(&at_position(1, discovered)).await.unwrap();

    let stored = repo.get_mailbox(&mailbox).await.unwrap().unwrap();
    assert_eq!(stored.path, path);
    assert_eq!(stored.display_name, "2026");
}
