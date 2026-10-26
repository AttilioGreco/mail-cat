macro_rules! accounts_conformance {
    ($make:expr) => {
        #[tokio::test]
        async fn account_write_and_read() {
            let (_guard, _es, repo) = $make;

            let (account, event) = Account::create(
                "Work".into(),
                EmailAddress::new("work@example.com").unwrap(),
                ProviderType::Imap,
                test_config(),
                test_identity(),
            );
            let account_id = account.id;

            // Save through write repo.
            AccountWriteRepository::save(&repo, &account, vec![event])
                .await
                .unwrap();

            // Load through write repo (rehydrate from event store).
            let loaded = AccountWriteRepository::load(&repo, &account_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.name, "Work");

            // Read through read repo (projected summary).
            let summary = AccountReadRepository::get_account(&repo, &account_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(summary.name, "Work");
            assert_eq!(summary.status, AccountStatus::Active);
        }

        #[tokio::test]
        async fn list_accounts_returns_all() {
            let (_guard, _es, repo) = $make;

            for name in ["Acc1", "Acc2", "Acc3"] {
                let (account, event) = Account::create(
                    name.into(),
                    EmailAddress::new(&format!("{name}@example.com").to_lowercase()).unwrap(),
                    ProviderType::Imap,
                    test_config(),
                    test_identity(),
                );
                AccountWriteRepository::save(&repo, &account, vec![event])
                    .await
                    .unwrap();
            }

            let accounts = AccountReadRepository::list_accounts(&repo).await.unwrap();
            assert_eq!(accounts.len(), 3);
        }

        #[tokio::test]
        async fn load_nonexistent_account_returns_none() {
            let (_guard, _es, repo) = $make;
            let result = AccountWriteRepository::load(&repo, &AccountId::new())
                .await
                .unwrap();
            assert!(result.is_none());
        }

        #[tokio::test]
        async fn account_stats_report_server_total_local_envelopes_and_queued_bodies() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let mailbox_id = MailboxId::new();

            repo.project(&make_event_envelope(
                1,
                &mailbox_id.to_string(),
                AggregateType::Mailbox,
                1,
                DomainEvent::MailboxDiscovered {
                    mailbox_id,
                    account_id,
                    path: MailboxPath::inbox(),
                    mailbox_type: MailboxType::Inbox,
                },
            ))
            .await
            .unwrap();

            // The server says 5 messages; we hold 2 envelopes, one with a body.
            repo.project(&make_event_envelope(
                2,
                &mailbox_id.to_string(),
                AggregateType::Mailbox,
                2,
                DomainEvent::MailboxCountsUpdated {
                    mailbox_id,
                    total_messages: 5,
                    unread_messages: 1,
                },
            ))
            .await
            .unwrap();

            let stored = MessageId::new();
            let queued = MessageId::new();
            for (pos, id, uid) in [(3u64, stored, 10u32), (4, queued, 11)] {
                repo.project(&make_event_envelope(
                    pos,
                    &id.to_string(),
                    AggregateType::Message,
                    1,
                    DomainEvent::MessageReceived {
                        message_id: id,
                        account_id,
                        mailbox_id,
                        remote_id: RemoteId::from(uid),
                        envelope: test_envelope(),
                        flags: vec![],
                        size_bytes: 0,
                        has_attachments: false,
                        message_rfc_id: None,
                        in_reply_to: None,
                        references: vec![],
                        received_at: chrono::Utc::now(),
                        restored: false,
                    },
                ))
                .await
                .unwrap();
            }

            repo.project(&make_event_envelope(
                5,
                &stored.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched {
                    message_id: stored,
                    attachments: vec![],
                },
            ))
            .await
            .unwrap();

            let stats = AccountReadRepository::account_stats(&repo, &account_id)
                .await
                .unwrap();
            assert_eq!(stats.total_messages, 5, "server-side count");
            assert_eq!(stats.synced_messages, 2, "envelopes held locally");
            assert_eq!(stats.pending_bodies, 1, "only the body-less one is queued");

            // A different account must not see these.
            let other = AccountReadRepository::account_stats(&repo, &AccountId::new())
                .await
                .unwrap();
            assert_eq!(other, AccountSyncStats::default());
        }

        #[tokio::test]
        async fn projection_checkpoint_roundtrip() {
            let (_guard, _es, repo) = $make;

            let pos = repo.get_checkpoint("test_projection").await.unwrap();
            assert_eq!(pos, 0);

            repo.save_checkpoint("test_projection", 42).await.unwrap();

            let pos = repo.get_checkpoint("test_projection").await.unwrap();
            assert_eq!(pos, 42);

            repo.save_checkpoint("test_projection", 100).await.unwrap();
            let pos = repo.get_checkpoint("test_projection").await.unwrap();
            assert_eq!(pos, 100);
        }

        #[tokio::test]
        async fn project_account_lifecycle() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();

            // Project AccountAdded.
            let env = make_event_envelope(
                1,
                &account_id.to_string(),
                AggregateType::Account,
                1,
                DomainEvent::AccountAdded {
                    account_id,
                    name: "Work".into(),
                    email: EmailAddress::new("work@example.com").unwrap(),
                    provider_type: ProviderType::Imap,
                },
            );
            repo.project(&env).await.unwrap();

            let summary = AccountReadRepository::get_account(&repo, &account_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(summary.name, "Work");
            assert_eq!(summary.status, AccountStatus::Active);

            // Project AccountUpdated.
            let env = make_event_envelope(
                2,
                &account_id.to_string(),
                AggregateType::Account,
                2,
                DomainEvent::AccountUpdated {
                    account_id,
                    name: Some("Personal".into()),
                    connection_config: None,
                },
            );
            repo.project(&env).await.unwrap();

            let summary = AccountReadRepository::get_account(&repo, &account_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(summary.name, "Personal");

            // Project AccountRemoved.
            let env = make_event_envelope(
                3,
                &account_id.to_string(),
                AggregateType::Account,
                3,
                DomainEvent::AccountRemoved { account_id },
            );
            repo.project(&env).await.unwrap();

            let result = AccountReadRepository::get_account(&repo, &account_id)
                .await
                .unwrap();
            assert!(result.is_none());
        }
    };
}

pub(crate) use accounts_conformance;
