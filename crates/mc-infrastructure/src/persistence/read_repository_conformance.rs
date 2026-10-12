//! Read-model behaviour every backend must share, instantiated per backend.

macro_rules! read_repository_conformance {
    ($make:expr) => {
        use futures::TryStreamExt;
        use mc_domain::{
            Account, AccountId, AccountReadRepository, AccountStatus, AccountSyncStats,
            AccountWriteRepository, Address, AggregateType, AuthConfig, BodyBackfillQueue,
            ConnectionConfig, ContactEmail, ContactEmailLabel, ContactId, ContactReadRepository,
            ContactSource, ConversationReadRepository, DomainEvent, EmailAddress, Envelope,
            EventEnvelope, EventStore, Flag, Identity, MailboxId, MailboxPath,
            MailboxReadRepository, MailboxType, MessageBodyStore, MessageId, MessageReadRepository,
            ProjectionCheckpoint, ProviderType, ProviderUid, ReadModelProjector, SuggestScope, ThreadId,
        };
        use mc_shared::pagination::Pagination;

        fn test_config() -> ConnectionConfig {
            ConnectionConfig {
                imap_host: Some("imap.example.com".into()),
                imap_port: Some(993),
                use_tls: true,
                auth: AuthConfig::Plain {
                    username: "user@example.com".into(),
                    password: "secret".into(),
                },
                smtp: None,
                smtp_host: None,
                smtp_port: None,
            }
        }

        fn test_identity() -> Identity {
            Identity {
                display_name: "Test".into(),
                email: EmailAddress::new("user@example.com").unwrap(),
                signature_html: None,
                signature_text: None,
            }
        }

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
            AccountWriteRepository::save(&repo, &account, vec![event]).await.unwrap();

            // Load through write repo (rehydrate from event store).
            let loaded = AccountWriteRepository::load(&repo, &account_id).await.unwrap().unwrap();
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
                AccountWriteRepository::save(&repo, &account, vec![event]).await.unwrap();
            }

            let accounts = AccountReadRepository::list_accounts(&repo).await.unwrap();
            assert_eq!(accounts.len(), 3);
        }

        #[tokio::test]
        async fn load_nonexistent_account_returns_none() {
            let (_guard, _es, repo) = $make;
            let result = AccountWriteRepository::load(&repo, &AccountId::new()).await.unwrap();
            assert!(result.is_none());
        }

        #[tokio::test]
        async fn backfill_queue_newest_first_and_dequeues_on_body() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let mailbox_id = MailboxId::new();
            let older = MessageId::new();
            let newer = MessageId::new();
            let t0 = chrono::Utc::now();
            let t1 = t0 + chrono::Duration::seconds(60); // strictly newer

            let recv = |pos: u64, id: MessageId, uid: u32, at: chrono::DateTime<chrono::Utc>| {
                make_event_envelope(
                    pos,
                    &id.to_string(),
                    AggregateType::Message,
                    1,
                    DomainEvent::MessageReceived {
                        message_id: id,
                        account_id,
                        mailbox_id,
                        provider_uid: ProviderUid::new(uid),
                        envelope: test_envelope(),
                        flags: vec![],
                        size_bytes: 0,
                        has_attachments: false,
                        message_rfc_id: None,
                        in_reply_to: None,
                        references: vec![],
                        received_at: at,
                    },
                )
            };

            repo.project(&recv(1, older, 10, t0)).await.unwrap();
            repo.project(&recv(2, newer, 20, t1)).await.unwrap();

            // Newest received comes first, carrying the right provider_uid.
            let pending = repo.list_pending_backfill(&account_id, 10).await.unwrap();
            assert_eq!(pending.len(), 2);
            assert_eq!(pending[0].message_id.to_string(), newer.to_string());
            assert_eq!(pending[0].provider_uid, 20);
            assert_eq!(pending[1].message_id.to_string(), older.to_string());

            // Fetching a body dequeues exactly that entry.
            repo.project(&make_event_envelope(
                3,
                &newer.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched { message_id: newer, attachments: vec![] },
            ))
            .await
            .unwrap();

            let pending = repo.list_pending_backfill(&account_id, 10).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].message_id.to_string(), older.to_string());
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
                        provider_uid: ProviderUid::new(uid),
                        envelope: test_envelope(),
                        flags: vec![],
                        size_bytes: 0,
                        has_attachments: false,
                        message_rfc_id: None,
                        in_reply_to: None,
                        references: vec![],
                        received_at: chrono::Utc::now(),
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
                DomainEvent::MessageBodyFetched { message_id: stored, attachments: vec![] },
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

        // ── ReadModelProjector integration tests ─────────────────────────

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
                subject: "Hello World".into(),
                date: chrono::Utc::now(),
            }
        }

        fn make_event_envelope(
            global_position: u64,
            aggregate_id: &str,
            aggregate_type: AggregateType,
            seq: u64,
            event: DomainEvent,
        ) -> EventEnvelope {
            let mut env = EventEnvelope::new(aggregate_id.into(), aggregate_type, seq, event);
            env.global_position = global_position;
            env
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
                DomainEvent::AccountRemoved {
                    account_id,
                },
            );
            repo.project(&env).await.unwrap();

            let result = AccountReadRepository::get_account(&repo, &account_id)
                .await
                .unwrap();
            assert!(result.is_none());
        }

        #[tokio::test]
        async fn project_mailbox_lifecycle() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let mailbox_id = MailboxId::new();

            // Project MailboxDiscovered.
            let env = make_event_envelope(
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
            );
            repo.project(&env).await.unwrap();

            let mailbox = MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(mailbox.display_name, "INBOX");
            assert_eq!(mailbox.total_messages, 0);

            // Project MailboxCountsUpdated.
            let env = make_event_envelope(
                2,
                &mailbox_id.to_string(),
                AggregateType::Mailbox,
                2,
                DomainEvent::MailboxCountsUpdated {
                    mailbox_id,
                    total_messages: 42,
                    unread_messages: 5,
                },
            );
            repo.project(&env).await.unwrap();

            let mailbox = MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(mailbox.total_messages, 42);
            assert_eq!(mailbox.unread_messages, 5);

            // Project MailboxTypeChanged — a resync must be able to correct a folder
            // classified by an older, name-only heuristic.
            let env = make_event_envelope(
                3,
                &mailbox_id.to_string(),
                AggregateType::Mailbox,
                3,
                DomainEvent::MailboxTypeChanged {
                    mailbox_id,
                    mailbox_type: MailboxType::Archive,
                },
            );
            repo.project(&env).await.unwrap();

            let mailbox = MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(mailbox.mailbox_type, MailboxType::Archive);
            assert_eq!(mailbox.total_messages, 42, "counts must survive reclassification");

            // Verify list_mailboxes by account works.
            let mailboxes = MailboxReadRepository::list_mailboxes(&repo, &account_id)
                .await
                .unwrap();
            assert_eq!(mailboxes.len(), 1);

            // Project MailboxDeleted.
            let env = make_event_envelope(
                3,
                &mailbox_id.to_string(),
                AggregateType::Mailbox,
                3,
                DomainEvent::MailboxDeleted {
                    mailbox_id,
                    account_id,
                },
            );
            repo.project(&env).await.unwrap();

            let result = MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
                .await
                .unwrap();
            assert!(result.is_none());
            let mailboxes = MailboxReadRepository::list_mailboxes(&repo, &account_id)
                .await
                .unwrap();
            assert!(mailboxes.is_empty());
        }

        /// The sidebar badge reads `mailbox.unread_messages`; flagging must maintain it
        /// incrementally, and must not double-count a repeated mark-as-read.
        #[tokio::test]
        async fn flagging_seen_maintains_mailbox_unread_count() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let mailbox_id = MailboxId::new();
            let message_id = MessageId::new();

            for (seq, event) in [
                DomainEvent::MailboxDiscovered {
                    mailbox_id,
                    account_id,
                    path: MailboxPath::inbox(),
                    mailbox_type: MailboxType::Inbox,
                },
                DomainEvent::MessageReceived {
                    message_id,
                    account_id,
                    mailbox_id,
                    provider_uid: ProviderUid::new(42),
                    envelope: test_envelope(),
                    flags: vec![Flag::Recent],
                    size_bytes: 2048,
                    has_attachments: false,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    received_at: chrono::Utc::now(),
                },
                DomainEvent::MailboxCountsUpdated {
                    mailbox_id,
                    total_messages: 1,
                    unread_messages: 1,
                },
            ]
            .into_iter()
            .enumerate()
            {
                let env = make_event_envelope(
                    seq as u64 + 1,
                    &mailbox_id.to_string(),
                    AggregateType::Mailbox,
                    seq as u64 + 1,
                    event,
                );
                repo.project(&env).await.unwrap();
            }

            macro_rules! unread {
                () => {
                    MailboxReadRepository::get_mailbox(&repo, &mailbox_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .unread_messages
                };
            }
            assert_eq!(unread!(), 1);

            let mark_seen = || DomainEvent::MessageFlagged {
                message_id,
                mailbox_id,
                flags_added: vec![Flag::Seen],
                flags_removed: vec![],
            };

            // Reading the message drops the badge to zero.
            let env = make_event_envelope(
                4,
                &message_id.to_string(),
                AggregateType::Message,
                4,
                mark_seen(),
            );
            repo.project(&env).await.unwrap();
            assert_eq!(unread!(), 0);

            // Re-applying Seen must not underflow or double-count.
            let env = make_event_envelope(
                5,
                &message_id.to_string(),
                AggregateType::Message,
                5,
                mark_seen(),
            );
            repo.project(&env).await.unwrap();
            assert_eq!(unread!(), 0);

            // Marking unread again puts it back.
            let env = make_event_envelope(
                6,
                &message_id.to_string(),
                AggregateType::Message,
                6,
                DomainEvent::MessageFlagged {
                    message_id,
                    mailbox_id,
                    flags_added: vec![],
                    flags_removed: vec![Flag::Seen],
                },
            );
            repo.project(&env).await.unwrap();
            assert_eq!(unread!(), 1);
        }

        #[tokio::test]
        async fn project_message_lifecycle() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let mailbox_id = MailboxId::new();
            let message_id = MessageId::new();
            let received_at = chrono::Utc::now();

            // Project MessageReceived.
            let env = make_event_envelope(
                1,
                &message_id.to_string(),
                AggregateType::Message,
                1,
                DomainEvent::MessageReceived {
                    message_id,
                    account_id,
                    mailbox_id,
                    provider_uid: ProviderUid::new(42),
                    envelope: test_envelope(),
                    flags: vec![Flag::Recent],
                    size_bytes: 2048,
                    has_attachments: false,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    received_at,
                },
            );
            repo.project(&env).await.unwrap();

            // Verify message stored.
            let msg = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(msg.envelope.subject, "Hello World");
            assert_eq!(msg.flags, vec![Flag::Recent]);
            assert!(msg.body_text.is_none());

            // Verify summary stored.
            let page = MessageReadRepository::list_messages(
                &repo,
                &mailbox_id,
                &Pagination {
                    page: 1,
                    page_size: 10,
                },
            )
            .await
            .unwrap();
            assert_eq!(page.total_count, 1);
            assert_eq!(page.items[0].subject, "Hello World");
            assert_eq!(page.items[0].from_name, Some("Sender".to_string()));
            assert_eq!(page.items[0].from_email, "sender@example.com");

            // Project MessageBodyFetched.
            repo.put_body(&message_id, Some("Hello plain".into()), Some("<p>Hello</p>".into()))
                .await
                .unwrap();
            let env = make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched { message_id, attachments: vec![] },
            );
            repo.project(&env).await.unwrap();

            let msg = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(msg.body_text.as_deref(), Some("Hello plain"));

            // Project MessageFlagged (mark as read).
            let env = make_event_envelope(
                3,
                &message_id.to_string(),
                AggregateType::Message,
                3,
                DomainEvent::MessageFlagged {
                    message_id,
                    mailbox_id: MailboxId::new(),
                    flags_added: vec![Flag::Seen],
                    flags_removed: vec![Flag::Recent],
                },
            );
            repo.project(&env).await.unwrap();

            let msg = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap()
                .unwrap();
            assert!(msg.flags.contains(&Flag::Seen));
            assert!(!msg.flags.contains(&Flag::Recent));

            // Verify summary flags also updated.
            let page = MessageReadRepository::list_messages(
                &repo,
                &mailbox_id,
                &Pagination {
                    page: 1,
                    page_size: 10,
                },
            )
            .await
            .unwrap();
            assert!(page.items[0].flags.contains(&Flag::Seen));

            // Project MessageDeleted.
            let env = make_event_envelope(
                4,
                &message_id.to_string(),
                AggregateType::Message,
                4,
                DomainEvent::MessageDeleted {
                    message_id,
                    mailbox_id,
                    permanently: false,
                },
            );
            repo.project(&env).await.unwrap();

            let result = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap();
            assert!(result.is_none());

            let page = MessageReadRepository::list_messages(
                &repo,
                &mailbox_id,
                &Pagination {
                    page: 1,
                    page_size: 10,
                },
            )
            .await
            .unwrap();
            assert_eq!(page.total_count, 0);
        }

        #[tokio::test]
        async fn project_message_deleted_cleans_conversation_and_thread() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let mailbox_id = MailboxId::new();
            let root_id = MessageId::new();
            let reply_id = MessageId::new();
            let now = chrono::Utc::now();

            let received = |mid: MessageId, pos: u64, rfc: &str, irt: Option<&str>| {
                make_event_envelope(
                    pos,
                    &mid.to_string(),
                    AggregateType::Message,
                    1,
                    DomainEvent::MessageReceived {
                        message_id: mid,
                        account_id,
                        mailbox_id,
                        provider_uid: ProviderUid::new(pos as u32),
                        envelope: test_envelope(),
                        flags: vec![], // unread
                        size_bytes: 10,
                        has_attachments: false,
                        message_rfc_id: Some(rfc.to_string()),
                        in_reply_to: irt.map(|s| s.to_string()),
                        references: vec![],
                        received_at: now,
                    },
                )
            };

            // Two messages in the same thread (a reply to the root).
            repo.project(&received(root_id, 1, "<a@x>", None)).await.unwrap();
            repo.project(&received(reply_id, 2, "<b@x>", Some("<a@x>"))).await.unwrap();

            let page = Pagination { page: 1, page_size: 10 };

            // One conversation, two messages, both unread.
            let convs = mc_domain::traits::repository::ConversationReadRepository::list_conversations(
                &repo, &account_id, None, &page,
            )
            .await
            .unwrap();
            assert_eq!(convs.items.len(), 1);
            assert_eq!(convs.items[0].message_count, 2);
            assert_eq!(convs.items[0].unread_count, 2);
            assert_eq!(
                MessageReadRepository::get_thread(&repo, &root_id).await.unwrap().len(),
                2
            );

            // Delete the reply → conversation shrinks, thread drops it.
            repo.project(&make_event_envelope(
                3,
                &reply_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageDeleted { message_id: reply_id, mailbox_id, permanently: false },
            ))
            .await
            .unwrap();

            let convs = mc_domain::traits::repository::ConversationReadRepository::list_conversations(
                &repo, &account_id, None, &page,
            )
            .await
            .unwrap();
            assert_eq!(convs.items.len(), 1, "conversation still present with 1 message");
            assert_eq!(convs.items[0].message_count, 1);
            assert_eq!(convs.items[0].unread_count, 1);
            let thread = MessageReadRepository::get_thread(&repo, &root_id).await.unwrap();
            assert_eq!(thread.len(), 1, "deleted reply no longer in thread");
            assert_eq!(thread[0].id, root_id);

            // Delete the root → conversation removed entirely.
            repo.project(&make_event_envelope(
                4,
                &root_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageDeleted { message_id: root_id, mailbox_id, permanently: false },
            ))
            .await
            .unwrap();

            let convs = mc_domain::traits::repository::ConversationReadRepository::list_conversations(
                &repo, &account_id, None, &page,
            )
            .await
            .unwrap();
            assert!(convs.items.is_empty(), "empty conversation removed");
            assert!(
                MessageReadRepository::get_thread(&repo, &root_id).await.unwrap().is_empty(),
                "thread fully cleaned"
            );
        }

        #[tokio::test]
        async fn project_message_move_reindexes_summary() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let inbox_id = MailboxId::new();
            let archive_id = MailboxId::new();
            let message_id = MessageId::new();
            let received_at = chrono::Utc::now();

            // Receive message in inbox.
            let env = make_event_envelope(
                1,
                &message_id.to_string(),
                AggregateType::Message,
                1,
                DomainEvent::MessageReceived {
                    message_id,
                    account_id,
                    mailbox_id: inbox_id,
                    provider_uid: ProviderUid::new(1),
                    envelope: test_envelope(),
                    flags: vec![],
                    size_bytes: 1024,
                    has_attachments: false,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    received_at,
                },
            );
            repo.project(&env).await.unwrap();

            // Move to archive.
            let env = make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageMoved {
                    message_id,
                    from_mailbox_id: inbox_id,
                    to_mailbox_id: archive_id,
                },
            );
            repo.project(&env).await.unwrap();

            // Message should now reference archive.
            let msg = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(msg.mailbox_id, archive_id);

            // Summary should be under archive, not inbox.
            let inbox_msgs = MessageReadRepository::list_messages(
                &repo,
                &inbox_id,
                &Pagination {
                    page: 1,
                    page_size: 10,
                },
            )
            .await
            .unwrap();
            assert_eq!(inbox_msgs.total_count, 0);

            let archive_msgs = MessageReadRepository::list_messages(
                &repo,
                &archive_id,
                &Pagination {
                    page: 1,
                    page_size: 10,
                },
            )
            .await
            .unwrap();
            assert_eq!(archive_msgs.total_count, 1);
        }

        /// Regression test for the "reply shows no content / doesn't attach to
        /// the conversation" bug: sending a message must (1) persist its body so
        /// a refetch doesn't come back empty, (2) land in the Sent mailbox's
        /// message list immediately instead of waiting for the next IMAP sync,
        /// with sender AND recipient populated, and (3) thread correctly with
        /// the message it replies to.
        #[tokio::test]
        async fn project_message_sent_persists_body_appears_in_list_and_threads() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let sent_mailbox_id = MailboxId::new();

            // A message received earlier — the thread root, with a real RFC id
            // (as IMAP-sourced messages always have).
            let root_id = MessageId::new();
            let root_rfc_id = "<root@example.com>".to_string();
            let env = make_event_envelope(
                1,
                &root_id.to_string(),
                AggregateType::Message,
                1,
                DomainEvent::MessageReceived {
                    message_id: root_id,
                    account_id,
                    mailbox_id: MailboxId::new(),
                    provider_uid: ProviderUid::new(1),
                    envelope: test_envelope(),
                    flags: vec![Flag::Seen],
                    size_bytes: 1024,
                    has_attachments: false,
                    message_rfc_id: Some(root_rfc_id.clone()),
                    in_reply_to: None,
                    references: vec![],
                    received_at: chrono::Utc::now(),
                },
            );
            repo.project(&env).await.unwrap();

            // Now send a reply to it.
            let reply_id = MessageId::new();
            let sent_at = chrono::Utc::now();
            let env = make_event_envelope(
                1,
                &reply_id.to_string(),
                AggregateType::Message,
                1,
                DomainEvent::MessageSent {
                    message_id: reply_id,
                    message_rfc_id: None,
                    account_id,
                    mailbox_id: sent_mailbox_id,
                    envelope: test_envelope(),
                    in_reply_to: Some(root_rfc_id.clone()),
                    references: vec![root_rfc_id.clone()],
                    body_text: Some("Hello plain reply".into()),
                    body_html: Some("<p>Hello reply</p>".into()),
                    sent_at,
                },
            );
            repo.project(&env).await.unwrap();

            // (1) Body must round-trip, not come back empty.
            let sent_msg = MessageReadRepository::get_message(&repo, &reply_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sent_msg.body_text.as_deref(), Some("Hello plain reply"));
            assert_eq!(sent_msg.body_html.as_deref(), Some("<p>Hello reply</p>"));
            assert_eq!(sent_msg.mailbox_id, sent_mailbox_id);

            // (2) Must appear in the Sent mailbox's list immediately, with
            // sender and recipient populated (not blank).
            let sent_list = MessageReadRepository::list_messages(
                &repo,
                &sent_mailbox_id,
                &Pagination { page: 1, page_size: 10 },
            )
            .await
            .unwrap();
            assert_eq!(sent_list.total_count, 1);
            assert_eq!(sent_list.items[0].from_email, "sender@example.com");
            assert_eq!(sent_list.items[0].to, vec!["recipient@example.com".to_string()]);

            // (3) Reply must thread with the root message, not fork into a new
            // conversation.
            let thread = MessageReadRepository::get_thread(&repo, &root_id).await.unwrap();
            let thread_ids: Vec<_> = thread.iter().map(|m| m.id).collect();
            assert!(thread_ids.contains(&root_id));
            assert!(thread_ids.contains(&reply_id));
        }

        fn received_event(
            account_id: AccountId,
            message_id: MessageId,
            received_at: chrono::DateTime<chrono::Utc>,
        ) -> EventEnvelope {
            make_event_envelope(
                1,
                &message_id.to_string(),
                AggregateType::Message,
                1,
                DomainEvent::MessageReceived {
                    message_id,
                    account_id,
                    mailbox_id: MailboxId::new(),
                    provider_uid: ProviderUid::new(1),
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    envelope: mc_domain::Envelope {
                        from: vec![],
                        to: vec![],
                        cc: vec![],
                        bcc: vec![],
                        reply_to: vec![],
                        subject: "s".into(),
                        date: received_at,
                    },
                    flags: vec![],
                    size_bytes: 1,
                    has_attachments: false,
                    received_at,
                },
            )
        }

        #[tokio::test]
        async fn put_body_sets_body_on_existing_message() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let message_id = MessageId::new();
            repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
                .await
                .unwrap();

            repo.put_body(&message_id, Some("plain".into()), Some("<p>html</p>".into()))
                .await
                .unwrap();

            let msg = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(msg.body_text.as_deref(), Some("plain"));
            assert_eq!(msg.body_html.as_deref(), Some("<p>html</p>"));
        }

        #[tokio::test]
        async fn put_body_on_unknown_message_is_a_noop() {
            let (_guard, _es, repo) = $make;
            repo.put_body(&MessageId::new(), Some("x".into()), None).await.unwrap();
        }

        #[tokio::test]
        async fn put_body_alone_does_not_drain_backfill() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let message_id = MessageId::new();
            repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
                .await
                .unwrap();

            repo.put_body(&message_id, Some("plain".into()), None).await.unwrap();

            // Only the appended MessageBodyFetched marks the body done; if that
            // append failed, the message must still be fetched again.
            assert_eq!(repo.count_pending_backfill(&account_id).await.unwrap(), 1);
        }

        #[tokio::test]
        async fn body_fetched_event_keeps_stored_body_and_drains_backfill() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let message_id = MessageId::new();
            repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
                .await
                .unwrap();
            repo.put_body(&message_id, Some("plain".into()), None).await.unwrap();

            let fetched = make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched { message_id, attachments: vec![] },
            );
            // Twice: catch-up and rebuild replay events that were already projected.
            repo.project(&fetched).await.unwrap();
            repo.project(&fetched).await.unwrap();

            let msg = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(msg.body_text.as_deref(), Some("plain"));
            assert_eq!(repo.count_pending_backfill(&account_id).await.unwrap(), 0);
        }

        fn body_fetched(message_id: MessageId) -> EventEnvelope {
            make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched { message_id, attachments: vec![] },
            )
        }

        async fn body_text(repo: &impl MessageReadRepository, id: &MessageId) -> Option<String> {
            MessageReadRepository::get_message(repo, id).await.unwrap().unwrap().body_text
        }

        #[tokio::test]
        async fn replaying_received_and_body_fetched_keeps_stored_body() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let message_id = MessageId::new();
            let received = received_event(account_id, message_id, chrono::Utc::now());
            repo.project(&received).await.unwrap();
            repo.put_body(&message_id, Some("plain".into()), None).await.unwrap();
            repo.project(&body_fetched(message_id)).await.unwrap();

            // Boot catch-up replays everything since the last saved checkpoint.
            repo.project(&received).await.unwrap();
            repo.project(&body_fetched(message_id)).await.unwrap();

            assert_eq!(body_text(&repo, &message_id).await.as_deref(), Some("plain"));
            assert_eq!(repo.count_pending_backfill(&account_id).await.unwrap(), 0);
        }

        /// Fixed clock: minutes after an arbitrary epoch, so orderings are exact.
        fn at(minutes: i64) -> chrono::DateTime<chrono::Utc> {
            chrono::DateTime::from_timestamp(1_760_000_000 + minutes * 60, 0).unwrap()
        }

        /// Projection ignores aggregate id and sequence.
        fn env_of(event: DomainEvent) -> EventEnvelope {
            make_event_envelope(1, "agg", event.aggregate_type(), 1, event)
        }

        async fn project_all(
            repo: &impl ReadModelProjector,
            events: impl IntoIterator<Item = DomainEvent>,
        ) {
            for event in events {
                repo.project(&env_of(event)).await.unwrap();
            }
        }

        /// Appends to the real store (one aggregate per type) and returns the
        /// log as the store numbered it.
        async fn append_all<ES: EventStore>(store: &ES, events: Vec<DomainEvent>) -> Vec<EventEnvelope> {
            for event in events {
                let ty = event.aggregate_type();
                let id = ty.to_string();
                let seq = store.current_sequence(&id).await.unwrap();
                store.append(&id, ty, seq, vec![event]).await.unwrap();
            }
            store.load_all_events(None, 0).try_collect().await.unwrap()
        }

        fn received(
            account_id: AccountId,
            mailbox_id: MailboxId,
            uid: u32,
            rfc: Option<&str>,
            references: &[&str],
            seen: bool,
            received_at: chrono::DateTime<chrono::Utc>,
        ) -> (MessageId, DomainEvent) {
            let message_id = MessageId::new();
            let mut envelope = test_envelope();
            envelope.date = received_at;
            let event = DomainEvent::MessageReceived {
                message_id,
                account_id,
                mailbox_id,
                provider_uid: ProviderUid::new(uid),
                message_rfc_id: rfc.map(str::to_owned),
                in_reply_to: None,
                references: references.iter().map(|r| r.to_string()).collect(),
                envelope,
                flags: if seen { vec![Flag::Seen] } else { vec![] },
                size_bytes: 100,
                has_attachments: false,
                received_at,
            };
            (message_id, event)
        }

        fn discovered(account_id: AccountId, mailbox_id: MailboxId, path: &str) -> DomainEvent {
            DomainEvent::MailboxDiscovered {
                mailbox_id,
                account_id,
                path: MailboxPath::parse(path, '/').unwrap(),
                mailbox_type: MailboxType::Custom,
            }
        }

        fn flagged(message_id: MessageId, mailbox_id: MailboxId, add: Vec<Flag>, remove: Vec<Flag>) -> DomainEvent {
            DomainEvent::MessageFlagged { message_id, mailbox_id, flags_added: add, flags_removed: remove }
        }

        fn email(raw: &str) -> EmailAddress {
            EmailAddress::new(raw).unwrap()
        }

        fn first_page() -> Pagination {
            Pagination::new(1, 200)
        }

        fn ids<T>(items: &[T], id: impl Fn(&T) -> MessageId) -> Vec<MessageId> {
            items.iter().map(id).collect()
        }

        /// Everything a user can see of one account, rendered for equality.
        async fn snapshot<R>(repo: &R, account_id: &AccountId, mailboxes: &[MailboxId]) -> String
        where
            R: AccountReadRepository
                + MailboxReadRepository
                + MessageReadRepository
                + ConversationReadRepository
                + ContactReadRepository
                + BodyBackfillQueue,
        {
            let page = first_page();
            let mut out = format!("{:?}\n", repo.list_accounts().await.unwrap());
            for mailbox in mailboxes {
                out += &format!("{:?}\n", repo.get_mailbox(mailbox).await.unwrap());
                out += &format!("{:?}\n", repo.list_messages(mailbox, &page).await.unwrap().items);
                out += &format!("{:?}\n", repo.find_unread_by_mailbox(mailbox).await.unwrap());
                out += &format!(
                    "{:?}\n",
                    repo.list_conversations(account_id, Some(mailbox), &page).await.unwrap().items
                );
            }
            out += &format!("{:?}\n", repo.scan_all_messages(Some(account_id)).await.unwrap());
            out += &format!("{:?}\n", repo.list_conversations(account_id, None, &page).await.unwrap().items);
            out += &format!("{:?}\n", repo.list_contacts(&page).await.unwrap().items);
            out += &format!("{:?}\n", repo.count_pending_backfill(account_id).await.unwrap());
            out
        }

        #[tokio::test]
        async fn list_messages_pages_newest_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let mut msgs = Vec::new();
            for minute in 0..5 {
                let (id, event) = received(account, inbox, minute as u32 + 1, None, &[], false, at(minute));
                repo.project(&env_of(event)).await.unwrap();
                msgs.push(id);
            }

            let page = repo.list_messages(&inbox, &Pagination::new(1, 2)).await.unwrap();
            assert_eq!(page.total_count, 5);
            assert_eq!(ids(&page.items, |m| m.id), [msgs[4], msgs[3]]);
            let last = repo.list_messages(&inbox, &Pagination::new(3, 2)).await.unwrap();
            assert_eq!(ids(&last.items, |m| m.id), [msgs[0]]);
        }

        #[tokio::test]
        async fn get_thread_returns_the_whole_thread_oldest_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (late, e_late) = received(account, inbox, 3, Some("<late@x>"), &["<root@x>"], false, at(20));
            let (mid, e_mid) = received(account, inbox, 2, Some("<mid@x>"), &["<root@x>"], false, at(10));
            let (_other, e_other) = received(account, inbox, 4, Some("<other@x>"), &[], false, at(5));
            project_all(&repo, [e_root, e_late, e_mid, e_other]).await;

            let thread = repo.get_thread(&late).await.unwrap();
            assert_eq!(ids(&thread, |m| m.id), [root, mid, late]);
            assert!(repo.get_thread(&MessageId::new()).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn find_by_provider_uid_is_scoped_and_survives_uid_reuse() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (old, e_old) = received(account, inbox, 7, None, &[], false, at(0));
            let (_elsewhere, e_elsewhere) = received(account, archive, 7, None, &[], false, at(1));
            // UIDVALIDITY reset: the server hands out uid 7 again in the same mailbox.
            let (new, e_new) = received(account, inbox, 7, None, &[], false, at(2));
            project_all(&repo, [e_old, e_elsewhere, e_new]).await;

            let found = repo.find_by_provider_uid(&inbox, 7).await.unwrap().unwrap();
            assert_eq!(found.id, new);
            assert_eq!(found.provider_uid, Some(ProviderUid::new(7)));
            assert!(repo.find_by_provider_uid(&inbox, 8).await.unwrap().is_none());
            assert_ne!(found.id, old);
        }

        #[tokio::test]
        async fn find_unread_by_mailbox_skips_seen_and_other_mailboxes() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (unread, e1) = received(account, inbox, 1, None, &[], false, at(0));
            let (_read, e2) = received(account, inbox, 2, None, &[], true, at(1));
            let (_elsewhere, e3) = received(account, archive, 3, None, &[], false, at(2));
            project_all(&repo, [e1, e2, e3]).await;

            let found = repo.find_unread_by_mailbox(&inbox).await.unwrap();
            assert_eq!(ids(&found, |m| m.id), [unread]);

            project_all(&repo, [flagged(unread, inbox, vec![Flag::Seen], vec![])]).await;
            assert!(repo.find_unread_by_mailbox(&inbox).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn list_conversations_scoped_to_a_mailbox() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(account, archive, 2, Some("<reply@x>"), &["<root@x>"], true, at(10));
            let (solo, e_solo) = received(account, inbox, 3, Some("<solo@x>"), &[], true, at(5));
            project_all(&repo, [e_root, e_reply, e_solo]).await;
            let page = first_page();

            let all = repo.list_conversations(&account, None, &page).await.unwrap();
            assert_eq!(all.total_count, 2);
            let stats: Vec<_> = all
                .items
                .iter()
                .map(|c| (c.thread_id.clone(), c.message_count, c.unread_count, c.sample_message_id))
                .collect();
            assert_eq!(
                stats,
                [(ThreadId::new("<root@x>"), 2, 1, reply), (ThreadId::new("<solo@x>"), 1, 0, solo)]
            );

            let in_inbox = repo.list_conversations(&account, Some(&inbox), &page).await.unwrap();
            assert_eq!(in_inbox.total_count, 2);
            let stats: Vec<_> = in_inbox
                .items
                .iter()
                .map(|c| (c.thread_id.clone(), c.message_count, c.unread_count, c.sample_message_id))
                .collect();
            assert_eq!(
                stats,
                [(ThreadId::new("<solo@x>"), 1, 0, solo), (ThreadId::new("<root@x>"), 1, 1, root)]
            );
            assert_eq!(in_inbox.items[1].last_message_at, at(0));
            assert_eq!(in_inbox.items[1].subject, "Hello World");
            assert_eq!(in_inbox.items[1].latest_sender, "Sender");

            let in_archive = repo.list_conversations(&account, Some(&archive), &page).await.unwrap();
            assert_eq!(in_archive.total_count, 1);
            assert_eq!(in_archive.items[0].sample_message_id, reply);
            assert_eq!(in_archive.items[0].unread_count, 0);
        }

        #[tokio::test]
        async fn rfc_copies_count_once_per_conversation() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, all_mail) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (_a, e_a) = received(account, inbox, 1, Some("<same@x>"), &[], false, at(0));
            let (_b, e_b) = received(account, all_mail, 9, Some("<same@x>"), &[], false, at(0));
            project_all(&repo, [e_a, e_b]).await;

            let conv = repo
                .get_conversation(&ThreadId::new("<same@x>"), &account)
                .await
                .unwrap()
                .unwrap();
            assert_eq!((conv.message_count, conv.unread_count), (1, 1));
        }

        #[tokio::test]
        async fn rebuild_index_restores_conversations() {
            let (_guard, store, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (_root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(account, inbox, 2, Some("<reply@x>"), &["<root@x>"], false, at(10));
            let log = append_all(
                store.as_ref(),
                vec![e_root, e_reply, flagged(reply, inbox, vec![Flag::Seen], vec![])],
            )
            .await;
            for event in &log {
                repo.project(event).await.unwrap();
            }
            let thread = ThreadId::new("<root@x>");
            let before = repo.get_conversation(&thread, &account).await.unwrap().unwrap();

            assert!(repo.rebuild_index().await.unwrap() > 0);

            let after = repo.get_conversation(&thread, &account).await.unwrap().unwrap();
            assert_eq!((after.message_count, after.unread_count), (2, 1));
            assert_eq!(format!("{after:?}"), format!("{before:?}"));
        }

        #[tokio::test]
        async fn drafts_stay_out_of_mailbox_views() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, draft) = (AccountId::new(), MailboxId::new(), MessageId::new());
            project_all(
                &repo,
                [DomainEvent::DraftSaved {
                    message_id: draft,
                    account_id: account,
                    envelope: test_envelope(),
                    body_text: Some("draft".into()),
                    body_html: None,
                }],
            )
            .await;

            let stored = repo.get_message(&draft).await.unwrap().unwrap();
            assert_eq!(stored.body_text.as_deref(), Some("draft"));
            assert!(stored.flags.contains(&Flag::Draft));
            assert_eq!(repo.list_messages(&inbox, &first_page()).await.unwrap().total_count, 0);
            assert_eq!(repo.count_pending_backfill(&account).await.unwrap(), 0);

            project_all(&repo, [DomainEvent::DraftDiscarded { message_id: draft }]).await;
            assert!(repo.get_message(&draft).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn contact_search_ranks_prefix_hits_then_correspondence() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, sent) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let mail = |from: &str, to: &str| Envelope {
                from: vec![Address { name: None, email: email(from) }],
                to: vec![Address { name: None, email: email(to) }],
                ..test_envelope()
            };
            let contact = |name: &str, addr: &str| DomainEvent::ContactAdded {
                contact_id: ContactId::new(),
                display_name: name.into(),
                emails: vec![ContactEmail::primary(email(addr))],
                source: ContactSource::LocalEmail,
                external_id: None,
                account_scope: Some(account),
            };
            let send_to = |to: &str, minutes| DomainEvent::MessageSent {
                message_id: MessageId::new(),
                message_rfc_id: None,
                account_id: account,
                mailbox_id: sent,
                envelope: mail("me@example.com", to),
                in_reply_to: None,
                references: vec![],
                body_text: None,
                body_html: None,
                sent_at: at(minutes),
            };
            let synced = |mailbox_id, from: &str, to: &str| DomainEvent::MessageReceived {
                message_id: MessageId::new(),
                account_id: account,
                mailbox_id,
                provider_uid: ProviderUid::new(1),
                message_rfc_id: None,
                in_reply_to: None,
                references: vec![],
                envelope: mail(from, to),
                flags: vec![],
                size_bytes: 1,
                has_attachments: false,
                received_at: at(5),
            };
            // Sent from another client, then synced from the Sent folder.
            let rare = synced(sent, "me@example.com", "ann.rare@example.com");
            project_all(
                &repo,
                [
                    DomainEvent::MailboxDiscovered {
                        mailbox_id: inbox,
                        account_id: account,
                        path: MailboxPath::parse("INBOX", '/').unwrap(),
                        mailbox_type: MailboxType::Inbox,
                    },
                    DomainEvent::MailboxDiscovered {
                        mailbox_id: sent,
                        account_id: account,
                        path: MailboxPath::parse("Sent", '/').unwrap(),
                        mailbox_type: MailboxType::Sent,
                    },
                    contact("Joanna", "joanna@example.com"),
                    contact("Zed", "zed@annex.org"),
                    contact("Bob Ann", "bob@example.com"),
                    contact("Zoe Annabel", "annabel@example.com"),
                    contact("Ann Vega", "ann.rare@example.com"),
                    contact("Anne Wu", "ann.often@example.com"),
                    send_to("ann.often@example.com", 1),
                    send_to("ann.often@example.com", 2),
                    rare.clone(),
                    synced(inbox, "annabel@example.com", "me@example.com"),
                ],
            )
            .await;
            // Catch-up replay must not inflate Ann Vega past Anne Wu.
            project_all(&repo, [rare.clone(), rare]).await;

            let names = |hits: Vec<mc_domain::ContactSummary>| {
                hits.into_iter().map(|c| c.display_name).collect::<Vec<_>>()
            };
            assert_eq!(
                names(repo.search_contacts("ANN", 10).await.unwrap()),
                ["Anne Wu", "Ann Vega", "Zoe Annabel", "Bob Ann", "Zed", "Joanna"],
            );
            assert_eq!(names(repo.search_contacts("ann", 2).await.unwrap()), ["Anne Wu", "Ann Vega"]);
        }

        #[tokio::test]
        async fn contact_lifecycle_and_lookup() {
            let (_guard, _es, repo) = $make;
            let (contact, account) = (ContactId::new(), AccountId::new());
            project_all(
                &repo,
                [DomainEvent::ContactAdded {
                    contact_id: contact,
                    display_name: "Ada Lovelace".into(),
                    emails: vec![
                        ContactEmail::primary(email("ada@example.com")),
                        ContactEmail::new(email("ada@work.example.com"), ContactEmailLabel::Work, false),
                    ],
                    source: ContactSource::Local,
                    external_id: None,
                    account_scope: Some(account),
                }],
            )
            .await;

            let summary = repo.get_contact(&contact).await.unwrap().unwrap();
            assert_eq!(summary.display_name, "Ada Lovelace");
            assert_eq!(summary.primary_email, email("ada@example.com"));
            assert_eq!(summary.emails.len(), 2);
            assert_eq!(summary.organization, None);
            assert_eq!(summary.account_scope, Some(account));
            let found = repo.find_by_email(&email("ada@work.example.com")).await.unwrap();
            assert_eq!(found.iter().map(|c| c.id).collect::<Vec<_>>(), [contact]);
            assert_eq!(repo.find_by_email_in_account(&email("ada@example.com"), &account).await.unwrap().len(), 1);
            assert!(repo.find_by_email_in_account(&email("ada@example.com"), &AccountId::new()).await.unwrap().is_empty());
            assert_eq!(repo.search_contacts("lovelace", 10).await.unwrap().len(), 1);
            assert_eq!(repo.search_contacts("ADA@EXAMPLE", 10).await.unwrap().len(), 1);
            // Autocomplete must find a contact by any of its addresses.
            assert_eq!(repo.search_contacts("ada@work", 10).await.unwrap().len(), 1);
            assert!(repo.search_contacts("nobody", 10).await.unwrap().is_empty());

            project_all(
                &repo,
                [DomainEvent::ContactUpdated {
                    contact_id: contact,
                    display_name: Some("Ada King".into()),
                    emails: Some(vec![ContactEmail::primary(email("ada@king.example.com"))]),
                    phones: None,
                    organization: Some(Some("Analytical".into())),
                    notes: None,
                }],
            )
            .await;

            let summary = repo.get_contact(&contact).await.unwrap().unwrap();
            assert_eq!(summary.display_name, "Ada King");
            assert_eq!(summary.primary_email, email("ada@king.example.com"));
            assert_eq!(summary.organization.as_deref(), Some("Analytical"));
            assert!(repo.find_by_email(&email("ada@work.example.com")).await.unwrap().is_empty());
            assert_eq!(repo.find_by_email(&email("ada@king.example.com")).await.unwrap().len(), 1);
            assert_eq!(repo.list_contacts(&first_page()).await.unwrap().total_count, 1);

            project_all(&repo, [DomainEvent::ContactRemoved { contact_id: contact }]).await;
            assert!(repo.get_contact(&contact).await.unwrap().is_none());
            assert!(repo.find_by_email(&email("ada@king.example.com")).await.unwrap().is_empty());
            assert_eq!(repo.list_contacts(&first_page()).await.unwrap().total_count, 0);
        }

        #[tokio::test]
        async fn duplicate_candidates_and_merge() {
            let (_guard, _es, repo) = $make;
            let (account, other_account) = (AccountId::new(), AccountId::new());
            let (c1, c2, c3) = (ContactId::new(), ContactId::new(), ContactId::new());
            let added = |id: ContactId, emails: Vec<ContactEmail>, scope: AccountId| DomainEvent::ContactAdded {
                contact_id: id,
                display_name: id.to_string(),
                emails,
                source: ContactSource::LocalEmail,
                external_id: None,
                account_scope: Some(scope),
            };
            let shared = || ContactEmail::primary(email("shared@example.com"));
            project_all(
                &repo,
                [
                    added(c1, vec![shared(), ContactEmail::new(email("one@example.com"), ContactEmailLabel::Home, false)], account),
                    added(c2, vec![shared()], account),
                    added(c3, vec![shared()], other_account),
                ],
            )
            .await;
            let pair_ids = |pairs: Vec<(mc_domain::ContactSummary, mc_domain::ContactSummary)>| {
                let mut out: Vec<(String, String)> = pairs
                    .into_iter()
                    .map(|(a, b)| {
                        let (a, b) = (a.id.to_string(), b.id.to_string());
                        if a < b { (a, b) } else { (b, a) }
                    })
                    .collect();
                out.sort();
                out
            };
            let sorted = |a: ContactId, b: ContactId| {
                let (a, b) = (a.to_string(), b.to_string());
                if a < b { (a, b) } else { (b, a) }
            };

            let scoped = pair_ids(repo.find_duplicate_candidates(Some(&account)).await.unwrap());
            assert_eq!(scoped, [sorted(c1, c2)]);
            let mut everywhere = vec![sorted(c1, c2), sorted(c1, c3), sorted(c2, c3)];
            everywhere.sort();
            assert_eq!(pair_ids(repo.find_duplicate_candidates(None).await.unwrap()), everywhere);

            project_all(
                &repo,
                [DomainEvent::ContactMerged {
                    kept_id: c1,
                    discarded_id: c2,
                    merged_emails: vec![shared(), ContactEmail::new(email("one@example.com"), ContactEmailLabel::Home, false)],
                }],
            )
            .await;
            assert!(repo.get_contact(&c2).await.unwrap().is_none());
            let mut holders: Vec<String> = repo
                .find_by_email(&email("shared@example.com"))
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.to_string())
                .collect();
            holders.sort();
            let mut expected = vec![c1.to_string(), c3.to_string()];
            expected.sort();
            assert_eq!(holders, expected);
            assert!(repo.find_duplicate_candidates(Some(&account)).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn replaying_mailbox_discovered_keeps_server_counts() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            project_all(
                &repo,
                [
                    discovered(account, inbox, "INBOX"),
                    DomainEvent::MailboxCountsUpdated { mailbox_id: inbox, total_messages: 10, unread_messages: 4 },
                    discovered(account, inbox, "INBOX"),
                ],
            )
            .await;

            let mailbox = repo.get_mailbox(&inbox).await.unwrap().unwrap();
            assert_eq!((mailbox.total_messages, mailbox.unread_messages), (10, 4));
        }

        /// Boot and post-sync catch-up re-project everything since the last
        /// checkpoint on top of what the live path already projected.
        #[tokio::test]
        async fn replaying_the_log_over_live_state_changes_nothing() {
            let (_guard, store, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(account, inbox, 2, Some("<reply@x>"), &["<root@x>"], false, at(10));
            let (gone, e_gone) = received(account, inbox, 3, Some("<gone@x>"), &[], false, at(20));
            let (sent, contact) = (MessageId::new(), ContactId::new());
            let log = append_all(
                store.as_ref(),
                vec![
                    DomainEvent::AccountAdded {
                        account_id: account,
                        name: "Work".into(),
                        email: email("me@example.com"),
                        provider_type: ProviderType::Imap,
                    },
                    discovered(account, inbox, "INBOX"),
                    discovered(account, archive, "Archive"),
                    e_root,
                    e_reply,
                    e_gone,
                    DomainEvent::MessageBodyFetched { message_id: root, attachments: vec![] },
                    flagged(root, inbox, vec![Flag::Seen], vec![]),
                    DomainEvent::MessageMoved { message_id: reply, from_mailbox_id: inbox, to_mailbox_id: archive },
                    DomainEvent::MessageDeleted { message_id: gone, mailbox_id: inbox, permanently: false },
                    DomainEvent::MessageSent {
                        message_id: sent,
                        message_rfc_id: None,
                        account_id: account,
                        mailbox_id: archive,
                        envelope: test_envelope(),
                        in_reply_to: Some("<root@x>".into()),
                        references: vec!["<root@x>".into()],
                        body_text: Some("thanks".into()),
                        body_html: None,
                        sent_at: at(30),
                    },
                    flagged(sent, archive, vec![], vec![Flag::Seen]),
                    DomainEvent::ContactAdded {
                        contact_id: contact,
                        display_name: "Ada".into(),
                        emails: vec![ContactEmail::primary(email("ada@example.com"))],
                        source: ContactSource::LocalEmail,
                        external_id: None,
                        account_scope: Some(account),
                    },
                    DomainEvent::ContactUpdated {
                        contact_id: contact,
                        display_name: Some("Ada L.".into()),
                        emails: None,
                        phones: None,
                        organization: Some(Some("Analytical".into())),
                        notes: None,
                    },
                ],
            )
            .await;
            for event in &log {
                repo.project(event).await.unwrap();
            }
            repo.put_body(&root, Some("hello".into()), None).await.unwrap();
            // Server counts are transient: they reach the read model only through project().
            project_all(
                &repo,
                [DomainEvent::MailboxCountsUpdated { mailbox_id: inbox, total_messages: 10, unread_messages: 4 }],
            )
            .await;
            let live = snapshot(&repo, &account, &[inbox, archive]).await;

            for event in &log {
                repo.project(event).await.unwrap();
            }

            assert_eq!(snapshot(&repo, &account, &[inbox, archive]).await, live);
            assert_eq!(
                repo.get_mailbox(&inbox).await.unwrap().unwrap().unread_messages,
                4,
                "replaying the received+deleted pair must not move the badge"
            );
        }

        /// Catch-up starts wherever the last checkpoint was, so replaying any
        /// suffix of the log over live state must change nothing.
        #[tokio::test]
        async fn replaying_any_suffix_over_live_state_changes_nothing() {
            let (_guard, store, repo) = $make;
            let (account, inbox, trash, archive, labels) =
                (AccountId::new(), MailboxId::new(), MailboxId::new(), MailboxId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (mid, e_mid) = received(account, inbox, 2, Some("<mid@x>"), &["<root@x>"], false, at(10));
            let (newest, e_newest) = received(account, inbox, 3, Some("<new@x>"), &["<root@x>"], false, at(20));
            let (copy_a, e_copy_a) = received(account, inbox, 4, Some("<copy@x>"), &[], false, at(30));
            let (_copy_b, e_copy_b) = received(account, labels, 9, Some("<copy@x>"), &[], false, at(30));
            let (roamer, e_roamer) = received(account, inbox, 5, Some("<roam@x>"), &[], false, at(40));
            let sent = MessageId::new();
            let (kept, merged, removed) = (ContactId::new(), ContactId::new(), ContactId::new());
            let contact = |id: ContactId, raw: &str| DomainEvent::ContactAdded {
                contact_id: id,
                display_name: raw.into(),
                emails: vec![ContactEmail::primary(email(raw))],
                source: ContactSource::LocalEmail,
                external_id: None,
                account_scope: Some(account),
            };
            let log = append_all(
                store.as_ref(),
                vec![
                    DomainEvent::AccountAdded {
                        account_id: account,
                        name: "Work".into(),
                        email: email("me@example.com"),
                        provider_type: ProviderType::Imap,
                    },
                    discovered(account, inbox, "INBOX"),
                    discovered(account, trash, "Trash"),
                    discovered(account, archive, "Archive"),
                    discovered(account, labels, "Labels"),
                    DomainEvent::MailboxRenamed {
                        mailbox_id: archive,
                        old_path: MailboxPath::parse("Archive", '/').unwrap(),
                        new_path: MailboxPath::parse("Old/Archive", '/').unwrap(),
                    },
                    e_root,
                    e_mid,
                    e_newest,
                    e_copy_a,
                    e_copy_b,
                    e_roamer,
                    DomainEvent::MessageBodyFetched { message_id: root, attachments: vec![] },
                    flagged(mid, inbox, vec![Flag::Seen], vec![]),
                    flagged(mid, inbox, vec![], vec![Flag::Seen]),
                    flagged(mid, inbox, vec![Flag::Seen], vec![]),
                    DomainEvent::MessageDeleted { message_id: newest, mailbox_id: inbox, permanently: false },
                    DomainEvent::MessageDeleted { message_id: copy_a, mailbox_id: inbox, permanently: false },
                    DomainEvent::MessageMoved { message_id: roamer, from_mailbox_id: inbox, to_mailbox_id: trash },
                    DomainEvent::MessageMoved { message_id: roamer, from_mailbox_id: trash, to_mailbox_id: archive },
                    DomainEvent::MessageSent {
                        message_id: sent,
                        message_rfc_id: None,
                        account_id: account,
                        mailbox_id: archive,
                        envelope: test_envelope(),
                        in_reply_to: Some("<root@x>".into()),
                        references: vec!["<root@x>".into()],
                        body_text: Some("thanks".into()),
                        body_html: None,
                        sent_at: at(50),
                    },
                    flagged(sent, archive, vec![], vec![Flag::Seen]),
                    contact(kept, "ada@example.com"),
                    contact(merged, "ada@work.example.com"),
                    DomainEvent::ContactMerged {
                        kept_id: kept,
                        discarded_id: merged,
                        merged_emails: vec![
                            ContactEmail::primary(email("ada@example.com")),
                            ContactEmail::new(email("ada@work.example.com"), ContactEmailLabel::Work, false),
                        ],
                    },
                    contact(removed, "bob@example.com"),
                    DomainEvent::ContactRemoved { contact_id: removed },
                ],
            )
            .await;
            for event in &log {
                repo.project(event).await.unwrap();
            }
            repo.put_body(&root, Some("hello".into()), None).await.unwrap();
            let mailboxes = [inbox, trash, archive, labels];
            let state = || async {
                let mut out = snapshot(&repo, &account, &mailboxes).await;
                for id in [root, mid, roamer, sent] {
                    out += &format!("{:?}\n", repo.get_message(&id).await.unwrap());
                }
                out += &format!("{:?}\n", repo.get_thread(&root).await.unwrap());
                for raw in ["ada@example.com", "ada@work.example.com", "bob@example.com"] {
                    out += &format!("{:?}\n", repo.find_by_email(&email(raw)).await.unwrap());
                }
                out
            };
            let live = state().await;

            for from in 0..log.len() {
                for event in &log[from..] {
                    repo.project(event).await.unwrap();
                }
                assert_eq!(state().await, live, "replay from log[{from}..] changed the read model");
            }
        }

        #[tokio::test]
        async fn suggest_groups_matching_values_by_field_most_frequent_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let addr = |name: Option<&str>, raw: &str| Address { name: name.map(str::to_owned), email: email(raw) };
            let mail = |uid: u32, from: Address, to: &str, subject: &str| {
                let (_, mut event) = received(account, inbox, uid, None, &[], false, at(uid as i64));
                if let DomainEvent::MessageReceived { envelope, .. } = &mut event {
                    envelope.from = vec![from];
                    envelope.to = vec![addr(None, to)];
                    envelope.subject = subject.into();
                }
                event
            };
            project_all(
                &repo,
                [
                    mail(1, addr(Some("Ada Lovelace"), "ada@example.com"), "me@example.com", "Notes"),
                    mail(2, addr(Some("Ada Lovelace"), "ada@example.com"), "me@example.com", "More notes"),
                    mail(3, addr(Some("Bob"), "bob@example.com"), "adam@example.com", "Adaptive design"),
                    mail(4, addr(None, "carol@example.com"), "me@example.com", "Lunch"),
                ],
            )
            .await;
            let found = |s: Vec<mc_domain::Suggestion>| {
                s.into_iter().map(|s| (s.field, s.value, s.doc_count)).collect::<Vec<_>>()
            };
            let row = |field: &str, value: &str, n: u32| (field.to_owned(), value.to_owned(), n);

            assert_eq!(
                found(repo.suggest("AD", None, 5).await.unwrap()),
                [
                    row("from_email", "ada@example.com", 2),
                    row("from_name", "Ada Lovelace", 2),
                    row("to", "adam@example.com", 1),
                    row("subject", "Adaptive design", 1),
                ]
            );
            assert_eq!(
                found(repo.suggest("example", Some(SuggestScope::From), 2).await.unwrap()),
                [row("from_email", "ada@example.com", 2), row("from_email", "bob@example.com", 1)]
            );
            assert_eq!(
                found(repo.suggest("me@", Some(SuggestScope::To), 5).await.unwrap()),
                [row("to", "me@example.com", 3)]
            );
            assert!(repo.suggest("notes", Some(SuggestScope::Body), 5).await.unwrap().is_empty());
            assert!(repo.suggest("", None, 5).await.unwrap().is_empty());
        }
    };
}

pub(crate) use read_repository_conformance;
