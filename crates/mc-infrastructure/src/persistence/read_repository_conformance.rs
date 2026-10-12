//! Read-model behaviour every backend must share, instantiated per backend.

macro_rules! read_repository_conformance {
    ($make:expr) => {
        use mc_domain::{
            Account, AccountId, AccountReadRepository, AccountStatus, AccountSyncStats,
            AccountWriteRepository, Address, AggregateType, AuthConfig, BodyBackfillQueue,
            ConnectionConfig, DomainEvent, EmailAddress, Envelope, EventEnvelope, Flag, Identity,
            MailboxId, MailboxPath, MailboxReadRepository, MailboxType, MessageBodyStore,
            MessageId, MessageReadRepository, ProjectionCheckpoint, ProviderType, ProviderUid,
            ReadModelProjector,
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
                DomainEvent::MailboxCountsUpdated {
                    mailbox_id,
                    total_messages: 1,
                    unread_messages: 1,
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
    };
}

pub(crate) use read_repository_conformance;
