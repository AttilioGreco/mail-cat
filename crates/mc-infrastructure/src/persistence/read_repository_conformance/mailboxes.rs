macro_rules! mailboxes_conformance {
    ($make:expr) => {
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
            assert_eq!(
                mailbox.total_messages, 42,
                "counts must survive reclassification"
            );

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
                    remote_id: RemoteId::from(42),
                    envelope: test_envelope(),
                    flags: vec![Flag::Recent],
                    size_bytes: 2048,
                    has_attachments: false,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    received_at: chrono::Utc::now(),
                    restored: false,
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
                    flags_added: vec![],
                    flags_removed: vec![Flag::Seen],
                },
            );
            repo.project(&env).await.unwrap();
            assert_eq!(unread!(), 1);
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

            project_all(&repo, [flagged(unread, vec![Flag::Seen], vec![])]).await;
            assert!(
                repo.find_unread_by_mailbox(&inbox)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    };
}

pub(crate) use mailboxes_conformance;
