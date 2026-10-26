macro_rules! messages_conformance {
    ($make:expr) => {
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
                    remote_id: RemoteId::from(42),
                    envelope: test_envelope(),
                    flags: vec![Flag::Recent],
                    size_bytes: 2048,
                    has_attachments: false,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    received_at,
                    restored: false,
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
            repo.put_body(
                &message_id,
                Some("Hello plain".into()),
                Some("<p>Hello</p>".into()),
                false,
            )
            .await
            .unwrap();
            let env = make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched {
                    message_id,
                    attachments: vec![],
                },
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
                    mailbox_ids: vec![mailbox_id],
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
                        remote_id: RemoteId::from(pos as u32),
                        envelope: test_envelope(),
                        flags: vec![], // unread
                        size_bytes: 10,
                        has_attachments: false,
                        message_rfc_id: Some(rfc.to_string()),
                        in_reply_to: irt.map(|s| s.to_string()),
                        references: vec![],
                        received_at: now,
                        restored: false,
                    },
                )
            };

            // Two messages in the same thread (a reply to the root).
            repo.project(&received(root_id, 1, "<a@x>", None))
                .await
                .unwrap();
            repo.project(&received(reply_id, 2, "<b@x>", Some("<a@x>")))
                .await
                .unwrap();

            let page = Pagination {
                page: 1,
                page_size: 10,
            };

            // One conversation, two messages, both unread.
            let convs =
                mc_domain::traits::repository::ConversationReadRepository::list_conversations(
                    &repo,
                    &account_id,
                    None,
                    &page,
                )
                .await
                .unwrap();
            assert_eq!(convs.items.len(), 1);
            assert_eq!(convs.items[0].message_count, 2);
            assert_eq!(convs.items[0].unread_count, 2);
            assert_eq!(
                MessageReadRepository::get_thread(&repo, &root_id)
                    .await
                    .unwrap()
                    .len(),
                2
            );

            // Delete the reply → conversation shrinks, thread drops it.
            repo.project(&make_event_envelope(
                3,
                &reply_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageDeleted {
                    message_id: reply_id,
                    mailbox_ids: vec![mailbox_id],
                },
            ))
            .await
            .unwrap();

            let convs =
                mc_domain::traits::repository::ConversationReadRepository::list_conversations(
                    &repo,
                    &account_id,
                    None,
                    &page,
                )
                .await
                .unwrap();
            assert_eq!(
                convs.items.len(),
                1,
                "conversation still present with 1 message"
            );
            assert_eq!(convs.items[0].message_count, 1);
            assert_eq!(convs.items[0].unread_count, 1);
            let thread = MessageReadRepository::get_thread(&repo, &root_id)
                .await
                .unwrap();
            assert_eq!(thread.len(), 1, "deleted reply no longer in thread");
            assert_eq!(thread[0].id, root_id);

            // Delete the root → conversation removed entirely.
            repo.project(&make_event_envelope(
                4,
                &root_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageDeleted {
                    message_id: root_id,
                    mailbox_ids: vec![mailbox_id],
                },
            ))
            .await
            .unwrap();

            let convs =
                mc_domain::traits::repository::ConversationReadRepository::list_conversations(
                    &repo,
                    &account_id,
                    None,
                    &page,
                )
                .await
                .unwrap();
            assert!(convs.items.is_empty(), "empty conversation removed");
            assert!(
                MessageReadRepository::get_thread(&repo, &root_id)
                    .await
                    .unwrap()
                    .is_empty(),
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
                    remote_id: RemoteId::from(1),
                    envelope: test_envelope(),
                    flags: vec![],
                    size_bytes: 1024,
                    has_attachments: false,
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    received_at,
                    restored: false,
                },
            );
            repo.project(&env).await.unwrap();

            // Move to archive.
            let env = make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageMailboxesChanged {
                    message_id,
                    added: vec![Membership {
                        mailbox_id: archive_id,
                        remote_id: None,
                    }],
                    removed: vec![inbox_id],
                },
            );
            repo.project(&env).await.unwrap();

            // Message should now reference archive.
            let msg = MessageReadRepository::get_message(&repo, &message_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                msg.memberships,
                [Membership {
                    mailbox_id: archive_id,
                    remote_id: None
                }]
            );

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
            // The inbox UID means nothing in archive.
            assert_eq!(
                repo.pending_messages(&archive_id).await.unwrap(),
                [message_id]
            );

            // The UID learned after the move lands on the same row.
            let env = make_event_envelope(
                3,
                &message_id.to_string(),
                AggregateType::Message,
                3,
                DomainEvent::MessageMailboxesChanged {
                    message_id,
                    added: vec![Membership {
                        mailbox_id: archive_id,
                        remote_id: Some(RemoteId::from(77)),
                    }],
                    removed: vec![],
                },
            );
            repo.project(&env).await.unwrap();
            let found = repo
                .find_by_remote_id(&archive_id, &RemoteId::from(77u32))
                .await
                .unwrap();
            assert_eq!(found.map(|m| m.id), Some(message_id));
            assert!(repo.pending_messages(&archive_id).await.unwrap().is_empty());
            let held = repo
                .held_ids(&archive_id, &[RemoteId::from(5), RemoteId::from(77)])
                .await
                .unwrap();
            assert_eq!(held, [RemoteId::from(77)].into());
            assert!(
                repo.held_ids(&inbox_id, &[RemoteId::from(77)])
                    .await
                    .unwrap()
                    .is_empty()
            );
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
                    remote_id: RemoteId::from(1),
                    envelope: test_envelope(),
                    flags: vec![Flag::Seen],
                    size_bytes: 1024,
                    has_attachments: false,
                    message_rfc_id: Some(root_rfc_id.clone()),
                    in_reply_to: None,
                    references: vec![],
                    received_at: chrono::Utc::now(),
                    restored: false,
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
            assert_eq!(sent_msg.memberships.len(), 1);
            assert_eq!(sent_msg.memberships[0].mailbox_id, sent_mailbox_id);

            // (2) Must appear in the Sent mailbox's list immediately, with
            // sender and recipient populated (not blank).
            let sent_list = MessageReadRepository::list_messages(
                &repo,
                &sent_mailbox_id,
                &Pagination {
                    page: 1,
                    page_size: 10,
                },
            )
            .await
            .unwrap();
            assert_eq!(sent_list.total_count, 1);
            assert_eq!(sent_list.items[0].from_email, "sender@example.com");
            assert_eq!(
                sent_list.items[0].to,
                vec!["recipient@example.com".to_string()]
            );

            // (3) Reply must thread with the root message, not fork into a new
            // conversation.
            let thread = MessageReadRepository::get_thread(&repo, &root_id)
                .await
                .unwrap();
            let thread_ids: Vec<_> = thread.iter().map(|m| m.id).collect();
            assert!(thread_ids.contains(&root_id));
            assert!(thread_ids.contains(&reply_id));
        }

        #[tokio::test]
        async fn list_messages_pages_newest_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let mut msgs = Vec::new();
            for minute in 0..5 {
                let (id, event) = received(
                    account,
                    inbox,
                    minute as u32 + 1,
                    None,
                    &[],
                    false,
                    at(minute),
                );
                repo.project(&env_of(event)).await.unwrap();
                msgs.push(id);
            }

            let page = repo
                .list_messages(&inbox, &Pagination::new(1, 2))
                .await
                .unwrap();
            assert_eq!(page.total_count, 5);
            assert_eq!(ids(&page.items, |m| m.id), [msgs[4], msgs[3]]);
            let last = repo
                .list_messages(&inbox, &Pagination::new(3, 2))
                .await
                .unwrap();
            assert_eq!(ids(&last.items, |m| m.id), [msgs[0]]);
        }

        #[tokio::test]
        async fn find_by_remote_id_is_scoped_and_survives_uid_reuse() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (old, e_old) = received(account, inbox, 7, None, &[], false, at(0));
            let (_elsewhere, e_elsewhere) = received(account, archive, 7, None, &[], false, at(1));
            // UIDVALIDITY reset: the server hands out uid 7 again in the same mailbox.
            let (new, e_new) = received(account, inbox, 7, None, &[], false, at(2));
            project_all(&repo, [e_old, e_elsewhere, e_new]).await;

            let found = repo
                .find_by_remote_id(&inbox, &RemoteId::from(7u32))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(found.id, new);
            assert_eq!(
                found.membership(inbox).and_then(|m| m.remote_id.clone()),
                Some(RemoteId::from(7))
            );
            assert!(
                repo.find_by_remote_id(&inbox, &RemoteId::from(8u32))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_ne!(found.id, old);
        }
    };
}

pub(crate) use messages_conformance;
