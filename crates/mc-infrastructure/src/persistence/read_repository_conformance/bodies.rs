macro_rules! bodies_conformance {
    ($make:expr) => {
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
                        remote_id: RemoteId::from(uid),
                        envelope: test_envelope(),
                        flags: vec![],
                        size_bytes: 0,
                        has_attachments: false,
                        message_rfc_id: None,
                        in_reply_to: None,
                        references: vec![],
                        received_at: at,
                        restored: false,
                        provider_key: None,
                    },
                )
            };

            repo.project(&recv(1, older, 10, t0)).await.unwrap();
            repo.project(&recv(2, newer, 20, t1)).await.unwrap();

            // Newest received comes first, carrying the right remote_id.
            let pending = repo.list_pending_backfill(&account_id, 10).await.unwrap();
            assert_eq!(pending.len(), 2);
            assert_eq!(pending[0].message_id.to_string(), newer.to_string());
            assert_eq!(pending[0].remote_id, RemoteId::from(20));
            assert_eq!(pending[1].message_id.to_string(), older.to_string());

            // Fetching a body dequeues exactly that entry.
            repo.project(&make_event_envelope(
                3,
                &newer.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched {
                    message_id: newer,
                    attachments: vec![],
                },
            ))
            .await
            .unwrap();

            let pending = repo.list_pending_backfill(&account_id, 10).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].message_id.to_string(), older.to_string());
        }

        #[tokio::test]
        async fn put_body_sets_body_on_existing_message() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let message_id = MessageId::new();
            repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
                .await
                .unwrap();

            repo.put_body(
                &message_id,
                Some("plain".into()),
                Some("<p>html</p>".into()),
                false,
            )
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
            repo.put_body(&MessageId::new(), Some("x".into()), None, false)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn put_body_alone_does_not_drain_backfill() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let message_id = MessageId::new();
            repo.project(&received_event(account_id, message_id, chrono::Utc::now()))
                .await
                .unwrap();

            repo.put_body(&message_id, Some("plain".into()), None, false)
                .await
                .unwrap();

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
            repo.put_body(&message_id, Some("plain".into()), None, false)
                .await
                .unwrap();

            let fetched = make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched {
                    message_id,
                    attachments: vec![],
                },
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

        #[tokio::test]
        async fn replaying_received_and_body_fetched_keeps_stored_body() {
            let (_guard, _es, repo) = $make;
            let account_id = AccountId::new();
            let message_id = MessageId::new();
            let received = received_event(account_id, message_id, chrono::Utc::now());
            repo.project(&received).await.unwrap();
            repo.put_body(&message_id, Some("plain".into()), None, false)
                .await
                .unwrap();
            repo.project(&body_fetched(message_id)).await.unwrap();

            // Boot catch-up replays everything since the last saved checkpoint.
            repo.project(&received).await.unwrap();
            repo.project(&body_fetched(message_id)).await.unwrap();

            assert_eq!(
                body_text(&repo, &message_id).await.as_deref(),
                Some("plain")
            );
            assert_eq!(repo.count_pending_backfill(&account_id).await.unwrap(), 0);
        }

        #[tokio::test]
        async fn backfill_skips_messages_waiting_for_their_uid() {
            let (_guard, _es, repo) = $make;
            let (account, archive) = (AccountId::new(), MailboxId::new());
            let (waiting, e_waiting) =
                received(account, archive, 4, Some("<w@x>"), &[], true, at(0));
            let (_ready, e_ready) = received(account, archive, 5, Some("<r@x>"), &[], true, at(1));
            project_all(
                &repo,
                [
                    discovered(account, archive, "Archive"),
                    e_waiting,
                    e_ready,
                    DomainEvent::MessageMailboxesChanged {
                        message_id: waiting,
                        added: vec![Membership {
                            mailbox_id: archive,
                            remote_id: None,
                        }],
                        removed: vec![archive],
                    },
                ],
            )
            .await;

            let pending = repo.list_pending_backfill(&account, 10).await.unwrap();
            assert_eq!(
                pending
                    .iter()
                    .map(|e| e.remote_id.clone())
                    .collect::<Vec<_>>(),
                [RemoteId::from(5)]
            );
            assert_eq!(repo.count_pending_backfill(&account).await.unwrap(), 1);
        }

        #[tokio::test]
        async fn backfill_goes_inbox_then_folders_then_all_mail_and_skips_spam_and_trash() {
            let (_guard, _es, repo) = $make;
            let account = AccountId::new();
            let typed = |path: &str, mailbox_type| {
                let id = MailboxId::new();
                let event = DomainEvent::MailboxDiscovered {
                    mailbox_id: id,
                    account_id: account,
                    path: MailboxPath::parse(path, '/').unwrap(),
                    mailbox_type,
                };
                (id, event)
            };
            let (all, e_all) = typed("[Gmail]/Tutti i messaggi", MailboxType::All);
            let (important, e_important) = typed("[Gmail]/Importanti", MailboxType::Important);
            let (work, e_work) = typed("Work", MailboxType::Custom);
            let (inbox, e_inbox) = typed("INBOX", MailboxType::Inbox);
            let (spam, e_spam) = typed("[Gmail]/Spam", MailboxType::Spam);
            let (trash, e_trash) = typed("[Gmail]/Cestino", MailboxType::Trash);
            // Newest in the folders that must wait, oldest in INBOX.
            let messages = [
                (all, 1, 9),
                (important, 2, 8),
                (spam, 3, 7),
                (trash, 4, 6),
                (work, 5, 2),
                (work, 6, 3),
                (inbox, 7, 0),
                (inbox, 8, 1),
            ];
            let mut events = vec![e_all, e_important, e_work, e_inbox, e_spam, e_trash];
            for (mailbox, uid, minutes) in messages {
                events.push(received(account, mailbox, uid, None, &[], false, at(minutes)).1);
            }
            project_all(&repo, events).await;

            let pending = repo.list_pending_backfill(&account, 10).await.unwrap();
            assert_eq!(
                pending
                    .iter()
                    .map(|e| e.remote_id.clone())
                    .collect::<Vec<_>>(),
                [8, 7, 6, 5, 1, 2].map(RemoteId::from)
            );
            assert_eq!(repo.count_pending_backfill(&account).await.unwrap(), 6);
            assert_eq!(
                repo.account_stats(&account).await.unwrap().pending_bodies,
                6
            );
        }

        #[tokio::test]
        async fn copies_of_a_message_share_one_body_full_over_truncated() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, all) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (in_inbox, e1) = received(account, inbox, 1, Some("<m@x>"), &[], false, at(0));
            let (in_all, e2) = received(account, all, 7, Some("<m@x>"), &[], false, at(0));
            let (other, e3) = received(account, all, 8, None, &[], false, at(0));
            project_all(
                &repo,
                [
                    discovered(account, inbox, "INBOX"),
                    discovered(account, all, "All"),
                    e1,
                    e2,
                    e3,
                ],
            )
            .await;

            // Nothing stored yet: nothing to share.
            assert!(
                repo.share_bodies(&account, &["<m@x>".into()])
                    .await
                    .unwrap()
                    .is_empty()
            );

            repo.put_body(&in_inbox, Some("start".into()), None, true)
                .await
                .unwrap();
            repo.project(&body_fetched(in_inbox)).await.unwrap();
            let shared = repo
                .share_bodies(&account, &["<m@x>".into()])
                .await
                .unwrap();
            assert_eq!(shared, [(in_all, true)]);
            repo.project(&body_fetched(in_all)).await.unwrap();
            let copy = MessageReadRepository::get_message(&repo, &in_all)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (copy.body_text.as_deref(), copy.body_truncated),
                (Some("start"), true)
            );

            // Opening one fetches the whole text; the other copy gets it too.
            repo.put_body(&in_all, Some("start and the rest".into()), None, false)
                .await
                .unwrap();
            assert_eq!(
                repo.share_bodies(&account, &["<m@x>".into()])
                    .await
                    .unwrap(),
                [(in_inbox, false)]
            );
            let first = MessageReadRepository::get_message(&repo, &in_inbox)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (first.body_text.as_deref(), first.body_truncated),
                (Some("start and the rest"), false)
            );
            assert!(
                repo.share_bodies(&account, &["<m@x>".into(), "<>".into()])
                    .await
                    .unwrap()
                    .is_empty()
            );

            // A message without a Message-ID shares with nothing.
            assert_eq!(body_text(&repo, &other).await, None);
            assert_eq!(repo.count_pending_backfill(&account).await.unwrap(), 1);
        }

        #[tokio::test]
        async fn a_text_only_body_fetch_keeps_the_attachment_flag() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (id, mut event) = received(account, inbox, 1, None, &[], false, at(0));
            if let DomainEvent::MessageReceived {
                has_attachments, ..
            } = &mut event
            {
                *has_attachments = true;
            }
            project_all(&repo, [discovered(account, inbox, "INBOX"), event]).await;

            repo.put_body(&id, Some("text".into()), None, false)
                .await
                .unwrap();
            repo.project(&body_fetched(id)).await.unwrap();

            let message = MessageReadRepository::get_message(&repo, &id)
                .await
                .unwrap()
                .unwrap();
            assert!(message.has_attachments);
        }

        #[tokio::test]
        async fn a_reused_message_id_with_another_size_shares_nothing() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (real, e1) = received(account, inbox, 1, Some("<m@x>"), &[], false, at(0));
            let (forged, mut e2) = received(account, inbox, 2, Some("<m@x>"), &[], false, at(1));
            if let DomainEvent::MessageReceived { size_bytes, .. } = &mut e2 {
                *size_bytes = 999;
            }
            project_all(&repo, [discovered(account, inbox, "INBOX"), e1, e2]).await;

            repo.put_body(&forged, Some("forged".into()), None, false)
                .await
                .unwrap();
            repo.project(&body_fetched(forged)).await.unwrap();

            assert!(
                repo.share_bodies(&account, &["<m@x>".into()])
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(body_text(&repo, &real).await, None);
            let pending = repo.list_pending_backfill(&account, 10).await.unwrap();
            assert_eq!(
                pending
                    .iter()
                    .map(|e| (e.message_id, e.size_bytes))
                    .collect::<Vec<_>>(),
                [(real, 100)]
            );
        }
    };
}

pub(crate) use bodies_conformance;
