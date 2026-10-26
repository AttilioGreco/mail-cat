macro_rules! conversations_conformance {
    ($make:expr) => {
        #[tokio::test]
        async fn get_thread_returns_the_whole_thread_oldest_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (late, e_late) = received(
                account,
                inbox,
                3,
                Some("<late@x>"),
                &["<root@x>"],
                false,
                at(20),
            );
            let (mid, e_mid) = received(
                account,
                inbox,
                2,
                Some("<mid@x>"),
                &["<root@x>"],
                false,
                at(10),
            );
            let (_other, e_other) =
                received(account, inbox, 4, Some("<other@x>"), &[], false, at(5));
            project_all(&repo, [e_root, e_late, e_mid, e_other]).await;

            let thread = repo.get_thread(&late).await.unwrap();
            assert_eq!(ids(&thread, |m| m.id), [root, mid, late]);
            assert!(repo.get_thread(&MessageId::new()).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn list_conversations_scoped_to_a_mailbox() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(
                account,
                archive,
                2,
                Some("<reply@x>"),
                &["<root@x>"],
                true,
                at(10),
            );
            let (solo, e_solo) = received(account, inbox, 3, Some("<solo@x>"), &[], true, at(5));
            project_all(&repo, [e_root, e_reply, e_solo]).await;
            let page = first_page();

            let all = repo
                .list_conversations(&account, None, &page)
                .await
                .unwrap();
            assert_eq!(all.total_count, 2);
            let stats: Vec<_> = all
                .items
                .iter()
                .map(|c| (c.message_count, c.unread_count, c.sample_message_id))
                .collect();
            assert_eq!(stats, [(2, 1, reply), (1, 0, solo)]);

            let in_inbox = repo
                .list_conversations(&account, Some(&inbox), &page)
                .await
                .unwrap();
            assert_eq!(in_inbox.total_count, 2);
            let stats: Vec<_> = in_inbox
                .items
                .iter()
                .map(|c| (c.message_count, c.unread_count, c.sample_message_id))
                .collect();
            assert_eq!(stats, [(1, 0, solo), (1, 1, root)]);
            assert_eq!(in_inbox.items[1].last_message_at, at(0));
            assert_eq!(in_inbox.items[1].subject, "Hello World");
            assert_eq!(in_inbox.items[1].latest_sender, "Sender");

            let in_archive = repo
                .list_conversations(&account, Some(&archive), &page)
                .await
                .unwrap();
            assert_eq!(in_archive.total_count, 1);
            assert_eq!(in_archive.items[0].sample_message_id, reply);
            assert_eq!(in_archive.items[0].unread_count, 0);
        }

        #[tokio::test]
        async fn rebuild_index_restores_conversations() {
            let (_guard, store, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let (_root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (reply, e_reply) = received(
                account,
                inbox,
                2,
                Some("<reply@x>"),
                &["<root@x>"],
                false,
                at(10),
            );
            let log = append_all(
                store.as_ref(),
                vec![e_root, e_reply, flagged(reply, vec![Flag::Seen], vec![])],
            )
            .await;
            for event in &log {
                repo.project(event).await.unwrap();
            }
            let thread = repo
                .list_conversations(&account, None, &first_page())
                .await
                .unwrap()
                .items[0]
                .thread_id;
            let before = repo
                .get_conversation(&thread, &account)
                .await
                .unwrap()
                .unwrap();

            assert!(repo.rebuild_index().await.unwrap() > 0);

            let after = repo
                .get_conversation(&thread, &account)
                .await
                .unwrap()
                .unwrap();
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
            assert_eq!(
                repo.list_messages(&inbox, &first_page())
                    .await
                    .unwrap()
                    .total_count,
                0
            );
            assert_eq!(repo.count_pending_backfill(&account).await.unwrap(), 0);

            project_all(&repo, [DomainEvent::DraftDiscarded { message_id: draft }]).await;
            assert!(repo.get_message(&draft).await.unwrap().is_none());
        }
    };
}

pub(crate) use conversations_conformance;
