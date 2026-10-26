macro_rules! memberships_conformance {
    ($make:expr) => {
        #[tokio::test]
        async fn a_message_in_two_mailboxes_is_listed_and_counted_in_both() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, label) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (id, e_received) = received(account, inbox, 1, Some("<m@x>"), &[], false, at(0));
            project_all(
                &repo,
                [
                    discovered(account, inbox, "INBOX"),
                    discovered(account, label, "Label"),
                    e_received,
                    gained(id, label, Some(5)),
                ],
            )
            .await;
            let page = first_page();

            for mailbox in [inbox, label] {
                let listed = repo.list_messages(&mailbox, &page).await.unwrap();
                assert_eq!(ids(&listed.items, |m| m.id), [id]);
                assert_eq!(listed.total_count, 1);
                assert_eq!(
                    listed.items[0].memberships.len(),
                    2,
                    "the summary lists every mailbox"
                );
                assert_eq!(unread_in(&repo, &mailbox).await, 1);
                assert_eq!(
                    repo.find_unread_by_mailbox(&mailbox).await.unwrap().len(),
                    1
                );
                let threads = repo
                    .list_conversations(&account, Some(&mailbox), &page)
                    .await
                    .unwrap();
                assert_eq!(threads.items.len(), 1);
            }
            assert_eq!(
                repo.find_by_remote_id(&label, &RemoteId::from(5u32))
                    .await
                    .unwrap()
                    .map(|m| m.id),
                Some(id)
            );
            assert_eq!(
                repo.find_by_remote_id(&inbox, &RemoteId::from(1u32))
                    .await
                    .unwrap()
                    .map(|m| m.id),
                Some(id)
            );
            assert!(
                repo.find_by_remote_id(&inbox, &RemoteId::from(5u32))
                    .await
                    .unwrap()
                    .is_none(),
                "UIDs are per mailbox"
            );
            let whole = repo
                .list_conversations(&account, None, &page)
                .await
                .unwrap();
            assert_eq!(
                whole.items[0].message_count, 1,
                "one message, however many mailboxes"
            );

            // Flags belong to the message: every mailbox it is in follows.
            project_all(&repo, [flagged(id, vec![Flag::Seen], vec![])]).await;
            assert_eq!(
                (
                    unread_in(&repo, &inbox).await,
                    unread_in(&repo, &label).await
                ),
                (0, 0)
            );
            project_all(&repo, [flagged(id, vec![], vec![Flag::Seen])]).await;
            assert_eq!(
                (
                    unread_in(&repo, &inbox).await,
                    unread_in(&repo, &label).await
                ),
                (1, 1)
            );

            // Replaying the membership change counts nothing twice.
            project_all(&repo, [gained(id, label, Some(5))]).await;
            assert_eq!(
                (
                    unread_in(&repo, &inbox).await,
                    unread_in(&repo, &label).await
                ),
                (1, 1)
            );
            assert_eq!(
                repo.held_ids(&label, &[RemoteId::from(5)])
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }

        #[tokio::test]
        async fn removing_one_membership_keeps_the_message_and_removing_the_last_deletes_it() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, label) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (id, e_received) = received(account, inbox, 1, Some("<m@x>"), &[], false, at(0));
            project_all(
                &repo,
                [
                    discovered(account, inbox, "INBOX"),
                    discovered(account, label, "Label"),
                    e_received,
                    gained(id, label, None),
                ],
            )
            .await;
            let page = first_page();
            assert_eq!(repo.pending_messages(&label).await.unwrap(), [id]);

            project_all(
                &repo,
                [DomainEvent::MessageMailboxesChanged {
                    message_id: id,
                    added: vec![],
                    removed: vec![inbox],
                }],
            )
            .await;

            assert_eq!(
                repo.list_messages(&inbox, &page).await.unwrap().total_count,
                0
            );
            assert_eq!(
                repo.list_messages(&label, &page).await.unwrap().total_count,
                1
            );
            assert_eq!(
                (
                    unread_in(&repo, &inbox).await,
                    unread_in(&repo, &label).await
                ),
                (0, 1)
            );
            let kept = repo.get_message(&id).await.unwrap().unwrap();
            assert_eq!(
                kept.memberships,
                [Membership {
                    mailbox_id: label,
                    remote_id: None
                }]
            );
            assert_eq!(
                repo.list_conversations(&account, None, &page)
                    .await
                    .unwrap()
                    .items
                    .len(),
                1
            );

            project_all(
                &repo,
                [DomainEvent::MessageDeleted {
                    message_id: id,
                    mailbox_ids: vec![label],
                }],
            )
            .await;

            assert!(repo.get_message(&id).await.unwrap().is_none());
            assert_eq!(
                repo.list_messages(&label, &page).await.unwrap().total_count,
                0
            );
            assert_eq!(unread_in(&repo, &label).await, 0);
            assert!(
                repo.list_conversations(&account, None, &page)
                    .await
                    .unwrap()
                    .items
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn rfc_copies_count_once_per_conversation() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, all_mail) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (_a, e_a) = received(account, inbox, 1, Some("<same@x>"), &[], false, at(0));
            let (_b, e_b) = received(account, all_mail, 9, Some("<same@x>"), &[], false, at(0));
            project_all(&repo, [e_a, e_b]).await;

            let convs = repo
                .list_conversations(&account, None, &first_page())
                .await
                .unwrap();
            assert_eq!(convs.items.len(), 1);
            assert_eq!(
                (convs.items[0].message_count, convs.items[0].unread_count),
                (1, 1)
            );
        }

        /// A Sent server copy drops its own placeholder — never a UID-less
        /// copy of the same Message-ID waiting in another mailbox.
        #[tokio::test]
        async fn sent_copy_drops_only_placeholders_in_sent() {
            let (_guard, _es, repo) = $make;
            let (account, sent, archive) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let (waiting, e_waiting) =
                received(account, archive, 4, Some("<me@x>"), &[], true, at(0));
            let (server_copy, e_server_copy) =
                received(account, sent, 9, Some("<me@x>"), &[], true, at(1));
            project_all(
                &repo,
                [
                    DomainEvent::MailboxDiscovered {
                        mailbox_id: sent,
                        account_id: account,
                        path: MailboxPath::parse("Sent", '/').unwrap(),
                        mailbox_type: MailboxType::Sent,
                    },
                    discovered(account, archive, "Archive"),
                    e_waiting,
                    DomainEvent::MessageMailboxesChanged {
                        message_id: waiting,
                        added: vec![Membership {
                            mailbox_id: archive,
                            remote_id: None,
                        }],
                        removed: vec![archive],
                    },
                    e_server_copy,
                ],
            )
            .await;

            assert!(
                repo.get_message(&waiting).await.unwrap().is_some(),
                "not a placeholder"
            );
            assert!(repo.get_message(&server_copy).await.unwrap().is_some());
        }
    };
}

pub(crate) use memberships_conformance;
