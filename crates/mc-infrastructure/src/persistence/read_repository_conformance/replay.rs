macro_rules! replay_conformance {
    ($make:expr) => {
        #[tokio::test]
        async fn replaying_mailbox_discovered_keeps_server_counts() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            project_all(
                &repo,
                [
                    discovered(account, inbox, "INBOX"),
                    DomainEvent::MailboxCountsUpdated {
                        mailbox_id: inbox,
                        total_messages: 10,
                        unread_messages: 4,
                    },
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
            let (reply, e_reply) = received(
                account,
                inbox,
                2,
                Some("<reply@x>"),
                &["<root@x>"],
                false,
                at(10),
            );
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
                    DomainEvent::MessageBodyFetched {
                        message_id: root,
                        attachments: vec![],
                    },
                    flagged(root, vec![Flag::Seen], vec![]),
                    DomainEvent::MessageMailboxesChanged {
                        message_id: reply,
                        added: vec![Membership {
                            mailbox_id: archive,
                            remote_id: None,
                        }],
                        removed: vec![inbox],
                    },
                    DomainEvent::MessageDeleted {
                        message_id: gone,
                        mailbox_ids: vec![inbox],
                    },
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
                    flagged(sent, vec![], vec![Flag::Seen]),
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
            repo.put_body(&root, Some("hello".into()), None, false)
                .await
                .unwrap();
            // Server counts are transient: they reach the read model only through project().
            project_all(
                &repo,
                [DomainEvent::MailboxCountsUpdated {
                    mailbox_id: inbox,
                    total_messages: 10,
                    unread_messages: 4,
                }],
            )
            .await;
            let live = snapshot(&repo, &account, &[inbox, archive]).await;

            for event in &log {
                repo.project(event).await.unwrap();
            }

            assert_eq!(snapshot(&repo, &account, &[inbox, archive]).await, live);
            assert_eq!(
                repo.get_mailbox(&inbox)
                    .await
                    .unwrap()
                    .unwrap()
                    .unread_messages,
                4,
                "replaying the received+deleted pair must not move the badge"
            );
        }

        /// Catch-up starts wherever the last checkpoint was, so replaying any
        /// suffix of the log over live state must change nothing.
        #[tokio::test]
        async fn replaying_any_suffix_over_live_state_changes_nothing() {
            let (_guard, store, repo) = $make;
            let (account, inbox, trash, archive, labels) = (
                AccountId::new(),
                MailboxId::new(),
                MailboxId::new(),
                MailboxId::new(),
                MailboxId::new(),
            );
            let (root, e_root) = received(account, inbox, 1, Some("<root@x>"), &[], false, at(0));
            let (mid, e_mid) = received(
                account,
                inbox,
                2,
                Some("<mid@x>"),
                &["<root@x>"],
                false,
                at(10),
            );
            let (newest, e_newest) = received(
                account,
                inbox,
                3,
                Some("<new@x>"),
                &["<root@x>"],
                false,
                at(20),
            );
            let (copy_a, e_copy_a) =
                received(account, inbox, 4, Some("<copy@x>"), &[], false, at(30));
            let (_copy_b, e_copy_b) =
                received(account, labels, 9, Some("<copy@x>"), &[], false, at(30));
            let (roamer, e_roamer) =
                received(account, inbox, 5, Some("<roam@x>"), &[], false, at(40));
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
                        display_path: "Old/Archive".into(),
                        display_name: "Archive".into(),
                    },
                    e_root,
                    e_mid,
                    e_newest,
                    e_copy_a,
                    e_copy_b,
                    e_roamer,
                    DomainEvent::MessageBodyFetched {
                        message_id: root,
                        attachments: vec![],
                    },
                    flagged(mid, vec![Flag::Seen], vec![]),
                    flagged(mid, vec![], vec![Flag::Seen]),
                    flagged(mid, vec![Flag::Seen], vec![]),
                    DomainEvent::MessageDeleted {
                        message_id: newest,
                        mailbox_ids: vec![inbox],
                    },
                    DomainEvent::MessageDeleted {
                        message_id: copy_a,
                        mailbox_ids: vec![inbox],
                    },
                    DomainEvent::MessageMailboxesChanged {
                        message_id: roamer,
                        added: vec![Membership {
                            mailbox_id: trash,
                            remote_id: None,
                        }],
                        removed: vec![inbox],
                    },
                    DomainEvent::MessageMailboxesChanged {
                        message_id: roamer,
                        added: vec![Membership {
                            mailbox_id: archive,
                            remote_id: None,
                        }],
                        removed: vec![trash],
                    },
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
                    flagged(sent, vec![], vec![Flag::Seen]),
                    contact(kept, "ada@example.com"),
                    contact(merged, "ada@work.example.com"),
                    DomainEvent::ContactMerged {
                        kept_id: kept,
                        discarded_id: merged,
                        merged_emails: vec![
                            ContactEmail::primary(email("ada@example.com")),
                            ContactEmail::new(
                                email("ada@work.example.com"),
                                ContactEmailLabel::Work,
                                false,
                            ),
                        ],
                    },
                    contact(removed, "bob@example.com"),
                    DomainEvent::ContactRemoved {
                        contact_id: removed,
                    },
                ],
            )
            .await;
            for event in &log {
                repo.project(event).await.unwrap();
            }
            repo.put_body(&root, Some("hello".into()), None, false)
                .await
                .unwrap();
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
                assert_eq!(
                    state().await,
                    live,
                    "replay from log[{from}..] changed the read model"
                );
            }
        }
    };
}

pub(crate) use replay_conformance;
