macro_rules! contacts_conformance {
    ($make:expr) => {
        #[tokio::test]
        async fn contact_search_ranks_prefix_hits_then_correspondence() {
            let (_guard, _es, repo) = $make;
            let (account, inbox, sent) = (AccountId::new(), MailboxId::new(), MailboxId::new());
            let mail = |from: &str, to: &str| Envelope {
                from: vec![Address {
                    name: None,
                    email: email(from),
                }],
                to: vec![Address {
                    name: None,
                    email: email(to),
                }],
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
                remote_id: RemoteId::from(1),
                message_rfc_id: None,
                in_reply_to: None,
                references: vec![],
                envelope: mail(from, to),
                flags: vec![],
                size_bytes: 1,
                has_attachments: false,
                received_at: at(5),
                restored: false,
                provider_key: None,
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
                        display_path: MailboxPath::parse("INBOX", '/').unwrap().full_path(),
                        mailbox_type: MailboxType::Inbox,
                    },
                    DomainEvent::MailboxDiscovered {
                        mailbox_id: sent,
                        account_id: account,
                        path: MailboxPath::parse("Sent", '/').unwrap(),
                        display_path: MailboxPath::parse("Sent", '/').unwrap().full_path(),
                        mailbox_type: MailboxType::Sent,
                    },
                    contact("Joanna", "joanna@example.com"),
                    contact("Zed", "zed@annex.org"),
                    contact("Bob Ann", "bob@example.com"),
                    contact("Zoe Annabel", "annabel@example.com"),
                    contact("Ann Vega", "ann.rare@example.com"),
                    contact("Anne Wu", "ann.often@example.com"),
                    // An auto-created duplicate collapses onto the named copy.
                    contact("", "ann.often@example.com"),
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
                [
                    "Anne Wu",
                    "Ann Vega",
                    "Zoe Annabel",
                    "Bob Ann",
                    "Zed",
                    "Joanna"
                ],
            );
            assert_eq!(
                names(repo.search_contacts("ann", 2).await.unwrap()),
                ["Anne Wu", "Ann Vega"]
            );
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
                        ContactEmail::new(
                            email("ada@work.example.com"),
                            ContactEmailLabel::Work,
                            false,
                        ),
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
            let found = repo
                .find_by_email(&email("ada@work.example.com"))
                .await
                .unwrap();
            assert_eq!(found.iter().map(|c| c.id).collect::<Vec<_>>(), [contact]);
            assert_eq!(
                repo.find_by_email_in_account(&email("ada@example.com"), &account)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert!(
                repo.find_by_email_in_account(&email("ada@example.com"), &AccountId::new())
                    .await
                    .unwrap()
                    .is_empty()
            );
            // A name match suggests each of the person's addresses.
            let addresses = |hits: Vec<mc_domain::ContactSummary>| {
                hits.into_iter()
                    .map(|c| c.primary_email.as_str().to_owned())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                addresses(repo.search_contacts("lovelace", 10).await.unwrap()),
                ["ada@example.com", "ada@work.example.com"],
            );
            assert_eq!(
                repo.search_contacts("ADA@EXAMPLE", 10).await.unwrap().len(),
                1
            );
            // Autocomplete must find a contact by any of its addresses.
            assert_eq!(
                addresses(repo.search_contacts("ada@work", 10).await.unwrap()),
                ["ada@work.example.com"]
            );
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
            assert!(
                repo.find_by_email(&email("ada@work.example.com"))
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                repo.find_by_email(&email("ada@king.example.com"))
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                repo.list_contacts(&first_page()).await.unwrap().total_count,
                1
            );

            project_all(
                &repo,
                [DomainEvent::ContactRemoved {
                    contact_id: contact,
                }],
            )
            .await;
            assert!(repo.get_contact(&contact).await.unwrap().is_none());
            assert!(
                repo.find_by_email(&email("ada@king.example.com"))
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                repo.list_contacts(&first_page()).await.unwrap().total_count,
                0
            );
        }

        #[tokio::test]
        async fn duplicate_candidates_and_merge() {
            let (_guard, _es, repo) = $make;
            let (account, other_account) = (AccountId::new(), AccountId::new());
            let (c1, c2, c3) = (ContactId::new(), ContactId::new(), ContactId::new());
            let added = |id: ContactId, emails: Vec<ContactEmail>, scope: AccountId| {
                DomainEvent::ContactAdded {
                    contact_id: id,
                    display_name: id.to_string(),
                    emails,
                    source: ContactSource::LocalEmail,
                    external_id: None,
                    account_scope: Some(scope),
                }
            };
            let shared = || ContactEmail::primary(email("shared@example.com"));
            project_all(
                &repo,
                [
                    added(
                        c1,
                        vec![
                            shared(),
                            ContactEmail::new(
                                email("one@example.com"),
                                ContactEmailLabel::Home,
                                false,
                            ),
                        ],
                        account,
                    ),
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

            let scoped = pair_ids(
                repo.find_duplicate_candidates(Some(&account))
                    .await
                    .unwrap(),
            );
            assert_eq!(scoped, [sorted(c1, c2)]);
            let mut everywhere = vec![sorted(c1, c2), sorted(c1, c3), sorted(c2, c3)];
            everywhere.sort();
            assert_eq!(
                pair_ids(repo.find_duplicate_candidates(None).await.unwrap()),
                everywhere
            );

            project_all(
                &repo,
                [DomainEvent::ContactMerged {
                    kept_id: c1,
                    discarded_id: c2,
                    merged_emails: vec![
                        shared(),
                        ContactEmail::new(email("one@example.com"), ContactEmailLabel::Home, false),
                    ],
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
            assert!(
                repo.find_duplicate_candidates(Some(&account))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn suggest_groups_matching_values_by_field_most_frequent_first() {
            let (_guard, _es, repo) = $make;
            let (account, inbox) = (AccountId::new(), MailboxId::new());
            let addr = |name: Option<&str>, raw: &str| Address {
                name: name.map(str::to_owned),
                email: email(raw),
            };
            let mail = |uid: u32, from: Address, to: &str, subject: &str| {
                let (_, mut event) =
                    received(account, inbox, uid, None, &[], false, at(uid as i64));
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
                    mail(
                        1,
                        addr(Some("Ada Lovelace"), "ada@example.com"),
                        "me@example.com",
                        "Notes",
                    ),
                    mail(
                        2,
                        addr(Some("Ada Lovelace"), "ada@example.com"),
                        "me@example.com",
                        "More notes",
                    ),
                    mail(
                        3,
                        addr(Some("Bob"), "bob@example.com"),
                        "adam@example.com",
                        "Adaptive design",
                    ),
                    mail(
                        4,
                        addr(None, "carol@example.com"),
                        "me@example.com",
                        "Lunch",
                    ),
                ],
            )
            .await;
            let found = |s: Vec<mc_domain::Suggestion>| {
                s.into_iter()
                    .map(|s| (s.field, s.value, s.doc_count))
                    .collect::<Vec<_>>()
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
                found(
                    repo.suggest("example", Some(SuggestScope::From), 2)
                        .await
                        .unwrap()
                ),
                [
                    row("from_email", "ada@example.com", 2),
                    row("from_email", "bob@example.com", 1)
                ]
            );
            assert_eq!(
                found(
                    repo.suggest("me@", Some(SuggestScope::To), 5)
                        .await
                        .unwrap()
                ),
                [row("to", "me@example.com", 3)]
            );
            assert!(
                repo.suggest("notes", Some(SuggestScope::Body), 5)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(repo.suggest("", None, 5).await.unwrap().is_empty());
        }
    };
}

pub(crate) use contacts_conformance;
