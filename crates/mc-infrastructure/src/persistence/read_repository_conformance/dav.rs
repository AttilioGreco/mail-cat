macro_rules! dav_conformance {
    ($make:expr) => {
        use mc_domain::{
            AddressBook, Calendar, CalendarComponent, CalendarEvent, CollectionId,
            DavReadRepository, Etag, Href, Origin, Ownership, PendingChange, RemoteBinding,
            RemoteCard, formats,
        };

        const ROME_SERIES: &str = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//t//\r\n\
            BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\n\
            DTSTART;TZID=Europe/Rome:20261024T100000\r\nDTEND;TZID=Europe/Rome:20261024T110000\r\n\
            RRULE:FREQ=DAILY;COUNT=5\r\nEXDATE;TZID=Europe/Rome:20261026T100000\r\nSUMMARY:Standup\r\n\
            END:VEVENT\r\n\
            BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\n\
            RECURRENCE-ID;TZID=Europe/Rome:20261025T100000\r\n\
            DTSTART;TZID=Europe/Rome:20261025T140000\r\nDTEND;TZID=Europe/Rome:20261025T150000\r\n\
            SUMMARY:Standup (moved)\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";

        const ONE_OFF: &str = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//t//\r\n\
            BEGIN:VEVENT\r\nUID:one\r\nDTSTAMP:20260101T000000Z\r\n\
            DTSTART:20261026T120000Z\r\nDTEND:20261026T130000Z\r\nSUMMARY:Lunch\r\n\
            END:VEVENT\r\nEND:VCALENDAR\r\n";

        fn dav_binding(
            account: AccountId,
            collection: &str,
            href: &str,
            etag: Option<&str>,
        ) -> RemoteBinding {
            RemoteBinding {
                account,
                collection: CollectionId::new(collection),
                href: Href::new(href),
                etag: etag.map(Etag::new),
                remote_modified: None,
            }
        }

        fn card(binding: RemoteBinding, raw: &str, origin: Origin) -> RemoteCard {
            RemoteCard {
                binding,
                raw: raw.into(),
                origin,
            }
        }

        fn remote_contact_added(id: ContactId, card: RemoteCard, emails: &[&str]) -> DomainEvent {
            DomainEvent::ContactAdded {
                contact_id: id,
                display_name: "Remote".into(),
                emails: emails
                    .iter()
                    .map(|e| ContactEmail::primary(email(e)))
                    .collect(),
                source: ContactSource::CardDav,
                external_id: None,
                account_scope: Some(card.binding.account),
                remote: Some(card),
            }
        }

        fn remote_contact_updated(id: ContactId, card: RemoteCard) -> DomainEvent {
            DomainEvent::ContactUpdated {
                contact_id: id,
                display_name: None,
                emails: None,
                phones: None,
                organization: None,
                notes: None,
                remote: Some(card),
            }
        }

        fn pushed(id: ContactId, href: &str, etag: &str) -> DomainEvent {
            DomainEvent::ContactPushedToServer {
                contact_id: id,
                href: Href::new(href),
                etag: Etag::new(etag),
            }
        }

        fn calendar_event(binding: RemoteBinding, raw: &str) -> CalendarEvent {
            CalendarEvent {
                binding,
                raw: raw.into(),
                fields: formats::parse_event(raw).unwrap().fields,
            }
        }

        fn calendar(account: AccountId, collection: &str, name: &str) -> Calendar {
            Calendar {
                account,
                collection: CollectionId::new(collection),
                name: name.into(),
                color: None,
                timezone: Some("Europe/Rome".into()),
                read_only: false,
                ownership: Ownership::Own,
                components: vec![CalendarComponent::Event],
            }
        }

        fn book_of(account: AccountId, collection: &str, name: &str) -> AddressBook {
            AddressBook {
                account,
                collection: CollectionId::new(collection),
                name: name.into(),
                description: None,
                read_only: false,
            }
        }

        #[tokio::test]
        async fn contact_is_dirty_from_a_local_edit_until_pushed() {
            let (_guard, _es, repo) = $make;
            let (account, id) = (AccountId::new(), ContactId::new());
            let book = CollectionId::new("/book/");
            let bound = |etag| dav_binding(account, "/book/", "/book/a.vcf", etag);

            // Created locally: nothing on the server yet, so a create to push.
            project_all(
                &repo,
                [remote_contact_added(
                    id,
                    card(bound(None), "v1", Origin::Local),
                    &["a@example.com"],
                )],
            )
            .await;
            let pending = repo.pending_contacts(&account, &book).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].aggregate_id, id.to_string());
            assert_eq!(pending[0].binding.etag, None);
            assert_eq!(pending[0].change, PendingChange::Put { raw: "v1".into() });
            assert!(repo.contact_etags(&account, &book).await.unwrap().is_empty());

            project_all(&repo, [pushed(id, "/book/a.vcf", "e1")]).await;
            assert!(repo.pending_contacts(&account, &book).await.unwrap().is_empty());
            let etags = repo.contact_etags(&account, &book).await.unwrap();
            assert_eq!(etags[&Href::new("/book/a.vcf")], Etag::new("e1"));

            // An edit based on e1 is dirty again, and keeps e1 as its base.
            project_all(
                &repo,
                [remote_contact_updated(id, card(bound(Some("e1")), "v2", Origin::Local))],
            )
            .await;
            let pending = repo.pending_contacts(&account, &book).await.unwrap();
            assert_eq!(pending[0].change, PendingChange::Put { raw: "v2".into() });
            assert_eq!(pending[0].binding.etag, Some(Etag::new("e1")));

            // A refusal is recorded, and a later edit starts over.
            project_all(
                &repo,
                [DomainEvent::ContactPushFailed {
                    contact_id: id,
                    reason: "403".into(),
                }],
            )
            .await;
            let pending = repo.pending_contacts(&account, &book).await.unwrap();
            assert_eq!(pending[0].failure.as_deref(), Some("403"));
            project_all(
                &repo,
                [remote_contact_updated(id, card(bound(Some("e1")), "v3", Origin::Local))],
            )
            .await;
            let pending = repo.pending_contacts(&account, &book).await.unwrap();
            assert_eq!(pending[0].failure, None);

            // What the server says is never dirty.
            project_all(
                &repo,
                [remote_contact_updated(id, card(bound(Some("e2")), "v4", Origin::Remote))],
            )
            .await;
            assert!(repo.pending_contacts(&account, &book).await.unwrap().is_empty());
            let etags = repo.contact_etags(&account, &book).await.unwrap();
            assert_eq!(etags[&Href::new("/book/a.vcf")], Etag::new("e2"));
        }

        #[tokio::test]
        async fn local_delete_of_a_server_contact_waits_for_the_server() {
            let (_guard, _es, repo) = $make;
            let account = AccountId::new();
            let book = CollectionId::new("/book/");
            let (synced, never_pushed) = (ContactId::new(), ContactId::new());
            project_all(
                &repo,
                [
                    remote_contact_added(
                        synced,
                        card(
                            dav_binding(account, "/book/", "/book/a.vcf", Some("e1")),
                            "v1",
                            Origin::Remote,
                        ),
                        &["a@example.com"],
                    ),
                    remote_contact_added(
                        never_pushed,
                        card(
                            dav_binding(account, "/book/", "/book/b.vcf", None),
                            "v1",
                            Origin::Local,
                        ),
                        &["b@example.com"],
                    ),
                    DomainEvent::ContactRemoved {
                        contact_id: synced,
                        origin: Origin::Local,
                    },
                    DomainEvent::ContactRemoved {
                        contact_id: never_pushed,
                        origin: Origin::Local,
                    },
                ],
            )
            .await;

            assert!(repo.get_contact(&synced).await.unwrap().is_none());
            // Only the contact the server has is left to delete there.
            let pending = repo.pending_contacts(&account, &book).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].aggregate_id, synced.to_string());
            assert_eq!(pending[0].change, PendingChange::Delete);
            assert_eq!(pending[0].binding.etag, Some(Etag::new("e1")));

            project_all(&repo, [pushed(synced, "/book/a.vcf", "e1")]).await;
            assert!(repo.pending_contacts(&account, &book).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn each_lost_conflict_counts_once_even_when_reprojected() {
            let (_guard, _es, repo) = $make;
            let (account, other, id) = (AccountId::new(), AccountId::new(), ContactId::new());
            project_all(
                &repo,
                [remote_contact_added(
                    id,
                    card(
                        dav_binding(account, "/book/", "/book/a.vcf", Some("e1")),
                        "v1",
                        Origin::Local,
                    ),
                    &["a@example.com"],
                )],
            )
            .await;
            let lost = env_of(DomainEvent::ContactConflictLost {
                contact_id: id,
                local_raw: "v1".into(),
            });
            repo.project(&lost).await.unwrap();
            repo.project(&lost).await.unwrap();
            assert_eq!(repo.conflicts_lost(&account).await.unwrap(), 1);

            project_all(
                &repo,
                [DomainEvent::CalendarEventConflictLost {
                    account_id: account,
                    href: Href::new("/cal/x.ics"),
                    local_raw: "v".into(),
                }],
            )
            .await;
            assert_eq!(repo.conflicts_lost(&account).await.unwrap(), 2);
            assert_eq!(repo.conflicts_lost(&other).await.unwrap(), 0);
        }

        #[tokio::test]
        async fn contact_without_email_is_tracked_but_not_listed() {
            let (_guard, _es, repo) = $make;
            let (account, id) = (AccountId::new(), ContactId::new());
            project_all(
                &repo,
                [remote_contact_added(
                    id,
                    card(
                        dav_binding(account, "/book/", "/book/p.vcf", Some("e1")),
                        "phone only",
                        Origin::Remote,
                    ),
                    &[],
                )],
            )
            .await;
            assert!(repo.get_contact(&id).await.unwrap().is_none());
            let all = repo.list_contacts(&Pagination::default()).await.unwrap();
            assert_eq!(all.total_count, 0);
            let etags = repo
                .contact_etags(&account, &CollectionId::new("/book/"))
                .await
                .unwrap();
            assert_eq!(etags.len(), 1);
        }

        #[tokio::test]
        async fn address_book_removal_takes_its_contacts() {
            let (_guard, _es, repo) = $make;
            let (account, id) = (AccountId::new(), ContactId::new());
            project_all(
                &repo,
                [
                    DomainEvent::AddressBookDiscovered {
                        book: book_of(account, "/book/", "Contacts"),
                    },
                    DomainEvent::AddressBookUpdated {
                        book: book_of(account, "/book/", "People"),
                    },
                    remote_contact_added(
                        id,
                        card(
                            dav_binding(account, "/book/", "/book/a.vcf", Some("e1")),
                            "v1",
                            Origin::Remote,
                        ),
                        &["a@example.com"],
                    ),
                ],
            )
            .await;
            let books = repo.list_address_books(&account).await.unwrap();
            assert_eq!(books.len(), 1);
            assert_eq!(books[0].name, "People");

            project_all(
                &repo,
                [DomainEvent::AddressBookRemoved {
                    account_id: account,
                    collection: CollectionId::new("/book/"),
                }],
            )
            .await;
            assert!(repo.list_address_books(&account).await.unwrap().is_empty());
            assert!(repo.get_contact(&id).await.unwrap().is_none());
        }

        #[tokio::test]
        async fn calendar_events_expand_for_a_range_and_track_pending_pushes() {
            let (_guard, _es, repo) = $make;
            let account = AccountId::new();
            let cal = CollectionId::new("/cal/");
            let series = Href::new("/cal/series.ics");
            let at_utc = |text: &str| {
                chrono::DateTime::parse_from_rfc3339(text)
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            };
            project_all(
                &repo,
                [
                    DomainEvent::CalendarDiscovered {
                        calendar: calendar(account, "/cal/", "Work"),
                    },
                    DomainEvent::CalendarEventAdded {
                        event: calendar_event(
                            dav_binding(account, "/cal/", "/cal/series.ics", Some("e1")),
                            ROME_SERIES,
                        ),
                        origin: Origin::Remote,
                    },
                    DomainEvent::CalendarEventAdded {
                        event: calendar_event(
                            dav_binding(account, "/cal/", "/cal/one.ics", Some("e2")),
                            ONE_OFF,
                        ),
                        origin: Origin::Local,
                    },
                ],
            )
            .await;

            // The override replaces the 25th, the EXDATE drops the 26th, and
            // Rome's DST ends on the 25th: 10:00 is 08:00Z before, 09:00Z after.
            let found = repo
                .events_in_range(
                    &account,
                    &[],
                    at_utc("2026-10-24T00:00:00Z"),
                    at_utc("2026-10-31T00:00:00Z"),
                )
                .await
                .unwrap();
            let seen: Vec<_> = found
                .iter()
                .map(|o| (o.event.start.to_rfc3339(), o.event.summary.as_str()))
                .collect();
            assert_eq!(
                seen,
                [
                    ("2026-10-24T08:00:00+00:00".to_string(), "Standup"),
                    ("2026-10-25T13:00:00+00:00".to_string(), "Standup (moved)"),
                    ("2026-10-26T12:00:00+00:00".to_string(), "Lunch"),
                    ("2026-10-27T09:00:00+00:00".to_string(), "Standup"),
                    ("2026-10-28T09:00:00+00:00".to_string(), "Standup"),
                ]
            );
            let only_lunch = repo
                .events_in_range(
                    &account,
                    &[],
                    at_utc("2026-10-26T12:30:00Z"),
                    at_utc("2026-10-26T14:00:00Z"),
                )
                .await
                .unwrap();
            assert_eq!(only_lunch.len(), 1);
            assert!(
                repo.events_in_range(
                    &account,
                    &[CollectionId::new("/other/")],
                    at_utc("2026-10-24T00:00:00Z"),
                    at_utc("2026-10-31T00:00:00Z"),
                )
                .await
                .unwrap()
                .is_empty()
            );

            // The one the user created is pending; the synced one is not.
            let pending = repo.pending_events(&account, &cal).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].binding.href, Href::new("/cal/one.ics"));
            assert_eq!(pending[0].aggregate_id.split(':').next(), Some("calendar_event"));
            project_all(
                &repo,
                [DomainEvent::CalendarEventPushedToServer {
                    account_id: account,
                    href: Href::new("/cal/one.ics"),
                    etag: Etag::new("e3"),
                }],
            )
            .await;
            assert!(repo.pending_events(&account, &cal).await.unwrap().is_empty());
            let etags = repo.event_etags(&account, &cal).await.unwrap();
            assert_eq!(etags[&Href::new("/cal/one.ics")], Etag::new("e3"));
            assert_eq!(etags[&series], Etag::new("e1"));
            let loaded = repo.get_event(&account, &series).await.unwrap().unwrap();
            assert_eq!(loaded.raw, ROME_SERIES);
            assert!(loaded.fields.unwrap().recurring);

            // Deleting locally leaves a DELETE to push; removing the calendar
            // takes the rest.
            project_all(
                &repo,
                [DomainEvent::CalendarEventRemoved {
                    account_id: account,
                    href: series.clone(),
                    origin: Origin::Local,
                }],
            )
            .await;
            let pending = repo.pending_events(&account, &cal).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].change, PendingChange::Delete);
            project_all(
                &repo,
                [DomainEvent::CalendarRemoved {
                    account_id: account,
                    collection: cal.clone(),
                }],
            )
            .await;
            assert!(repo.list_calendars(&account).await.unwrap().is_empty());
            assert!(repo.pending_events(&account, &cal).await.unwrap().is_empty());
            assert!(repo.event_etags(&account, &cal).await.unwrap().is_empty());
        }
    };
}

pub(crate) use dav_conformance;
