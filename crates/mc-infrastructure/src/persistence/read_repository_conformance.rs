//! Read-model behaviour every backend must share, instantiated per backend.

pub(crate) mod accounts;
pub(crate) mod bodies;
pub(crate) mod contacts;
pub(crate) mod conversations;
pub(crate) mod mailboxes;
pub(crate) mod memberships;
pub(crate) mod messages;
pub(crate) mod replay;

macro_rules! read_repository_conformance {
    ($make:expr) => {
        use futures::TryStreamExt;
        use mc_domain::{
            Account, AccountId, AccountReadRepository, AccountStatus, AccountSyncStats,
            AccountWriteRepository, Address, AggregateType, AuthConfig, BodyBackfillQueue,
            ConnectionConfig, ContactEmail, ContactEmailLabel, ContactId, ContactReadRepository,
            ContactSource, ConversationReadRepository, DomainEvent, EmailAddress, Envelope,
            EventEnvelope, EventStore, Flag, Identity, IncomingServer, MailboxId, MailboxPath,
            MailboxReadRepository, MailboxType, Membership, MessageBodyStore, MessageId,
            MessageReadRepository, ProjectionCheckpoint, ProviderType, ReadModelProjector,
            RemoteId, SuggestScope,
        };
        use mc_shared::pagination::Pagination;

        fn test_config() -> ConnectionConfig {
            ConnectionConfig {
                incoming: IncomingServer::Imap {
                    host: "imap.example.com".into(),
                    port: 993,
                    use_tls: true,
                },
                auth: AuthConfig::Plain {
                    username: "user@example.com".into(),
                    password: "secret".into(),
                },
                smtp: None,
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
                    remote_id: RemoteId::from(1),
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
                    restored: false,
                    provider_key: None,
                },
            )
        }

        fn body_fetched(message_id: MessageId) -> EventEnvelope {
            make_event_envelope(
                2,
                &message_id.to_string(),
                AggregateType::Message,
                2,
                DomainEvent::MessageBodyFetched {
                    message_id,
                    attachments: vec![],
                },
            )
        }

        async fn body_text(repo: &impl MessageReadRepository, id: &MessageId) -> Option<String> {
            MessageReadRepository::get_message(repo, id)
                .await
                .unwrap()
                .unwrap()
                .body_text
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
        async fn append_all<ES: EventStore>(
            store: &ES,
            events: Vec<DomainEvent>,
        ) -> Vec<EventEnvelope> {
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
                remote_id: RemoteId::from(uid),
                message_rfc_id: rfc.map(str::to_owned),
                in_reply_to: None,
                references: references.iter().map(|r| r.to_string()).collect(),
                envelope,
                flags: if seen { vec![Flag::Seen] } else { vec![] },
                size_bytes: 100,
                has_attachments: false,
                received_at,
                restored: false,
                provider_key: None,
            };
            (message_id, event)
        }

        fn discovered(account_id: AccountId, mailbox_id: MailboxId, path: &str) -> DomainEvent {
            DomainEvent::MailboxDiscovered {
                mailbox_id,
                account_id,
                path: MailboxPath::parse(path, '/').unwrap(),
                display_path: MailboxPath::parse(path, '/').unwrap().full_path(),
                mailbox_type: MailboxType::Custom,
            }
        }

        fn flagged(message_id: MessageId, add: Vec<Flag>, remove: Vec<Flag>) -> DomainEvent {
            DomainEvent::MessageFlagged {
                message_id,
                flags_added: add,
                flags_removed: remove,
            }
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
                out += &format!(
                    "{:?}\n",
                    repo.list_messages(mailbox, &page).await.unwrap().items
                );
                out += &format!(
                    "{:?}\n",
                    repo.find_unread_by_mailbox(mailbox).await.unwrap()
                );
                out += &format!(
                    "{:?}\n",
                    repo.list_conversations(account_id, Some(mailbox), &page)
                        .await
                        .unwrap()
                        .items
                );
            }
            out += &format!(
                "{:?}\n",
                repo.scan_all_messages(Some(account_id)).await.unwrap()
            );
            out += &format!(
                "{:?}\n",
                repo.list_conversations(account_id, None, &page)
                    .await
                    .unwrap()
                    .items
            );
            out += &format!("{:?}\n", repo.list_contacts(&page).await.unwrap().items);
            out += &format!(
                "{:?}\n",
                repo.count_pending_backfill(account_id).await.unwrap()
            );
            out
        }

        async fn unread_in(repo: &impl MailboxReadRepository, mailbox: &MailboxId) -> u32 {
            repo.get_mailbox(mailbox)
                .await
                .unwrap()
                .unwrap()
                .unread_messages
        }

        fn gained(message_id: MessageId, mailbox_id: MailboxId, uid: Option<u32>) -> DomainEvent {
            DomainEvent::MessageMailboxesChanged {
                message_id,
                added: vec![Membership {
                    mailbox_id,
                    remote_id: uid.map(RemoteId::from),
                }],
                removed: vec![],
            }
        }

        $crate::persistence::read_repository_conformance::accounts::accounts_conformance!($make);
        $crate::persistence::read_repository_conformance::bodies::bodies_conformance!($make);
        $crate::persistence::read_repository_conformance::contacts::contacts_conformance!($make);
        $crate::persistence::read_repository_conformance::conversations::conversations_conformance!(
            $make
        );
        $crate::persistence::read_repository_conformance::mailboxes::mailboxes_conformance!($make);
        $crate::persistence::read_repository_conformance::memberships::memberships_conformance!(
            $make
        );
        $crate::persistence::read_repository_conformance::messages::messages_conformance!($make);
        $crate::persistence::read_repository_conformance::replay::replay_conformance!($make);
    };
}

pub(crate) use read_repository_conformance;
