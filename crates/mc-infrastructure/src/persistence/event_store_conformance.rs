//! `EventStore` behaviour every backend must share, instantiated per backend.

macro_rules! event_store_conformance {
    ($make:expr) => {
        use futures::{StreamExt, TryStreamExt};
        use mc_domain::{
            AccountId, AggregateType, DomainError, DomainEvent, EmailAddress, EventEnvelope,
            EventStore, ProviderType,
        };

        fn make_event() -> DomainEvent {
            DomainEvent::AccountAdded {
                account_id: AccountId::new(),
                name: "Test".into(),
                email: EmailAddress::new("test@example.com").unwrap(),
                provider_type: ProviderType::Imap,
            }
        }

        #[tokio::test]
        async fn append_and_load_events() {
            let (_guard, store) = $make;

            let seq = store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();
            assert_eq!(seq, 1);
            let seq = store
                .append("acc-1", AggregateType::Account, 1, vec![make_event(), make_event()])
                .await
                .unwrap();
            assert_eq!(seq, 3);

            let events = store.load_events("acc-1", 0).await.unwrap();
            let sequences: Vec<u64> = events.iter().map(|e| e.sequence_number).collect();
            assert_eq!(sequences, [1, 2, 3]);
            assert_eq!(store.current_sequence("acc-1").await.unwrap(), 3);
            assert_eq!(store.current_sequence("unknown").await.unwrap(), 0);
        }

        #[tokio::test]
        async fn load_events_from_sequence() {
            let (_guard, store) = $make;
            store
                .append(
                    "acc-1",
                    AggregateType::Account,
                    0,
                    vec![make_event(), make_event(), make_event()],
                )
                .await
                .unwrap();

            let events = store.load_events("acc-1", 2).await.unwrap();
            let sequences: Vec<u64> = events.iter().map(|e| e.sequence_number).collect();
            assert_eq!(sequences, [2, 3]);
        }

        #[tokio::test]
        async fn concurrency_conflict() {
            let (_guard, store) = $make;
            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();

            let err = store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap_err();
            assert!(
                matches!(err, DomainError::ConcurrencyConflict { expected: 0, actual: 1 }),
                "got {err:?}"
            );
        }

        #[tokio::test]
        async fn global_position_is_monotonic() {
            let (_guard, store) = $make;
            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();
            store
                .append("acc-2", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();

            let all: Vec<EventEnvelope> =
                store.load_all_events(None, 0).try_collect().await.unwrap();
            let order: Vec<(u64, &str)> =
                all.iter().map(|e| (e.global_position, e.aggregate_id.as_str())).collect();
            assert_eq!(order, [(1, "acc-1"), (2, "acc-2")]);
        }

        #[tokio::test]
        async fn load_all_events_filtered_by_type() {
            let (_guard, store) = $make;
            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();
            let discarded = DomainEvent::DraftDiscarded { message_id: mc_domain::MessageId::new() };
            store
                .append("msg-1", AggregateType::Message, 0, vec![discarded])
                .await
                .unwrap();

            for (filter, expected) in [
                (Some(AggregateType::Account), 1),
                (Some(AggregateType::Message), 1),
                (None, 2),
            ] {
                let events: Vec<EventEnvelope> =
                    store.load_all_events(filter, 0).try_collect().await.unwrap();
                assert_eq!(events.len(), expected, "filter {filter:?}");
            }
        }

        #[tokio::test]
        async fn empty_append_returns_expected_sequence() {
            let (_guard, store) = $make;
            let seq = store.append("acc-1", AggregateType::Account, 0, vec![]).await.unwrap();
            assert_eq!(seq, 0);
        }

        #[tokio::test]
        async fn separate_aggregates_have_independent_sequences() {
            let (_guard, store) = $make;
            for id in ["acc-1", "acc-2"] {
                store.append(id, AggregateType::Account, 0, vec![make_event()]).await.unwrap();
            }
            for id in ["acc-1", "acc-2"] {
                let events = store.load_events(id, 0).await.unwrap();
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].sequence_number, 1);
            }
        }

        #[tokio::test]
        async fn subscribe_receives_new_events() {
            let (_guard, store) = $make;
            let mut live = store.subscribe();

            store
                .append("acc-1", AggregateType::Account, 0, vec![make_event()])
                .await
                .unwrap();

            let envelope = live.next().await.expect("should receive event");
            assert_eq!(envelope.aggregate_id, "acc-1");
            assert_eq!(envelope.payload.event_name(), "AccountAdded");
        }
    };
}

pub(crate) use event_store_conformance;
