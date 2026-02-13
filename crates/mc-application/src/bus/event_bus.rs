//! Event bus for publishing and subscribing to domain events.
//!
//! The [`InProcessEventBus`] uses `tokio::sync::broadcast` for in-process
//! fan-out. Every subscriber receives every event (at-least-once within
//! the process lifetime; durable at-least-once is handled by the event
//! store subscription + projection checkpoints).

use mc_domain::EventEnvelope;
use tokio::sync::broadcast;
use tracing::{debug, warn};

/// Trait for publishing domain events to interested subscribers.
pub trait EventPublisher: Send + Sync {
    /// Publish an event envelope to all subscribers.
    fn publish(&self, envelope: EventEnvelope);
}

/// A handle for receiving events from the bus.
pub struct EventSubscriber {
    receiver: broadcast::Receiver<EventEnvelope>,
}

impl EventSubscriber {
    /// Receive the next event. Returns `None` if the bus is closed.
    ///
    /// If the subscriber falls behind, lagged events are skipped and
    /// a warning is logged.
    pub async fn recv(&mut self) -> Option<EventEnvelope> {
        loop {
            match self.receiver.recv().await {
                Ok(envelope) => return Some(envelope),
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!(
                        lagged = count,
                        "event subscriber lagged, skipped {count} events"
                    );
                    // Continue to receive the next available event.
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

/// In-process event bus backed by `tokio::sync::broadcast`.
///
/// # Capacity
/// The default capacity is 1024 events. If all subscribers lag behind by
/// more than this, older events are dropped for slow consumers.
pub struct InProcessEventBus {
    sender: broadcast::Sender<EventEnvelope>,
}

impl InProcessEventBus {
    /// Create a new event bus with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Create a new subscriber that will receive all future events.
    pub fn subscribe(&self) -> EventSubscriber {
        EventSubscriber {
            receiver: self.sender.subscribe(),
        }
    }

    /// Number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for InProcessEventBus {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl EventPublisher for InProcessEventBus {
    fn publish(&self, envelope: EventEnvelope) {
        debug!(
            event = envelope.payload.event_name(),
            aggregate_id = %envelope.aggregate_id,
            "publishing event to bus"
        );
        // send returns Err if there are no receivers — that's fine,
        // it just means nobody is listening yet.
        let _ = self.sender.send(envelope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{
        AccountId, AggregateType, DomainEvent, EmailAddress, EventEnvelope, ProviderType,
    };

    fn make_envelope() -> EventEnvelope {
        EventEnvelope::new(
            "acc-test".into(),
            AggregateType::Account,
            1,
            DomainEvent::AccountAdded {
                account_id: AccountId::new(),
                name: "Test".into(),
                email: EmailAddress::new("test@example.com").unwrap(),
                provider_type: ProviderType::Imap,
            },
        )
    }

    #[tokio::test]
    async fn publish_and_receive() {
        let bus = InProcessEventBus::default();
        let mut sub = bus.subscribe();

        let envelope = make_envelope();
        bus.publish(envelope.clone());

        let received = sub.recv().await.unwrap();
        assert_eq!(received.aggregate_id, "acc-test");
        assert_eq!(received.payload.event_name(), "AccountAdded");
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_same_event() {
        let bus = InProcessEventBus::default();
        let mut sub1 = bus.subscribe();
        let mut sub2 = bus.subscribe();

        bus.publish(make_envelope());

        let r1 = sub1.recv().await.unwrap();
        let r2 = sub2.recv().await.unwrap();
        assert_eq!(r1.aggregate_id, r2.aggregate_id);
    }

    #[tokio::test]
    async fn no_subscribers_does_not_panic() {
        let bus = InProcessEventBus::default();
        bus.publish(make_envelope()); // no subscribers — should not panic
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn subscriber_count() {
        let bus = InProcessEventBus::default();
        assert_eq!(bus.subscriber_count(), 0);

        let _sub1 = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 1);

        let _sub2 = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 2);

        drop(_sub1);
        assert_eq!(bus.subscriber_count(), 1);
    }
}
