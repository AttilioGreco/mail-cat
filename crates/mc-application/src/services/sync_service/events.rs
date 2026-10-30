//! Writing sync's results: events appended to the log and projected, and the
//! transient server-reported state that is projected only.

use mc_domain::{
    AggregateType, Attachment, DomainError, DomainEvent, EventEnvelope, EventStore, Mailbox,
    Message, MessageId, ProviderRegistry,
};

use super::{SyncService, SyncStore};
use crate::error::ApplicationError;

/// Maps a storage failure to a sync error saying which step failed.
pub(super) fn sync_failed(step: &'static str) -> impl FnOnce(DomainError) -> ApplicationError {
    move |e| ApplicationError::SyncFailed(format!("{step} failed: {e}"))
}

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// Apply `change` to the message's current state and append what it
    /// produces at that state's sequence: a decision taken from the read model
    /// must not land on a message that has changed since (moved again, say).
    pub(super) async fn update_message(
        &self,
        message_id: &MessageId,
        change: impl FnOnce(&mut Message) -> Option<DomainEvent>,
    ) -> Result<bool, ApplicationError> {
        let aggregate_id = message_id.to_string();
        let events = self
            .event_store
            .load_events(&aggregate_id, 0)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to load events: {e}")))?;
        let seq = events.len() as u64;
        let events: Vec<DomainEvent> = events.into_iter().map(|e| e.payload).collect();
        let Some(event) = Message::rehydrate(&events).as_mut().and_then(change) else {
            return Ok(false);
        };
        self.append_at(&aggregate_id, AggregateType::Message, seq, event)
            .await?;
        Ok(true)
    }

    /// Body first, then the event: if the append fails, the backfill queue (drained
    /// only by projecting `MessageBodyFetched`) still lists the message, so it is
    /// fetched again instead of being marked done without a record.
    pub(super) async fn store_body(
        &self,
        message_id: &MessageId,
        body_text: Option<String>,
        body_html: Option<String>,
        truncated: bool,
        attachments: Vec<Attachment>,
    ) -> Result<(), ApplicationError> {
        self.projector
            .put_body(message_id, body_text, body_html, truncated)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to store body: {e}")))?;
        let event = DomainEvent::MessageBodyFetched {
            message_id: *message_id,
            attachments,
        };
        self.append_and_project(&message_id.to_string(), AggregateType::Message, event)
            .await
    }

    /// Append a domain event to the event store and project it into the read model.
    ///
    /// Reads the aggregate's current sequence for optimistic concurrency, then
    /// appends the event and immediately projects it.
    pub(super) async fn append_and_project(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        let expected_seq = self
            .event_store
            .current_sequence(aggregate_id)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to load sequence: {e}")))?;
        self.append_at(aggregate_id, aggregate_type, expected_seq, event)
            .await
    }

    pub(super) async fn append_at(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        expected_seq: u64,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        let new_seq = self
            .event_store
            .append(
                aggregate_id,
                aggregate_type,
                expected_seq,
                vec![event.clone()],
            )
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to append event: {e}")))?;

        let envelope = EventEnvelope::new(aggregate_id.to_string(), aggregate_type, new_seq, event);
        self.project_and_publish(envelope).await
    }

    /// Server-reported state (sync lifecycle, mailbox counts): applied to the
    /// read model and announced on the bus, but never appended — it is not a
    /// domain fact, and appending it every poll grew the log without bound.
    pub(super) async fn project_transient(
        &self,
        aggregate_id: &str,
        aggregate_type: AggregateType,
        event: DomainEvent,
    ) -> Result<(), ApplicationError> {
        self.project_and_publish(EventEnvelope::new(
            aggregate_id.to_string(),
            aggregate_type,
            0,
            event,
        ))
        .await
    }

    pub(super) async fn project_and_publish(
        &self,
        envelope: EventEnvelope,
    ) -> Result<(), ApplicationError> {
        self.projector
            .project(&envelope)
            .await
            .map_err(|e| ApplicationError::SyncFailed(format!("Failed to project event: {e}")))?;
        if let Some(publisher) = &self.event_publisher {
            publisher.publish(envelope);
        }
        Ok(())
    }

    /// Update mailbox message counts (transient: read model + bus, not the log).
    pub(super) async fn update_mailbox_counts(
        &self,
        mailbox: &Mailbox,
        total_messages: u32,
        unread_messages: u32,
    ) -> Result<(), ApplicationError> {
        let event = DomainEvent::MailboxCountsUpdated {
            mailbox_id: mailbox.id,
            total_messages,
            unread_messages,
        };
        self.project_transient(&mailbox.id.to_string(), AggregateType::Mailbox, event)
            .await
    }
}

impl<PR: ProviderRegistry, ES: EventStore, P: SyncStore> SyncService<PR, ES, P> {
    /// The model the provider prefers, recorded once: only while the account's
    /// log holds no model change, which a user's own choice would have added.
    pub(super) async fn default_mailbox_model(
        &self,
        account_id: &mc_domain::AccountId,
        model: mc_domain::MailboxModel,
    ) -> Result<(), ApplicationError> {
        let stream = account_id.to_string();
        let chosen = self
            .event_store
            .load_events(&stream, 0)
            .await
            .map_err(sync_failed("load_events"))?
            .iter()
            .any(|e| matches!(e.payload, DomainEvent::AccountMailboxModelChanged { .. }));
        if chosen {
            return Ok(());
        }
        let event = DomainEvent::AccountMailboxModelChanged {
            account_id: *account_id,
            model,
        };
        self.append_and_project(&stream, AggregateType::Account, event)
            .await
    }
}
