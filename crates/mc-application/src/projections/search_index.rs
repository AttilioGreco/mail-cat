//! Search-index projection handler.
//!
//! Projects domain events into the Tantivy search index, tracking its own
//! [`ProjectionCheckpoint`] (`"search_index"`) so the index is an event-sourced
//! projection — brought up to date incrementally from the event store rather
//! than fully rebuilt on every boot.
//!
//! Unlike a delta-apply projector, this handler is **refresh-by-id**: for each
//! message-affecting event it takes the `message_id` and re-indexes that one
//! message from the read model (the source of truth for message content), or
//! removes it. This makes replay idempotent and self-healing — replaying an old
//! event just re-indexes the message's current state, and an event for a message
//! that no longer exists removes it.
//!
//! Ordering: because it reads the read model, the read-model projection for an
//! event must be applied before this handler reads it. Callers guarantee this
//! (read-model catch-up runs before search catch-up on boot; the live subscriber
//! chains read-model then search per event).

use futures::StreamExt;
use tracing::{debug, info};

use mc_domain::{
    DomainEvent, EventEnvelope, EventStore, MessageReadRepository, ProjectionCheckpoint, SearchIndex,
};

use crate::error::ApplicationError;

/// Name used for the search-index projection checkpoint.
const PROJECTION_NAME: &str = "search_index";

/// Flush the index and save the checkpoint at least this often during catch-up,
/// bounding how many events replay after a crash.
const CHECKPOINT_EVERY: u64 = 1000;

pub struct SearchProjectionHandler<SI, R, C> {
    search_index: SI,
    read_repo: R,
    checkpoint: C,
}

impl<SI, R, C> SearchProjectionHandler<SI, R, C>
where
    SI: SearchIndex,
    R: MessageReadRepository,
    C: ProjectionCheckpoint,
{
    pub fn new(search_index: SI, read_repo: R, checkpoint: C) -> Self {
        Self {
            search_index,
            read_repo,
            checkpoint,
        }
    }

    /// Refresh the index for the message an event refers to. Idempotent.
    async fn project(&self, envelope: &EventEnvelope) -> Result<(), ApplicationError> {
        let message_id = match &envelope.payload {
            DomainEvent::MessageDeleted { message_id, .. } => {
                self.search_index
                    .remove_message(message_id)
                    .await
                    .map_err(ApplicationError::Domain)?;
                return Ok(());
            }
            DomainEvent::MessageReceived { message_id, .. }
            | DomainEvent::MessageBodyFetched { message_id, .. }
            | DomainEvent::MessageMoved { message_id, .. }
            | DomainEvent::MessageFlagged { message_id, .. } => message_id,
            _ => return Ok(()),
        };

        // Refresh from the read model (source of truth for content). If the
        // message is gone, ensure it's not left in the index.
        match self
            .read_repo
            .get_message(message_id)
            .await
            .map_err(ApplicationError::Domain)?
        {
            Some(msg) => self
                .search_index
                .index_message(&msg)
                .await
                .map_err(ApplicationError::Domain),
            None => self
                .search_index
                .remove_message(message_id)
                .await
                .map_err(ApplicationError::Domain),
        }
    }

    /// Apply a single event on the live path (best-effort, for search latency).
    /// Deliberately does **not** touch the checkpoint: the event bus can drop
    /// events under load, so advancing the checkpoint here would leapfrog dropped
    /// events and lose them permanently. The checkpoint advances only in
    /// `catch_up`, which reads the event store sequentially with no gaps; that is
    /// the correctness backstop. `search()` flushes pending writes before
    /// querying, so no explicit flush is needed here.
    pub async fn apply_live(&self, envelope: &EventEnvelope) -> Result<(), ApplicationError> {
        self.project(envelope).await
    }

    /// Replay events after the checkpoint from the event store. On a fresh index
    /// (checkpoint 0) this indexes everything once; thereafter it's incremental.
    /// Commits and advances the checkpoint in batches so a crash replays at most
    /// `CHECKPOINT_EVERY` events (safe: indexing is idempotent).
    pub async fn catch_up<ES: EventStore>(&self, store: &ES) -> Result<u64, ApplicationError> {
        let last_pos = self
            .checkpoint
            .get_checkpoint(PROJECTION_NAME)
            .await
            .map_err(ApplicationError::Domain)?;

        debug!(from_position = last_pos + 1, "search index catching up");

        let mut stream = store.load_all_events(None, last_pos + 1);
        let mut count = 0u64;
        let mut last_committed = last_pos;
        let mut last_position = last_pos;
        while let Some(envelope) = stream.next().await {
            let envelope = envelope.map_err(ApplicationError::Domain)?;
            last_position = envelope.global_position;
            self.project(&envelope).await?;
            count += 1;
            if count.is_multiple_of(CHECKPOINT_EVERY) {
                self.search_index
                    .flush()
                    .await
                    .map_err(ApplicationError::Domain)?;
                self.checkpoint
                    .save_checkpoint(PROJECTION_NAME, last_position)
                    .await
                    .map_err(ApplicationError::Domain)?;
                last_committed = last_position;
                info!(progress = count, "search index catch-up progress");
            }
        }

        // Final flush + checkpoint for the remainder.
        if last_position > last_committed {
            self.search_index
                .flush()
                .await
                .map_err(ApplicationError::Domain)?;
            self.checkpoint
                .save_checkpoint(PROJECTION_NAME, last_position)
                .await
                .map_err(ApplicationError::Domain)?;
        }

        info!(count, "search index catch-up complete");
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use mc_domain::{
        AccountId, AggregateType, DomainError, Envelope, MailboxId, Message, MessageId,
        MessageSummary, SuggestScope, Suggestion,
    };
    use mc_shared::pagination::{PagedResult, Pagination};
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    // In-memory search index recording live message ids.
    #[derive(Clone, Default)]
    struct FakeIndex {
        docs: Arc<Mutex<HashSet<String>>>,
    }
    #[async_trait]
    impl SearchIndex for FakeIndex {
        async fn index_message(&self, m: &Message) -> Result<(), DomainError> {
            self.docs.lock().unwrap().insert(m.id.to_string());
            Ok(())
        }
        async fn remove_message(&self, id: &MessageId) -> Result<(), DomainError> {
            self.docs.lock().unwrap().remove(&id.to_string());
            Ok(())
        }
        async fn search(
            &self,
            _q: &str,
            _a: Option<&AccountId>,
            _mb: Option<&MailboxId>,
            _p: &Pagination,
        ) -> Result<mc_domain::traits::search_index::SearchResults, DomainError> {
            unimplemented!()
        }
        async fn suggest(
            &self,
            _p: &str,
            _s: Option<SuggestScope>,
            _m: usize,
        ) -> Result<Vec<Suggestion>, DomainError> {
            Ok(vec![])
        }
        async fn rebuild(&self, _m: Vec<Message>) -> Result<u64, DomainError> {
            Ok(0)
        }
        async fn flush(&self) -> Result<(), DomainError> {
            Ok(())
        }
    }

    // Read repo returning whatever messages are put in the map.
    #[derive(Clone, Default)]
    struct FakeRepo {
        messages: Arc<Mutex<HashMap<String, Message>>>,
    }
    #[async_trait]
    impl MessageReadRepository for FakeRepo {
        async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
            Ok(self.messages.lock().unwrap().get(&id.to_string()).cloned())
        }
        async fn list_messages(
            &self,
            _m: &MailboxId,
            _p: &Pagination,
        ) -> Result<PagedResult<MessageSummary>, DomainError> {
            unimplemented!()
        }
        async fn get_thread(&self, _id: &MessageId) -> Result<Vec<Message>, DomainError> {
            unimplemented!()
        }
        async fn find_by_provider_uid(
            &self,
            _m: &MailboxId,
            _u: u32,
        ) -> Result<Option<MessageSummary>, DomainError> {
            Ok(None)
        }
        async fn scan_all_messages(
            &self,
            _a: Option<&AccountId>,
        ) -> Result<Vec<Message>, DomainError> {
            Ok(vec![])
        }
    }

    #[derive(Clone, Default)]
    struct FakeCheckpoint {
        pos: Arc<Mutex<u64>>,
    }
    #[async_trait]
    impl ProjectionCheckpoint for FakeCheckpoint {
        async fn get_checkpoint(&self, _n: &str) -> Result<u64, DomainError> {
            Ok(*self.pos.lock().unwrap())
        }
        async fn save_checkpoint(&self, _n: &str, p: u64) -> Result<(), DomainError> {
            *self.pos.lock().unwrap() = p;
            Ok(())
        }
    }

    struct FakeStore {
        events: Vec<EventEnvelope>,
    }
    #[async_trait]
    impl EventStore for FakeStore {
        async fn append(
            &self,
            _a: &str,
            _t: AggregateType,
            _s: u64,
            _e: Vec<DomainEvent>,
        ) -> Result<u64, DomainError> {
            unimplemented!()
        }
        async fn load_events(
            &self,
            _a: &str,
            _s: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            unimplemented!()
        }
        fn load_all_events(
            &self,
            _t: Option<AggregateType>,
            from: u64,
        ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>>
        {
            let events: Vec<_> = self
                .events
                .iter()
                .filter(|e| e.global_position >= from)
                .cloned()
                .collect();
            Box::pin(futures::stream::iter(events.into_iter().map(Ok)))
        }
        fn subscribe(
            &self,
        ) -> std::pin::Pin<Box<dyn futures::Stream<Item = EventEnvelope> + Send + '_>> {
            unimplemented!()
        }
    }

    fn envelope() -> Envelope {
        Envelope {
            from: vec![],
            to: vec![],
            cc: vec![],
            bcc: vec![],
            reply_to: vec![],
            subject: "hi".into(),
            date: chrono::Utc::now(),
        }
    }

    fn msg(id: &str) -> Message {
        let now = chrono::Utc::now();
        Message {
            id: MessageId::from_uuid(uuid::Uuid::parse_str(id).unwrap()),
            account_id: AccountId::new(),
            mailbox_id: MailboxId::new(),
            provider_uid: None,
            message_rfc_id: None,
            in_reply_to: None,
            references: vec![],
            envelope: envelope(),
            flags: vec![],
            size_bytes: 0,
            has_attachments: false,
            attachments: vec![],
            body_text: None,
            body_html: None,
            received_at: now,
            internal_date: now,
        }
    }

    fn env(pos: u64, event: DomainEvent) -> EventEnvelope {
        let mut e = EventEnvelope::new("m".into(), AggregateType::Message, pos, event);
        e.global_position = pos;
        e
    }

    const ID1: &str = "01234567-89ab-cdef-0123-456789abcdef";

    #[tokio::test]
    async fn catch_up_indexes_present_and_removes_absent() {
        let index = FakeIndex::default();
        let repo = FakeRepo::default();
        let mid = MessageId::from_uuid(uuid::Uuid::parse_str(ID1).unwrap());
        repo.messages.lock().unwrap().insert(ID1.to_string(), msg(ID1));

        let handler =
            SearchProjectionHandler::new(index.clone(), repo.clone(), FakeCheckpoint::default());

        let store = FakeStore {
            events: vec![env(
                1,
                DomainEvent::MessageReceived {
                    message_id: mid,
                    account_id: AccountId::new(),
                    mailbox_id: MailboxId::new(),
                    provider_uid: 1.into(),
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    envelope: envelope(),
                    flags: vec![],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: chrono::Utc::now(),
                },
            )],
        };
        handler.catch_up(&store).await.unwrap();
        assert!(index.docs.lock().unwrap().contains(ID1));

        // Now delete it and replay from scratch — index converges to empty.
        repo.messages.lock().unwrap().clear();
        let store2 = FakeStore {
            events: vec![env(
                2,
                DomainEvent::MessageDeleted {
                    message_id: mid,
                    mailbox_id: MailboxId::new(),
                    permanently: false,
                },
            )],
        };
        handler.catch_up(&store2).await.unwrap();
        assert!(!index.docs.lock().unwrap().contains(ID1));
    }

    #[tokio::test]
    async fn catch_up_advances_checkpoint_and_is_idempotent() {
        let index = FakeIndex::default();
        let repo = FakeRepo::default();
        repo.messages.lock().unwrap().insert(ID1.to_string(), msg(ID1));
        let checkpoint = FakeCheckpoint::default();
        let mid = MessageId::from_uuid(uuid::Uuid::parse_str(ID1).unwrap());

        let handler =
            SearchProjectionHandler::new(index.clone(), repo.clone(), checkpoint.clone());
        let store = FakeStore {
            events: vec![env(
                7,
                DomainEvent::MessageMoved {
                    message_id: mid,
                    from_mailbox_id: MailboxId::new(),
                    to_mailbox_id: MailboxId::new(),
                },
            )],
        };

        assert_eq!(handler.catch_up(&store).await.unwrap(), 1);
        assert_eq!(*checkpoint.pos.lock().unwrap(), 7);
        // Re-running processes nothing (checkpoint already at 7).
        assert_eq!(handler.catch_up(&store).await.unwrap(), 0);
    }
}
