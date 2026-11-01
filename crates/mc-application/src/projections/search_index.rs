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

use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;
use tracing::{debug, info};

use mc_domain::{
    AccountId, AccountReadRepository, DomainEvent, EventEnvelope, EventStore,
    MailboxReadRepository, MessageReadRepository, ProjectionCheckpoint, SearchIndex,
};

use crate::error::ApplicationError;
use rebuild::LabelingCache;

mod rebuild;

/// Name used for the search-index projection checkpoint.
const PROJECTION_NAME: &str = "search_index";

/// Flush the index and save the checkpoint at least this often during catch-up,
/// bounding how many events replay after a crash.
const CHECKPOINT_EVERY: u64 = 1000;

pub struct SearchProjectionHandler<SI, R, C> {
    search_index: SI,
    read_repo: R,
    checkpoint: C,
    /// Set while [`Self::rebuild`] runs.
    rebuilding: AtomicBool,
}

impl<SI, R, C> SearchProjectionHandler<SI, R, C>
where
    SI: SearchIndex,
    R: MessageReadRepository + MailboxReadRepository + AccountReadRepository,
    C: ProjectionCheckpoint,
{
    pub fn new(search_index: SI, read_repo: R, checkpoint: C) -> Self {
        Self {
            search_index,
            read_repo,
            checkpoint,
            rebuilding: AtomicBool::new(false),
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
            | DomainEvent::MessageMailboxesChanged { message_id, .. }
            | DomainEvent::MessageFlagged { message_id, .. } => message_id,
            // Labels come from mailbox names (a parent's rename is one event
            // per descendant), and a role change moves a mailbox in or out
            // of the labels.
            DomainEvent::MailboxRenamed { mailbox_id, .. }
            | DomainEvent::MailboxTypeChanged { mailbox_id, .. } => {
                return self.reindex_mailbox(mailbox_id).await;
            }
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
            Some(msg) => self.index(&msg, &mut LabelingCache::new()).await,
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

    /// Drops the account's documents and commits.
    pub async fn remove_account(&self, account_id: &AccountId) -> Result<(), ApplicationError> {
        self.search_index.remove_account(account_id).await?;
        Ok(())
    }

    /// Forget the checkpoint so the next `catch_up` re-indexes the whole log —
    /// for an index that was just created empty.
    pub async fn reset(&self) -> Result<(), ApplicationError> {
        self.checkpoint
            .save_checkpoint(PROJECTION_NAME, 0)
            .await
            .map_err(ApplicationError::Domain)
    }

    /// Replay events after the checkpoint from the event store. On a fresh index
    /// (checkpoint 0) this indexes everything once; thereafter it's incremental.
    /// Commits and advances the checkpoint in batches so a crash replays at most
    /// `CHECKPOINT_EVERY` events (safe: indexing is idempotent).
    pub async fn catch_up<ES: EventStore>(&self, store: &ES) -> Result<u64, ApplicationError> {
        if self.rebuilding.load(Ordering::Relaxed) {
            return Ok(0);
        }
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
    use mc_domain::model::account::MailboxModel;
    use mc_domain::{
        AccountId, AccountStatus, AccountSummary, AccountSyncStats, AggregateType, DomainError,
        EmailAddress, Envelope, Flag, IndexExtras, Mailbox, MailboxId, MailboxPath, MailboxType,
        Message, MessageId, MessageSummary, ProviderType,
    };
    use mc_shared::pagination::{PagedResult, Pagination};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    // In-memory search index recording live message ids and their labels.
    #[derive(Clone, Default)]
    struct FakeIndex {
        docs: Arc<Mutex<HashMap<String, IndexExtras>>>,
    }
    #[async_trait]
    impl SearchIndex for FakeIndex {
        async fn index_message(&self, m: &Message, e: &IndexExtras) -> Result<(), DomainError> {
            self.docs
                .lock()
                .unwrap()
                .insert(m.id.to_string(), e.clone());
            Ok(())
        }
        async fn remove_message(&self, id: &MessageId) -> Result<(), DomainError> {
            self.docs.lock().unwrap().remove(&id.to_string());
            Ok(())
        }
        async fn remove_account(&self, _id: &AccountId) -> Result<(), DomainError> {
            Ok(())
        }
        async fn search(
            &self,
            _q: &str,
            _a: Option<&AccountId>,
            _mb: Option<&MailboxId>,
            _d: &mc_domain::SearchDirectory,
            _p: &Pagination,
        ) -> Result<mc_domain::IndexResults, DomainError> {
            unimplemented!()
        }
        fn snippet(&self, _q: &str, _m: &Message) -> Option<String> {
            None
        }
        async fn flush(&self) -> Result<(), DomainError> {
            Ok(())
        }
    }

    // Read repo returning whatever messages are put in the map.
    #[derive(Clone, Default)]
    struct FakeRepo {
        messages: Arc<Mutex<HashMap<String, Message>>>,
        mailboxes: Arc<Mutex<Vec<Mailbox>>>,
        /// The only account's model: Gmail-style labels, or folders.
        labels_model: bool,
    }
    #[async_trait]
    impl MailboxReadRepository for FakeRepo {
        async fn get_mailbox(&self, _id: &MailboxId) -> Result<Option<Mailbox>, DomainError> {
            Ok(None)
        }
        async fn list_mailboxes(&self, _a: &AccountId) -> Result<Vec<Mailbox>, DomainError> {
            Ok(self.mailboxes.lock().unwrap().clone())
        }
    }
    #[async_trait]
    impl AccountReadRepository for FakeRepo {
        async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, DomainError> {
            Ok(Some(AccountSummary {
                id: *id,
                name: "a".into(),
                email: EmailAddress::new("a@b.it").unwrap(),
                provider_type: ProviderType::Imap,
                mailbox_model: if self.labels_model {
                    MailboxModel::Labels
                } else {
                    MailboxModel::Folders
                },
                supports_tags: None,
                status: AccountStatus::Active,
                total_unread: 0,
                status_error: None,
                status_since: None,
            }))
        }
        async fn list_accounts(&self) -> Result<Vec<AccountSummary>, DomainError> {
            unimplemented!()
        }
        async fn account_stats(&self, _id: &AccountId) -> Result<AccountSyncStats, DomainError> {
            unimplemented!()
        }
    }
    #[async_trait]
    impl MessageReadRepository for FakeRepo {
        async fn get_message(&self, id: &MessageId) -> Result<Option<Message>, DomainError> {
            Ok(self.messages.lock().unwrap().get(&id.to_string()).cloned())
        }
        async fn list_messages(
            &self,
            m: &MailboxId,
            _p: &Pagination,
        ) -> Result<PagedResult<MessageSummary>, DomainError> {
            let items: Vec<_> = self
                .messages
                .lock()
                .unwrap()
                .values()
                .filter(|msg| msg.memberships.iter().any(|x| x.mailbox_id == *m))
                .map(|msg| MessageSummary {
                    id: msg.id,
                    account_id: msg.account_id,
                    memberships: msg.memberships.clone(),
                    from_name: None,
                    from_email: String::new(),
                    to: vec![],
                    cc: vec![],
                    subject: String::new(),
                    flags: vec![],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: msg.received_at,
                })
                .collect();
            Ok(PagedResult {
                total_count: items.len() as u64,
                items,
                page: 1,
                page_size: 500,
            })
        }
        async fn get_thread(&self, _id: &MessageId) -> Result<Vec<Message>, DomainError> {
            unimplemented!()
        }
        async fn find_by_remote_id(
            &self,
            _m: &MailboxId,
            _u: &mc_domain::RemoteId,
        ) -> Result<Option<MessageSummary>, DomainError> {
            Ok(None)
        }
        async fn held_ids(
            &self,
            _mailbox_id: &MailboxId,
            _uids: &[mc_domain::RemoteId],
        ) -> Result<std::collections::HashSet<mc_domain::RemoteId>, DomainError> {
            Ok(Default::default())
        }
        async fn find_by_keys(
            &self,
            _account_id: &AccountId,
            _keys: &[mc_domain::MessageKey],
        ) -> Result<std::collections::HashMap<mc_domain::MessageKey, MessageId>, DomainError>
        {
            Ok(Default::default())
        }
        async fn pending_messages(
            &self,
            _mailbox_id: &MailboxId,
        ) -> Result<Vec<MessageId>, DomainError> {
            Ok(vec![])
        }
        async fn find_by_rfc_id(
            &self,
            _account_id: &AccountId,
            _rfc_id: &str,
        ) -> Result<Vec<MessageSummary>, DomainError> {
            Ok(vec![])
        }

        async fn find_unread_by_mailbox(
            &self,
            _m: &MailboxId,
        ) -> Result<Vec<MessageSummary>, DomainError> {
            Ok(vec![])
        }
        async fn scan_all_messages(
            &self,
            _a: Option<&AccountId>,
        ) -> Result<Vec<Message>, DomainError> {
            Ok(vec![])
        }

        async fn message_ids_after(
            &self,
            after: Option<&MessageId>,
            limit: usize,
        ) -> Result<Vec<MessageId>, DomainError> {
            let mut ids: Vec<_> = self
                .messages
                .lock()
                .unwrap()
                .values()
                .map(|m| m.id)
                .collect();
            ids.sort_by_key(|id| id.to_string());
            ids.retain(|id| after.is_none_or(|a| id.to_string() > a.to_string()));
            ids.truncate(limit);
            Ok(ids)
        }

        async fn suggest(
            &self,
            _prefix: &str,
            _scope: Option<mc_domain::SuggestScope>,
            _max_per_field: usize,
        ) -> Result<Vec<mc_domain::Suggestion>, DomainError> {
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
        async fn load_events(&self, _a: &str, _s: u64) -> Result<Vec<EventEnvelope>, DomainError> {
            unimplemented!()
        }
        fn load_all_events(
            &self,
            _t: Option<AggregateType>,
            from: u64,
        ) -> std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<EventEnvelope, DomainError>> + Send + '_>,
        > {
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
            memberships: vec![mc_domain::Membership {
                mailbox_id: MailboxId::new(),
                remote_id: None,
            }],
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
            body_truncated: false,
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
        repo.messages
            .lock()
            .unwrap()
            .insert(ID1.to_string(), msg(ID1));

        let handler =
            SearchProjectionHandler::new(index.clone(), repo.clone(), FakeCheckpoint::default());

        let store = FakeStore {
            events: vec![env(
                1,
                DomainEvent::MessageReceived {
                    message_id: mid,
                    account_id: AccountId::new(),
                    mailbox_id: MailboxId::new(),
                    remote_id: 1.into(),
                    message_rfc_id: None,
                    in_reply_to: None,
                    references: vec![],
                    envelope: envelope(),
                    flags: vec![],
                    size_bytes: 0,
                    has_attachments: false,
                    received_at: chrono::Utc::now(),
                    restored: false,
                    provider_key: None,
                    list_headers: Default::default(),
                },
            )],
        };
        handler.catch_up(&store).await.unwrap();
        assert!(index.docs.lock().unwrap().contains_key(ID1));

        // Now delete it and replay from scratch — index converges to empty.
        repo.messages.lock().unwrap().clear();
        let store2 = FakeStore {
            events: vec![env(
                2,
                DomainEvent::MessageDeleted {
                    message_id: mid,
                    mailbox_ids: vec![MailboxId::new()],
                },
            )],
        };
        handler.catch_up(&store2).await.unwrap();
        assert!(!index.docs.lock().unwrap().contains_key(ID1));
    }

    #[tokio::test]
    async fn catch_up_advances_checkpoint_and_is_idempotent() {
        let index = FakeIndex::default();
        let repo = FakeRepo::default();
        repo.messages
            .lock()
            .unwrap()
            .insert(ID1.to_string(), msg(ID1));
        let checkpoint = FakeCheckpoint::default();
        let mid = MessageId::from_uuid(uuid::Uuid::parse_str(ID1).unwrap());

        let handler = SearchProjectionHandler::new(index.clone(), repo.clone(), checkpoint.clone());
        let store = FakeStore {
            events: vec![env(
                7,
                DomainEvent::MessageMailboxesChanged {
                    message_id: mid,
                    added: vec![],
                    removed: vec![MailboxId::new()],
                },
            )],
        };

        assert_eq!(handler.catch_up(&store).await.unwrap(), 1);
        assert_eq!(*checkpoint.pos.lock().unwrap(), 7);
        // Re-running processes nothing (checkpoint already at 7).
        assert_eq!(handler.catch_up(&store).await.unwrap(), 0);
    }

    const ID2: &str = "11234567-89ab-cdef-0123-456789abcdef";

    fn label(path: &str) -> Mailbox {
        let path = MailboxPath::parse(path, '/').unwrap();
        Mailbox::discover(AccountId::new(), path, MailboxType::Custom).0
    }

    fn msg_in(id: &str, mailbox: &Mailbox, flags: Vec<Flag>) -> Message {
        let mut m = msg(id);
        m.memberships[0].mailbox_id = mailbox.id;
        m.flags = flags;
        m
    }

    fn handler(
        labels_model: bool,
        mailboxes: Vec<Mailbox>,
        messages: Vec<Message>,
    ) -> (
        SearchProjectionHandler<FakeIndex, FakeRepo, FakeCheckpoint>,
        FakeIndex,
        FakeRepo,
        FakeCheckpoint,
    ) {
        let (index, checkpoint) = (FakeIndex::default(), FakeCheckpoint::default());
        let repo = FakeRepo {
            labels_model,
            ..Default::default()
        };
        *repo.mailboxes.lock().unwrap() = mailboxes;
        for m in messages {
            repo.messages.lock().unwrap().insert(m.id.to_string(), m);
        }
        let handler = SearchProjectionHandler::new(index.clone(), repo.clone(), checkpoint.clone());
        (handler, index, repo, checkpoint)
    }

    fn labels_of(index: &FakeIndex, id: &str) -> Vec<String> {
        index.docs.lock().unwrap()[id].labels.clone()
    }

    #[tokio::test]
    async fn a_labels_account_labels_with_custom_mailboxes_and_a_folders_account_with_keywords() {
        let dev = label("Dev/GitHub");
        let keyworded = |id| {
            msg_in(
                id,
                &dev,
                vec![
                    Flag::Seen,
                    Flag::Custom("Work".into()),
                    Flag::Custom("Needs_reply".into()),
                    Flag::Custom("$Forwarded".into()),
                ],
            )
        };

        let (h, index, ..) = handler(true, vec![dev.clone()], vec![keyworded(ID1)]);
        h.apply_live(&env(
            1,
            DomainEvent::MessageFlagged {
                message_id: mid(ID1),
                flags_added: vec![],
                flags_removed: vec![],
            },
        ))
        .await
        .unwrap();
        assert_eq!(labels_of(&index, ID1), ["Dev/GitHub"]);

        let (h, index, ..) = handler(false, vec![dev.clone()], vec![keyworded(ID1)]);
        h.apply_live(&env(
            1,
            DomainEvent::MessageFlagged {
                message_id: mid(ID1),
                flags_added: vec![],
                flags_removed: vec![],
            },
        ))
        .await
        .unwrap();
        assert_eq!(labels_of(&index, ID1), ["Work", "Needs reply"]);
    }

    fn mid(id: &str) -> MessageId {
        MessageId::from_uuid(uuid::Uuid::parse_str(id).unwrap())
    }

    #[tokio::test]
    async fn renaming_a_label_reindexes_its_messages_under_the_new_name() {
        let mut dev = label("Dev");
        let other = label("Other");
        let (h, index, repo, _) = handler(
            true,
            vec![dev.clone(), other.clone()],
            vec![msg_in(ID1, &dev, vec![]), msg_in(ID2, &other, vec![])],
        );
        dev.display_path = "Code".into();
        *repo.mailboxes.lock().unwrap() = vec![dev.clone(), other];
        let renamed = DomainEvent::MailboxRenamed {
            mailbox_id: dev.id,
            old_path: dev.path.clone(),
            new_path: dev.path.clone(),
            display_path: "Code".into(),
            display_name: "Code".into(),
        };
        h.apply_live(&env(1, renamed)).await.unwrap();
        assert_eq!(labels_of(&index, ID1), ["Code"]);
        assert!(!index.docs.lock().unwrap().contains_key(ID2));
    }

    #[tokio::test]
    async fn rebuild_indexes_the_read_model_and_moves_the_checkpoint_to_the_log_head() {
        let dev = label("Dev");
        let (h, index, _, checkpoint) = handler(
            true,
            vec![dev.clone()],
            vec![msg_in(ID1, &dev, vec![]), msg_in(ID2, &dev, vec![])],
        );
        let store = FakeStore {
            events: vec![
                env(
                    3,
                    DomainEvent::MessageDeleted {
                        message_id: mid(ID1),
                        mailbox_ids: vec![],
                    },
                ),
                env(
                    9,
                    DomainEvent::MessageDeleted {
                        message_id: mid(ID2),
                        mailbox_ids: vec![],
                    },
                ),
            ],
        };
        assert_eq!(h.rebuild(&store).await.unwrap(), 2);
        assert_eq!(index.docs.lock().unwrap().len(), 2);
        assert_eq!(labels_of(&index, ID2), ["Dev"]);
        assert_eq!(*checkpoint.pos.lock().unwrap(), 9);
        // The deletes in the log are not replayed on top of the rebuilt index.
        assert_eq!(h.catch_up(&store).await.unwrap(), 0);
        assert_eq!(index.docs.lock().unwrap().len(), 2);
    }
}
