//! Email synchronization service — orchestrates account sync with checkpoint resume.
//!
//! The SyncService coordinates the full sync workflow:
//! 1. List mailboxes and discover new ones
//! 2. Fetch message envelopes (metadata-only)
//! 3. Lazy-load message bodies on demand
//! 4. Track progress via checkpoint for resume on restart
//! 5. Emit SyncProgressEvent stream for UI feedback

use std::sync::Arc;

#[allow(unused_imports)]
use mc_domain::{
    AccountId, MailboxId, MessageId, EmailProvider, EventStore, ReadModelProjector, MailboxPath,
    ProviderUid, Message, DomainError,
};
use tracing::{debug, error, info, instrument};

use crate::error::ApplicationError;

/// Tracks the state of a sync operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncState {
    /// Sync not yet started.
    Idle,
    /// Currently listing mailboxes.
    ListingMailboxes,
    /// Currently fetching envelopes for a mailbox.
    FetchingEnvelopes { mailbox_id: MailboxId },
    /// Fetching body for a single message.
    FetchingMessageBody { message_id: MessageId },
    /// Sync completed successfully.
    Completed,
    /// Sync failed with error.
    Failed,
}

/// Progress event emitted during sync, for streaming to client.
#[derive(Debug, Clone)]
pub struct SyncProgressEvent {
    pub account_id: AccountId,
    pub state: SyncState,
    pub processed: u32,
    pub total: u32,
    pub message: String,
}

/// Configuration for sync behavior.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Maximum number of envelopes to fetch in one batch.
    pub batch_size: usize,
    /// Maximum concurrent message body fetches.
    pub max_concurrent_bodies: usize,
    /// Whether to eagerly fetch all message bodies or lazy-load on demand.
    pub eager_body_fetch: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            batch_size: 50,
            max_concurrent_bodies: 4,
            eager_body_fetch: false,
        }
    }
}

/// Orchestrates email synchronization for an account.
/// Provides a state machine interface for sync workflow with progress tracking.
pub struct SyncService<P: EmailProvider, ES: EventStore, PR: ReadModelProjector> {
    email_provider: Arc<P>,
    #[allow(dead_code)]
    event_store: Arc<ES>,
    #[allow(dead_code)]
    projector: Arc<PR>,
    #[allow(dead_code)]
    config: SyncConfig,
}

impl<P: EmailProvider, ES: EventStore, PR: ReadModelProjector>
    SyncService<P, ES, PR>
{
    /// Creates a new SyncService with the given dependencies.
    pub fn new(
        email_provider: Arc<P>,
        event_store: Arc<ES>,
        projector: Arc<PR>,
        config: SyncConfig,
    ) -> Self {
        Self {
            email_provider,
            event_store,
            projector,
            config,
        }
    }

    /// Starts a sync for the given account.
    /// Returns a stream of SyncProgressEvent for UI feedback.
    /// This is a stub that will be completed in next iteration with actual sync logic.
    #[instrument(skip(self), fields(account_id = %account_id))]
    pub async fn start_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        info!("Starting sync for account");

        let mut events = vec![];

        // Phase 1: List mailboxes
        events.push(SyncProgressEvent {
            account_id: account_id.clone(),
            state: SyncState::ListingMailboxes,
            processed: 0,
            total: 0,
            message: "Listing mailboxes...".to_string(),
        });

        let mailboxes = self
            .email_provider
            .list_mailboxes()
            .await
            .map_err(|e| {
                error!("Failed to list mailboxes: {}", e);
                ApplicationError::SyncFailed(format!("Failed to list mailboxes: {}", e))
            })?;

        info!("Discovered {} mailboxes", mailboxes.len());

        // Phase 2: Fetch envelopes for each mailbox (stub for now)
        // In future, this will:
        // - Check UIDVALIDITY to detect if mailbox was recreated
        // - Use UIDNEXT for incremental sync
        // - Fetch new messages in batches
        // - Produce MessageReceived events for each new message
        // - Track progress via checkpoint

        events.push(SyncProgressEvent {
            account_id: account_id.clone(),
            state: SyncState::Completed,
            processed: mailboxes.len() as u32,
            total: mailboxes.len() as u32,
            message: format!("Sync completed: {} mailboxes discovered", mailboxes.len()),
        });

        Ok(events)
    }

    /// Resumes a sync operation from a checkpoint.
    /// Stub for future implementation: will load checkpoint, determine
    /// which mailboxes/messages were already synced, and continue from there.
    pub async fn resume_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError> {
        info!(account_id = %account_id, "Resuming sync from checkpoint");

        // TODO: Day 14
        // 1. Load checkpoint for account
        // 2. Determine which mailboxes/messages were already synced
        // 3. Fetch new/changed messages since last sync
        // 4. Produce events for new messages
        // 5. Update checkpoint

        Ok(vec![SyncProgressEvent {
            account_id: account_id.clone(),
            state: SyncState::Completed,
            processed: 0,
            total: 0,
            message: "Resume sync not yet implemented".to_string(),
        }])
    }

    /// Fetches the body of a single message.
    /// Stub for future: will call EmailProvider::fetch_message_detail,
    /// produce MessageBodyFetched event, and trigger search indexing.
    pub async fn fetch_message_body(
        &self,
        _account_id: &AccountId,
        _mailbox_path: &MailboxPath,
        _provider_uid: ProviderUid,
    ) -> Result<Option<Message>, ApplicationError> {
        debug!("Fetching message body");

        // TODO: Day 14
        // 1. Call EmailProvider::fetch_message_detail(mailbox_path, provider_uid)
        // 2. Produce MessageBodyFetched event
        // 3. Trigger search index update
        // 4. Return updated message

        Ok(None)
    }

    /// Cancels an ongoing sync operation.
    /// Stub: will be used to allow client to stop sync mid-stream.
    pub async fn cancel_sync(&self, _account_id: &AccountId) -> Result<(), ApplicationError> {
        // TODO: Day 14 - implement cancellation token
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::Stream;
    use mc_domain::{AggregateType, EventEnvelope, MailboxType};
    use std::pin::Pin;

    // ── Mock EmailProvider ─────────────────────────────────────────────

    struct MockEmailProvider;

    #[async_trait]
    impl EmailProvider for MockEmailProvider {
        async fn list_mailboxes(&self) -> Result<Vec<mc_domain::Mailbox>, DomainError> {
            let (mailbox, _) = mc_domain::Mailbox::discover(
                AccountId::new(),
                MailboxPath::new(vec!["INBOX".to_string()], '/').unwrap(),
                MailboxType::Inbox,
            );
            Ok(vec![mailbox])
        }

        async fn fetch_message_envelopes(
            &self,
            _mailbox: &MailboxPath,
            _range: mc_domain::FetchRange,
        ) -> Result<Vec<mc_domain::MessageEnvelopeSummary>, DomainError> {
            Ok(vec![])
        }

        async fn fetch_message_detail(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Message, DomainError> {
            Err(DomainError::Provider("Not implemented".to_string()))
        }

        async fn fetch_raw_message(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Vec<u8>, DomainError> {
            Err(DomainError::Provider("Not implemented".to_string()))
        }

        async fn set_flags(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
            _add: &[mc_domain::Flag],
            _remove: &[mc_domain::Flag],
        ) -> Result<(), DomainError> {
            Ok(())
        }

        async fn move_message(
            &self,
            _uid: ProviderUid,
            _from: &MailboxPath,
            _to: &MailboxPath,
        ) -> Result<Option<ProviderUid>, DomainError> {
            Ok(None)
        }

        async fn delete_message(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
            _permanently: bool,
        ) -> Result<(), DomainError> {
            Ok(())
        }

        async fn send_message(
            &self,
            _message: &mc_domain::OutgoingMessage,
        ) -> Result<(), DomainError> {
            Ok(())
        }

        async fn mailbox_status(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<mc_domain::MailboxStatus, DomainError> {
            Err(DomainError::Provider("Not implemented".to_string()))
        }

        fn watch_mailbox(
            &self,
            _mailbox: &MailboxPath,
        ) -> Pin<Box<dyn Stream<Item = Result<mc_domain::ProviderNotification, DomainError>> + Send + '_>>
        {
            Box::pin(futures::stream::empty())
        }
    }

    // ── Mock EventStore ───────────────────────────────────────────────

    struct MockEventStore;

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            _aggregate_id: &str,
            _aggregate_type: AggregateType,
            _expected_sequence: u64,
            _events: Vec<mc_domain::DomainEvent>,
        ) -> Result<u64, DomainError> {
            Ok(0)
        }

        async fn load_events(
            &self,
            _aggregate_id: &str,
            _from_sequence: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            Ok(vec![])
        }

        async fn load_all_events(
            &self,
            _aggregate_type: Option<AggregateType>,
            _from_global_position: u64,
        ) -> Result<Vec<EventEnvelope>, DomainError> {
            Ok(vec![])
        }

        fn subscribe(
            &self,
        ) -> Pin<Box<dyn Stream<Item = EventEnvelope> + Send + '_>> {
            Box::pin(futures::stream::empty())
        }
    }

    // ── Mock ReadModelProjector ────────────────────────────────────────

    struct MockProjector;

    #[async_trait]
    impl ReadModelProjector for MockProjector {
        async fn project(&self, _envelope: &EventEnvelope) -> Result<(), DomainError> {
            Ok(())
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sync_emits_progress_events() {
        let provider = Arc::new(MockEmailProvider);

        let service = SyncService::new(
            provider,
            Arc::new(MockEventStore),
            Arc::new(MockProjector),
            SyncConfig::default(),
        );

        let account_id = AccountId::new();
        let events = service.start_sync(&account_id).await.unwrap();

        assert!(!events.is_empty());
        assert_eq!(events.last().unwrap().state, SyncState::Completed);
        assert_eq!(events.last().unwrap().total, 1);
    }

    #[tokio::test]
    async fn sync_returns_progress() {
        let provider = Arc::new(MockEmailProvider);

        let service = SyncService::new(
            provider,
            Arc::new(MockEventStore),
            Arc::new(MockProjector),
            SyncConfig::default(),
        );

        let account_id = AccountId::new();
        let events = service.start_sync(&account_id).await.unwrap();

        assert!(events.iter().any(|e| e.state == SyncState::ListingMailboxes));
        assert!(events.iter().any(|e| e.state == SyncState::Completed));
    }
}
