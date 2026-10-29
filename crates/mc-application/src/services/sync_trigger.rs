//! Thin trait for triggering account sync from the interface layer.
//!
//! `SyncTrigger` is object-safe (`Arc<dyn SyncTrigger>`) so that gRPC services
//! don't need to thread `SyncService`'s three generic parameters.

use async_trait::async_trait;

use mc_domain::AccountId;

use crate::error::ApplicationError;
use crate::services::sync_service::SyncProgressEvent;

/// Trigger an email sync for a given account.
#[async_trait]
pub trait SyncTrigger: Send + Sync {
    async fn trigger_sync(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<SyncProgressEvent>, ApplicationError>;

    /// Sync one mailbox, identified by its stored path, from its cursor.
    async fn trigger_mailbox_sync(
        &self,
        account_id: &AccountId,
        mailbox_path: &str,
    ) -> Result<u32, ApplicationError>;

    /// Live progress for every account, including syncs this client didn't start
    /// (boot sync, the periodic tick, the body backfill worker).
    fn subscribe_progress(&self) -> tokio::sync::broadcast::Receiver<SyncProgressEvent>;
}
