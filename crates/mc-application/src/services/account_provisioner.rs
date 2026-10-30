//! Port: persist account configuration on the server host.
//!
//! Accounts are declared in the server's own `config.toml` — that file is what
//! boot reads to build providers, and secrets live in the OS keychain next to
//! it. So adding an account is not just an event-store write: the server has to
//! record it where the next boot will find it. `AccountProvisioner` is the seam
//! that lets the gRPC layer ask for that without knowing about files.
//!
//! Object-safe (`Arc<dyn AccountProvisioner>`) for the same reason as
//! [`SyncTrigger`](crate::services::sync_trigger::SyncTrigger).

use async_trait::async_trait;
use mc_domain::model::account::SmtpConnectionConfig;
use mc_domain::{AccountId, IncomingServer};

use crate::error::ApplicationError;

/// Everything needed to declare a password-authenticated account.
#[derive(Debug, Clone)]
pub struct ProvisionParams {
    pub name: String,
    pub email: String,
    pub incoming: IncomingServer,
    pub username: String,
    pub password: String,
    pub smtp: Option<SmtpConnectionConfig>,
}

#[async_trait]
pub trait AccountProvisioner: Send + Sync {
    /// Record a password account in the server's configuration, storing its
    /// password in the keychain rather than on disk.
    async fn provision(&self, params: ProvisionParams) -> Result<(), ApplicationError>;

    /// Drop an account from the server's configuration and forget its secrets.
    /// Without this a removed account comes back on the next restart.
    async fn deprovision(&self, email: &str) -> Result<(), ApplicationError>;

    /// Replace the keychain password of an already-declared account.
    async fn set_password(&self, email: &str, password: &str) -> Result<(), ApplicationError>;
}

/// Port: the live side of an account whose declaration is in the server's
/// configuration (gate, provider, sync, watchers), and its end. Without it an
/// account added while the server runs would only come alive at the next boot,
/// and a removed one would keep running and keep its data.
#[async_trait]
pub trait AccountLifecycle: Send + Sync {
    /// Idempotent: activating a running account is a no-op.
    async fn activate(&self, email: &str) -> Result<(), ApplicationError>;

    /// Stops the account, then deletes everything the server keeps of it:
    /// events, read-model rows, search documents, config entry and secrets.
    /// The mailbox on the provider is untouched. Finishes at the next boot if
    /// the server dies half-way.
    async fn remove(&self, account_id: AccountId) -> Result<(), ApplicationError>;
}
