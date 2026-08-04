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

use crate::error::ApplicationError;

/// Everything needed to declare a password-authenticated IMAP account.
#[derive(Debug, Clone)]
pub struct ImapProvisionParams {
    pub name: String,
    pub email: String,
    pub imap_host: String,
    pub imap_port: u16,
    pub use_tls: bool,
    pub username: String,
    pub password: String,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<u16>,
}

#[async_trait]
pub trait AccountProvisioner: Send + Sync {
    /// Record an IMAP account in the server's configuration, storing its
    /// password in the keychain rather than on disk.
    async fn provision_imap(&self, params: ImapProvisionParams) -> Result<(), ApplicationError>;

    /// Drop an account from the server's configuration and forget its secrets.
    /// Without this a removed account comes back on the next restart.
    async fn deprovision(&self, email: &str) -> Result<(), ApplicationError>;
}
