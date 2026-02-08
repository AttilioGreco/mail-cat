use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::identity::Identity;
use crate::value_objects::email_address::EmailAddress;

/// Unique account identifier (UUID v7).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountId(Uuid);

impl AccountId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for AccountId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for AccountId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

/// Email provider type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderType {
    Imap,
    GmailApi,
    OutlookApi,
}

impl std::fmt::Display for ProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Imap => write!(f, "IMAP"),
            Self::GmailApi => write!(f, "Gmail API"),
            Self::OutlookApi => write!(f, "Outlook API"),
        }
    }
}

/// Account status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountStatus {
    Active,
    Disabled,
    Error,
}

/// Authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthConfig {
    Plain {
        username: String,
        password: String,
    },
    OAuth2 {
        access_token: String,
        refresh_token: String,
        expires_at: DateTime<Utc>,
    },
    AppPassword {
        username: String,
        app_password: String,
    },
}

/// Connection configuration for an email account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub imap_host: Option<String>,
    pub imap_port: Option<u16>,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<u16>,
    pub use_tls: bool,
    pub auth: AuthConfig,
}

/// Account aggregate root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: AccountId,
    pub name: String,
    pub email: EmailAddress,
    pub provider_type: ProviderType,
    pub connection_config: ConnectionConfig,
    pub identity: Identity,
    pub status: AccountStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
