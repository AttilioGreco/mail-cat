use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::identity::Identity;
use crate::events::DomainEvent;
use crate::value_objects::email_address::EmailAddress;

/// Unique account identifier (UUID v7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    Jmap,
}

impl std::fmt::Display for ProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Imap => write!(f, "IMAP"),
            Self::Jmap => write!(f, "JMAP"),
        }
    }
}

/// How an account's mailboxes are presented and which operations the UI
/// offers. Never changes stored data: `message_mailboxes` is M:N either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailboxModel {
    /// A message sits in one folder; moving replaces it.
    Folders,
    /// A message carries one or more mailboxes as labels.
    Labels,
}

impl MailboxModel {
    /// Gmail over IMAP is `Folders` here too: the server decides, and the
    /// provider reports `Labels` once it sees X-GM-EXT-1 (see
    /// `EmailProvider::preferred_mailbox_model`).
    pub fn default_for(provider: ProviderType) -> Self {
        match provider {
            ProviderType::Imap => Self::Folders,
            ProviderType::Jmap => Self::Labels,
        }
    }
}

/// Account status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountStatus {
    Active,
    Disabled,
    Error,
    NeedsReauth,
    /// The server rejected the account's password: the user has to change it.
    CredentialsRejected,
}

/// SMTP security mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SmtpSecurity {
    Smtps, // implicit TLS on port 465
    #[default]
    StartTls, // plain → STARTTLS upgrade on port 587
}

/// SMTP-specific connection configuration stored on the account.
///
/// `username` and `password` are optional; when absent the account's primary
/// `AuthConfig` credentials are used as fallback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmtpConnectionConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub security: SmtpSecurity,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Which OAuth2 provider issued the tokens on an `AuthConfig::OAuth2` account.
/// Needed to pick the right token-refresh endpoint later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthProvider {
    Google,
    Microsoft,
}

/// Authentication configuration.
///
/// Secret fields (`password`, tokens, `app_password`) are `#[serde(skip)]`: they
/// are NEVER written to the event store. Secrets live only in the OS keychain
/// (see [`crate::SecretStore`]) and are resolved by account email at runtime.
/// On load these fields deserialize to their default (empty), which is expected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthConfig {
    Plain {
        username: String,
        #[serde(skip)]
        password: String,
    },
    OAuth2 {
        provider: OAuthProvider,
        #[serde(skip)]
        access_token: String,
        #[serde(skip)]
        refresh_token: String,
        expires_at: DateTime<Utc>,
    },
    AppPassword {
        username: String,
        #[serde(skip)]
        app_password: String,
    },
}

/// Where an account's mail is fetched from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncomingServer {
    Imap {
        host: String,
        port: u16,
        use_tls: bool,
    },
    Jmap {
        session_url: String,
    },
}

/// Connection configuration for an email account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub incoming: IncomingServer,
    pub auth: AuthConfig,
    /// When `None` outbound sending is disabled for this account.
    pub smtp: Option<SmtpConnectionConfig>,
}

/// Account aggregate root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: AccountId,
    pub name: String,
    pub email: EmailAddress,
    pub provider_type: ProviderType,
    pub mailbox_model: MailboxModel,
    pub connection_config: ConnectionConfig,
    pub identity: Identity,
    pub status: AccountStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Account {
    /// Create a new account and produce the corresponding domain event.
    pub fn create(
        name: String,
        email: EmailAddress,
        provider_type: ProviderType,
        connection_config: ConnectionConfig,
        identity: Identity,
    ) -> (Self, DomainEvent) {
        let now = Utc::now();
        let id = AccountId::new();

        let event = DomainEvent::AccountAdded {
            account_id: id,
            name: name.clone(),
            email: email.clone(),
            provider_type,
        };

        let account = Self {
            id,
            name,
            email,
            provider_type,
            mailbox_model: MailboxModel::default_for(provider_type),
            connection_config,
            identity,
            status: AccountStatus::Active,
            created_at: now,
            updated_at: now,
        };

        (account, event)
    }

    /// Apply an AccountUpdated event to update the account's state.
    pub fn apply_update(
        &mut self,
        name: Option<String>,
        connection_config: Option<ConnectionConfig>,
    ) -> DomainEvent {
        if let Some(ref new_name) = name {
            self.name = new_name.clone();
        }
        if let Some(ref new_config) = connection_config {
            self.connection_config = new_config.clone();
        }
        self.updated_at = Utc::now();

        DomainEvent::AccountUpdated {
            account_id: self.id,
            name,
            connection_config,
        }
    }

    /// Always an event, even when `model` is the current one: the log must
    /// record the choice so a provider's default never overrides it later.
    pub fn set_mailbox_model(&mut self, model: MailboxModel) -> DomainEvent {
        self.mailbox_model = model;
        self.updated_at = Utc::now();
        DomainEvent::AccountMailboxModelChanged {
            account_id: self.id,
            model,
        }
    }

    /// Mark the account as removed and produce the event.
    pub fn remove(&self) -> DomainEvent {
        DomainEvent::AccountRemoved {
            account_id: self.id,
        }
    }

    /// Mark sync as started.
    pub fn start_sync(&self) -> DomainEvent {
        DomainEvent::AccountSyncStarted {
            account_id: self.id,
        }
    }

    // ── Event Sourcing rehydration ─────────────────────────────────────

    /// Apply a single domain event to mutate aggregate state.
    ///
    /// Used during rehydration from the event store. This method is
    /// infallible: events are facts that already happened.
    pub fn apply(&mut self, event: &DomainEvent) {
        match event {
            DomainEvent::AccountAdded { .. } => {
                // Initial state is set by the constructor; nothing to do.
            }
            DomainEvent::AccountUpdated {
                name,
                connection_config,
                ..
            } => {
                if let Some(new_name) = name {
                    self.name = new_name.clone();
                }
                if let Some(new_config) = connection_config {
                    self.connection_config = new_config.clone();
                }
                self.updated_at = Utc::now();
            }
            DomainEvent::AccountMailboxModelChanged { model, .. } => {
                self.mailbox_model = *model;
            }
            DomainEvent::AccountRemoved { .. } => {
                self.status = AccountStatus::Disabled;
            }
            DomainEvent::AccountAuthExpired { .. } => {
                self.status = AccountStatus::NeedsReauth;
            }
            DomainEvent::AccountCredentialsRejected { .. } => {
                self.status = AccountStatus::CredentialsRejected;
            }
            DomainEvent::AccountReauthenticated { .. } => {
                self.status = AccountStatus::Active;
            }
            DomainEvent::AccountSyncStarted { .. }
            | DomainEvent::AccountSyncCompleted { .. }
            | DomainEvent::AccountSyncFailed { .. }
            | DomainEvent::AccountTagsDetected { .. } => {
                // Sync events don't mutate aggregate state.
                // They're tracked for audit/projection purposes.
            }
            _ => {} // Ignore events belonging to other aggregates.
        }
    }

    /// Rehydrate an Account from a sequence of domain events.
    ///
    /// The first event must be `AccountAdded`. Returns `None` if the
    /// events slice is empty or doesn't start with a creation event.
    pub fn rehydrate(events: &[DomainEvent]) -> Option<Self> {
        let first = events.first()?;
        let DomainEvent::AccountAdded {
            account_id,
            name,
            email,
            provider_type,
        } = first
        else {
            return None;
        };

        let now = Utc::now();
        let mut account = Self {
            id: *account_id,
            name: name.clone(),
            email: email.clone(),
            provider_type: *provider_type,
            mailbox_model: MailboxModel::default_for(*provider_type),
            connection_config: ConnectionConfig {
                incoming: IncomingServer::Imap {
                    host: String::new(),
                    port: 0,
                    use_tls: true,
                },
                auth: AuthConfig::Plain {
                    username: String::new(),
                    password: String::new(),
                },
                smtp: None,
            },
            identity: Identity {
                display_name: name.clone(),
                email: email.clone(),
                signature_html: None,
                signature_text: None,
            },
            status: AccountStatus::Active,
            created_at: now,
            updated_at: now,
        };

        for event in &events[1..] {
            account.apply(event);
        }

        Some(account)
    }
}

#[cfg(test)]
mod tests;
