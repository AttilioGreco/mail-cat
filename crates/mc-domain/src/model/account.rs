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

/// SMTP security mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SmtpSecurity {
    Smtps,     // implicit TLS on port 465
    #[default]
    StartTls,  // plain → STARTTLS upgrade on port 587
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

/// Connection configuration for an email account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    // ── IMAP ─────────────────────────────────────────────────────────────────
    pub imap_host: Option<String>,
    pub imap_port: Option<u16>,
    pub use_tls: bool,
    pub auth: AuthConfig,
    // ── SMTP ─────────────────────────────────────────────────────────────────
    /// When `None` outbound sending is disabled for this account.
    #[serde(default)]
    pub smtp: Option<SmtpConnectionConfig>,
    // Legacy flat fields kept for backward compatibility with stored events.
    #[serde(default)]
    pub smtp_host: Option<String>,
    #[serde(default)]
    pub smtp_port: Option<u16>,
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
            DomainEvent::AccountRemoved { .. } => {
                self.status = AccountStatus::Disabled;
            }
            DomainEvent::AccountSyncStarted { .. }
            | DomainEvent::AccountSyncCompleted { .. }
            | DomainEvent::AccountSyncFailed { .. } => {
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
            connection_config: ConnectionConfig {
                imap_host: None,
                imap_port: None,
                use_tls: true,
                auth: AuthConfig::Plain {
                    username: String::new(),
                    password: String::new(),
                },
                smtp: None,
                smtp_host: None,
                smtp_port: None,
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
mod tests {
    use super::*;

    fn test_connection_config() -> ConnectionConfig {
        ConnectionConfig {
            imap_host: Some("imap.example.com".into()),
            imap_port: Some(993),
            use_tls: true,
            auth: AuthConfig::Plain {
                username: "user@example.com".into(),
                password: "secret".into(),
            },
            smtp: None,
            smtp_host: None,
            smtp_port: None,
        }
    }

    fn test_identity() -> Identity {
        Identity {
            display_name: "Test User".into(),
            email: EmailAddress::new("user@example.com").unwrap(),
            signature_html: None,
            signature_text: None,
        }
    }

    #[test]
    fn create_account_produces_event() {
        let (account, event) = Account::create(
            "Work".into(),
            EmailAddress::new("work@example.com").unwrap(),
            ProviderType::Imap,
            test_connection_config(),
            test_identity(),
        );

        assert_eq!(account.name, "Work");
        assert_eq!(account.status, AccountStatus::Active);
        assert_eq!(event.event_name(), "AccountAdded");
    }

    #[test]
    fn apply_update_changes_name() {
        let (mut account, _) = Account::create(
            "Old Name".into(),
            EmailAddress::new("test@example.com").unwrap(),
            ProviderType::Imap,
            test_connection_config(),
            test_identity(),
        );

        let event = account.apply_update(Some("New Name".into()), None);
        assert_eq!(account.name, "New Name");
        assert_eq!(event.event_name(), "AccountUpdated");
    }

    #[test]
    fn remove_produces_event() {
        let (account, _) = Account::create(
            "Test".into(),
            EmailAddress::new("test@example.com").unwrap(),
            ProviderType::Imap,
            test_connection_config(),
            test_identity(),
        );

        let event = account.remove();
        assert_eq!(event.event_name(), "AccountRemoved");
    }

    #[test]
    fn rehydrate_account_from_events() {
        let events = vec![
            DomainEvent::AccountAdded {
                account_id: AccountId::new(),
                name: "Original".into(),
                email: EmailAddress::new("test@example.com").unwrap(),
                provider_type: ProviderType::Imap,
            },
            DomainEvent::AccountUpdated {
                account_id: AccountId::new(),
                name: Some("Updated Name".into()),
                connection_config: None,
            },
        ];

        let account = Account::rehydrate(&events).unwrap();
        assert_eq!(account.name, "Updated Name");
        assert_eq!(account.status, AccountStatus::Active);
    }

    #[test]
    fn rehydrate_removed_account() {
        let id = AccountId::new();
        let events = vec![
            DomainEvent::AccountAdded {
                account_id: id,
                name: "Test".into(),
                email: EmailAddress::new("test@example.com").unwrap(),
                provider_type: ProviderType::Imap,
            },
            DomainEvent::AccountRemoved {
                account_id: id,
            },
        ];

        let account = Account::rehydrate(&events).unwrap();
        assert_eq!(account.status, AccountStatus::Disabled);
    }

    #[test]
    fn rehydrate_empty_events_returns_none() {
        let result = Account::rehydrate(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn account_serialization_roundtrip() {
        let (account, _) = Account::create(
            "Serialize Test".into(),
            EmailAddress::new("ser@example.com").unwrap(),
            ProviderType::GmailApi,
            test_connection_config(),
            test_identity(),
        );

        let json = serde_json::to_string(&account).unwrap();
        let deserialized: Account = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "Serialize Test");
        assert_eq!(deserialized.provider_type, ProviderType::GmailApi);
    }
}
