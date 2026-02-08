use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::identity::Identity;
use crate::events::DomainEvent;
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
            account_id: id.clone(),
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
            account_id: self.id.clone(),
            name,
            connection_config,
        }
    }

    /// Mark the account as removed and produce the event.
    pub fn remove(&self) -> DomainEvent {
        DomainEvent::AccountRemoved {
            account_id: self.id.clone(),
        }
    }

    /// Mark sync as started.
    pub fn start_sync(&self) -> DomainEvent {
        DomainEvent::AccountSyncStarted {
            account_id: self.id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_connection_config() -> ConnectionConfig {
        ConnectionConfig {
            imap_host: Some("imap.example.com".into()),
            imap_port: Some(993),
            smtp_host: Some("smtp.example.com".into()),
            smtp_port: Some(587),
            use_tls: true,
            auth: AuthConfig::Plain {
                username: "user@example.com".into(),
                password: "secret".into(),
            },
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
