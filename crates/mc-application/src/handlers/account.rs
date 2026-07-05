//! Command handler for Account aggregate operations.

#[allow(unused_imports)]
use async_trait::async_trait;
use std::sync::Arc;

use mc_domain::{
    Account, AccountId, AccountWriteRepository, AggregateType, ConnectionConfig, DomainEvent,
    EmailAddress, EventEnvelope, Identity, ProviderType,
};
use tracing::info;

use crate::bus::EventPublisher;
use crate::commands::CommandResult;
use crate::error::ApplicationError;

/// Handles Account-related commands.
///
/// Generic over the write repository and event publisher so that
/// tests can inject mocks and the server wiring provides real adapters.
pub struct AccountCommandHandler<W: AccountWriteRepository, B: EventPublisher> {
    write_repo: Arc<W>,
    event_bus: Arc<B>,
}

impl<W: AccountWriteRepository, B: EventPublisher> AccountCommandHandler<W, B> {
    pub fn new(write_repo: Arc<W>, event_bus: Arc<B>) -> Self {
        Self {
            write_repo,
            event_bus,
        }
    }

    pub async fn handle_add(
        &self,
        name: String,
        email: EmailAddress,
        provider_type: ProviderType,
        connection_config: ConnectionConfig,
        identity: Identity,
    ) -> Result<CommandResult, ApplicationError> {
        let (account, event) =
            Account::create(name, email, provider_type, connection_config, identity);
        let account_id = account.id;

        self.write_repo.save(&account, vec![event.clone()]).await?;

        self.publish_event(account_id.to_string(), AggregateType::Account, event);

        info!(%account_id, "account added");
        Ok(CommandResult::AccountAdded { account_id })
    }

    pub async fn handle_update(
        &self,
        account_id: AccountId,
        name: Option<String>,
        connection_config: Option<ConnectionConfig>,
    ) -> Result<CommandResult, ApplicationError> {
        let mut account = self.load_account(&account_id).await?;
        let event = account.apply_update(name, connection_config);

        self.write_repo.save(&account, vec![event.clone()]).await?;

        self.publish_event(account_id.to_string(), AggregateType::Account, event);

        info!(%account_id, "account updated");
        Ok(CommandResult::AccountUpdated)
    }

    pub async fn handle_remove(
        &self,
        account_id: AccountId,
    ) -> Result<CommandResult, ApplicationError> {
        let account = self.load_account(&account_id).await?;
        let event = account.remove();

        self.write_repo.save(&account, vec![event.clone()]).await?;

        self.publish_event(account_id.to_string(), AggregateType::Account, event);

        info!(%account_id, "account removed");
        Ok(CommandResult::AccountRemoved)
    }

    // ── Helpers ────────────────────────────────────────────────────────

    async fn load_account(&self, id: &AccountId) -> Result<Account, ApplicationError> {
        self.write_repo
            .load(id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Account", id.to_string()))
    }

    fn publish_event(
        &self,
        aggregate_id: String,
        aggregate_type: AggregateType,
        event: DomainEvent,
    ) {
        let envelope = EventEnvelope::new(aggregate_id, aggregate_type, 0, event);
        self.event_bus.publish(envelope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::{AccountStatus, AuthConfig, DomainError};
    use std::collections::HashMap;
    use std::sync::RwLock;

    // ── Mock implementations ───────────────────────────────────────────

    struct MockAccountWriteRepo {
        accounts: RwLock<HashMap<String, (Account, Vec<DomainEvent>)>>,
    }

    impl MockAccountWriteRepo {
        fn new() -> Self {
            Self {
                accounts: RwLock::new(HashMap::new()),
            }
        }

        fn get_account(&self, id: &AccountId) -> Option<Account> {
            self.accounts
                .read()
                .unwrap()
                .get(&id.to_string())
                .map(|(a, _)| a.clone())
        }

        fn get_events(&self, id: &AccountId) -> Vec<DomainEvent> {
            self.accounts
                .read()
                .unwrap()
                .get(&id.to_string())
                .map(|(_, e)| e.clone())
                .unwrap_or_default()
        }
    }

    #[async_trait]
    impl AccountWriteRepository for MockAccountWriteRepo {
        async fn save(
            &self,
            account: &Account,
            events: Vec<DomainEvent>,
        ) -> Result<(), DomainError> {
            let mut store = self.accounts.write().unwrap();
            let entry = store
                .entry(account.id.to_string())
                .or_insert_with(|| (account.clone(), Vec::new()));
            entry.0 = account.clone();
            entry.1.extend(events);
            Ok(())
        }

        async fn load(&self, id: &AccountId) -> Result<Option<Account>, DomainError> {
            Ok(self
                .accounts
                .read()
                .unwrap()
                .get(&id.to_string())
                .map(|(a, _)| a.clone()))
        }
    }

    struct MockEventBus {
        published: RwLock<Vec<EventEnvelope>>,
    }

    impl MockEventBus {
        fn new() -> Self {
            Self {
                published: RwLock::new(Vec::new()),
            }
        }

        fn published_count(&self) -> usize {
            self.published.read().unwrap().len()
        }

        fn last_event_name(&self) -> Option<String> {
            self.published
                .read()
                .unwrap()
                .last()
                .map(|e| e.payload.event_name().to_string())
        }
    }

    impl EventPublisher for MockEventBus {
        fn publish(&self, envelope: EventEnvelope) {
            self.published.write().unwrap().push(envelope);
        }
    }

    fn test_config() -> ConnectionConfig {
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

    fn make_handler() -> (
        AccountCommandHandler<MockAccountWriteRepo, MockEventBus>,
        Arc<MockAccountWriteRepo>,
        Arc<MockEventBus>,
    ) {
        let repo = Arc::new(MockAccountWriteRepo::new());
        let bus = Arc::new(MockEventBus::new());
        let handler = AccountCommandHandler::new(repo.clone(), bus.clone());
        (handler, repo, bus)
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn handle_add_creates_account() {
        let (handler, repo, bus) = make_handler();

        let result = handler
            .handle_add(
                "Work".into(),
                EmailAddress::new("work@example.com").unwrap(),
                ProviderType::Imap,
                test_config(),
                test_identity(),
            )
            .await
            .unwrap();

        let CommandResult::AccountAdded { account_id } = result else {
            panic!("expected AccountAdded");
        };

        let saved = repo.get_account(&account_id).unwrap();
        assert_eq!(saved.name, "Work");
        assert_eq!(saved.status, AccountStatus::Active);

        assert_eq!(bus.published_count(), 1);
        assert_eq!(bus.last_event_name().unwrap(), "AccountAdded");
    }

    #[tokio::test]
    async fn handle_update_changes_name() {
        let (handler, repo, bus) = make_handler();

        let CommandResult::AccountAdded { account_id } = handler
            .handle_add(
                "Old".into(),
                EmailAddress::new("test@example.com").unwrap(),
                ProviderType::Imap,
                test_config(),
                test_identity(),
            )
            .await
            .unwrap()
        else {
            panic!("expected AccountAdded");
        };

        handler
            .handle_update(account_id, Some("New Name".into()), None)
            .await
            .unwrap();

        let updated = repo.get_account(&account_id).unwrap();
        assert_eq!(updated.name, "New Name");

        assert_eq!(bus.published_count(), 2);
        assert_eq!(bus.last_event_name().unwrap(), "AccountUpdated");
    }

    #[tokio::test]
    async fn handle_remove_marks_disabled() {
        let (handler, repo, bus) = make_handler();

        let CommandResult::AccountAdded { account_id } = handler
            .handle_add(
                "ToRemove".into(),
                EmailAddress::new("rm@example.com").unwrap(),
                ProviderType::Imap,
                test_config(),
                test_identity(),
            )
            .await
            .unwrap()
        else {
            panic!("expected AccountAdded");
        };

        handler.handle_remove(account_id).await.unwrap();

        let events = repo.get_events(&account_id);
        assert_eq!(events.len(), 2); // AccountAdded + AccountRemoved
        assert_eq!(events[1].event_name(), "AccountRemoved");
        assert_eq!(bus.published_count(), 2);
    }

    #[tokio::test]
    async fn handle_update_nonexistent_returns_not_found() {
        let (handler, _, _) = make_handler();

        let err = handler
            .handle_update(AccountId::new(), Some("X".into()), None)
            .await
            .unwrap_err();

        assert!(matches!(err, ApplicationError::NotFound(..)));
    }
}
