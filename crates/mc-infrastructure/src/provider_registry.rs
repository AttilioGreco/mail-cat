use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use mc_domain::{AccountId, DomainError, EmailProvider, ProviderRegistry};

/// In-memory provider registry backed by a `RwLock<HashMap>`.
pub struct InMemoryProviderRegistry<P: EmailProvider> {
    providers: RwLock<HashMap<AccountId, Arc<P>>>,
}

impl<P: EmailProvider> InMemoryProviderRegistry<P> {
    pub fn new() -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
        }
    }
}

impl<P: EmailProvider> Default for InMemoryProviderRegistry<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: EmailProvider + 'static> ProviderRegistry for InMemoryProviderRegistry<P> {
    type Provider = P;

    fn get_provider(&self, account_id: &AccountId) -> Result<Arc<P>, DomainError> {
        self.providers
            .read()
            .unwrap()
            .get(account_id)
            .cloned()
            .ok_or_else(|| {
                DomainError::Provider(format!("no provider registered for account {account_id}"))
            })
    }

    fn register_provider(&self, account_id: AccountId, provider: Arc<P>) {
        self.providers.write().unwrap().insert(account_id, provider);
    }

    fn remove_provider(&self, account_id: &AccountId) {
        self.providers.write().unwrap().remove(account_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use mc_domain::{
        DomainError, FetchRange, Flag, Mailbox, MailboxPath, MailboxStatus, Message,
        MessageEnvelopeSummary, OutgoingMessage, ProviderNotification, ProviderUid,
    };
    use std::pin::Pin;

    #[derive(Debug)]
    struct StubProvider;

    #[async_trait]
    impl EmailProvider for StubProvider {
        async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_message_envelopes(
            &self,
            _mailbox: &MailboxPath,
            _range: FetchRange,
        ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_message_detail(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Message, DomainError> {
            Err(DomainError::Provider("stub".into()))
        }
        async fn fetch_raw_message(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
        ) -> Result<Vec<u8>, DomainError> {
            Ok(vec![])
        }
        async fn set_flags(
            &self,
            _mailbox: &MailboxPath,
            _uid: ProviderUid,
            _add: &[Flag],
            _remove: &[Flag],
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn set_flags_bulk(
            &self,
            _mailbox: &MailboxPath,
            _uids: &[ProviderUid],
            _add: &[Flag],
            _remove: &[Flag],
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
        async fn send_message(&self, _message: &OutgoingMessage) -> Result<(), DomainError> {
            Ok(())
        }
        async fn mailbox_status(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<MailboxStatus, DomainError> {
            Ok(MailboxStatus {
                uid_validity: 1,
                uid_next: 1,
                total_messages: 0,
                unseen_messages: 0,
            })
        }
        async fn fetch_uid_flags(
            &self,
            _mailbox: &MailboxPath,
        ) -> Result<Vec<mc_domain::traits::email_provider::UidFlagEntry>, DomainError> {
            Ok(vec![])
        }
        fn watch_mailbox(
            &self,
            _mailbox: &MailboxPath,
        ) -> Pin<
            Box<dyn futures::Stream<Item = Result<ProviderNotification, DomainError>> + Send + '_>,
        > {
            Box::pin(futures::stream::empty())
        }
        async fn fetch_bodies_bulk(
            &self,
            _mailbox: &MailboxPath,
            _uids: &[ProviderUid],
        ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError> {
            Ok(vec![])
        }
    }

    #[test]
    fn register_and_get_provider() {
        let registry = InMemoryProviderRegistry::new();
        let account_id = AccountId::new();
        let provider = Arc::new(StubProvider);

        registry.register_provider(account_id, provider);
        let result = registry.get_provider(&account_id);
        assert!(result.is_ok());
    }

    #[test]
    fn get_missing_provider_returns_error() {
        let registry = InMemoryProviderRegistry::<StubProvider>::new();
        let account_id = AccountId::new();

        let result = registry.get_provider(&account_id);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no provider registered")
        );
    }

    #[test]
    fn remove_provider() {
        let registry = InMemoryProviderRegistry::new();
        let account_id = AccountId::new();
        let provider = Arc::new(StubProvider);

        registry.register_provider(account_id, provider);
        registry.remove_provider(&account_id);

        let result = registry.get_provider(&account_id);
        assert!(result.is_err());
    }
}
