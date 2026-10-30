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
        DomainError, Existing, Flag, Mailbox, MailboxChanges, MailboxCounts, MailboxPath, Message,
        MessageEnvelopeSummary, OutgoingMessage, ProviderNotification, RemoteId, SyncCursor,
    };
    use std::collections::HashMap;
    use std::pin::Pin;

    #[derive(Debug)]
    struct StubProvider;

    #[async_trait]
    impl EmailProvider for StubProvider {
        async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError> {
            Ok(vec![])
        }
        async fn mailbox_changes(
            &self,
            _mailbox: &MailboxPath,
            _since: Option<&SyncCursor>,
        ) -> Result<MailboxChanges, DomainError> {
            Ok(MailboxChanges {
                cursor: SyncCursor::new("stub"),
                reset: None,
                added: vec![],
                keys: Default::default(),
                labels: Default::default(),
                existing: Existing::Snapshot(vec![]),
                counts: MailboxCounts {
                    total: 0,
                    unread: 0,
                },
            })
        }
        async fn fetch_envelopes(
            &self,
            _mailbox: &MailboxPath,
            _ids: &[RemoteId],
        ) -> Result<Vec<MessageEnvelopeSummary>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_message_detail(
            &self,
            _mailbox: &MailboxPath,
            _id: &RemoteId,
        ) -> Result<Message, DomainError> {
            Err(DomainError::Provider("stub".into()))
        }
        async fn find_by_message_id(
            &self,
            _mailbox: &MailboxPath,
            _rfc_id: &str,
        ) -> Result<Vec<RemoteId>, DomainError> {
            Ok(vec![])
        }
        async fn fetch_raw_message(
            &self,
            _mailbox: &MailboxPath,
            _id: &RemoteId,
        ) -> Result<Vec<u8>, DomainError> {
            Ok(vec![])
        }
        async fn set_flags_bulk(
            &self,
            _mailbox: &MailboxPath,
            _ids: &[RemoteId],
            _add: &[Flag],
            _remove: &[Flag],
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn move_messages(
            &self,
            _ids: &[RemoteId],
            _from: &MailboxPath,
            _to: &MailboxPath,
        ) -> Result<HashMap<RemoteId, Option<RemoteId>>, DomainError> {
            Ok(HashMap::new())
        }
        async fn delete_messages(
            &self,
            _mailbox: &MailboxPath,
            _ids: &[RemoteId],
            _permanently: bool,
        ) -> Result<(), DomainError> {
            Ok(())
        }
        async fn send_message(
            &self,
            _message: &OutgoingMessage,
            _sent: Option<&MailboxPath>,
        ) -> Result<mc_domain::SendOutcome, DomainError> {
            Ok(mc_domain::SendOutcome {
                sent_copy: mc_domain::SentCopy::NotStored,
            })
        }
        async fn save_draft(
            &self,
            _drafts: &MailboxPath,
            _message: &OutgoingMessage,
            _replacing: Option<&RemoteId>,
        ) -> Result<RemoteId, DomainError> {
            Ok(RemoteId::from(1))
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
            _ids: &[RemoteId],
        ) -> Result<Vec<mc_domain::traits::email_provider::MessageBodyResult>, DomainError>
        {
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
