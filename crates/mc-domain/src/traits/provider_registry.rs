use std::sync::Arc;

use crate::DomainError;
use crate::model::account::AccountId;
use crate::traits::email_provider::EmailProvider;

/// Registry that maps AccountId to an EmailProvider instance.
///
/// Allows multi-account support by resolving the correct provider
/// for each account at runtime.
pub trait ProviderRegistry: Send + Sync {
    type Provider: EmailProvider;

    /// Get the provider for the given account.
    fn get_provider(&self, account_id: &AccountId) -> Result<Arc<Self::Provider>, DomainError>;

    /// Register a provider for the given account.
    fn register_provider(&self, account_id: AccountId, provider: Arc<Self::Provider>);

    /// Remove the provider for the given account.
    fn remove_provider(&self, account_id: &AccountId);
}
