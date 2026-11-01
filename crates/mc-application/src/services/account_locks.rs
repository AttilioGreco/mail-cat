//! One lock per account, shared by whoever must not run beside a sync.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mc_domain::AccountId;

#[derive(Default)]
pub struct AccountLocks(Mutex<HashMap<AccountId, Arc<tokio::sync::Mutex<()>>>>);

impl AccountLocks {
    pub async fn lock(&self, account_id: &AccountId) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = Arc::clone(self.0.lock().unwrap().entry(*account_id).or_default());
        lock.lock_owned().await
    }

    pub fn forget(&self, account_id: &AccountId) {
        self.0.lock().unwrap().remove(account_id);
    }
}
