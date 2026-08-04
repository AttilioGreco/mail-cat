//! Wraps the raw OAuth login flow with the bookkeeping a *running* server needs.
//!
//! [`OAuthChallengeManager`] stops at "tokens are in the keychain and the account
//! is in config.toml". That is enough for a brand-new account (which needs a
//! restart to be picked up anyway), but not for re-authentication: the refresh
//! loop and the IMAP providers read from the live [`OAuthRuntimeState`] cells,
//! and once `needs_reauth` is set nothing ever re-reads the keychain. So a
//! successful re-login has to be pushed back into those cells or the account
//! stays dead until a restart.
//!
//! That reconciliation used to live inside the desktop-notification handler,
//! which meant only the notification's "Ri-autentica" button actually revived an
//! account. Doing it here makes every entry point — notification, UI button —
//! behave the same.

use std::sync::Arc;

use mc_application::{EventPublisher, OAuthLoginProgress, OAuthLoginTrigger};
use mc_domain::{
    AccountId, AccountWriteRepository, DomainEvent, EventStore, OAuthProvider, SecretStore,
};
use tokio::sync::mpsc;

use crate::oauth_login::OAuthChallengeManager;
use crate::oauth_refresh::{self, OAuthRuntimeState};

pub struct ReauthTrigger {
    inner: Arc<OAuthChallengeManager>,
    states: Vec<OAuthRuntimeState>,
    secret_store: Arc<dyn SecretStore>,
    event_store: Arc<dyn EventStore>,
    repository: Arc<dyn AccountWriteRepository>,
    publisher: Arc<dyn EventPublisher>,
}

impl ReauthTrigger {
    pub fn new(
        inner: Arc<OAuthChallengeManager>,
        states: Vec<OAuthRuntimeState>,
        secret_store: Arc<dyn SecretStore>,
        event_store: Arc<dyn EventStore>,
        repository: Arc<dyn AccountWriteRepository>,
        publisher: Arc<dyn EventPublisher>,
    ) -> Self {
        Self {
            inner,
            states,
            secret_store,
            event_store,
            repository,
            publisher,
        }
    }
}

impl OAuthLoginTrigger for ReauthTrigger {
    fn start_login(
        &self,
        name: String,
        email: String,
        provider: OAuthProvider,
    ) -> mpsc::Receiver<OAuthLoginProgress> {
        let mut inner_rx = self.inner.start_login(name, email, provider);
        let (tx, rx) = mpsc::channel(2);

        let states = self.states.clone();
        let secret_store = self.secret_store.clone();
        let event_store = self.event_store.clone();
        let repository = self.repository.clone();
        let publisher = self.publisher.clone();

        tokio::spawn(async move {
            while let Some(event) = inner_rx.recv().await {
                // Reconcile *before* forwarding Succeeded, so a client that
                // refetches accounts on that event already sees "active".
                if let OAuthLoginProgress::Succeeded { email } = &event {
                    revive_account(
                        &states,
                        email,
                        &secret_store,
                        &event_store,
                        &repository,
                        &publisher,
                    )
                    .await;
                }
                if tx.send(event).await.is_err() {
                    break; // caller dropped the stream
                }
            }
        });

        rx
    }

    fn start_reauth(&self, account_id: AccountId) -> mpsc::Receiver<OAuthLoginProgress> {
        match self.states.iter().find(|s| s.account_id == account_id) {
            Some(state) => {
                self.start_login(state.name.clone(), state.email.clone(), state.provider)
            }
            None => failed(format!(
                "account {} is not a known OAuth account",
                account_id.as_uuid()
            )),
        }
    }
}

fn failed(message: String) -> mpsc::Receiver<OAuthLoginProgress> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let _ = tx.send(OAuthLoginProgress::Failed { message }).await;
    });
    rx
}

/// Pushes freshly stored tokens back into the live runtime state and records
/// `AccountReauthenticated`, so the refresh loop and the IMAP providers pick the
/// account back up without a restart.
///
/// No-op for an address with no runtime state — that's a brand-new account being
/// added, which has nothing live to reconcile yet.
async fn revive_account(
    states: &[OAuthRuntimeState],
    email: &str,
    secret_store: &Arc<dyn SecretStore>,
    event_store: &Arc<dyn EventStore>,
    repository: &Arc<dyn AccountWriteRepository>,
    publisher: &Arc<dyn EventPublisher>,
) {
    let Some(account_id) = apply_fresh_tokens(states, email, secret_store).await else {
        return;
    };

    if let Err(e) = oauth_refresh::append_account_event(
        account_id,
        DomainEvent::AccountReauthenticated { account_id },
        event_store,
        repository,
        publisher,
    )
    .await
    {
        tracing::warn!("Failed to record AccountReauthenticated for {email}: {e}");
    }
}

/// Copies the keychain's current tokens into the live cells for `email` and
/// clears `needs_reauth`, returning the account that was revived.
///
/// `None` means there is no runtime state for the address, i.e. nothing to
/// reconcile.
async fn apply_fresh_tokens(
    states: &[OAuthRuntimeState],
    email: &str,
    secret_store: &Arc<dyn SecretStore>,
) -> Option<AccountId> {
    let state = states.iter().find(|s| s.email == email)?;

    match secret_store.load(email) {
        Ok(Some(bundle)) => {
            if let Some(oauth) = bundle.oauth {
                *state.access_token.write().await = oauth.access_token;
                *state.refresh_token.write().await = oauth.refresh_token;
                *state.expires_at.write().await = oauth.expires_at;
            }
        }
        Ok(None) => {
            tracing::warn!("Re-auth succeeded for {email} but keychain entry is missing")
        }
        Err(e) => {
            tracing::warn!("Re-auth succeeded for {email} but re-reading keychain failed: {e}")
        }
    }
    // Clear the flag even when the keychain read failed: the tokens are known to
    // be fresh, and leaving it set would keep the refresh loop skipping this
    // account forever.
    *state.needs_reauth.write().await = false;

    Some(state.account_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use mc_domain::{AccountSecrets, DomainError, OAuthSecrets};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::sync::RwLock;

    #[derive(Default)]
    struct MockStore(Mutex<HashMap<String, AccountSecrets>>);
    impl SecretStore for MockStore {
        fn load(&self, email: &str) -> Result<Option<AccountSecrets>, DomainError> {
            Ok(self.0.lock().unwrap().get(email).cloned())
        }
        fn store(&self, email: &str, s: &AccountSecrets) -> Result<(), DomainError> {
            self.0.lock().unwrap().insert(email.into(), s.clone());
            Ok(())
        }
        fn delete(&self, email: &str) -> Result<(), DomainError> {
            self.0.lock().unwrap().remove(email);
            Ok(())
        }
    }

    fn dead_state(email: &str) -> OAuthRuntimeState {
        OAuthRuntimeState {
            account_id: AccountId::new(),
            name: "Work".into(),
            email: email.into(),
            provider: OAuthProvider::Google,
            access_token: Arc::new(RwLock::new("stale".into())),
            refresh_token: Arc::new(RwLock::new(String::new())),
            expires_at: Arc::new(RwLock::new(Utc::now() - Duration::days(1))),
            needs_reauth: Arc::new(RwLock::new(true)),
        }
    }

    /// The whole point of the wrapper: without this the refresh loop keeps
    /// skipping the account (needs_reauth) and the providers keep presenting the
    /// stale token, so a successful browser login changes nothing until restart.
    #[tokio::test]
    async fn fresh_tokens_land_in_the_live_state() {
        let store = Arc::new(MockStore::default());
        store
            .store(
                "me@gmail.com",
                &AccountSecrets {
                    oauth: Some(OAuthSecrets {
                        access_token: "new-access".into(),
                        refresh_token: "new-refresh".into(),
                        expires_at: Utc::now() + Duration::hours(1),
                    }),
                    ..Default::default()
                },
            )
            .unwrap();
        let states = vec![dead_state("me@gmail.com")];
        let secret_store = store as Arc<dyn SecretStore>;

        let revived = apply_fresh_tokens(&states, "me@gmail.com", &secret_store).await;

        assert_eq!(revived, Some(states[0].account_id));
        assert_eq!(*states[0].access_token.read().await, "new-access");
        assert_eq!(*states[0].refresh_token.read().await, "new-refresh");
        assert!(!*states[0].needs_reauth.read().await);
    }

    #[tokio::test]
    async fn a_brand_new_account_has_nothing_to_revive() {
        let secret_store = Arc::new(MockStore::default()) as Arc<dyn SecretStore>;
        let states = vec![dead_state("me@gmail.com")];

        let revived = apply_fresh_tokens(&states, "other@gmail.com", &secret_store).await;

        assert!(revived.is_none());
        assert!(*states[0].needs_reauth.read().await, "untouched");
    }
}
