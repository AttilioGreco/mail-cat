//! Wraps the raw OAuth login flow with the bookkeeping a *running* server needs.
//!
//! [`OAuthChallengeManager`] stops at "tokens are in the keychain and the account
//! is in config.toml". That is enough for a brand-new account (which needs a
//! restart to be picked up anyway), but not for re-authentication: every login
//! takes its token from the account's live [`AuthGate`], and a paused gate never
//! re-reads the keychain. So a successful re-login has to be handed to the gate
//! or the account stays paused until a restart.
//!
//! Doing it here makes every entry point — notification, UI button — behave the
//! same.

use std::collections::HashMap;
use std::sync::Arc;

use mc_application::{AuthGate, AuthGates, OAuthLoginProgress, OAuthLoginTrigger};
use mc_domain::{AccountId, OAuthProvider, SecretStore};
use tokio::sync::mpsc;

use crate::oauth_login::OAuthChallengeManager;

pub struct ReauthTrigger {
    inner: Arc<OAuthChallengeManager>,
    gates: Arc<AuthGates>,
    /// Display names of the OAuth accounts, which the login flow asks for.
    names: HashMap<AccountId, String>,
    secret_store: Arc<dyn SecretStore>,
}

impl ReauthTrigger {
    pub fn new(
        inner: Arc<OAuthChallengeManager>,
        gates: Arc<AuthGates>,
        names: HashMap<AccountId, String>,
        secret_store: Arc<dyn SecretStore>,
    ) -> Self {
        Self { inner, gates, names, secret_store }
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

        let gates = self.gates.clone();
        let secret_store = self.secret_store.clone();

        tokio::spawn(async move {
            while let Some(event) = inner_rx.recv().await {
                // Reconcile *before* forwarding Succeeded, so a client that
                // refetches accounts on that event already sees "active".
                // No gate for the address: a brand-new account, nothing live yet.
                if let OAuthLoginProgress::Succeeded { email } = &event
                    && let Some(gate) = gates.by_email(email)
                {
                    apply_fresh_tokens(gate, secret_store.as_ref());
                }
                if tx.send(event).await.is_err() {
                    break; // caller dropped the stream
                }
            }
        });

        rx
    }

    fn start_reauth(&self, account_id: AccountId) -> mpsc::Receiver<OAuthLoginProgress> {
        let target = self.gates.get(&account_id).and_then(|gate| {
            Some((self.names.get(&account_id)?.clone(), gate.email().to_owned(), gate.oauth_provider()?))
        });
        match target {
            Some((name, email, provider)) => self.start_login(name, email, provider),
            None => failed(format!("account {} is not a known OAuth account", account_id.as_uuid())),
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

/// Hands the keychain's freshly stored tokens to the gate, which resumes the
/// account and records `AccountReauthenticated`.
fn apply_fresh_tokens(gate: &AuthGate, secret_store: &dyn SecretStore) {
    let tokens = match secret_store.load(gate.email()) {
        Ok(bundle) => bundle.and_then(|b| b.oauth),
        Err(e) => {
            tracing::warn!("Re-auth succeeded for {} but re-reading keychain failed: {e}", gate.email());
            None
        }
    };
    // Resume even without tokens: the login just proved the account works, and
    // staying paused would need a restart to undo.
    gate.reauthenticated(tokens);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use mc_application::{AuthState, Credential};
    use mc_domain::{AccountSecrets, DomainError, LoginGate, LoginSecret, OAuthSecrets};
    use std::sync::Mutex;

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

    /// Without this the gate stays paused and keeps refusing logins, so a
    /// successful browser login changes nothing until restart.
    #[tokio::test]
    async fn fresh_tokens_resume_the_paused_account() {
        let store = Arc::new(MockStore::default());
        let refresh: mc_application::RefreshFn = Arc::new(|_, _| {
            Box::pin(async { Err(DomainError::AuthenticationFailed("invalid_grant".into())) })
        });
        let mut gates = AuthGates::new(refresh, store.clone(), mpsc::unbounded_channel().0);
        let stale = OAuthSecrets {
            access_token: "stale".into(),
            refresh_token: "dead".into(),
            expires_at: Utc::now() - Duration::days(1),
        };
        let gate = gates.add(
            AccountId::new(),
            "me@gmail.com",
            Credential::OAuth { provider: OAuthProvider::Google, tokens: stale },
        );
        gate.secret().await.unwrap_err();
        assert_eq!(gate.status().state, AuthState::NeedsReauth);

        let fresh = OAuthSecrets {
            access_token: "new-access".into(),
            refresh_token: "new-refresh".into(),
            expires_at: Utc::now() + Duration::hours(1),
        };
        store
            .store("me@gmail.com", &AccountSecrets { oauth: Some(fresh), ..Default::default() })
            .unwrap();

        apply_fresh_tokens(&gate, store.as_ref());

        assert_eq!(gate.status().state, AuthState::Active);
        assert_eq!(gate.secret().await.unwrap(), LoginSecret::OAuthToken("new-access".into()));
    }
}
