//! Background refresh of OAuth2 tokens close to expiry. Piggybacks on the
//! same 5-minute cadence as the existing periodic re-sync in `wiring.rs`
//! rather than running its own timer.

use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mc_domain::{OAuthProvider, SecretStore};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// A live account's OAuth runtime state, shared with its `ImapEmailProvider`
/// and `SmtpSender` via the `Arc<RwLock<..>>` cells — refreshing here updates
/// both immediately, no restart needed.
#[derive(Clone)]
pub struct OAuthRuntimeState {
    pub email: String,
    pub provider: OAuthProvider,
    pub access_token: Arc<RwLock<String>>,
    pub refresh_token: Arc<RwLock<String>>,
    pub expires_at: Arc<RwLock<DateTime<Utc>>>,
}

/// True once `expires_at` is within 2 minutes of (or past) `now`.
pub fn needs_refresh(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    expires_at - now <= ChronoDuration::minutes(2)
}

/// Spawns the periodic check. Call once per server startup with every
/// OAuth-authenticated account's runtime state.
pub fn spawn_refresh_loop(
    states: Vec<OAuthRuntimeState>,
    secret_store: Arc<dyn SecretStore>,
    shutdown: CancellationToken,
) {
    if states.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
        interval.tick().await; // skip immediate first tick
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            for state in &states {
                let expires_at = *state.expires_at.read().await;
                if !needs_refresh(expires_at, Utc::now()) {
                    continue;
                }
                let refresh_token = state.refresh_token.read().await.clone();
                match mc_infrastructure::oauth::refresh(state.provider, &refresh_token).await {
                    Ok(new_secrets) => {
                        *state.access_token.write().await = new_secrets.access_token.clone();
                        *state.refresh_token.write().await = new_secrets.refresh_token.clone();
                        *state.expires_at.write().await = new_secrets.expires_at;

                        // Don't let a real Err (e.g. a corrupt/undeserializable
                        // keychain entry) silently collapse to "no entry" — that
                        // would overwrite the keychain with only the new OAuth
                        // tokens, discarding any existing imap/smtp passwords for
                        // this account. Skip this account's refresh persistence
                        // (not the whole loop) on a real read error.
                        let mut bundle = match secret_store.load(&state.email) {
                            Ok(existing) => existing.unwrap_or_default(),
                            Err(e) => {
                                tracing::error!(
                                    "OAuth refresh for {}: failed reading keychain before update: {e}",
                                    state.email
                                );
                                continue;
                            }
                        };
                        bundle.oauth = Some(new_secrets);
                        if let Err(e) = secret_store.store(&state.email, &bundle) {
                            tracing::error!("OAuth refresh for {}: failed writing keychain: {e}", state.email);
                        } else {
                            tracing::info!("OAuth token refreshed for {}", state.email);
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "OAuth refresh for {} failed ({e}) — token likely expired; run `mc accounts add-oauth` again to re-authenticate",
                            state.email
                        );
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_refresh_true_when_within_threshold() {
        let now = Utc::now();
        assert!(needs_refresh(now + ChronoDuration::minutes(1), now));
        assert!(needs_refresh(now - ChronoDuration::minutes(1), now));
    }

    #[test]
    fn needs_refresh_false_when_far_from_expiry() {
        let now = Utc::now();
        assert!(!needs_refresh(now + ChronoDuration::minutes(30), now));
    }
}
