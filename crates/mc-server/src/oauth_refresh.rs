//! Background refresh of OAuth2 tokens close to expiry. Piggybacks on the
//! same 5-minute cadence as the existing periodic re-sync in `wiring.rs`
//! rather than running its own timer.

use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mc_application::EventPublisher;
use mc_domain::{
    AccountId, AccountWriteRepository, AggregateType, DomainError, DomainEvent, EventEnvelope,
    EventStore, OAuthProvider, SecretStore,
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// A live account's OAuth runtime state, shared with its `ImapEmailProvider`
/// and `SmtpSender` via the `Arc<RwLock<..>>` cells — refreshing here updates
/// both immediately, no restart needed.
#[derive(Clone)]
pub struct OAuthRuntimeState {
    pub account_id: AccountId,
    pub name: String,
    pub email: String,
    pub provider: OAuthProvider,
    pub access_token: Arc<RwLock<String>>,
    pub refresh_token: Arc<RwLock<String>>,
    pub expires_at: Arc<RwLock<DateTime<Utc>>>,
    /// Set once a refresh attempt proves the refresh token is dead
    /// (revoked/invalid). While true, the loop stops retrying this account.
    /// Cleared back to false once the user successfully re-authenticates.
    pub needs_reauth: Arc<RwLock<bool>>,
}

/// True once `expires_at` is within 10 minutes of (or past) `now`.
pub fn needs_refresh(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    expires_at - now <= ChronoDuration::minutes(10)
}

/// True when a live token cell should be refreshed: not already flagged
/// dead by a previous cycle, and within 10 minutes of (or past) expiry.
fn should_attempt_refresh(needs_reauth: bool, expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    !needs_reauth && needs_refresh(expires_at, now)
}

/// Appends a single event to an `Account` aggregate and publishes it to the
/// in-process event bus, mirroring `SyncService::append_and_project`'s
/// append-then-publish pattern (repository `save()` alone does not publish).
pub(crate) async fn append_account_event(
    account_id: AccountId,
    event: DomainEvent,
    event_store: &Arc<dyn EventStore>,
    account_repository: &Arc<dyn AccountWriteRepository>,
    event_publisher: &Arc<dyn EventPublisher>,
) -> Result<(), DomainError> {
    let mut account = account_repository
        .load(&account_id)
        .await?
        .ok_or_else(|| DomainError::AccountNotFound(account_id.to_string()))?;
    let current_events = event_store.load_events(&account_id.to_string(), 0).await?;
    let sequence_number = current_events.len() as u64 + 1;

    account.apply(&event);
    account_repository.save(&account, vec![event.clone()]).await?;

    event_publisher.publish(EventEnvelope::new(
        account_id.to_string(),
        AggregateType::Account,
        sequence_number,
        event,
    ));
    Ok(())
}

/// Spawns the periodic check. Call once per server startup with every
/// OAuth-authenticated account's runtime state.
pub fn spawn_refresh_loop(
    states: Vec<OAuthRuntimeState>,
    secret_store: Arc<dyn SecretStore>,
    event_store: Arc<dyn EventStore>,
    account_repository: Arc<dyn AccountWriteRepository>,
    event_publisher: Arc<dyn EventPublisher>,
    shutdown: CancellationToken,
) {
    if states.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            for state in &states {
                let expires_at = *state.expires_at.read().await;
                let dead = *state.needs_reauth.read().await;
                if !should_attempt_refresh(dead, expires_at, Utc::now()) {
                    continue;
                }
                let refresh_token = state.refresh_token.read().await.clone();
                let refresh_result = if refresh_token.is_empty() {
                    Err(DomainError::AuthenticationFailed(
                        "no refresh token stored — account needs re-authentication".into(),
                    ))
                } else {
                    mc_infrastructure::oauth::refresh(state.provider, &refresh_token).await
                };
                match refresh_result {
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
                    Err(DomainError::AuthenticationFailed(msg)) => {
                        tracing::error!(
                            "OAuth refresh token for {} is invalid or revoked ({msg}) — \
                             clearing tokens, account needs re-authentication",
                            state.email
                        );
                        *state.needs_reauth.write().await = true;

                        let bundle = match secret_store.load(&state.email) {
                            Ok(existing) => Some(existing.unwrap_or_default()),
                            Err(e) => {
                                // Don't `continue` here: even though we can't clear the
                                // keychain, the account still needs the user's attention
                                // (needs_reauth is already true) — falling through to
                                // append_account_event below is what drives the desktop
                                // notification. Skipping it would strand the account
                                // silently until a server restart.
                                tracing::error!(
                                    "OAuth de-auth for {}: failed reading keychain before clearing: {e}",
                                    state.email
                                );
                                None
                            }
                        };
                        if let Some(mut bundle) = bundle {
                            bundle.oauth = None;
                            if let Err(e) = secret_store.store(&state.email, &bundle) {
                                tracing::error!("OAuth de-auth for {}: failed clearing keychain: {e}", state.email);
                            }
                        }

                        if let Err(e) = append_account_event(
                            state.account_id,
                            DomainEvent::AccountAuthExpired {
                                account_id: state.account_id,
                                error: msg,
                            },
                            &event_store,
                            &account_repository,
                            &event_publisher,
                        )
                        .await
                        {
                            tracing::error!(
                                "OAuth de-auth for {}: failed recording AccountAuthExpired event: {e}",
                                state.email
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "OAuth refresh for {} failed ({e}) — will retry next cycle",
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

    #[test]
    fn should_attempt_refresh_false_once_flagged_needs_reauth() {
        let now = Utc::now();
        assert!(!should_attempt_refresh(true, now + ChronoDuration::minutes(1), now));
    }

    #[test]
    fn should_attempt_refresh_true_when_due_and_not_flagged() {
        let now = Utc::now();
        assert!(should_attempt_refresh(false, now - ChronoDuration::minutes(1), now));
    }
}
