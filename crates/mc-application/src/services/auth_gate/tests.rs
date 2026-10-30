use super::*;
use futures::FutureExt;
use mc_domain::{AccountSecrets, login_with};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Default)]
struct Keychain(Mutex<HashMap<String, AccountSecrets>>);

impl SecretStore for Keychain {
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

const EMAIL: &str = "me@gmail.com";

fn tokens(access: &str, expires_in: TimeDelta) -> OAuthSecrets {
    OAuthSecrets {
        access_token: access.into(),
        refresh_token: "refresh".into(),
        expires_at: Utc::now() + expires_in,
    }
}

struct Harness {
    gates: AuthGates,
    keychain: Arc<Keychain>,
    refreshes: Arc<AtomicUsize>,
    events: mpsc::UnboundedReceiver<(AccountId, DomainEvent)>,
}

impl Harness {
    /// `refresh` answers every refresh; each call is counted and takes 50ms.
    fn new(refresh: fn() -> Result<OAuthSecrets, DomainError>) -> Self {
        let refreshes = Arc::new(AtomicUsize::new(0));
        let counter = refreshes.clone();
        let refresh: RefreshFn = Arc::new(move |_, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                refresh()
            }
            .boxed()
        });
        let keychain = Arc::new(Keychain::default());
        keychain
            .store(
                EMAIL,
                &AccountSecrets {
                    oauth: Some(tokens("old", TimeDelta::zero())),
                    ..Default::default()
                },
            )
            .unwrap();
        let (tx, events) = mpsc::unbounded_channel();
        Self {
            gates: AuthGates::new(refresh, keychain.clone(), tx),
            keychain,
            refreshes,
            events,
        }
    }

    fn oauth(&mut self, expires_in: TimeDelta) -> Arc<AuthGate> {
        let credential = Credential::OAuth {
            provider: OAuthProvider::Google,
            tokens: tokens("old", expires_in),
        };
        self.gates.add(AccountId::new(), EMAIL, credential)
    }

    fn password(&mut self) -> Arc<AuthGate> {
        self.gates.add(
            AccountId::new(),
            "me@example.com",
            Credential::Password("hunter2".into()),
        )
    }

    fn refreshes(&self) -> usize {
        self.refreshes.load(Ordering::SeqCst)
    }

    fn event_names(&mut self) -> Vec<&'static str> {
        std::iter::from_fn(|| self.events.try_recv().ok())
            .map(|(_, e)| e.event_name())
            .collect()
    }
}

fn fresh() -> Result<OAuthSecrets, DomainError> {
    Ok(tokens("new", TimeDelta::hours(1)))
}

fn revoked() -> Result<OAuthSecrets, DomainError> {
    Err(DomainError::AuthenticationFailed("invalid_grant".into()))
}

fn rejected() -> DomainError {
    DomainError::AuthenticationFailed("AUTHENTICATIONFAILED".into())
}

/// A server that accepts only `accepted` and records every login.
async fn login(
    gate: &AuthGate,
    accepted: &'static str,
    seen: &Mutex<Vec<String>>,
) -> Result<(), DomainError> {
    login_with(gate, |secret| async move {
        let token = match secret {
            LoginSecret::OAuthToken(t) | LoginSecret::Password(t) => t,
        };
        seen.lock().unwrap().push(token.clone());
        if token == accepted {
            Ok(())
        } else {
            Err(rejected())
        }
    })
    .await
}

#[tokio::test]
async fn an_expired_token_is_refreshed_before_the_login() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::minutes(-30));
    let seen = Mutex::new(vec![]);

    login(&gate, "new", &seen).await.unwrap();

    assert_eq!(
        *seen.lock().unwrap(),
        ["new"],
        "the stale token never reached the server"
    );
    assert_eq!(h.refreshes(), 1);
    assert_eq!(
        h.keychain
            .load(EMAIL)
            .unwrap()
            .unwrap()
            .oauth
            .unwrap()
            .access_token,
        "new"
    );
    assert_eq!(gate.status().state, AuthState::Active);
}

#[tokio::test]
async fn a_rejected_token_is_refreshed_and_retried_once() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::hours(1));
    let seen = Mutex::new(vec![]);

    login(&gate, "new", &seen).await.unwrap();

    assert_eq!(*seen.lock().unwrap(), ["old", "new"]);
    assert_eq!(gate.status().state, AuthState::Active);
    assert!(h.event_names().is_empty());
}

#[tokio::test]
async fn a_refreshed_token_still_rejected_needs_reauth() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::hours(1));
    let seen = Mutex::new(vec![]);

    login(&gate, "never", &seen).await.unwrap_err();

    assert_eq!(seen.lock().unwrap().len(), 2, "exactly one retry");
    let status = gate.status();
    assert_eq!(status.state, AuthState::NeedsReauth);
    let reason = status.reason.unwrap();
    assert!(
        reason.contains("freshly refreshed") && reason.contains("AUTHENTICATIONFAILED"),
        "{reason}"
    );
    assert_eq!(h.event_names(), ["AccountAuthExpired"]);
}

#[tokio::test]
async fn invalid_grant_needs_reauth_and_keeps_the_keychain() {
    let mut h = Harness::new(revoked);
    let gate = h.oauth(TimeDelta::minutes(-1));
    let before = h.keychain.load(EMAIL).unwrap();

    login(&gate, "new", &Mutex::new(vec![])).await.unwrap_err();

    assert_eq!(gate.status().state, AuthState::NeedsReauth);
    assert_eq!(
        h.keychain.load(EMAIL).unwrap(),
        before,
        "keychain untouched"
    );
    assert_eq!(h.event_names(), ["AccountAuthExpired"]);
}

#[tokio::test]
async fn a_rejected_password_is_not_an_oauth_reauth() {
    let mut h = Harness::new(fresh);
    let gate = h.password();
    let seen = Mutex::new(vec![]);

    login(&gate, "other", &seen).await.unwrap_err();

    assert_eq!(seen.lock().unwrap().len(), 1, "a password is never retried");
    assert_eq!(gate.status().state, AuthState::CredentialsRejected);
    assert_eq!(h.refreshes(), 0);
    assert_eq!(h.event_names(), ["AccountCredentialsRejected"]);

    gate.set_password("other".into());
    assert_eq!(gate.status().state, AuthState::Active);
    assert_eq!(h.event_names(), ["AccountReauthenticated"]);
    login(&gate, "other", &seen).await.unwrap();
}

#[tokio::test]
async fn the_offline_schedule_is_fast_first_and_resets_on_success() {
    let mut h = Harness::new(fresh);
    let gate = h.password();
    let down = DomainError::ConnectionFailed("TCP connection failed".into());
    let secret = LoginSecret::Password("hunter2".into());
    let delay = |gate: &AuthGate| match gate.status().state {
        AuthState::Offline { next_retry } => (next_retry - Utc::now()).num_milliseconds(),
        other => panic!("not offline: {other:?}"),
    };

    let mut delays = vec![];
    for _ in 0..8 {
        // Each round is a retry that came due.
        gate.tracked.send_modify(|t| {
            if let AuthState::Offline { next_retry } = &mut t.state {
                *next_retry = Utc::now();
            }
        });
        assert!(!gate.login_failed(&secret, &down, false).await);
        delays.push((delay(&gate) + 500) / 1000);
    }
    assert_eq!(delays, [2, 5, 10, 30, 60, 120, 300, 300]);

    // A failure before the retry is due does not advance the schedule.
    gate.login_failed(&secret, &down, false).await;
    assert_eq!((delay(&gate) + 500) / 1000, 300);

    gate.login_succeeded();
    assert_eq!(gate.status().state, AuthState::Active);
    gate.login_failed(&secret, &down, false).await;
    assert_eq!((delay(&gate) + 500) / 1000, 2, "the schedule starts over");
    assert!(h.event_names().is_empty(), "offline is not persisted");
}

#[tokio::test]
async fn a_sleeping_loop_wakes_at_the_fast_retry() {
    let mut h = Harness::new(fresh);
    let gate = h.password();
    let waiting = tokio::spawn({
        let gate = gate.clone();
        async move { gate.retry_due().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "active: nothing to retry");

    let down = DomainError::ConnectionFailed("TCP connection failed".into());
    gate.login_failed(&LoginSecret::Password("hunter2".into()), &down, false)
        .await;
    tokio::time::timeout(Duration::from_secs(4), waiting)
        .await
        .expect("woke at the 2s retry")
        .unwrap();
}

#[tokio::test]
async fn smtp_never_refreshes_an_offline_account() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::hours(1));
    let busy = DomainError::Throttled("Too many simultaneous connections. (Failure)".into());
    gate.went_offline(&busy);
    let before = gate.status().state;

    assert!(
        !gate
            .smtp_token_refused(&LoginSecret::OAuthToken("old".into()))
            .await
    );

    assert_eq!(h.refreshes(), 0);
    assert_eq!(
        gate.status().state,
        before,
        "still offline, same retry time"
    );
}

#[tokio::test]
async fn a_first_throttle_after_network_errors_starts_at_five_minutes() {
    let mut h = Harness::new(fresh);
    let gate = h.password();
    let secret = LoginSecret::Password("hunter2".into());
    let due = |gate: &AuthGate| {
        gate.tracked.send_modify(|t| {
            if let AuthState::Offline { next_retry } = &mut t.state {
                *next_retry = Utc::now();
            }
        })
    };
    for _ in 0..4 {
        due(&gate);
        gate.login_failed(
            &secret,
            &DomainError::ConnectionFailed("down".into()),
            false,
        )
        .await;
    }
    due(&gate);
    gate.login_failed(&secret, &DomainError::Throttled("busy".into()), false)
        .await;

    let AuthState::Offline { next_retry } = gate.status().state else {
        panic!("not offline")
    };
    assert_eq!(
        (next_retry - Utc::now() + TimeDelta::seconds(30)).num_minutes(),
        5
    );
}

#[tokio::test]
async fn a_throttled_account_backs_off_in_minutes() {
    let mut h = Harness::new(fresh);
    let gate = h.password();
    let busy = DomainError::Throttled("Too many simultaneous connections. (Failure)".into());
    let secret = LoginSecret::Password("hunter2".into());

    let mut delays = vec![];
    for _ in 0..5 {
        gate.tracked.send_modify(|t| {
            if let AuthState::Offline { next_retry } = &mut t.state {
                *next_retry = Utc::now();
            }
        });
        gate.login_failed(&secret, &busy, false).await;
        let AuthState::Offline { next_retry } = gate.status().state else {
            panic!("not offline")
        };
        delays.push((next_retry - Utc::now() + TimeDelta::seconds(30)).num_minutes());
    }
    assert_eq!(delays, [5, 10, 20, 30, 30]);
    assert_eq!(
        gate.status().reason.as_deref(),
        Some("over the server's usage limit")
    );
    assert!(h.event_names().is_empty());
}

#[tokio::test]
async fn loops_wait_while_paused_and_resume_after_reauth() {
    let mut h = Harness::new(revoked);
    let gate = h.oauth(TimeDelta::minutes(-1));
    let seen = Mutex::new(vec![]);
    login(&gate, "new", &seen).await.unwrap_err();
    assert_eq!(h.refreshes(), 1);

    // Paused: logins fail without reaching the server or the token endpoint.
    login(&gate, "new", &seen).await.unwrap_err();
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(h.refreshes(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), gate.ready())
            .await
            .is_err()
    );

    let waiting = tokio::spawn({
        let gate = gate.clone();
        async move { gate.ready().await }
    });
    gate.reauthenticated(Some(tokens("new", TimeDelta::hours(1))))
        .await;
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap();
    login(&gate, "new", &seen).await.unwrap();
    assert_eq!(
        h.event_names(),
        ["AccountAuthExpired", "AccountReauthenticated"]
    );
}

#[tokio::test]
async fn concurrent_logins_share_one_refresh() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::minutes(-1));
    let seen = Mutex::new(vec![]);

    let logins: Vec<_> = (0..5).map(|_| login(&gate, "new", &seen)).collect();
    for result in futures::future::join_all(logins).await {
        result.unwrap();
    }

    assert_eq!(h.refreshes(), 1);
}

#[tokio::test]
async fn concurrent_rejections_share_one_forced_refresh() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::hours(1));
    let seen = Mutex::new(vec![]);

    let logins: Vec<_> = (0..5).map(|_| login(&gate, "new", &seen)).collect();
    for result in futures::future::join_all(logins).await {
        result.unwrap();
    }

    assert_eq!(h.refreshes(), 1);
}

#[tokio::test]
async fn concurrent_logins_on_a_revoked_grant_share_one_refresh() {
    let mut h = Harness::new(revoked);
    let gate = h.oauth(TimeDelta::minutes(-1));
    let seen = Mutex::new(vec![]);

    let logins: Vec<_> = (0..5).map(|_| login(&gate, "new", &seen)).collect();
    for result in futures::future::join_all(logins).await {
        result.unwrap_err();
    }

    assert_eq!(h.refreshes(), 1, "the waiters saw the pause and stopped");
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(h.event_names(), ["AccountAuthExpired"]);
}

#[tokio::test]
async fn a_successful_refresh_ends_offline() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::minutes(-1));
    let down = DomainError::ConnectionFailed("TCP connection failed".into());
    gate.went_offline(&down);
    gate.tracked.send_modify(|t| {
        t.state = AuthState::Offline {
            next_retry: Utc::now(),
        }
    });

    gate.valid_secret(REFRESH_MARGIN).await.unwrap();

    assert_eq!(gate.status().state, AuthState::Active);
}

#[tokio::test]
async fn a_relogin_wins_over_a_refresh_in_flight() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::minutes(-1));
    let relogin = tokens("relogin", TimeDelta::hours(1));

    let refreshing = gate.valid_secret(REFRESH_MARGIN);
    let reauth = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        gate.reauthenticated(Some(relogin.clone())).await;
    };
    let (refreshed, ()) = tokio::join!(refreshing, reauth);

    assert_eq!(
        refreshed.unwrap(),
        LoginSecret::OAuthToken("new".into()),
        "the refresh did run"
    );
    assert_eq!(
        gate.secret().await.unwrap(),
        LoginSecret::OAuthToken("relogin".into())
    );
    assert_eq!(
        h.keychain.load(EMAIL).unwrap().unwrap().oauth,
        Some(relogin)
    );
}

#[tokio::test]
async fn a_paused_account_gets_one_login_per_boot() {
    let mut h = Harness::new(fresh);
    let gate = h.password();
    let persisted = AccountSummary {
        id: gate.account_id(),
        name: "Work".into(),
        email: mc_domain::EmailAddress::new("me@example.com").unwrap(),
        provider_type: mc_domain::ProviderType::Imap,
        mailbox_model: mc_domain::MailboxModel::Folders,
        supports_tags: None,
        status: AccountStatus::CredentialsRejected,
        total_unread: 0,
        status_error: Some("the server rejected the password".into()),
        status_since: None,
    };
    gate.restore(&persisted);
    let attempts = AtomicUsize::new(0);

    gate.retry_after_restart(|_| async {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err(rejected())
    })
    .await;
    assert_eq!(
        gate.status().state,
        AuthState::CredentialsRejected,
        "still refused: stays paused"
    );
    assert!(h.event_names().is_empty());

    // The next boot, with the password imported from config.toml.
    gate.retry_after_restart(|secret| async move {
        assert_eq!(secret, LoginSecret::Password("hunter2".into()));
        Ok(())
    })
    .await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(gate.status().state, AuthState::Active);
    assert_eq!(h.event_names(), ["AccountReauthenticated"]);
}

#[tokio::test]
async fn a_paused_oauth_account_refreshes_once_at_boot() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::hours(1));
    gate.tracked
        .send_modify(|t| t.state = AuthState::NeedsReauth);

    gate.retry_after_restart(|secret| async move {
        assert_eq!(
            secret,
            LoginSecret::OAuthToken("new".into()),
            "logs in with the refreshed token"
        );
        Ok(())
    })
    .await;

    assert_eq!(h.refreshes(), 1);
    assert_eq!(gate.status().state, AuthState::Active);
}

#[tokio::test]
async fn a_rejected_oauth_client_pauses_without_persisting() {
    let mut h = Harness::new(|| Err(DomainError::OAuthClientInvalid("invalid_client".into())));
    let gate = h.oauth(TimeDelta::minutes(-1));

    login(&gate, "new", &Mutex::new(vec![])).await.unwrap_err();

    assert_eq!(gate.status().state, AuthState::OAuthClientInvalid);
    assert!(h.event_names().is_empty());
}

#[tokio::test]
async fn a_persisted_pause_survives_a_restart() {
    let mut h = Harness::new(fresh);
    let gate = h.oauth(TimeDelta::hours(1));
    gate.restore(&AccountSummary {
        id: gate.account_id(),
        name: "Work".into(),
        email: mc_domain::EmailAddress::new(EMAIL).unwrap(),
        provider_type: mc_domain::ProviderType::Imap,
        mailbox_model: mc_domain::MailboxModel::Folders,
        supports_tags: None,
        status: AccountStatus::NeedsReauth,
        total_unread: 0,
        status_error: Some("refresh token revoked or expired".into()),
        status_since: None,
    });

    assert!(gate.secret().await.is_err());
    assert_eq!(
        gate.status().reason.as_deref(),
        Some("refresh token revoked or expired")
    );
    assert!(h.event_names().is_empty(), "restoring records nothing new");
}

#[tokio::test]
async fn a_password_account_flagged_needs_reauth_by_older_builds_asks_for_the_password() {
    let mut h = Harness::new(fresh);
    let gate = h.password();
    gate.restore(&AccountSummary {
        id: gate.account_id(),
        name: "Work".into(),
        email: mc_domain::EmailAddress::new("me@example.com").unwrap(),
        provider_type: mc_domain::ProviderType::Imap,
        mailbox_model: mc_domain::MailboxModel::Folders,
        supports_tags: None,
        status: AccountStatus::NeedsReauth,
        total_unread: 0,
        status_error: None,
        status_since: None,
    });
    assert_eq!(gate.status().state, AuthState::CredentialsRejected);

    gate.set_password("new".into());
    assert_eq!(gate.status().state, AuthState::Active);
    assert_eq!(h.event_names(), ["AccountReauthenticated"]);
}

#[tokio::test(start_paused = true)]
async fn the_refresh_timer_follows_accounts_added_and_removed_after_it_started() {
    let h = Harness::new(fresh);
    let refreshes = h.refreshes.clone();
    let gates = Arc::new(h.gates);
    let token = CancellationToken::new();
    gates.spawn_refresh_timer(token.clone());
    tokio::time::sleep(Duration::from_secs(1)).await; // first tick: nothing to do

    let oauth = |gates: &AuthGates| {
        gates.add(
            AccountId::new(),
            EMAIL,
            Credential::OAuth {
                provider: OAuthProvider::Google,
                tokens: tokens("old", TimeDelta::minutes(1)),
            },
        )
    };
    let kept = oauth(&gates);
    let removed = oauth(&gates);
    assert!(gates.get(&removed.account_id()).is_some());
    assert!(gates.remove(&removed.account_id()).is_some());
    assert!(gates.get(&removed.account_id()).is_none());
    assert!(gates.by_email(EMAIL).is_some());
    assert_eq!(gates.statuses().len(), 1);

    tokio::time::sleep(TIMER_PERIOD).await;
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(
        kept.secret().await.unwrap(),
        LoginSecret::OAuthToken("new".into())
    );
    token.cancel();
}
