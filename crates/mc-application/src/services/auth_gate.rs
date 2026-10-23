//! The one owner of each account's credential and auth state.
//!
//! Every IMAP/SMTP login goes through an account's [`AuthGate`] (as a
//! [`LoginGate`]): it refreshes an OAuth token before it is presented, retries
//! once after a rejection with a forced refresh, and turns each outcome into a
//! single state. The background loops wait on [`LoginGate::ready`], so an
//! account that needs the user stops connecting until the user acts, and an
//! unreachable server is retried on one schedule instead of per loop.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use futures::future::BoxFuture;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use mc_domain::{
    AccountId, AccountStatus, AccountSummary, DomainError, DomainEvent, LoginGate, LoginSecret,
    OAuthProvider, OAuthSecrets, SecretStore,
};

/// A token this close to expiry is refreshed before a login presents it.
const REFRESH_MARGIN: TimeDelta = TimeDelta::minutes(5);

/// The background timer refreshes earlier, so a login rarely has to.
const TIMER_REFRESH_MARGIN: TimeDelta = TimeDelta::minutes(10);
const TIMER_PERIOD: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Delay before offline retry round `n` (1-based): fast at first because the
/// user is usually waiting, then every 5 minutes.
fn offline_delay(round: u32) -> TimeDelta {
    const SECS: [i64; 6] = [2, 5, 10, 30, 60, 120];
    TimeDelta::seconds(SECS.get(round.saturating_sub(1) as usize).copied().unwrap_or(300))
}

/// Retry delays once the server says the account is over a usage limit
/// (Gmail: too many connections, bandwidth): knocking every few seconds keeps
/// the limit hit, so start at 5 minutes and double up to half an hour.
const THROTTLED_FIRST_RETRY: TimeDelta = TimeDelta::minutes(5);
const THROTTLED_MAX_RETRY: TimeDelta = TimeDelta::minutes(30);
/// A limit is lifted only after a quiet while: a throttle within this long of
/// the last one carries on its backoff instead of starting over at 5 minutes.
const THROTTLE_MEMORY: TimeDelta = TimeDelta::minutes(10);

fn throttled_delay(round: u32) -> TimeDelta {
    (THROTTLED_FIRST_RETRY * (1 << round.saturating_sub(1).min(4))).min(THROTTLED_MAX_RETRY)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthState {
    Active,
    /// The server or the token endpoint is unreachable.
    Offline { next_retry: DateTime<Utc> },
    /// The OAuth grant is gone or its token is refused: the user has to log in.
    NeedsReauth,
    /// The server refused the password: the user has to change it.
    CredentialsRejected,
    /// The OAuth provider refused the app itself: a configuration problem.
    OAuthClientInvalid,
}

impl AuthState {
    /// Nothing connects again until the user acts.
    pub fn is_paused(self) -> bool {
        matches!(self, Self::NeedsReauth | Self::CredentialsRejected | Self::OAuthClientInvalid)
    }

    /// The part of the state that survives a restart.
    fn persisted(self) -> AccountStatus {
        match self {
            Self::NeedsReauth => AccountStatus::NeedsReauth,
            Self::CredentialsRejected => AccountStatus::CredentialsRejected,
            _ => AccountStatus::Active,
        }
    }

    fn same_kind(self, other: Self) -> bool {
        std::mem::discriminant(&self) == std::mem::discriminant(&other)
    }
}

/// An account's auth as the UI and the CLI see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthStatus {
    pub account_id: AccountId,
    pub state: AuthState,
    /// Why the account is in `state`; `None` while active.
    pub reason: Option<String>,
    /// The most recent failure, kept after the account recovers.
    pub last_error: Option<String>,
    pub since: DateTime<Utc>,
    pub oauth_provider: Option<OAuthProvider>,
    pub token_expires_at: Option<DateTime<Utc>>,
    /// Why the last SMTP login was refused; sending only, the account's
    /// state is IMAP's.
    pub smtp_error: Option<String>,
}

/// Exchanges a refresh token for fresh tokens at the provider.
pub type RefreshFn =
    Arc<dyn Fn(OAuthProvider, String) -> BoxFuture<'static, Result<OAuthSecrets, DomainError>> + Send + Sync>;

pub enum Credential {
    Password(String),
    OAuth { provider: OAuthProvider, tokens: OAuthSecrets },
}

struct Tracked {
    state: AuthState,
    reason: Option<String>,
    last_error: Option<String>,
    since: DateTime<Utc>,
    offline_rounds: u32,
    throttle_rounds: u32,
    last_throttled: Option<DateTime<Utc>>,
    smtp_error: Option<String>,
}

pub struct AuthGate {
    account_id: AccountId,
    email: String,
    credential: Mutex<Credential>,
    /// One refresh in flight per account; the others wait and reuse its token.
    refreshing: tokio::sync::Mutex<()>,
    tracked: watch::Sender<Tracked>,
    refresh: RefreshFn,
    secret_store: Arc<dyn SecretStore>,
    events: mpsc::UnboundedSender<(AccountId, DomainEvent)>,
    changes: broadcast::Sender<AccountId>,
}

impl AuthGate {
    pub fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub fn email(&self) -> &str {
        &self.email
    }

    pub fn oauth_provider(&self) -> Option<OAuthProvider> {
        match &*self.credential() {
            Credential::OAuth { provider, .. } => Some(*provider),
            Credential::Password(_) => None,
        }
    }

    pub fn status(&self) -> AuthStatus {
        let (oauth_provider, token_expires_at) = match &*self.credential() {
            Credential::OAuth { provider, tokens } => (Some(*provider), Some(tokens.expires_at)),
            Credential::Password(_) => (None, None),
        };
        let tracked = self.tracked.borrow();
        AuthStatus {
            account_id: self.account_id,
            state: tracked.state,
            reason: tracked.reason.clone(),
            last_error: tracked.last_error.clone(),
            since: tracked.since,
            oauth_provider,
            token_expires_at,
            smtp_error: tracked.smtp_error.clone(),
        }
    }

    /// Takes over the state persisted before a restart: an account that was
    /// waiting for the user stays paused, until [`Self::retry_after_restart`].
    pub fn restore(&self, account: &AccountSummary) {
        let oauth = self.oauth_provider().is_some();
        let state = match account.status {
            AccountStatus::NeedsReauth if oauth => AuthState::NeedsReauth,
            // Older builds flagged a rejected password as NeedsReauth; no login fixes that.
            AccountStatus::NeedsReauth | AccountStatus::CredentialsRejected => AuthState::CredentialsRejected,
            _ => return,
        };
        self.tracked.send_modify(|t| {
            t.state = state;
            t.reason = account.status_error.clone();
            t.last_error = account.status_error.clone();
            t.since = account.status_since.unwrap_or(t.since);
        });
    }

    /// The one real attempt a paused account gets per boot: the user may have
    /// fixed things meanwhile (a password imported from config.toml, a grant
    /// that works again). OAuth refreshes once and `login` runs once with the
    /// result. Success resumes the account; a failure leaves it paused.
    pub async fn retry_after_restart<F, Fut>(&self, login: F)
    where
        F: FnOnce(LoginSecret) -> Fut,
        Fut: Future<Output = Result<(), DomainError>>,
    {
        if !self.tracked.borrow().state.is_paused() {
            return;
        }
        let secret = if self.oauth_provider().is_some() {
            let _one = self.refreshing.lock().await;
            match self.refresh_now().await {
                Ok(secret) => secret,
                Err(_) => return, // refresh_now recorded why
            }
        } else {
            match self.current_secret(TimeDelta::zero()) {
                Some(secret) => secret,
                None => return,
            }
        };
        match login(secret).await {
            Ok(()) => {
                info!("{}: login works again after the restart, resuming", self.email);
                self.recover();
            }
            Err(e) => {
                info!("{}: still refused after the restart ({e})", self.email);
                self.tracked.send_modify(|t| t.last_error = Some(e.to_string()));
            }
        }
    }

    /// A fresh OAuth login stored `tokens` in the keychain. Waits out a refresh
    /// in flight and writes them again after it, so its older tokens can't
    /// win in memory or in the keychain.
    pub async fn reauthenticated(&self, tokens: Option<OAuthSecrets>) {
        let _one = self.refreshing.lock().await;
        if let Some(fresh) = tokens {
            self.persist_tokens(&fresh);
            if let Credential::OAuth { tokens, .. } = &mut *self.credential() {
                *tokens = fresh;
            }
        }
        self.recover();
    }

    /// The user stored a new password for the account.
    pub fn set_password(&self, password: String) {
        if let Credential::Password(current) = &mut *self.credential() {
            *current = password;
        }
        self.recover();
    }

    /// A token valid for longer than `margin`, refreshing it when it is not.
    pub async fn valid_secret(&self, margin: TimeDelta) -> Result<LoginSecret, DomainError> {
        if let Some(secret) = self.current_secret(margin) {
            return Ok(secret);
        }
        let _one = self.refreshing.lock().await;
        // Whoever held the lock before us may just have refreshed, or found the
        // grant dead or the endpoint down: then don't ask again.
        if let Some(secret) = self.current_secret(margin) {
            return Ok(secret);
        }
        if let Some(e) = self.paused_error() {
            return Err(e);
        }
        if let AuthState::Offline { next_retry } = self.tracked.borrow().state
            && Utc::now() < next_retry
        {
            return Err(DomainError::ConnectionFailed(format!("offline, next retry at {next_retry}")));
        }
        self.refresh_now().await
    }

    fn credential(&self) -> std::sync::MutexGuard<'_, Credential> {
        self.credential.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn current_secret(&self, margin: TimeDelta) -> Option<LoginSecret> {
        match &*self.credential() {
            Credential::Password(password) => Some(LoginSecret::Password(password.clone())),
            Credential::OAuth { tokens, .. } => (tokens.expires_at - Utc::now() > margin)
                .then(|| LoginSecret::OAuthToken(tokens.access_token.clone())),
        }
    }

    fn paused_error(&self) -> Option<DomainError> {
        let tracked = self.tracked.borrow();
        tracked.state.is_paused().then(|| {
            DomainError::AuthenticationFailed(format!(
                "{} is paused until the user acts: {}",
                self.email,
                tracked.reason.as_deref().unwrap_or("credentials rejected")
            ))
        })
    }

    /// Callers hold `refreshing`.
    async fn refresh_now(&self) -> Result<LoginSecret, DomainError> {
        let (provider, refresh_token) = match &*self.credential() {
            Credential::OAuth { provider, tokens } => (*provider, tokens.refresh_token.clone()),
            Credential::Password(password) => return Ok(LoginSecret::Password(password.clone())),
        };
        if refresh_token.is_empty() {
            let e = DomainError::AuthenticationFailed("no refresh token stored".into());
            self.enter(AuthState::NeedsReauth, "no refresh token stored", &e);
            return Err(e);
        }
        match (self.refresh)(provider, refresh_token).await {
            Ok(fresh) => {
                info!("OAuth token refreshed for {}", self.email);
                self.persist_tokens(&fresh);
                let token = fresh.access_token.clone();
                if let Credential::OAuth { tokens, .. } = &mut *self.credential() {
                    *tokens = fresh;
                }
                // The token endpoint answered: whatever made us offline is over.
                self.clear_offline();
                Ok(LoginSecret::OAuthToken(token))
            }
            Err(e) => {
                match e {
                    DomainError::AuthenticationFailed(_) => {
                        self.enter(AuthState::NeedsReauth, "refresh token revoked or expired", &e)
                    }
                    DomainError::OAuthClientInvalid(_) => self.enter(
                        AuthState::OAuthClientInvalid,
                        "the OAuth provider rejected the app's client id",
                        &e,
                    ),
                    _ => self.went_offline(&e),
                }
                Err(e)
            }
        }
    }

    /// Writes refreshed tokens to the keychain, keeping the bundle's other
    /// secrets. An unreadable entry is left alone rather than overwritten with
    /// only the tokens, which would lose its passwords.
    fn persist_tokens(&self, fresh: &OAuthSecrets) {
        let mut bundle = match self.secret_store.load(&self.email) {
            Ok(existing) => existing.unwrap_or_default(),
            Err(e) => {
                error!("OAuth refresh for {}: keychain read failed, not persisting: {e}", self.email);
                return;
            }
        };
        bundle.oauth = Some(fresh.clone());
        if let Err(e) = self.secret_store.store(&self.email, &bundle) {
            error!("OAuth refresh for {}: keychain write failed: {e}", self.email);
        }
    }

    /// The server refused `rejected`. True when a different token is now
    /// available to retry with.
    async fn refresh_rejected(&self, rejected: &str) -> bool {
        let _one = self.refreshing.lock().await;
        if self.tracked.borrow().state.is_paused() {
            return false;
        }
        let current = match &*self.credential() {
            Credential::OAuth { tokens, .. } => tokens.access_token.clone(),
            Credential::Password(_) => return false,
        };
        if current != rejected {
            return true;
        }
        self.refresh_now().await.is_ok()
    }

    fn enter(&self, state: AuthState, reason: &str, error: &DomainError) {
        warn!("{}: {reason} ({error})", self.email);
        self.transition(state, Some(reason.to_owned()), Some(error.to_string()));
    }

    fn recover(&self) {
        self.transition(AuthState::Active, None, None);
    }

    fn clear_offline(&self) {
        if matches!(self.tracked.borrow().state, AuthState::Offline { .. }) {
            self.recover();
        }
    }

    /// The only place the state changes. Appends the event when the persisted
    /// part changes, inside the watch lock so events keep the order of the
    /// transitions they record.
    fn transition(&self, state: AuthState, reason: Option<String>, error: Option<String>) {
        let changed = self.tracked.send_if_modified(|t| {
            let before = (t.state, t.reason.clone(), t.last_error.clone());
            if !t.state.same_kind(state) {
                t.since = Utc::now();
            }
            let old = std::mem::replace(&mut t.state, state);
            t.reason = reason.clone();
            if error.is_some() {
                t.last_error = error.clone();
            }
            t.offline_rounds = 0;
            if old.persisted() != state.persisted() {
                let event = self.event_for(state, reason.clone().unwrap_or_default());
                // The receiver only goes away at shutdown.
                let _ = self.events.send((self.account_id, event));
            }
            before != (t.state, t.reason.clone(), t.last_error.clone())
        });
        if changed {
            let _ = self.changes.send(self.account_id);
        }
    }

    fn event_for(&self, state: AuthState, error: String) -> DomainEvent {
        let account_id = self.account_id;
        match state.persisted() {
            AccountStatus::NeedsReauth => DomainEvent::AccountAuthExpired { account_id, error },
            AccountStatus::CredentialsRejected => DomainEvent::AccountCredentialsRejected { account_id, error },
            _ => DomainEvent::AccountReauthenticated { account_id },
        }
    }

    fn went_offline(&self, error: &DomainError) {
        warn!("{}: server unreachable ({error})", self.email);
        let throttled = matches!(error, DomainError::Throttled(_));
        let now = Utc::now();
        let changed = self.tracked.send_if_modified(|t| {
            t.last_error = Some(error.to_string());
            match t.state {
                // An attempt that started before the retry was due: same round.
                AuthState::Offline { next_retry } if now < next_retry => {}
                AuthState::Active | AuthState::Offline { .. } => {
                    if t.state == AuthState::Active {
                        t.since = now;
                    }
                    let delay = if throttled {
                        if t.last_throttled.is_none_or(|at| now - at > THROTTLE_MEMORY) {
                            t.throttle_rounds = 0;
                        }
                        t.throttle_rounds += 1;
                        t.last_throttled = Some(now);
                        throttled_delay(t.throttle_rounds)
                    } else {
                        t.offline_rounds += 1;
                        offline_delay(t.offline_rounds)
                    };
                    t.state = AuthState::Offline { next_retry: now + delay };
                    t.reason = Some(if throttled { "over the server's usage limit" } else { "server unreachable" }.into());
                }
                _ => {}
            }
            true
        });
        if changed {
            let _ = self.changes.send(self.account_id);
        }
    }
}

impl AuthGate {
    /// Resolves once the account is offline and its next retry has come.
    pub async fn retry_due(&self) {
        let mut rx = self.tracked.subscribe();
        loop {
            let state = rx.borrow_and_update().state;
            if let AuthState::Offline { next_retry } = state {
                let wait = (next_retry - Utc::now()).to_std().unwrap_or_default();
                tokio::select! {
                    () = tokio::time::sleep(wait) => return,
                    _ = rx.changed() => continue,
                }
            }
            // The sender lives as long as `self`, so this only returns on a change.
            let _ = rx.changed().await;
        }
    }

    /// Resolves the next time the account comes back to `Active` after being
    /// offline or paused.
    pub async fn recovered(&self) {
        let mut rx = self.tracked.subscribe();
        let mut down = rx.borrow_and_update().state != AuthState::Active;
        // The sender lives as long as `self`, so `changed` only returns on a change.
        while rx.changed().await.is_ok() {
            let up = rx.borrow_and_update().state == AuthState::Active;
            if down && up {
                return;
            }
            down = !up;
        }
    }
}

#[async_trait]
impl LoginGate for AuthGate {
    async fn secret(&self) -> Result<LoginSecret, DomainError> {
        if let Some(e) = self.paused_error() {
            return Err(e);
        }
        self.valid_secret(REFRESH_MARGIN).await
    }

    async fn login_failed(&self, secret: &LoginSecret, error: &DomainError, retried: bool) -> bool {
        match (error, secret) {
            (DomainError::AuthenticationFailed(text), LoginSecret::Password(_)) => {
                let reason = format!("the server rejected the password: {text}");
                self.enter(AuthState::CredentialsRejected, &reason, error);
                false
            }
            (DomainError::AuthenticationFailed(_), LoginSecret::OAuthToken(rejected)) if !retried => {
                self.refresh_rejected(rejected).await
            }
            (DomainError::AuthenticationFailed(text), LoginSecret::OAuthToken(_)) => {
                let reason = format!("the server rejects a freshly refreshed token for this account: {text}");
                self.enter(AuthState::NeedsReauth, &reason, error);
                false
            }
            (DomainError::ConnectionFailed(_) | DomainError::ConnectionTimeout(_) | DomainError::Throttled(_), _) => {
                self.went_offline(error);
                false
            }
            _ => false,
        }
    }

    fn login_succeeded(&self) {
        self.clear_offline();
    }

    async fn smtp_token_refused(&self, rejected: &LoginSecret) -> bool {
        // Offline or throttled, a refresh would end the backoff for every loop.
        if matches!(self.tracked.borrow().state, AuthState::Offline { .. }) {
            return false;
        }
        match rejected {
            LoginSecret::OAuthToken(token) => self.refresh_rejected(token).await,
            LoginSecret::Password(_) => false,
        }
    }

    fn smtp_login(&self, refused: Option<String>) {
        if self.tracked.send_if_modified(|t| std::mem::replace(&mut t.smtp_error, refused.clone()) != refused) {
            let _ = self.changes.send(self.account_id);
        }
    }

    async fn ready(&self) {
        let mut rx = self.tracked.subscribe();
        loop {
            let state = rx.borrow_and_update().state;
            match state {
                AuthState::Active => return,
                AuthState::Offline { next_retry } => {
                    let wait = (next_retry - Utc::now()).to_std().unwrap_or_default();
                    tokio::select! {
                        () = tokio::time::sleep(wait) => return,
                        _ = rx.changed() => {}
                    }
                }
                // The sender lives as long as `self`, so this only returns on a change.
                _ => {
                    let _ = rx.changed().await;
                }
            }
        }
    }
}

/// Every account's gate, plus what they share.
pub struct AuthGates {
    gates: HashMap<AccountId, Arc<AuthGate>>,
    refresh: RefreshFn,
    secret_store: Arc<dyn SecretStore>,
    events: mpsc::UnboundedSender<(AccountId, DomainEvent)>,
    changes: broadcast::Sender<AccountId>,
}

impl AuthGates {
    /// Transitions that must survive a restart arrive on `events`, for the
    /// caller to append to the account's stream.
    pub fn new(
        refresh: RefreshFn,
        secret_store: Arc<dyn SecretStore>,
        events: mpsc::UnboundedSender<(AccountId, DomainEvent)>,
    ) -> Self {
        Self {
            gates: HashMap::new(),
            refresh,
            secret_store,
            events,
            changes: broadcast::channel(64).0,
        }
    }

    pub fn add(&mut self, account_id: AccountId, email: &str, credential: Credential) -> Arc<AuthGate> {
        let gate = Arc::new(AuthGate {
            account_id,
            email: email.to_owned(),
            credential: Mutex::new(credential),
            refreshing: tokio::sync::Mutex::new(()),
            tracked: watch::Sender::new(Tracked {
                state: AuthState::Active,
                reason: None,
                last_error: None,
                since: Utc::now(),
                offline_rounds: 0,
                throttle_rounds: 0,
                last_throttled: None,
                smtp_error: None,
            }),
            refresh: self.refresh.clone(),
            secret_store: self.secret_store.clone(),
            events: self.events.clone(),
            changes: self.changes.clone(),
        });
        self.gates.insert(account_id, gate.clone());
        gate
    }

    pub fn get(&self, account_id: &AccountId) -> Option<&Arc<AuthGate>> {
        self.gates.get(account_id)
    }

    pub fn by_email(&self, email: &str) -> Option<&Arc<AuthGate>> {
        self.gates.values().find(|g| g.email == email)
    }

    pub fn statuses(&self) -> Vec<AuthStatus> {
        self.gates.values().map(|g| g.status()).collect()
    }

    /// The id of every account whose status changed; read the status itself
    /// with [`AuthGate::status`], so a lagging receiver never sees a stale one.
    pub fn subscribe(&self) -> broadcast::Receiver<AccountId> {
        self.changes.subscribe()
    }

    /// Keeps OAuth tokens fresh in the background. Logins refresh on their
    /// own; this only makes it rare that one has to wait for it.
    pub fn spawn_refresh_timer(self: &Arc<Self>, shutdown: CancellationToken) {
        let gates = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TIMER_PERIOD);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {}
                }
                for gate in gates.gates.values() {
                    if gate.oauth_provider().is_some() && !gate.tracked.borrow().state.is_paused() {
                        let _ = gate.valid_secret(TIMER_REFRESH_MARGIN).await;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use mc_domain::{login_with, AccountSecrets};
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
                .store(EMAIL, &AccountSecrets { oauth: Some(tokens("old", TimeDelta::zero())), ..Default::default() })
                .unwrap();
            let (tx, events) = mpsc::unbounded_channel();
            Self { gates: AuthGates::new(refresh, keychain.clone(), tx), keychain, refreshes, events }
        }

        fn oauth(&mut self, expires_in: TimeDelta) -> Arc<AuthGate> {
            let credential = Credential::OAuth { provider: OAuthProvider::Google, tokens: tokens("old", expires_in) };
            self.gates.add(AccountId::new(), EMAIL, credential)
        }

        fn password(&mut self) -> Arc<AuthGate> {
            self.gates.add(AccountId::new(), "me@example.com", Credential::Password("hunter2".into()))
        }

        fn refreshes(&self) -> usize {
            self.refreshes.load(Ordering::SeqCst)
        }

        fn event_names(&mut self) -> Vec<&'static str> {
            std::iter::from_fn(|| self.events.try_recv().ok()).map(|(_, e)| e.event_name()).collect()
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
    async fn login(gate: &AuthGate, accepted: &'static str, seen: &Mutex<Vec<String>>) -> Result<(), DomainError> {
        login_with(gate, |secret| async move {
            let token = match secret {
                LoginSecret::OAuthToken(t) | LoginSecret::Password(t) => t,
            };
            seen.lock().unwrap().push(token.clone());
            if token == accepted { Ok(()) } else { Err(rejected()) }
        })
        .await
    }

    #[tokio::test]
    async fn an_expired_token_is_refreshed_before_the_login() {
        let mut h = Harness::new(fresh);
        let gate = h.oauth(TimeDelta::minutes(-30));
        let seen = Mutex::new(vec![]);

        login(&gate, "new", &seen).await.unwrap();

        assert_eq!(*seen.lock().unwrap(), ["new"], "the stale token never reached the server");
        assert_eq!(h.refreshes(), 1);
        assert_eq!(h.keychain.load(EMAIL).unwrap().unwrap().oauth.unwrap().access_token, "new");
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
        assert!(reason.contains("freshly refreshed") && reason.contains("AUTHENTICATIONFAILED"), "{reason}");
        assert_eq!(h.event_names(), ["AccountAuthExpired"]);
    }

    #[tokio::test]
    async fn invalid_grant_needs_reauth_and_keeps_the_keychain() {
        let mut h = Harness::new(revoked);
        let gate = h.oauth(TimeDelta::minutes(-1));
        let before = h.keychain.load(EMAIL).unwrap();

        login(&gate, "new", &Mutex::new(vec![])).await.unwrap_err();

        assert_eq!(gate.status().state, AuthState::NeedsReauth);
        assert_eq!(h.keychain.load(EMAIL).unwrap(), before, "keychain untouched");
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
        gate.login_failed(&LoginSecret::Password("hunter2".into()), &down, false).await;
        tokio::time::timeout(Duration::from_secs(4), waiting).await.expect("woke at the 2s retry").unwrap();
    }

    #[tokio::test]
    async fn smtp_never_refreshes_an_offline_account() {
        let mut h = Harness::new(fresh);
        let gate = h.oauth(TimeDelta::hours(1));
        let busy = DomainError::Throttled("Too many simultaneous connections. (Failure)".into());
        gate.went_offline(&busy);
        let before = gate.status().state;

        assert!(!gate.smtp_token_refused(&LoginSecret::OAuthToken("old".into())).await);

        assert_eq!(h.refreshes(), 0);
        assert_eq!(gate.status().state, before, "still offline, same retry time");
    }

    #[tokio::test]
    async fn a_first_throttle_after_network_errors_starts_at_five_minutes() {
        let mut h = Harness::new(fresh);
        let gate = h.password();
        let secret = LoginSecret::Password("hunter2".into());
        let due = |gate: &AuthGate| gate.tracked.send_modify(|t| {
            if let AuthState::Offline { next_retry } = &mut t.state {
                *next_retry = Utc::now();
            }
        });
        for _ in 0..4 {
            due(&gate);
            gate.login_failed(&secret, &DomainError::ConnectionFailed("down".into()), false).await;
        }
        due(&gate);
        gate.login_failed(&secret, &DomainError::Throttled("busy".into()), false).await;

        let AuthState::Offline { next_retry } = gate.status().state else { panic!("not offline") };
        assert_eq!((next_retry - Utc::now() + TimeDelta::seconds(30)).num_minutes(), 5);
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
            let AuthState::Offline { next_retry } = gate.status().state else { panic!("not offline") };
            delays.push((next_retry - Utc::now() + TimeDelta::seconds(30)).num_minutes());
        }
        assert_eq!(delays, [5, 10, 20, 30, 30]);
        assert_eq!(gate.status().reason.as_deref(), Some("over the server's usage limit"));
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
        assert!(tokio::time::timeout(Duration::from_millis(100), gate.ready()).await.is_err());

        let waiting = tokio::spawn({
            let gate = gate.clone();
            async move { gate.ready().await }
        });
        gate.reauthenticated(Some(tokens("new", TimeDelta::hours(1)))).await;
        tokio::time::timeout(Duration::from_secs(1), waiting).await.unwrap().unwrap();
        login(&gate, "new", &seen).await.unwrap();
        assert_eq!(h.event_names(), ["AccountAuthExpired", "AccountReauthenticated"]);
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
        gate.tracked.send_modify(|t| t.state = AuthState::Offline { next_retry: Utc::now() });

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

        assert_eq!(refreshed.unwrap(), LoginSecret::OAuthToken("new".into()), "the refresh did run");
        assert_eq!(gate.secret().await.unwrap(), LoginSecret::OAuthToken("relogin".into()));
        assert_eq!(h.keychain.load(EMAIL).unwrap().unwrap().oauth, Some(relogin));
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
            status: AccountStatus::CredentialsRejected,
            total_unread: 0,
            status_error: Some("the server rejected the password".into()),
            status_since: None,
        };
        gate.restore(&persisted);
        let attempts = AtomicUsize::new(0);

        gate.retry_after_restart(|_| async { attempts.fetch_add(1, Ordering::SeqCst); Err(rejected()) }).await;
        assert_eq!(gate.status().state, AuthState::CredentialsRejected, "still refused: stays paused");
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
        gate.tracked.send_modify(|t| t.state = AuthState::NeedsReauth);

        gate.retry_after_restart(|secret| async move {
            assert_eq!(secret, LoginSecret::OAuthToken("new".into()), "logs in with the refreshed token");
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
            status: AccountStatus::NeedsReauth,
            total_unread: 0,
            status_error: Some("refresh token revoked or expired".into()),
            status_since: None,
        });

        assert!(gate.secret().await.is_err());
        assert_eq!(gate.status().reason.as_deref(), Some("refresh token revoked or expired"));
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
}
