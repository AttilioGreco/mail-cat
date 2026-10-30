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
use std::sync::{Arc, Mutex, PoisonError, RwLock};

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
    TimeDelta::seconds(
        SECS.get(round.saturating_sub(1) as usize)
            .copied()
            .unwrap_or(300),
    )
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
    Offline {
        next_retry: DateTime<Utc>,
    },
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
        matches!(
            self,
            Self::NeedsReauth | Self::CredentialsRejected | Self::OAuthClientInvalid
        )
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
pub type RefreshFn = Arc<
    dyn Fn(OAuthProvider, String) -> BoxFuture<'static, Result<OAuthSecrets, DomainError>>
        + Send
        + Sync,
>;

pub enum Credential {
    Password(String),
    OAuth {
        provider: OAuthProvider,
        tokens: OAuthSecrets,
    },
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
            AccountStatus::NeedsReauth | AccountStatus::CredentialsRejected => {
                AuthState::CredentialsRejected
            }
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
                info!(
                    "{}: login works again after the restart, resuming",
                    self.email
                );
                self.recover();
            }
            Err(e) => {
                info!("{}: still refused after the restart ({e})", self.email);
                self.tracked
                    .send_modify(|t| t.last_error = Some(e.to_string()));
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
            return Err(DomainError::ConnectionFailed(format!(
                "offline, next retry at {next_retry}"
            )));
        }
        self.refresh_now().await
    }

    fn credential(&self) -> std::sync::MutexGuard<'_, Credential> {
        self.credential
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
                    DomainError::AuthenticationFailed(_) => self.enter(
                        AuthState::NeedsReauth,
                        "refresh token revoked or expired",
                        &e,
                    ),
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
                error!(
                    "OAuth refresh for {}: keychain read failed, not persisting: {e}",
                    self.email
                );
                return;
            }
        };
        bundle.oauth = Some(fresh.clone());
        if let Err(e) = self.secret_store.store(&self.email, &bundle) {
            error!(
                "OAuth refresh for {}: keychain write failed: {e}",
                self.email
            );
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
            AccountStatus::CredentialsRejected => {
                DomainEvent::AccountCredentialsRejected { account_id, error }
            }
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
                    t.state = AuthState::Offline {
                        next_retry: now + delay,
                    };
                    t.reason = Some(
                        if throttled {
                            "over the server's usage limit"
                        } else {
                            "server unreachable"
                        }
                        .into(),
                    );
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
            (DomainError::AuthenticationFailed(_), LoginSecret::OAuthToken(rejected))
                if !retried =>
            {
                self.refresh_rejected(rejected).await
            }
            (DomainError::AuthenticationFailed(text), LoginSecret::OAuthToken(_)) => {
                let reason = format!(
                    "the server rejects a freshly refreshed token for this account: {text}"
                );
                self.enter(AuthState::NeedsReauth, &reason, error);
                false
            }
            (
                DomainError::ConnectionFailed(_)
                | DomainError::ConnectionTimeout(_)
                | DomainError::Throttled(_),
                _,
            ) => {
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
        if self
            .tracked
            .send_if_modified(|t| std::mem::replace(&mut t.smtp_error, refused.clone()) != refused)
        {
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
    /// Accounts come and go while the server runs.
    gates: RwLock<HashMap<AccountId, Arc<AuthGate>>>,
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
            gates: RwLock::default(),
            refresh,
            secret_store,
            events,
            changes: broadcast::channel(64).0,
        }
    }

    pub fn add(&self, account_id: AccountId, email: &str, credential: Credential) -> Arc<AuthGate> {
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
        self.gates.write().unwrap().insert(account_id, gate.clone());
        gate
    }

    pub fn remove(&self, account_id: &AccountId) -> Option<Arc<AuthGate>> {
        self.gates.write().unwrap().remove(account_id)
    }

    pub fn get(&self, account_id: &AccountId) -> Option<Arc<AuthGate>> {
        self.gates.read().unwrap().get(account_id).cloned()
    }

    pub fn by_email(&self, email: &str) -> Option<Arc<AuthGate>> {
        self.gates
            .read()
            .unwrap()
            .values()
            .find(|g| g.email == email)
            .cloned()
    }

    pub fn statuses(&self) -> Vec<AuthStatus> {
        self.gates
            .read()
            .unwrap()
            .values()
            .map(|g| g.status())
            .collect()
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
                // Snapshot per tick: the lock must not be held across a refresh.
                let live: Vec<_> = gates.gates.read().unwrap().values().cloned().collect();
                for gate in live {
                    if gate.oauth_provider().is_some() && !gate.tracked.borrow().state.is_paused() {
                        let _ = gate.valid_secret(TIMER_REFRESH_MARGIN).await;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests;
