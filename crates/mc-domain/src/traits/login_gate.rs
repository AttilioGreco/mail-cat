//! Port: the single owner of an account's credential and auth state.
//!
//! Every IMAP and SMTP login asks the gate for the secret to present and
//! reports how the login went, so refresh-before-connect, the retry after a
//! rejected token and the account's auth state live in one place instead of in
//! every loop that happens to connect.

use std::future::Future;

use async_trait::async_trait;

use crate::error::DomainError;

/// What a login presents to the server.
#[derive(Clone, PartialEq, Eq)]
pub enum LoginSecret {
    Password(String),
    OAuthToken(String),
}

// Hand-written so a secret can never end up in a log line.
impl std::fmt::Debug for LoginSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Password(_) => "Password(..)",
            Self::OAuthToken(_) => "OAuthToken(..)",
        })
    }
}

// async_trait marks its boxed futures #[must_use] twice.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait LoginGate: Send + Sync {
    /// The secret for the next login. Refreshes an OAuth token that is expired
    /// or about to be, and fails without touching the server while the account
    /// waits for the user.
    async fn secret(&self) -> Result<LoginSecret, DomainError>;

    /// Reports a failed login with `secret`. `true` means a fresh secret is
    /// available and the login should be retried once.
    async fn login_failed(&self, secret: &LoginSecret, error: &DomainError, retried: bool) -> bool;

    fn login_succeeded(&self);

    /// How the account's last SMTP login went: why it was refused, or `None`
    /// once one is accepted. Shown to the user only: IMAP alone judges the
    /// account's credential, so this never pauses anything.
    fn smtp_login(&self, _refused: Option<String>) {}

    /// SMTP refused this OAuth token, which IMAP shares. `true` when a fresh
    /// one is available to retry the send with; never changes the account's
    /// state, and never refreshes while the account is offline.
    async fn smtp_token_refused(&self, _rejected: &LoginSecret) -> bool {
        false
    }

    /// Resolves when background work may connect: at once while healthy, at the
    /// next scheduled retry while offline, after the user acts while paused.
    async fn ready(&self);
}

/// A fixed secret with no state of its own, for one-off logins such as
/// verifying a credential before it is stored.
#[async_trait]
impl LoginGate for LoginSecret {
    async fn secret(&self) -> Result<LoginSecret, DomainError> {
        Ok(self.clone())
    }

    async fn login_failed(&self, _: &LoginSecret, _: &DomainError, _: bool) -> bool {
        false
    }

    fn login_succeeded(&self) {}

    async fn ready(&self) {}
}

/// Runs one login through `gate`, retrying once when the gate obtained a fresh
/// token after the server rejected the previous one.
pub async fn login_with<T, F, Fut>(gate: &dyn LoginGate, mut login: F) -> Result<T, DomainError>
where
    F: FnMut(LoginSecret) -> Fut,
    Fut: Future<Output = Result<T, DomainError>>,
{
    let mut retried = false;
    loop {
        let secret = gate.secret().await?;
        match login(secret.clone()).await {
            Ok(session) => {
                gate.login_succeeded();
                return Ok(session);
            }
            Err(e) => {
                if !gate.login_failed(&secret, &e, retried).await || retried {
                    return Err(e);
                }
                retried = true;
            }
        }
    }
}
