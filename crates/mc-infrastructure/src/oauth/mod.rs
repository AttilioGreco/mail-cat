//! OAuth2 authorization-code + PKCE flow for Google/Microsoft, used to
//! authenticate on the generic IMAP/SMTP pipeline via SASL XOAUTH2.

pub mod loopback;
mod presets;

pub use presets::{provider_preset, ProviderPreset};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use mc_domain::{DomainError, OAuthProvider, OAuthSecrets};
use oauth2::basic::BasicClient;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, RefreshToken, Scope, TokenResponse, TokenUrl,
};
use tokio::net::TcpListener;

/// Holds everything needed to complete a login once the browser redirects
/// back: the bound loopback listener, the PKCE verifier, and the CSRF token
/// to check the redirect against.
pub struct PendingLogin {
    client: BasicClient,
    pkce_verifier: PkceCodeVerifier,
    csrf_token: CsrfToken,
    listener: TcpListener,
}

/// Binds the loopback listener and builds the provider's authorization URL.
/// Returns the URL to show the user plus the handle needed to complete it.
pub async fn begin_authorization(
    provider: OAuthProvider,
) -> Result<(String, PendingLogin), DomainError> {
    let preset = provider_preset(provider);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| DomainError::Provider(format!("failed to bind OAuth loopback listener: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| DomainError::Provider(format!("failed to read loopback port: {e}")))?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");

    let client = BasicClient::new(
        ClientId::new(preset.client_id()),
        preset.client_secret().map(ClientSecret::new),
        AuthUrl::new(preset.auth_url.to_string())
            .map_err(|e| DomainError::Provider(format!("invalid auth url: {e}")))?,
        Some(
            TokenUrl::new(preset.token_url.to_string())
                .map_err(|e| DomainError::Provider(format!("invalid token url: {e}")))?,
        ),
    )
    .set_redirect_uri(
        RedirectUrl::new(redirect_uri)
            .map_err(|e| DomainError::Provider(format!("invalid redirect url: {e}")))?,
    );

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let mut request = client
        .authorize_url(CsrfToken::new_random)
        .set_pkce_challenge(pkce_challenge);
    for scope in preset.scopes {
        request = request.add_scope(Scope::new((*scope).to_string()));
    }
    for (k, v) in preset.auth_extra_params {
        request = request.add_extra_param(*k, *v);
    }
    let (authorize_url, csrf_token) = request.url();

    Ok((
        authorize_url.to_string(),
        PendingLogin { client, pkce_verifier, csrf_token, listener },
    ))
}

/// Awaits the one redirect on the loopback listener, exchanges the code for
/// tokens. Validates the CSRF state before exchanging anything.
pub async fn complete_authorization(pending: PendingLogin) -> Result<OAuthSecrets, DomainError> {
    let (code, state) = loopback::accept_one_redirect(&pending.listener).await?;
    if state != *pending.csrf_token.secret() {
        return Err(DomainError::Provider("OAuth state mismatch — possible CSRF".into()));
    }

    let token = pending
        .client
        .exchange_code(AuthorizationCode::new(code))
        .set_pkce_verifier(pending.pkce_verifier)
        .request_async(oauth2::reqwest::async_http_client)
        .await
        .map_err(|e| DomainError::Provider(format!("OAuth token exchange failed: {e}")))?;

    token_response_to_secrets(&token)
}

/// Exchanges a refresh token for a fresh access token.
pub async fn refresh(provider: OAuthProvider, refresh_token: &str) -> Result<OAuthSecrets, DomainError> {
    let preset = provider_preset(provider);
    let client = BasicClient::new(
        ClientId::new(preset.client_id()),
        preset.client_secret().map(ClientSecret::new),
        AuthUrl::new(preset.auth_url.to_string())
            .map_err(|e| DomainError::Provider(format!("invalid auth url: {e}")))?,
        Some(
            TokenUrl::new(preset.token_url.to_string())
                .map_err(|e| DomainError::Provider(format!("invalid token url: {e}")))?,
        ),
    );

    let token = client
        .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
        .request_async(oauth2::reqwest::async_http_client)
        .await
        .map_err(classify_refresh_error)?;

    let mut secrets = token_response_to_secrets(&token)?;
    if secrets.refresh_token.is_empty() {
        // Some providers omit refresh_token on refresh responses; keep the old one.
        secrets.refresh_token = refresh_token.to_string();
    }
    Ok(secrets)
}

/// Distinguishes a dead refresh token (revoked/invalid — the account needs a
/// full user re-auth) from a transient failure (network hiccup, timeout,
/// provider 5xx — safe to retry next cycle).
fn classify_refresh_error<RE: std::error::Error + 'static>(
    e: oauth2::RequestTokenError<RE, oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>>,
) -> DomainError {
    match e {
        oauth2::RequestTokenError::ServerResponse(resp)
            if matches!(resp.error(), oauth2::basic::BasicErrorResponseType::InvalidGrant) =>
        {
            DomainError::AuthenticationFailed(format!("refresh token invalid or revoked: {resp}"))
        }
        other => DomainError::Provider(format!("OAuth token refresh failed: {other}")),
    }
}

fn token_response_to_secrets<TR: TokenResponse<oauth2::basic::BasicTokenType>>(
    token: &TR,
) -> Result<OAuthSecrets, DomainError> {
    let expires_in = token.expires_in().unwrap_or(std::time::Duration::from_secs(3600));
    let expires_at: DateTime<Utc> =
        Utc::now() + ChronoDuration::from_std(expires_in).unwrap_or(ChronoDuration::hours(1));
    Ok(OAuthSecrets {
        access_token: token.access_token().secret().clone(),
        refresh_token: token.refresh_token().map(|t| t.secret().clone()).unwrap_or_default(),
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_authorization_builds_google_url_with_scope_and_pkce() {
        let (url, _pending) = begin_authorization(OAuthProvider::Google).await.unwrap();
        assert!(url.starts_with("https://accounts.google.com/o/oauth2/v2/auth"));
        assert!(url.contains("code_challenge"));
        assert!(url.contains("mail.google.com"));
        // Without these Google returns no refresh token and the account has to
        // re-authenticate every time the 1h access token dies.
        assert!(url.contains("access_type=offline"));
        assert!(url.contains("prompt=consent"));
    }

    #[test]
    fn classify_invalid_grant_as_authentication_failed() {
        let resp = oauth2::StandardErrorResponse::new(
            oauth2::basic::BasicErrorResponseType::InvalidGrant,
            None,
            None,
        );
        let err: oauth2::RequestTokenError<
            std::io::Error,
            oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>,
        > = oauth2::RequestTokenError::ServerResponse(resp);

        let classified = classify_refresh_error(err);

        assert!(matches!(classified, DomainError::AuthenticationFailed(_)));
    }

    #[test]
    fn classify_other_server_response_as_provider_error() {
        let resp = oauth2::StandardErrorResponse::new(
            oauth2::basic::BasicErrorResponseType::InvalidRequest,
            None,
            None,
        );
        let err: oauth2::RequestTokenError<
            std::io::Error,
            oauth2::StandardErrorResponse<oauth2::basic::BasicErrorResponseType>,
        > = oauth2::RequestTokenError::ServerResponse(resp);

        let classified = classify_refresh_error(err);

        assert!(matches!(classified, DomainError::Provider(_)));
    }
}
