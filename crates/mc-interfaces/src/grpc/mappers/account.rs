use super::datetime_to_timestamp;
use crate::grpc::proto::{
    self, AccountDetail, AccountStatus as ProtoAccountStatus, AccountSummary, AppPasswordAuth,
    AuthConfig as ProtoAuthConfig, ConnectionConfig as ProtoConnectionConfig, ImapServer,
    IncomingServer as ProtoIncomingServer, JmapServer, OAuth2Auth, PlainAuth,
    ProviderType as ProtoProviderType, SmtpServer,
};
use mc_domain::model::account::{
    Account, AccountStatus as DomainAccountStatus, AuthConfig as DomainAuthConfig,
    ConnectionConfig as DomainConnectionConfig, IncomingServer, ProviderType as DomainProviderType,
    SmtpConnectionConfig, SmtpSecurity,
};

/// Convert domain ProviderType to proto ProviderType.
pub fn domain_provider_type_to_proto(provider: &DomainProviderType) -> i32 {
    match provider {
        DomainProviderType::Imap => ProtoProviderType::Imap as i32,
        DomainProviderType::Jmap => ProtoProviderType::Jmap as i32,
    }
}

/// Convert proto ProviderType to domain ProviderType.
pub fn proto_provider_type_to_domain(provider: i32) -> Result<DomainProviderType, String> {
    match ProtoProviderType::try_from(provider).ok() {
        Some(ProtoProviderType::Imap) => Ok(DomainProviderType::Imap),
        Some(ProtoProviderType::Jmap) => Ok(DomainProviderType::Jmap),
        _ => Err(format!("Invalid provider type: {}", provider)),
    }
}

/// Convert domain AccountStatus to proto AccountStatus.
pub fn domain_account_status_to_proto(status: &DomainAccountStatus) -> i32 {
    match status {
        DomainAccountStatus::Active => ProtoAccountStatus::Active as i32,
        DomainAccountStatus::Disabled => ProtoAccountStatus::Disabled as i32,
        DomainAccountStatus::Error => ProtoAccountStatus::Error as i32,
        DomainAccountStatus::NeedsReauth => ProtoAccountStatus::NeedsReauth as i32,
        DomainAccountStatus::CredentialsRejected => ProtoAccountStatus::CredentialsRejected as i32,
    }
}

/// Convert proto AccountStatus to domain AccountStatus.
pub fn proto_account_status_to_domain(status: i32) -> Result<DomainAccountStatus, String> {
    match ProtoAccountStatus::try_from(status).ok() {
        Some(ProtoAccountStatus::Active) => Ok(DomainAccountStatus::Active),
        Some(ProtoAccountStatus::Disabled) => Ok(DomainAccountStatus::Disabled),
        Some(ProtoAccountStatus::Error) => Ok(DomainAccountStatus::Error),
        Some(ProtoAccountStatus::NeedsReauth) => Ok(DomainAccountStatus::NeedsReauth),
        Some(ProtoAccountStatus::CredentialsRejected) => {
            Ok(DomainAccountStatus::CredentialsRejected)
        }
        _ => Err(format!("Invalid account status: {}", status)),
    }
}

/// Convert proto OAuthProvider to domain OAuthProvider.
pub fn proto_oauth_provider_to_domain(provider: i32) -> Result<mc_domain::OAuthProvider, String> {
    match proto::OAuthProvider::try_from(provider).ok() {
        Some(proto::OAuthProvider::OauthProviderGoogle) => Ok(mc_domain::OAuthProvider::Google),
        Some(proto::OAuthProvider::OauthProviderMicrosoft) => {
            Ok(mc_domain::OAuthProvider::Microsoft)
        }
        _ => Err(format!("Invalid OAuth provider: {}", provider)),
    }
}

/// Convert domain AuthConfig to proto AuthConfig.
pub fn domain_auth_to_proto(auth: &DomainAuthConfig) -> ProtoAuthConfig {
    let auth_type = match auth {
        DomainAuthConfig::Plain { username, password } => {
            Some(proto::auth_config::AuthType::Plain(PlainAuth {
                username: username.clone(),
                password: password.clone(),
            }))
        }
        DomainAuthConfig::OAuth2 {
            provider: _,
            access_token,
            refresh_token,
            expires_at,
        } => Some(proto::auth_config::AuthType::Oauth2(OAuth2Auth {
            client_id: String::new(),     // Not stored in domain model
            client_secret: String::new(), // Not stored in domain model
            refresh_token: refresh_token.clone(),
            access_token: access_token.clone(),
            expires_at: expires_at.timestamp(),
        })),
        DomainAuthConfig::AppPassword {
            username,
            app_password,
        } => Some(proto::auth_config::AuthType::AppPassword(AppPasswordAuth {
            username: username.clone(),
            app_password: app_password.clone(),
        })),
    };

    ProtoAuthConfig { auth_type }
}

/// Convert proto AuthConfig to domain AuthConfig.
pub fn proto_auth_to_domain(auth: &ProtoAuthConfig) -> Result<DomainAuthConfig, String> {
    match &auth.auth_type {
        Some(proto::auth_config::AuthType::Plain(plain)) => Ok(DomainAuthConfig::Plain {
            username: plain.username.clone(),
            password: plain.password.clone(),
        }),
        Some(proto::auth_config::AuthType::Oauth2(_)) => {
            Err("OAuth2Auth via AddAccount/UpdateAccount is not supported — the proto message carries no provider field; use the StartOAuthLogin RPC instead".to_string())
        }
        Some(proto::auth_config::AuthType::AppPassword(app)) => Ok(DomainAuthConfig::AppPassword {
            username: app.username.clone(),
            app_password: app.app_password.clone(),
        }),
        None => Err("Missing auth type".to_string()),
    }
}

fn domain_incoming_to_proto(incoming: &IncomingServer) -> ProtoIncomingServer {
    let kind = match incoming {
        IncomingServer::Imap {
            host,
            port,
            use_tls,
        } => proto::incoming_server::Kind::Imap(ImapServer {
            host: host.clone(),
            port: u32::from(*port),
            use_tls: *use_tls,
        }),
        IncomingServer::Jmap { session_url } => proto::incoming_server::Kind::Jmap(JmapServer {
            session_url: session_url.clone(),
        }),
    };
    ProtoIncomingServer { kind: Some(kind) }
}

fn proto_incoming_to_domain(incoming: &ProtoIncomingServer) -> Result<IncomingServer, String> {
    match &incoming.kind {
        Some(proto::incoming_server::Kind::Imap(imap)) => Ok(IncomingServer::Imap {
            host: imap.host.clone(),
            port: u16::try_from(imap.port)
                .map_err(|_| format!("Invalid IMAP port: {}", imap.port))?,
            use_tls: imap.use_tls,
        }),
        Some(proto::incoming_server::Kind::Jmap(jmap)) => Ok(IncomingServer::Jmap {
            session_url: jmap.session_url.clone(),
        }),
        None => Err("Missing incoming server".to_string()),
    }
}

/// Convert domain ConnectionConfig to proto ConnectionConfig.
pub fn domain_connection_to_proto(config: &DomainConnectionConfig) -> ProtoConnectionConfig {
    ProtoConnectionConfig {
        incoming: Some(domain_incoming_to_proto(&config.incoming)),
        auth: Some(domain_auth_to_proto(&config.auth)),
        smtp: config.smtp.as_ref().map(|smtp| SmtpServer {
            host: smtp.host.clone(),
            port: u32::from(smtp.port),
        }),
    }
}

/// Convert proto ConnectionConfig to domain ConnectionConfig.
pub fn proto_connection_to_domain(
    config: &ProtoConnectionConfig,
) -> Result<DomainConnectionConfig, String> {
    let incoming = config
        .incoming
        .as_ref()
        .ok_or_else(|| "Missing incoming server".to_string())?;
    let auth = config
        .auth
        .as_ref()
        .ok_or_else(|| "Missing auth config".to_string())?;
    let smtp = config
        .smtp
        .as_ref()
        .map(|smtp| {
            let port = u16::try_from(smtp.port)
                .map_err(|_| format!("Invalid SMTP port: {}", smtp.port))?;
            Ok::<_, String>(SmtpConnectionConfig {
                host: smtp.host.clone(),
                port,
                // 465 is implicit TLS; everything else (587, 25) negotiates with STARTTLS.
                security: if port == 465 {
                    SmtpSecurity::Smtps
                } else {
                    SmtpSecurity::StartTls
                },
                username: None,
                password: None,
            })
        })
        .transpose()?;

    Ok(DomainConnectionConfig {
        incoming: proto_incoming_to_domain(incoming)?,
        auth: proto_auth_to_domain(auth)?,
        smtp,
    })
}

/// Convert domain AccountSummary to proto AccountSummary.
pub fn domain_account_summary_to_proto(summary: &mc_domain::AccountSummary) -> AccountSummary {
    use chrono::Utc;

    AccountSummary {
        id: summary.id.to_string(),
        name: summary.name.clone(),
        email: Some(proto::EmailAddress {
            email: summary.email.as_str().to_string(),
            name: None,
        }),
        provider_type: domain_provider_type_to_proto(&summary.provider_type),
        status: domain_account_status_to_proto(&summary.status),
        // AccountSummary doesn't have timestamps, use current time as placeholder
        created_at: Some(datetime_to_timestamp(&Utc::now())),
        updated_at: Some(datetime_to_timestamp(&Utc::now())),
        auth: None,
        mailbox_model: domain_mailbox_model_to_proto(summary.mailbox_model),
        supports_tags: summary.supports_tags,
    }
}

pub fn domain_mailbox_model_to_proto(model: mc_domain::MailboxModel) -> i32 {
    match model {
        mc_domain::MailboxModel::Folders => proto::MailboxModel::Folders as i32,
        mc_domain::MailboxModel::Labels => proto::MailboxModel::Labels as i32,
    }
}

pub fn proto_mailbox_model_to_domain(model: i32) -> Result<mc_domain::MailboxModel, String> {
    match proto::MailboxModel::try_from(model).ok() {
        Some(proto::MailboxModel::Folders) => Ok(mc_domain::MailboxModel::Folders),
        Some(proto::MailboxModel::Labels) => Ok(mc_domain::MailboxModel::Labels),
        _ => Err(format!("invalid mailbox model: {model}")),
    }
}

pub fn auth_status_to_proto(status: &mc_application::AuthStatus) -> proto::AccountAuthStatus {
    use mc_application::AuthState;

    let (state, next_retry) = match status.state {
        AuthState::Active => (proto::AuthState::Active, None),
        AuthState::Offline { next_retry } => (proto::AuthState::Offline, Some(next_retry)),
        AuthState::NeedsReauth => (proto::AuthState::NeedsReauth, None),
        AuthState::CredentialsRejected => (proto::AuthState::CredentialsRejected, None),
        AuthState::OAuthClientInvalid => (proto::AuthState::OauthClientInvalid, None),
    };
    proto::AccountAuthStatus {
        account_id: status.account_id.to_string(),
        state: state as i32,
        reason: status.reason.clone(),
        last_error: status.last_error.clone(),
        since: Some(datetime_to_timestamp(&status.since)),
        next_retry: next_retry.as_ref().map(datetime_to_timestamp),
        oauth_provider: match status.oauth_provider {
            Some(mc_domain::OAuthProvider::Google) => proto::OAuthProvider::OauthProviderGoogle,
            Some(mc_domain::OAuthProvider::Microsoft) => {
                proto::OAuthProvider::OauthProviderMicrosoft
            }
            None => proto::OAuthProvider::OauthProviderUnspecified,
        } as i32,
        token_expires_at: status.token_expires_at.as_ref().map(datetime_to_timestamp),
        smtp_error: status.smtp_error.clone(),
    }
}

/// Convert domain AccountSummary to proto AccountDetail (without connection_config).
pub fn domain_account_summary_to_detail(summary: &mc_domain::AccountSummary) -> AccountDetail {
    use chrono::Utc;

    AccountDetail {
        id: summary.id.to_string(),
        name: summary.name.clone(),
        email: Some(proto::EmailAddress {
            email: summary.email.as_str().to_string(),
            name: None,
        }),
        provider_type: domain_provider_type_to_proto(&summary.provider_type),
        connection_config: None, // Not available in AccountSummary
        status: domain_account_status_to_proto(&summary.status),
        created_at: Some(datetime_to_timestamp(&Utc::now())),
        updated_at: Some(datetime_to_timestamp(&Utc::now())),
        mailbox_model: domain_mailbox_model_to_proto(summary.mailbox_model),
        supports_tags: summary.supports_tags,
    }
}

/// Convert domain Account to proto AccountDetail.
pub fn domain_account_to_detail(account: &Account) -> AccountDetail {
    AccountDetail {
        id: account.id.to_string(),
        name: account.name.clone(),
        email: Some(proto::EmailAddress {
            email: account.email.as_str().to_string(),
            name: None,
        }),
        provider_type: domain_provider_type_to_proto(&account.provider_type),
        connection_config: Some(domain_connection_to_proto(&account.connection_config)),
        status: domain_account_status_to_proto(&account.status),
        created_at: Some(datetime_to_timestamp(&account.created_at)),
        updated_at: Some(datetime_to_timestamp(&account.updated_at)),
        mailbox_model: domain_mailbox_model_to_proto(account.mailbox_model),
        supports_tags: None,
    }
}
