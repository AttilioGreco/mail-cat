use super::datetime_to_timestamp;
use crate::grpc::proto::{
    self, AccountDetail, AccountStatus as ProtoAccountStatus, AccountSummary, AppPasswordAuth,
    AuthConfig as ProtoAuthConfig, ConnectionConfig as ProtoConnectionConfig, OAuth2Auth,
    PlainAuth, ProviderType as ProtoProviderType,
};
use mc_domain::model::account::{
    Account, AccountStatus as DomainAccountStatus, AuthConfig as DomainAuthConfig,
    ConnectionConfig as DomainConnectionConfig, ProviderType as DomainProviderType,
};

/// Convert domain ProviderType to proto ProviderType.
pub fn domain_provider_type_to_proto(provider: &DomainProviderType) -> i32 {
    match provider {
        DomainProviderType::Imap => ProtoProviderType::Imap as i32,
        DomainProviderType::GmailApi => ProtoProviderType::GmailApi as i32,
        DomainProviderType::OutlookApi => ProtoProviderType::OutlookApi as i32,
    }
}

/// Convert proto ProviderType to domain ProviderType.
pub fn proto_provider_type_to_domain(provider: i32) -> Result<DomainProviderType, String> {
    match ProtoProviderType::try_from(provider).ok() {
        Some(ProtoProviderType::Imap) => Ok(DomainProviderType::Imap),
        Some(ProtoProviderType::GmailApi) => Ok(DomainProviderType::GmailApi),
        Some(ProtoProviderType::OutlookApi) => Ok(DomainProviderType::OutlookApi),
        _ => Err(format!("Invalid provider type: {}", provider)),
    }
}

/// Convert domain AccountStatus to proto AccountStatus.
pub fn domain_account_status_to_proto(status: &DomainAccountStatus) -> i32 {
    match status {
        DomainAccountStatus::Active => ProtoAccountStatus::Active as i32,
        DomainAccountStatus::Disabled => ProtoAccountStatus::Disabled as i32,
        DomainAccountStatus::Error => ProtoAccountStatus::Error as i32,
    }
}

/// Convert proto AccountStatus to domain AccountStatus.
pub fn proto_account_status_to_domain(status: i32) -> Result<DomainAccountStatus, String> {
    match ProtoAccountStatus::try_from(status).ok() {
        Some(ProtoAccountStatus::Active) => Ok(DomainAccountStatus::Active),
        Some(ProtoAccountStatus::Disabled) => Ok(DomainAccountStatus::Disabled),
        Some(ProtoAccountStatus::Error) => Ok(DomainAccountStatus::Error),
        _ => Err(format!("Invalid account status: {}", status)),
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
        Some(proto::auth_config::AuthType::Oauth2(oauth2)) => {
            use chrono::{DateTime, Utc};
            let expires_at =
                DateTime::from_timestamp(oauth2.expires_at, 0).unwrap_or_else(Utc::now);
            Ok(DomainAuthConfig::OAuth2 {
                access_token: oauth2.access_token.clone(),
                refresh_token: oauth2.refresh_token.clone(),
                expires_at,
            })
        }
        Some(proto::auth_config::AuthType::AppPassword(app)) => Ok(DomainAuthConfig::AppPassword {
            username: app.username.clone(),
            app_password: app.app_password.clone(),
        }),
        None => Err("Missing auth type".to_string()),
    }
}

/// Convert domain ConnectionConfig to proto ConnectionConfig.
pub fn domain_connection_to_proto(config: &DomainConnectionConfig) -> ProtoConnectionConfig {
    ProtoConnectionConfig {
        imap_host: config.imap_host.clone(),
        imap_port: config.imap_port.map(|p| p as u32),
        smtp_host: config.smtp_host.clone(),
        smtp_port: config.smtp_port.map(|p| p as u32),
        use_tls: config.use_tls,
        auth: Some(domain_auth_to_proto(&config.auth)),
    }
}

/// Convert proto ConnectionConfig to domain ConnectionConfig.
pub fn proto_connection_to_domain(
    config: &ProtoConnectionConfig,
) -> Result<DomainConnectionConfig, String> {
    let auth = config
        .auth
        .as_ref()
        .ok_or_else(|| "Missing auth config".to_string())?;

    Ok(DomainConnectionConfig {
        imap_host: config.imap_host.clone(),
        imap_port: config.imap_port.map(|p| p as u16),
        use_tls: config.use_tls,
        auth: proto_auth_to_domain(auth)?,
        smtp: None, // SMTP config is managed via server config file, not proto
        smtp_host: config.smtp_host.clone(),
        smtp_port: config.smtp_port.map(|p| p as u16),
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
    }
}
