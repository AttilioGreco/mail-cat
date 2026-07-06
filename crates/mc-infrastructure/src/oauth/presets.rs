//! Per-provider OAuth2 endpoints, scopes, and IMAP/SMTP connection presets.
//!
//! One shared "mail-cat" OAuth app per provider (client_id — and for Google,
//! client_secret — registered once by the maintainer, not per-user). Override
//! via env var for local dev; the compiled-in defaults are empty until set.

use mc_domain::OAuthProvider;

pub struct ProviderPreset {
    pub auth_url: &'static str,
    pub token_url: &'static str,
    pub scopes: &'static [&'static str],
    pub imap_host: &'static str,
    pub imap_port: u16,
    pub smtp_host: &'static str,
    pub smtp_port: u16,
    client_id_env: &'static str,
    client_secret_env: Option<&'static str>,
}

impl ProviderPreset {
    pub fn client_id(&self) -> String {
        std::env::var(self.client_id_env).unwrap_or_default()
    }

    pub fn client_secret(&self) -> Option<String> {
        let env = self.client_secret_env?;
        Some(std::env::var(env).unwrap_or_default())
    }
}

pub fn provider_preset(provider: OAuthProvider) -> ProviderPreset {
    match provider {
        OAuthProvider::Google => ProviderPreset {
            auth_url: "https://accounts.google.com/o/oauth2/v2/auth",
            token_url: "https://oauth2.googleapis.com/token",
            scopes: &["https://mail.google.com/"],
            imap_host: "imap.gmail.com",
            imap_port: 993,
            smtp_host: "smtp.gmail.com",
            smtp_port: 587,
            client_id_env: "MAILCAT_GOOGLE_CLIENT_ID",
            client_secret_env: Some("MAILCAT_GOOGLE_CLIENT_SECRET"),
        },
        OAuthProvider::Microsoft => ProviderPreset {
            auth_url: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
            token_url: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
            scopes: &[
                "https://outlook.office.com/IMAP.AccessAsUser.All",
                "https://outlook.office.com/SMTP.Send",
                "offline_access",
            ],
            imap_host: "outlook.office365.com",
            imap_port: 993,
            smtp_host: "smtp.office365.com",
            smtp_port: 587,
            client_id_env: "MAILCAT_MICROSOFT_CLIENT_ID",
            client_secret_env: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_preset_has_gmail_hosts() {
        let preset = provider_preset(OAuthProvider::Google);
        assert_eq!(preset.imap_host, "imap.gmail.com");
        assert_eq!(preset.smtp_host, "smtp.gmail.com");
    }

    #[test]
    fn microsoft_preset_has_no_client_secret() {
        let preset = provider_preset(OAuthProvider::Microsoft);
        assert!(preset.client_secret().is_none());
    }
}
