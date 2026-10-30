use super::*;

fn test_connection_config() -> ConnectionConfig {
    ConnectionConfig {
        incoming: IncomingServer::Imap {
            host: "imap.example.com".into(),
            port: 993,
            use_tls: true,
        },
        auth: AuthConfig::Plain {
            username: "user@example.com".into(),
            password: "secret".into(),
        },
        smtp: None,
    }
}

fn test_identity() -> Identity {
    Identity {
        display_name: "Test User".into(),
        email: EmailAddress::new("user@example.com").unwrap(),
        signature_html: None,
        signature_text: None,
    }
}

#[test]
fn create_account_produces_event() {
    let (account, event) = Account::create(
        "Work".into(),
        EmailAddress::new("work@example.com").unwrap(),
        ProviderType::Imap,
        test_connection_config(),
        test_identity(),
    );

    assert_eq!(account.name, "Work");
    assert_eq!(account.status, AccountStatus::Active);
    assert_eq!(event.event_name(), "AccountAdded");
}

#[test]
fn apply_update_changes_name() {
    let (mut account, _) = Account::create(
        "Old Name".into(),
        EmailAddress::new("test@example.com").unwrap(),
        ProviderType::Imap,
        test_connection_config(),
        test_identity(),
    );

    let event = account.apply_update(Some("New Name".into()), None);
    assert_eq!(account.name, "New Name");
    assert_eq!(event.event_name(), "AccountUpdated");
}

#[test]
fn remove_produces_event() {
    let (account, _) = Account::create(
        "Test".into(),
        EmailAddress::new("test@example.com").unwrap(),
        ProviderType::Imap,
        test_connection_config(),
        test_identity(),
    );

    let event = account.remove();
    assert_eq!(event.event_name(), "AccountRemoved");
}

#[test]
fn rehydrate_account_from_events() {
    let events = vec![
        DomainEvent::AccountAdded {
            account_id: AccountId::new(),
            name: "Original".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        },
        DomainEvent::AccountUpdated {
            account_id: AccountId::new(),
            name: Some("Updated Name".into()),
            connection_config: None,
        },
    ];

    let account = Account::rehydrate(&events).unwrap();
    assert_eq!(account.name, "Updated Name");
    assert_eq!(account.status, AccountStatus::Active);
}

#[test]
fn rehydrate_removed_account() {
    let id = AccountId::new();
    let events = vec![
        DomainEvent::AccountAdded {
            account_id: id,
            name: "Test".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        },
        DomainEvent::AccountRemoved { account_id: id },
    ];

    let account = Account::rehydrate(&events).unwrap();
    assert_eq!(account.status, AccountStatus::Disabled);
}

#[test]
fn rehydrate_account_needs_reauth() {
    let id = AccountId::new();
    let events = vec![
        DomainEvent::AccountAdded {
            account_id: id,
            name: "Test".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        },
        DomainEvent::AccountAuthExpired {
            account_id: id,
            error: "invalid_grant".into(),
        },
    ];

    let account = Account::rehydrate(&events).unwrap();
    assert_eq!(account.status, AccountStatus::NeedsReauth);
}

#[test]
fn reauthenticated_returns_account_to_active() {
    let id = AccountId::new();
    let events = vec![
        DomainEvent::AccountAdded {
            account_id: id,
            name: "Test".into(),
            email: EmailAddress::new("test@example.com").unwrap(),
            provider_type: ProviderType::Imap,
        },
        DomainEvent::AccountAuthExpired {
            account_id: id,
            error: "invalid_grant".into(),
        },
        DomainEvent::AccountReauthenticated { account_id: id },
    ];

    let account = Account::rehydrate(&events).unwrap();
    assert_eq!(account.status, AccountStatus::Active);
}

#[test]
fn rehydrate_empty_events_returns_none() {
    let result = Account::rehydrate(&[]);
    assert!(result.is_none());
}

#[test]
fn account_serialization_roundtrip() {
    let (account, _) = Account::create(
        "Serialize Test".into(),
        EmailAddress::new("ser@example.com").unwrap(),
        ProviderType::Jmap,
        test_connection_config(),
        test_identity(),
    );

    let json = serde_json::to_string(&account).unwrap();
    let deserialized: Account = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.name, "Serialize Test");
    assert_eq!(deserialized.provider_type, ProviderType::Jmap);
}

#[test]
fn oauth2_variant_carries_provider() {
    let auth = AuthConfig::OAuth2 {
        provider: OAuthProvider::Google,
        access_token: "at".into(),
        refresh_token: "rt".into(),
        expires_at: Utc::now(),
    };
    match auth {
        AuthConfig::OAuth2 { provider, .. } => assert_eq!(provider, OAuthProvider::Google),
        _ => panic!("expected OAuth2 variant"),
    }
}

#[test]
fn mailbox_model_defaults_by_provider_and_changes_once() {
    use crate::model::account::MailboxModel;
    assert_eq!(
        MailboxModel::default_for(ProviderType::Imap),
        MailboxModel::Folders
    );
    assert_eq!(
        MailboxModel::default_for(ProviderType::Jmap),
        MailboxModel::Labels
    );

    let (mut account, added) = Account::create(
        "n".into(),
        EmailAddress::new("a@example.com").unwrap(),
        ProviderType::Jmap,
        ConnectionConfig {
            incoming: IncomingServer::Jmap {
                session_url: "https://x".into(),
            },
            auth: AuthConfig::Plain {
                username: "u".into(),
                password: String::new(),
            },
            smtp: None,
        },
        Identity {
            display_name: "n".into(),
            email: EmailAddress::new("a@example.com").unwrap(),
            signature_html: None,
            signature_text: None,
        },
    );
    assert_eq!(account.mailbox_model, MailboxModel::Labels);
    // Choosing the current model is still recorded, so a default never overrides it.
    assert!(matches!(
        account.set_mailbox_model(MailboxModel::Labels),
        DomainEvent::AccountMailboxModelChanged {
            model: MailboxModel::Labels,
            ..
        }
    ));
    let event = account.set_mailbox_model(MailboxModel::Folders);
    let rebuilt = Account::rehydrate(&[added, event]).unwrap();
    assert_eq!(rebuilt.mailbox_model, MailboxModel::Folders);
}
