use super::*;
use mc_application::QueryBus;
use mc_domain::{AccountSecrets, ProviderRegistry};
use std::collections::HashMap;

#[derive(Default)]
struct MemoryStore(Mutex<HashMap<String, AccountSecrets>>);

impl SecretStore for MemoryStore {
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

struct Fixture {
    ctx: crate::wiring::AppContext,
    _dir: tempfile::TempDir,
}

const ACCOUNT_TOML: &str = r#"
[[accounts]]
name = "Hot"
email = "hot@example.com"
provider_type = "imap"
imap_host = "127.0.0.1"
imap_port = 1
use_tls = false
username = "hot"
"#;

async fn fixture(accounts_toml: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, accounts_toml).unwrap();
    let config = ServerConfig {
        grpc_addr: "127.0.0.1:0".into(),
        db_path: dir.path().join("mail-cat.db").display().to_string(),
        index_path: dir.path().join("index").display().to_string(),
        accounts: vec![],
        ui_bin: "mail-cat-ui".into(),
        log_level: None,
    };
    let secrets: Arc<dyn SecretStore> = Arc::new(MemoryStore::default());
    let path = config_path.display().to_string();
    let oauth = Arc::new(crate::oauth_login::OAuthChallengeManager::new(
        config_path.clone(),
        secrets.clone(),
    ));
    let ctx = crate::wiring::AppContext::new(&config, &path, secrets, oauth)
        .await
        .unwrap();
    Fixture { ctx, _dir: dir }
}

fn hot_account() -> AccountConfig {
    toml::from_str::<ServerConfig>(ACCOUNT_TOML)
        .unwrap()
        .accounts
        .remove(0)
}

async fn account_count(f: &Fixture) -> usize {
    let query_bus = &f.ctx.account_runtime.deps.query_bus;
    let accounts = query_bus
        .dispatch(mc_application::Query::ListAccounts)
        .await;
    accounts.unwrap().into_accounts().unwrap().len()
}

#[tokio::test]
async fn start_brings_the_account_up_and_is_idempotent() {
    let f = fixture("").await;
    let runtime = &f.ctx.account_runtime;

    let id = runtime.start(&hot_account()).await.unwrap();

    assert!(runtime.is_running(&id));
    assert_eq!(runtime.name(&id).as_deref(), Some("Hot"));
    assert!(f.ctx.provider_registry.get_provider(&id).is_ok());
    let gate = runtime.deps.auth_gates.get(&id).expect("gate registered");

    assert_eq!(runtime.start(&hot_account()).await.unwrap(), id);
    assert_eq!(account_count(&f).await, 1, "no duplicate aggregate");
    assert!(Arc::ptr_eq(
        &gate,
        &runtime.deps.auth_gates.get(&id).unwrap()
    ));
}

#[tokio::test]
async fn stop_cancels_the_tasks_and_unregisters_provider_and_gate() {
    let f = fixture("").await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start(&hot_account()).await.unwrap();
    let stop = runtime.running.lock().unwrap()[&id].stop.clone();
    assert!(!stop.is_cancelled());

    assert!(runtime.stop(&id));

    assert!(stop.is_cancelled());
    assert!(!runtime.is_running(&id));
    assert!(f.ctx.provider_registry.get_provider(&id).is_err());
    assert!(runtime.deps.auth_gates.get(&id).is_none());
    assert!(!runtime.stop(&id), "second stop is a no-op");
    assert!(
        !f.ctx.shutdown.is_cancelled(),
        "other accounts keep running"
    );

    assert_eq!(runtime.start(&hot_account()).await.unwrap(), id);
    assert!(runtime.is_running(&id), "a stopped account can start again");
}

#[tokio::test]
async fn shutdown_stops_every_account_task() {
    let f = fixture("").await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start(&hot_account()).await.unwrap();
    let stop = runtime.running.lock().unwrap()[&id].stop.clone();

    f.ctx.shutdown.cancel();

    assert!(stop.is_cancelled());
}

#[tokio::test]
async fn start_declared_reads_config_and_creates_the_aggregate() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;

    let id = runtime.start_declared("hot@example.com").await.unwrap();

    assert!(runtime.is_running(&id));
    assert_eq!(account_count(&f).await, 1);
    let err = runtime
        .start_declared("nobody@example.com")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not declared"), "{err}");
}

#[tokio::test]
async fn a_failed_start_leaves_nothing_behind() {
    let f = fixture("").await;
    let runtime = &f.ctx.account_runtime;
    let mut broken = hot_account();
    broken.imap_host.clear();

    let err = runtime.start(&broken).await.unwrap_err();

    assert!(
        format!("{err:#}").contains("neither imap_host nor jmap_url"),
        "{err:#}"
    );
    assert!(
        runtime
            .deps
            .auth_gates
            .by_email("hot@example.com")
            .is_none()
    );
    assert!(runtime.running.lock().unwrap().is_empty());
}
