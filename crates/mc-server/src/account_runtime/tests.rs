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

    assert!(runtime.stop(&id).await);

    assert!(stop.is_cancelled());
    assert!(!runtime.is_running(&id));
    assert!(f.ctx.provider_registry.get_provider(&id).is_err());
    assert!(runtime.deps.auth_gates.get(&id).is_none());
    assert!(!runtime.stop(&id).await, "second stop is a no-op");
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

/// Rows of the account in the event log and the read model.
async fn rows_of(f: &Fixture, id: AccountId) -> (i64, i64, i64) {
    f.ctx
        .account_runtime
        .deps
        .db
        .read(move |c| {
            let count = |sql: &str, p: &dyn rusqlite::ToSql| {
                c.query_row(sql, [p], |r| r.get::<_, i64>(0)).unwrap()
            };
            Ok((
                count(
                    "SELECT count(*) FROM events WHERE account_id = ?1",
                    &id.to_string(),
                ),
                count("SELECT count(*) FROM accounts WHERE id = ?1", id.as_uuid()),
                count(
                    "SELECT count(*) FROM mailboxes WHERE account_id = ?1",
                    id.as_uuid(),
                ),
            ))
        })
        .await
        .unwrap()
}

async fn give_it_a_mailbox(f: &Fixture, id: AccountId) {
    let deps = &f.ctx.account_runtime.deps;
    let mailbox_id = mc_domain::MailboxId::new();
    let event = mc_domain::DomainEvent::MailboxDiscovered {
        mailbox_id,
        account_id: id,
        path: mc_domain::MailboxPath::inbox(),
        display_path: "INBOX".into(),
        mailbox_type: mc_domain::MailboxType::Inbox,
    };
    use mc_domain::{EventStore, ReadModelProjector};
    deps.event_store
        .append(
            &mailbox_id.to_string(),
            mc_domain::AggregateType::Mailbox,
            0,
            vec![event],
        )
        .await
        .unwrap();
    let envelope = deps
        .event_store
        .load_events(&mailbox_id.to_string(), 0)
        .await
        .unwrap()
        .remove(0);
    deps.repository.project(&envelope).await.unwrap();
}

fn declared_in(f: &Fixture, email: &str) -> bool {
    let path = &f.ctx.account_runtime.deps.config_path;
    ServerConfig::load_or_default(path)
        .accounts
        .iter()
        .any(|a| a.email == email)
}

#[tokio::test]
async fn remove_stops_the_account_deletes_its_data_and_a_re_add_starts_clean() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start_declared("hot@example.com").await.unwrap();
    give_it_a_mailbox(&f, id).await;
    let stop = runtime.running.lock().unwrap()[&id].stop.clone();
    let rows = rows_of(&f, id).await;
    assert!(rows.0 > 1 && rows.2 == 1, "{rows:?}");

    runtime.remove(id).await.unwrap();

    assert!(stop.is_cancelled());
    assert!(!runtime.is_running(&id));
    assert!(f.ctx.provider_registry.get_provider(&id).is_err());
    assert!(runtime.deps.auth_gates.get(&id).is_none());
    assert_eq!(rows_of(&f, id).await, (0, 0, 0));
    assert_eq!(
        account_count(&f).await,
        0,
        "removing the last account works"
    );
    assert!(!declared_in(&f, "hot@example.com"));
    assert!(
        runtime
            .deps
            .repository
            .pending_removals()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        runtime.remove(id).await,
        Err(ApplicationError::NotFound(..))
    ));

    let fresh = runtime.start(&hot_account()).await.unwrap();
    assert_ne!(fresh, id, "a new aggregate, not the old one back");
    assert!(runtime.is_running(&fresh));
    assert_eq!(account_count(&f).await, 1);
    assert_eq!(rows_of(&f, id).await, (0, 0, 0));
    assert_eq!(rows_of(&f, fresh).await.2, 0, "no mailboxes carried over");
}

#[tokio::test]
async fn a_removal_the_server_died_in_is_finished_at_the_next_boot() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start_declared("hot@example.com").await.unwrap();
    give_it_a_mailbox(&f, id).await;
    // The crash: removal recorded, config entry and data still there.
    runtime.stop(&id).await;
    runtime
        .deps
        .command_bus
        .dispatch(Command::RemoveAccount { account_id: id })
        .await
        .unwrap();
    assert!(declared_in(&f, "hot@example.com"));

    assert_eq!(runtime.recover_removals().await.unwrap(), 1);

    assert_eq!(rows_of(&f, id).await, (0, 0, 0));
    assert!(!declared_in(&f, "hot@example.com"));
    assert_eq!(runtime.recover_removals().await.unwrap(), 0);
}

#[tokio::test]
async fn a_leftover_removal_does_not_touch_an_account_re_added_under_the_same_address() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;
    let old = runtime.start_declared("hot@example.com").await.unwrap();
    runtime.stop(&old).await;
    runtime
        .deps
        .command_bus
        .dispatch(Command::RemoveAccount { account_id: old })
        .await
        .unwrap();
    let new = runtime.start(&hot_account()).await.unwrap();

    runtime.recover_removals().await.unwrap();

    assert_eq!(rows_of(&f, old).await.0, 0);
    assert!(rows_of(&f, new).await.0 > 0);
    assert!(
        declared_in(&f, "hot@example.com"),
        "the new account stays declared"
    );
}

#[tokio::test]
async fn stop_returns_only_after_the_account_tasks_finished() {
    let f = fixture("").await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start(&hot_account()).await.unwrap();
    let (stop, tasks) = {
        let running = runtime.running.lock().unwrap();
        (running[&id].stop.clone(), running[&id].tasks.clone())
    };
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = finished.clone();
    tasks.spawn(async move {
        stop.cancelled().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    runtime.stop(&id).await;

    assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn remove_waits_for_a_sync_in_flight_and_leaves_nothing_behind() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start_declared("hot@example.com").await.unwrap();
    give_it_a_mailbox(&f, id).await;
    // A sync a client asked for, still running when the removal begins.
    let in_flight = runtime.deps.sync.exclusive(&id).await;

    let removal = runtime.remove(id);
    tokio::pin!(removal);
    let waited = tokio::time::timeout(std::time::Duration::from_millis(200), &mut removal).await;
    assert!(waited.is_err(), "must wait for the running sync");
    assert!(rows_of(&f, id).await.0 > 0, "nothing purged under it");

    drop(in_flight);
    removal.await.unwrap();

    assert_eq!(rows_of(&f, id).await, (0, 0, 0));
    let late = runtime.deps.sync.start_sync(&id).await;
    assert!(late.is_err(), "a sync arriving later finds no provider");
    assert_eq!(rows_of(&f, id).await, (0, 0, 0));
}
