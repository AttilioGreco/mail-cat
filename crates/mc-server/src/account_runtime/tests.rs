use super::*;
use mc_application::{FeatureState, QueryBus};
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
    ctx.account_runtime
        .dav_discovery
        .store(false, std::sync::atomic::Ordering::Relaxed);
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

#[tokio::test]
async fn restart_brings_the_account_back_with_fresh_tasks_and_keeps_its_data() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start_declared("hot@example.com").await.unwrap();
    give_it_a_mailbox(&f, id).await;
    let rows = rows_of(&f, id).await;
    let (old_stop, old_gate) = (
        runtime.running.lock().unwrap()[&id].stop.clone(),
        runtime.deps.auth_gates.get(&id).unwrap(),
    );
    old_gate.task_failed("watcher stopped".into());

    runtime.restart(id).await.unwrap();

    assert!(old_stop.is_cancelled());
    let new_stop = runtime.running.lock().unwrap()[&id].stop.clone();
    assert!(!new_stop.is_cancelled(), "fresh tasks");
    assert!(f.ctx.provider_registry.get_provider(&id).is_ok());
    let gate = runtime.deps.auth_gates.get(&id).unwrap();
    assert!(!Arc::ptr_eq(&gate, &old_gate));
    assert_eq!(
        gate.status().task_error,
        None,
        "the crash report is cleared"
    );
    assert_eq!(rows_of(&f, id).await, rows, "data untouched");
    assert_eq!(account_count(&f).await, 1);
}

#[tokio::test]
async fn restart_keeps_the_throttle_backoff() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start_declared("hot@example.com").await.unwrap();
    let old_gate = runtime.deps.auth_gates.get(&id).unwrap();
    let busy = DomainError::Throttled("Too many simultaneous connections.".into());
    old_gate
        .login_failed(&mc_domain::LoginSecret::Password("pw".into()), &busy, false)
        .await;
    let before = old_gate.status();
    assert!(matches!(
        before.state,
        mc_application::AuthState::Offline { .. }
    ));

    runtime.restart(id).await.unwrap();

    let gate = runtime.deps.auth_gates.get(&id).unwrap();
    assert!(!Arc::ptr_eq(&gate, &old_gate));
    let after = gate.status();
    assert_eq!(after.state, before.state, "same next_retry");
    assert_eq!(after.reason, before.reason);
    assert_eq!(after.since, before.since);
}

#[tokio::test]
async fn restart_of_an_unknown_or_undeclared_account_changes_nothing() {
    let f = fixture(ACCOUNT_TOML).await;
    let runtime = &f.ctx.account_runtime;
    assert!(matches!(
        runtime.restart(AccountId::new()).await,
        Err(ApplicationError::NotFound(..))
    ));

    let id = runtime.start_declared("hot@example.com").await.unwrap();
    std::fs::write(&runtime.deps.config_path, "").unwrap();
    assert!(runtime.restart(id).await.is_err());
    assert!(runtime.is_running(&id), "still running as before");
}

fn plan(
    config: &str,
    scopes: &[&str],
    dav: Option<mc_domain::model::account::DavConfig>,
) -> tasks::DavSetup {
    let account: AccountConfig = toml::from_str::<ServerConfig>(config)
        .unwrap()
        .accounts
        .remove(0);
    let secrets = AccountSecrets {
        oauth: Some(mc_domain::OAuthSecrets {
            access_token: "a".into(),
            refresh_token: "r".into(),
            expires_at: Utc::now(),
            scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
        }),
        ..Default::default()
    };
    let gate = Arc::new(mc_domain::LoginSecret::Password("pw".into()));
    tasks::dav_setup(&account, &secrets, AccountId::new(), dav.as_ref(), gate).unwrap()
}

fn oauth_toml(provider: &str) -> String {
    format!("{ACCOUNT_TOML}oauth_provider = \"{provider}\"\n")
}

#[test]
fn dav_provisioning_decision_table() {
    use mc_infrastructure::oauth::{GOOGLE_CALENDAR, GOOGLE_CARDDAV};
    let google = oauth_toml("google");
    let url = |u: &str| mc_domain::model::account::DavConfig {
        carddav: Some(u.parse().unwrap()),
        caldav: None,
        ..Default::default()
    };

    let both = [GOOGLE_CARDDAV, GOOGLE_CALENDAR];
    let state = |setup: &tasks::DavSetup| match setup {
        tasks::DavSetup::Known(_, s) | tasks::DavSetup::None(s) => {
            Some((s.contacts.state, s.calendar.state))
        }
        tasks::DavSetup::Discover(_) => None,
    };
    let (on, missing, off) = (
        FeatureState::Enabled,
        FeatureState::MissingPermission,
        FeatureState::Unavailable,
    );

    assert_eq!(state(&plan(&google, &both, None)), Some((on, on)));
    assert_eq!(
        state(&plan(&google, &[GOOGLE_CARDDAV], None)),
        Some((on, missing))
    );
    assert_eq!(
        state(&plan(&google, &[GOOGLE_CALENDAR], None)),
        Some((missing, on))
    );
    let none = plan(&google, &[], None);
    assert!(matches!(none, tasks::DavSetup::None(_)));
    assert_eq!(state(&none), Some((missing, missing)));
    let tasks::DavSetup::None(none) = none else {
        unreachable!()
    };
    assert_eq!(none.calendar.missing_scopes, [GOOGLE_CALENDAR]);

    assert_eq!(
        state(&plan(
            ACCOUNT_TOML,
            &[],
            Some(url("https://dav.example.com/"))
        )),
        Some((on, off)),
        "a generic account serves what it has an endpoint for"
    );
    assert_eq!(
        state(&plan(ACCOUNT_TOML, &[], None)),
        None,
        "discovery decides"
    );
    assert_eq!(
        state(&plan(ACCOUNT_TOML, &[], Some(Default::default()))),
        None
    );

    assert_eq!(
        state(&plan(&oauth_toml("microsoft"), &both, None)),
        Some((off, off))
    );
}

#[tokio::test]
async fn a_google_account_without_dav_scopes_keeps_mail_and_reports_it() {
    let f = fixture("").await;
    let runtime = &f.ctx.account_runtime;
    let mut account = hot_account();
    account.oauth_provider = Some(crate::config::OAuthProviderConfig::Google);
    let id = runtime.start(&account).await.unwrap();

    let status = runtime.deps.auth_gates.get(&id).unwrap().status();

    assert_eq!(status.dav.contacts.state, FeatureState::MissingPermission);
    assert_eq!(status.dav.calendar.state, FeatureState::MissingPermission);
    // The account's state is IMAP's to judge (this fixture can never log in,
    // so it is not asserted); missing DAV scopes only must not stop the mail.
    assert!(
        f.ctx.provider_registry.get_provider(&id).is_ok(),
        "mail keeps working"
    );
    assert!(
        runtime.deps.dav.sync_account(&id).await.is_err(),
        "no provider"
    );
}

/// Answers every listing like a DAV server that is throttling.
struct Failing {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl Failing {
    async fn fail(&self) -> DomainError {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        DomainError::Throttled("slow down".into())
    }
}

#[async_trait::async_trait]
impl mc_domain::ContactsProvider for Failing {
    async fn list_address_books(&self) -> Result<Vec<mc_domain::AddressBook>, DomainError> {
        Err(self.fail().await)
    }
    async fn address_book_changes(
        &self,
        _: &mc_domain::CollectionId,
        _: Option<&mc_domain::SyncCursor>,
    ) -> Result<mc_domain::CollectionChanges, DomainError> {
        unreachable!()
    }
    async fn fetch_contacts(
        &self,
        _: &mc_domain::CollectionId,
        _: &[mc_domain::Href],
    ) -> Result<Vec<mc_domain::RemoteResource>, DomainError> {
        unreachable!()
    }
    async fn put_contact(
        &self,
        _: &mc_domain::CollectionId,
        _: mc_domain::ResourcePut,
    ) -> Result<mc_domain::PutOutcome, DomainError> {
        unreachable!()
    }
    async fn delete_contact(
        &self,
        _: &mc_domain::CollectionId,
        _: &mc_domain::Href,
        _: &mc_domain::Etag,
    ) -> Result<mc_domain::DeleteOutcome, DomainError> {
        unreachable!()
    }
}

#[async_trait::async_trait]
impl mc_domain::CalendarProvider for Failing {
    async fn list_calendars(&self) -> Result<Vec<mc_domain::Calendar>, DomainError> {
        Err(self.fail().await)
    }
    async fn calendar_changes(
        &self,
        _: &mc_domain::CollectionId,
        _: Option<&mc_domain::SyncCursor>,
    ) -> Result<mc_domain::CollectionChanges, DomainError> {
        unreachable!()
    }
    async fn fetch_events(
        &self,
        _: &mc_domain::CollectionId,
        _: &[mc_domain::Href],
    ) -> Result<Vec<mc_domain::RemoteResource>, DomainError> {
        unreachable!()
    }
    async fn put_event(
        &self,
        _: &mc_domain::CollectionId,
        _: mc_domain::ResourcePut,
    ) -> Result<mc_domain::PutOutcome, DomainError> {
        unreachable!()
    }
    async fn delete_event(
        &self,
        _: &mc_domain::CollectionId,
        _: &mc_domain::Href,
        _: &mc_domain::Etag,
    ) -> Result<mc_domain::DeleteOutcome, DomainError> {
        unreachable!()
    }
    async fn subscribe(&self, _: &str) -> Result<mc_domain::Subscribed, DomainError> {
        unreachable!()
    }
    async fn unsubscribe(&self, _: &mc_domain::CollectionId) -> Result<(), DomainError> {
        unreachable!()
    }
}

/// Starts the DAV loop of a running account on `backend`.
fn start_dav_loop(f: &Fixture, id: AccountId, backend: Arc<dyn mc_application::DavBackend>) {
    let runtime = &f.ctx.account_runtime;
    let running = runtime.running.lock().unwrap();
    let ctx = TaskCtx {
        id,
        name: "Hot".into(),
        gate: runtime.deps.auth_gates.get(&id).unwrap(),
        stop: running[&id].stop.clone(),
        tasks: running[&id].tasks.clone(),
    };
    runtime.spawn_dav_task(ctx, backend, mc_application::DavStatus::all_enabled(), None);
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..100 {
        if done() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn the_dav_backoff_doubles_up_to_half_an_hour() {
    let mins = |n| tasks::dav_delay(n).as_secs() / 60;
    assert_eq!([0, 1, 2, 3, 9].map(mins), [5, 10, 20, 30, 30]);
}

#[tokio::test]
async fn the_dav_loop_runs_once_throttled_backs_off_and_stops_with_the_account() {
    let f = fixture("").await;
    let runtime = &f.ctx.account_runtime;
    let id = runtime.start(&hot_account()).await.unwrap();
    let gate = runtime.deps.auth_gates.get(&id).unwrap();
    assert!(
        matches!(
            runtime.deps.dav.sync_account(&id).await,
            Err(ApplicationError::NotFound(..))
        ),
        "no provider, no task"
    );
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let backend = Failing {
        calls: calls.clone(),
    };
    let seen = || calls.load(std::sync::atomic::Ordering::SeqCst);
    gate.login_succeeded();

    start_dav_loop(&f, id, Arc::new(backend));
    until("the first DAV pass", || seen() >= 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // One pass lists books, then the throttle stops it before calendars.
    assert_eq!(seen(), 1, "the throttled loop must not retry at once");

    assert!(runtime.stop(&id).await, "the loop ends with its account");
    assert!(matches!(
        runtime.deps.dav.sync_account(&id).await,
        Err(ApplicationError::NotFound(..))
    ));
}
