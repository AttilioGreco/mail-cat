//! Manual wakes of the DAV loop, against a backend that counts its passes.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::time::Duration;
use tokio::sync::Semaphore;

struct NoSecrets;

impl SecretStore for NoSecrets {
    fn load(&self, _: &str) -> Result<Option<AccountSecrets>, DomainError> {
        Ok(None)
    }
    fn store(&self, _: &str, _: &AccountSecrets) -> Result<(), DomainError> {
        Ok(())
    }
    fn delete(&self, _: &str) -> Result<(), DomainError> {
        Ok(())
    }
}

/// Every pass lists the address books once, so `passes` counts passes. A pass
/// holds one `turns` permit while it runs, which lets a test keep one in flight.
struct Counting {
    passes: AtomicUsize,
    active: AtomicUsize,
    most_active: AtomicUsize,
    fails: AtomicBool,
    turns: Semaphore,
}

impl Counting {
    fn new(fails: bool, turns: usize) -> Arc<Self> {
        Arc::new(Self {
            passes: 0.into(),
            active: 0.into(),
            most_active: 0.into(),
            fails: fails.into(),
            turns: Semaphore::new(turns),
        })
    }

    fn passes(&self) -> usize {
        self.passes.load(SeqCst)
    }
}

#[async_trait::async_trait]
impl mc_domain::ContactsProvider for Counting {
    async fn list_address_books(&self) -> Result<Vec<mc_domain::AddressBook>, DomainError> {
        self.passes.fetch_add(1, SeqCst);
        let now = self.active.fetch_add(1, SeqCst) + 1;
        self.most_active.fetch_max(now, SeqCst);
        self.turns.acquire().await.unwrap().forget();
        self.active.fetch_sub(1, SeqCst);
        match self.fails.load(SeqCst) {
            true => Err(DomainError::Throttled("slow down".into())),
            false => Ok(vec![]),
        }
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
impl mc_domain::CalendarProvider for Counting {
    async fn list_calendars(&self) -> Result<Vec<mc_domain::Calendar>, DomainError> {
        Ok(vec![])
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
}

struct Fixture {
    runtime: Arc<AccountRuntime>,
    id: AccountId,
    gate: Arc<AuthGate>,
    _ctx: crate::wiring::AppContext,
    _dir: tempfile::TempDir,
}

/// A running account whose DAV loop talks to `backend`.
async fn running(backend: Arc<Counting>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "").unwrap();
    let config = ServerConfig {
        grpc_addr: "127.0.0.1:0".into(),
        db_path: dir.path().join("mail-cat.db").display().to_string(),
        index_path: dir.path().join("index").display().to_string(),
        accounts: vec![],
        ui_bin: "mail-cat-ui".into(),
        log_level: None,
    };
    let secrets: Arc<dyn SecretStore> = Arc::new(NoSecrets);
    let oauth = Arc::new(crate::oauth_login::OAuthChallengeManager::new(
        config_path.clone(),
        secrets.clone(),
    ));
    let ctx =
        crate::wiring::AppContext::new(&config, &config_path.display().to_string(), secrets, oauth)
            .await
            .unwrap();
    let runtime = ctx.account_runtime.clone();
    runtime.dav_discovery.store(false, SeqCst);
    let account = toml::from_str::<ServerConfig>(
        r#"
[[accounts]]
name = "Hot"
email = "hot@example.com"
provider_type = "imap"
imap_host = "127.0.0.1"
imap_port = 1
use_tls = false
username = "hot"
"#,
    )
    .unwrap()
    .accounts
    .remove(0);
    let id = runtime.start(&account).await.unwrap();
    let gate = runtime.deps.auth_gates.get(&id).unwrap();
    gate.login_succeeded();
    {
        let running = runtime.running.lock().unwrap();
        let task = TaskCtx {
            id,
            name: "Hot".into(),
            gate: gate.clone(),
            stop: running[&id].stop.clone(),
            tasks: running[&id].tasks.clone(),
        };
        runtime.spawn_dav_task(
            task,
            backend,
            mc_application::DavStatus::all_enabled(),
            None,
        );
    }
    Fixture {
        runtime,
        id,
        gate,
        _ctx: ctx,
        _dir: dir,
    }
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    // The account's own mail sync fails against the dead address and takes the
    // gate offline for a couple of seconds, which holds the DAV loop too.
    for _ in 0..500 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_wake_ends_the_sleep_and_runs_a_pass_at_once() {
    let backend = Counting::new(false, 100);
    let f = running(backend.clone()).await;
    until("the first pass", || backend.passes() == 1).await;
    until("its end", || f.gate.status().dav_pass.last_sync.is_some()).await;
    assert!(!f.gate.status().dav_pass.running);
    assert_eq!(f.gate.status().dav_pass.last_error, None);

    f.runtime.deps.dav_providers.wake(Some(f.id));

    until("the woken pass", || backend.passes() == 2).await;
}

#[tokio::test]
async fn a_wake_skips_the_failure_backoff_and_the_error_is_shown() {
    let backend = Counting::new(true, 100);
    let f = running(backend.clone()).await;
    until("the failed pass", || {
        f.gate.status().dav_pass.last_error.is_some()
    })
    .await;
    assert_eq!(backend.passes(), 1);

    f.runtime.deps.dav_providers.wake(None);

    until("a pass inside the backoff", || backend.passes() == 2).await;
}

#[tokio::test]
async fn wakes_during_a_pass_collapse_into_one_more_pass() {
    let backend = Counting::new(false, 0);
    let f = running(backend.clone()).await;
    until("the pass to start", || f.gate.status().dav_pass.running).await;

    for _ in 0..10 {
        f.runtime.deps.dav_providers.wake(Some(f.id));
    }
    backend.turns.add_permits(100);
    until("the extra pass", || backend.passes() == 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(backend.passes(), 2, "a burst yields one pass, not ten");
    assert_eq!(backend.most_active.load(SeqCst), 1, "never two at once");
    assert!(!f.gate.status().dav_pass.running);
}
