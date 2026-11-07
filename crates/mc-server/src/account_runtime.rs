//! Everything that lives per account while the server runs: the auth gate, the
//! provider in the registry, the sync loop, the body backfill and the IDLE/SSE
//! watcher. Boot starts every configured account through [`AccountRuntime::start`];
//! an account added later takes the same path, so it never needs a restart.

use anyhow::Context;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use mc_application::{
    AccountLifecycle, AccountProvisioner, ApplicationError, AuthGate, AuthGates, Backoff, Command,
    CommandBus, CommandResult, Credential,
};
use mc_domain::model::account::{IncomingServer, SmtpConnectionConfig, SmtpSecurity};
use mc_domain::{
    AccountId, AccountReadRepository, AccountSecrets, AuthConfig, ConnectionConfig, DomainError,
    EmailAddress, Identity, LoginGate, ProviderRegistry, SecretStore,
};
use mc_infrastructure::{
    AnyProvider, DownloadMeter, ImapEmailProvider, JmapEmailProvider, SqliteDb, SqliteEventStore,
    SqliteRepository, email::smtp::SmtpSender,
};

use crate::config::{AccountConfig, ServerConfig, SmtpSecurityConfig};
use crate::wiring::{
    CommandBusImpl, ProviderRegistryImpl, QueryBusImpl, SearchHandlerImpl, SyncServiceImpl,
    TaggingServiceInner,
};

mod supervisor;
mod tasks;
mod watcher;

/// What the runtime needs from the rest of the server.
pub struct RuntimeDeps {
    pub config_path: String,
    pub db: Arc<SqliteDb>,
    pub secret_store: Arc<dyn SecretStore>,
    pub provisioner: Arc<dyn AccountProvisioner>,
    pub auth_gates: Arc<AuthGates>,
    pub registry: Arc<ProviderRegistryImpl>,
    pub repository: Arc<SqliteRepository>,
    pub event_store: Arc<SqliteEventStore>,
    pub command_bus: Arc<CommandBusImpl>,
    pub query_bus: Arc<QueryBusImpl>,
    pub sync: Arc<SyncServiceImpl>,
    pub search: Arc<SearchHandlerImpl>,
    pub tagging: Arc<TaggingServiceInner>,
    /// Push watchers off: tests need syncs to happen only when they ask.
    pub idle_enabled: bool,
    pub shutdown: CancellationToken,
}

/// What a supervised per-account task needs to run, stop and report.
#[derive(Clone)]
struct TaskCtx {
    id: AccountId,
    name: String,
    gate: Arc<AuthGate>,
    stop: CancellationToken,
    tasks: TaskTracker,
}

struct Running {
    name: String,
    /// Child of the global shutdown; cancelling it stops this account's tasks only.
    stop: CancellationToken,
    /// Every task spawned for the account, so stopping it can wait them out.
    tasks: TaskTracker,
}

pub struct AccountRuntime {
    deps: RuntimeDeps,
    /// Serialises `start` and `remove`, so two activations of one address cannot
    /// both create it and a removal never races a start.
    starting: tokio::sync::Mutex<()>,
    running: Mutex<HashMap<AccountId, Running>>,
}

impl AccountRuntime {
    pub fn new(deps: RuntimeDeps) -> Self {
        Self {
            deps,
            starting: tokio::sync::Mutex::new(()),
            running: Mutex::new(HashMap::new()),
        }
    }

    /// The display name of a running account.
    pub fn name(&self, id: &AccountId) -> Option<String> {
        self.running.lock().unwrap().get(id).map(|r| r.name.clone())
    }

    pub fn is_running(&self, id: &AccountId) -> bool {
        self.running.lock().unwrap().contains_key(id)
    }

    /// Starts an account declared in `config.toml`, creating its aggregate if
    /// the database has none yet. Idempotent. On failure nothing is left behind.
    pub async fn start(&self, account_config: &AccountConfig) -> anyhow::Result<AccountId> {
        let _starting = self.starting.lock().await;
        self.start_locked(account_config, None).await
    }

    async fn start_locked(
        &self,
        account_config: &AccountConfig,
        backoff: Option<Backoff>,
    ) -> anyhow::Result<AccountId> {
        let deps = &self.deps;
        let existing = deps
            .repository
            .list_accounts()
            .await
            .context("failed to list accounts")?
            .into_iter()
            .find(|a| a.email.as_str() == account_config.email);
        let (account_id, name) = match &existing {
            Some(summary) => (summary.id, summary.name.clone()),
            None => (
                create_account(account_config, &deps.command_bus).await?,
                account_config.name.clone(),
            ),
        };
        if self.is_running(&account_id) {
            return Ok(account_id);
        }

        let secrets = deps
            .secret_store
            .load(&account_config.email)
            .with_context(|| format!("reading keychain secrets for {}", account_config.email))?
            .unwrap_or_default();
        let imap_password = secrets.password.clone().unwrap_or_default();
        let gate = deps.auth_gates.add(
            account_id,
            &account_config.email,
            credential(account_config, &secrets, &imap_password),
        );
        if let Some(summary) = &existing {
            gate.restore(summary);
        }
        if let Some(backoff) = backoff {
            gate.inherit(backoff);
        }

        // JMAP submits through the provider, never over SMTP.
        let smtp_sender = if account_config.jmap_url.is_some() {
            None
        } else {
            build_smtp_sender(account_config, &secrets, &imap_password, &gate).await
        };
        let provider = match build_provider(
            account_config,
            gate.clone() as Arc<dyn LoginGate>,
            smtp_sender,
            Some(DownloadMeter::new(deps.db.clone(), &account_config.email)),
        ) {
            Ok(provider) => Arc::new(provider),
            Err(e) => {
                deps.auth_gates.remove(&account_id);
                return Err(e).context(format!("building provider for {}", account_config.email));
            }
        };
        deps.registry
            .register_provider(account_id, provider.clone());
        tracing::info!(
            "Registered provider for '{name}' ({})",
            account_config.email
        );

        retry_paused_login(&gate, account_config);

        let stop = deps.shutdown.child_token();
        let tasks = TaskTracker::new();
        let first_sync = Arc::new(Notify::new());
        let task_ctx = TaskCtx {
            id: account_id,
            name: name.clone(),
            gate,
            stop: stop.clone(),
            tasks: tasks.clone(),
        };
        self.spawn_sync_loop(task_ctx.clone(), first_sync.clone());
        self.spawn_backfill(task_ctx.clone(), account_config.daily_download_limit());
        if deps.idle_enabled {
            self.spawn_watcher(task_ctx, provider, first_sync);
        }
        self.running
            .lock()
            .unwrap()
            .insert(account_id, Running { name, stop, tasks });
        Ok(account_id)
    }

    /// Starts the account `config.toml` declares under `email`: what a login
    /// or `AddAccount` has just written there.
    pub async fn start_declared(&self, email: &str) -> anyhow::Result<AccountId> {
        self.start(&self.declared(email)?).await
    }

    fn declared(&self, email: &str) -> anyhow::Result<AccountConfig> {
        ServerConfig::load_or_default(&self.deps.config_path)
            .accounts
            .into_iter()
            .find(|a| a.email == email)
            .with_context(|| format!("{email} is not declared in {}", self.deps.config_path))
    }

    /// Stops the account and starts it again from `config.toml`, which
    /// rebuilds everything per run: the credential, the SMTP sender and the
    /// tasks. Its data stays. A failed start leaves it stopped.
    pub async fn restart(&self, account_id: AccountId) -> Result<(), ApplicationError> {
        let _lifecycle = self.starting.lock().await;
        let account = self
            .deps
            .repository
            .get_account(&account_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Account", account_id.to_string()))?;
        let internal = |e: anyhow::Error| ApplicationError::Internal(format!("{e:#}"));
        let account_config = self.declared(account.email.as_str()).map_err(internal)?;
        // A throttled account must not log in again the moment it is restarted.
        let backoff = self
            .deps
            .auth_gates
            .get(&account_id)
            .and_then(|gate| gate.backoff());
        self.stop(&account_id).await;
        self.start_locked(&account_config, backoff)
            .await
            .map(|_| ())
            .map_err(internal)
    }

    /// Stops the account's tasks, waits until they are gone, and takes its
    /// provider and gate out of service. Its data stays; returns whether it
    /// was running.
    pub async fn stop(&self, account_id: &AccountId) -> bool {
        let Some(running) = self.running.lock().unwrap().remove(account_id) else {
            return false;
        };
        running.stop.cancel();
        self.deps.registry.remove_provider(account_id);
        self.deps.auth_gates.remove(account_id);
        running.tasks.close();
        running.tasks.wait().await;
        tracing::info!("Stopped account '{}'", running.name);
        true
    }

    /// Removes an account for good: stops it, records the removal in the log,
    /// then forgets everything it had. The order makes a crash harmless. The
    /// `AccountRemoved` event is the point of no return: from there on boot
    /// ([`Self::recover_removals`]) finishes the job before it reads
    /// `config.toml`, so the account cannot come back as a fresh one. The config
    /// entry goes before the data, so no window leaves it declared with its
    /// data gone.
    pub async fn remove(&self, account_id: AccountId) -> Result<(), ApplicationError> {
        let _lifecycle = self.starting.lock().await;
        let deps = &self.deps;
        let account = deps
            .repository
            .get_account(&account_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Account", account_id.to_string()))?;
        self.stop(&account_id).await;
        // What is left is a sync a client asked for over gRPC. Waiting for its
        // slot drains it, and holding the slot until the data is gone makes
        // one that comes later find no provider, so nothing writes for the
        // account after the purge.
        let _no_sync = deps.sync.exclusive(&account_id).await;
        deps.command_bus
            .dispatch(Command::RemoveAccount { account_id })
            .await?;
        let removed = self
            .finish_removal(account_id, Some(account.email.as_str().to_owned()))
            .await
            .map_err(|e| ApplicationError::Internal(format!("{e:#}")));
        deps.sync.forget(&account_id);
        removed
    }

    /// Finishes removals the server was interrupted in, returning how many it
    /// found. Runs at boot before accounts are started or created from
    /// `config.toml`, which may have changed.
    pub async fn recover_removals(&self) -> anyhow::Result<usize> {
        let _lifecycle = self.starting.lock().await;
        let pending = self.deps.repository.pending_removals().await?;
        for (account_id, email) in &pending {
            let (account_id, email) = (*account_id, email.clone());
            tracing::warn!("Finishing the removal of account {account_id} that was interrupted");
            if let Err(e) = self.finish_removal(account_id, email).await {
                tracing::error!("Could not finish removing account {account_id}: {e:#}");
            }
        }
        Ok(pending.len())
    }

    /// Idempotent: each step may already be done.
    async fn finish_removal(
        &self,
        account_id: AccountId,
        email: Option<String>,
    ) -> anyhow::Result<()> {
        let deps = &self.deps;
        // The address may belong to an account added again since; its config
        // entry, keychain secrets and download log are not the old one's.
        let email = match email {
            Some(email) if !self.address_in_use(&email).await? => Some(email),
            _ => None,
        };
        if let Some(email) = &email {
            deps.provisioner
                .deprovision(email)
                .await
                .context("removing the config entry and keychain secrets")?;
        }
        // Search goes after the purge: once the events are gone no catch-up
        // can index the account again.
        deps.repository
            .purge_account(account_id, email)
            .await
            .context("deleting the account's data")?;
        deps.search
            .remove_account(&account_id)
            .await
            .context("removing search documents")?;
        // The purge emptied the account's group of what it had taught.
        if let Err(e) = deps.tagging.retrain_pending().await {
            tracing::warn!("Could not retrain labels after removing {account_id}: {e:#}");
        }
        tracing::info!("Removed account {account_id} and all of its data");
        Ok(())
    }

    async fn address_in_use(&self, email: &str) -> anyhow::Result<bool> {
        Ok(self
            .deps
            .repository
            .list_accounts()
            .await?
            .iter()
            .any(|a| a.email.as_str() == email))
    }
}

#[async_trait::async_trait]
impl AccountLifecycle for AccountRuntime {
    async fn activate(&self, email: &str) -> Result<(), ApplicationError> {
        self.start_declared(email)
            .await
            .map(|_| ())
            .map_err(|e| ApplicationError::Internal(format!("{e:#}")))
    }

    async fn restart(&self, account_id: AccountId) -> Result<(), ApplicationError> {
        AccountRuntime::restart(self, account_id).await
    }

    async fn remove(&self, account_id: AccountId) -> Result<(), ApplicationError> {
        AccountRuntime::remove(self, account_id).await
    }
}

/// A pause persisted by an older run gets one real login now; the user may
/// have fixed it meanwhile (e.g. a password in config.toml).
fn retry_paused_login(gate: &Arc<AuthGate>, account_config: &AccountConfig) {
    if !gate.status().state.is_paused() {
        return;
    }
    let gate = gate.clone();
    let account_config = account_config.clone();
    tokio::spawn(async move {
        gate.retry_after_restart(|secret| async move {
            build_provider(&account_config, Arc::new(secret), None, None)?
                .verify_login()
                .await
        })
        .await;
    });
}

fn credential(
    account_config: &AccountConfig,
    secrets: &AccountSecrets,
    imap_password: &str,
) -> Credential {
    if let Some(oauth_cfg) = account_config.oauth_provider {
        // An empty refresh token makes the gate ask for a login on first use
        // instead of knocking with a token it cannot have.
        let tokens = secrets.oauth.clone().unwrap_or_else(|| {
            tracing::warn!(
                "⚠ {}: nessun token OAuth nel keychain — esegui `mc accounts add-oauth`",
                account_config.email
            );
            mc_domain::OAuthSecrets {
                access_token: String::new(),
                refresh_token: String::new(),
                expires_at: Utc::now(),
            }
        });
        return Credential::OAuth {
            provider: oauth_cfg.into(),
            tokens,
        };
    }
    if imap_password.is_empty() {
        tracing::warn!(
            "⚠ {}: nessuna password nel keychain di sistema — inserisci `password = \"...\"` in config.toml per importarla",
            account_config.email
        );
    }
    Credential::Password(imap_password.to_owned())
}

/// Creates the aggregate for an account declared in config but not yet in the
/// database, the way `AddAccount` does.
pub async fn create_account(
    account_config: &AccountConfig,
    command_bus: &CommandBusImpl,
) -> anyhow::Result<AccountId> {
    let email = EmailAddress::new(&account_config.email).context("invalid email in config")?;
    let smtp = account_config.smtp.as_ref().map(|s| SmtpConnectionConfig {
        host: s.host.clone(),
        port: s.port,
        security: smtp_security(s.security),
        username: s.username.clone(),
        // Secret lives in the keychain, not the persisted aggregate
        // (AuthConfig secret fields are #[serde(skip)]).
        password: None,
    });
    let connection_config = ConnectionConfig {
        dav: None,
        incoming: match &account_config.jmap_url {
            Some(session_url) => IncomingServer::Jmap {
                session_url: session_url.clone(),
            },
            None => IncomingServer::Imap {
                host: account_config.imap_host.clone(),
                port: account_config.imap_port,
                use_tls: account_config.use_tls,
            },
        },
        auth: AuthConfig::Plain {
            username: account_config.username.clone(),
            password: String::new(), // resolved from keychain at runtime
        },
        smtp,
    };
    let identity = Identity {
        display_name: account_config.name.clone(),
        email: email.clone(),
        signature_html: None,
        signature_text: None,
    };
    let result = command_bus
        .dispatch(Command::AddAccount {
            name: account_config.name.clone(),
            email,
            provider_type: account_config.provider_type.into(),
            connection_config,
            identity,
        })
        .await
        .context("failed to create account")?;
    let CommandResult::AccountAdded { account_id } = result else {
        anyhow::bail!("unexpected result creating account");
    };
    tracing::info!(
        "Created account '{}' ({})",
        account_config.name,
        account_config.email
    );
    Ok(account_id)
}

fn smtp_security(security: SmtpSecurityConfig) -> SmtpSecurity {
    match security {
        SmtpSecurityConfig::Smtps => SmtpSecurity::Smtps,
        SmtpSecurityConfig::StartTls => SmtpSecurity::StartTls,
    }
}

/// The backend `account_config` declares.
fn build_provider(
    account_config: &AccountConfig,
    gate: Arc<dyn LoginGate>,
    smtp_sender: Option<SmtpSender>,
    meter: Option<DownloadMeter>,
) -> Result<AnyProvider, DomainError> {
    if let Some(session_url) = &account_config.jmap_url {
        let provider = JmapEmailProvider::new(session_url, account_config.username.clone(), gate)?;
        return Ok(AnyProvider::Jmap(match meter {
            Some(meter) => provider.with_download_meter(meter),
            None => provider,
        }));
    }
    if account_config.imap_host.is_empty() {
        return Err(DomainError::Validation(format!(
            "{}: neither imap_host nor jmap_url is set",
            account_config.email
        )));
    }
    let provider = ImapEmailProvider::new(
        account_config.imap_host.clone(),
        account_config.imap_port,
        account_config.username.clone(),
        gate,
        account_config.use_tls,
        smtp_sender,
    );
    Ok(AnyProvider::Imap(match meter {
        Some(meter) => provider.with_download_meter(meter),
        None => provider,
    }))
}

/// The account's SMTP sender, checked once at start unless the account is
/// paused. It takes the account's own credential from the gate (the OAuth
/// token, or the IMAP password) unless it has an SMTP password of its own.
/// Either way a refused SMTP login only shows up as the status's SMTP error:
/// IMAP alone decides whether the account's credential works.
async fn build_smtp_sender(
    account_config: &AccountConfig,
    secrets: &AccountSecrets,
    imap_password: &str,
    gate: &Arc<AuthGate>,
) -> Option<SmtpSender> {
    let Some(smtp_cfg) = &account_config.smtp else {
        tracing::warn!(
            "⚠ SMTP {}: nessun blocco [smtp] in config — invio disabilitato",
            account_config.email
        );
        return None;
    };
    let domain_smtp = SmtpConnectionConfig {
        host: smtp_cfg.host.clone(),
        port: smtp_cfg.port,
        security: smtp_security(smtp_cfg.security),
        username: smtp_cfg.username.clone(),
        password: secrets.smtp_password.clone(),
    };
    let own_password = secrets
        .smtp_password
        .as_deref()
        .is_some_and(|p| p != imap_password);
    let sender = if account_config.oauth_provider.is_some() || !own_password {
        SmtpSender::build_gated(&domain_smtp, &account_config.username, gate.clone())
    } else {
        match SmtpSender::build(
            &domain_smtp,
            &account_config.username,
            imap_password,
            gate.clone(),
        ) {
            Ok(sender) => sender,
            Err(e) => {
                tracing::warn!(
                    "⚠ SMTP {}: configurazione non valida — {e}",
                    account_config.email
                );
                return None;
            }
        }
    };
    if gate.status().state.is_paused() {
        return Some(sender);
    }
    match sender.test_connection().await {
        Ok(true) => tracing::info!(
            "✓ SMTP {} ({}:{}) connessione OK",
            account_config.email,
            smtp_cfg.host,
            smtp_cfg.port
        ),
        Ok(false) => tracing::warn!(
            "⚠ SMTP {}: server raggiungibile ma NOOP fallito",
            account_config.email
        ),
        Err(e) => tracing::warn!("⚠ SMTP {}: non raggiungibile — {e}", account_config.email),
    }
    Some(sender)
}

#[cfg(test)]
mod tests;
