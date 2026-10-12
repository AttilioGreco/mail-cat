use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use mc_client::{
    Account, ClientError, ComposeAttachment, ComposeInput, ComposeView, ContactHit, ConversationSummary,
    ImapAccountParams, MailCatClient, Mailbox, MailboxNotificationKind, MessageBody, MessageFlag,
    MessageSummary, OAuthLoginEvent, OAuthProvider, SearchHit, Suggestion, SyncPhase,
    SyncProgress, ThreadMessage,
};

/// Tauri managed state — single shared client instance.
pub struct AppState {
    pub client: Mutex<Option<MailCatClient>>,
    pub server_url: String,
}

/// Where a desktop notification wants the UI to navigate.
#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeepLink {
    pub account_id: String,
    pub mailbox_id: String,
    pub message_id: String,
}

impl DeepLink {
    /// Parse `--open-message <account>/<mailbox>/<message>` out of a process argv.
    ///
    /// Returns `None` for anything malformed: a bad deep link must degrade to
    /// "just open the app", never abort startup.
    pub fn from_args<I, S>(args: I) -> Option<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            let arg = arg.as_ref();
            // Accept both `--open-message VALUE` and `--open-message=VALUE`.
            if let Some(value) = arg.strip_prefix(Self::FLAG) {
                return match value.strip_prefix('=') {
                    Some(inline) => Self::parse(inline),
                    None if value.is_empty() => Self::parse(args.next()?.as_ref()),
                    None => None, // e.g. `--open-messageXYZ`
                };
            }
        }
        None
    }

    pub const FLAG: &'static str = "--open-message";

    fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split('/');
        let account_id = parts.next()?.trim().to_owned();
        let mailbox_id = parts.next()?.trim().to_owned();
        let message_id = parts.next()?.trim().to_owned();
        if parts.next().is_some()
            || account_id.is_empty()
            || mailbox_id.is_empty()
            || message_id.is_empty()
        {
            return None;
        }
        Some(Self {
            account_id,
            mailbox_id,
            message_id,
        })
    }

    /// Render back to the `<account>/<mailbox>/<message>` wire form.
    pub fn to_arg_value(&self) -> String {
        format!(
            "{}/{}/{}",
            self.account_id, self.mailbox_id, self.message_id
        )
    }
}

/// Deep link captured at startup, waiting for the frontend to ask for it.
///
/// The webview is not listening yet when the process starts, so a cold-start
/// link cannot be delivered as an event — the frontend pulls it instead.
pub struct PendingDeepLink(pub Mutex<Option<DeepLink>>);

/// Hand the startup deep link to the frontend exactly once.
#[tauri::command]
pub async fn take_pending_deep_link(
    state: State<'_, PendingDeepLink>,
) -> Result<Option<DeepLink>, String> {
    Ok(state.0.lock().await.take())
}

/// Tracks active mailbox watch tasks (mailbox_id → (token, ref_count)).
///
/// Multiple callers can independently request the same mailbox watcher
/// (e.g. MessageList for the selected folder AND the permanent INBOX watcher
/// in Sidebar). The background task runs as long as ref_count > 0 and is
/// cancelled only when the last caller calls `stop_watching`.
pub struct WatchState {
    pub watchers: Mutex<HashMap<String, (CancellationToken, usize)>>,
}

impl WatchState {
    pub fn new() -> Self {
        Self {
            watchers: Mutex::new(HashMap::new()),
        }
    }
}

// ── Serializable DTOs (mirrors mc-client types for JS) ──────────────────────

#[derive(Serialize)]
pub struct AccountDto {
    pub id: String,
    pub name: String,
    pub email: String,
    /// "active" | "disabled" | "error" | "needs_reauth" | "unknown"
    pub status: String,
}

/// Download progress for one account.
#[derive(Serialize)]
pub struct AccountStatsDto {
    pub total_messages: u32,
    pub synced_messages: u32,
    pub pending_bodies: u32,
}

/// Live sync progress, emitted as the `sync:progress` Tauri event.
#[derive(Serialize, Clone)]
pub struct SyncProgressPayload {
    pub account_id: String,
    /// "discovering_mailboxes" | "fetching_envelopes" | "fetching_bodies" | "indexing" | "complete" | "error"
    pub phase: String,
    pub processed: u32,
    pub total: Option<u32>,
    pub message: Option<String>,
}

/// OAuth login progress, emitted as the `oauth:event` Tauri event.
#[derive(Serialize, Clone)]
pub struct OAuthEventPayload {
    /// "awaiting_browser" | "succeeded" | "failed"
    pub kind: String,
    pub authorize_url: Option<String>,
    pub email: Option<String>,
    pub message: Option<String>,
}

#[derive(Serialize)]
pub struct MailboxDto {
    pub id: String,
    pub account_id: String,
    pub path: String,
    pub display_name: String,
    /// "inbox" | "sent" | "drafts" | "trash" | "spam" | "archive" | "all" | "custom"
    pub mailbox_type: String,
    pub total_messages: u32,
    pub unread_messages: u32,
}

#[derive(Serialize)]
pub struct MessageSummaryDto {
    pub id: String,
    pub mailbox_id: String,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub date: String, // ISO 8601
    pub is_read: bool,
    pub is_flagged: bool,
    pub has_attachments: bool,
}

#[derive(Serialize)]
pub struct MessageBodyDto {
    pub id: String,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub rfc_message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
}

/// Payload emitted on "mailbox:event" Tauri event.
#[derive(Clone, Serialize)]
pub struct MailboxEventPayload {
    pub mailbox_id: String,
    pub kind: String, // "counts_changed" | "new_message" | "message_expunged" | "flags_changed"
    pub total: Option<u32>,
    pub unseen: Option<u32>,
    pub provider_uid: Option<u32>,
}

// ── Conversions ──────────────────────────────────────────────────────────────

fn account_to_dto(a: Account) -> AccountDto {
    AccountDto {
        id: a.id,
        name: a.name,
        email: a.email,
        status: a.status.as_str().to_string(),
    }
}

fn sync_phase_to_str(phase: &SyncPhase) -> &'static str {
    match phase {
        SyncPhase::DiscoveringMailboxes => "discovering_mailboxes",
        SyncPhase::FetchingEnvelopes => "fetching_envelopes",
        SyncPhase::FetchingBodies => "fetching_bodies",
        SyncPhase::Indexing => "indexing",
        SyncPhase::Complete => "complete",
        SyncPhase::Error(_) => "error",
    }
}

fn sync_progress_to_payload(p: SyncProgress) -> SyncProgressPayload {
    SyncProgressPayload {
        account_id: p.account_id,
        phase: sync_phase_to_str(&p.phase).to_string(),
        processed: p.processed,
        total: p.total,
        message: match &p.phase {
            // The failure text is inside the phase, not the message field.
            SyncPhase::Error(e) => Some(e.clone()),
            _ => p.message,
        },
    }
}

fn mailbox_to_dto(m: Mailbox) -> MailboxDto {
    MailboxDto {
        id: m.id,
        account_id: m.account_id,
        path: m.path,
        display_name: m.display_name,
        mailbox_type: m.mailbox_type.as_str().to_string(),
        total_messages: m.total_messages,
        unread_messages: m.unread_messages,
    }
}

fn message_to_dto(m: MessageSummary) -> MessageSummaryDto {
    MessageSummaryDto {
        id: m.id,
        mailbox_id: m.mailbox_id,
        subject: m.subject,
        from_name: m.from_name,
        from_email: m.from_email,
        to: m.to,
        cc: m.cc,
        date: m.date.to_rfc3339(),
        is_read: m.is_read,
        is_flagged: m.is_flagged,
        has_attachments: m.has_attachments,
    }
}

fn body_to_dto(b: MessageBody) -> MessageBodyDto {
    MessageBodyDto {
        id: b.id,
        html_body: b.html_body,
        text_body: b.text_body,
        rfc_message_id: b.rfc_message_id,
        in_reply_to: b.in_reply_to,
        references: b.references,
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn get_or_connect(state: &State<'_, AppState>) -> Result<MailCatClient, String> {
    let mut guard = state.client.lock().await;
    if guard.is_none() {
        let client = MailCatClient::connect(&state.server_url)
            .await
            .map_err(|e| e.to_string())?;
        *guard = Some(client);
    }
    Ok(guard.as_ref().unwrap().clone())
}

// ── Commands ─────────────────────────────────────────────────────────────────

#[tauri::command]
pub async fn list_accounts(state: State<'_, AppState>) -> Result<Vec<AccountDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .list_accounts()
        .await
        .map(|v| v.into_iter().map(account_to_dto).collect())
        .map_err(|e| e.to_string())
}

/// Reserved `WatchState` key for the single sync-progress watcher — it shares the
/// mailbox watcher's ref-counting so repeated mounts don't spawn duplicate tasks.
const SYNC_WATCH_KEY: &str = "__sync_progress__";

/// First delay before re-opening a dropped server stream.
const INITIAL_WATCH_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
/// Ceiling for the reconnect delay: a watcher that stays down this long is
/// waiting for the server to come back, and hammering it buys nothing.
const MAX_WATCH_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

/// Double the reconnect delay, capped. Kept separate so the ramp is testable.
fn next_backoff(current: std::time::Duration) -> std::time::Duration {
    (current * 2).min(MAX_WATCH_BACKOFF)
}

#[tauri::command]
pub async fn account_stats(
    account_id: String,
    state: State<'_, AppState>,
) -> Result<AccountStatsDto, String> {
    let client = get_or_connect(&state).await?;
    client
        .account_stats(&account_id)
        .await
        .map(|s| AccountStatsDto {
            total_messages: s.total_messages,
            synced_messages: s.synced_messages,
            pending_bodies: s.pending_bodies,
        })
        .map_err(|e| e.to_string())
}

/// Settings-form input for a manually configured IMAP account.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImapAccountInput {
    pub name: String,
    pub email: String,
    pub imap_host: String,
    pub imap_port: u16,
    pub use_tls: bool,
    pub username: String,
    pub password: String,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<u16>,
}

/// Add an IMAP account. The server records it in its config and keychain, but
/// only starts syncing it after a restart — see `restart_server`.
#[tauri::command]
pub async fn add_imap_account(
    params: ImapAccountInput,
    state: State<'_, AppState>,
) -> Result<AccountDto, String> {
    let client = get_or_connect(&state).await?;
    client
        .add_imap_account(ImapAccountParams {
            name: params.name,
            email: params.email,
            imap_host: params.imap_host,
            imap_port: params.imap_port,
            smtp_host: params.smtp_host,
            smtp_port: params.smtp_port,
            use_tls: params.use_tls,
            username: params.username,
            password: params.password,
        })
        .await
        .map(account_to_dto)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn remove_account(account_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let client = get_or_connect(&state).await?;
    client
        .remove_account(&account_id)
        .await
        .map_err(|e| e.to_string())
}

/// Ask the server to sync an account now. Returns as soon as the sync is under
/// way — progress arrives through the `sync:progress` event stream.
#[tauri::command]
pub async fn sync_now(account_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let client = get_or_connect(&state).await?;

    // Opening the stream is what triggers the sync; the events themselves reach
    // the frontend through the watcher, so this task only has to drain it. The
    // stream borrows the client, so both move into the task.
    tokio::spawn(async move {
        let stream = match client.sync_account(&account_id, false).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("sync_now failed for {account_id}: {e}");
                return;
            }
        };
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                tracing::warn!("sync_now stream error for {account_id}: {e}");
                break;
            }
        }
    });

    Ok(())
}

/// Stream server-side sync progress into `sync:progress` Tauri events.
///
/// Watches every account, including syncs the client never asked for (boot sync,
/// the periodic tick, the body backfill worker) — that is where the download
/// percentage actually comes from. Idempotent; ref-counted like mailbox watchers.
#[tauri::command]
pub async fn start_sync_watch(
    app: AppHandle,
    state: State<'_, AppState>,
    watch_state: State<'_, WatchState>,
) -> Result<(), String> {
    let mut watchers = watch_state.watchers.lock().await;
    if let Some((_, ref_count)) = watchers.get_mut(SYNC_WATCH_KEY) {
        *ref_count += 1;
        return Ok(());
    }

    let client = get_or_connect(&state).await?;
    let token = CancellationToken::new();
    watchers.insert(SYNC_WATCH_KEY.to_string(), (token.clone(), 1));
    drop(watchers);

    tokio::spawn(async move {
        let mut backoff = INITIAL_WATCH_BACKOFF;

        // Same reconnect discipline as the mailbox watcher: without it a server
        // restart silently ends progress updates for the rest of the session.
        'reconnect: while !token.is_cancelled() {
            match client.watch_sync_progress(None).await {
                Ok(stream) => {
                    backoff = INITIAL_WATCH_BACKOFF;
                    tokio::pin!(stream);

                    loop {
                        tokio::select! {
                            _ = token.cancelled() => break 'reconnect,
                            item = stream.next() => match item {
                                None => {
                                    tracing::warn!("sync progress stream ended, reconnecting");
                                    break;
                                }
                                Some(Err(e)) => {
                                    tracing::error!("sync progress stream error: {e}, reconnecting");
                                    break;
                                }
                                Some(Ok(progress)) => {
                                    let payload = sync_progress_to_payload(progress);
                                    if let Err(e) = app.emit("sync:progress", &payload) {
                                        tracing::warn!("Failed to emit sync:progress: {e}");
                                    }
                                }
                            },
                        }
                    }
                }
                Err(e) => tracing::warn!("watch_sync_progress failed: {e}, retrying"),
            }

            tokio::select! {
                _ = token.cancelled() => break 'reconnect,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = next_backoff(backoff);
        }
    });

    Ok(())
}

#[tauri::command]
pub async fn stop_sync_watch(watch_state: State<'_, WatchState>) -> Result<(), String> {
    stop_watching(SYNC_WATCH_KEY.to_string(), watch_state).await
}

/// Run an OAuth2 login. Opens the system browser when the server hands us the
/// authorize URL, then reports the outcome through `oauth:event`.
#[tauri::command]
pub async fn start_oauth_login(
    name: String,
    email: String,
    provider: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let provider = match provider.as_str() {
        "google" => OAuthProvider::Google,
        "microsoft" => OAuthProvider::Microsoft,
        other => return Err(format!("unknown OAuth provider: {other}")),
    };

    let client = get_or_connect(&state).await?;

    tokio::spawn(async move {
        match client.start_oauth_login(&name, &email, provider).await {
            Ok(stream) => pump_oauth_events(app, stream).await,
            Err(e) => emit_oauth_failure(&app, e.to_string()),
        }
    });

    Ok(())
}

/// Re-run the OAuth2 login for an existing account whose token was revoked or
/// expired. Reports through the same `oauth:event` channel as a fresh login.
#[tauri::command]
pub async fn reauth_account(
    account_id: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let client = get_or_connect(&state).await?;

    tokio::spawn(async move {
        match client.reauth_account(&account_id).await {
            Ok(stream) => pump_oauth_events(app, stream).await,
            Err(e) => emit_oauth_failure(&app, e.to_string()),
        }
    });

    Ok(())
}

fn emit_oauth_failure(app: &AppHandle, message: String) {
    let _ = app.emit(
        "oauth:event",
        &OAuthEventPayload {
            kind: "failed".into(),
            authorize_url: None,
            email: None,
            message: Some(message),
        },
    );
}

/// Forwards an OAuth progress stream to the frontend as `oauth:event`, opening
/// the system browser when the authorize URL arrives.
async fn pump_oauth_events(
    app: AppHandle,
    stream: impl tokio_stream::Stream<Item = mc_client::Result<OAuthLoginEvent>>,
) {
    tokio::pin!(stream);
    while let Some(item) = stream.next().await {
        let payload = match item {
            Ok(OAuthLoginEvent::AwaitingBrowser { authorize_url }) => {
                // Best-effort: the URL also goes to the frontend so the user
                // can open it by hand if there is no desktop handler.
                if let Err(e) = mc_shared::browser::open_browser(&authorize_url) {
                    tracing::warn!("Failed to open browser for OAuth login: {e}");
                }
                OAuthEventPayload {
                    kind: "awaiting_browser".into(),
                    authorize_url: Some(authorize_url),
                    email: None,
                    message: None,
                }
            }
            Ok(OAuthLoginEvent::Succeeded { email }) => OAuthEventPayload {
                kind: "succeeded".into(),
                authorize_url: None,
                email: Some(email),
                message: None,
            },
            Ok(OAuthLoginEvent::Failed { message }) => OAuthEventPayload {
                kind: "failed".into(),
                authorize_url: None,
                email: None,
                message: Some(message),
            },
            Err(e) => OAuthEventPayload {
                kind: "failed".into(),
                authorize_url: None,
                email: None,
                message: Some(e.to_string()),
            },
        };

        let done = payload.kind != "awaiting_browser";
        if let Err(e) = app.emit("oauth:event", &payload) {
            tracing::warn!("Failed to emit oauth:event: {e}");
        }
        if done {
            break;
        }
    }
}

/// Restart the mail-cat server so newly added (or removed) accounts take effect.
///
/// Accounts are declared in the server's config file and turned into providers
/// at boot, so add/remove only becomes real after a restart.
// ponytail: assumes the packaged systemd --user unit (see packaging/systemd).
// If the server is started some other way, surface the error and let the user
// restart it by hand.
#[tauri::command]
pub async fn restart_server() -> Result<(), String> {
    let output = tokio::process::Command::new("systemctl")
        .args(["--user", "restart", "mail-cat-server"])
        .output()
        .await
        .map_err(|e| format!("could not run systemctl: {e}"))?;

    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "systemctl --user restart mail-cat-server failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

#[tauri::command]
pub async fn list_mailboxes(
    account_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<MailboxDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .list_mailboxes(&account_id)
        .await
        .map(|v| v.into_iter().map(mailbox_to_dto).collect())
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_messages(
    account_id: String,
    mailbox_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<MessageSummaryDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .list_messages(&account_id, &mailbox_id)
        .await
        .map(|v| v.into_iter().map(message_to_dto).collect())
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_message(
    message_id: String,
    state: State<'_, AppState>,
) -> Result<MessageBodyDto, String> {
    let client = get_or_connect(&state).await?;
    client
        .get_message(&message_id)
        .await
        .map(body_to_dto)
        .map_err(|e| e.to_string())
}

/// Start watching a mailbox for real-time changes.
///
/// Spawns a background task that streams IMAP IDLE notifications and emits
/// Tauri "mailbox:event" events to the frontend. Calling again with the same
/// mailbox_id is a no-op (idempotent). Call `stop_watching` to cancel.
#[tauri::command]
pub async fn start_watching(
    mailbox_id: String,
    app: AppHandle,
    state: State<'_, AppState>,
    watch_state: State<'_, WatchState>,
) -> Result<(), String> {
    let mut watchers: tokio::sync::MutexGuard<'_, HashMap<String, (CancellationToken, usize)>> =
        watch_state.watchers.lock().await;

    // Already watching — just increment the ref count (idempotent task spawn)
    if let Some((_, ref_count)) = watchers.get_mut(&mailbox_id) {
        *ref_count += 1;
        return Ok(());
    }

    let client = get_or_connect(&state).await?;
    let token = CancellationToken::new();
    watchers.insert(mailbox_id.clone(), (token.clone(), 1));
    drop(watchers); // release lock before spawning

    let mid = mailbox_id.clone();
    tokio::spawn(async move {
        tracing::info!("Starting mailbox watcher for {mid}");
        let mut backoff = INITIAL_WATCH_BACKOFF;

        // Reconnect until cancelled. A dropped stream (server restart, transient
        // error) used to end this task for good: the `watchers` entry stayed
        // behind with its ref count, so every later `start_watching` was a no-op
        // and live updates never came back until the whole app was restarted.
        // The INBOX watcher makes that permanent, since the sidebar holds a
        // reference for the lifetime of the window.
        'reconnect: while !token.is_cancelled() {
            match client.watch_mailbox(&mid).await {
                Ok(stream) => {
                    backoff = INITIAL_WATCH_BACKOFF; // connected — reset the delay
                    tokio::pin!(stream);

                    loop {
                        tokio::select! {
                            _ = token.cancelled() => {
                                tracing::info!("Mailbox watcher cancelled for {mid}");
                                break 'reconnect;
                            }
                            item = stream.next() => {
                                match item {
                                    None => {
                                        tracing::warn!("Mailbox watch stream ended for {mid}, reconnecting");
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        tracing::error!("Mailbox watch error for {mid}: {e}, reconnecting");
                                        break;
                                    }
                                    Some(Ok(notification)) => {
                            let payload = match notification.event {
                                MailboxNotificationKind::CountsChanged { total, unseen } => {
                                    MailboxEventPayload {
                                        mailbox_id: mid.clone(),
                                        kind: "counts_changed".into(),
                                        total: Some(total),
                                        unseen: Some(unseen),
                                        provider_uid: None,
                                    }
                                }
                                MailboxNotificationKind::NewMessage { provider_uid } => {
                                    MailboxEventPayload {
                                        mailbox_id: mid.clone(),
                                        kind: "new_message".into(),
                                        total: None,
                                        unseen: None,
                                        provider_uid: Some(provider_uid),
                                    }
                                }
                                MailboxNotificationKind::MessageExpunged { provider_uid } => {
                                    MailboxEventPayload {
                                        mailbox_id: mid.clone(),
                                        kind: "message_expunged".into(),
                                        total: None,
                                        unseen: None,
                                        provider_uid: Some(provider_uid),
                                    }
                                }
                                MailboxNotificationKind::FlagsChanged => {
                                    MailboxEventPayload {
                                        mailbox_id: mid.clone(),
                                        kind: "flags_changed".into(),
                                        total: None,
                                        unseen: None,
                                        provider_uid: None,
                                    }
                                }
                            };

                                        tracing::debug!(
                                            "[tauri] Emitting mailbox:event kind={} mailbox_id={}",
                                            payload.kind,
                                            payload.mailbox_id
                                        );
                                        if let Err(e) = app.emit("mailbox:event", &payload) {
                                            tracing::warn!("Failed to emit mailbox:event: {e}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!("watch_mailbox failed for {mid}: {e}, retrying"),
            }

            tokio::select! {
                _ = token.cancelled() => break 'reconnect,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = next_backoff(backoff);
        }
    });

    Ok(())
}

/// Stop watching a mailbox (cancels the background IDLE task).
#[tauri::command]
pub async fn stop_watching(
    mailbox_id: String,
    watch_state: State<'_, WatchState>,
) -> Result<(), String> {
    let mut watchers: tokio::sync::MutexGuard<'_, HashMap<String, (CancellationToken, usize)>> =
        watch_state.watchers.lock().await;
    if let Some((_, ref_count)) = watchers.get_mut(&mailbox_id) {
        *ref_count -= 1;
        if *ref_count == 0 {
            let (token, _) = watchers.remove(&mailbox_id).unwrap();
            token.cancel();
        }
    }
    Ok(())
}

// ── Mutation commands ─────────────────────────────────────────────────────

/// DTO for search results.
#[derive(Serialize)]
pub struct SearchHitDto {
    pub message_id: String,
    pub score: f32,
    pub snippet: Option<String>,
    pub subject: String,
    pub from_name: Option<String>,
    pub from_email: String,
    pub date: i64,
}

fn search_hit_to_dto(h: SearchHit) -> SearchHitDto {
    SearchHitDto {
        message_id: h.message_id,
        score: h.score,
        snippet: h.snippet,
        subject: h.subject,
        from_name: h.from_name,
        from_email: h.from_email,
        date: h.date,
    }
}

fn str_to_flag(s: &str) -> Option<MessageFlag> {
    match s {
        "seen" => Some(MessageFlag::Seen),
        "answered" => Some(MessageFlag::Answered),
        "flagged" => Some(MessageFlag::Flagged),
        "deleted" => Some(MessageFlag::Deleted),
        "draft" => Some(MessageFlag::Draft),
        _ => None,
    }
}

#[tauri::command]
pub async fn set_flags(
    message_id: String,
    flags_to_add: Vec<String>,
    flags_to_remove: Vec<String>,
    state: State<'_, AppState>,
) -> Result<MessageSummaryDto, String> {
    let client = get_or_connect(&state).await?;
    let add: Vec<MessageFlag> = flags_to_add.iter().filter_map(|s| str_to_flag(s)).collect();
    let remove: Vec<MessageFlag> = flags_to_remove
        .iter()
        .filter_map(|s| str_to_flag(s))
        .collect();
    client
        .set_flags(&message_id, &add, &remove)
        .await
        .map(message_to_dto)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn mark_mailbox_read(
    mailbox_id: String,
    state: State<'_, AppState>,
) -> Result<u32, String> {
    let client = get_or_connect(&state).await?;
    client
        .mark_mailbox_read(&mailbox_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn move_message(
    message_id: String,
    to_mailbox_id: String,
    state: State<'_, AppState>,
) -> Result<MessageSummaryDto, String> {
    let client = get_or_connect(&state).await?;
    client
        .move_message(&message_id, &to_mailbox_id)
        .await
        .map(message_to_dto)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn delete_message(
    message_id: String,
    permanently: bool,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let client = get_or_connect(&state).await?;
    client
        .delete_message(&message_id, permanently)
        .await
        .map_err(|e| e.to_string())
}

/// Bulk delete. Returns `[succeeded, failed]` so the UI can report a partial
/// result instead of claiming everything worked.
#[tauri::command]
pub async fn delete_messages(
    message_ids: Vec<String>,
    permanently: bool,
    state: State<'_, AppState>,
) -> Result<[u32; 2], String> {
    let client = get_or_connect(&state).await?;
    client
        .delete_messages(message_ids, permanently)
        .await
        .map(|(ok, ko)| [ok, ko])
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn move_messages(
    message_ids: Vec<String>,
    to_mailbox_id: String,
    state: State<'_, AppState>,
) -> Result<[u32; 2], String> {
    let client = get_or_connect(&state).await?;
    client
        .move_messages(message_ids, &to_mailbox_id)
        .await
        .map(|(ok, ko)| [ok, ko])
        .map_err(|e| e.to_string())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComposeIdentityDto {
    pub account_id: String,
    pub display_name: String,
    pub email: String,
    pub signature_html: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForwardedAttachmentDto {
    pub id: String,
    pub filename: String,
    pub size: u64,
    pub mime_type: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InlineImageDto {
    pub content_id: String,
    pub mime_type: String,
    pub data_base64: String,
}

/// Starting state of the compose window; every default is computed server-side.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComposeDto {
    pub draft_id: Option<String>,
    pub account_id: String,
    pub identities: Vec<ComposeIdentityDto>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub quoted_html: Option<String>,
    pub signature_html: Option<String>,
    pub forwarded_attachments: Vec<ForwardedAttachmentDto>,
    pub inline_images: Vec<InlineImageDto>,
    pub mode: String,
    pub source_message_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComposeAttachmentInput {
    pub filename: String,
    pub mime_type: String,
    pub content_id: Option<String>,
    pub data_base64: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComposeInputDto {
    pub draft_id: Option<String>,
    pub account_id: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub mode: String,
    pub source_message_id: Option<String>,
    pub attachments: Vec<ComposeAttachmentInput>,
    pub forwarded_attachment_ids: Vec<String>,
}

impl ComposeInputDto {
    fn into_client(self) -> Result<ComposeInput, String> {
        let attachments = self
            .attachments
            .into_iter()
            .map(|a| {
                let data = BASE64
                    .decode(a.data_base64.as_bytes())
                    .map_err(|e| format!("Allegato «{}» non leggibile: {e}", a.filename))?;
                Ok(ComposeAttachment {
                    filename: a.filename,
                    mime_type: a.mime_type,
                    content_id: a.content_id,
                    data,
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(ComposeInput {
            draft_id: self.draft_id,
            account_id: self.account_id,
            to: self.to,
            cc: self.cc,
            bcc: self.bcc,
            subject: self.subject,
            body_md: self.body_md,
            mode: self.mode,
            source_message_id: self.source_message_id,
            attachments,
            forwarded_attachment_ids: self.forwarded_attachment_ids,
        })
    }
}

fn compose_to_dto(v: ComposeView) -> ComposeDto {
    ComposeDto {
        draft_id: v.draft_id,
        account_id: v.account_id,
        identities: v
            .identities
            .into_iter()
            .map(|i| ComposeIdentityDto {
                account_id: i.account_id,
                display_name: i.display_name,
                email: i.email,
                signature_html: i.signature_html,
            })
            .collect(),
        to: v.to,
        cc: v.cc,
        bcc: v.bcc,
        subject: v.subject,
        body_md: v.body_md,
        quoted_html: v.quoted_html,
        signature_html: v.signature_html,
        forwarded_attachments: v
            .forwarded_attachments
            .into_iter()
            .map(|a| ForwardedAttachmentDto {
                id: a.id,
                filename: a.filename,
                size: a.size,
                mime_type: a.mime_type,
            })
            .collect(),
        inline_images: v
            .inline_images
            .into_iter()
            .map(|i| InlineImageDto {
                content_id: i.content_id,
                mime_type: i.mime_type,
                data_base64: BASE64.encode(i.data),
            })
            .collect(),
        mode: v.mode,
        source_message_id: v.source_message_id,
    }
}

/// Compose errors are shown to the user as-is ("Indirizzo non valido: «x»"),
/// so drop the gRPC status wrapping around the server's message.
fn compose_error(e: ClientError) -> String {
    match e {
        ClientError::Grpc(status) => status.message().to_owned(),
        e => e.to_string(),
    }
}

/// `mode`: "new" | "reply" | "replyAll" | "forward" | "draft".
#[tauri::command]
pub async fn prepare_compose(
    mode: String,
    message_id: Option<String>,
    account_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<ComposeDto, String> {
    let client = get_or_connect(&state).await?;
    client
        .prepare_compose(&mode, message_id, account_id)
        .await
        .map(compose_to_dto)
        .map_err(compose_error)
}

/// Returns the id of the sent message.
#[tauri::command]
pub async fn send_compose(input: ComposeInputDto, state: State<'_, AppState>) -> Result<String, String> {
    let input = input.into_client()?;
    let client = get_or_connect(&state).await?;
    client.send_compose(input).await.map_err(compose_error)
}

/// Returns the id of the stored draft (pass it back as `draftId` next time).
#[tauri::command]
pub async fn save_draft(input: ComposeInputDto, state: State<'_, AppState>) -> Result<String, String> {
    let input = input.into_client()?;
    let client = get_or_connect(&state).await?;
    client.save_draft(input).await.map_err(compose_error)
}

#[tauri::command]
pub async fn discard_draft(draft_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let client = get_or_connect(&state).await?;
    client.discard_draft(&draft_id).await.map_err(compose_error)
}

#[tauri::command]
pub async fn search_messages(
    query: String,
    account_id: Option<String>,
    mailbox_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<SearchHitDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .search(&query, account_id.as_deref(), mailbox_id.as_deref())
        .await
        .map(|v| v.into_iter().map(search_hit_to_dto).collect())
        .map_err(|e| e.to_string())
}

// ── Contacts ─────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct ContactHitDto {
    pub id: String,
    pub display_name: String,
    pub primary_email: String,
    pub all_emails: Vec<String>,
    pub organization: Option<String>,
    pub account_scope: Option<String>,
    pub is_cross_account: bool,
}

fn contact_hit_to_dto(h: ContactHit) -> ContactHitDto {
    ContactHitDto {
        id: h.id,
        display_name: h.display_name,
        primary_email: h.primary_email,
        all_emails: h.all_emails,
        organization: h.organization,
        account_scope: h.account_scope,
        is_cross_account: h.is_cross_account,
    }
}

#[tauri::command]
pub async fn search_contacts(
    query: String,
    limit: Option<u32>,
    requesting_account_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<ContactHitDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .search_contacts(&query, limit.unwrap_or(10), requesting_account_id)
        .await
        .map(|v| v.into_iter().map(contact_hit_to_dto).collect())
        .map_err(|e| e.to_string())
}

/// Backfill the address book by scanning all stored messages.
///
/// For each from/to/cc address found, idempotently creates a contact if one
/// doesn't already exist. Returns `[new_contacts, already_existed]`.
#[tauri::command]
pub async fn backfill_contacts_from_messages(
    account_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<(u32, u32), String> {
    let client = get_or_connect(&state).await?;
    client
        .backfill_contacts_from_messages(account_id)
        .await
        .map_err(|e| e.to_string())
}

/// Rebuild the server-side contact search index from stored contact data.
///
/// Useful after a fresh install or when the Tantivy index is out of sync.
/// Returns the number of contacts that were indexed.
#[tauri::command]
pub async fn rebuild_contact_index(
    account_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<u32, String> {
    let client = get_or_connect(&state).await?;
    client
        .rebuild_contact_index(account_id)
        .await
        .map_err(|e| e.to_string())
}

// ── Suggest ──────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct SuggestionDto {
    pub field: String,
    pub value: String,
    pub doc_count: u32,
}

fn suggestion_to_dto(s: Suggestion) -> SuggestionDto {
    SuggestionDto {
        field: s.field,
        value: s.value,
        doc_count: s.doc_count,
    }
}

#[tauri::command]
pub async fn suggest_messages(
    prefix: String,
    scope: Option<String>,
    max_per_field: Option<u32>,
    state: State<'_, AppState>,
) -> Result<Vec<SuggestionDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .suggest(&prefix, scope.as_deref(), max_per_field.unwrap_or(5))
        .await
        .map(|v| v.into_iter().map(suggestion_to_dto).collect())
        .map_err(|e| e.to_string())
}

// ── Conversations ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct ConversationSummaryDto {
    pub thread_id: String,
    pub account_id: String,
    pub subject: String,
    pub message_count: u32,
    pub unread_count: u32,
    pub last_message_at: String, // ISO 8601
    pub latest_sender: String,
    /// Domain message UUID — pass this to `get_thread`, NOT thread_id.
    pub sample_message_id: String,
}

fn conversation_to_dto(c: ConversationSummary) -> ConversationSummaryDto {
    ConversationSummaryDto {
        thread_id: c.thread_id,
        account_id: c.account_id,
        subject: c.subject,
        message_count: c.message_count,
        unread_count: c.unread_count,
        last_message_at: c.last_message_at.to_rfc3339(),
        latest_sender: c.latest_sender,
        sample_message_id: c.sample_message_id,
    }
}

#[tauri::command]
pub async fn list_conversations(
    account_id: String,
    mailbox_id: Option<String>,
    page: Option<u32>,
    page_size: Option<u32>,
    state: State<'_, AppState>,
) -> Result<Vec<ConversationSummaryDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .list_conversations(
            &account_id,
            mailbox_id.as_deref(),
            page.unwrap_or(1),
            page_size.unwrap_or(100),
        )
        .await
        .map(|v| v.into_iter().map(conversation_to_dto).collect())
        .map_err(|e| e.to_string())
}

#[derive(Serialize)]
pub struct ThreadMessageDto {
    pub id: String,
    pub rfc_message_id: Option<String>,
    pub from_name: Option<String>,
    pub from_email: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub subject: String,
    pub date: String, // ISO 8601
    pub is_read: bool,
    pub html_body: Option<String>,
    pub text_body: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
}

fn thread_message_to_dto(m: ThreadMessage) -> ThreadMessageDto {
    ThreadMessageDto {
        id: m.id,
        rfc_message_id: m.rfc_message_id,
        from_name: m.from_name,
        from_email: m.from_email,
        to: m.to,
        cc: m.cc,
        subject: m.subject,
        date: m.date.to_rfc3339(),
        is_read: m.is_read,
        html_body: m.html_body,
        text_body: m.text_body,
        in_reply_to: m.in_reply_to,
        references: m.references,
    }
}

#[tauri::command]
pub async fn get_thread(
    message_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<ThreadMessageDto>, String> {
    let client = get_or_connect(&state).await?;
    client
        .get_thread(&message_id)
        .await
        .map(|v| v.into_iter().map(thread_message_to_dto).collect())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::{DeepLink, INITIAL_WATCH_BACKOFF, MAX_WATCH_BACKOFF, next_backoff};

    fn link(a: &str, m: &str, msg: &str) -> DeepLink {
        DeepLink {
            account_id: a.into(),
            mailbox_id: m.into(),
            message_id: msg.into(),
        }
    }

    #[test]
    fn watch_backoff_doubles_and_caps() {
        let mut d = INITIAL_WATCH_BACKOFF;
        let mut seen = vec![d];
        for _ in 0..10 {
            d = next_backoff(d);
            seen.push(d);
        }
        assert_eq!(seen[1], INITIAL_WATCH_BACKOFF * 2, "the delay doubles");
        assert_eq!(d, MAX_WATCH_BACKOFF, "and stops at the ceiling");
        assert!(
            seen.windows(2).all(|w| w[1] >= w[0]),
            "never goes backwards: {seen:?}"
        );
    }

    #[test]
    fn parses_both_flag_spellings() {
        let expected = Some(link("acc", "mb", "msg"));
        assert_eq!(
            DeepLink::from_args(["--open-message", "acc/mb/msg"]),
            expected
        );
        assert_eq!(DeepLink::from_args(["--open-message=acc/mb/msg"]), expected);
        // Must still be found after unrelated arguments.
        assert_eq!(
            DeepLink::from_args(["--other", "x", "--open-message", "acc/mb/msg"]),
            expected
        );
    }

    #[test]
    fn round_trips_through_the_wire_form() {
        let original = link("acc", "mb", "msg");
        let parsed = DeepLink::from_args(["--open-message", &original.to_arg_value()]);
        assert_eq!(parsed, Some(original));
    }

    /// A malformed link must degrade to "just open the app", never panic or
    /// half-navigate — the notification path has no way to report an error.
    #[test]
    fn malformed_links_are_ignored() {
        assert_eq!(DeepLink::from_args(Vec::<String>::new()), None);
        assert_eq!(
            DeepLink::from_args(["--open-message"]),
            None,
            "flag with no value"
        );
        assert_eq!(
            DeepLink::from_args(["--open-message", "acc/mb"]),
            None,
            "too few parts"
        );
        assert_eq!(
            DeepLink::from_args(["--open-message", "a/b/c/d"]),
            None,
            "too many parts"
        );
        assert_eq!(
            DeepLink::from_args(["--open-message", "acc//msg"]),
            None,
            "empty part"
        );
        assert_eq!(DeepLink::from_args(["--open-message", "//"]), None);
        assert_eq!(
            DeepLink::from_args(["--open-messageXYZ", "a/b/c"]),
            None,
            "flag typo"
        );
    }
}
