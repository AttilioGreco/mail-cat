use std::collections::HashMap;
use serde::Serialize;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_stream::StreamExt;

use mc_client::{
    Account, ContactHit, ConversationSummary, Mailbox, MailCatClient, MailboxNotificationKind,
    MessageBody, MessageFlag, MessageSummary, SearchHit, SendMessageParams, Suggestion,
    ThreadMessage,
};

/// Tauri managed state — single shared client instance.
pub struct AppState {
    pub client: Mutex<Option<MailCatClient>>,
    pub server_url: String,
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
        Self { watchers: Mutex::new(HashMap::new()) }
    }
}

// ── Serializable DTOs (mirrors mc-client types for JS) ──────────────────────

#[derive(Serialize)]
pub struct AccountDto {
    pub id: String,
    pub name: String,
    pub email: String,
    pub is_active: bool,
}

#[derive(Serialize)]
pub struct MailboxDto {
    pub id: String,
    pub account_id: String,
    pub path: String,
    pub display_name: String,
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
    pub kind: String,          // "counts_changed" | "new_message" | "message_expunged" | "flags_changed"
    pub total: Option<u32>,
    pub unseen: Option<u32>,
    pub provider_uid: Option<u32>,
}

// ── Conversions ──────────────────────────────────────────────────────────────

fn account_to_dto(a: Account) -> AccountDto {
    AccountDto { id: a.id, name: a.name, email: a.email, is_active: a.is_active }
}

fn mailbox_to_dto(m: Mailbox) -> MailboxDto {
    MailboxDto {
        id: m.id,
        account_id: m.account_id,
        path: m.path,
        display_name: m.display_name,
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

        let stream = match client.watch_mailbox(&mid).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("watch_mailbox failed for {mid}: {e}");
                return;
            }
        };

        tokio::pin!(stream);

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    tracing::info!("Mailbox watcher cancelled for {mid}");
                    break;
                }
                item = stream.next() => {
                    match item {
                        None => {
                            tracing::warn!("Mailbox watch stream ended for {mid}");
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::error!("Mailbox watch error for {mid}: {e}");
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

                            tracing::info!("[tauri] Emitting mailbox:event kind={} mailbox_id={}", payload.kind, payload.mailbox_id);
                            if let Err(e) = app.emit("mailbox:event", &payload) {
                                tracing::warn!("Failed to emit mailbox:event: {e}");
                            }
                        }
                    }
                }
            }
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
    let remove: Vec<MessageFlag> = flags_to_remove.iter().filter_map(|s| str_to_flag(s)).collect();
    client
        .set_flags(&message_id, &add, &remove)
        .await
        .map(message_to_dto)
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

#[tauri::command]
pub async fn send_message(
    account_id: String,
    from_email: String,
    to: Vec<String>,
    cc: Vec<String>,
    bcc: Vec<String>,
    subject: String,
    body_text: Option<String>,
    body_html: Option<String>,
    in_reply_to: Option<String>,
    references: Vec<String>,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let client = get_or_connect(&state).await?;
    client
        .send_message(SendMessageParams {
            account_id,
            from_email,
            from_name: None,
            to,
            cc,
            bcc,
            subject,
            body_text,
            body_html,
            in_reply_to,
            references,
        })
        .await
        .map_err(|e| e.to_string())
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
    SuggestionDto { field: s.field, value: s.value, doc_count: s.doc_count }
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
