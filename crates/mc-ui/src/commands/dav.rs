//! Tauri commands for contacts and calendars; thin wrappers over the gRPC
//! client, whose types already serialize the way the UI wants them.

use chrono::{DateTime, Utc};
use mc_client::{
    AddressBook, Calendar, ClientError, ContactCard, ContactFields, EventChanges, EventDetail,
    EventOccurrence, NewEvent, RemoteContact,
};
use serde::Deserialize;
use tauri::{AppHandle, Emitter, State};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use super::{
    AppState, INITIAL_WATCH_BACKOFF, WatchState, get_or_connect, next_backoff, stop_watching,
};

fn err(e: impl ToString) -> String {
    e.to_string()
}

/// The two refusals the UI words itself; anything else is shown as it is.
fn subscription_err(e: ClientError) -> String {
    match e {
        ClientError::Grpc(s) if s.code() == tonic::Code::NotFound => "not_found".into(),
        ClientError::Grpc(s) if s.code() == tonic::Code::PermissionDenied => "forbidden".into(),
        e => e.to_string(),
    }
}

/// Times are RFC 3339 instants, or `YYYY-MM-DD` for an all-day event.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventInput {
    calendar: String,
    summary: String,
    start: String,
    end: String,
    location: Option<String>,
    description: Option<String>,
}

/// Absent fields are left as they are; an empty location or description
/// clears it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventChangesInput {
    summary: Option<String>,
    location: Option<String>,
    start: Option<String>,
    end: Option<String>,
    description: Option<String>,
}

/// Absent fields are left as they are; an empty organization or notes
/// clears it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactFieldsInput {
    display_name: Option<String>,
    emails: Option<Vec<String>>,
    phones: Option<Vec<String>>,
    organization: Option<String>,
    notes: Option<String>,
}

impl From<ContactFieldsInput> for ContactFields {
    fn from(f: ContactFieldsInput) -> Self {
        Self {
            display_name: f.display_name,
            emails: f.emails,
            phones: f.phones,
            organization: f.organization,
            notes: f.notes,
        }
    }
}

#[tauri::command]
pub async fn list_calendars(
    account_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<Calendar>, String> {
    get_or_connect(&state)
        .await?
        .list_calendars(&account_id)
        .await
        .map_err(err)
}

/// Occurrences overlapping `[from, to)` across the account's calendars.
#[tauri::command]
pub async fn list_events(
    account_id: String,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    state: State<'_, AppState>,
) -> Result<Vec<EventOccurrence>, String> {
    get_or_connect(&state)
        .await?
        .list_events(&account_id, vec![], from, to)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn get_event(
    account_id: String,
    href: String,
    state: State<'_, AppState>,
) -> Result<EventDetail, String> {
    get_or_connect(&state)
        .await?
        .get_event(&account_id, &href)
        .await
        .map_err(err)
}

/// Returns the new event's href.
#[tauri::command]
pub async fn create_event(
    account_id: String,
    event: EventInput,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let event = NewEvent {
        calendar: event.calendar,
        summary: event.summary,
        start: event.start,
        end: event.end,
        location: event.location,
        description: event.description,
    };
    get_or_connect(&state)
        .await?
        .create_event(&account_id, event)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn update_event(
    account_id: String,
    href: String,
    changes: EventChangesInput,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let changes = EventChanges {
        summary: changes.summary,
        location: changes.location,
        start: changes.start,
        end: changes.end,
        description: changes.description,
    };
    get_or_connect(&state)
        .await?
        .update_event(&account_id, &href, changes)
        .await
        .map_err(err)
}

/// Returns before the calendars show up: they come with the sync pass this starts.
#[tauri::command]
pub async fn subscribe_calendar(
    account_id: String,
    email: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .subscribe_calendar(&account_id, &email)
        .await
        .map_err(subscription_err)
}

#[tauri::command]
pub async fn unsubscribe_calendar(
    account_id: String,
    collection: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .unsubscribe_calendar(&account_id, &collection)
        .await
        .map_err(err)
}

/// Runs a contacts and calendars pass now; the pass itself is followed through
/// `dav:changed` and the account's auth status.
#[tauri::command]
pub async fn sync_dav_now(
    account_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .sync_dav_now(account_id.as_deref())
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn delete_event(
    account_id: String,
    href: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .delete_event(&account_id, &href)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn list_address_books(
    account_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<AddressBook>, String> {
    get_or_connect(&state)
        .await?
        .list_address_books(&account_id)
        .await
        .map_err(err)
}

/// `address_book` of `None` lists every book of the account.
#[tauri::command]
pub async fn list_remote_contacts(
    account_id: String,
    address_book: Option<String>,
    state: State<'_, AppState>,
) -> Result<Vec<RemoteContact>, String> {
    get_or_connect(&state)
        .await?
        .list_remote_contacts(&account_id, address_book)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn get_remote_contact(
    contact_id: String,
    state: State<'_, AppState>,
) -> Result<RemoteContact, String> {
    get_or_connect(&state)
        .await?
        .get_remote_contact(&contact_id)
        .await
        .map_err(err)
}

/// Returns the new contact's id.
#[tauri::command]
pub async fn create_contact(
    account_id: String,
    address_book: String,
    fields: ContactFieldsInput,
    state: State<'_, AppState>,
) -> Result<String, String> {
    get_or_connect(&state)
        .await?
        .create_remote_contact(&account_id, &address_book, fields.into())
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn update_contact(
    contact_id: String,
    fields: ContactFieldsInput,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .update_remote_contact(&contact_id, fields.into())
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn delete_contact(contact_id: String, state: State<'_, AppState>) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .delete_remote_contact(&contact_id)
        .await
        .map_err(err)
}

/// The card for an address, preferring a contact of `account_id`; `None`
/// when nobody has it.
#[tauri::command]
pub async fn lookup_contact(
    email: String,
    account_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<Option<ContactCard>, String> {
    get_or_connect(&state)
        .await?
        .lookup_contact(&email, account_id)
        .await
        .map_err(err)
}

/// Contacts that live only here, for the "Dalle mail" group.
#[tauri::command]
pub async fn list_harvested_contacts(
    state: State<'_, AppState>,
) -> Result<Vec<ContactCard>, String> {
    get_or_connect(&state)
        .await?
        .list_harvested_contacts()
        .await
        .map_err(err)
}

const DAV_WATCH_KEY: &str = "__dav_changes__";

/// Forward the server's coalesced contacts/calendar change signals as
/// `dav:changed` Tauri events. Ref-counted like the other watchers.
#[tauri::command]
pub async fn start_dav_watch(
    app: AppHandle,
    state: State<'_, AppState>,
    watch_state: State<'_, WatchState>,
) -> Result<(), String> {
    let mut watchers = watch_state.watchers.lock().await;
    if let Some((_, ref_count)) = watchers.get_mut(DAV_WATCH_KEY) {
        *ref_count += 1;
        return Ok(());
    }

    let client = get_or_connect(&state).await?;
    let token = CancellationToken::new();
    watchers.insert(DAV_WATCH_KEY.to_string(), (token.clone(), 1));
    drop(watchers);

    tokio::spawn(async move {
        let mut backoff = INITIAL_WATCH_BACKOFF;
        'reconnect: while !token.is_cancelled() {
            match client.watch_dav_changes().await {
                Ok(stream) => {
                    backoff = INITIAL_WATCH_BACKOFF;
                    tokio::pin!(stream);
                    loop {
                        tokio::select! {
                            _ = token.cancelled() => break 'reconnect,
                            item = stream.next() => match item {
                                Some(Ok(change)) => {
                                    if let Err(e) = app.emit("dav:changed", change) {
                                        tracing::warn!("Failed to emit dav:changed: {e}");
                                    }
                                }
                                Some(Err(e)) => {
                                    tracing::warn!("dav change stream error: {e}, reconnecting");
                                    break;
                                }
                                None => break,
                            },
                        }
                    }
                }
                Err(e) => tracing::warn!("watch_dav_changes failed: {e}, retrying"),
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
pub async fn stop_dav_watch(watch_state: State<'_, WatchState>) -> Result<(), String> {
    stop_watching(DAV_WATCH_KEY.to_string(), watch_state).await
}
