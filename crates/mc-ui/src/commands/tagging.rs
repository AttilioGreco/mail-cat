//! Tauri commands for label suggestion; thin wrappers over the gRPC client.

use mc_client::{
    ApplyPreview, BackgroundStats, EmbeddingsStatus, LabelStatus, MessageTags, TrainingGroup,
};
use tauri::State;

use super::{AppState, get_or_connect};

fn err(e: impl ToString) -> String {
    e.to_string()
}

#[tauri::command]
pub async fn list_training_groups(
    state: State<'_, AppState>,
) -> Result<Vec<TrainingGroup>, String> {
    get_or_connect(&state)
        .await?
        .list_training_groups()
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn create_training_group(
    name: String,
    state: State<'_, AppState>,
) -> Result<TrainingGroup, String> {
    get_or_connect(&state)
        .await?
        .create_training_group(&name)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn rename_training_group(
    id: String,
    name: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .rename_training_group(&id, &name)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn delete_training_group(id: String, state: State<'_, AppState>) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .delete_training_group(&id)
        .await
        .map_err(err)
}

/// `group_id` `None` takes the account out of every group.
#[tauri::command]
pub async fn assign_account_group(
    account_id: String,
    group_id: Option<String>,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .assign_account_group(&account_id, group_id)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn get_label_settings(
    account_id: String,
    label: String,
    state: State<'_, AppState>,
) -> Result<LabelStatus, String> {
    get_or_connect(&state)
        .await?
        .get_label_settings(&account_id, &label)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn set_label_settings(
    account_id: String,
    label: String,
    mode: String,
    threshold: f64,
    state: State<'_, AppState>,
) -> Result<LabelStatus, String> {
    get_or_connect(&state)
        .await?
        .set_label_settings(&account_id, &label, &mode, threshold)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn get_message_tags(
    message_id: String,
    state: State<'_, AppState>,
) -> Result<MessageTags, String> {
    get_or_connect(&state)
        .await?
        .get_message_tags(&message_id)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn accept_suggestion(
    message_id: String,
    label: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .accept_suggestion(&message_id, &label)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn reject_suggestion(
    message_id: String,
    label: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .reject_suggestion(&message_id, &label)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn undo_auto_label(
    message_id: String,
    label: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    get_or_connect(&state)
        .await?
        .undo_auto_label(&message_id, &label)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn preview_apply(
    account_id: String,
    label: String,
    mode: String,
    threshold: f64,
    state: State<'_, AppState>,
) -> Result<ApplyPreview, String> {
    get_or_connect(&state)
        .await?
        .preview_apply(&account_id, &label, &mode, threshold)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn confirm_apply(
    account_id: String,
    label: String,
    mode: String,
    threshold: f64,
    state: State<'_, AppState>,
) -> Result<ApplyPreview, String> {
    get_or_connect(&state)
        .await?
        .confirm_apply(&account_id, &label, &mode, threshold)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn get_embeddings(state: State<'_, AppState>) -> Result<EmbeddingsStatus, String> {
    get_or_connect(&state)
        .await?
        .get_embeddings()
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn set_embeddings(
    enabled: bool,
    state: State<'_, AppState>,
) -> Result<EmbeddingsStatus, String> {
    get_or_connect(&state)
        .await?
        .set_embeddings(enabled)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn set_background_speed(
    speed: String,
    state: State<'_, AppState>,
) -> Result<EmbeddingsStatus, String> {
    get_or_connect(&state)
        .await?
        .set_background_speed(&speed)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn set_embedding_model(
    profile: String,
    state: State<'_, AppState>,
) -> Result<EmbeddingsStatus, String> {
    get_or_connect(&state)
        .await?
        .set_embedding_model(&profile)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn set_execution_target(
    target: String,
    state: State<'_, AppState>,
) -> Result<EmbeddingsStatus, String> {
    get_or_connect(&state)
        .await?
        .set_execution_target(&target)
        .await
        .map_err(err)
}

#[tauri::command]
pub async fn get_background_stats(state: State<'_, AppState>) -> Result<BackgroundStats, String> {
    get_or_connect(&state)
        .await?
        .get_background_stats()
        .await
        .map_err(err)
}
