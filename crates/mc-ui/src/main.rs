#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use commands::{AppState, WatchState};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let server_url = std::env::var("MAILCAT_SERVER_URL")
        .unwrap_or_else(|_| "http://localhost:50051".to_owned());

    tracing::info!("Connecting to mc-server at {server_url}");

    tauri::Builder::default()
        .manage(AppState {
            client: Mutex::new(None), // lazy connect on first command
            server_url,
        })
        .manage(WatchState::new())
        .invoke_handler(tauri::generate_handler![
            commands::list_accounts,
            commands::list_mailboxes,
            commands::list_messages,
            commands::get_message,
            commands::start_watching,
            commands::stop_watching,
            commands::set_flags,
            commands::move_message,
            commands::delete_message,
            commands::send_message,
            commands::search_messages,
            commands::suggest_messages,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
