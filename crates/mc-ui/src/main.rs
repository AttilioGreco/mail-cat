#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use commands::{AppState, DeepLink, PendingDeepLink, WatchState};
use tauri::{Emitter, Manager};
use tokio::sync::Mutex;

/// Event carrying a deep link to an already-running window.
const DEEP_LINK_EVENT: &str = "deep-link";

#[tokio::main]
async fn main() {
    // WebKitGTK's TLS stack enumerates every p11-kit PKCS#11 module at startup,
    // including tpm2-pkcs11, which spews TCTI/FAPI errors when no TPM is
    // accessible. mail-cat never touches the TPM — mute the tss2 log noise.
    // ponytail: unconditional; harmless even on machines with a working TPM.
    if std::env::var_os("TSS2_LOG").is_none() {
        // SAFETY: set before any code reads the env / spawns webview threads.
        unsafe { std::env::set_var("TSS2_LOG", "all+NONE") };
    }

    tracing_subscriber::fmt::init();

    let server_url =
        std::env::var("MAILCAT_SERVER_URL").unwrap_or_else(|_| "http://localhost:50051".to_owned());

    tracing::info!("Connecting to mc-server at {server_url}");

    // A notification click launches us with `--open-message <a>/<m>/<msg>`. On a
    // cold start the webview is not listening yet, so stash it and let the
    // frontend pull it once mounted.
    let startup_link = DeepLink::from_args(std::env::args().skip(1));
    if let Some(link) = &startup_link {
        tracing::info!("Startup deep link: {}", link.to_arg_value());
    }

    tauri::Builder::default()
        // Second launch (notification clicked while the client is open): forward
        // its argv here instead of opening another window, then focus and navigate.
        .plugin(tauri_plugin_single_instance::init(|app, argv, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.unminimize();
                let _ = window.show();
                let _ = window.set_focus();
            }
            match DeepLink::from_args(argv.iter().skip(1)) {
                Some(link) => {
                    tracing::info!("Deep link from second instance: {}", link.to_arg_value());
                    if let Err(e) = app.emit(DEEP_LINK_EVENT, link) {
                        tracing::warn!("Failed to emit deep link: {e}");
                    }
                }
                // Plain relaunch with no link — focusing the window is the whole job.
                None => tracing::debug!("Second instance with no deep link"),
            }
        }))
        .manage(AppState {
            client: Mutex::new(None), // lazy connect on first command
            server_url,
        })
        .manage(PendingDeepLink(Mutex::new(startup_link)))
        .manage(WatchState::new())
        .invoke_handler(tauri::generate_handler![
            commands::list_accounts,
            commands::account_stats,
            commands::add_imap_account,
            commands::remove_account,
            commands::sync_now,
            commands::start_sync_watch,
            commands::stop_sync_watch,
            commands::start_oauth_login,
            commands::reauth_account,
            commands::restart_server,
            commands::list_mailboxes,
            commands::list_messages,
            commands::get_message,
            commands::start_watching,
            commands::stop_watching,
            commands::set_flags,
            commands::mark_mailbox_read,
            commands::move_message,
            commands::delete_message,
            commands::delete_messages,
            commands::move_messages,
            commands::prepare_compose,
            commands::send_compose,
            commands::save_draft,
            commands::discard_draft,
            commands::search_messages,
            commands::search_contacts,
            commands::backfill_contacts_from_messages,
            commands::rebuild_contact_index,
            commands::suggest_messages,
            commands::list_conversations,
            commands::get_thread,
            commands::take_pending_deep_link,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
