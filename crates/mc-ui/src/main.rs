#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use commands::{AppState, DeepLink, PendingDeepLink, WatchState};
use tauri::{Emitter, Manager};
use tokio::sync::Mutex;

/// Event carrying a deep link to an already-running window.
const DEEP_LINK_EVENT: &str = "deep-link";

/// Whether the main window gets the native titlebar (close/minimise/maximise,
/// drag to move). Tiling compositors tile and frame windows themselves, so there
/// the GTK titlebar is dead weight; floating desktops (GNOME, Cinnamon, KDE…)
/// need it, since the frontend draws no titlebar of its own.
/// `MAILCAT_DECORATIONS=0|1` overrides the detection.
fn wants_decorations(env: impl Fn(&str) -> Option<String>) -> bool {
    match env("MAILCAT_DECORATIONS").as_deref() {
        Some("0" | "false" | "no" | "off") => return false,
        Some("1" | "true" | "yes" | "on") => return true,
        _ => {}
    }
    const TILING_SOCKETS: [&str; 4] = [
        "SWAYSOCK",
        "I3SOCK",
        "HYPRLAND_INSTANCE_SIGNATURE",
        "NIRI_SOCKET",
    ];
    const TILING_DESKTOPS: [&str; 6] = ["sway", "i3", "hyprland", "niri", "river", "bspwm"];
    if TILING_SOCKETS.iter().any(|var| env(var).is_some()) {
        return false;
    }
    // XDG_CURRENT_DESKTOP is a colon-separated list, e.g. "sway" or "X-Cinnamon".
    let desktop = env("XDG_CURRENT_DESKTOP")
        .unwrap_or_default()
        .to_lowercase();
    !desktop.split(':').any(|d| TILING_DESKTOPS.contains(&d))
}

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
        // tauri.conf.json starts the window undecorated (the tiling-WM case);
        // turn the titlebar back on for floating desktops.
        .setup(|app| {
            if wants_decorations(|var| std::env::var(var).ok())
                && let Some(window) = app.get_webview_window("main")
            {
                window.set_decorations(true)?;
            }
            Ok(())
        })
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
            commands::start_auth_watch,
            commands::stop_auth_watch,
            commands::start_oauth_login,
            commands::reauth_account,
            commands::set_account_password,
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

#[cfg(test)]
mod tests {
    use super::wants_decorations;

    fn env<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn floating_desktops_get_a_titlebar() {
        assert!(wants_decorations(env(&[("XDG_CURRENT_DESKTOP", "GNOME")])));
        assert!(wants_decorations(env(&[(
            "XDG_CURRENT_DESKTOP",
            "X-Cinnamon"
        )])));
        assert!(wants_decorations(env(&[])));
    }

    #[test]
    fn tiling_compositors_stay_undecorated() {
        assert!(!wants_decorations(env(&[(
            "SWAYSOCK",
            "/run/user/1000/sway-ipc.sock"
        )])));
        assert!(!wants_decorations(env(&[("XDG_CURRENT_DESKTOP", "sway")])));
        assert!(!wants_decorations(env(&[(
            "XDG_CURRENT_DESKTOP",
            "Hyprland"
        )])));
    }

    #[test]
    fn the_env_override_wins() {
        assert!(wants_decorations(env(&[
            ("SWAYSOCK", "x"),
            ("MAILCAT_DECORATIONS", "1")
        ])));
        assert!(!wants_decorations(env(&[
            ("XDG_CURRENT_DESKTOP", "GNOME"),
            ("MAILCAT_DECORATIONS", "0")
        ])));
    }
}
