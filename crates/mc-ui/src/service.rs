//! Registers the bundled `mail-cat-server` as a per-user autostart on launch, so a
//! fresh install syncs mail at login without anyone opening a terminal.

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tauri::{AppHandle, Emitter, Manager, State};

use crate::commands::AppState;
use mc_client::MailCatClient;

/// Emitted when the server could not be registered or started.
const ERROR_EVENT: &str = "server-service-error";

/// The start-up check runs in the background; the webview may not be listening
/// yet when it fails, so the message is also kept for the UI to pull.
#[derive(Default)]
pub struct ServiceState(Mutex<Option<String>>);

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Nothing,
    Start,
    Install,
}

/// Task Scheduler stores its own spelling of the path, and `current_exe` may
/// carry a `\\?\` verbatim prefix.
fn windows_key(p: &Path) -> String {
    let s = p.to_string_lossy();
    let s = s.strip_prefix(r"\\?\").unwrap_or(&s);
    s.trim_end_matches(['\\', '/'])
        .replace('/', "\\")
        .to_lowercase()
}

fn same_path(a: &Path, b: &Path) -> bool {
    if cfg!(windows) {
        return windows_key(a) == windows_key(b);
    }
    a == b
}

/// Maps `service status` (key=value lines: `registered`, `enabled`, `running`, and
/// `exe` when registered; exit 3 is an error) to what is needed to get `exe`
/// running at login. A unit that exists but is not enabled (deb/rpm) needs `install`.
fn plan(code: Option<i32>, stdout: &str, exe: &Path) -> Result<Action, String> {
    if !matches!(code, Some(0..=2)) {
        return Err(format!("`service status` exited with {code:?}"));
    }
    let field = |key: &str| {
        stdout
            .lines()
            .find_map(|l| l.trim().strip_prefix(key)?.strip_prefix('='))
    };
    let registered_here = field("enabled") == Some("yes")
        && field("exe").is_some_and(|p| same_path(Path::new(p.trim()), exe));
    match (registered_here, field("running")) {
        (true, Some("yes")) => Ok(Action::Nothing),
        (true, _) => Ok(Action::Start),
        // Absent, disabled, or registered to another executable (moved or updated app).
        (false, _) => Ok(Action::Install),
    }
}

/// Dev builds are run from the justfile, and a custom server URL means the user
/// manages that server themselves.
fn enabled() -> bool {
    if std::env::var_os("MAILCAT_MANAGE_SERVICE").is_some() {
        return true;
    }
    !cfg!(debug_assertions) && std::env::var_os("MAILCAT_SERVER_URL").is_none()
}

fn is_ephemeral(p: &Path) -> bool {
    let p = p.to_string_lossy();
    p.starts_with("/Volumes/") || p.contains("/AppTranslocation/")
}

fn bundled_server() -> Result<PathBuf, String> {
    let name = format!("mail-cat-server{}", std::env::consts::EXE_SUFFIX);
    let exe = std::env::current_exe().map_err(|e| format!("cannot locate the app: {e}"))?;
    let server = exe.with_file_name(name);
    if server.is_file() {
        return Ok(server);
    }
    Err(format!("bundled server not found at {}", server.display()))
}

/// An AppImage runs from a mount that disappears on exit, so the autostart cannot
/// point into it: the server lives in a stable copy instead.
fn appimage_copy(bundled: &Path) -> Option<PathBuf> {
    std::env::var_os("APPIMAGE")?;
    Some(
        dirs::data_local_dir()?
            .join("mail-cat")
            .join("bin")
            .join(bundled.file_name()?),
    )
}

fn same_contents(a: &Path, b: &Path) -> io::Result<bool> {
    if std::fs::metadata(a)?.len() != std::fs::metadata(b)?.len() {
        return Ok(false);
    }
    Ok(std::fs::read(a)? == std::fs::read(b)?)
}

/// Returns the path to register, refreshing the AppImage copy when it is stale.
/// The running old server is stopped first so the next step starts the new one.
fn registrable_server() -> Result<PathBuf, String> {
    let bundled = bundled_server()?;
    let Some(copy) = appimage_copy(&bundled) else {
        return Ok(bundled);
    };
    if copy.is_file() && same_contents(&bundled, &copy).unwrap_or(false) {
        return Ok(copy);
    }
    let _ = run(&copy, "stop");
    refresh_copy(&bundled, &copy).map_err(|e| format!("cannot copy the server: {e}"))?;
    Ok(copy)
}

fn refresh_copy(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::create_dir_all(to.parent().unwrap_or(Path::new(".")))?;
    // Rename over the target: overwriting a running executable fails with ETXTBSY.
    let tmp = to.with_extension("new");
    std::fs::copy(from, &tmp)?;
    std::fs::rename(tmp, to)
}

fn run(server: &Path, subcommand: &str) -> io::Result<Output> {
    let mut cmd = Command::new(server);
    cmd.args(["service", subcommand]);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd.output()
}

fn run_ok(server: &Path, subcommand: &str) -> Result<(), String> {
    let out = run(server, subcommand).map_err(|e| format!("`service {subcommand}`: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    Err(format!(
        "`service {subcommand}` failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    ))
}

/// Whether anything was registered or started (the server then needs a moment).
fn ensure_blocking() -> Result<bool, String> {
    let server = registrable_server()?;
    // The server refuses too; checking here gives the user an actionable message.
    if cfg!(target_os = "macos") && is_ephemeral(&server) {
        return Err("Sposta mail-cat nella cartella Applicazioni e riaprilo".into());
    }
    let status = run(&server, "status").map_err(|e| format!("`service status`: {e}"))?;
    let stdout = String::from_utf8_lossy(&status.stdout);
    match plan(status.status.code(), &stdout, &server)? {
        Action::Nothing => Ok(false),
        Action::Start => run_ok(&server, "start").map(|()| true),
        Action::Install => run_ok(&server, "install").map(|()| true),
    }
}

async fn wait_until_reachable(url: &str) {
    for _ in 0..40 {
        if MailCatClient::connect(url).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Never blocks the window: commands that connect meanwhile retry (see
/// `get_or_connect`) while `AppState::starting` is set.
pub fn spawn_ensure(app: &AppHandle) {
    if !enabled() {
        return;
    }
    let app = app.clone();
    app.state::<AppState>()
        .starting
        .store(true, Ordering::SeqCst);
    tauri::async_runtime::spawn(async move {
        let state = app.state::<AppState>();
        let outcome = tauri::async_runtime::spawn_blocking(ensure_blocking)
            .await
            .map_err(|e| e.to_string())
            .and_then(|r| r);
        match outcome {
            Ok(true) => wait_until_reachable(&state.server_url).await,
            Ok(false) => {}
            Err(message) => {
                tracing::error!("server autostart: {message}");
                *app.state::<ServiceState>().0.lock().unwrap() = Some(message.clone());
                if let Err(e) = app.emit(ERROR_EVENT, message) {
                    tracing::warn!("Failed to emit {ERROR_EVENT}: {e}");
                }
            }
        }
        state.starting.store(false, Ordering::SeqCst);
    });
}

#[tauri::command]
pub fn take_server_service_error(state: State<'_, ServiceState>) -> Option<String> {
    state.0.lock().unwrap().take()
}

/// Windows cannot replace an executable that is running, so the updater stops the
/// server before installing; the relaunched app starts it again from the new files.
#[tauri::command]
pub async fn stop_server_service() -> Result<(), String> {
    if !enabled() {
        return Ok(());
    }
    tauri::async_runtime::spawn_blocking(|| {
        let bundled = bundled_server()?;
        let server = appimage_copy(&bundled)
            .filter(|c| c.is_file())
            .unwrap_or(bundled);
        run_ok(&server, "stop")
    })
    .await
    .map_err(|e| e.to_string())?
}

/// Re-runs the start-up check, for when an update failed after the server was stopped.
#[tauri::command]
pub fn ensure_server_service(app: AppHandle) {
    spawn_ensure(&app);
}

#[cfg(test)]
mod tests {
    use super::{Action, plan};
    use std::path::Path;

    const EXE: &str = "/usr/bin/mail-cat-server";

    fn status(running: &str) -> String {
        format!("registered=yes\nenabled=yes\nrunning={running}\nexe={EXE}\n")
    }

    #[test]
    fn running_here_needs_nothing() {
        assert_eq!(
            plan(Some(0), &status("yes"), Path::new(EXE)),
            Ok(Action::Nothing)
        );
    }

    #[test]
    fn stopped_here_is_started() {
        assert_eq!(
            plan(Some(1), &status("no"), Path::new(EXE)),
            Ok(Action::Start)
        );
    }

    #[test]
    fn absent_or_elsewhere_is_reinstalled() {
        assert_eq!(
            plan(
                Some(2),
                "registered=no\nenabled=no\nrunning=no\n",
                Path::new(EXE)
            ),
            Ok(Action::Install)
        );
        assert_eq!(
            plan(Some(0), &status("yes"), Path::new("/new/mail-cat-server")),
            Ok(Action::Install)
        );
    }

    #[test]
    fn packaged_but_disabled_is_installed() {
        let s = format!("registered=yes\nenabled=no\nrunning=no\nexe={EXE}\n");
        assert_eq!(plan(Some(2), &s, Path::new(EXE)), Ok(Action::Install));
    }

    #[test]
    fn windows_paths_compare_loosely() {
        let k = super::windows_key;
        assert_eq!(
            k(Path::new(r"\\?\C:\Apps\Mail Cat\")),
            k(Path::new("c:/apps/mail cat"))
        );
        assert_ne!(k(Path::new(r"C:\a")), k(Path::new(r"C:\b")));
    }

    #[test]
    fn error_exit_is_an_error() {
        assert!(plan(Some(3), "", Path::new(EXE)).is_err());
        assert!(plan(None, "", Path::new(EXE)).is_err());
    }
}
