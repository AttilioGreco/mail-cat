//! systemd user unit.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

use super::{Status, capture, check, kv};
use std::process::Command;

const UNIT: &str = "mail-cat-server.service";
/// Where deb/rpm ship their unit.
const PACKAGED_DIRS: [&str; 2] = ["/usr/lib/systemd/user", "/etc/systemd/user"];

fn systemctl(args: &[&str]) -> anyhow::Result<String> {
    check(Command::new("systemctl").arg("--user").args(args))
}

fn user_unit_path() -> anyhow::Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("cannot locate the config directory")?
        .join("systemd/user")
        .join(UNIT))
}

fn packaged_unit_exists() -> bool {
    PACKAGED_DIRS
        .iter()
        .any(|d| Path::new(d).join(UNIT).exists())
}

fn unit_text(exe: &Path) -> String {
    // ExecStart is parsed by systemd: quote the path and double `%` specifiers.
    let exe = exe
        .display()
        .to_string()
        .replace('%', "%%")
        .replace('"', "\\\"");
    format!(
        "\
[Unit]
Description=Mail Cat - gRPC email server
After=network.target

[Service]
Type=simple
WorkingDirectory=%h
ExecStart=\"{exe}\"
Restart=on-failure
RestartSec=5
# glibc gives every thread that allocates concurrently its own 64 MB arena and
# rarely hands the memory back; two arenas are plenty for this server.
Environment=MALLOC_ARENA_MAX=2
StandardOutput=journal
StandardError=journal
SyslogIdentifier=mail-cat-server

[Install]
WantedBy=default.target
"
    )
}

pub fn install(exe: &Path) -> anyhow::Result<()> {
    let unit = user_unit_path()?;
    // A packaged unit already points at the packaged binary: enable it instead
    // of shadowing it with a second copy, unless a user unit already exists.
    if !unit.exists() && packaged_unit_exists() {
        systemctl(&["enable", "--now", UNIT])?;
        return Ok(());
    }
    std::fs::create_dir_all(unit.parent().context("unit path has no parent")?)?;
    std::fs::write(&unit, unit_text(exe))
        .with_context(|| format!("cannot write {}", unit.display()))?;
    systemctl(&["daemon-reload"])?;
    systemctl(&["enable", UNIT])?;
    // restart, not start: a running instance may come from the previous binary.
    systemctl(&["restart", UNIT])?;
    Ok(())
}

pub fn uninstall() -> anyhow::Result<()> {
    let unit = user_unit_path()?;
    if !unit.exists() && !packaged_unit_exists() {
        return Ok(());
    }
    systemctl(&["disable", "--now", UNIT])?;
    if unit.exists() {
        std::fs::remove_file(&unit)?;
        systemctl(&["daemon-reload"])?;
    }
    Ok(())
}

pub fn start() -> anyhow::Result<()> {
    systemctl(&["start", UNIT]).map(drop)
}

pub fn stop() -> anyhow::Result<()> {
    systemctl(&["stop", UNIT]).map(drop)
}

pub fn status() -> anyhow::Result<Status> {
    let out = capture(Command::new("systemctl").args([
        "--user",
        "show",
        UNIT,
        "-p",
        "LoadState",
        "-p",
        "ActiveState",
        "-p",
        "ExecStart",
        "-p",
        "UnitFileState",
    ]))?;
    if !out.status.success() {
        bail!(
            "systemctl show failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(parse_show(&String::from_utf8_lossy(&out.stdout)))
}

/// `systemctl show` exits 0 for unknown units and reports `LoadState=not-found`.
fn parse_show(text: &str) -> Status {
    if kv(text, "LoadState") != Some("loaded") {
        return Status {
            exe: None,
            enabled: false,
            running: false,
        };
    }
    // ExecStart={ path=/usr/bin/x ; argv[]=/usr/bin/x ; ignore_errors=no ; ... }
    let exe = kv(text, "ExecStart")
        .and_then(|v| v.split_once("path=")?.1.split_once(" ;"))
        .map(|(p, _)| p.to_string());
    Status {
        exe,
        // `static` units cannot be enabled and `enabled-runtime` is gone after a reboot.
        enabled: kv(text, "UnitFileState") == Some("enabled"),
        running: kv(text, "ActiveState") == Some("active"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_runs_current_exe_and_restarts_on_failure() {
        let t = unit_text(Path::new("/opt/my app/mail-cat-server"));
        assert!(t.contains("ExecStart=\"/opt/my app/mail-cat-server\"\n"));
        for want in [
            "Restart=on-failure",
            "RestartSec=5",
            "MALLOC_ARENA_MAX=2",
            "WantedBy=default.target",
            "StandardOutput=journal",
        ] {
            assert!(t.contains(want), "{want}");
        }
    }

    #[test]
    fn show_output_parsing() {
        let loaded = "LoadState=loaded\nActiveState=active\nUnitFileState=enabled\nExecStart={ path=/usr/bin/mail-cat-server ; argv[]=/usr/bin/mail-cat-server ; ignore_errors=no }\n";
        let s = parse_show(loaded);
        assert_eq!(s.exe.as_deref(), Some("/usr/bin/mail-cat-server"));
        assert!(s.running && s.enabled);
        // deb/rpm ship the unit disabled.
        let packaged = loaded
            .replace("=enabled", "=disabled")
            .replace("=active", "=inactive");
        let s = parse_show(&packaged);
        assert!(s.exe.is_some() && !s.enabled && !s.running);
        assert!(!parse_show(&loaded.replace("=enabled", "=static")).enabled);
        assert!(!parse_show(&loaded.replace("=active", "=failed")).running);
        assert!(
            parse_show("LoadState=not-found\nActiveState=inactive\n")
                .exe
                .is_none()
        );
    }
}
