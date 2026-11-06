//! Scheduled Task at logon of the current user, via PowerShell's ScheduledTasks
//! module (schtasks cannot express restart-on-failure or "no time limit").
#![cfg_attr(not(windows), allow(dead_code))]

use std::path::Path;
use std::process::Command;

use super::{Status, check, kv};

const TASK: &str = "MailCatServer";

/// Piped PowerShell output is in the OEM codepage by default, which garbles
/// non-ASCII paths when decoded as UTF-8.
fn utf8_script(script: &str) -> String {
    format!(
        "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; $OutputEncoding = [System.Text.Encoding]::UTF8; {script}"
    )
}

fn powershell(script: &str) -> anyhow::Result<String> {
    check(Command::new("powershell").args([
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        &utf8_script(script),
    ]))
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn register_script(exe: &Path) -> String {
    let exe = quote(&exe.display().to_string());
    // Interactive logon type = runs only while the user is logged on, which is
    // what gives the process the user's Credential Manager. Restart interval
    // 1 min is the minimum Task Scheduler accepts; the time limit is lifted
    // because the default kills tasks after 72 h.
    format!(
        "$ErrorActionPreference = 'Stop'
$user = $env:USERDOMAIN + '\\' + $env:USERNAME
Stop-ScheduledTask -TaskName '{TASK}' -ErrorAction SilentlyContinue
$action = New-ScheduledTaskAction -Execute {exe}
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -Hidden -MultipleInstances IgnoreNew -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName '{TASK}' -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName '{TASK}'"
    )
}

pub fn install(exe: &Path) -> anyhow::Result<()> {
    powershell(&register_script(exe)).map(drop)
}

pub fn uninstall() -> anyhow::Result<()> {
    powershell(&format!(
        "$ErrorActionPreference = 'Stop'
if (Get-ScheduledTask -TaskName '{TASK}' -ErrorAction SilentlyContinue) {{
  Stop-ScheduledTask -TaskName '{TASK}' -ErrorAction SilentlyContinue
  Unregister-ScheduledTask -TaskName '{TASK}' -Confirm:$false
}}"
    ))
    .map(drop)
}

pub fn start() -> anyhow::Result<()> {
    powershell(&format!(
        "$ErrorActionPreference = 'Stop'; Start-ScheduledTask -TaskName '{TASK}'"
    ))
    .map(drop)
}

pub fn stop() -> anyhow::Result<()> {
    powershell(&format!(
        "$ErrorActionPreference = 'Stop'; Stop-ScheduledTask -TaskName '{TASK}'"
    ))
    .map(drop)
}

pub fn status() -> anyhow::Result<Status> {
    let out = powershell(&format!(
        "$t = Get-ScheduledTask -TaskName '{TASK}' -ErrorAction SilentlyContinue
if (-not $t) {{ 'registered=no'; exit 0 }}
'registered=yes'; 'state=' + $t.State; 'exe=' + $t.Actions[0].Execute"
    ))?;
    Ok(parse_status(&out))
}

fn parse_status(text: &str) -> Status {
    if kv(text, "registered") != Some("yes") {
        return Status {
            exe: None,
            enabled: false,
            running: false,
        };
    }
    Status {
        exe: kv(text, "exe").map(str::to_string),
        enabled: kv(text, "state") != Some("Disabled"),
        running: kv(text, "state") == Some("Running"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_definition_is_per_user_restarting_and_unlimited() {
        let s = register_script(Path::new(
            r"C:\Users\o'neil\AppData\Local\Mail Cat\mail-cat-server.exe",
        ));
        assert!(s.contains(r"New-ScheduledTaskAction -Execute 'C:\Users\o''neil\AppData\Local\Mail Cat\mail-cat-server.exe'"));
        for want in [
            "-AtLogOn -User $user",
            "-LogonType Interactive",
            "-RunLevel Limited",
            "-RestartCount 999",
            "-ExecutionTimeLimit ([TimeSpan]::Zero)",
            "-Hidden",
        ] {
            assert!(s.contains(want), "{want}");
        }
        assert!(
            s.trim_end()
                .ends_with("Start-ScheduledTask -TaskName 'MailCatServer'")
        );
    }

    #[test]
    fn scripts_force_utf8_output() {
        let s = utf8_script("Get-Date");
        assert!(s.starts_with("[Console]::OutputEncoding = [System.Text.Encoding]::UTF8;"));
        assert!(s.contains("$OutputEncoding = [System.Text.Encoding]::UTF8;"));
        assert!(s.ends_with("Get-Date"));
    }

    #[test]
    fn status_parsing() {
        let s = parse_status("registered=yes\r\nstate=Running\r\nexe=C:\\a\\b.exe\r\n");
        assert!(s.running);
        assert_eq!(s.exe.as_deref(), Some("C:\\a\\b.exe"));
        assert!(s.enabled);
        assert!(!parse_status("registered=yes\nstate=Ready\nexe=x\n").running);
        assert!(!parse_status("registered=yes\nstate=Disabled\nexe=x\n").enabled);
        assert!(parse_status("registered=no\n").exe.is_none());
    }
}
