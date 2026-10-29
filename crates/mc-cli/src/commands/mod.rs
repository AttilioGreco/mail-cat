pub mod accounts;
pub mod contacts;
pub mod conversations;
pub mod mailboxes;
pub mod messages;
pub mod sync;
pub mod watch;

/// Machine-readable output for scripts and the itest harness.
pub fn print_json(value: &impl serde::Serialize) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
