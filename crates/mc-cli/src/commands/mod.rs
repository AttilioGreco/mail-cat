pub mod accounts;
pub mod contacts;
pub mod conversations;
pub mod labels;
pub mod mailboxes;
pub mod messages;
pub mod sync;
pub mod watch;

/// Machine-readable output for scripts and the itest harness.
pub fn print_json(value: &impl serde::Serialize) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// How list-style output is printed. `toon` is the compact form for
/// assistants (see `crate::toon`); commands without one print tables.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    Table,
    Json,
    Toon,
}
