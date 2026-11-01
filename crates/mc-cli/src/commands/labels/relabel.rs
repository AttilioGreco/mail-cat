//! `mc labels rename|delete`: change an existing label on every account of
//! its training group.

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use mc_client::{LabelChange, MailCatClient};

use super::super::{Format, print_json};
use super::{proposals::optional_account_id, render};

#[derive(Args)]
pub struct RenameArgs {
    /// Label to rename, any case
    from: String,
    /// New name, a label path such as `Dev/GitHub`. A label that exists
    /// already is merged into
    to: String,
    /// Account ID, name or email; selects the training group (needed when
    /// several have the label)
    #[arg(long)]
    account: Option<String>,
    /// Move only the mail of `from` itself into `to`; `from` and its child
    /// labels stay, and `from` goes once it is empty and childless. `to` may
    /// be inside `from`
    #[arg(long)]
    mail_only: bool,
    /// Do it (in the background); without it only prints what would happen
    #[arg(long)]
    yes: bool,
}

#[derive(Args)]
pub struct DeleteArgs {
    /// Label to delete, any case. Its mail is kept
    label: String,
    /// Account ID, name or email; selects the training group (needed when
    /// several have the label)
    #[arg(long)]
    account: Option<String>,
    /// Do it (in the background); without it only prints what would happen
    #[arg(long)]
    yes: bool,
}

pub async fn rename(args: RenameArgs, client: MailCatClient, fmt: Format) -> Result<()> {
    let account = optional_account_id(&client, args.account.as_deref()).await?;
    let change = client
        .rename_label(
            &args.from,
            &args.to,
            account.as_deref(),
            args.mail_only,
            !args.yes,
        )
        .await?;
    show(&change, fmt)
}

pub async fn delete(args: DeleteArgs, client: MailCatClient, fmt: Format) -> Result<()> {
    let account = optional_account_id(&client, args.account.as_deref()).await?;
    let change = client
        .delete_label(&args.label, account.as_deref(), !args.yes)
        .await?;
    show(&change, fmt)
}

fn show(c: &LabelChange, fmt: Format) -> Result<()> {
    match fmt {
        Format::Json => return print_json(c),
        Format::Toon => {
            render::label_change(c).print();
            return Ok(());
        }
        Format::Table => {}
    }
    match &c.into {
        Some(to) if c.merge => println!("Merges {:?} into the existing {to:?}", c.label),
        Some(to) => println!("Renames {:?} to {to:?}", c.label),
        None => println!("Deletes {:?}; its mail stays", c.label),
    }
    println!("{} messages change", c.messages);
    for a in &c.accounts {
        let mut line = format!("{}: {}, {} messages", a.account_id, a.kind, a.messages);
        if a.rehomed > 0 {
            line += &format!(
                "; {} go to the Archive (they have no other mailbox)",
                a.rehomed
            );
        }
        println!("  {line}");
        for m in &a.mailboxes {
            let to = m
                .into
                .as_deref()
                .map_or(String::new(), |t| format!(" -> {t}"));
            println!("    {}: {}{to} ({} messages)", m.how, m.label, m.messages);
        }
    }
    if c.dry_run {
        println!(
            "{}",
            "Dry run: nothing changed. Add --yes to do it.".yellow()
        );
    } else {
        println!("Working in the background: `mc labels stats`.");
    }
    Ok(())
}
