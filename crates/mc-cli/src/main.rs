mod commands;

use anyhow::Result;
use clap::{Parser, Subcommand};
use mc_client::MailCatClient;

#[derive(Parser)]
#[command(
    name = "mc",
    about = "mail-cat CLI — debug and explore the gRPC server",
    version,
    long_version = concat!(
        env!("CARGO_PKG_VERSION"),
        "\ncommit: ", env!("BUILD_GIT_SHA"),
        "\nbuilt:  ", env!("BUILD_DATE"),
        "\n",        env!("BUILD_RUSTC"),
    ),
)]
struct Cli {
    /// gRPC server URL
    #[arg(
        long,
        env = "MAILCAT_SERVER_URL",
        default_value = "http://localhost:50051"
    )]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manage email accounts
    #[command(subcommand)]
    Accounts(commands::accounts::AccountsCmd),

    /// Manage contacts and the contact search index
    #[command(subcommand)]
    Contacts(commands::contacts::ContactsCmd),

    /// List mailboxes for an account
    #[command(subcommand)]
    Mailboxes(commands::mailboxes::MailboxesCmd),

    /// List and read messages
    #[command(subcommand)]
    Messages(commands::messages::MessagesCmd),

    /// List and explore email conversations (threads)
    #[command(subcommand)]
    Conversations(commands::conversations::ConversationsCmd),

    /// Sync an account with the mail server
    Sync {
        /// Account ID to sync
        account_id: String,
        /// Force full sync instead of incremental
        #[arg(long)]
        full: bool,
    },

    /// Watch a mailbox for real-time IMAP IDLE events
    Watch {
        /// Mailbox ID to watch
        mailbox_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let client = MailCatClient::connect(&cli.server).await.map_err(|e| {
        anyhow::anyhow!(
            "Cannot connect to server at {}: {}\nIs `just server` running?",
            cli.server,
            e
        )
    })?;

    match cli.command {
        Command::Accounts(cmd) => commands::accounts::run(cmd, client).await,
        Command::Contacts(cmd) => commands::contacts::run(cmd, client).await,
        Command::Mailboxes(cmd) => commands::mailboxes::run(cmd, client).await,
        Command::Messages(cmd) => commands::messages::run(cmd, client).await,
        Command::Conversations(cmd) => commands::conversations::run(cmd, client).await,
        Command::Sync { account_id, full } => commands::sync::run(account_id, full, client).await,
        Command::Watch { mailbox_id } => commands::watch::run(mailbox_id, client).await,
    }
}
