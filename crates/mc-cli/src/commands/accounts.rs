use anyhow::{bail, Context, Result};
use clap::Subcommand;
use colored::Colorize;
use mc_client::types::{OAuthLoginEvent, OAuthProvider};
use mc_client::MailCatClient;
use tabled::{Table, Tabled};
use tokio_stream::StreamExt;

#[derive(Subcommand)]
pub enum AccountsCmd {
    /// List all configured accounts
    List,
    /// Connect a Gmail/Outlook account via OAuth2 (opens your browser)
    AddOauth {
        /// OAuth provider to sign in with
        #[arg(long, value_enum)]
        provider: OauthProviderArg,
        /// The account's email address
        #[arg(long)]
        email: String,
        /// Display name for the account (defaults to the email)
        #[arg(long)]
        name: Option<String>,
    },
    /// Rename an existing account's display name
    Rename {
        /// Email address of the account to rename
        #[arg(long)]
        email: String,
        /// New display name
        #[arg(long)]
        name: String,
    },
}

#[derive(Clone, clap::ValueEnum)]
pub enum OauthProviderArg {
    Google,
    Microsoft,
}

impl From<OauthProviderArg> for OAuthProvider {
    fn from(arg: OauthProviderArg) -> Self {
        match arg {
            OauthProviderArg::Google => OAuthProvider::Google,
            OauthProviderArg::Microsoft => OAuthProvider::Microsoft,
        }
    }
}

pub async fn run(cmd: AccountsCmd, client: MailCatClient) -> Result<()> {
    match cmd {
        AccountsCmd::List => list(client).await,
        AccountsCmd::AddOauth { provider, email, name } => {
            add_oauth(client, provider.into(), email, name).await
        }
        AccountsCmd::Rename { email, name } => rename(client, email, name).await,
    }
}

async fn rename(client: MailCatClient, email: String, name: String) -> Result<()> {
    let account = client
        .list_accounts()
        .await?
        .into_iter()
        .find(|a| a.email == email)
        .with_context(|| format!("nessun account con email {email}"))?;
    client.rename_account(&account.id, &name).await?;
    println!("{} account {} rinominato in \"{}\"", "✓".green(), email, name);
    Ok(())
}

async fn add_oauth(
    client: MailCatClient,
    provider: OAuthProvider,
    email: String,
    name: Option<String>,
) -> Result<()> {
    let display_name = name.unwrap_or_else(|| email.clone());
    let mut stream = client
        .start_oauth_login(&display_name, &email, provider)
        .await
        .context("starting OAuth login")?;

    while let Some(event) = stream.next().await {
        match event.context("reading OAuth login progress")? {
            OAuthLoginEvent::AwaitingBrowser { authorize_url } => {
                println!("{} {}", "Apro il browser per il login:".cyan(), authorize_url);
                if let Err(e) = mc_shared::browser::open_browser(&authorize_url) {
                    println!(
                        "{} apri manualmente questo link: {}",
                        format!("Impossibile aprire il browser automaticamente ({e}) —").yellow(),
                        authorize_url
                    );
                }
            }
            OAuthLoginEvent::Succeeded { email } => {
                println!(
                    "{} account {} aggiunto a config.toml. Riavvia mail-cat-server per attivarlo.",
                    "✓".green(),
                    email
                );
                return Ok(());
            }
            OAuthLoginEvent::Failed { message } => {
                bail!("Login OAuth fallito: {message}");
            }
        }
    }
    bail!("Login OAuth interrotto: stream terminato senza un evento finale");
}


#[derive(Tabled)]
struct AccountRow {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Email")]
    email: String,
    #[tabled(rename = "Status")]
    status: String,
}

async fn list(client: MailCatClient) -> Result<()> {
    let accounts = client.list_accounts().await?;

    if accounts.is_empty() {
        println!("{}", "No accounts found.".yellow());
        return Ok(());
    }

    let rows: Vec<AccountRow> = accounts
        .into_iter()
        .map(|a| AccountRow {
            id: a.id,
            name: a.name,
            email: a.email,
            status: match a.status {
                mc_client::AccountStatus::Active => "active".green().to_string(),
                mc_client::AccountStatus::NeedsReauth => "needs re-auth".yellow().to_string(),
                other => other.as_str().red().to_string(),
            },
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}
