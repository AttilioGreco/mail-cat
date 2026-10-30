use anyhow::{Context, Result, bail};
use clap::Subcommand;
use colored::Colorize;
use mc_client::types::{OAuthLoginEvent, OAuthProvider};
use mc_client::{ImapAccountParams, JmapAccountParams, MailCatClient, MailboxModel};
use tabled::{Table, Tabled};
use tokio_stream::StreamExt;

#[derive(Subcommand)]
pub enum AccountsCmd {
    /// List all configured accounts
    List,
    /// Show each account's live auth state (read-only)
    Status,
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
    /// Add an IMAP (--imap-host) or JMAP (--jmap-url) account with a password. The password is read from stdin
    /// (--password-stdin) or $MAILCAT_PASSWORD and ends up in the server's keychain.
    Add {
        /// The account's email address
        #[arg(long)]
        email: String,
        /// Display name for the account (defaults to the email)
        #[arg(long)]
        name: Option<String>,
        #[arg(
            long,
            required_unless_present = "jmap_url",
            conflicts_with = "jmap_url"
        )]
        imap_host: Option<String>,
        /// JMAP session URL, or just the server's domain
        #[arg(long)]
        jmap_url: Option<String>,
        #[arg(long, default_value_t = 993)]
        imap_port: u16,
        /// Connect to IMAP without TLS
        #[arg(long)]
        no_tls: bool,
        #[arg(long)]
        smtp_host: Option<String>,
        #[arg(long, requires = "smtp_host")]
        smtp_port: Option<u16>,
        /// Login name (defaults to the email)
        #[arg(long)]
        username: Option<String>,
        /// Read the password from the first line of stdin
        #[arg(long)]
        password_stdin: bool,
    },
    /// Present an account's mailboxes as labels or as folders (data is not touched)
    SetModel {
        /// Account name, email or ID
        account: String,
        #[arg(value_enum)]
        model: ModelArg,
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
pub enum ModelArg {
    Labels,
    Folders,
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

pub async fn run(cmd: AccountsCmd, client: MailCatClient, json: bool) -> Result<()> {
    match cmd {
        AccountsCmd::List => list(client, json).await,
        AccountsCmd::Status => status(client).await,
        AccountsCmd::SetModel { account, model } => set_model(client, account, model).await,
        AccountsCmd::AddOauth {
            provider,
            email,
            name,
        } => add_oauth(client, provider.into(), email, name).await,
        AccountsCmd::Add {
            email,
            name,
            imap_host,
            jmap_url,
            imap_port,
            no_tls,
            smtp_host,
            smtp_port,
            username,
            password_stdin,
        } => {
            let password = read_password(password_stdin)?;
            let name = name.unwrap_or_else(|| email.clone());
            let username = username.unwrap_or_else(|| email.clone());
            let account = match (imap_host, jmap_url) {
                (_, Some(jmap_url)) => {
                    let params = JmapAccountParams {
                        name,
                        email,
                        jmap_url,
                        username,
                        password,
                    };
                    client.add_jmap_account(params).await?
                }
                (imap_host, None) => {
                    let params = ImapAccountParams {
                        name,
                        email,
                        username,
                        imap_host: imap_host.unwrap_or_default(),
                        imap_port,
                        smtp_host,
                        smtp_port,
                        use_tls: !no_tls,
                        password,
                    };
                    client.add_imap_account(params).await?
                }
            };
            println!(
                "{} account {} aggiunto (id {}) e attivo.",
                "✓".green(),
                account.email,
                account.id
            );
            Ok(())
        }
        AccountsCmd::Rename { email, name } => rename(client, email, name).await,
    }
}

fn read_password(from_stdin: bool) -> Result<String> {
    let password = if from_stdin {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        line.trim_end_matches(['\r', '\n']).to_owned()
    } else {
        std::env::var("MAILCAT_PASSWORD").unwrap_or_default()
    };
    if password.is_empty() {
        bail!("no password: pass --password-stdin or set MAILCAT_PASSWORD");
    }
    Ok(password)
}

async fn set_model(client: MailCatClient, wanted: String, model: ModelArg) -> Result<()> {
    let account = client
        .list_accounts()
        .await?
        .into_iter()
        .find(|a| {
            [&a.id, &a.name, &a.email]
                .iter()
                .any(|v| v.eq_ignore_ascii_case(&wanted))
        })
        .with_context(|| format!("no account named {wanted:?} (see `mc accounts list`)"))?;
    let model = match model {
        ModelArg::Labels => MailboxModel::Labels,
        ModelArg::Folders => MailboxModel::Folders,
    };
    client.set_account_mailbox_model(&account.id, model).await?;
    println!("{} {} now uses {model:?}", "✓".green(), account.email);
    Ok(())
}

async fn rename(client: MailCatClient, email: String, name: String) -> Result<()> {
    let account = client
        .list_accounts()
        .await?
        .into_iter()
        .find(|a| a.email == email)
        .with_context(|| format!("nessun account con email {email}"))?;
    client.rename_account(&account.id, &name).await?;
    println!(
        "{} account {} rinominato in \"{}\"",
        "✓".green(),
        email,
        name
    );
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
                println!(
                    "{} {}",
                    "Apro il browser per il login:".cyan(),
                    authorize_url
                );
                if let Err(e) = mc_shared::browser::open_browser(&authorize_url) {
                    println!(
                        "{} apri manualmente questo link: {}",
                        format!("Impossibile aprire il browser automaticamente ({e}) —").yellow(),
                        authorize_url
                    );
                }
            }
            OAuthLoginEvent::Succeeded { email } => {
                println!("{} account {} aggiunto e attivo.", "✓".green(), email);
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
    #[tabled(rename = "Model")]
    model: String,
    #[tabled(rename = "Tags")]
    tags: String,
}

async fn list(client: MailCatClient, json: bool) -> Result<()> {
    let accounts = client.list_accounts().await?;
    if json {
        return super::print_json(&accounts);
    }

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
                mc_client::AccountStatus::CredentialsRejected => {
                    "password rejected".yellow().to_string()
                }
                other => other.as_str().red().to_string(),
            },
            model: format!("{:?}", a.mailbox_model).to_lowercase(),
            tags: match a.supports_tags {
                Some(true) => "yes",
                Some(false) => "no",
                None => "?",
            }
            .into(),
        })
        .collect();

    println!("{}", Table::new(rows));
    Ok(())
}

#[derive(Tabled)]
struct StatusRow {
    #[tabled(rename = "Email")]
    email: String,
    #[tabled(rename = "State")]
    state: String,
    #[tabled(rename = "Reason")]
    reason: String,
    #[tabled(rename = "Since")]
    since: String,
    #[tabled(rename = "Last error")]
    last_error: String,
    #[tabled(rename = "Token expires")]
    token_expires: String,
    #[tabled(rename = "Next retry")]
    next_retry: String,
    #[tabled(rename = "SMTP")]
    smtp: String,
}

async fn status(client: MailCatClient) -> Result<()> {
    use mc_client::AuthState;

    let local = |t: chrono::DateTime<chrono::Utc>| {
        t.with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    };
    let dash = || "-".to_owned();
    let rows: Vec<StatusRow> = client
        .list_accounts()
        .await?
        .into_iter()
        .map(|a| {
            let Some(auth) = a.auth else {
                return StatusRow {
                    email: a.email,
                    state: "unknown".red().to_string(),
                    reason: "no auth gate on the server".into(),
                    since: dash(),
                    last_error: dash(),
                    token_expires: dash(),
                    next_retry: dash(),
                    smtp: dash(),
                };
            };
            let state = auth.state.as_str();
            StatusRow {
                email: a.email,
                state: match auth.state {
                    AuthState::Active => state.green(),
                    AuthState::Offline => state.yellow(),
                    _ => state.red(),
                }
                .to_string(),
                reason: auth.reason.unwrap_or_else(dash),
                since: auth.since.map_or_else(dash, local),
                last_error: auth.last_error.unwrap_or_else(dash),
                token_expires: auth.token_expires_at.map_or_else(dash, local),
                next_retry: auth.next_retry.map_or_else(dash, |t| {
                    match (t - chrono::Utc::now()).num_seconds() {
                        secs if secs > 0 => format!("in {secs}s"),
                        _ => "due now".into(),
                    }
                }),
                smtp: auth
                    .smtp_error
                    .map_or_else(dash, |e| format!("refused: {e}")),
            }
        })
        .collect();

    if rows.is_empty() {
        println!("{}", "No accounts found.".yellow());
        return Ok(());
    }
    println!("{}", Table::new(rows));
    Ok(())
}
