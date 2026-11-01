//! RENAME of a mailbox, children and subscriptions included.

use tracing::warn;

use mc_domain::error::DomainError;
use mc_domain::value_objects::mailbox_path::MailboxPath;

use super::super::mapper::mailbox_path_to_string;
use super::commands::exchange;
use super::mailbox::{create_on, lsub_names, set_subscribed_on};
use super::{COMMAND_TIMEOUT, ImapEmailProvider};

impl ImapEmailProvider {
    pub(super) async fn rename(
        &self,
        mailbox: &MailboxPath,
        new_name: &str,
    ) -> Result<MailboxPath, DomainError> {
        let mut session = self.session().await?;
        let target = rename_on(&mut session, mailbox, new_name).await?;
        self.release(session);
        Ok(target)
    }
}

/// RENAME `mailbox` to `new_name` (written with `/`), making the parents it
/// lacks first: Gmail and plain IMAP both move the children along (RFC 3501),
/// but neither is trusted to make the parents. A rename that only changes the
/// case goes through a temporary name, as servers that compare names without
/// case refuse it as "already exists".
pub(super) async fn rename_on<T>(
    session: &mut async_imap::Session<T>,
    mailbox: &MailboxPath,
    new_name: &str,
) -> Result<MailboxPath, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let delimiter = mailbox.delimiter();
    let target = MailboxPath::new(new_name.split('/').map(str::to_owned).collect(), delimiter)?;
    let (from, to) = (
        mailbox_path_to_string(mailbox),
        mailbox_path_to_string(&target),
    );
    let subscribed = lsub_names(session).await;
    if let Some((parents, _)) = new_name.rsplit_once('/') {
        create_on(session, parents, delimiter).await?;
    }
    if from.eq_ignore_ascii_case(&to) {
        let temporary = format!("mc-rename-{}", uuid::Uuid::new_v4().simple());
        exchange(COMMAND_TIMEOUT, "RENAME", session.rename(&from, &temporary)).await?;
        if let Err(e) = exchange(COMMAND_TIMEOUT, "RENAME", session.rename(&temporary, &to)).await {
            let restored =
                exchange(COMMAND_TIMEOUT, "RENAME", session.rename(&temporary, &from)).await;
            return Err(match restored {
                Ok(()) => e,
                Err(_) => DomainError::Provider(format!(
                    "{e}; the mailbox was left under the name {temporary:?}"
                )),
            });
        }
    } else {
        exchange(COMMAND_TIMEOUT, "RENAME", session.rename(&from, &to)).await?;
    }
    carry_subscriptions(session, subscribed, &from, &to, delimiter).await;
    Ok(target)
}

/// The server leaves subscriptions under the old names, and a mailbox it
/// lists as unsubscribed is dropped by the next sync. An old subscription is
/// given up only once the new name holds one, and never when the two names
/// are the same mailbox to a server that ignores case. When LSUB said nothing
/// (sync then takes every mailbox as subscribed) the new name is subscribed
/// outright, so a later LSUB that does list names cannot leave it out.
async fn carry_subscriptions<T>(
    session: &mut async_imap::Session<T>,
    subscribed: Option<std::collections::HashSet<String>>,
    from: &str,
    to: &str,
    delimiter: char,
) where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let Some(subscribed) = subscribed else {
        if let Err(e) = set_subscribed_on(session, to, true).await {
            warn!(mailbox = to, "renamed mailbox could not be subscribed: {e}");
        }
        return;
    };
    let below = format!("{from}{delimiter}");
    for old in &subscribed {
        let rest = match old.strip_prefix(&below) {
            Some(rest) => format!("{delimiter}{rest}"),
            None if old == from => String::new(),
            None => continue,
        };
        let new = format!("{to}{rest}");
        // Some servers carry the subscription over and refuse a second one
        // ("already subscribed"), others leave it under the old name.
        if let Err(e) = set_subscribed_on(session, &new, true).await {
            warn!(
                mailbox = new,
                "renamed mailbox could not be subscribed: {e}"
            );
            continue;
        }
        if !old.eq_ignore_ascii_case(&new) {
            let _ = set_subscribed_on(session, old, false).await;
        }
    }
}
