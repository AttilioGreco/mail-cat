//! Recording a mailbox rename the server made: the events that move the
//! mailbox and everything below it, and the settling of renames whose local
//! record was never written.

use mc_domain::{
    AccountId, DomainEvent, EmailProvider, Mailbox, MailboxId, MailboxReadRepository,
    PendingRenameStore,
};
use tracing::warn;

use crate::error::ApplicationError;

/// What a rename writes locally.
#[derive(Default)]
pub(crate) struct Renames {
    pub events: Vec<(MailboxId, DomainEvent)>,
    /// Mailboxes the server did not list after all.
    pub missing: usize,
}

/// The level separator of `display_path`: a JMAP path is an id and its
/// display path always uses `/`.
pub(crate) fn display_delimiter(mailbox: &Mailbox) -> char {
    if mailbox.path.is_opaque() {
        '/'
    } else {
        mailbox.path.delimiter()
    }
}

pub(crate) fn is_below(display_path: &str, parent: &str, delimiter: char) -> bool {
    display_path
        .strip_prefix(parent)
        .is_some_and(|rest| rest.starts_with(delimiter))
}

/// Events moving the mailbox `root_id` (shown as `own` before) and every
/// mailbox below `own` to where the server lists them, with `new_root` the
/// root as listed. Mailboxes already there are left out, so a rerun after a
/// partial write finishes the job. Refused when it would give two mailboxes
/// one path, which the next sync would take for one mailbox and one ghost.
pub(crate) fn rename_events(
    known: &[Mailbox],
    own: &str,
    root_id: MailboxId,
    new_root: &Mailbox,
    listed: &[Mailbox],
) -> Result<Renames, ApplicationError> {
    let delimiter = display_delimiter(new_root);
    let moving = |m: &Mailbox| m.id == root_id || is_below(&m.display_path, own, delimiter);
    let mut occupied: Vec<_> = known
        .iter()
        .filter(|m| !moving(m))
        .map(|m| m.path.clone())
        .collect();
    let mut renames = Renames::default();
    for mailbox in known.iter().filter(|m| moving(m)) {
        // A path that is a name (IMAP) is found by where its display path
        // lands; one that is an id (JMAP) stays as it is. Servers that
        // ignore case in names may list another spelling than was asked.
        let now = if mailbox.path.is_opaque() {
            listed.iter().find(|l| l.path == mailbox.path)
        } else {
            let wanted = format!(
                "{}{}",
                new_root.display_path,
                &mailbox.display_path[own.len()..]
            );
            listed
                .iter()
                .find(|l| l.display_path == wanted)
                .or_else(|| {
                    listed
                        .iter()
                        .find(|l| l.display_path.eq_ignore_ascii_case(&wanted))
                })
        };
        let Some(now) = now else {
            warn!(mailbox = %mailbox.display_path, "renamed mailbox not listed by the server");
            renames.missing += 1;
            continue;
        };
        if occupied.contains(&now.path) {
            return Err(ApplicationError::PreconditionFailed(format!(
                "{} would share its path with another mailbox",
                now.display_path
            )));
        }
        occupied.push(now.path.clone());
        if now.path == mailbox.path && now.display_path == mailbox.display_path {
            continue;
        }
        let mut moved = mailbox.clone();
        let event = moved.rename(
            now.path.clone(),
            now.display_path.clone(),
            now.display_name.clone(),
        );
        renames.events.push((moved.id, event));
    }
    Ok(renames)
}

/// The pending renames of an account, settled by what the server lists now:
/// the events to write for each (none when it did not happen), to be cleared
/// by the caller once written.
pub(crate) async fn settle<S: MailboxReadRepository + PendingRenameStore>(
    store: &S,
    provider: &impl EmailProvider,
    account_id: &AccountId,
) -> Result<Vec<(MailboxId, Vec<(MailboxId, DomainEvent)>)>, ApplicationError> {
    let pending = store.pending_renames(account_id).await?;
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let known = store.list_mailboxes(account_id).await?;
    let listed = provider.list_mailboxes().await?;
    let mut settled = Vec::with_capacity(pending.len());
    for rename in pending {
        let new = listed.iter().find(|l| l.display_path == rename.new_display);
        let old_listed = listed.iter().any(|l| l.display_path == rename.old_display);
        let events = match (new, old_listed) {
            (Some(new_root), false) => rename_events(
                &known,
                &rename.old_display,
                rename.mailbox_id,
                new_root,
                &listed,
            )
            .map(|r| r.events)
            .unwrap_or_else(|e| {
                warn!(mailbox = %rename.new_display, error = %e, "rename not recorded");
                Vec::new()
            }),
            (None, true) => Vec::new(),
            _ => {
                warn!(old = %rename.old_display, new = %rename.new_display, "cannot tell whether a rename went through; dropped");
                Vec::new()
            }
        };
        settled.push((rename.mailbox_id, events));
    }
    Ok(settled)
}

#[cfg(test)]
mod tests;
