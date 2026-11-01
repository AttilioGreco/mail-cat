//! Which mailboxes of an account a label change touches, and how.

use std::collections::BTreeMap;

use mc_domain::{Mailbox, MailboxType};

use super::{Named, Step, StepKind, within};
use crate::error::ApplicationError;
use crate::services::tagging::{label_key, label_mailbox};

/// What to do on one account, parents before children. A mailbox whose new
/// place is free (no mailbox of that name or below it) is renamed with its
/// whole tree; otherwise its mail joins the label there and each child is
/// planned the same way. `mail_only` plans just the mailbox itself.
pub(super) fn plan_steps(
    own: &[Mailbox],
    key: &str,
    from: &str,
    into: Option<&Named>,
    mail_only: bool,
    folders: bool,
) -> Result<Vec<Step>, ApplicationError> {
    let mut steps = Vec::new();
    let Some((to_key, to_name)) = into else {
        let delete = label_mailbox(own, key).map(|f| Step {
            from: f.clone(),
            into: None,
            how: StepKind::Delete,
            target: None,
            deletes: true,
        });
        return Ok(delete.into_iter().collect());
    };
    if to_key == key {
        let renamed = label_mailbox(own, key).filter(|f| f.display_path != *to_name);
        return Ok(renamed
            .map(|f| rename_step(f, to_key, to_name))
            .into_iter()
            .collect());
    }
    if !mail_only {
        plan_tree(own, key, (to_key, to_name), &mut steps);
        return Ok(steps);
    }
    let Some(f) = label_mailbox(own, key) else {
        return Ok(steps);
    };
    let below = |k: &str| within(k, key) && k != key;
    let has_children = own
        .iter()
        .any(|m| m.mailbox_type == MailboxType::Custom && below(&label_key(&m.display_path)));
    let deletes = !has_children && !within(to_key, key);
    if folders && !deletes {
        return Err(ApplicationError::PreconditionFailed(format!(
            "{from:?} is a folder with children: its mail cannot be moved without deleting it"
        )));
    }
    steps.push(join_step(own, f, to_key, to_name, deletes));
    Ok(steps)
}

fn rename_step(f: &Mailbox, to_key: &str, to_name: &str) -> Step {
    Step {
        from: f.clone(),
        into: Some((to_key.to_owned(), to_name.to_owned())),
        how: StepKind::Rename,
        target: None,
        deletes: false,
    }
}

fn join_step(own: &[Mailbox], f: &Mailbox, to_key: &str, to_name: &str, deletes: bool) -> Step {
    let target = label_mailbox(own, to_key).map(|t| t.id);
    Step {
        from: f.clone(),
        into: Some((to_key.to_owned(), to_name.to_owned())),
        how: if target.is_some() {
            StepKind::Merge
        } else {
            StepKind::CreateMove
        },
        target,
        deletes,
    }
}

fn plan_tree(own: &[Mailbox], from: &str, to: (&str, &str), steps: &mut Vec<Step>) {
    let free = !own.iter().any(|m| {
        m.mailbox_type == MailboxType::Custom && within(&label_key(&m.display_path), to.0)
    });
    match label_mailbox(own, from) {
        Some(f) if free => return steps.push(rename_step(f, to.0, to.1)),
        Some(f) => steps.push(join_step(own, f, to.0, to.1, true)),
        None => {}
    }
    // A child may exist only as part of a deeper label's name.
    let depth = from.split('/').count();
    let mut children: BTreeMap<String, String> = BTreeMap::new();
    for m in own.iter().filter(|m| m.mailbox_type == MailboxType::Custom) {
        if !within(&label_key(&m.display_path), from) {
            continue;
        }
        if let Some(segment) = m.display_path.split('/').nth(depth).map(str::trim) {
            children
                .entry(segment.to_lowercase())
                .or_insert_with(|| segment.to_owned());
        }
    }
    for (lower, segment) in children {
        plan_tree(
            own,
            &format!("{from}/{lower}"),
            (&format!("{}/{lower}", to.0), &format!("{}/{segment}", to.1)),
            steps,
        );
    }
}
