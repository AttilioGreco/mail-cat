use mc_domain::{AccountId, MailboxPath, MailboxType};

use super::*;

fn custom(account: AccountId, name: &str) -> Mailbox {
    Mailbox::discover(
        account,
        MailboxPath::new(name.split('/').map(str::to_owned).collect(), '/').unwrap(),
        MailboxType::Custom,
    )
    .0
}

#[test]
fn two_mailboxes_never_end_up_with_one_path() {
    let account = AccountId::new();
    let (work, taken) = (custom(account, "Work"), custom(account, "Jobs"));
    let listed = [custom(account, "Jobs")];

    let result = rename_events(&[work.clone(), taken], "Work", work.id, &listed[0], &listed);

    assert!(matches!(
        result,
        Err(ApplicationError::PreconditionFailed(_))
    ));
}

#[test]
fn what_was_renamed_already_is_left_out() {
    let account = AccountId::new();
    let (jobs, kid) = (custom(account, "Jobs"), custom(account, "Work/Kid"));
    let listed = [custom(account, "Jobs"), custom(account, "Jobs/Kid")];

    // The root was written before a crash; its child was not.
    let renames = rename_events(
        &[jobs.clone(), kid.clone()],
        "Work",
        jobs.id,
        &listed[0],
        &listed,
    )
    .unwrap();

    assert_eq!(renames.events.len(), 1);
    assert_eq!(renames.events[0].0, kid.id);
}
