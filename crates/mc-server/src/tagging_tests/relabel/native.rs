//! Renaming a label on the server (its tree following), merging level by
//! level, filing only a label's own mail, and deleting an empty container.

use super::*;

const SCOPE: Scope = Scope::Group(GroupId::DEFAULT);

async fn setting(s: &Setup, label: &str) {
    let settings = LabelSettings {
        label: label.into(),
        mode: LabelMode::Suggest,
        threshold: 0.8,
    };
    s.store.put_settings(SCOPE, &settings).await.unwrap();
}

async fn labels_set(s: &Setup) -> Vec<String> {
    let mut labels: Vec<_> = s
        .store
        .settings(SCOPE)
        .await
        .unwrap()
        .into_iter()
        .map(|l| l.label)
        .collect();
    labels.sort();
    labels
}

fn renames(commands: &[Command]) -> Vec<(MailboxId, String)> {
    commands
        .iter()
        .filter_map(|c| match c {
            Command::RenameMailbox {
                mailbox_id,
                new_name,
            } => Some((*mailbox_id, new_name.clone())),
            _ => None,
        })
        .collect()
}

fn deletes(commands: &[Command]) -> Vec<MailboxId> {
    commands
        .iter()
        .filter_map(|c| match c {
            Command::DeleteMailbox { mailbox_id, .. } => Some(*mailbox_id),
            _ => None,
        })
        .collect()
}

async fn child_with_mail(s: &Setup, name: &str) -> (MailboxId, MessageId) {
    let child = mailbox(&s.world, s.world.account, name, MailboxType::Custom).await;
    let (message, _) = s
        .world
        .message_in(
            s.world.account,
            s.world.inbox,
            Some(child),
            vec![],
            "news@shop.example",
            name,
        )
        .await;
    (child, message)
}

#[tokio::test]
async fn a_label_the_account_lacks_is_renamed_with_its_tree_and_the_classifier_follows() {
    let s = setup().await;
    let (weekly, weekly_mail) = child_with_mail(&s, "Newsletter/Weekly").await;
    for label in ["newsletter", "newsletter/weekly"] {
        setting(&s, label).await;
    }
    let proposal = accepted("Newsletter/Weekly", vec![weekly], &[weekly_mail]);
    s.store.put_proposal(&proposal).await.unwrap();
    auto(&s.store, weekly_mail, "newsletter/weekly", &proposal).await;
    auto(&s.store, s.messages[0], "newsletter", &proposal).await;

    let dry = s
        .service
        .rename_label("newsletter", "Dev/News", None, false, true)
        .await
        .unwrap();
    let steps = &dry.accounts[0].mailboxes;
    assert_eq!(steps.len(), 1, "the children follow the one rename");
    assert_eq!(
        (steps[0].how, steps[0].label.as_str(), steps[0].messages),
        (StepKind::Rename, "Newsletter", 3)
    );
    assert!(!dry.accounts[0].deletes_mailbox && !dry.merge);

    s.service
        .rename_label("newsletter", "Dev/News", None, false, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let commands = seen(&s.commands);
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert_eq!(renames(&commands), [(s.world.label, "Dev/News".to_owned())]);
    assert_eq!(labels_set(&s).await, ["dev/news", "dev/news/weekly"]);
    let autos = |id| s.store.autos(id);
    assert_eq!(autos(s.messages[0]).await.unwrap()[0].label, "dev/news");
    assert_eq!(
        autos(weekly_mail).await.unwrap()[0].label,
        "dev/news/weekly"
    );
    let after = s.store.proposal(proposal.id).await.unwrap().unwrap();
    assert_eq!(
        (after.name.as_str(), after.created.as_slice()),
        ("Dev/News/Weekly", &[weekly][..]),
        "a renamed mailbox keeps its id"
    );
}

#[tokio::test]
async fn a_rename_that_only_changes_the_case_keeps_the_classifier_keys() {
    let s = setup().await;
    setting(&s, "newsletter").await;
    let proposal = accepted("Newsletter", vec![s.world.label], &s.messages);
    s.store.put_proposal(&proposal).await.unwrap();
    auto(&s.store, s.messages[0], "newsletter", &proposal).await;

    let dry = s
        .service
        .rename_label("newsletter", "NEWSLETTER", None, false, true)
        .await
        .unwrap();
    assert_eq!(dry.accounts[0].mailboxes[0].how, StepKind::Rename);
    s.service
        .rename_label("newsletter", "NEWSLETTER", None, false, false)
        .await
        .unwrap();
    finished(&s.service).await;

    assert_eq!(
        renames(&seen(&s.commands)),
        [(s.world.label, "NEWSLETTER".to_owned())]
    );
    assert_eq!(labels_set(&s).await, ["newsletter"]);
    assert_eq!(
        s.store.autos(s.messages[0]).await.unwrap()[0].label,
        "newsletter"
    );
    let after = s.store.proposal(proposal.id).await.unwrap().unwrap();
    assert_eq!(after.name, "NEWSLETTER");
}

#[tokio::test]
async fn merging_goes_level_by_level_renaming_the_children_whose_place_is_free() {
    let s = setup().await;
    let account = s.world.account;
    let digest = mailbox(&s.world, account, "Digest", MailboxType::Custom).await;
    let digest_old = mailbox(&s.world, account, "Digest/Old", MailboxType::Custom).await;
    let (old, _) = child_with_mail(&s, "Newsletter/Old").await;
    let (weekly, _) = child_with_mail(&s, "Newsletter/Weekly").await;

    let dry = s
        .service
        .rename_label("newsletter", "Digest", None, false, true)
        .await
        .unwrap();
    let steps: Vec<_> = dry.accounts[0]
        .mailboxes
        .iter()
        .map(|m| (m.label.as_str(), m.into.as_deref(), m.how))
        .collect();
    assert_eq!(
        steps,
        [
            ("Newsletter", Some("Digest"), StepKind::Merge),
            ("Newsletter/Old", Some("Digest/Old"), StepKind::Merge),
            ("Newsletter/Weekly", Some("Digest/Weekly"), StepKind::Rename),
        ]
    );
    s.service
        .rename_label("newsletter", "Digest", None, false, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let commands = seen(&s.commands);
    assert_eq!(renames(&commands), [(weekly, "Digest/Weekly".to_owned())]);
    assert!(commands.iter().any(|c| matches!(c,
        Command::ChangeLabels { add, remove, .. } if *add == [digest] && *remove == [s.world.label])));
    assert!(commands.iter().any(|c| matches!(c,
        Command::ChangeLabels { add, remove, .. } if *add == [digest_old] && *remove == [old])));
    assert_eq!(
        deletes(&commands),
        [old, s.world.label],
        "children go before the parent"
    );
}

#[tokio::test]
async fn mail_only_files_the_labels_own_mail_and_leaves_it_with_its_children() {
    let s = setup().await;
    child_with_mail(&s, "Newsletter/Weekly").await;
    let new = MailboxId::new();
    *s.commands.1.lock().unwrap() = Some(new);

    let dry = s
        .service
        .rename_label("Newsletter", "Newsletter/Buste paga", None, true, true)
        .await
        .unwrap();
    assert!(dry.mail_only && dry.messages == 3);
    let steps = &dry.accounts[0].mailboxes;
    assert_eq!((steps.len(), steps[0].how), (1, StepKind::CreateMove));
    assert!(!dry.accounts[0].deletes_mailbox, "it still has children");

    s.service
        .rename_label("Newsletter", "Newsletter/Buste paga", None, true, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let commands = seen(&s.commands);
    assert!(matches!(&commands[0],
        Command::CreateLabel { name, .. } if name == "Newsletter/Buste paga"));
    assert!(matches!(&commands[1],
        Command::ChangeLabels { message_ids, add, remove }
            if message_ids.len() == 3 && *add == [new] && *remove == [s.world.label]));
    assert_eq!(commands.len(), 2, "no rename, no delete: {commands:?}");
}

#[tokio::test]
async fn mail_only_deletes_the_label_when_it_ends_empty_and_childless() {
    let s = setup().await;
    let digest = mailbox(&s.world, s.world.account, "Digest", MailboxType::Custom).await;
    s.service
        .rename_label("Newsletter", "Digest", None, true, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let commands = seen(&s.commands);
    assert!(commands.iter().any(|c| matches!(c,
        Command::ChangeLabels { add, .. } if *add == [digest])));
    assert_eq!(deletes(&commands), [s.world.label]);
}

#[tokio::test]
async fn an_empty_container_with_no_tagging_data_can_be_deleted_but_not_with_children() {
    let s = setup().await;
    let account = s.world.account;
    let empty = mailbox(&s.world, account, "Clienti", MailboxType::Custom).await;
    mailbox(&s.world, account, "Pending", MailboxType::Custom).await;
    mailbox(&s.world, account, "Pending/Old", MailboxType::Custom).await;

    let dry = s.service.delete_label("clienti", None, true).await.unwrap();
    assert_eq!(dry.accounts[0].mailboxes[0].how, StepKind::Delete);
    s.service
        .delete_label("clienti", None, false)
        .await
        .unwrap();
    finished(&s.service).await;
    assert_eq!(deletes(&seen(&s.commands)), [empty]);

    let why = refused(s.service.delete_label("pending", None, true).await);
    assert!(why.contains("rename or delete the children first"), "{why}");
}
