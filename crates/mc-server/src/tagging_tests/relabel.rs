//! Renaming, merging and deleting an existing label, on a real database. The
//! recording command bus changes no mail, so tests that need the effect
//! record it themselves.

use chrono::Utc;
use mc_application::services::tagging::{ChangeKind, StepKind};
use mc_domain::{
    AutoApplied, Example, LabelSettings, Proposal, ProposalId, ProposalStatus, Scope, TaggingStore,
};

use super::*;

mod native;

struct Setup {
    world: World,
    service: Arc<Service>,
    commands: Arc<Recording>,
    store: SqliteTaggingStore,
    /// Three messages under the label, all in the Inbox too.
    messages: Vec<MessageId>,
}

async fn setup() -> Setup {
    let world = World::open().await;
    let commands = Arc::new(Recording::default());
    let service = Arc::new(Service::new(
        SqliteTaggingStore::new(world.db.clone()),
        world.repo.clone(),
        commands.clone(),
    ));
    let mut messages = Vec::new();
    for i in 0..3 {
        messages.push(
            world
                .message("news@shop.example", &format!("offerta {i}"), true)
                .await
                .0,
        );
    }
    Setup {
        store: SqliteTaggingStore::new(world.db.clone()),
        world,
        service,
        commands,
        messages,
    }
}

pub(super) async fn mailbox(
    world: &World,
    account: AccountId,
    name: &str,
    kind: MailboxType,
) -> MailboxId {
    let id = MailboxId::new();
    let segments = name.split('/').map(str::to_owned).collect();
    world
        .record(
            id.to_string(),
            AggregateType::Mailbox,
            DomainEvent::MailboxDiscovered {
                mailbox_id: id,
                account_id: account,
                path: MailboxPath::new(segments, '/').unwrap(),
                display_path: name.into(),
                mailbox_type: kind,
            },
        )
        .await;
    id
}

/// A mailbox below `Dev/News`, so the account cannot just rename a label to
/// it: the mail has to be moved.
async fn occupy(world: &World, account: AccountId) {
    mailbox(world, account, "Dev/News/Old", MailboxType::Custom).await;
}

/// What the provider would have done to a message: swap one mailbox for another.
async fn swapped(world: &World, id: MessageId, add: MailboxId, remove: MailboxId) {
    world
        .record(
            id.to_string(),
            AggregateType::Message,
            DomainEvent::MessageMailboxesChanged {
                message_id: id,
                added: vec![Membership {
                    mailbox_id: add,
                    remote_id: Some(RemoteId::from(3)),
                }],
                removed: vec![remove],
            },
        )
        .await;
}

async fn finished(service: &Service) {
    for _ in 0..200 {
        if service
            .background_stats()
            .await
            .unwrap()
            .applying
            .is_empty()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the job never finished");
}

fn seen(commands: &Recording) -> Vec<Command> {
    commands.0.lock().unwrap().clone()
}

fn refused<T: std::fmt::Debug>(result: Result<T, ApplicationError>) -> String {
    match result {
        Err(e) => e.to_string(),
        Ok(ok) => panic!("expected a refusal, got {ok:?}"),
    }
}

fn accepted(name: &str, created: Vec<MailboxId>, members: &[MessageId]) -> Proposal {
    Proposal {
        id: ProposalId::new(),
        scope: Scope::Group(GroupId::DEFAULT),
        name: name.into(),
        keywords: vec![],
        members: members.to_vec(),
        inbox: 0,
        samples: vec![],
        status: ProposalStatus::Accepted,
        created_at: Utc::now(),
        curated: true,
        created,
    }
}

async fn auto(store: &SqliteTaggingStore, message_id: MessageId, label: &str, p: &Proposal) {
    store
        .record_auto(&AutoApplied {
            message_id,
            label: label.into(),
            score: 1.0,
            archived: false,
            confirmed: true,
            proposal_id: Some(p.id),
            at: Utc::now(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn renaming_creates_the_new_label_moves_the_mail_and_deletes_the_old_one() {
    let s = setup().await;
    occupy(&s.world, s.world.account).await;
    let new = MailboxId::new();
    *s.commands.1.lock().unwrap() = Some(new);

    let dry = s
        .service
        .rename_label("NEWSLETTER", "Dev / News", None, false, true)
        .await
        .unwrap();
    assert_eq!((dry.messages, dry.merge, dry.dry_run), (3, false, true));
    assert_eq!(dry.into.as_deref(), Some("Dev/News"));
    let account = &dry.accounts[0];
    assert_eq!(account.kind, ChangeKind::Mailbox);
    assert!(!account.merges && account.deletes_mailbox);
    assert_eq!(
        (account.mailboxes[0].how, account.mailboxes[0].messages),
        (StepKind::CreateMove, 3)
    );
    assert!(seen(&s.commands).is_empty(), "a dry run touches nothing");

    s.service
        .rename_label("newsletter", "Dev/News", None, false, false)
        .await
        .unwrap();
    assert_eq!(
        s.service.background_stats().await.unwrap().applying[0].total,
        3,
        "the job shows as soon as it is queued"
    );
    finished(&s.service).await;

    let commands = seen(&s.commands);
    assert!(matches!(&commands[0], Command::CreateLabel { name, .. } if name == "Dev/News"));
    let Command::ChangeLabels {
        message_ids,
        add,
        remove,
    } = &commands[1]
    else {
        panic!("{commands:?}");
    };
    let mut moved = message_ids.clone();
    moved.sort_by_key(|m| *m.as_uuid());
    let mut all = s.messages.clone();
    all.sort_by_key(|m| *m.as_uuid());
    assert_eq!(
        (moved, add, remove),
        (all, &vec![new], &vec![s.world.label])
    );
    assert!(matches!(
        &commands[2],
        Command::DeleteMailbox { mailbox_id, move_to: None } if *mailbox_id == s.world.label
    ));
}

#[tokio::test]
async fn renaming_into_an_existing_label_merges_and_keeps_its_settings() {
    let s = setup().await;
    let digest = mailbox(&s.world, s.world.account, "Digest", MailboxType::Custom).await;
    let scope = Scope::Group(GroupId::DEFAULT);
    for (label, mode) in [
        ("newsletter", LabelMode::Apply),
        ("digest", LabelMode::Suggest),
    ] {
        let settings = LabelSettings {
            label: label.into(),
            mode,
            threshold: 0.8,
        };
        s.store.put_settings(scope, &settings).await.unwrap();
    }
    let example = Example {
        message_id: s.messages[0],
        account_id: s.world.account,
        scope: scope.key(),
        positives: vec![],
        negatives: vec!["newsletter".into(), "digest".into(), "other".into()],
        with_body: false,
        weight: 1.0,
    };
    s.store.put_example(&example).await.unwrap();

    let dry = s
        .service
        .rename_label("Newsletter", "digest", None, false, true)
        .await
        .unwrap();
    assert!(dry.merge && dry.accounts[0].merges);
    s.service
        .rename_label("Newsletter", "digest", None, false, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let commands = seen(&s.commands);
    assert!(
        !commands
            .iter()
            .any(|c| matches!(c, Command::CreateLabel { .. }))
    );
    assert!(commands.iter().any(|c| matches!(
        c,
        Command::ChangeLabels { add, remove, .. } if *add == [digest] && *remove == [s.world.label]
    )));
    let settings = s.store.settings(scope).await.unwrap();
    assert_eq!(settings.len(), 1);
    assert_eq!(
        (settings[0].label.as_str(), settings[0].mode),
        ("digest", LabelMode::Suggest)
    );
    let kept = s.store.example(s.messages[0]).await.unwrap().unwrap();
    assert_eq!(kept.negatives, ["digest", "other"]);
}

#[tokio::test]
async fn the_records_of_an_accepted_proposal_follow_a_rename_and_undo_still_works() {
    let s = setup().await;
    occupy(&s.world, s.world.account).await;
    let new = MailboxId::new();
    *s.commands.1.lock().unwrap() = Some(new);
    let proposal = accepted("Newsletter", vec![s.world.label], &s.messages);
    s.store.put_proposal(&proposal).await.unwrap();
    for &m in &s.messages {
        auto(&s.store, m, "newsletter", &proposal).await;
    }

    s.service
        .rename_label("newsletter", "Dev/News", None, false, false)
        .await
        .unwrap();
    finished(&s.service).await;

    assert_eq!(
        s.store.autos(s.messages[0]).await.unwrap()[0].label,
        "dev/news"
    );
    let renamed = s.store.proposal(proposal.id).await.unwrap().unwrap();
    assert_eq!(
        (renamed.name.as_str(), renamed.created.clone()),
        ("Dev/News", vec![new])
    );
    assert_eq!(s.store.proposal_autos(proposal.id).await.unwrap().len(), 3);

    // What the provider did.
    mailbox_as(&s.world, new, "Dev/News").await;
    for &m in &s.messages {
        swapped(&s.world, m, new, s.world.label).await;
    }
    let undo = s
        .service
        .undo_proposal(proposal.id, false, true)
        .await
        .unwrap();
    assert_eq!(undo.to_unlabel, 3);
    assert_eq!(undo.labels_to_delete, ["Dev/News"]);
}

async fn mailbox_as(world: &World, id: MailboxId, name: &str) {
    world
        .record(
            id.to_string(),
            AggregateType::Mailbox,
            DomainEvent::MailboxDiscovered {
                mailbox_id: id,
                account_id: world.account,
                path: MailboxPath::new(name.split('/').map(str::to_owned).collect(), '/').unwrap(),
                display_path: name.into(),
                mailbox_type: MailboxType::Custom,
            },
        )
        .await;
}

#[tokio::test]
async fn a_merge_never_lets_an_undo_delete_the_label_it_merged_into() {
    let s = setup().await;
    let digest = mailbox(&s.world, s.world.account, "Digest", MailboxType::Custom).await;
    let proposal = accepted("Newsletter", vec![s.world.label], &s.messages);
    s.store.put_proposal(&proposal).await.unwrap();

    s.service
        .rename_label("newsletter", "Digest", None, false, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let after = s.store.proposal(proposal.id).await.unwrap().unwrap();
    assert_eq!((after.name.as_str(), after.created.len()), ("Digest", 0));
    assert!(!after.created.contains(&digest));
}

#[tokio::test]
async fn labels_with_children_system_mailboxes_and_no_change_are_refused() {
    let s = setup().await;
    mailbox(
        &s.world,
        s.world.account,
        "Newsletter/Weekly",
        MailboxType::Custom,
    )
    .await;
    let rename = |from: &'static str, to: &'static str| {
        let service = s.service.clone();
        async move { service.rename_label(from, to, None, false, true).await }
    };

    assert!(
        refused(s.service.delete_label("newsletter", None, true).await).contains("child labels")
    );
    assert!(refused(rename("Newsletter/Weekly", "Newsletter/Weekly").await).contains("same label"));
    assert!(
        refused(rename("Newsletter/Weekly", "Newsletter/Weekly/Old").await)
            .contains("inside itself")
    );
    assert!(refused(rename("Newsletter/Weekly", "INBOX").await).contains("system mailbox"));
    assert!(refused(rename("Inbox", "Mail").await).contains("system mailbox"));
    assert!(refused(rename("Nothing", "Something").await).contains("nothing"));
    assert!(refused(rename("A//B", "C").await).contains("not a label name"));
}

#[tokio::test]
async fn a_second_change_is_refused_while_the_label_is_being_changed() {
    let s = setup().await;
    occupy(&s.world, s.world.account).await;
    s.service
        .rename_label("newsletter", "Dev/News", None, false, false)
        .await
        .unwrap();
    let second = s.service.delete_label("newsletter", None, false).await;
    assert!(refused(second).contains("still being applied"));
    finished(&s.service).await;
}

#[tokio::test]
async fn deleting_keeps_the_mail_and_files_what_would_be_in_no_mailbox_in_the_archive() {
    let s = setup().await;
    let account = s.world.account;
    let archive = mailbox(&s.world, account, "Archive", MailboxType::Archive).await;
    s.world
        .message_in(
            account,
            s.world.label,
            None,
            vec![],
            "news@shop.example",
            "solo",
        )
        .await;
    let proposal = accepted("Newsletter", vec![s.world.label], &s.messages);
    s.store.put_proposal(&proposal).await.unwrap();
    auto(&s.store, s.messages[0], "newsletter", &proposal).await;
    let scope = Scope::Group(GroupId::DEFAULT);
    s.store
        .put_settings(scope, &LabelSettings::off("newsletter"))
        .await
        .unwrap();

    let dry = s
        .service
        .delete_label("Newsletter", None, true)
        .await
        .unwrap();
    assert_eq!(
        (dry.messages, dry.accounts[0].rehomed, dry.into),
        (4, 1, None)
    );
    s.service
        .delete_label("Newsletter", None, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let commands = seen(&s.commands);
    let changes: Vec<_> = commands
        .iter()
        .filter_map(|c| match c {
            Command::ChangeLabels {
                message_ids,
                add,
                remove,
            } => Some((message_ids.len(), add.clone(), remove.clone())),
            _ => None,
        })
        .collect();
    assert!(
        changes.contains(&(3, vec![], vec![s.world.label])),
        "{changes:?}"
    );
    assert!(
        changes.contains(&(1, vec![archive], vec![s.world.label])),
        "{changes:?}"
    );
    assert!(commands.iter().any(|c| matches!(
        c,
        Command::DeleteMailbox { mailbox_id, move_to: None } if *mailbox_id == s.world.label
    )));
    assert!(
        commands
            .iter()
            .all(|c| !matches!(c, Command::DeleteMessages { .. }))
    );

    let after = s.store.proposal(proposal.id).await.unwrap().unwrap();
    assert_eq!(
        (after.status, after.created.len()),
        (ProposalStatus::Rejected, 0)
    );
    assert!(s.store.autos(s.messages[0]).await.unwrap().is_empty());
    assert!(s.store.settings(scope).await.unwrap().is_empty());
}

#[tokio::test]
async fn keyword_accounts_swap_the_keyword_and_folders_move_as_a_whole() {
    let s = setup().await;
    let (keywords, inbox) = s
        .world
        .other_account("k@x.example", MailboxModel::Folders, true)
        .await;
    let mut tagged = Vec::new();
    for i in 0..2 {
        let flags = vec![Flag::Custom("newsletter".into())];
        let (id, _) = s
            .world
            .message_in(
                keywords,
                inbox,
                None,
                flags,
                "news@shop.example",
                &format!("k{i}"),
            )
            .await;
        tagged.push(id);
    }
    let folder = mailbox(&s.world, keywords, "Newsletter", MailboxType::Custom).await;
    occupy(&s.world, keywords).await;
    occupy(&s.world, s.world.account).await;
    let new = MailboxId::new();
    *s.commands.1.lock().unwrap() = Some(new);

    let dry = s
        .service
        .rename_label("newsletter", "Dev/News", None, false, true)
        .await
        .unwrap();
    let kinds: Vec<_> = dry.accounts.iter().map(|a| a.kind).collect();
    assert_eq!(kinds, {
        let mut k = vec![ChangeKind::Mailbox, ChangeKind::Folder];
        if keywords.as_uuid() < s.world.account.as_uuid() {
            k.reverse();
        }
        k
    });
    assert!(
        refused(s.service.delete_label("newsletter", None, true).await)
            .contains("mailboxes delete")
    );

    s.service
        .rename_label("newsletter", "Dev/News", None, false, false)
        .await
        .unwrap();
    finished(&s.service).await;

    let commands = seen(&s.commands);
    let swaps: Vec<_> = commands
        .iter()
        .filter_map(|c| match c {
            Command::SetMessageFlags {
                message_id,
                flags_to_add,
                flags_to_remove,
            } => Some((*message_id, flags_to_add.clone(), flags_to_remove.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(swaps.len(), 2);
    for (id, add, remove) in swaps {
        assert!(tagged.contains(&id));
        assert_eq!(
            (add, remove),
            (
                vec![Flag::Custom("dev/news".into())],
                vec![Flag::Custom("newsletter".into())]
            )
        );
    }
    assert!(commands.iter().any(|c| matches!(
        c,
        Command::DeleteMailbox { mailbox_id, move_to: Some(to) } if *mailbox_id == folder && *to == new
    )));
}

#[tokio::test]
async fn a_keyword_is_removed_when_its_label_is_deleted() {
    let s = setup().await;
    let (keywords, inbox) = s
        .world
        .other_account("k@x.example", MailboxModel::Folders, true)
        .await;
    let flags = vec![Flag::Custom("promo".into())];
    let (tagged, _) = s
        .world
        .message_in(keywords, inbox, None, flags, "p@shop.example", "k")
        .await;
    // Moving the account out and back is how a group is retrained from scratch.
    s.service.assign_account(keywords, None).await.unwrap();
    s.service
        .assign_account(keywords, Some(GroupId::DEFAULT))
        .await
        .unwrap();

    s.service.delete_label("promo", None, false).await.unwrap();
    finished(&s.service).await;

    assert!(seen(&s.commands).iter().any(|c| matches!(
        c,
        Command::SetMessageFlags { message_id, flags_to_add, flags_to_remove }
            if *message_id == tagged && flags_to_add.is_empty()
                && *flags_to_remove == [Flag::Custom("promo".into())]
    )));
}

fn label_changes(commands: &[Command], id: MessageId) -> Vec<(Vec<MailboxId>, Vec<MailboxId>)> {
    commands
        .iter()
        .filter_map(|c| match c {
            Command::ChangeLabels {
                message_ids,
                add,
                remove,
            } if message_ids.contains(&id) => Some((add.clone(), remove.clone())),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_message_in_the_trash_is_relabelled_in_place_so_the_mailbox_can_go() {
    for deleting in [false, true] {
        let s = setup().await;
        let account = s.world.account;
        let archive = mailbox(&s.world, account, "Archive", MailboxType::Archive).await;
        let trash = mailbox(&s.world, account, "Trash", MailboxType::Trash).await;
        let (binned, _) = s
            .world
            .message_in(
                account,
                trash,
                Some(s.world.label),
                vec![],
                "n@shop.example",
                "bin",
            )
            .await;
        occupy(&s.world, account).await;
        let new = MailboxId::new();
        *s.commands.1.lock().unwrap() = Some(new);

        if deleting {
            s.service
                .delete_label("Newsletter", None, false)
                .await
                .unwrap();
        } else {
            s.service
                .rename_label("Newsletter", "Dev/News", None, false, false)
                .await
                .unwrap();
        }
        finished(&s.service).await;

        let commands = seen(&s.commands);
        let add = if deleting { vec![] } else { vec![new] };
        assert_eq!(
            label_changes(&commands, binned),
            [(add, vec![s.world.label])],
            "deleting: {deleting}"
        );
        assert!(
            !label_changes(&commands, binned)
                .iter()
                .any(|(a, _)| a.contains(&archive))
        );
        assert!(commands.iter().any(|c| matches!(
            c,
            Command::DeleteMailbox { mailbox_id, .. } if *mailbox_id == s.world.label
        )));
    }
}

#[tokio::test]
async fn keywords_set_by_another_client_in_other_case_are_removed_too() {
    for deleting in [false, true] {
        let s = setup().await;
        let (keywords, inbox) = s
            .world
            .other_account("k@x.example", MailboxModel::Folders, true)
            .await;
        let flags = vec![Flag::Custom("Work".into())];
        let (tagged, _) = s
            .world
            .message_in(keywords, inbox, None, flags, "w@shop.example", "k")
            .await;
        s.service.assign_account(keywords, None).await.unwrap();
        s.service
            .assign_account(keywords, Some(GroupId::DEFAULT))
            .await
            .unwrap();

        if deleting {
            s.service.delete_label("work", None, false).await.unwrap();
        } else {
            s.service
                .rename_label("work", "Dev/Work", None, false, false)
                .await
                .unwrap();
        }
        finished(&s.service).await;

        let add = if deleting {
            vec![]
        } else {
            vec![Flag::Custom("dev/work".into())]
        };
        assert!(
            seen(&s.commands).iter().any(|c| matches!(
                c,
                Command::SetMessageFlags { message_id, flags_to_add, flags_to_remove }
                    if *message_id == tagged && *flags_to_add == add
                        && *flags_to_remove == [Flag::Custom("Work".into())]
            )),
            "deleting: {deleting}"
        );
    }
}
