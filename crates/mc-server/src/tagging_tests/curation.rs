//! Curating at scale: curated proposals, plans, progress and undo, on a real
//! database. The recording command bus changes no mail, so tests that need
//! the label on the mail record its effect themselves.

use mc_application::services::tagging::{OpStatus, PlanOp};
use mc_domain::{Membership, Proposal, ProposalId, ProposalStatus, RemoteId, TaggingStore};

use super::discovery::{found, offers, until_sent};
use super::*;

fn reference(id: ProposalId) -> String {
    id.as_uuid().to_string()
}

fn failure(result: Result<impl std::fmt::Debug, ApplicationError>) -> String {
    match result {
        Err(ApplicationError::PreconditionFailed(m)) => m,
        other => panic!("expected a refused plan, got {other:?}"),
    }
}

async fn applied(f: &super::discovery::Found) {
    for _ in 0..200 {
        if f.service
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
    panic!("labelling never finished");
}

#[tokio::test]
async fn a_curated_proposal_survives_discovery_and_the_clusters_overlapping_it_are_dropped() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    f.service
        .rename_proposal(orders.id, "Ordini")
        .await
        .unwrap();

    f.discover().await;
    let pending = f.pending().await;
    assert_eq!(pending.len(), 2, "{pending:?}");
    let kept = f.orders().await;
    assert_eq!(
        (kept.id, kept.name.as_str(), kept.curated),
        (orders.id, "Ordini", true)
    );
    assert!(
        !pending
            .iter()
            .find(|p| p.proposal.id == offers.id)
            .unwrap()
            .proposal
            .curated
    );

    // A curated proposal smaller than the cluster still wins over it.
    let store = SqliteTaggingStore::new(f.world.db.clone());
    store.delete_proposal(orders.id).await.unwrap();
    let mut subset = orders.members.clone();
    subset.sort_by_key(|m| *m != f.groups[0][0]);
    subset.truncate(30);
    let mine = Proposal {
        id: ProposalId::new(),
        name: "Mio".into(),
        members: subset,
        ..kept
    };
    store.put_proposal(&mine).await.unwrap();
    f.discover().await;
    let pending = f.pending().await;
    assert_eq!(
        pending.len(),
        2,
        "no second proposal for the same mail: {pending:?}"
    );
    assert_eq!(f.orders().await.members.len(), 30);
    assert_eq!(f.orders().await.id, mine.id);
}

#[tokio::test]
async fn merging_marks_the_result_curated_and_a_nested_name_is_accepted_whole() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    let merged = f
        .service
        .merge_proposals(&[orders.id, offers.id], Some("Shopping / Tutto"))
        .await
        .unwrap();
    assert!(merged.proposal.curated);
    assert_eq!(merged.proposal.name, "Shopping/Tutto");

    assert!(
        f.service
            .rename_proposal(merged.proposal.id, "A//B")
            .await
            .is_err()
    );
    f.service
        .accept_proposal(merged.proposal.id, None, false)
        .await
        .unwrap();
    let created: Vec<String> = f
        .commands
        .0
        .lock()
        .unwrap()
        .iter()
        .filter_map(|c| match c {
            Command::CreateLabel { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(created, ["Shopping/Tutto"]);
}

#[tokio::test]
async fn a_plan_that_does_not_validate_changes_nothing_and_lists_every_problem() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    let ghost = ProposalId::new();
    let id = |p: &Proposal| reference(p.id);
    let ops = vec![
        PlanOp::Rename {
            id: id(&orders),
            name: "Ok".into(),
        },
        accept_ref("@nope"),
        PlanOp::Assign {
            id: id(&offers),
            label: "Non esiste".into(),
            archive: false,
        },
        PlanOp::Reject {
            id: reference(ghost),
        },
        PlanOp::Merge {
            ids: vec![id(&orders), id(&offers)],
            name: None,
            alias: Some("x".into()),
        },
        accept_ref("@x"),
        PlanOp::Reject { id: id(&orders) },
        PlanOp::Mode {
            label: "nulla".into(),
            mode: LabelMode::Suggest,
            threshold: None,
            existing: false,
            account: None,
        },
    ];

    let message = failure(f.service.apply_plan(&ops, false).await);

    for expected in [
        "op 2 (accept): no merge is called @nope",
        "op 3 (assign): label \"Non esiste\" does not exist",
        "op 4 (reject)",
        "op 6 (accept): name the merge",
        "op 7 (reject)",
        "was already merged",
        "op 8 (mode)",
    ] {
        assert!(message.contains(expected), "{expected:?} in\n{message}");
    }
    assert!(
        !message.contains("op 1 ") && !message.contains("op 5 "),
        "{message}"
    );
    assert_eq!(
        f.orders().await.name,
        "Amazon",
        "the valid rename was not applied"
    );
    assert_eq!(f.pending().await.len(), 2);
    assert!(f.commands.0.lock().unwrap().is_empty());
}

fn accept_ref(reference: &str) -> PlanOp {
    PlanOp::Accept {
        id: reference.into(),
        name: None,
        archive: false,
    }
}

#[tokio::test]
async fn a_dry_run_counts_what_would_happen_and_does_nothing() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    let ops = vec![
        PlanOp::Merge {
            ids: vec![reference(orders.id), reference(offers.id)],
            name: Some("Acquisti/Tutto".into()),
            alias: Some("all".into()),
        },
        PlanOp::Accept {
            id: "@all".into(),
            name: None,
            archive: true,
        },
    ];

    let report = f.service.apply_plan(&ops, true).await.unwrap();

    let s = &report.summary;
    assert_eq!(
        (s.merged, s.accepted, s.to_label, s.to_archive),
        (1, 1, 80, 80)
    );
    assert_eq!(
        s.labels_to_create,
        [("Acquisti/Tutto".to_owned(), vec![f.world.account])]
    );
    assert_eq!(s.accounts.len(), 1);
    assert_eq!((s.accounts[0].messages, s.accounts[0].archived), (80, 80));
    assert!(s.labels_touched.is_empty());
    assert!(report.results.iter().all(|r| r.status == OpStatus::Planned));
    assert_eq!(f.pending().await.len(), 2, "nothing was merged");
    assert!(f.commands.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_second_accept_of_a_name_the_plan_creates_is_not_a_second_creation() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    let ops = vec![
        PlanOp::Accept {
            id: reference(orders.id),
            name: Some("Shopping/Ordini".into()),
            archive: false,
        },
        PlanOp::Assign {
            id: reference(offers.id),
            label: "shopping/ordini".into(),
            archive: false,
        },
    ];
    let report = f.service.apply_plan(&ops, true).await.unwrap();
    assert_eq!(report.summary.labels_to_create.len(), 1);
    assert_eq!((report.summary.assigned, report.summary.to_label), (1, 80));
}

#[tokio::test]
async fn a_plan_runs_in_order_and_an_alias_names_the_merged_proposal() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    let ops = vec![
        PlanOp::Merge {
            ids: vec![reference(orders.id), reference(offers.id)],
            name: Some("Shopping/Tutto".into()),
            alias: Some("all".into()),
        },
        PlanOp::Rename {
            id: "@all".into(),
            name: "Shopping/Acquisti".into(),
        },
        PlanOp::Accept {
            id: "@all".into(),
            name: None,
            archive: false,
        },
    ];

    let report = f.service.apply_plan(&ops, false).await.unwrap();

    assert!(
        report.results.iter().all(|r| r.status == OpStatus::Done),
        "{:?}",
        report.results
    );
    let merged = report.results[0].proposal.expect("the merge's proposal");
    assert_eq!(report.results[2].queued, Some(80));
    until_sent(&f, 80).await;
    let store = SqliteTaggingStore::new(f.world.db.clone());
    let accepted = store.proposal(merged).await.unwrap().unwrap();
    assert_eq!(
        (accepted.status, accepted.name.as_str(), accepted.curated),
        (ProposalStatus::Accepted, "Shopping/Acquisti", true)
    );
    assert!(
        store.proposal(orders.id).await.unwrap().is_none(),
        "merged away"
    );
    let order: Vec<&str> = f
        .commands
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|c| match c {
            Command::CreateLabel { .. } => "create",
            Command::ChangeLabels { .. } => "change",
            _ => "other",
        })
        .fold(Vec::new(), |mut seen, kind| {
            if seen.last() != Some(&kind) {
                seen.push(kind);
            }
            seen
        });
    assert_eq!(order, ["create", "change"]);
    applied(&f).await;
}

#[tokio::test]
async fn a_mode_step_sets_an_existing_label_and_refuses_one_the_plan_fills() {
    let (world, service, _) = super::group_with_taught_label().await;
    let mode = |label: &str| PlanOp::Mode {
        label: label.into(),
        mode: LabelMode::Suggest,
        threshold: Some(0.9),
        existing: false,
        account: None,
    };

    let report = service
        .apply_plan(&[mode("Newsletter")], false)
        .await
        .unwrap();

    assert_eq!(report.results[0].status, OpStatus::Done);
    let status = service
        .label_status(world.account, "newsletter")
        .await
        .unwrap();
    assert_eq!(status.settings.mode, LabelMode::Suggest);
    assert!(failure(service.apply_plan(&[mode("Nuova")], true).await).contains("does not exist"));
    let apply = PlanOp::Mode {
        label: "newsletter".into(),
        mode: LabelMode::Apply,
        threshold: None,
        existing: true,
        account: None,
    };
    assert_eq!(
        service
            .apply_plan(&[apply], true)
            .await
            .unwrap()
            .summary
            .modes,
        1
    );
}

#[tokio::test]
async fn labelling_shows_as_a_job_with_progress_and_undoing_puts_everything_back() {
    let f = found().await;
    f.discover().await;
    let orders = f.orders().await;
    let ordini = MailboxId::new();
    *f.commands.1.lock().unwrap() = Some(ordini);

    f.service
        .accept_proposal(orders.id, Some("Ordini"), true)
        .await
        .unwrap();

    let job = f.service.background_stats().await.unwrap().applying;
    assert_eq!((job[0].label.as_str(), job[0].total), ("ordini", 40));
    assert!(
        f.service
            .undo_proposal(orders.id, false, true)
            .await
            .is_err(),
        "still applying"
    );
    applied(&f).await;
    // What the provider would have done: the mail is under the label, out of the Inbox.
    f.world
        .record(
            ordini.to_string(),
            AggregateType::Mailbox,
            DomainEvent::MailboxDiscovered {
                mailbox_id: ordini,
                account_id: f.world.account,
                path: MailboxPath::new(vec!["Ordini".into()], '/').unwrap(),
                display_path: "Ordini".into(),
                mailbox_type: MailboxType::Custom,
            },
        )
        .await;
    for &message_id in &orders.members {
        f.world
            .record(
                message_id.to_string(),
                AggregateType::Message,
                DomainEvent::MessageMailboxesChanged {
                    message_id,
                    added: vec![Membership {
                        mailbox_id: ordini,
                        remote_id: Some(RemoteId::from(3)),
                    }],
                    removed: vec![],
                },
            )
            .await;
    }
    let store = SqliteTaggingStore::new(f.world.db.clone());
    let accepted = store.proposal(orders.id).await.unwrap().unwrap();
    assert_eq!(
        accepted.created,
        [ordini],
        "the acceptance created the label"
    );

    let dry = f
        .service
        .undo_proposal(orders.id, true, true)
        .await
        .unwrap();
    assert_eq!((dry.to_unlabel, dry.to_unarchive), (40, 40));
    assert_eq!(dry.labels_to_delete, ["Ordini"]);
    assert_eq!(dry.status, ProposalStatus::Rejected);
    assert_eq!(
        (dry.accounts[0].messages, dry.accounts[0].archived),
        (40, 40)
    );
    let before = f.commands.0.lock().unwrap().len();

    f.service
        .undo_proposal(orders.id, false, false)
        .await
        .unwrap();
    let working = f.service.background_stats().await.unwrap().applying;
    assert_eq!(
        (working[0].label.as_str(), working[0].total),
        ("ordini", 40)
    );
    applied(&f).await;

    let commands = f.commands.0.lock().unwrap()[before..].to_vec();
    let mut unlabelled = 0;
    for c in &commands {
        if let Command::ChangeLabels {
            message_ids,
            add,
            remove,
        } = c
        {
            assert_eq!(
                (add.as_slice(), remove.as_slice()),
                (&[f.world.inbox][..], &[ordini][..])
            );
            unlabelled += message_ids.len();
        }
    }
    assert_eq!(unlabelled, 40);
    assert!(commands.iter().any(|c| matches!(c, Command::DeleteMailbox { mailbox_id, move_to: None } if *mailbox_id == ordini)));
    let reopened = store.proposal(orders.id).await.unwrap().unwrap();
    assert_eq!(
        (reopened.status, reopened.created.len()),
        (ProposalStatus::Pending, 0)
    );
    assert!(store.autos(orders.members[0]).await.unwrap().is_empty());
    assert!(
        store
            .example(orders.members[0])
            .await
            .unwrap()
            .is_none_or(|e| e.positives.is_empty())
    );
    assert!(
        f.service
            .undo_proposal(orders.id, false, true)
            .await
            .is_err(),
        "not accepted any more"
    );
}

#[tokio::test]
async fn a_parent_label_is_not_a_label_of_the_mail_in_its_child() {
    let world = World::open().await;
    let service = Arc::new(Service::new(
        SqliteTaggingStore::new(world.db.clone()),
        world.repo.clone(),
        Arc::new(Recording::default()),
    ));
    let (shopping, ebay) = (MailboxId::new(), MailboxId::new());
    for (id, segments, display) in [
        (shopping, vec!["Shopping"], "Shopping"),
        (ebay, vec!["Shopping", "Ebay"], "Shopping/Ebay"),
    ] {
        world
            .record(
                id.to_string(),
                AggregateType::Mailbox,
                DomainEvent::MailboxDiscovered {
                    mailbox_id: id,
                    account_id: world.account,
                    path: MailboxPath::new(segments.into_iter().map(Into::into).collect(), '/')
                        .unwrap(),
                    display_path: display.into(),
                    mailbox_type: MailboxType::Custom,
                },
            )
            .await;
    }
    world
        .message_in(
            world.account,
            world.inbox,
            Some(ebay),
            vec![],
            "shop@ebay.example",
            "ordine",
        )
        .await;
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();

    let listed = service.list_labels(None).await.unwrap();
    let names: Vec<_> = listed[0].labels.iter().map(|l| l.name.as_str()).collect();
    assert_eq!(
        names,
        ["Shopping/Ebay"],
        "the empty container is no training label"
    );

    let shown = service
        .label_detail("SHOPPING/ebay", None, 5)
        .await
        .unwrap();
    assert_eq!(shown[0].entry.status.positives, 1);
    assert_eq!(shown[0].samples.len(), 1);
    let parent = service.label_detail("shopping", None, 5).await.unwrap();
    assert_eq!(
        parent[0].entry.status.positives, 0,
        "known as a mailbox, taught nothing"
    );
    assert!(
        service
            .label_detail("shopping/amazon", None, 5)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn showing_a_label_tells_where_each_account_keeps_it() {
    let (world, service, _) = super::group_with_taught_label().await;

    let shown = service
        .label_detail("newsletter", Some(world.account), 3)
        .await
        .unwrap();

    let d = &shown[0];
    assert_eq!(
        (d.entry.name.as_str(), d.entry.status.positives),
        ("Newsletter", 8)
    );
    assert_eq!(d.scope.group, "Tutti");
    assert_eq!(d.samples.len(), 3);
    assert_eq!(d.presence.len(), 1);
    assert_eq!(d.presence[0].examples, 8);
    assert!(
        matches!(&d.presence[0].home, mc_application::services::tagging::LabelHome::Mailbox { id, .. } if *id == world.label)
    );
}
