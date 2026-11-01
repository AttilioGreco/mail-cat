//! Labelling mail in bulk on a real database: everything from a sender or
//! list, and every label the classifier is sure of. The recording command bus
//! changes no mail, so what is checked is the commands it saw and what the
//! model recorded.

use mc_domain::TaggingStore;

use super::relabel::mailbox;
use super::*;

fn service(world: &World) -> (Arc<Service>, Arc<Recording>, SqliteTaggingStore) {
    let commands = Arc::new(Recording::default());
    let service = Arc::new(Service::new(
        SqliteTaggingStore::new(world.db.clone()),
        world.repo.clone(),
        commands.clone(),
    ));
    (service, commands, SqliteTaggingStore::new(world.db.clone()))
}

impl Recording {
    /// (messages, added, removed) of every label change seen.
    fn changes(&self) -> Vec<(Vec<MessageId>, Vec<MailboxId>, Vec<MailboxId>)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|c| match c {
                Command::ChangeLabels {
                    message_ids,
                    add,
                    remove,
                } => Some((message_ids.clone(), add.clone(), remove.clone())),
                _ => None,
            })
            .collect()
    }

    fn created(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|c| match c {
                Command::CreateLabel { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect()
    }

    fn changed(&self) -> usize {
        self.changes().iter().map(|c| c.0.len()).sum()
    }
}

async fn until(what: &str, mut done: impl AsyncFnMut() -> bool) {
    for _ in 0..200 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("never happened: {what}");
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|s| s.to_string()).collect()
}

async fn trash(world: &World) -> MailboxId {
    mailbox(world, world.account, "Trash", MailboxType::Trash).await
}

#[tokio::test]
async fn a_domain_is_labelled_with_a_new_label_and_the_model_learns_it() {
    let world = World::open().await;
    let (service, commands, store) = service(&world);
    let new_label = MailboxId::new();
    *commands.1.lock().unwrap() = Some(new_label);
    let mut wanted = Vec::new();
    for from in [
        "preventivo@remind.facile.it",
        "preventivo@remind.facile.it",
        "info@facile.it",
    ] {
        wanted.push(world.message(from, "Il tuo preventivo", false).await.0);
    }
    world.message("x@notfacile.it", "Altro", false).await;
    let trash = trash(&world).await;
    world
        .message_in(world.account, trash, None, vec![], "a@facile.it", "Cestino")
        .await;

    let domains = strings(&["facile.it"]);
    let asked =
        |dry_run| service.assign_matching("Casa/Facile", &domains, &[], None, true, dry_run);
    let preview = asked(true).await.unwrap();

    assert_eq!(
        (preview.to_label, preview.to_archive, preview.already),
        (3, 3, 0)
    );
    assert!(preview.creates && preview.accounts[0].creates);
    assert_eq!(
        (preview.label.as_str(), preview.sample.len()),
        ("Casa/Facile", 3)
    );
    assert!(
        commands.0.lock().unwrap().is_empty(),
        "a dry run does nothing"
    );

    let report = asked(false).await.unwrap();
    assert_eq!(report.to_label, 3);
    until("labelled", async || commands.changed() == 3).await;
    assert_eq!(commands.created(), ["Casa/Facile"]);
    let (ids, add, remove) = commands.changes().remove(0);
    assert_eq!(
        (add, remove),
        (vec![new_label], vec![world.inbox]),
        "archived out of the Inbox"
    );
    assert_eq!(ids.len(), 3);

    // The model learns them as the user's own, and the records are not autos
    // that a retrain would count against the label.
    for id in wanted {
        until("learnt", async || {
            store
                .example(id)
                .await
                .unwrap()
                .is_some_and(|e| e.positives == ["casa/facile"])
        })
        .await;
        let record = &store.autos(id).await.unwrap()[0];
        assert!(record.confirmed && record.proposal_id.is_none() && record.archived);
    }
    let settings = store
        .settings(mc_domain::Scope::Group(GroupId::DEFAULT))
        .await
        .unwrap();
    assert_eq!(settings[0].mode, LabelMode::Suggest);
}

#[tokio::test]
async fn a_list_id_matches_and_labelled_mail_is_skipped() {
    let world = World::open().await;
    let (service, commands, _) = service(&world);
    *world.list_id.lock().unwrap() = Some("Weekly <News.Example.com>".into());
    let fresh = world.message("a@x.example", "uno", false).await.0;
    let fresh2 = world.message("b@y.example", "due", false).await.0;
    world.message("c@x.example", "gia' a posto", true).await;
    *world.list_id.lock().unwrap() = None;
    world.message("d@x.example", "senza lista", false).await;

    let report = service
        .assign_matching(
            "newsletter",
            &[],
            &strings(&["<news.example.com>"]),
            None,
            false,
            false,
        )
        .await
        .unwrap();

    assert_eq!(
        (report.to_label, report.already, report.creates),
        (2, 1, false)
    );
    assert_eq!(report.label, "Newsletter", "the group's spelling");
    until("labelled", async || commands.changed() == 2).await;
    let (mut ids, add, remove) = commands.changes().remove(0);
    ids.sort_by_key(|i| *i.as_uuid());
    let mut expected = vec![fresh, fresh2];
    expected.sort_by_key(|i| *i.as_uuid());
    assert_eq!((ids, add, remove), (expected, vec![world.label], vec![]));
    assert!(commands.created().is_empty());
}

#[tokio::test]
async fn assigning_needs_a_filter() {
    let world = World::open().await;
    let (service, commands, _) = service(&world);
    let refused = service
        .assign_matching("Casa", &strings(&[" "]), &[], None, false, false)
        .await;
    assert!(matches!(
        refused,
        Err(ApplicationError::PreconditionFailed(_))
    ));
    assert!(commands.0.lock().unwrap().is_empty());
}

struct Trained {
    world: World,
    service: Arc<Service>,
    commands: Arc<Recording>,
    store: SqliteTaggingStore,
    family: MailboxId,
    shop: Vec<MessageId>,
    home: Vec<MessageId>,
}

/// Two labels with eight examples each, and unlabelled mail that looks like
/// each of them: three shop offers and two family dinners.
async fn trained() -> Trained {
    let world = World::open().await;
    let (service, commands, store) = service(&world);
    let family = mailbox(&world, world.account, "Famiglia", MailboxType::Custom).await;
    for i in 0..8 {
        world
            .message(
                "news@shop.example",
                &format!("offerte della settimana {i}"),
                true,
            )
            .await;
        world
            .message_in(
                world.account,
                world.inbox,
                Some(family),
                vec![],
                "anna@family.example",
                &format!("cena domenica {i}"),
            )
            .await;
    }
    let (mut shop, mut home) = (Vec::new(), Vec::new());
    for i in 0..3 {
        shop.push(
            world
                .message("news@shop.example", &format!("offerte speciali {i}"), false)
                .await
                .0,
        );
    }
    for i in 0..2 {
        home.push(
            world
                .message(
                    "anna@family.example",
                    &format!("cena domenica sera {i}"),
                    false,
                )
                .await
                .0,
        );
    }
    // Moving the account out and back is how a group is retrained from scratch.
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();
    Trained {
        world,
        service,
        commands,
        store,
        family,
        shop,
        home,
    }
}

#[tokio::test]
async fn the_preview_counts_each_label_in_one_pass_and_changes_nothing() {
    let t = trained().await;

    let all = t.service.apply_all(None, &[], true).await.unwrap();

    let counts: Vec<_> = all
        .iter()
        .map(|l| (l.name.as_str(), l.mode, l.positives, l.to_label))
        .collect();
    assert_eq!(
        counts,
        [
            ("Newsletter", LabelMode::Off, 8, 3),
            ("Famiglia", LabelMode::Off, 8, 2)
        ]
    );
    assert_eq!(all[0].sample.len(), 3);
    let one = t
        .service
        .apply_all(None, &strings(&["FAMIGLIA"]), true)
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert!(
        t.service
            .apply_all(None, &strings(&["nulla"]), true)
            .await
            .is_err()
    );
    assert!(t.commands.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_run_labels_only_the_chosen_labels_without_archiving_or_changing_modes() {
    let t = trained().await;
    t.service
        .set_label(t.world.account, "Newsletter", LabelMode::ApplyArchive, 0.9)
        .await
        .unwrap();

    // By default the labels that are not off: Famiglia is off.
    let queued = t.service.apply_all(None, &[], false).await.unwrap();
    assert_eq!(queued.len(), 1);
    until("shop mail labelled", async || t.commands.changed() == 3).await;
    let (mut ids, add, remove) = t.commands.changes().remove(0);
    ids.sort_by_key(|i| *i.as_uuid());
    let mut expected = t.shop.clone();
    expected.sort_by_key(|i| *i.as_uuid());
    assert_eq!(
        (ids, add, remove),
        (expected, vec![t.world.label], vec![]),
        "never archives"
    );
    let record = &t.store.autos(t.shop[0]).await.unwrap()[0];
    assert!(!record.confirmed && !record.archived);
    let status = t
        .service
        .label_status(t.world.account, "newsletter")
        .await
        .unwrap();
    assert_eq!(status.settings.mode, LabelMode::ApplyArchive);

    t.service
        .apply_all(None, &strings(&["famiglia"]), false)
        .await
        .unwrap();
    until("family mail labelled", async || t.commands.changed() == 5).await;
    let changes = t.commands.changes();
    assert_eq!(changes[1].1, [t.family]);
    assert_eq!(changes[1].0.len(), t.home.len());
    let status = t
        .service
        .label_status(t.world.account, "famiglia")
        .await
        .unwrap();
    assert_eq!(status.settings.mode, LabelMode::Off, "modes are left alone");
}
