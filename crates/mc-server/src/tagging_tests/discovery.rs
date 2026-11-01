//! Discovery end to end on a real database: clusters of unlabelled mail
//! become proposals, a refusal sticks, and an acceptance labels the mail,
//! teaches the model and leaves the label in `suggest`.

use std::time::Instant;

use mc_domain::{ModelId, ProposalStatus, TaggingStore};

use super::embeddings::Fake;
use super::*;

const MODEL: (&str, &str) = ("fake", "1");
const DIMS: usize = 6;

fn noise(seed: &mut u32) -> f32 {
    *seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    (*seed >> 8) as f32 / (1 << 24) as f32 - 0.5
}

/// A point near the centre of group `group`, in the cone all vectors share.
fn near(group: usize, seed: &mut u32) -> Vec<f32> {
    let centre = [
        [2.0, -2.0, 1.0, 0.0, 0.0, 1.0],
        [-2.0, 1.0, 0.0, 2.0, -1.0, -2.0],
    ];
    let mut v: Vec<f32> = (0..DIMS)
        .map(|d| 3.0 + centre[group][d] + noise(seed) * 0.2)
        .collect();
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.iter_mut().for_each(|x| *x /= norm);
    v
}

struct Found {
    world: World,
    service: Arc<Service>,
    commands: Arc<Recording>,
    /// Members of each planted group, as discovery should find them.
    groups: [Vec<MessageId>; 2],
    labelled: MessageId,
    trashed: MessageId,
    bare: MessageId,
}

async fn put_vector(world: &World, id: MessageId, vector: Vec<f32>) {
    let model = ModelId::new(MODEL.0, MODEL.1).to_string();
    SqliteTaggingStore::new(world.db.clone())
        .put_vectors(&model, &[(id, vector)])
        .await
        .unwrap();
}

/// Two groups of 40 unlabelled messages (orders from Amazon, offers from a
/// shop), plus three that look like the first group but must be left out: one
/// carrying a label, one in the Trash and one without a vector.
async fn found() -> Found {
    let world = World::open().await;
    let commands = Arc::new(Recording::default());
    let service = Arc::new(
        Service::new(
            SqliteTaggingStore::new(world.db.clone()),
            world.repo.clone(),
            commands.clone(),
        )
        .with_embedder(Arc::new(Fake::real(ModelId::new(MODEL.0, MODEL.1)))),
    );
    service.set_embeddings(true).await.unwrap();
    // Training is not what is tested: a scope being trained would hold discovery back.
    SqliteTaggingStore::new(world.db.clone())
        .mark_trained(mc_domain::Scope::Group(GroupId::DEFAULT))
        .await
        .unwrap();
    let mut seed = 11;
    let mut groups = [Vec::new(), Vec::new()];
    for i in 0..40 {
        let (id, _) = world
            .message(
                "orders@amazon.example",
                &format!("Il tuo ordine {i} e' in arrivo"),
                false,
            )
            .await;
        put_vector(&world, id, near(0, &mut seed)).await;
        groups[0].push(id);
        let (id, _) = world
            .message(
                "promo@shop.example",
                &format!("Offerte della settimana {i}"),
                false,
            )
            .await;
        put_vector(&world, id, near(1, &mut seed)).await;
        groups[1].push(id);
    }
    let (labelled, _) = world
        .message("orders@amazon.example", "ordine gia' smistato", true)
        .await;
    put_vector(&world, labelled, near(0, &mut seed)).await;
    let trash = MailboxId::new();
    world
        .record(
            trash.to_string(),
            AggregateType::Mailbox,
            DomainEvent::MailboxDiscovered {
                mailbox_id: trash,
                account_id: world.account,
                path: MailboxPath::new(vec!["Trash".into()], '/').unwrap(),
                display_path: "Trash".into(),
                mailbox_type: MailboxType::Trash,
            },
        )
        .await;
    let (trashed, _) = world
        .message_in(
            world.account,
            trash,
            None,
            vec![],
            "orders@amazon.example",
            "ordine cestinato",
        )
        .await;
    put_vector(&world, trashed, near(0, &mut seed)).await;
    let (bare, _) = world
        .message("orders@amazon.example", "ordine senza vettore", false)
        .await;
    Found {
        world,
        service,
        commands,
        groups,
        labelled,
        trashed,
        bare,
    }
}

impl Found {
    /// Run a search and wait for it to end.
    async fn discover(&self) {
        assert!(self.service.start_discovery(None).await.unwrap());
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if self
                .service
                .background_stats()
                .await
                .unwrap()
                .discovery
                .is_none()
            {
                return;
            }
        }
        panic!("discovery never ended");
    }

    fn sent(&self) -> Vec<(Vec<MessageId>, Vec<MailboxId>, Vec<MailboxId>)> {
        self.commands
            .0
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

    async fn pending(&self) -> Vec<mc_application::services::tagging::ProposalView> {
        self.service
            .list_proposals(None, Some(ProposalStatus::Pending), false)
            .await
            .unwrap()
    }

    /// The pending proposal about orders.
    async fn orders(&self) -> mc_domain::Proposal {
        let pending = self.pending().await;
        pending
            .into_iter()
            .map(|p| p.proposal)
            .find(|p| p.members.contains(&self.groups[0][0]))
            .expect("the orders cluster")
    }
}

async fn until_sent(found: &Found, messages: usize) {
    for _ in 0..200 {
        if found.sent().iter().map(|s| s.0.len()).sum::<usize>() >= messages {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "never applied to {messages} messages: {:?}",
        found.sent().len()
    );
}

#[tokio::test]
async fn clusters_of_unlabelled_readable_mail_with_a_vector_become_proposals() {
    let f = found().await;
    f.discover().await;

    let pending = f.pending().await;
    assert_eq!(pending.len(), 2, "{pending:?}");
    let orders = f.orders().await;
    assert_eq!(orders.size(), 40);
    assert_eq!(orders.inbox, 40);
    assert_eq!(orders.name, "Amazon");
    assert!(
        orders.keywords.iter().any(|k| k == "ordine"),
        "{:?}",
        orders.keywords
    );
    assert!(!orders.samples.is_empty() && orders.samples.len() <= 5);
    for out in [f.labelled, f.trashed, f.bare] {
        assert!(!orders.members.contains(&out), "{out} is not discoverable");
    }
    let mut members = orders.members.clone();
    let mut planted = f.groups[0].clone();
    members.sort_by_key(|m| *m.as_uuid());
    planted.sort_by_key(|m| *m.as_uuid());
    assert_eq!(members, planted);
    assert_eq!(pending[0].group, "Tutti");

    // Again: refreshed, not duplicated, and the same ids stay.
    let before: Vec<_> = pending.iter().map(|p| p.proposal.id).collect();
    f.discover().await;
    let mut after: Vec<_> = f.pending().await.iter().map(|p| p.proposal.id).collect();
    let mut before = before;
    before.sort_by_key(|i| *i.as_uuid());
    after.sort_by_key(|i| *i.as_uuid());
    assert_eq!(before, after);
}

#[tokio::test]
async fn a_rejected_cluster_is_not_proposed_again_even_when_it_has_grown() {
    let f = found().await;
    f.discover().await;
    let orders = f.orders().await;
    f.service.reject_proposal(orders.id).await.unwrap();
    assert_eq!(f.pending().await.len(), 1);

    let mut seed = 99;
    for i in 0..5 {
        let (id, _) = f
            .world
            .message(
                "orders@amazon.example",
                &format!("Il tuo ordine {} e' in arrivo", 100 + i),
                false,
            )
            .await;
        put_vector(&f.world, id, near(0, &mut seed)).await;
    }
    f.discover().await;

    let pending = f.pending().await;
    assert_eq!(pending.len(), 1, "only the offers remain: {pending:?}");
    assert!(!pending[0].proposal.members.contains(&f.groups[0][0]));
    let store = SqliteTaggingStore::new(f.world.db.clone());
    let rejected = store.proposals(ProposalStatus::Rejected).await.unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].id, orders.id);
    assert!(
        f.service.reject_proposal(orders.id).await.is_err(),
        "answered once"
    );
}

#[tokio::test]
async fn accepting_creates_the_label_applies_it_teaches_it_and_leaves_it_suggesting() {
    let f = found().await;
    f.discover().await;
    let orders = f.orders().await;

    let queued = f
        .service
        .accept_proposal(orders.id, Some("  Ordini "), false)
        .await
        .unwrap();
    assert_eq!(queued, 40);
    until_sent(&f, 40).await;

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
    assert_eq!(
        created,
        ["Ordini"],
        "created once, with the name the user gave"
    );
    let sent = f.sent();
    assert!(
        sent.iter()
            .all(|(_, add, remove)| add.len() == 1 && remove.is_empty())
    );
    let mut labelled: Vec<_> = sent.iter().flat_map(|s| s.0.clone()).collect();
    labelled.sort_by_key(|m| *m.as_uuid());
    labelled.dedup();
    assert_eq!(labelled.len(), 40);

    let status = f
        .service
        .label_status(f.world.account, "ordini")
        .await
        .unwrap();
    assert_eq!(status.settings.mode, LabelMode::Suggest);
    for _ in 0..100 {
        if f.service
            .label_status(f.world.account, "ordini")
            .await
            .unwrap()
            .positives
            == 40
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        f.service
            .label_status(f.world.account, "ordini")
            .await
            .unwrap()
            .positives,
        40,
        "the model learnt them as the user's own"
    );
    let store = SqliteTaggingStore::new(f.world.db.clone());
    let record = store.autos(f.groups[0][0]).await.unwrap().remove(0);
    assert!((record.label.as_str(), record.archived, record.confirmed) == ("ordini", false, true));

    assert_eq!(f.pending().await.len(), 1);
    let accepted = store.proposals(ProposalStatus::Accepted).await.unwrap();
    assert_eq!(
        (accepted[0].name.as_str(), accepted[0].id),
        ("Ordini", orders.id)
    );
    assert!(
        f.service
            .accept_proposal(orders.id, None, false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn accepting_and_archiving_takes_the_mail_out_of_the_inbox() {
    let f = found().await;
    f.discover().await;
    let orders = f.orders().await;
    f.service
        .accept_proposal(orders.id, None, true)
        .await
        .unwrap();
    until_sent(&f, 40).await;

    assert!(
        f.sent()
            .iter()
            .all(|(_, _, remove)| remove == &[f.world.inbox])
    );
    let store = SqliteTaggingStore::new(f.world.db.clone());
    let record = store.autos(f.groups[0][0]).await.unwrap().remove(0);
    assert!(record.archived && record.confirmed);
    assert_eq!(record.label, "amazon");
}
async fn offers(f: &Found) -> mc_domain::Proposal {
    f.pending()
        .await
        .into_iter()
        .map(|p| p.proposal)
        .find(|p| p.members.contains(&f.groups[1][0]))
        .expect("the offers cluster")
}

#[tokio::test]
async fn merging_joins_the_members_rereads_the_keywords_and_removes_the_sources() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);

    let merged = f
        .service
        .merge_proposals(&[orders.id, offers.id], None)
        .await
        .unwrap();

    let p = &merged.proposal;
    assert_eq!((p.size(), p.inbox), (80, 80));
    assert_eq!(p.status, ProposalStatus::Pending);
    assert!(p.id != orders.id && p.id != offers.id);
    let mut union: Vec<_> = f.groups.iter().flatten().copied().collect();
    let mut members = p.members.clone();
    union.sort_by_key(|m| *m.as_uuid());
    members.sort_by_key(|m| *m.as_uuid());
    assert_eq!(members, union);
    assert_ne!(p.keywords, orders.keywords);
    assert_ne!(p.keywords, offers.keywords);
    assert!(!p.samples.is_empty() && p.samples.len() <= 5);
    assert!(p.samples.iter().any(|s| orders.samples.contains(s)));
    assert!(p.samples.iter().any(|s| offers.samples.contains(s)));
    let domains: Vec<_> = merged
        .senders
        .iter()
        .map(|(d, n)| (d.as_str(), *n))
        .collect();
    assert_eq!(domains, [("amazon.example", 40), ("shop.example", 40)]);

    let store = SqliteTaggingStore::new(f.world.db.clone());
    assert_eq!(f.pending().await.len(), 1);
    assert!(store.proposal(orders.id).await.unwrap().is_none());
    assert!(
        store
            .proposals(ProposalStatus::Rejected)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn merging_takes_a_name_and_refuses_other_scopes_and_answered_proposals() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    let store = SqliteTaggingStore::new(f.world.db.clone());

    let stranger = mc_domain::Proposal {
        id: mc_domain::ProposalId::new(),
        scope: mc_domain::Scope::Alone(AccountId::new()),
        ..offers.clone()
    };
    store.put_proposal(&stranger).await.unwrap();
    let refused = f
        .service
        .merge_proposals(&[orders.id, stranger.id], None)
        .await;
    assert!(
        matches!(refused, Err(ApplicationError::PreconditionFailed(m)) if m.contains("groups"))
    );
    assert!(f.service.merge_proposals(&[orders.id], None).await.is_err());
    assert!(
        f.service
            .merge_proposals(&[orders.id, orders.id], None)
            .await
            .is_err()
    );
    assert!(
        store.proposal(orders.id).await.unwrap().is_some(),
        "nothing was lost"
    );

    f.service.reject_proposal(offers.id).await.unwrap();
    assert!(
        f.service
            .merge_proposals(&[orders.id, offers.id], None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_merge_takes_the_name_it_is_given() {
    let f = found().await;
    f.discover().await;
    let (orders, offers) = (f.orders().await, offers(&f).await);
    let merged = f
        .service
        .merge_proposals(&[offers.id, orders.id], Some(" Acquisti "))
        .await
        .unwrap();
    assert_eq!(merged.proposal.name, "Acquisti");
    assert_eq!(merged.group, "Tutti");
}

#[tokio::test]
async fn renaming_changes_a_pending_proposal_only() {
    let f = found().await;
    f.discover().await;
    let orders = f.orders().await;

    let renamed = f
        .service
        .rename_proposal(orders.id, " Ordini ")
        .await
        .unwrap();
    assert_eq!(renamed.proposal.name, "Ordini");
    assert_eq!(f.orders().await.name, "Ordini");
    assert!(f.service.rename_proposal(orders.id, "  ").await.is_err());
    f.service.reject_proposal(orders.id).await.unwrap();
    assert!(f.service.rename_proposal(orders.id, "Altro").await.is_err());
}

#[tokio::test]
async fn assigning_puts_the_existing_label_instead_of_creating_one() {
    let f = found().await;
    f.discover().await;
    let orders = f.orders().await;

    let missing = f
        .service
        .assign_proposal(orders.id, "Non esiste", false)
        .await;
    assert!(matches!(
        missing,
        Err(ApplicationError::NotFound("Label", _))
    ));
    assert_eq!(f.pending().await.len(), 2, "still waiting");

    let queued = f
        .service
        .assign_proposal(orders.id, " newsletter ", false)
        .await
        .unwrap();
    assert_eq!(queued, 40);
    until_sent(&f, 40).await;

    let created = f
        .commands
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|c| matches!(c, Command::CreateLabel { .. }))
        .count();
    assert_eq!(created, 0);
    assert!(f.sent().iter().all(|(_, add, _)| add == &[f.world.label]));
    let store = SqliteTaggingStore::new(f.world.db.clone());
    let accepted = store.proposals(ProposalStatus::Accepted).await.unwrap();
    assert_eq!(
        (accepted[0].id, accepted[0].name.as_str()),
        (orders.id, "Newsletter")
    );
    let record = store.autos(f.groups[0][0]).await.unwrap().remove(0);
    assert!((record.label.as_str(), record.confirmed) == ("newsletter", true));
}

#[tokio::test]
async fn listing_filters_by_status_and_account_and_shows_the_senders() {
    let f = found().await;
    f.discover().await;
    let orders = f.orders().await;
    f.service.reject_proposal(orders.id).await.unwrap();

    let count = |views: Vec<mc_application::services::tagging::ProposalView>| views.len();
    let list = |account, status, senders| f.service.list_proposals(account, status, senders);
    assert_eq!(
        count(
            list(None, Some(ProposalStatus::Pending), false)
                .await
                .unwrap()
        ),
        1
    );
    assert_eq!(
        count(
            list(None, Some(ProposalStatus::Rejected), false)
                .await
                .unwrap()
        ),
        1
    );
    assert_eq!(count(list(None, None, false).await.unwrap()), 2);
    assert_eq!(
        count(list(Some(f.world.account), None, false).await.unwrap()),
        2
    );
    let with = list(None, Some(ProposalStatus::Rejected), true)
        .await
        .unwrap();
    assert_eq!(with[0].senders, [("amazon.example".to_owned(), 40)]);
    assert!(
        list(None, None, false)
            .await
            .unwrap()
            .iter()
            .all(|v| v.senders.is_empty())
    );

    let (detail, messages) = f.service.proposal_detail(orders.id, 7).await.unwrap();
    assert_eq!(detail.proposal.status, ProposalStatus::Rejected);
    assert_eq!(messages.len(), 7);
    assert!(messages.iter().all(|m| m.from == "orders@amazon.example"));
    let first: Vec<_> = messages
        .iter()
        .take(orders.samples.len())
        .map(|m| m.message_id)
        .collect();
    let samples: Vec<_> = orders.samples.iter().map(|s| s.message_id).collect();
    assert_eq!(first, samples, "the central ones come first");
}

#[tokio::test]
async fn labels_are_listed_with_their_settings_per_scope() {
    let (world, service, _) = group_with_taught_label().await;
    let scopes = service.list_labels(Some(world.account)).await.unwrap();
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[0].group, "Tutti");
    let label = &scopes[0].labels[0];
    assert_eq!(label.name, "Newsletter");
    assert_eq!(label.status.positives, 8);
    assert_eq!(label.status.settings.mode, LabelMode::Off);
    assert_eq!(service.list_labels(None).await.unwrap().len(), 1);
}

/// Not a regression test: run it by hand on a copy of a real database to see
/// what discovery makes of the mail (`MC_REAL_DB=/path/to/copy`).
#[tokio::test]
#[ignore = "needs a copy of a real database"]
async fn real_mail() {
    let path = std::env::var("MC_REAL_DB").expect("MC_REAL_DB");
    let db = SqliteDb::open(std::path::Path::new(&path)).unwrap();
    let events = Arc::new(SqliteEventStore::new(db.clone()));
    let repo = SqliteRepository::new(db.clone(), events);
    let fake = Arc::new(super::embeddings::Fake::real(ModelId::new(
        "multilingual-e5-small",
        "761b726dd34fb83930e26aab4e9ac3899aa1fa78",
    )));
    let service = Arc::new(
        Service::new(
            SqliteTaggingStore::new(db),
            repo,
            Arc::new(Recording::default()),
        )
        .with_embedder(fake),
    );
    service.set_embeddings(true).await.unwrap();
    let started = Instant::now();
    assert!(service.start_discovery(None).await.unwrap());
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let stats = service.background_stats().await.unwrap();
        match stats.discovery {
            Some(p) => eprintln!("{:?} {}/{}", p.phase, p.done, p.total),
            None => break,
        }
    }
    eprintln!("discovery took {:?}", started.elapsed());
    let proposals = service
        .list_proposals(None, Some(ProposalStatus::Pending), false)
        .await
        .unwrap();
    eprintln!("{} proposals", proposals.len());
    for p in &proposals {
        let p = &p.proposal;
        eprintln!(
            "{:>5} (inbox {:>5}) {:<24} {:?}",
            p.size(),
            p.inbox,
            p.name,
            p.keywords
        );
        for s in p.samples.iter().take(2) {
            eprintln!("          - {} <{}>", s.subject, s.from);
        }
    }
    let rss = std::fs::read_to_string("/proc/self/status").unwrap();
    eprintln!(
        "{}",
        rss.lines()
            .filter(|l| l.starts_with("VmHWM"))
            .collect::<String>()
    );
}
