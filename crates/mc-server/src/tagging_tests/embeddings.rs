//! The nearest-neighbour signal end to end, with a fake model: nothing is
//! embedded before the user agrees, the backlog is embedded once they do, and
//! neighbours move a suggestion where the words alone would not.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use mc_domain::{
    Confidence, DomainError, EmbedPurpose, Embedder, MessageBodyStore, ModelId, ModelProfile,
    TaggingStore,
};

use super::*;

/// Two directions: mail about discounts ("scont...") and everything else.
pub(super) struct Fake(AtomicUsize, std::sync::Mutex<ModelProfile>, Option<ModelId>);

impl Fake {
    /// Reports a model of the given id, whatever the profile.
    pub(super) fn real(model: ModelId) -> Self {
        Self(
            AtomicUsize::new(0),
            std::sync::Mutex::default(),
            Some(model),
        )
    }

    fn new() -> Self {
        Self(AtomicUsize::new(0), std::sync::Mutex::default(), None)
    }
}

#[async_trait]
impl Embedder for Fake {
    /// "fake@1" for Light, "fake@2" for the others.
    fn model(&self) -> ModelId {
        if let Some(model) = &self.2 {
            return model.clone();
        }
        let revision = if *self.1.lock().unwrap() == ModelProfile::Light {
            "1"
        } else {
            "2"
        };
        ModelId::new("fake", revision)
    }

    fn dims(&self) -> usize {
        2
    }

    fn select(&self, profile: ModelProfile, target: &str) -> Result<(), DomainError> {
        if target != "cpu" {
            return Err(DomainError::Validation("non disponibile".into()));
        }
        *self.1.lock().unwrap() = profile;
        Ok(())
    }

    async fn embed(
        &self,
        texts: Vec<String>,
        purpose: EmbedPurpose,
    ) -> Result<Vec<Vec<f32>>, DomainError> {
        assert_eq!(purpose, EmbedPurpose::Document, "messages are documents");
        self.0.fetch_add(texts.len(), Ordering::SeqCst);
        Ok(texts
            .iter()
            .map(|t| {
                if t.contains("scont") {
                    vec![1.0, 0.0]
                } else {
                    vec![0.0, 1.0]
                }
            })
            .collect())
    }
}

// Generous: CI runners are slow and run every test in parallel.
async fn until<F: Future<Output = bool>>(what: &str, mut ready: impl FnMut() -> F) {
    for _ in 0..400 {
        if ready().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("never happened: {what}");
}

async fn give_body(world: &World, id: MessageId, text: &str) {
    world
        .repo
        .put_body(&id, Some(text.into()), None, false)
        .await
        .unwrap();
}

async fn confidence(service: &Service, id: MessageId) -> Option<Confidence> {
    let tags = service.tags_for(id).await.unwrap();
    tags.suggestions
        .iter()
        .find(|s| s.label == "newsletter")
        .map(|s| s.confidence)
}

#[tokio::test]
async fn neighbours_join_the_words_once_the_user_agrees() {
    let (world, _, _) = group_with_taught_label().await;
    let fake = Arc::new(Fake::new());
    let service = Arc::new(
        Service::new(
            SqliteTaggingStore::new(world.db.clone()),
            world.repo.clone(),
            Arc::new(Recording::default()),
        )
        .with_embedder(fake.clone()),
    );
    let store = SqliteTaggingStore::new(world.db.clone());
    // `group_with_taught_label` made 8 labelled "offerte" and 8 plain "cena".
    let ids: Vec<(MessageId, bool)> = {
        let accounts = vec![world.account];
        store
            .source_page(&accounts, mc_domain::SourceFilter::Any, None, false, 100)
            .await
            .unwrap()
            .into_iter()
            .map(|s| (s.id, s.subject.starts_with("offerte")))
            .collect()
    };
    for &(id, news) in &ids {
        give_body(&world, id, if news { "sconti" } else { "cena" }).await;
    }
    service.assign_account(world.account, None).await.unwrap();
    service
        .assign_account(world.account, Some(GroupId::DEFAULT))
        .await
        .unwrap();
    service
        .set_label(world.account, "newsletter", LabelMode::Suggest, 0.9)
        .await
        .unwrap();

    let bus = InProcessEventBus::new(16);
    let shutdown = CancellationToken::new();
    service.spawn(bus.subscribe(), shutdown.clone());

    // A stranger with an unseen body, near the dinner mail: the words know nothing and fall back on how
    // common the label is. A regular sender of the newsletter, but about dinner.
    let (stranger, a1) = world.message("y@new.example", "ciao", false).await;
    give_body(&world, stranger, "qqqq").await;
    let (regular, a2) = world
        .message("news@shop.example", "offerte nuove", false)
        .await;
    give_body(&world, regular, "cena").await;
    bus.publish(a1);
    bus.publish(a2);

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        fake.0.load(Ordering::SeqCst),
        0,
        "nothing is embedded without consent"
    );
    assert!(
        store
            .vectors("fake@1", &[stranger])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        confidence(&service, stranger).await,
        Some(Confidence::Medium)
    );
    assert_eq!(confidence(&service, regular).await, Some(Confidence::High));

    service.set_embeddings(true).await.unwrap();
    until("the backlog is embedded", || async {
        fake.0.load(Ordering::SeqCst) == ids.len() + 2
    })
    .await;
    assert_eq!(
        store
            .vectors("fake@1", &[stranger, regular])
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        confidence(&service, stranger).await,
        None,
        "its neighbours are not newsletters"
    );
    assert_ne!(confidence(&service, regular).await, Some(Confidence::High));

    service.set_embeddings(false).await.unwrap();
    assert_eq!(
        confidence(&service, stranger).await,
        Some(Confidence::Medium)
    );
    assert_eq!(confidence(&service, regular).await, Some(Confidence::High));
    shutdown.cancel();
}

#[tokio::test]
async fn the_backlog_waits_while_a_scope_is_being_retrained() {
    let (world, _, _) = group_with_taught_label().await;
    let fake = Arc::new(Fake::new());
    let service = Arc::new(
        Service::new(
            SqliteTaggingStore::new(world.db.clone()),
            world.repo.clone(),
            Arc::new(Recording::default()),
        )
        .with_embedder(fake.clone()),
    );
    let store = SqliteTaggingStore::new(world.db.clone());
    let accounts = vec![world.account];
    let ids: Vec<_> = store
        .source_page(&accounts, mc_domain::SourceFilter::Any, None, false, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    for &id in &ids {
        give_body(&world, id, "cena").await;
    }
    // The retrain rests between pages (500 ms at the normal speed), so it
    // is under way for about a second after the service starts.
    store.set_embeddings_enabled(true).await.unwrap();
    store
        .mark_untrained(mc_domain::Scope::Group(GroupId::DEFAULT))
        .await
        .unwrap();
    let shutdown = CancellationToken::new();
    service.spawn(InProcessEventBus::new(16).subscribe(), shutdown.clone());

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        fake.0.load(Ordering::SeqCst),
        0,
        "embedding waits for the retrain"
    );
    assert!(
        !store
            .is_trained(mc_domain::Scope::Group(GroupId::DEFAULT))
            .await
            .unwrap()
    );

    until("the backlog is embedded after the retrain", || async {
        fake.0.load(Ordering::SeqCst) == ids.len()
    })
    .await;
    assert!(
        store
            .is_trained(mc_domain::Scope::Group(GroupId::DEFAULT))
            .await
            .unwrap()
    );
    shutdown.cancel();
}

#[tokio::test]
async fn a_new_model_embeds_the_mail_again_and_a_refused_target_changes_nothing() {
    let (world, _, _) = group_with_taught_label().await;
    let fake = Arc::new(Fake::new());
    let service = Arc::new(
        Service::new(
            SqliteTaggingStore::new(world.db.clone()),
            world.repo.clone(),
            Arc::new(Recording::default()),
        )
        .with_embedder(fake.clone()),
    );
    let store = SqliteTaggingStore::new(world.db.clone());
    let accounts = vec![world.account];
    let ids: Vec<_> = store
        .source_page(&accounts, mc_domain::SourceFilter::Any, None, false, 100)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    for &id in &ids {
        give_body(&world, id, "cena").await;
    }
    assert_eq!(service.model_profile(), ModelProfile::Light);
    assert_eq!(service.execution_target(), "cpu");

    let shutdown = CancellationToken::new();
    service.spawn(InProcessEventBus::new(16).subscribe(), shutdown.clone());
    service.set_embeddings(true).await.unwrap();
    until("embedded with the first model", || async {
        fake.0.load(Ordering::SeqCst) == ids.len()
    })
    .await;
    assert_eq!(
        store.vectors("fake@1", &ids).await.unwrap().len(),
        ids.len()
    );

    service
        .set_model_profile(ModelProfile::Balanced)
        .await
        .unwrap();
    until("embedded again with the second", || async {
        fake.0.load(Ordering::SeqCst) == 2 * ids.len()
    })
    .await;
    assert_eq!(
        store.vectors("fake@2", &ids).await.unwrap().len(),
        ids.len()
    );
    assert_eq!(store.model_profile().await.unwrap(), ModelProfile::Balanced);

    assert!(service.set_execution_target("gpu-cuda").await.is_err());
    assert_eq!(service.execution_target(), "cpu");
    assert_eq!(store.execution_target().await.unwrap(), "cpu");
    shutdown.cancel();
}
