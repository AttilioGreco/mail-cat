//! The nearest-neighbour signal end to end, with a fake model: nothing is
//! embedded before the user agrees, the backlog is embedded once they do, and
//! neighbours move a suggestion where the words alone would not.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use mc_domain::{Confidence, DomainError, Embedder, MessageBodyStore, TaggingStore};

use super::*;

/// Two directions: mail about discounts ("scont...") and everything else.
struct Fake(AtomicUsize);

#[async_trait]
impl Embedder for Fake {
    fn model(&self) -> &str {
        "fake@1"
    }

    async fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError> {
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

async fn until<F: Future<Output = bool>>(what: &str, mut ready: impl FnMut() -> F) {
    for _ in 0..100 {
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
    let fake = Arc::new(Fake(AtomicUsize::new(0)));
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
