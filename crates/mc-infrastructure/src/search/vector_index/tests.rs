use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use mc_domain::{DomainError, MessageId, VectorIndex, VectorSource, vector_key};

use super::UsearchIndex;

const MODEL: &str = "m@1";

/// The database's vectors, ordered by id like its keyset pages.
#[derive(Clone, Default)]
struct Source(Arc<parking_lot::Mutex<BTreeMap<Uuid, Vec<f32>>>>);

#[async_trait]
impl VectorSource for Source {
    async fn vector_page(
        &self,
        _model: &str,
        after: Option<MessageId>,
        limit: u32,
    ) -> Result<Vec<(MessageId, Vec<f32>)>, DomainError> {
        Ok(self
            .0
            .lock()
            .iter()
            .filter(|(id, _)| after.is_none_or(|a| **id > *a.as_uuid()))
            .take(limit as usize)
            .map(|(id, v)| (MessageId::from_uuid(*id), v.clone()))
            .collect())
    }

    async fn vector_count(&self, _model: &str) -> Result<u64, DomainError> {
        Ok(self.0.lock().len() as u64)
    }
}

fn unit(v: [f32; 3]) -> Vec<f32> {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    v.iter().map(|x| x / n).collect()
}

fn keys(hits: &[(u64, f32)]) -> Vec<u64> {
    hits.iter().map(|h| h.0).collect()
}

fn open(dir: &std::path::Path) -> (UsearchIndex<Source>, Source) {
    let source = Source::default();
    (UsearchIndex::new(dir, source.clone()), source)
}

fn seed(source: &Source, id: MessageId, v: Vec<f32>) {
    source.0.lock().insert(*id.as_uuid(), v);
}

/// Stores in the database and in the index, as the embedder does.
fn put(index: &UsearchIndex<Source>, source: &Source, id: MessageId, v: Vec<f32>) {
    source.0.lock().insert(*id.as_uuid(), v.clone());
    index.add(MODEL, &[(id, v)]);
}

#[tokio::test]
async fn added_vectors_are_found_closest_first() {
    let dir = tempfile::tempdir().unwrap();
    let (index, source) = open(dir.path());
    let (near, far) = (MessageId::new(), MessageId::new());
    put(&index, &source, near, unit([1.0, 0.1, 0.0]));
    put(&index, &source, far, unit([0.0, 1.0, 0.0]));
    index.ensure(MODEL).await.unwrap();
    let hits = index.nearest(MODEL, &unit([1.0, 0.0, 0.0]), 2, &|_| true);
    assert_eq!(keys(&hits), [vector_key(&near), vector_key(&far)]);
    assert!(hits[0].1 > 0.95 && hits[1].1 < 0.1, "{hits:?}");
    assert!(
        index
            .nearest("other", &unit([1.0, 0.0, 0.0]), 2, &|_| true)
            .is_empty()
    );
    assert!(index.nearest(MODEL, &[1.0], 2, &|_| true).is_empty());
}

#[tokio::test]
async fn a_removed_vector_is_no_longer_a_neighbour() {
    let dir = tempfile::tempdir().unwrap();
    let (index, source) = open(dir.path());
    let (a, b) = (MessageId::new(), MessageId::new());
    seed(&source, a, unit([1.0, 0.0, 0.0]));
    seed(&source, b, unit([1.0, 0.2, 0.0]));
    index.ensure(MODEL).await.unwrap();
    index.remove(&a);
    source.0.lock().remove(a.as_uuid());
    let hits = index.nearest(MODEL, &unit([1.0, 0.0, 0.0]), 2, &|_| true);
    assert_eq!(keys(&hits), [vector_key(&b)]);
}

#[tokio::test]
async fn adding_again_replaces_the_vector() {
    let dir = tempfile::tempdir().unwrap();
    let (index, source) = open(dir.path());
    let (a, b) = (MessageId::new(), MessageId::new());
    seed(&source, a, unit([1.0, 0.0, 0.0]));
    seed(&source, b, unit([0.0, 1.0, 0.0]));
    index.ensure(MODEL).await.unwrap();
    put(&index, &source, a, unit([0.0, 0.0, 1.0]));
    let hits = index.nearest(MODEL, &unit([1.0, 0.0, 0.0]), 5, &|_| true);
    assert_eq!(hits.len(), 2, "no stale copy of `a`: {hits:?}");
    let a_hit = hits.iter().find(|h| h.0 == vector_key(&a)).unwrap();
    assert!(a_hit.1 < 0.1, "the new vector counts: {a_hit:?}");
}

#[tokio::test]
async fn the_filter_decides_who_can_be_a_neighbour() {
    let dir = tempfile::tempdir().unwrap();
    let (index, source) = open(dir.path());
    let (a, b) = (MessageId::new(), MessageId::new());
    put(&index, &source, a, unit([1.0, 0.0, 0.0]));
    put(&index, &source, b, unit([0.0, 1.0, 0.0]));
    index.ensure(MODEL).await.unwrap();
    let only_b = index.nearest(MODEL, &unit([1.0, 0.0, 0.0]), 2, &|k| k == vector_key(&b));
    assert_eq!(keys(&only_b), [vector_key(&b)]);
}

#[tokio::test]
async fn a_file_out_of_step_with_the_database_is_rebuilt_and_a_matching_one_is_mapped() {
    let dir = tempfile::tempdir().unwrap();
    let (first, source) = open(dir.path());
    let ids: Vec<_> = (0..3).map(|_| MessageId::new()).collect();
    for id in &ids {
        source
            .0
            .lock()
            .insert(*id.as_uuid(), unit([1.0, ids.len() as f32, 0.0]));
    }
    first.ensure(MODEL).await.unwrap();
    assert!(dir.path().join("m_1.usearch").exists());

    // Restart with the file intact: nothing is rebuilt, the file is mapped.
    let again = UsearchIndex::new(dir.path(), source.clone());
    again.ensure(MODEL).await.unwrap();
    assert_eq!(
        again
            .nearest(MODEL, &unit([1.0, 3.0, 0.0]), 5, &|_| true)
            .len(),
        3
    );
    {
        let models = again.inner.models.lock();
        let base = models[MODEL].base.as_ref().unwrap();
        assert!(models[MODEL].delta.is_none(), "no rebuild was needed");
        assert_eq!(base.size(), 3);
    }

    // A vector the file does not know: the next start rebuilds it.
    let extra = MessageId::new();
    source
        .0
        .lock()
        .insert(*extra.as_uuid(), unit([0.0, 0.0, 1.0]));
    let restarted = UsearchIndex::new(dir.path(), source.clone());
    restarted.ensure(MODEL).await.unwrap();
    let hits = restarted.nearest(MODEL, &unit([0.0, 0.0, 1.0]), 1, &|_| true);
    assert_eq!(keys(&hits), [vector_key(&extra)]);

    // A corrupt file is rebuilt too.
    std::fs::write(dir.path().join("m_1.usearch"), b"nonsense").unwrap();
    let healed = UsearchIndex::new(dir.path(), source);
    healed.ensure(MODEL).await.unwrap();
    assert_eq!(
        healed
            .nearest(MODEL, &unit([1.0, 3.0, 0.0]), 9, &|_| true)
            .len(),
        4
    );
}

#[tokio::test]
async fn a_model_without_vectors_has_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let (index, _) = open(dir.path());
    index.ensure(MODEL).await.unwrap();
    assert!(
        index
            .nearest(MODEL, &unit([1.0, 0.0, 0.0]), 3, &|_| true)
            .is_empty()
    );
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
}

/// `cargo test --release -p mc-infrastructure sparse_filter -- --ignored --nocapture`
#[tokio::test]
#[ignore = "timing"]
async fn sparse_filter_time_at_100k_vectors() {
    let dir = tempfile::tempdir().unwrap();
    let (index, source) = open(dir.path());
    let mut seed = 1u64;
    let mut random = move || {
        (0..384)
            .map(|_| {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (seed >> 40) as f32 / (1u64 << 24) as f32 - 0.5
            })
            .collect::<Vec<f32>>()
    };
    let ids: Vec<_> = (0..100_000).map(|_| MessageId::new()).collect();
    for id in &ids {
        let v = random();
        let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        seed_vec(&source, *id, v.iter().map(|x| x / n).collect());
    }
    let start = std::time::Instant::now();
    index.ensure(MODEL).await.unwrap();
    println!("build: {:?}", start.elapsed());
    let wanted: std::collections::HashSet<u64> = ids.iter().step_by(100).map(vector_key).collect();
    let q = unit([1.0, 0.0, 0.0]);
    let q: Vec<f32> = q.iter().cycle().take(384).copied().collect();
    let start = std::time::Instant::now();
    let mut found = 0;
    for _ in 0..20 {
        found = index.nearest(MODEL, &q, 15, &|k| wanted.contains(&k)).len();
    }
    println!("1% filter, per query: {:?}, found {found}", start.elapsed() / 20);
}

fn seed_vec(source: &Source, id: MessageId, v: Vec<f32>) {
    source.0.lock().insert(*id.as_uuid(), v);
}
