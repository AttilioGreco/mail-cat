//! Cosine nearest neighbours over message embeddings, in a usearch file per
//! model that is memory-mapped, so a million vectors cost page cache rather
//! than heap.
//!
//! The file is immutable. What changes after it was built lives in memory: a
//! small index of the vectors added since, and the keys removed (or replaced)
//! from the file. Past `COMPACT_AT` added vectors the file is rebuilt from the
//! database in the background.
//!
//! ponytail: the delta and the tombstones are not saved, so a restart that
//! finds the file out of step with the database rebuilds it. Persist them if
//! rebuilds at startup get too slow at 2M vectors.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, warn};
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use mc_domain::{DomainError, MessageId, VectorIndex, VectorSource, vector_key};

#[cfg(test)]
mod tests;

const COMPACT_AT: usize = 20_000;
const PAGE: u32 = 2000;

fn options(dimensions: usize) -> IndexOptions {
    IndexOptions {
        dimensions,
        metric: MetricKind::Cos,
        // Unit vectors lose little in 8 bits, and the file is a quarter the size.
        quantization: ScalarKind::I8,
        ..Default::default()
    }
}

fn new_index(dimensions: usize) -> Result<Index, DomainError> {
    Index::new(&options(dimensions)).map_err(internal)
}

fn internal(e: impl std::fmt::Display) -> DomainError {
    DomainError::SearchIndex(format!("vector index: {e}"))
}

#[derive(Default)]
struct Rebuild {
    /// What was added and removed since it started, to carry over.
    added: HashMap<u64, Vec<f32>>,
    removed: HashSet<u64>,
}

#[derive(Default)]
struct Model {
    base: Option<Index>,
    /// Keys of `base` that no longer count.
    tombstones: HashSet<u64>,
    delta: Option<Index>,
    rebuilding: Option<Rebuild>,
    checked: bool,
}

impl Model {
    fn len(&self) -> u64 {
        let base = self.base.as_ref().map_or(0, Index::size);
        let delta = self.delta.as_ref().map_or(0, Index::size);
        (base + delta - self.tombstones.len()) as u64
    }

    fn remove(&mut self, key: u64) {
        if let Some(delta) = &self.delta {
            let _ = delta.remove(key);
        }
        if self.base.as_ref().is_some_and(|b| b.contains(key)) {
            self.tombstones.insert(key);
        }
        if let Some(r) = &mut self.rebuilding {
            r.added.remove(&key);
            r.removed.insert(key);
        }
    }

    fn add(&mut self, key: u64, vector: &[f32]) -> Result<(), DomainError> {
        self.remove(key);
        let delta = match &mut self.delta {
            Some(d) => d,
            none => none.insert(new_index(vector.len())?),
        };
        if delta.dimensions() != vector.len() {
            return Err(internal("vector of another size than the index"));
        }
        if delta.size() >= delta.capacity() {
            delta
                .reserve(delta.capacity().max(1024) * 2)
                .map_err(internal)?;
        }
        delta.add(key, vector).map_err(internal)?;
        if let Some(r) = &mut self.rebuilding {
            r.added.insert(key, vector.to_vec());
        }
        Ok(())
    }
}

struct Inner<S> {
    dir: PathBuf,
    source: S,
    models: parking_lot::Mutex<HashMap<String, Model>>,
    /// One rebuild at a time, whoever asks.
    build: tokio::sync::Mutex<()>,
}

pub struct UsearchIndex<S> {
    inner: Arc<Inner<S>>,
}

impl<S: VectorSource + 'static> UsearchIndex<S> {
    pub fn new(dir: impl Into<PathBuf>, source: S) -> Self {
        Self {
            inner: Arc::new(Inner {
                dir: dir.into(),
                source,
                models: parking_lot::Mutex::default(),
                build: tokio::sync::Mutex::new(()),
            }),
        }
    }
}

impl<S: VectorSource + 'static> Inner<S> {
    fn path(&self, model: &str) -> PathBuf {
        let name: String = model
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.dir.join(format!("{name}.usearch"))
    }

    /// The model's file, mapped; `None` when it is missing or unreadable.
    fn open(&self, model: &str) -> Option<Index> {
        let path = self.path(model);
        if !path.exists() {
            return None;
        }
        // The dimensions come from the file's header on `view`.
        let index = new_index(1).and_then(|i| view(&i, &path).map(|()| i));
        index
            .inspect_err(|e| warn!(error = %e, "vector index: unreadable file, rebuilding"))
            .ok()
    }

    async fn ensure(self: &Arc<Self>, model: &str) -> Result<(), DomainError> {
        let _turn = self.build.lock().await;
        if self
            .models
            .lock()
            .entry(model.to_owned())
            .or_default()
            .checked
        {
            return Ok(());
        }
        let opened = self.open(model);
        let expected = self.source.vector_count(model).await?;
        let in_step = {
            let mut models = self.models.lock();
            let state = models.entry(model.to_owned()).or_default();
            state.checked = true;
            state.base = opened;
            state.len() == expected
        };
        if in_step {
            return Ok(());
        }
        self.rebuild(model).await
    }

    /// Fresh file from the database. What changed meanwhile is carried over.
    async fn rebuild(self: &Arc<Self>, model: &str) -> Result<(), DomainError> {
        {
            let mut models = self.models.lock();
            let state = models.entry(model.to_owned()).or_default();
            if state.rebuilding.is_some() {
                return Ok(());
            }
            state.rebuilding = Some(Rebuild::default());
        }
        let built = self.build_file(model).await;
        let mut models = self.models.lock();
        let state = models.entry(model.to_owned()).or_default();
        let rebuilt = state.rebuilding.take().unwrap_or_default();
        let Some(base) = built? else {
            return Ok(());
        };
        info!(model, vectors = base.size(), "vector index rebuilt");
        state.base = Some(base);
        state.tombstones = rebuilt.removed;
        state.delta = None;
        for (key, vector) in rebuilt.added {
            state.add(key, &vector)?;
        }
        Ok(())
    }

    /// `None` when the model has no vectors: no file then.
    async fn build_file(self: &Arc<Self>, model: &str) -> Result<Option<Index>, DomainError> {
        let (inner, model) = (Arc::clone(self), model.to_owned());
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let mut index: Option<Index> = None;
            let mut after = None;
            loop {
                let page = handle.block_on(inner.source.vector_page(&model, after, PAGE))?;
                let Some(last) = page.last().map(|(id, _)| *id) else {
                    break;
                };
                after = Some(last);
                let index = match &mut index {
                    Some(i) => i,
                    none => none.insert(new_index(page[0].1.len())?),
                };
                if index.size() + page.len() > index.capacity() {
                    index
                        .reserve((index.capacity() + page.len()) * 2)
                        .map_err(internal)?;
                }
                for (id, vector) in page.iter().filter(|(_, v)| v.len() == index.dimensions()) {
                    index.add(vector_key(id), vector).map_err(internal)?;
                }
            }
            let path = inner.path(&model);
            let Some(index) = index else {
                let _ = std::fs::remove_file(&path);
                return Ok(None);
            };
            std::fs::create_dir_all(&inner.dir).map_err(internal)?;
            let tmp = path.with_extension("tmp");
            save(&index, &tmp)?;
            std::fs::rename(&tmp, &path).map_err(internal)?;
            // Mapped from the file, so the heap copy is dropped.
            let mapped = new_index(index.dimensions())?;
            view(&mapped, &path)?;
            Ok(Some(mapped))
        })
        .await
        .map_err(internal)?
    }
}

fn save(index: &Index, path: &Path) -> Result<(), DomainError> {
    index.save(&path.to_string_lossy()).map_err(internal)
}

fn view(index: &Index, path: &Path) -> Result<(), DomainError> {
    index.view(&path.to_string_lossy()).map_err(internal)
}

#[async_trait]
impl<S: VectorSource + 'static> VectorIndex for UsearchIndex<S> {
    fn add(&self, model: &str, vectors: &[(MessageId, Vec<f32>)]) {
        let mut models = self.inner.models.lock();
        let state = models.entry(model.to_owned()).or_default();
        for (id, vector) in vectors {
            if let Err(e) = state.add(vector_key(id), vector) {
                warn!(error = %e, "vector index: vector skipped");
            }
        }
        let compact = state.checked
            && state.rebuilding.is_none()
            && state.delta.as_ref().is_some_and(|d| d.size() >= COMPACT_AT);
        drop(models);
        if !compact {
            return;
        }
        let (inner, model) = (Arc::clone(&self.inner), model.to_owned());
        tokio::spawn(async move {
            let _turn = inner.build.lock().await;
            if let Err(e) = inner.rebuild(&model).await {
                warn!(error = %e, "vector index: rebuild failed");
            }
        });
    }

    fn remove(&self, id: &MessageId) {
        let key = vector_key(id);
        for state in self.inner.models.lock().values_mut() {
            state.remove(key);
        }
    }

    fn nearest(
        &self,
        model: &str,
        query: &[f32],
        k: usize,
        keep: &dyn Fn(u64) -> bool,
    ) -> Vec<(u64, f32)> {
        let models = self.inner.models.lock();
        let Some(state) = models.get(model) else {
            return Vec::new();
        };
        let search = |index: &Option<Index>, live: &dyn Fn(u64) -> bool| -> Vec<(u64, f32)> {
            let Some(index) = index.as_ref().filter(|i| i.dimensions() == query.len()) else {
                return Vec::new();
            };
            index
                .filtered_search(query, k, |key| live(key) && keep(key))
                .map(|m| {
                    m.keys
                        .into_iter()
                        .zip(m.distances)
                        .map(|(key, d)| (key, 1.0 - d))
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut hits = search(&state.base, &|key| !state.tombstones.contains(&key));
        hits.extend(search(&state.delta, &|_| true));
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
        hits.dedup_by_key(|h| h.0);
        hits.truncate(k);
        hits
    }

    async fn ensure(&self, model: &str) -> Result<(), DomainError> {
        self.inner.ensure(model).await
    }
}
