use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mc_domain::{DomainError, EmbedPurpose, Embedder, ModelId, ModelProfile};

use super::*;

#[derive(Default)]
struct Counts {
    loads: AtomicUsize,
    drops: AtomicUsize,
}

struct Fake(Arc<Counts>);

impl Engine for Fake {
    fn embed(&mut self, texts: Vec<String>, _: EmbedPurpose) -> Result<Vec<Vec<f32>>, DomainError> {
        Ok(texts.iter().map(|t| vec![t.len() as f32]).collect())
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

const DOC: EmbedPurpose = EmbedPurpose::Document;
const IDLE: Duration = Duration::from_secs(300);

fn embedder(counts: &Arc<Counts>) -> LazyEmbedder<Fake> {
    let counts = counts.clone();
    LazyEmbedder::new(
        ModelId::new("fake", "1"),
        1,
        IDLE,
        Box::new(move |workers, _| {
            let counts = counts.clone();
            Box::pin(async move {
                counts.loads.fetch_add(workers, Ordering::SeqCst);
                Ok((0..workers).map(|_| Fake(counts.clone())).collect())
            })
        }),
    )
}

#[tokio::test(start_paused = true)]
async fn nothing_loads_until_asked_then_the_engine_is_reused() {
    let counts = Arc::new(Counts::default());
    let e = embedder(&counts);
    assert_eq!(counts.loads.load(Ordering::SeqCst), 0);
    assert!(!e.loaded());

    assert_eq!(
        e.embed(vec!["abc".into()], DOC).await.unwrap(),
        vec![vec![3.0]]
    );
    e.embed(vec!["x".into()], DOC).await.unwrap();
    assert_eq!(counts.loads.load(Ordering::SeqCst), 1);
    assert!(e.loaded());
    assert_eq!(e.model().to_string(), "fake@1");
}

#[tokio::test(start_paused = true)]
async fn the_engine_is_dropped_after_idle_and_loaded_again_on_demand() {
    let counts = Arc::new(Counts::default());
    let e = embedder(&counts);
    e.embed(vec!["a".into()], DOC).await.unwrap();

    tokio::time::sleep(IDLE - Duration::from_secs(1)).await;
    assert!(e.loaded(), "not yet idle long enough");
    // Using it restarts the clock.
    e.embed(vec!["a".into()], DOC).await.unwrap();
    tokio::time::sleep(IDLE - Duration::from_secs(1)).await;
    assert!(e.loaded());

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!e.loaded());
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);

    e.embed(vec!["a".into()], DOC).await.unwrap();
    assert_eq!(counts.loads.load(Ordering::SeqCst), 2);
    tokio::time::sleep(IDLE + Duration::from_secs(1)).await;
    assert_eq!(counts.drops.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn concurrent_first_requests_keep_one_engine() {
    let counts = Arc::new(Counts::default());
    let e = Arc::new(embedder(&counts));
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let e = e.clone();
            tokio::spawn(async move { e.embed(vec!["a".into()], DOC).await.unwrap() })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1)).await;
    // Losers of the race were dropped; one engine stays.
    assert_eq!(
        counts.loads.load(Ordering::SeqCst) - counts.drops.load(Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn a_failed_load_is_reported_and_retried() {
    let tries = Arc::new(AtomicUsize::new(0));
    let t = tries.clone();
    let e: LazyEmbedder<Fake> = LazyEmbedder::new(
        ModelId::new("fake", "1"),
        1,
        IDLE,
        Box::new(move |_, _| {
            let n = t.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if n == 0 {
                    return Err(DomainError::Provider("offline".into()));
                }
                Ok(vec![Fake(Arc::default())])
            })
        }),
    );
    assert!(e.embed(vec!["a".into()], DOC).await.is_err());
    assert!(!e.loaded());
    assert!(e.embed(vec!["a".into()], DOC).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn changing_the_shape_rebuilds_the_engines_and_texts_are_shared_out() {
    let counts = Arc::new(Counts::default());
    let e = embedder(&counts);
    e.embed(vec!["a".into()], DOC).await.unwrap();
    e.set_parallelism(1, 1);
    e.embed(vec!["a".into()], DOC).await.unwrap();
    assert_eq!(counts.loads.load(Ordering::SeqCst), 1, "same shape: kept");

    e.set_parallelism(3, 2);
    assert!(!e.loaded());
    let texts: Vec<String> = ["a", "bb", "ccc", "dddd", "eeeee"].map(Into::into).into();
    let out = e.embed(texts, DOC).await.unwrap();
    assert_eq!(counts.loads.load(Ordering::SeqCst), 4);
    let lens: Vec<f32> = out.into_iter().map(|v| v[0]).collect();
    assert_eq!(lens, [1.0, 2.0, 3.0, 4.0, 5.0], "in order across workers");
}

#[test]
fn checksums_decide_what_a_file_is() {
    // sha256("abc")
    let abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    assert!(onnx::matches(b"abc", abc));
    assert!(!onnx::matches(b"abd", abc));
}

/// The pipeline of every catalog model: loads (downloading it on first run,
/// kept in `MC_EMBED_TEST_DIR` if set), gives unit vectors of the catalogued
/// size, and puts translations closer than unrelated mail.
/// `cargo test -p mc-infrastructure --release real_model -- --ignored --nocapture`
#[tokio::test]
#[ignore = "downloads the real models"]
async fn real_models_embed_italian_and_english_by_meaning() {
    let dir = test_dir();
    let texts = [
        "Offerte della settimana: sconti fino al 50% su tutta la collezione",
        "Weekly deals: up to 50% off the whole collection",
        "Il tuo ordine è stato spedito, tracking del corriere",
        "Riunione di progetto spostata a giovedì alle 15",
        "Project meeting moved to Thursday at 3pm",
    ];
    for spec in &CATALOG {
        let e = onnx::onnx_embedder(spec, dir.clone(), IDLE);
        e.set_parallelism(1, 2);
        let started = std::time::Instant::now();
        let v = e
            .embed(texts.iter().map(|t| t.to_string()).collect(), DOC)
            .await
            .unwrap();
        let first = started.elapsed();
        let dot = |a: usize, b: usize| v[a].iter().zip(&v[b]).map(|(x, y)| x * y).sum::<f32>();
        assert_eq!(v[0].len(), spec.dims);
        assert_eq!(e.dims(), spec.dims);
        assert!((dot(0, 0) - 1.0).abs() < 1e-3, "unit length");
        println!(
            "{}: dims {} first batch {:.1}s  it/en deals {:.3}  it/en meeting {:.3}  deals/meeting {:.3}  deals/order {:.3}",
            spec.id,
            v[0].len(),
            first.as_secs_f64(),
            dot(0, 1),
            dot(3, 4),
            dot(0, 3),
            dot(0, 2)
        );
        assert!(
            dot(0, 1) > dot(0, 3),
            "{}: translations are closer than unrelated mail",
            spec.id
        );
        println!(
            "{}: meeting it/en {:.3} vs meeting/order {:.3}",
            spec.id,
            dot(3, 4),
            dot(3, 2)
        );
    }
}

fn test_dir() -> std::path::PathBuf {
    std::env::var_os("MC_EMBED_TEST_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("mc-embed-test"))
}

#[test]
fn the_catalog_is_complete_and_unambiguous() {
    let mut ids: Vec<_> = CATALOG.iter().map(ModelSpec::model_id).collect();
    ids.sort_by_key(ToString::to_string);
    ids.dedup();
    assert_eq!(ids.len(), CATALOG.len(), "unique ids");
    let profiles: Vec<_> = CATALOG.iter().map(|s| s.profile).collect();
    assert_eq!(
        profiles,
        [
            ModelProfile::Light,
            ModelProfile::Balanced,
            ModelProfile::Quality
        ]
    );
    for s in &CATALOG {
        assert_eq!(s.revision.len(), 40, "{}: pinned to a commit", s.id);
        assert!(s.dims > 0 && s.max_tokens > 0 && s.worker_bytes > 0);
        assert!((1..=3).contains(&s.quality) && !s.languages.is_empty());
        assert_eq!(s.files[0].0, "onnx/model_quantized.onnx");
        for (name, sha256, bytes) in s.files {
            assert!(
                sha256.len() == 64 && sha256.bytes().all(|b| b.is_ascii_hexdigit()),
                "{}: {name} has a SHA-256",
                s.id
            );
            assert!(bytes > 0, "{}: {name} has a size", s.id);
        }
        assert!(s.download_bytes() > s.files[0].2);
    }
    assert!(
        CATALOG.windows(2).all(|w| {
            w[0].download_bytes() < w[1].download_bytes() && w[0].quality < w[1].quality
        }),
        "profiles grow in size and quality"
    );
}

#[test]
fn each_model_prefixes_texts_by_purpose() {
    for s in &CATALOG {
        assert_eq!(s.prefix(EmbedPurpose::Document), "passage: ", "{}", s.id);
        assert_eq!(s.prefix(EmbedPurpose::Query), "query: ", "{}", s.id);
    }
}

#[test]
fn the_first_model_id_differs_from_phase_two_so_old_vectors_are_embedded_again() {
    // Phase 2 stored vectors made with "query: " under this name.
    let phase_two = "multilingual-e5-small-q@761b726";
    let light = ModelSpec::for_profile(ModelProfile::Light).model_id();
    assert_ne!(light.to_string(), phase_two);
}

#[test]
fn switching_profile_changes_the_model_id_and_only_available_targets_are_taken() {
    let e = CatalogEmbedder::new(std::env::temp_dir().join("mc-never-used"), IDLE);
    let light = e.model();
    assert_eq!(e.dims(), 384);
    e.select(ModelProfile::Balanced, "cpu").unwrap();
    assert_ne!(e.model(), light);
    assert_eq!(e.model().id, "multilingual-e5-base");
    assert_eq!(e.dims(), 768);
    e.select(ModelProfile::Light, "cpu").unwrap();
    assert_eq!(
        e.model(),
        light,
        "back to the same id, so its vectors count again"
    );

    for target in [
        "gpu-cuda",
        "gpu-rocm",
        "gpu-directml",
        "gpu-coreml",
        "npu-vitisai",
    ] {
        let err = e.select(ModelProfile::Quality, target).unwrap_err();
        assert!(err.to_string().contains("non disponibile"), "{err}");
    }
    assert_eq!(e.model(), light, "a refused target changes nothing");
    assert!(e.select(ModelProfile::Light, "tpu").is_err());

    let targets = e.targets();
    assert_eq!(targets.len(), 6);
    assert_eq!(
        targets
            .iter()
            .filter(|t| t.available)
            .map(|t| t.id.as_str())
            .collect::<Vec<_>>(),
        ["cpu"]
    );
    assert_eq!(e.options().len(), CATALOG.len());
}

/// Threads of this process whose nice value is the background one.
#[cfg(target_os = "linux")]
fn niced_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("stat")).ok())
        // Fields after the parenthesised name; nice is the 19th overall.
        .filter(|stat| {
            stat.rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().nth(16).map(|n| n == "10"))
                .unwrap_or(false)
        })
        .count()
}

/// Msgs/s of workers x threads on this machine, memory per worker, and that
/// ONNX Runtime's own threads inherited the background priority. Needs the
/// model in `MC_EMBED_TEST_DIR`; `MC_BENCH_PROFILE` picks it (default light),
/// `MC_BENCH=workers,threads,batch` a single shape.
/// `MC_EMBED_TEST_DIR=... cargo test -p mc-infrastructure --release bench_parallelism -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the real model"]
async fn bench_parallelism() {
    let dir = std::path::PathBuf::from(std::env::var_os("MC_EMBED_TEST_DIR").unwrap());
    let profile =
        std::env::var("MC_BENCH_PROFILE").map_or(ModelProfile::Light, |p| p.parse().unwrap());
    let text = |i: usize| {
        format!(
            "Messaggio {i}. {}",
            "Offerte della settimana per i nostri clienti, sconti fino al 50% su tutta la collezione \
             e spedizione gratuita; riunione di progetto spostata a giovedì, ordine {i} spedito. "
                .repeat(8)
        )
    };
    let shapes = std::env::var("MC_BENCH").map_or_else(
        |_| {
            vec![
                (1, 1, 8),
                (1, 2, 16),
                (1, 4, 32),
                (2, 2, 32),
                (4, 2, 32),
                (8, 2, 32),
            ]
        },
        |v| {
            let n: Vec<usize> = v.split(',').map(|n| n.parse().unwrap()).collect();
            vec![(n[0], n[1], n[2])]
        },
    );
    for (workers, threads, batch) in shapes {
        let e = onnx::onnx_embedder(ModelSpec::for_profile(profile), dir.clone(), IDLE);
        e.set_parallelism(workers, threads);
        let loading = std::time::Instant::now();
        e.embed((0..batch).map(text).collect(), DOC).await.unwrap();
        let loaded_in = loading.elapsed();
        #[cfg(target_os = "linux")]
        let niced = niced_threads();
        #[cfg(not(target_os = "linux"))]
        let niced = 0;
        let batches = 4;
        let started = std::time::Instant::now();
        for b in 0..batches {
            e.embed((0..batch).map(|i| text(b * batch + i)).collect(), DOC)
                .await
                .unwrap();
        }
        let resident = mc_shared::memory::resident_bytes().unwrap_or_default();
        let rate = (batches * batch) as f64 / started.elapsed().as_secs_f64();
        println!(
            "{workers}x{threads} batch {batch}: {rate:.1} msg/s (first batch with loading {:.1}s), {} MB per worker, {niced} niced threads, process RSS {} MB",
            loaded_in.as_secs_f64(),
            e.worker_bytes().unwrap_or_default() / 1_000_000,
            resident / 1_000_000
        );
        // The idle watcher would keep the engines alive otherwise.
        e.set_parallelism(workers + 1, threads);
        #[cfg(target_os = "linux")]
        assert!(
            niced >= workers * (threads - 1),
            "ONNX pool threads inherit the nice value"
        );
    }
}
