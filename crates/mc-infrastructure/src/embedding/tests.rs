use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mc_domain::{DomainError, Embedder};

use super::*;

#[derive(Default)]
struct Counts {
    loads: AtomicUsize,
    drops: AtomicUsize,
}

struct Fake(Arc<Counts>);

impl Engine for Fake {
    fn embed(&mut self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError> {
        Ok(texts.iter().map(|t| vec![t.len() as f32]).collect())
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

const IDLE: Duration = Duration::from_secs(300);

fn embedder(counts: &Arc<Counts>) -> LazyEmbedder<Fake> {
    let counts = counts.clone();
    LazyEmbedder::new(
        "fake@1",
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

    assert_eq!(e.embed(vec!["abc".into()]).await.unwrap(), vec![vec![3.0]]);
    e.embed(vec!["x".into()]).await.unwrap();
    assert_eq!(counts.loads.load(Ordering::SeqCst), 1);
    assert!(e.loaded());
    assert_eq!(e.model(), "fake@1");
}

#[tokio::test(start_paused = true)]
async fn the_engine_is_dropped_after_idle_and_loaded_again_on_demand() {
    let counts = Arc::new(Counts::default());
    let e = embedder(&counts);
    e.embed(vec!["a".into()]).await.unwrap();

    tokio::time::sleep(IDLE - Duration::from_secs(1)).await;
    assert!(e.loaded(), "not yet idle long enough");
    // Using it restarts the clock.
    e.embed(vec!["a".into()]).await.unwrap();
    tokio::time::sleep(IDLE - Duration::from_secs(1)).await;
    assert!(e.loaded());

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!e.loaded());
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);

    e.embed(vec!["a".into()]).await.unwrap();
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
            tokio::spawn(async move { e.embed(vec!["a".into()]).await.unwrap() })
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
        "fake@1",
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
    assert!(e.embed(vec!["a".into()]).await.is_err());
    assert!(!e.loaded());
    assert!(e.embed(vec!["a".into()]).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn changing_the_shape_rebuilds_the_engines_and_texts_are_shared_out() {
    let counts = Arc::new(Counts::default());
    let e = embedder(&counts);
    e.embed(vec!["a".into()]).await.unwrap();
    e.set_parallelism(1, 1);
    e.embed(vec!["a".into()]).await.unwrap();
    assert_eq!(counts.loads.load(Ordering::SeqCst), 1, "same shape: kept");

    e.set_parallelism(3, 2);
    assert!(!e.loaded());
    let texts: Vec<String> = ["a", "bb", "ccc", "dddd", "eeeee"].map(Into::into).into();
    let out = e.embed(texts).await.unwrap();
    assert_eq!(counts.loads.load(Ordering::SeqCst), 4);
    let lens: Vec<f32> = out.into_iter().map(|v| v[0]).collect();
    assert_eq!(lens, [1.0, 2.0, 3.0, 4.0, 5.0], "in order across workers");
}

#[test]
fn checksums_decide_what_a_file_is() {
    // sha256("abc")
    let abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    assert!(e5::matches(b"abc", abc));
    assert!(!e5::matches(b"abd", abc));
}

/// Downloads ~120 MB on first run (kept in `MC_EMBED_TEST_DIR` if set).
/// `cargo test -p mc-infrastructure real_model -- --ignored --nocapture`
#[tokio::test]
#[ignore = "downloads the real model"]
async fn real_model_embeds_italian_and_english_by_meaning() {
    let dir = std::env::var_os("MC_EMBED_TEST_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("mc-embed-test"));
    let e = multilingual_e5_small(dir, IDLE);
    let texts = [
        "Offerte della settimana: sconti fino al 50% su tutta la collezione",
        "Weekly deals: up to 50% off the whole collection",
        "Il tuo ordine è stato spedito, tracking del corriere",
        "Riunione di progetto spostata a giovedì alle 15",
        "Project meeting moved to Thursday at 3pm",
    ];
    let v = e
        .embed(texts.iter().map(|t| t.to_string()).collect())
        .await
        .unwrap();
    let dot = |a: usize, b: usize| v[a].iter().zip(&v[b]).map(|(x, y)| x * y).sum::<f32>();
    assert_eq!(v[0].len(), 384);
    assert!((dot(0, 0) - 1.0).abs() < 1e-3, "unit length");
    println!(
        "it/en deals {:.3}  it/en meeting {:.3}  deals/meeting {:.3}  deals/order {:.3}",
        dot(0, 1),
        dot(3, 4),
        dot(0, 3),
        dot(0, 2)
    );
    assert!(
        dot(0, 1) > dot(0, 3),
        "translations are closer than unrelated mail"
    );
    assert!(dot(3, 4) > dot(3, 0));
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
/// model in `MC_EMBED_TEST_DIR`.
/// `MC_EMBED_TEST_DIR=... cargo test -p mc-infrastructure --release bench_parallelism -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the real model"]
async fn bench_parallelism() {
    let dir = std::path::PathBuf::from(std::env::var_os("MC_EMBED_TEST_DIR").unwrap());
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
        let e = multilingual_e5_small(dir.clone(), IDLE);
        e.set_parallelism(workers, threads);
        let loading = std::time::Instant::now();
        e.embed((0..batch).map(text).collect()).await.unwrap();
        let loaded_in = loading.elapsed();
        #[cfg(target_os = "linux")]
        let niced = niced_threads();
        #[cfg(not(target_os = "linux"))]
        let niced = 0;
        let batches = 4;
        let started = std::time::Instant::now();
        for b in 0..batches {
            e.embed((0..batch).map(|i| text(b * batch + i)).collect())
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
