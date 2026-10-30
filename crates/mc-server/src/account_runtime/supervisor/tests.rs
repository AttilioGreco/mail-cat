use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::oneshot;

/// Runs that panic for the first `panicking` times, then wait for `stop`.
fn flaky(
    panicking: u32,
    stop: &CancellationToken,
) -> (
    Arc<AtomicU32>,
    impl FnMut() -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + use<>,
) {
    let runs = Arc::new(AtomicU32::new(0));
    let (counter, stop) = (runs.clone(), stop.clone());
    let make = move || {
        let run = counter.fetch_add(1, Ordering::SeqCst);
        let stop = stop.clone();
        Box::pin(async move {
            if run < panicking {
                panic!("boom");
            }
            stop.cancelled().await;
        }) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
    };
    (runs, make)
}

fn failure_channel() -> (
    impl FnOnce(String) + Send + 'static,
    oneshot::Receiver<String>,
) {
    let (tx, rx) = oneshot::channel();
    (move |reason| drop(tx.send(reason)), rx)
}

#[tokio::test(start_paused = true)]
async fn a_panicked_task_is_rerun_after_the_backoff() {
    let (stop, tasks) = (CancellationToken::new(), TaskTracker::new());
    let (runs, make) = flaky(1, &stop);
    let (on_failed, _rx) = failure_channel();
    supervise(
        ("sync loop", "a".into()),
        stop.clone(),
        &tasks,
        on_failed,
        make,
    );

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1, "waiting out the backoff");
    tokio::time::sleep(BACKOFF_FLOOR).await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    stop.cancel();
    tasks.close();
    tasks.wait().await;
}

#[tokio::test(start_paused = true)]
async fn a_task_that_keeps_panicking_is_given_up_on_and_reported() {
    let (stop, tasks) = (CancellationToken::new(), TaskTracker::new());
    let (runs, make) = flaky(u32::MAX, &stop);
    let (on_failed, rx) = failure_channel();
    supervise(("watcher", "a".into()), stop, &tasks, on_failed, make);

    let reason = rx.await.unwrap();
    assert!(reason.contains("watcher"), "{reason}");
    assert_eq!(runs.load(Ordering::SeqCst), MAX_PANICS);
    tasks.close();
    tasks.wait().await;
    assert_eq!(runs.load(Ordering::SeqCst), MAX_PANICS, "no further reruns");
}

#[tokio::test(start_paused = true)]
async fn cancelling_during_the_backoff_ends_it_at_once() {
    let (stop, tasks) = (CancellationToken::new(), TaskTracker::new());
    let (runs, make) = flaky(u32::MAX, &stop);
    let (on_failed, _rx) = failure_channel();
    supervise(
        ("sync loop", "a".into()),
        stop.clone(),
        &tasks,
        on_failed,
        make,
    );

    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.cancel();
    tasks.close();
    let before = Instant::now();
    tasks.wait().await;
    assert_eq!(before.elapsed(), Duration::ZERO);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_stable_run_resets_the_crash_count() {
    let (stop, tasks) = (CancellationToken::new(), TaskTracker::new());
    let runs = Arc::new(AtomicU32::new(0));
    let counter = runs.clone();
    // Panics at once, except every run after the 4th, which lives past
    // STABLE_AFTER before panicking: 8 panics in total, never 5 in a row.
    let make = move || {
        let run = counter.fetch_add(1, Ordering::SeqCst);
        async move {
            if run == 4 {
                tokio::time::sleep(STABLE_AFTER).await;
            }
            if run < 8 {
                panic!("boom");
            }
            std::future::pending::<()>().await;
        }
    };
    let (on_failed, mut rx) = failure_channel();
    supervise(
        ("sync loop", "a".into()),
        stop.clone(),
        &tasks,
        on_failed,
        make,
    );

    tokio::time::sleep(Duration::from_secs(3 * 60 * 60)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 9);
    assert!(rx.try_recv().is_err(), "never gave up");
    stop.cancel();
}

#[tokio::test(start_paused = true)]
async fn a_task_that_returns_is_not_rerun() {
    let (stop, tasks) = (CancellationToken::new(), TaskTracker::new());
    let runs = Arc::new(AtomicU32::new(0));
    let counter = runs.clone();
    let (on_failed, _rx) = failure_channel();
    supervise(
        ("sync loop", "a".into()),
        stop,
        &tasks,
        on_failed,
        move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async {}
        },
    );
    tasks.close();
    tasks.wait().await;
    tokio::time::sleep(Duration::from_secs(600)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
