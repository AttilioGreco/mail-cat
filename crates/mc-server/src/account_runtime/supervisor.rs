//! Keeps an account's long-running tasks alive. A panic in a spawned task
//! kills only that task, which would leave the account looking active while it
//! no longer syncs, backfills or watches; so each is rerun after a backoff, and
//! given up on if it keeps crashing.

use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const BACKOFF_FLOOR: Duration = Duration::from_secs(5);
const BACKOFF_CAP: Duration = Duration::from_secs(5 * 60);
/// A run this long was not a crash loop: the next panic starts over.
const STABLE_AFTER: Duration = Duration::from_secs(10 * 60);
/// Consecutive panics without a stable run before the task is abandoned.
const MAX_PANICS: u32 = 5;

/// Runs `make()` on `tasks` and reruns it after each panic. A task that
/// returns is done: only a panic restarts it. `on_failed` gets the reason when
/// the task is abandoned. Cancelling `stop` ends the supervisor, also while it
/// waits to rerun; the task itself is expected to watch `stop` too.
pub(super) fn supervise<F, Fut>(
    (what, account): (&'static str, String),
    stop: CancellationToken,
    tasks: &TaskTracker,
    on_failed: impl FnOnce(String) + Send + 'static,
    mut make: F,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let spawner = tasks.clone();
    tasks.spawn(async move {
        let mut panics = 0;
        let mut backoff = BACKOFF_FLOOR;
        loop {
            let started = Instant::now();
            let Err(crashed) = spawner.spawn(make()).await else {
                return;
            };
            if !crashed.is_panic() || stop.is_cancelled() {
                return;
            }
            if started.elapsed() >= STABLE_AFTER {
                panics = 0;
                backoff = BACKOFF_FLOOR;
            }
            panics += 1;
            if panics >= MAX_PANICS {
                tracing::error!(
                    "{what} of '{account}' panicked {panics} times in a row: giving up on it until the account is restarted"
                );
                on_failed(format!(
                    "{what} stopped after {panics} crashes; restart the account"
                ));
                return;
            }
            tracing::error!(
                "{what} of '{account}' panicked ({panics}/{MAX_PANICS}): restarting in {backoff:?}"
            );
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(BACKOFF_CAP);
        }
    });
}

#[cfg(test)]
mod tests;
