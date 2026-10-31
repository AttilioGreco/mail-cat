//! What the background work is doing, for the user who tuned its speed:
//! counted in memory as it happens, nothing stored.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use mc_domain::{
    BackgroundProfile, BackgroundSpeed, MailboxReadRepository, ModelId, Scope, TaggingStore,
};

use super::TaggingService;
use super::discovery::{DiscoveryPhase, DiscoveryProgress};
use crate::bus::CommandBus;
use crate::error::ApplicationError;

/// Throughput is averaged over this long.
const WINDOW: Duration = Duration::from_secs(60);
/// Counting the mail is a query over all of it: not on every poll.
const COUNTS_TTL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedderState {
    /// No consent, or no model in this build.
    Off,
    /// A batch is waiting for the model to be read into memory.
    Loading,
    Active,
    /// A scope is being retrained; the backlog gives way.
    PausedForRetrain,
    /// Nothing left to embed.
    Idle,
}

#[derive(Debug, Clone)]
pub struct RetrainProgress {
    pub scope: String,
    pub done: u64,
    pub total: u64,
}

#[derive(Debug, Clone)]
pub struct BackgroundStats {
    pub speed: BackgroundSpeed,
    pub model: Option<ModelId>,
    pub profile: BackgroundProfile,
    pub state: EmbedderState,
    pub model_loaded: bool,
    pub embedded: u64,
    pub eligible: u64,
    pub per_second: f64,
    pub eta: Option<Duration>,
    pub worker_bytes: Option<u64>,
    pub retrains: Vec<RetrainProgress>,
    pub discovery: Option<DiscoveryProgress>,
}

#[derive(Default)]
pub(super) struct Activity {
    /// (when, messages) per stored batch, newest last.
    batches: VecDeque<(Instant, u32)>,
    pub retrains: HashMap<Scope, (u64, u64)>,
    /// Inside a call to the embedder.
    pub embedding: bool,
    /// The group being searched for labels, how far along: (scope, phase, done, total).
    pub discovery: Option<(Option<Scope>, DiscoveryPhase, u64, u64)>,
    counts: Option<(Instant, (u64, u64))>,
}

impl Activity {
    pub fn record(&mut self, now: Instant, messages: u32) {
        self.batches.push_back((now, messages));
        self.expire(now);
    }

    fn expire(&mut self, now: Instant) {
        while self
            .batches
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > WINDOW)
        {
            self.batches.pop_front();
        }
    }

    /// Messages per second over the window. The first batch only marks when
    /// counting began, as how long it took is not known; idling lowers the rate.
    pub fn per_second(&self, now: Instant) -> f64 {
        let Some((start, _)) = self.batches.front().filter(|_| self.batches.len() > 1) else {
            return 0.0;
        };
        let counted: u32 = self.batches.iter().skip(1).map(|(_, n)| n).sum();
        let elapsed = now.saturating_duration_since(*start).as_secs_f64();
        if elapsed > 0.0 {
            f64::from(counted) / elapsed
        } else {
            0.0
        }
    }
}

pub(super) fn eta(remaining: u64, per_second: f64) -> Option<Duration> {
    (remaining > 0 && per_second > 0.0)
        .then(|| Duration::from_secs_f64(remaining as f64 / per_second))
}

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    pub async fn background_stats(&self) -> Result<BackgroundStats, ApplicationError> {
        let (embedded, eligible) = self.embedding_counts().await?;
        let now = Instant::now();
        let (per_second, embedding, retrains) = {
            let mut activity = self.lock_activity();
            activity.expire(now);
            (
                activity.per_second(now),
                activity.embedding,
                activity.retrains.clone(),
            )
        };
        let model_loaded = self.embedder.as_ref().is_some_and(|e| e.loaded());
        let remaining = eligible.saturating_sub(embedded);
        let state = if !self.embeddings_enabled() {
            EmbedderState::Off
        } else if remaining == 0 {
            EmbedderState::Idle
        } else if self.retraining.load(Ordering::Relaxed) > 0 {
            EmbedderState::PausedForRetrain
        } else if embedding && !model_loaded {
            EmbedderState::Loading
        } else {
            EmbedderState::Active
        };
        let names = self.scope_names().await?;
        let retrains = retrains
            .into_iter()
            .map(|(scope, (done, total))| RetrainProgress {
                scope: names
                    .get(&scope)
                    .cloned()
                    .unwrap_or_else(|| "Account senza gruppo".into()),
                done,
                total,
            })
            .collect();
        Ok(BackgroundStats {
            speed: self.background_speed(),
            model: self.embedding_model(),
            profile: self.profile(),
            state,
            model_loaded,
            embedded,
            eligible,
            per_second,
            eta: eta(remaining, per_second),
            worker_bytes: self.embedder.as_ref().and_then(|e| e.worker_bytes()),
            retrains,
            discovery: self.discovery_progress().await?,
        })
    }

    pub(super) async fn embedding_counts(&self) -> Result<(u64, u64), ApplicationError> {
        let Some(embedder) = self.embedder.as_ref() else {
            return Ok((0, 0));
        };
        if let Some((at, counts)) = self.lock_activity().counts
            && at.elapsed() < COUNTS_TTL
        {
            return Ok(counts);
        }
        let counts = self
            .store
            .embedding_counts(&embedder.model().to_string())
            .await?;
        self.lock_activity().counts = Some((Instant::now(), counts));
        Ok(counts)
    }

    pub(super) fn lock_activity(&self) -> std::sync::MutexGuard<'_, Activity> {
        self.activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throughput_counts_batches_after_the_first_over_the_elapsed_time() {
        let t0 = Instant::now();
        let at = |s| t0 + Duration::from_secs(s);
        let mut a = Activity::default();
        assert_eq!(a.per_second(at(1)), 0.0, "nothing yet");
        a.record(at(0), 16);
        assert_eq!(a.per_second(at(1)), 0.0, "one batch has no duration");
        a.record(at(2), 16);
        a.record(at(4), 16);
        assert_eq!(a.per_second(at(4)), 8.0);
        assert_eq!(a.per_second(at(8)), 4.0, "idling lowers it");
    }

    #[test]
    fn old_batches_leave_the_window() {
        let t0 = Instant::now();
        let mut a = Activity::default();
        a.record(t0, 100);
        a.record(t0 + Duration::from_secs(1), 100);
        a.record(t0 + Duration::from_secs(100), 10);
        a.record(t0 + Duration::from_secs(110), 10);
        assert_eq!(a.per_second(t0 + Duration::from_secs(110)), 1.0);
    }

    #[test]
    fn eta_needs_work_left_and_a_rate() {
        assert_eq!(eta(100, 4.0), Some(Duration::from_secs(25)));
        assert_eq!(eta(0, 4.0), None);
        assert_eq!(eta(100, 0.0), None);
    }
}
