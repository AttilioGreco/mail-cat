//! Label suggestion and auto-apply: who learns together, how each label is
//! treated, and what a message offers the classifier.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::account::AccountId;
use crate::model::mailbox::MailboxId;
use crate::model::message::ListHeaders;
use crate::value_objects::message_id::MessageId;

/// A set of accounts whose labels are learned together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(Uuid);

impl GroupId {
    /// The group every account is in until the user says otherwise ("Tutti").
    pub const DEFAULT: Self = Self(Uuid::from_u128(1));

    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for GroupId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingGroup {
    pub id: GroupId,
    pub name: String,
    pub accounts: Vec<AccountId>,
}

/// Whose mail one model learns from: a group, or an account in no group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    Group(GroupId),
    Alone(AccountId),
}

impl Scope {
    /// The key under which the store files the scope's rows.
    pub fn key(&self) -> String {
        match self {
            Self::Group(g) => format!("g:{}", g.as_uuid()),
            Self::Alone(a) => format!("a:{}", a.as_uuid()),
        }
    }

    pub fn from_key(key: &str) -> Option<Self> {
        let (kind, id) = key.split_once(':')?;
        let id = Uuid::parse_str(id).ok()?;
        match kind {
            "g" => Some(Self::Group(GroupId(id))),
            "a" => Some(Self::Alone(AccountId::from_uuid(id))),
            _ => None,
        }
    }
}

/// What may happen to a message the classifier is sure about.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelMode {
    /// The default: nothing is offered, nothing is applied.
    #[default]
    Off,
    Suggest,
    Apply,
    /// Apply, and take the message out of the Inbox.
    ApplyArchive,
}

impl LabelMode {
    pub fn applies(self) -> bool {
        matches!(self, Self::Apply | Self::ApplyArchive)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Suggest => "suggest",
            Self::Apply => "apply",
            Self::ApplyArchive => "apply_archive",
        }
    }
}

impl FromStr for LabelMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Ok(match s {
            "off" => Self::Off,
            "suggest" => Self::Suggest,
            "apply" => Self::Apply,
            "apply_archive" => Self::ApplyArchive,
            other => return Err(format!("unknown label mode {other:?}")),
        })
    }
}

/// How hard background learning and embedding may work. They always run at
/// low OS priority; this only sets how much of the machine they may fill.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackgroundSpeed {
    Light,
    #[default]
    Normal,
    Max,
}

/// What a `BackgroundSpeed` comes to on a machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackgroundProfile {
    /// Copies of the model embedding side by side.
    pub workers: usize,
    /// Inference threads of each.
    pub threads: usize,
    /// Messages embedded per batch.
    pub batch: u32,
    /// Rest between batches (and between pages of a retrain).
    pub pause: Duration,
}

/// Each worker holds its own copy of the model: about 600 MB resident.
const MAX_WORKERS: usize = 4;

impl BackgroundSpeed {
    pub fn profile(self, cores: usize) -> BackgroundProfile {
        let (workers, threads, batch, pause) = match self {
            Self::Light => (1, 1, 8, Duration::from_secs(3)),
            Self::Normal => (
                (cores / 8).clamp(1, MAX_WORKERS),
                2,
                16,
                Duration::from_millis(500),
            ),
            Self::Max => ((cores / 4).clamp(1, MAX_WORKERS), 2, 32, Duration::ZERO),
        };
        BackgroundProfile {
            workers,
            threads,
            batch,
            pause,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Normal => "normal",
            Self::Max => "max",
        }
    }
}

impl FromStr for BackgroundSpeed {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Ok(match s {
            "light" => Self::Light,
            "normal" => Self::Normal,
            "max" => Self::Max,
            other => return Err(format!("unknown background speed {other:?}")),
        })
    }
}

/// How the group treats a label. `label` is the lowercased label name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabelSettings {
    pub label: String,
    pub mode: LabelMode,
    /// Confidence from which `Apply` modes act.
    pub threshold: f64,
    /// A palette key (see [`LabelSettings::is_color`]). Kept here only: mail servers have no label
    /// colors. Saving the other settings never changes it.
    pub color: Option<String>,
}

const HUES: [&str; 12] = [
    "red", "orange", "amber", "yellow", "lime", "green", "teal", "cyan", "blue", "indigo",
    "purple", "pink",
];

impl LabelSettings {
    /// Whether `key` is `hue` or `hue-soft` for one of the twelve hues.
    pub fn is_color(key: &str) -> bool {
        let hue = key.strip_suffix("-soft").unwrap_or(key);
        HUES.contains(&hue)
    }
}

impl LabelSettings {
    pub const DEFAULT_THRESHOLD: f64 = 0.9;
    /// A label cannot leave `Off` with fewer positive examples.
    pub const MIN_POSITIVES: u32 = 5;

    pub fn off(label: &str) -> Self {
        Self {
            label: label.to_owned(),
            mode: LabelMode::Off,
            threshold: Self::DEFAULT_THRESHOLD,
            color: None,
        }
    }
}

/// How sure the classifier is, as shown to people. The score behind it is a
/// heuristic blend, not a probability, so it is never shown as a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Confidence {
    Medium,
    High,
}

impl Confidence {
    pub const MEDIUM_FROM: f64 = 0.6;
    pub const HIGH_FROM: f64 = 0.9;

    pub fn of(score: f64) -> Option<Self> {
        if score >= Self::HIGH_FROM {
            Some(Self::High)
        } else if score >= Self::MEDIUM_FROM {
            Some(Self::Medium)
        } else {
            None
        }
    }
}

/// The classifier's idea that a message belongs under a label.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Score {
    /// The lowercased label name.
    pub label: String,
    pub score: f64,
}

/// A message's text, as the classifier reads it.
#[derive(Debug, Clone)]
pub struct TagSource {
    pub id: MessageId,
    pub account_id: AccountId,
    pub from_email: String,
    pub from_name: Option<String>,
    pub subject: String,
    pub list_headers: ListHeaders,
    /// `None` until the body is downloaded.
    pub body: Option<String>,
    /// Lowercased names of the labels it carries: custom mailboxes on a
    /// labels account, keywords (tags) on a folders account.
    pub labels: Vec<String>,
    /// The account has folders, so its labels are keywords.
    pub keywords: bool,
    /// The server is known to keep custom keywords; without it a keyword
    /// store may be silently dropped.
    pub tags: bool,
    /// The one folder of a folders account's message.
    pub folder: Option<MailboxId>,
    pub in_inbox: bool,
    /// How many mailboxes hold it (labels accounts): taking its last one away
    /// would leave it nowhere.
    pub mailboxes: u32,
    /// The keywords as the server holds them (a folders account), whatever
    /// their case: removing a label has to name each spelling.
    pub spellings: Vec<String>,
    pub received_at: chrono::DateTime<chrono::Utc>,
}

impl TagSource {
    /// A label can be put on it: not a keyword the account cannot keep.
    pub fn can_label(&self) -> bool {
        !self.keywords || self.tags
    }
}

/// Which stored messages a page of sources walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFilter {
    /// Carrying at least one label.
    Labelled,
    /// Carrying none.
    Unlabelled,
    Any,
    /// `Labelled`, also in the Trash, Spam and Drafts that training skips:
    /// changing a label has to reach every message that carries it.
    Held,
}

/// What the model was last taught about a message, kept so the lesson can be
/// withdrawn exactly when the message changes (a label moved, a body arrived).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Example {
    pub message_id: MessageId,
    pub account_id: AccountId,
    pub scope: String,
    pub positives: Vec<String>,
    /// Rejected suggestions and undone auto-applies.
    pub negatives: Vec<String>,
    pub with_body: bool,
    pub weight: f64,
}

/// A change to the token counts of one scope.
#[derive(Debug, Clone)]
pub struct Lesson<'a> {
    pub tokens: &'a [String],
    pub positives: &'a [String],
    pub negatives: &'a [String],
    /// Negative to withdraw a lesson.
    pub weight: f64,
}

/// Counts the scorer needs for the tokens of one message.
#[derive(Debug, Default)]
pub struct ModelStats {
    /// Weighted documents seen in the scope.
    pub documents: f64,
    /// Per token, weighted documents containing it.
    pub corpus: HashMap<String, f64>,
    pub labels: HashMap<String, LabelStats>,
}

#[derive(Debug, Default, Clone)]
pub struct LabelStats {
    pub positive_docs: f64,
    pub negative_docs: f64,
    /// Per token: (positive, explicit negative) weighted documents.
    pub tokens: HashMap<String, (f64, f64)>,
}

/// What a label was taught from.
#[derive(Debug, Clone, Default)]
pub struct LabelExamples {
    /// Newest first.
    pub recent: Vec<MessageId>,
    pub per_account: Vec<(AccountId, u32)>,
}

/// A trained message's embedding with what the model was taught about it.
#[derive(Debug, Clone)]
pub struct LabelledVector {
    pub message_id: MessageId,
    pub vector: Vec<f32>,
    pub positives: Vec<String>,
    /// Rejected suggestions and undone auto-applies.
    pub negatives: Vec<String>,
}

/// A label put on a message by the classifier, kept so it can be shown as
/// automatic and undone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AutoApplied {
    pub message_id: MessageId,
    pub label: String,
    pub score: f64,
    pub archived: bool,
    /// Applied on the user's say-so (an accepted discovery): the model learns
    /// it as the user's own label, not as something it did by itself.
    pub confirmed: bool,
    /// The accepted proposal that put it, so the acceptance can be undone.
    pub proposal_id: Option<crate::model::discovery::ProposalId>,
    pub at: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_speed_comes_to_threads_batch_and_pause() {
        let p = |speed: BackgroundSpeed, cores| {
            let p = speed.profile(cores);
            (p.workers, p.threads, p.batch, p.pause)
        };
        assert_eq!(
            p(BackgroundSpeed::Light, 16),
            (1, 1, 8, Duration::from_secs(3))
        );
        assert_eq!(
            p(BackgroundSpeed::Normal, 16),
            (2, 2, 16, Duration::from_millis(500))
        );
        assert_eq!(
            p(BackgroundSpeed::Normal, 1),
            (1, 2, 16, Duration::from_millis(500))
        );
        assert_eq!(p(BackgroundSpeed::Max, 16), (4, 2, 32, Duration::ZERO));
        assert_eq!(BackgroundSpeed::default(), BackgroundSpeed::Normal);
    }

    #[test]
    fn speeds_survive_their_name() {
        for s in [
            BackgroundSpeed::Light,
            BackgroundSpeed::Normal,
            BackgroundSpeed::Max,
        ] {
            assert_eq!(s.as_str().parse(), Ok(s));
        }
        assert!("fast".parse::<BackgroundSpeed>().is_err());
    }
}
