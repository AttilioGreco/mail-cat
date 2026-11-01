//! Label suggestion: training groups, per-label settings, what the classifier
//! offers on a message, and applying a label to existing mail.

use crate::client::MailCatClient;
use crate::error::Result;
use crate::proto::mail_cat::v1::{self as pb, tagging_service_client::TaggingServiceClient};

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrainingGroup {
    pub id: String,
    pub name: String,
    pub account_ids: Vec<String>,
    pub is_default: bool,
}

/// `mode`: `off`, `suggest`, `apply` or `apply_archive`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelStatus {
    pub label: String,
    pub mode: String,
    pub threshold: f64,
    pub positives: u32,
    pub min_positives: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelEntry {
    /// As the scope spells it.
    pub name: String,
    #[serde(flatten)]
    pub status: LabelStatus,
}

/// The labels one model learns. `scope` is `g:<group id>`, or `a:<account id>`
/// for an account that learns alone (then `group` is empty).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelScope {
    pub scope: String,
    pub group: String,
    pub account_ids: Vec<String>,
    pub labels: Vec<LabelEntry>,
}

/// `confidence`: `high` or `medium`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TagSuggestion {
    pub label: String,
    pub mailbox_id: String,
    pub name: String,
    pub confidence: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoLabel {
    pub label: String,
    pub mailbox_id: String,
    pub name: String,
    pub archived: bool,
    pub at: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageTags {
    pub suggestions: Vec<TagSuggestion>,
    pub auto: Vec<AutoLabel>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyPreviewItem {
    pub message_id: String,
    pub subject: String,
    pub from: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyPreview {
    pub to_label: u32,
    pub to_archive: u32,
    pub sample: Vec<ApplyPreviewItem>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvalRow {
    pub threshold: f64,
    pub flagged: u32,
    pub correct: u32,
    pub precision: f64,
    pub recall: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelEval {
    pub label: String,
    pub held_out: u32,
    /// Held-out messages with an embedding.
    pub embedded: u32,
    /// Words only.
    pub bayes_rows: Vec<EvalRow>,
    /// Words and nearest neighbours.
    pub rows: Vec<EvalRow>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmbeddingsStatus {
    pub enabled: bool,
    /// False where the server has no model.
    pub available: bool,
    pub model: String,
    /// "light", "normal" or "max".
    pub speed: String,
    /// "light", "balanced" or "quality".
    pub profile: String,
    pub models: Vec<ModelOption>,
    /// "cpu", "gpu-cuda", ...
    pub target: String,
    pub targets: Vec<TargetOption>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelOption {
    pub profile: String,
    pub model: String,
    pub dims: u32,
    pub download_bytes: u64,
    pub worker_bytes: u64,
    /// 1 (good) to 3 (best).
    pub quality: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetOption {
    pub id: String,
    pub available: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrainProgress {
    pub scope: String,
    pub done: u64,
    pub total: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundStats {
    /// Model in use; empty without one.
    pub model: String,
    /// "light", "normal" or "max".
    pub speed: String,
    pub workers: u32,
    pub threads: u32,
    pub batch: u32,
    pub pause_ms: u32,
    /// "off", "loading", "active", "paused_for_retrain" or "idle".
    pub state: String,
    pub model_loaded: bool,
    pub embedded: u64,
    pub eligible: u64,
    pub per_second: f64,
    /// Zero when unknown.
    pub eta_seconds: u64,
    /// Zero when unknown.
    pub worker_bytes: u64,
    pub retrains: Vec<RetrainProgress>,
    pub discovery: Option<DiscoveryProgress>,
}

/// A search for new labels under way. `phase`: `loading`, `clustering` or
/// `naming`; `total` is zero while the phase has nothing to count.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryProgress {
    pub scope: String,
    pub phase: String,
    pub done: u64,
    pub total: u64,
}

fn group(g: pb::TrainingGroup) -> TrainingGroup {
    TrainingGroup {
        id: g.id,
        name: g.name,
        account_ids: g.account_ids,
        is_default: g.is_default,
    }
}

fn status(s: pb::LabelStatus) -> LabelStatus {
    LabelStatus {
        label: s.label,
        mode: s.mode,
        threshold: s.threshold,
        positives: s.positives,
        min_positives: s.min_positives,
    }
}

fn preview(p: pb::ApplyPreview) -> ApplyPreview {
    ApplyPreview {
        to_label: p.to_label,
        to_archive: p.to_archive,
        sample: p
            .sample
            .into_iter()
            .map(|i| ApplyPreviewItem {
                message_id: i.message_id,
                subject: i.subject,
                from: i.from,
            })
            .collect(),
    }
}

fn apply_request(account_id: &str, label: &str, mode: &str, threshold: f64) -> pb::ApplyRequest {
    pb::ApplyRequest {
        account_id: account_id.into(),
        label: label.into(),
        mode: mode.into(),
        threshold,
    }
}

fn suggestion_ref(message_id: &str, label: &str) -> pb::SuggestionRef {
    pb::SuggestionRef {
        message_id: message_id.into(),
        label: label.into(),
    }
}

impl MailCatClient {
    pub(crate) fn tagging(&self) -> TaggingServiceClient<tonic::transport::Channel> {
        TaggingServiceClient::new(self.channel())
    }

    pub async fn list_training_groups(&self) -> Result<Vec<TrainingGroup>> {
        let resp = self.tagging().list_groups(pb::ListGroupsRequest {}).await?;
        Ok(resp.into_inner().groups.into_iter().map(group).collect())
    }

    pub async fn create_training_group(&self, name: &str) -> Result<TrainingGroup> {
        let resp = self
            .tagging()
            .create_group(pb::CreateGroupRequest { name: name.into() })
            .await?;
        Ok(group(resp.into_inner()))
    }

    pub async fn rename_training_group(&self, id: &str, name: &str) -> Result<()> {
        self.tagging()
            .rename_group(pb::RenameGroupRequest {
                id: id.into(),
                name: name.into(),
            })
            .await?;
        Ok(())
    }

    pub async fn delete_training_group(&self, id: &str) -> Result<()> {
        self.tagging()
            .delete_group(pb::DeleteGroupRequest { id: id.into() })
            .await?;
        Ok(())
    }

    /// `group_id` `None`: the account learns alone.
    pub async fn assign_account_group(
        &self,
        account_id: &str,
        group_id: Option<String>,
    ) -> Result<()> {
        self.tagging()
            .assign_account(pb::AssignAccountRequest {
                account_id: account_id.into(),
                group_id,
            })
            .await?;
        Ok(())
    }

    pub async fn get_label_settings(&self, account_id: &str, label: &str) -> Result<LabelStatus> {
        let resp = self
            .tagging()
            .get_label_settings(pb::LabelRef {
                account_id: account_id.into(),
                label: label.into(),
            })
            .await?;
        Ok(status(resp.into_inner()))
    }

    pub async fn set_label_settings(
        &self,
        account_id: &str,
        label: &str,
        mode: &str,
        threshold: f64,
    ) -> Result<LabelStatus> {
        let resp = self
            .tagging()
            .set_label_settings(pb::SetLabelSettingsRequest {
                account_id: account_id.into(),
                label: label.into(),
                mode: mode.into(),
                threshold,
            })
            .await?;
        Ok(status(resp.into_inner()))
    }

    /// Labels with their settings per training scope; one scope with an account.
    pub async fn list_labels(&self, account_id: Option<&str>) -> Result<Vec<LabelScope>> {
        let resp = self
            .tagging()
            .list_labels(pb::ListLabelsRequest {
                account_id: account_id.map(str::to_owned),
            })
            .await?;
        Ok(resp
            .into_inner()
            .scopes
            .into_iter()
            .map(|s| LabelScope {
                scope: s.scope,
                group: s.group,
                account_ids: s.account_ids,
                labels: s
                    .labels
                    .into_iter()
                    .map(|l| LabelEntry {
                        name: l.name,
                        status: status(l.status.unwrap_or_default()),
                    })
                    .collect(),
            })
            .collect())
    }

    pub async fn get_message_tags(&self, message_id: &str) -> Result<MessageTags> {
        let resp = self
            .tagging()
            .get_message_tags(pb::MessageTagsRequest {
                message_id: message_id.into(),
            })
            .await?
            .into_inner();
        Ok(MessageTags {
            suggestions: resp
                .suggestions
                .into_iter()
                .map(|s| TagSuggestion {
                    label: s.label,
                    mailbox_id: s.mailbox_id,
                    name: s.name,
                    confidence: s.confidence,
                })
                .collect(),
            auto: resp
                .auto
                .into_iter()
                .map(|a| AutoLabel {
                    label: a.label,
                    mailbox_id: a.mailbox_id,
                    name: a.name,
                    archived: a.archived,
                    at: a.at,
                })
                .collect(),
        })
    }

    pub async fn accept_suggestion(&self, message_id: &str, label: &str) -> Result<()> {
        self.tagging()
            .accept_suggestion(suggestion_ref(message_id, label))
            .await?;
        Ok(())
    }

    pub async fn reject_suggestion(&self, message_id: &str, label: &str) -> Result<()> {
        self.tagging()
            .reject_suggestion(suggestion_ref(message_id, label))
            .await?;
        Ok(())
    }

    pub async fn undo_auto_label(&self, message_id: &str, label: &str) -> Result<()> {
        self.tagging()
            .undo_auto_label(suggestion_ref(message_id, label))
            .await?;
        Ok(())
    }

    pub async fn preview_apply(
        &self,
        account_id: &str,
        label: &str,
        mode: &str,
        threshold: f64,
    ) -> Result<ApplyPreview> {
        let resp = self
            .tagging()
            .preview_apply(apply_request(account_id, label, mode, threshold))
            .await?;
        Ok(preview(resp.into_inner()))
    }

    /// Saves the mode and labels existing mail in the background.
    pub async fn confirm_apply(
        &self,
        account_id: &str,
        label: &str,
        mode: &str,
        threshold: f64,
    ) -> Result<ApplyPreview> {
        let resp = self
            .tagging()
            .confirm_apply(apply_request(account_id, label, mode, threshold))
            .await?;
        Ok(preview(resp.into_inner()))
    }

    pub async fn eval_labels(&self, account_id: &str) -> Result<Vec<LabelEval>> {
        let resp = self
            .tagging()
            .eval_labels(pb::EvalRequest {
                account_id: account_id.into(),
            })
            .await?
            .into_inner();
        let rows = |rows: Vec<pb::EvalRow>| {
            rows.into_iter()
                .map(|r| EvalRow {
                    threshold: r.threshold,
                    flagged: r.flagged,
                    correct: r.correct,
                    precision: r.precision,
                    recall: r.recall,
                })
                .collect()
        };
        Ok(resp
            .labels
            .into_iter()
            .map(|l| LabelEval {
                label: l.label,
                held_out: l.held_out,
                embedded: l.embedded,
                bayes_rows: rows(l.bayes_rows),
                rows: rows(l.rows),
            })
            .collect())
    }

    pub async fn get_embeddings(&self) -> Result<EmbeddingsStatus> {
        let resp = self
            .tagging()
            .get_embeddings(pb::GetEmbeddingsRequest {})
            .await?;
        Ok(embeddings(resp.into_inner()))
    }

    /// Agreeing downloads the model (once) and embeds the mail in the background.
    pub async fn set_embeddings(&self, enabled: bool) -> Result<EmbeddingsStatus> {
        let resp = self
            .tagging()
            .set_embeddings(pb::SetEmbeddingsRequest { enabled })
            .await?;
        Ok(embeddings(resp.into_inner()))
    }

    pub async fn get_background_stats(&self) -> Result<BackgroundStats> {
        let s = self
            .tagging()
            .get_background_stats(pb::GetBackgroundStatsRequest {})
            .await?
            .into_inner();
        Ok(BackgroundStats {
            model: s.model,
            speed: s.speed,
            workers: s.workers,
            threads: s.threads,
            batch: s.batch,
            pause_ms: s.pause_ms,
            state: s.state,
            model_loaded: s.model_loaded,
            embedded: s.embedded,
            eligible: s.eligible,
            per_second: s.per_second,
            eta_seconds: s.eta_seconds,
            worker_bytes: s.worker_bytes,
            retrains: s
                .retrains
                .into_iter()
                .map(|r| RetrainProgress {
                    scope: r.scope,
                    done: r.done,
                    total: r.total,
                })
                .collect(),
            discovery: s.discovery.map(|d| DiscoveryProgress {
                scope: d.scope,
                phase: d.phase,
                done: d.done,
                total: d.total,
            }),
        })
    }

    pub async fn set_background_speed(&self, speed: &str) -> Result<EmbeddingsStatus> {
        let resp = self
            .tagging()
            .set_background_speed(pb::SetBackgroundSpeedRequest {
                speed: speed.to_owned(),
            })
            .await?;
        Ok(embeddings(resp.into_inner()))
    }

    pub async fn set_embedding_model(&self, profile: &str) -> Result<EmbeddingsStatus> {
        let resp = self
            .tagging()
            .set_embedding_model(pb::SetEmbeddingModelRequest {
                profile: profile.to_owned(),
            })
            .await?;
        Ok(embeddings(resp.into_inner()))
    }

    pub async fn set_execution_target(&self, target: &str) -> Result<EmbeddingsStatus> {
        let resp = self
            .tagging()
            .set_execution_target(pb::SetExecutionTargetRequest {
                target: target.to_owned(),
            })
            .await?;
        Ok(embeddings(resp.into_inner()))
    }
}

fn embeddings(s: pb::EmbeddingsStatus) -> EmbeddingsStatus {
    EmbeddingsStatus {
        enabled: s.enabled,
        available: s.available,
        model: s.model,
        speed: s.speed,
        profile: s.profile,
        models: s
            .models
            .into_iter()
            .map(|m| ModelOption {
                profile: m.profile,
                model: m.model,
                dims: m.dims,
                download_bytes: m.download_bytes,
                worker_bytes: m.worker_bytes,
                quality: m.quality,
            })
            .collect(),
        target: s.target,
        targets: s
            .targets
            .into_iter()
            .map(|t| TargetOption {
                id: t.id,
                available: t.available,
            })
            .collect(),
    }
}
