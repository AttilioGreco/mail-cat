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
    pub rows: Vec<EvalRow>,
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
    fn tagging(&self) -> TaggingServiceClient<tonic::transport::Channel> {
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
        Ok(resp
            .labels
            .into_iter()
            .map(|l| LabelEval {
                label: l.label,
                held_out: l.held_out,
                rows: l
                    .rows
                    .into_iter()
                    .map(|r| EvalRow {
                        threshold: r.threshold,
                        flagged: r.flagged,
                        correct: r.correct,
                        precision: r.precision,
                        recall: r.recall,
                    })
                    .collect(),
            })
            .collect())
    }
}
