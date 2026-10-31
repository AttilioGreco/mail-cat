use std::sync::Arc;

use tonic::{Request, Response, Status};
use uuid::Uuid;

use mc_application::CommandBus;
use mc_application::services::tagging::{LabelStatus as AppLabelStatus, TaggingService};
use mc_domain::{
    AccountId, Confidence, GroupId, LabelMode, LabelSettings, MailboxReadRepository, MessageId,
    TaggingStore,
};

use crate::grpc::mappers::application_error_to_status;
use crate::grpc::proto::{self as pb, tagging_service_server::TaggingService as Grpc};

pub struct TaggingServiceImpl<S, R, C> {
    service: Arc<TaggingService<S, R, C>>,
}

impl<S, R, C> TaggingServiceImpl<S, R, C> {
    pub fn new(service: Arc<TaggingService<S, R, C>>) -> Self {
        Self { service }
    }
}

impl<S, R, C> TaggingServiceImpl<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    fn embeddings(&self) -> pb::EmbeddingsStatus {
        pb::EmbeddingsStatus {
            enabled: self.service.embeddings_enabled(),
            available: self.service.embedding_model().is_some(),
            model: self.service.embedding_model().unwrap_or_default().into(),
        }
    }
}

fn uuid(id: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(id).map_err(|_| Status::invalid_argument(format!("invalid id {id:?}")))
}

fn account(id: &str) -> Result<AccountId, Status> {
    uuid(id).map(AccountId::from_uuid)
}

fn mode(mode: &str) -> Result<LabelMode, Status> {
    mode.parse().map_err(Status::invalid_argument)
}

fn status(s: AppLabelStatus) -> pb::LabelStatus {
    pb::LabelStatus {
        label: s.settings.label,
        mode: s.settings.mode.as_str().into(),
        threshold: s.settings.threshold,
        positives: s.positives,
        min_positives: LabelSettings::MIN_POSITIVES,
    }
}

fn group(g: mc_domain::TrainingGroup) -> pb::TrainingGroup {
    pb::TrainingGroup {
        is_default: g.id == GroupId::DEFAULT,
        id: g.id.as_uuid().to_string(),
        name: g.name,
        account_ids: g.accounts.iter().map(ToString::to_string).collect(),
    }
}

fn preview(p: mc_application::services::tagging::Preview) -> pb::ApplyPreview {
    pb::ApplyPreview {
        to_label: p.to_label,
        to_archive: p.to_archive,
        sample: p
            .sample
            .into_iter()
            .map(|i| pb::ApplyPreviewItem {
                message_id: i.message_id.to_string(),
                subject: i.subject,
                from: i.from,
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl<S, R, C> Grpc for TaggingServiceImpl<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    async fn list_groups(
        &self,
        _: Request<pb::ListGroupsRequest>,
    ) -> Result<Response<pb::ListGroupsResponse>, Status> {
        let groups = self
            .service
            .groups()
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::ListGroupsResponse {
            groups: groups.into_iter().map(group).collect(),
        }))
    }

    async fn create_group(
        &self,
        request: Request<pb::CreateGroupRequest>,
    ) -> Result<Response<pb::TrainingGroup>, Status> {
        let created = self
            .service
            .create_group(&request.into_inner().name)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(group(created)))
    }

    async fn rename_group(
        &self,
        request: Request<pb::RenameGroupRequest>,
    ) -> Result<Response<pb::RenameGroupResponse>, Status> {
        let req = request.into_inner();
        self.service
            .rename_group(GroupId::from_uuid(uuid(&req.id)?), &req.name)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::RenameGroupResponse {}))
    }

    async fn delete_group(
        &self,
        request: Request<pb::DeleteGroupRequest>,
    ) -> Result<Response<pb::DeleteGroupResponse>, Status> {
        self.service
            .delete_group(GroupId::from_uuid(uuid(&request.into_inner().id)?))
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::DeleteGroupResponse {}))
    }

    async fn assign_account(
        &self,
        request: Request<pb::AssignAccountRequest>,
    ) -> Result<Response<pb::AssignAccountResponse>, Status> {
        let req = request.into_inner();
        let group = req
            .group_id
            .as_deref()
            .map(|g| uuid(g).map(GroupId::from_uuid))
            .transpose()?;
        self.service
            .assign_account(account(&req.account_id)?, group)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::AssignAccountResponse {}))
    }

    async fn get_label_settings(
        &self,
        request: Request<pb::LabelRef>,
    ) -> Result<Response<pb::LabelStatus>, Status> {
        let req = request.into_inner();
        let found = self
            .service
            .label_status(account(&req.account_id)?, &req.label)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(status(found)))
    }

    async fn set_label_settings(
        &self,
        request: Request<pb::SetLabelSettingsRequest>,
    ) -> Result<Response<pb::LabelStatus>, Status> {
        let req = request.into_inner();
        let saved = self
            .service
            .set_label(
                account(&req.account_id)?,
                &req.label,
                mode(&req.mode)?,
                req.threshold,
            )
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(status(saved)))
    }

    async fn get_message_tags(
        &self,
        request: Request<pb::MessageTagsRequest>,
    ) -> Result<Response<pb::MessageTagsResponse>, Status> {
        let id = MessageId::from_uuid(uuid(&request.into_inner().message_id)?);
        let tags = self
            .service
            .tags_for(id)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::MessageTagsResponse {
            suggestions: tags
                .suggestions
                .into_iter()
                .map(|s| pb::TagSuggestion {
                    label: s.label,
                    mailbox_id: s.mailbox_id.map(|m| m.to_string()).unwrap_or_default(),
                    name: s.name,
                    confidence: match s.confidence {
                        Confidence::High => "high",
                        Confidence::Medium => "medium",
                    }
                    .into(),
                })
                .collect(),
            auto: tags
                .auto
                .into_iter()
                .map(|a| pb::AutoLabel {
                    label: a.label,
                    mailbox_id: a.mailbox_id.map(|m| m.to_string()).unwrap_or_default(),
                    name: a.name,
                    archived: a.archived,
                    at: a.at.timestamp(),
                })
                .collect(),
        }))
    }

    async fn accept_suggestion(
        &self,
        request: Request<pb::SuggestionRef>,
    ) -> Result<Response<pb::TaggingAck>, Status> {
        let req = request.into_inner();
        self.service
            .accept(MessageId::from_uuid(uuid(&req.message_id)?), &req.label)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::TaggingAck {}))
    }

    async fn reject_suggestion(
        &self,
        request: Request<pb::SuggestionRef>,
    ) -> Result<Response<pb::TaggingAck>, Status> {
        let req = request.into_inner();
        self.service
            .reject(MessageId::from_uuid(uuid(&req.message_id)?), &req.label)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::TaggingAck {}))
    }

    async fn undo_auto_label(
        &self,
        request: Request<pb::SuggestionRef>,
    ) -> Result<Response<pb::TaggingAck>, Status> {
        let req = request.into_inner();
        self.service
            .undo_auto(MessageId::from_uuid(uuid(&req.message_id)?), &req.label)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::TaggingAck {}))
    }

    async fn preview_apply(
        &self,
        request: Request<pb::ApplyRequest>,
    ) -> Result<Response<pb::ApplyPreview>, Status> {
        let req = request.into_inner();
        let plan = self
            .service
            .preview_apply(
                account(&req.account_id)?,
                &req.label,
                mode(&req.mode)?,
                req.threshold,
            )
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(preview(plan)))
    }

    async fn confirm_apply(
        &self,
        request: Request<pb::ApplyRequest>,
    ) -> Result<Response<pb::ApplyPreview>, Status> {
        let req = request.into_inner();
        let plan = self
            .service
            .confirm_apply(
                account(&req.account_id)?,
                &req.label,
                mode(&req.mode)?,
                req.threshold,
            )
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(preview(plan)))
    }

    async fn eval_labels(
        &self,
        request: Request<pb::EvalRequest>,
    ) -> Result<Response<pb::EvalResponse>, Status> {
        let report = self
            .service
            .eval(account(&request.into_inner().account_id)?)
            .await
            .map_err(application_error_to_status)?;
        let rows = |rows: Vec<mc_application::services::tagging::EvalRow>| {
            rows.into_iter()
                .map(|r| pb::EvalRow {
                    threshold: r.threshold,
                    flagged: r.flagged,
                    correct: r.correct,
                    precision: r.precision,
                    recall: r.recall,
                })
                .collect()
        };
        Ok(Response::new(pb::EvalResponse {
            labels: report
                .into_iter()
                .map(|l| pb::LabelEval {
                    label: l.label,
                    held_out: l.held_out,
                    embedded: l.embedded,
                    rows: rows(l.rows),
                    bayes_rows: rows(l.bayes_rows),
                })
                .collect(),
        }))
    }

    async fn get_embeddings(
        &self,
        _: Request<pb::GetEmbeddingsRequest>,
    ) -> Result<Response<pb::EmbeddingsStatus>, Status> {
        Ok(Response::new(self.embeddings()))
    }

    async fn set_embeddings(
        &self,
        request: Request<pb::SetEmbeddingsRequest>,
    ) -> Result<Response<pb::EmbeddingsStatus>, Status> {
        self.service
            .set_embeddings(request.into_inner().enabled)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(self.embeddings()))
    }
}
