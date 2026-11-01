use std::sync::Arc;

use tonic::{Request, Response, Status};
use uuid::Uuid;

use mc_application::CommandBus;
use mc_application::services::tagging::{
    AccountCount, ApplyKind, DiscoveryPhase, EmbedderState, LabelChange, LabelHome, LabelPresence,
    LabelStatus as AppLabelStatus, OpResult, OpStatus, PlanOp, PlanSummary, ProposalMessage,
    ProposalView, TaggingService,
};
use mc_domain::{
    AccountId, Confidence, GroupId, LabelMode, LabelSettings, MailboxReadRepository, MessageId,
    ProposalStatus, TaggingStore,
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
            model: self
                .service
                .embedding_model()
                .map(|m| m.to_string())
                .unwrap_or_default(),
            speed: self.service.background_speed().as_str().into(),
            profile: self.service.model_profile().as_str().into(),
            models: self
                .service
                .model_options()
                .into_iter()
                .map(|m| pb::ModelOption {
                    profile: m.profile.as_str().into(),
                    model: m.model.to_string(),
                    dims: m.dims as u32,
                    download_bytes: m.download_bytes,
                    worker_bytes: m.worker_bytes,
                    quality: m.quality.into(),
                })
                .collect(),
            target: self.service.execution_target(),
            targets: self
                .service
                .target_options()
                .into_iter()
                .map(|t| pb::TargetOption {
                    id: t.id,
                    available: t.available,
                })
                .collect(),
        }
    }
}

fn uuid(id: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(id).map_err(|_| Status::invalid_argument(format!("invalid id {id:?}")))
}

fn proposal(id: &str) -> Result<mc_domain::ProposalId, Status> {
    uuid(id).map(mc_domain::ProposalId::from_uuid)
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

fn proposal_view(p: ProposalView) -> pb::LabelProposal {
    pb::LabelProposal {
        id: p.proposal.id.as_uuid().to_string(),
        size: p.proposal.size() as u32,
        inbox: p.proposal.inbox,
        group: p.group,
        created_at: p.proposal.created_at.timestamp(),
        status: p.proposal.status.as_str().into(),
        scope: p.proposal.scope.key(),
        senders: p
            .senders
            .into_iter()
            .map(|(domain, count)| pb::SenderCount { domain, count })
            .collect(),
        curated: p.proposal.curated,
        name: p.proposal.name,
        keywords: p.proposal.keywords,
        samples: p
            .proposal
            .samples
            .into_iter()
            .map(|s| pb::ProposalSample {
                message_id: s.message_id.to_string(),
                subject: s.subject,
                from: s.from,
            })
            .collect(),
    }
}

fn proposal_message(m: ProposalMessage) -> pb::ProposalMessage {
    pb::ProposalMessage {
        message_id: m.message_id.to_string(),
        received_at: m.received_at.to_rfc3339(),
        from: m.from,
        subject: m.subject,
        excerpt: m.excerpt,
    }
}

fn account_count(a: AccountCount) -> pb::AccountCount {
    pb::AccountCount {
        account_id: a.account.to_string(),
        messages: a.messages,
        archived: a.archived,
    }
}

fn label_change(r: LabelChange) -> pb::LabelChangeReport {
    pb::LabelChangeReport {
        dry_run: r.dry_run,
        label: r.label,
        into: r.into.unwrap_or_default(),
        merge: r.merge,
        mail_only: r.mail_only,
        messages: r.messages,
        accounts: r
            .accounts
            .into_iter()
            .map(|a| pb::LabelChangeAccount {
                account_id: a.account.to_string(),
                kind: a.kind.as_str().into(),
                messages: a.messages,
                merges: a.merges,
                deletes_mailbox: a.deletes_mailbox,
                rehomed: a.rehomed,
                mailboxes: a
                    .mailboxes
                    .into_iter()
                    .map(|m| pb::LabelChangeMailbox {
                        label: m.label,
                        into: m.into.unwrap_or_default(),
                        how: m.how.as_str().into(),
                        messages: m.messages,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn plan_op(op: pb::CurationOp) -> Result<PlanOp, Status> {
    let named = |v: String| Some(v).filter(|v| !v.trim().is_empty());
    Ok(match op.kind.as_str() {
        "merge" => PlanOp::Merge {
            ids: op.ids,
            name: named(op.name),
            alias: named(op.alias),
        },
        "rename" => PlanOp::Rename {
            id: op.id,
            name: op.name,
        },
        "assign" => PlanOp::Assign {
            id: op.id,
            label: op.label,
            archive: op.archive,
        },
        "accept" => PlanOp::Accept {
            id: op.id,
            name: named(op.name),
            archive: op.archive,
        },
        "reject" => PlanOp::Reject { id: op.id },
        "mode" => PlanOp::Mode {
            mode: mode(&op.mode)?,
            label: op.label,
            threshold: op.threshold,
            existing: op.existing,
            account: op.account_id.as_deref().map(account).transpose()?,
        },
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown operation {other:?}"
            )));
        }
    })
}

fn op_result(r: OpResult) -> pb::OpResult {
    pb::OpResult {
        index: r.index as u32,
        kind: r.kind.into(),
        status: match r.status {
            OpStatus::Planned => "planned",
            OpStatus::Done => "done",
            OpStatus::Failed => "failed",
            OpStatus::Skipped => "skipped",
        }
        .into(),
        detail: r.detail,
        proposal_id: r
            .proposal
            .map(|p| p.as_uuid().to_string())
            .unwrap_or_default(),
        queued: r.queued,
    }
}

fn plan_summary(s: PlanSummary) -> pb::PlanSummary {
    pb::PlanSummary {
        merged: s.merged,
        renamed: s.renamed,
        accepted: s.accepted,
        assigned: s.assigned,
        rejected: s.rejected,
        modes: s.modes,
        labels_to_create: s
            .labels_to_create
            .into_iter()
            .map(|(name, accounts)| pb::LabelCreation {
                name,
                account_ids: accounts.iter().map(ToString::to_string).collect(),
            })
            .collect(),
        labels_touched: s.labels_touched,
        to_label: s.to_label,
        to_archive: s.to_archive,
        accounts: s.accounts.into_iter().map(account_count).collect(),
    }
}

fn presence(p: LabelPresence) -> pb::LabelPresence {
    let (kind, mailbox_id, path, messages, keyword) = match p.home {
        LabelHome::Mailbox { id, path, messages } => {
            ("mailbox", id.to_string(), path, messages, String::new())
        }
        LabelHome::Keyword(k) => ("keyword", String::new(), String::new(), 0, k),
        LabelHome::Missing => ("missing", String::new(), String::new(), 0, String::new()),
    };
    pb::LabelPresence {
        account_id: p.account.to_string(),
        kind: kind.into(),
        mailbox_id,
        path,
        messages,
        keyword,
        examples: p.examples,
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

    async fn set_background_speed(
        &self,
        request: Request<pb::SetBackgroundSpeedRequest>,
    ) -> Result<Response<pb::EmbeddingsStatus>, Status> {
        let speed = request
            .into_inner()
            .speed
            .parse()
            .map_err(Status::invalid_argument)?;
        self.service
            .set_background_speed(speed)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(self.embeddings()))
    }

    async fn set_embedding_model(
        &self,
        request: Request<pb::SetEmbeddingModelRequest>,
    ) -> Result<Response<pb::EmbeddingsStatus>, Status> {
        let profile = request
            .into_inner()
            .profile
            .parse()
            .map_err(Status::invalid_argument)?;
        self.service
            .set_model_profile(profile)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(self.embeddings()))
    }

    async fn set_execution_target(
        &self,
        request: Request<pb::SetExecutionTargetRequest>,
    ) -> Result<Response<pb::EmbeddingsStatus>, Status> {
        self.service
            .set_execution_target(&request.into_inner().target)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(self.embeddings()))
    }

    async fn get_background_stats(
        &self,
        _: Request<pb::GetBackgroundStatsRequest>,
    ) -> Result<Response<pb::BackgroundStats>, Status> {
        let s = self
            .service
            .background_stats()
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::BackgroundStats {
            speed: s.speed.as_str().into(),
            workers: s.profile.workers as u32,
            threads: s.profile.threads as u32,
            batch: s.profile.batch,
            pause_ms: s.profile.pause.as_millis() as u32,
            state: match s.state {
                EmbedderState::Off => "off",
                EmbedderState::Loading => "loading",
                EmbedderState::Active => "active",
                EmbedderState::PausedForRetrain => "paused_for_retrain",
                EmbedderState::Idle => "idle",
            }
            .into(),
            model_loaded: s.model_loaded,
            model: s.model.map(|m| m.to_string()).unwrap_or_default(),
            embedded: s.embedded,
            eligible: s.eligible,
            per_second: s.per_second,
            eta_seconds: s.eta.map_or(0, |d| d.as_secs()),
            worker_bytes: s.worker_bytes.unwrap_or_default(),
            retrains: s
                .retrains
                .into_iter()
                .map(|r| pb::RetrainProgress {
                    scope: r.scope,
                    done: r.done,
                    total: r.total,
                })
                .collect(),
            applying: s
                .applying
                .into_iter()
                .map(|a| pb::ApplyProgress {
                    label: a.label,
                    kind: match a.kind {
                        ApplyKind::Apply => "apply",
                        ApplyKind::Undo => "undo",
                        ApplyKind::Relabel => "relabel",
                        ApplyKind::Delete => "delete",
                    }
                    .into(),
                    done: a.done,
                    total: a.total,
                    archived: a.archived,
                })
                .collect(),
            discovery: s.discovery.map(|d| pb::DiscoveryProgress {
                scope: d.scope,
                phase: match d.phase {
                    DiscoveryPhase::Loading => "loading",
                    DiscoveryPhase::Clustering => "clustering",
                    DiscoveryPhase::Naming => "naming",
                }
                .into(),
                done: d.done,
                total: d.total,
            }),
        }))
    }

    async fn discover_labels(
        &self,
        request: Request<pb::DiscoverLabelsRequest>,
    ) -> Result<Response<pb::DiscoverLabelsResponse>, Status> {
        let account = request
            .into_inner()
            .account_id
            .map(|a| account(&a))
            .transpose()?;
        let started = self
            .service
            .start_discovery(account)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::DiscoverLabelsResponse { started }))
    }

    async fn list_label_proposals(
        &self,
        request: Request<pb::ListLabelProposalsRequest>,
    ) -> Result<Response<pb::ListLabelProposalsResponse>, Status> {
        let req = request.into_inner();
        let account = req.account_id.as_deref().map(account).transpose()?;
        let status = match req.status.as_str() {
            "" => Some(ProposalStatus::Pending),
            "all" => None,
            other => Some(other.parse().map_err(Status::invalid_argument)?),
        };
        let proposals = self
            .service
            .list_proposals(account, status, req.with_senders)
            .await
            .map_err(application_error_to_status)?
            .into_iter()
            .map(proposal_view)
            .collect();
        Ok(Response::new(pb::ListLabelProposalsResponse { proposals }))
    }

    async fn get_label_proposal(
        &self,
        request: Request<pb::GetLabelProposalRequest>,
    ) -> Result<Response<pb::LabelProposalDetail>, Status> {
        let req = request.into_inner();
        let (view, messages) = self
            .service
            .proposal_detail(proposal(&req.id)?, req.samples as usize)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::LabelProposalDetail {
            proposal: Some(proposal_view(view)),
            messages: messages.into_iter().map(proposal_message).collect(),
        }))
    }

    async fn rename_label_proposal(
        &self,
        request: Request<pb::RenameLabelProposalRequest>,
    ) -> Result<Response<pb::LabelProposal>, Status> {
        let req = request.into_inner();
        let renamed = self
            .service
            .rename_proposal(proposal(&req.id)?, &req.name)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(proposal_view(renamed)))
    }

    async fn merge_label_proposals(
        &self,
        request: Request<pb::MergeLabelProposalsRequest>,
    ) -> Result<Response<pb::LabelProposal>, Status> {
        let req = request.into_inner();
        let ids = req
            .ids
            .iter()
            .map(|id| proposal(id))
            .collect::<Result<Vec<_>, _>>()?;
        let name = Some(req.name.as_str()).filter(|n| !n.trim().is_empty());
        let merged = self
            .service
            .merge_proposals(&ids, name)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(proposal_view(merged)))
    }

    async fn assign_label_proposal(
        &self,
        request: Request<pb::AssignLabelProposalRequest>,
    ) -> Result<Response<pb::AcceptLabelProposalResponse>, Status> {
        let req = request.into_inner();
        let messages = self
            .service
            .assign_proposal(proposal(&req.id)?, &req.label, req.archive)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::AcceptLabelProposalResponse { messages }))
    }

    async fn list_labels(
        &self,
        request: Request<pb::ListLabelsRequest>,
    ) -> Result<Response<pb::ListLabelsResponse>, Status> {
        let account = request
            .into_inner()
            .account_id
            .as_deref()
            .map(account)
            .transpose()?;
        let scopes = self
            .service
            .list_labels(account)
            .await
            .map_err(application_error_to_status)?
            .into_iter()
            .map(|s| pb::LabelScope {
                scope: s.scope,
                group: s.group,
                account_ids: s.accounts.iter().map(ToString::to_string).collect(),
                labels: s
                    .labels
                    .into_iter()
                    .map(|l| pb::LabelEntry {
                        name: l.name,
                        status: Some(status(l.status)),
                    })
                    .collect(),
            })
            .collect();
        Ok(Response::new(pb::ListLabelsResponse { scopes }))
    }

    async fn get_label(
        &self,
        request: Request<pb::GetLabelRequest>,
    ) -> Result<Response<pb::GetLabelResponse>, Status> {
        let req = request.into_inner();
        let account = req.account_id.as_deref().map(account).transpose()?;
        let labels = self
            .service
            .label_detail(&req.label, account, req.samples as usize)
            .await
            .map_err(application_error_to_status)?
            .into_iter()
            .map(|d| pb::LabelDetail {
                scope: d.scope.scope,
                group: d.scope.group,
                account_ids: d.scope.accounts.iter().map(ToString::to_string).collect(),
                name: d.entry.name,
                status: Some(status(d.entry.status)),
                accounts: d.presence.into_iter().map(presence).collect(),
                samples: d.samples.into_iter().map(proposal_message).collect(),
            })
            .collect();
        Ok(Response::new(pb::GetLabelResponse { labels }))
    }

    async fn apply_curation_plan(
        &self,
        request: Request<pb::ApplyCurationPlanRequest>,
    ) -> Result<Response<pb::ApplyCurationPlanResponse>, Status> {
        let req = request.into_inner();
        let ops = req
            .ops
            .into_iter()
            .map(plan_op)
            .collect::<Result<Vec<_>, _>>()?;
        let report = self
            .service
            .apply_plan(&ops, req.dry_run)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::ApplyCurationPlanResponse {
            dry_run: report.dry_run,
            summary: Some(plan_summary(report.summary)),
            results: report.results.into_iter().map(op_result).collect(),
        }))
    }

    async fn undo_label_proposal(
        &self,
        request: Request<pb::UndoLabelProposalRequest>,
    ) -> Result<Response<pb::UndoLabelProposalResponse>, Status> {
        let req = request.into_inner();
        let report = self
            .service
            .undo_proposal(proposal(&req.id)?, req.reject, req.dry_run)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::UndoLabelProposalResponse {
            dry_run: report.dry_run,
            to_unlabel: report.to_unlabel,
            to_unarchive: report.to_unarchive,
            accounts: report.accounts.into_iter().map(account_count).collect(),
            labels_to_delete: report.labels_to_delete,
            status: report.status.as_str().into(),
        }))
    }

    async fn rename_label(
        &self,
        request: Request<pb::RenameLabelRequest>,
    ) -> Result<Response<pb::LabelChangeReport>, Status> {
        let req = request.into_inner();
        let account = req.account_id.as_deref().map(account).transpose()?;
        let report = self
            .service
            .rename_label(&req.from, &req.to, account, req.mail_only, req.dry_run)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(label_change(report)))
    }

    async fn delete_label(
        &self,
        request: Request<pb::DeleteLabelRequest>,
    ) -> Result<Response<pb::LabelChangeReport>, Status> {
        let req = request.into_inner();
        let account = req.account_id.as_deref().map(account).transpose()?;
        let report = self
            .service
            .delete_label(&req.label, account, req.dry_run)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(label_change(report)))
    }

    async fn accept_label_proposal(
        &self,
        request: Request<pb::AcceptLabelProposalRequest>,
    ) -> Result<Response<pb::AcceptLabelProposalResponse>, Status> {
        let r = request.into_inner();
        let name = Some(r.name.as_str()).filter(|n| !n.trim().is_empty());
        let messages = self
            .service
            .accept_proposal(proposal(&r.id)?, name, r.archive)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::AcceptLabelProposalResponse { messages }))
    }

    async fn reject_label_proposal(
        &self,
        request: Request<pb::RejectLabelProposalRequest>,
    ) -> Result<Response<pb::TaggingAck>, Status> {
        self.service
            .reject_proposal(proposal(&request.into_inner().id)?)
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(pb::TaggingAck {}))
    }
}
