use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{
    application_error_to_status, compose_view_to_proto, domain_message_to_detail,
    domain_message_to_summary, domain_tags, paged_result_to_page_info, proto_compose_input_to_app,
    proto_compose_mode, proto_flags_to_domain, proto_to_domain_pagination,
};
use crate::grpc::proto::{
    ArchiveMessagesRequest, BulkChangeResponse, ChangeLabelsRequest, ComposeInput, ComposeView,
    CountsChangedEvent, DeleteMessageRequest, DeleteMessageResponse, DeleteMessagesRequest,
    DiscardDraftRequest, DiscardDraftResponse, FlagsChangedEvent, GetMessageRequest,
    GetMessageResponse, GetThreadRequest, GetThreadResponse, ListConversationsRequest,
    ListConversationsResponse, ListMessagesRequest, ListMessagesResponse, MailboxEvent,
    MarkMailboxReadRequest, MarkMailboxReadResponse, MessageExpungedEvent, MessageRef,
    ModifyTagsRequest, MoveMessageRequest, MoveMessageResponse, MoveMessagesRequest,
    NewMessageEvent, PrepareComposeRequest, RebuildConversationIndexRequest,
    RebuildConversationIndexResponse, SaveDraftResponse, SendComposeResponse, SetFlagsRequest,
    SetFlagsResponse, WatchMailboxRequest, mailbox_event, message_service_server::MessageService,
};
use mc_application::{
    InProcessEventBus,
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::Query,
};
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::Message;
use mc_domain::{AccountId, DomainEvent, Flag, MailboxId, MessageId};

fn parse_mailbox_id(id: &str, field: &str) -> Result<MailboxId, Status> {
    uuid::Uuid::parse_str(id)
        .map(MailboxId::from_uuid)
        .map_err(|e| Status::invalid_argument(format!("invalid {field}: {e}")))
}

/// Parse a bulk request's ids up-front: one malformed id means the caller sent
/// garbage, so reject the whole request rather than half-deleting a selection.
fn parse_message_refs(refs: &[MessageRef]) -> Result<Vec<(MessageId, MailboxId)>, Status> {
    refs.iter()
        .map(|r| {
            let message_id = uuid::Uuid::parse_str(&r.message_id)
                .map(MessageId::from_uuid)
                .map_err(|e| {
                    Status::invalid_argument(format!("invalid message_id {}: {e}", r.message_id))
                })?;
            Ok((message_id, parse_mailbox_id(&r.mailbox_id, "mailbox_id")?))
        })
        .collect()
}

fn parse_message_ids(ids: &[String]) -> Result<Vec<MessageId>, Status> {
    ids.iter()
        .map(|id| {
            uuid::Uuid::parse_str(id)
                .map(MessageId::from_uuid)
                .map_err(|e| Status::invalid_argument(format!("invalid message_id {id}: {e}")))
        })
        .collect()
}

fn parse_mailbox_ids(ids: &[String]) -> Result<Vec<MailboxId>, Status> {
    ids.iter()
        .map(|id| parse_mailbox_id(id, "mailbox_id"))
        .collect()
}

/// A tag is an IMAP keyword and a JMAP keyword at once: an atom without the
/// characters either grammar reserves, and not a `\` system flag.
fn validate_tag(tag: &str) -> Result<(), Status> {
    // JMAP maps these keywords onto \Seen, \Flagged, \Draft and \Answered, so as
    // tags they would silently change message state behind the flag path.
    let reserved = ["$seen", "$flagged", "$draft", "$answered"]
        .iter()
        .any(|k| tag.eq_ignore_ascii_case(k));
    let valid = !tag.is_empty()
        && !reserved
        && tag.len() <= 255
        && tag
            .chars()
            .all(|c| c.is_ascii_graphic() && !"(){]%*\"\\".contains(c));
    if valid {
        return Ok(());
    }
    Err(Status::invalid_argument(format!("invalid tag {tag:?}")))
}

fn bulk_counts(result: mc_application::commands::CommandResult) -> Result<(u32, u32), Status> {
    match result {
        mc_application::commands::CommandResult::MessagesBulkChanged { succeeded, failed } => {
            Ok((succeeded, failed))
        }
        other => Err(Status::internal(format!(
            "unexpected command result for bulk operation: {other:?}"
        ))),
    }
}

/// Message service implementation with CQRS bus integration.
pub struct MessageServiceImpl<C: CommandBus + 'static, Q: QueryBus + 'static> {
    command_bus: Arc<C>,
    query_bus: Arc<Q>,
    /// Domain event bus: watch_mailbox subscribes here and filters events by mailbox.
    event_bus: Arc<InProcessEventBus>,
}

impl<C: CommandBus + 'static, Q: QueryBus + 'static> MessageServiceImpl<C, Q> {
    pub fn new(command_bus: Arc<C>, query_bus: Arc<Q>, event_bus: Arc<InProcessEventBus>) -> Self {
        Self {
            command_bus,
            query_bus,
            event_bus,
        }
    }

    async fn load_message(&self, message_id: MessageId) -> Result<Message, Status> {
        self.query_bus
            .dispatch(Query::GetMessage { message_id })
            .await
            .map_err(application_error_to_status)?
            .into_message()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("message not found"))
    }

    /// The message and the mailbox a request acts from. A message that is not
    /// in that mailbox is an error, never another of its mailboxes.
    async fn message_in_mailbox(
        &self,
        message_id_str: &str,
        mailbox_id_str: &str,
    ) -> Result<(Message, Mailbox), Status> {
        let message_id = MessageId::from_uuid(
            uuid::Uuid::parse_str(message_id_str)
                .map_err(|e| Status::invalid_argument(format!("invalid message_id: {e}")))?,
        );
        let mailbox_id = parse_mailbox_id(mailbox_id_str, "mailbox_id")?;

        let message = self.load_message(message_id).await?;
        if message.membership(mailbox_id).is_none() {
            return Err(Status::not_found("message is not in that mailbox"));
        }
        let mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox { mailbox_id })
            .await
            .map_err(application_error_to_status)?
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("mailbox not found"))?;

        Ok((message, mailbox))
    }
}

#[tonic::async_trait]
impl<C, Q> MessageService for MessageServiceImpl<C, Q>
where
    C: CommandBus + 'static,
    Q: QueryBus + 'static,
{
    async fn list_messages(
        &self,
        request: Request<ListMessagesRequest>,
    ) -> Result<Response<ListMessagesResponse>, Status> {
        let req = request.into_inner();

        let mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.mailbox_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let pagination = proto_to_domain_pagination(req.pagination);

        let query = Query::ListMessages {
            mailbox_id,
            pagination,
        };

        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let paged = result
            .into_messages()
            .map_err(|e| Status::internal(e.to_string()))?;

        let messages = paged
            .items
            .iter()
            .map(|summary| {
                let envelope = crate::grpc::proto::MessageEnvelope {
                    from: vec![crate::grpc::proto::EmailAddress {
                        email: summary.from_email.clone(),
                        name: summary.from_name.clone(),
                    }],
                    to: summary
                        .to
                        .iter()
                        .map(|email| crate::grpc::proto::EmailAddress {
                            email: email.clone(),
                            name: None,
                        })
                        .collect(),
                    cc: summary
                        .cc
                        .iter()
                        .map(|email| crate::grpc::proto::EmailAddress {
                            email: email.clone(),
                            name: None,
                        })
                        .collect(),
                    subject: summary.subject.clone(),
                    date: Some(crate::grpc::mappers::datetime_to_timestamp(
                        &summary.received_at,
                    )),
                    ..Default::default()
                };
                crate::grpc::proto::MessageSummary {
                    id: summary.id.to_string(),
                    account_id: summary.account_id.to_string(),
                    mailbox_ids: summary
                        .memberships
                        .iter()
                        .map(|m| m.mailbox_id.to_string())
                        .collect(),
                    envelope: Some(envelope),
                    flags: crate::grpc::mappers::domain_flags_to_proto(&summary.flags),
                    size_bytes: summary.size_bytes,
                    has_attachments: summary.has_attachments,
                    received_at: Some(crate::grpc::mappers::datetime_to_timestamp(
                        &summary.received_at,
                    )),
                    tags: domain_tags(&summary.flags),
                }
            })
            .collect();

        Ok(Response::new(ListMessagesResponse {
            messages,
            page_info: Some(paged_result_to_page_info(&paged)),
        }))
    }

    async fn get_message(
        &self,
        request: Request<GetMessageRequest>,
    ) -> Result<Response<GetMessageResponse>, Status> {
        let req = request.into_inner();

        let (message, mailbox) = self
            .message_in_mailbox(&req.message_id, &req.mailbox_id)
            .await?;

        // The background backfill worker drains newest-first, so a just-arrived
        // message can still be body-less when the user clicks it — which renders
        // as a blank pane. Opening a message is the strongest possible "I want
        // this one now" signal, so jump the queue and fetch it inline. Failure
        // (offline, provider hiccup) falls through to the empty body rather than
        // erroring the whole read: the backfill worker will retry.
        // A message without a known UID still goes: the handler finds it by
        // Message-ID. So does one the backfill only got the start of.
        let message = if message.body_truncated
            || (message.body_text.is_none() && message.body_html.is_none())
        {
            let message_id = message.id;
            match self
                .command_bus
                .dispatch(Command::FetchMessageBody {
                    message_id,
                    mailbox_id: mailbox.id,
                })
                .await
            {
                Ok(_) => self
                    .query_bus
                    .dispatch(Query::GetMessage { message_id })
                    .await
                    .map_err(application_error_to_status)?
                    .into_message()
                    .map_err(|e| Status::internal(e.to_string()))?
                    .unwrap_or(message),
                Err(e) => {
                    tracing::warn!(%message_id, "on-open body fetch failed: {e}");
                    message
                }
            }
        } else {
            message
        };

        Ok(Response::new(GetMessageResponse {
            message: Some(domain_message_to_detail(&message)),
        }))
    }

    async fn list_conversations(
        &self,
        request: Request<ListConversationsRequest>,
    ) -> Result<Response<ListConversationsResponse>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(format!("invalid account_id: {e}")))?,
        );
        let pagination = proto_to_domain_pagination(req.pagination);
        let mailbox_id = req
            .mailbox_id
            .map(|id| {
                uuid::Uuid::parse_str(&id)
                    .map(MailboxId::from_uuid)
                    .map_err(|e| Status::invalid_argument(format!("invalid mailbox_id: {e}")))
            })
            .transpose()?;

        let query = Query::ListConversations {
            account_id,
            mailbox_id,
            pagination,
        };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let paged = result
            .into_conversations()
            .map_err(|e| Status::internal(e.to_string()))?;

        let conversations = paged
            .items
            .iter()
            .map(|c| crate::grpc::proto::ConversationSummary {
                thread_id: c.thread_id.to_string(),
                account_id: c.account_id.to_string(),
                subject: c.subject.clone(),
                message_count: c.message_count,
                unread_count: c.unread_count,
                last_message_at: Some(crate::grpc::mappers::datetime_to_timestamp(
                    &c.last_message_at,
                )),
                latest_sender: c.latest_sender.clone(),
                sample_message_id: c.sample_message_id.to_string(),
            })
            .collect();

        Ok(Response::new(ListConversationsResponse {
            conversations,
            page_info: Some(paged_result_to_page_info(&paged)),
        }))
    }

    async fn get_thread(
        &self,
        request: Request<GetThreadRequest>,
    ) -> Result<Response<GetThreadResponse>, Status> {
        let req = request.into_inner();

        let message_id = MessageId::from_uuid(
            uuid::Uuid::parse_str(&req.message_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let query = Query::GetThread { message_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let messages = result
            .into_thread()
            .map_err(|e| Status::internal(e.to_string()))?;

        let details = messages.iter().map(domain_message_to_detail).collect();

        Ok(Response::new(GetThreadResponse { messages: details }))
    }

    async fn rebuild_conversation_index(
        &self,
        _request: Request<RebuildConversationIndexRequest>,
    ) -> Result<Response<RebuildConversationIndexResponse>, Status> {
        let result = self
            .query_bus
            .dispatch(Query::RebuildConversationIndex)
            .await
            .map_err(application_error_to_status)?;

        let events_replayed = result
            .into_index_rebuilt()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RebuildConversationIndexResponse {
            events_replayed,
        }))
    }

    async fn set_flags(
        &self,
        request: Request<SetFlagsRequest>,
    ) -> Result<Response<SetFlagsResponse>, Status> {
        let req = request.into_inner();

        let message_id = MessageId::from_uuid(
            uuid::Uuid::parse_str(&req.message_id)
                .map_err(|e| Status::invalid_argument(format!("invalid message_id: {e}")))?,
        );
        let message = self.load_message(message_id).await?;

        let flags_to_add = proto_flags_to_domain(&req.flags_to_add);
        let flags_to_remove = proto_flags_to_domain(&req.flags_to_remove);

        let command = Command::SetMessageFlags {
            message_id: message.id,
            flags_to_add,
            flags_to_remove,
        };

        self.command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        // Re-query for updated state
        let updated = self
            .query_bus
            .dispatch(Query::GetMessage { message_id })
            .await
            .map_err(application_error_to_status)?
            .into_message()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("message not found after update"))?;

        Ok(Response::new(SetFlagsResponse {
            message: Some(domain_message_to_summary(&updated)),
        }))
    }

    async fn mark_mailbox_read(
        &self,
        request: Request<MarkMailboxReadRequest>,
    ) -> Result<Response<MarkMailboxReadResponse>, Status> {
        let req = request.into_inner();

        let mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.mailbox_id)
                .map_err(|e| Status::invalid_argument(format!("invalid mailbox_id: {e}")))?,
        );

        let mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox { mailbox_id })
            .await
            .map_err(application_error_to_status)?
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("mailbox not found"))?;

        let command = Command::MarkMailboxRead {
            mailbox_id,
            mailbox_path: mailbox.path,
        };

        let result = self
            .command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        let count = match result {
            mc_application::commands::CommandResult::MailboxMarkedRead { count } => count,
            _ => {
                return Err(Status::internal(
                    "unexpected command result for MarkMailboxRead",
                ));
            }
        };

        Ok(Response::new(MarkMailboxReadResponse {
            marked_count: count,
        }))
    }

    async fn change_labels(
        &self,
        request: Request<ChangeLabelsRequest>,
    ) -> Result<Response<BulkChangeResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .command_bus
            .dispatch(Command::ChangeLabels {
                message_ids: parse_message_ids(&req.message_ids)?,
                add: parse_mailbox_ids(&req.add_mailbox_ids)?,
                remove: parse_mailbox_ids(&req.remove_mailbox_ids)?,
            })
            .await
            .map_err(application_error_to_status)?;
        let (succeeded, failed) = bulk_counts(result)?;
        Ok(Response::new(BulkChangeResponse { succeeded, failed }))
    }

    async fn archive_messages(
        &self,
        request: Request<ArchiveMessagesRequest>,
    ) -> Result<Response<BulkChangeResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .command_bus
            .dispatch(Command::ArchiveMessages {
                message_ids: parse_message_ids(&req.message_ids)?,
            })
            .await
            .map_err(application_error_to_status)?;
        let (succeeded, failed) = bulk_counts(result)?;
        Ok(Response::new(BulkChangeResponse { succeeded, failed }))
    }

    async fn modify_tags(
        &self,
        request: Request<ModifyTagsRequest>,
    ) -> Result<Response<SetFlagsResponse>, Status> {
        let req = request.into_inner();
        for tag in req.add.iter().chain(&req.remove) {
            validate_tag(tag)?;
        }
        let message_id = parse_message_ids(std::slice::from_ref(&req.message_id))?[0];
        let tags = |names: Vec<String>| names.into_iter().map(Flag::Custom).collect();
        self.command_bus
            .dispatch(Command::SetMessageFlags {
                message_id,
                flags_to_add: tags(req.add),
                flags_to_remove: tags(req.remove),
            })
            .await
            .map_err(application_error_to_status)?;
        let updated = self.load_message(message_id).await?;
        Ok(Response::new(SetFlagsResponse {
            message: Some(domain_message_to_summary(&updated)),
        }))
    }

    async fn move_message(
        &self,
        request: Request<MoveMessageRequest>,
    ) -> Result<Response<MoveMessageResponse>, Status> {
        let req = request.into_inner();

        let (message, from_mailbox) = self
            .message_in_mailbox(&req.message_id, &req.from_mailbox_id)
            .await?;

        let to_mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.to_mailbox_id)
                .map_err(|e| Status::invalid_argument(format!("invalid to_mailbox_id: {e}")))?,
        );

        let to_mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox {
                mailbox_id: to_mailbox_id,
            })
            .await
            .map_err(application_error_to_status)?
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("target mailbox not found"))?;

        let message_id = message.id;

        let command = Command::MoveMessage {
            message_id: message.id,
            from_mailbox_id: from_mailbox.id,
            to_mailbox_id: to_mailbox.id,
            to_mailbox_path: to_mailbox.path,
        };

        self.command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        // Re-query for updated state
        let updated = self
            .query_bus
            .dispatch(Query::GetMessage { message_id })
            .await
            .map_err(application_error_to_status)?
            .into_message()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("message not found after move"))?;

        Ok(Response::new(MoveMessageResponse {
            message: Some(domain_message_to_summary(&updated)),
        }))
    }

    async fn delete_message(
        &self,
        request: Request<DeleteMessageRequest>,
    ) -> Result<Response<DeleteMessageResponse>, Status> {
        let req = request.into_inner();

        let (message, mailbox) = self
            .message_in_mailbox(&req.message_id, &req.mailbox_id)
            .await?;

        let command = Command::DeleteMessage {
            message_id: message.id,
            mailbox_id: mailbox.id,
            permanently: req.permanently,
        };

        self.command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        Ok(Response::new(DeleteMessageResponse { success: true }))
    }

    async fn delete_messages(
        &self,
        request: Request<DeleteMessagesRequest>,
    ) -> Result<Response<BulkChangeResponse>, Status> {
        let req = request.into_inner();
        let messages = parse_message_refs(&req.messages)?;

        let result = self
            .command_bus
            .dispatch(Command::DeleteMessages {
                messages,
                permanently: req.permanently,
            })
            .await
            .map_err(application_error_to_status)?;

        let (succeeded, failed) = bulk_counts(result)?;
        Ok(Response::new(BulkChangeResponse { succeeded, failed }))
    }

    async fn move_messages(
        &self,
        request: Request<MoveMessagesRequest>,
    ) -> Result<Response<BulkChangeResponse>, Status> {
        let req = request.into_inner();
        let messages = parse_message_refs(&req.messages)?;

        let to_mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.to_mailbox_id)
                .map_err(|e| Status::invalid_argument(format!("invalid to_mailbox_id: {e}")))?,
        );
        let to_mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox {
                mailbox_id: to_mailbox_id,
            })
            .await
            .map_err(application_error_to_status)?
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("target mailbox not found"))?;

        let result = self
            .command_bus
            .dispatch(Command::MoveMessages {
                messages,
                to_mailbox_id: to_mailbox.id,
                to_mailbox_path: to_mailbox.path,
            })
            .await
            .map_err(application_error_to_status)?;

        let (succeeded, failed) = bulk_counts(result)?;
        Ok(Response::new(BulkChangeResponse { succeeded, failed }))
    }

    async fn prepare_compose(
        &self,
        request: Request<PrepareComposeRequest>,
    ) -> Result<Response<ComposeView>, Status> {
        let req = request.into_inner();
        let parse = |field: &str, id: Option<String>| -> Result<Option<uuid::Uuid>, Status> {
            id.map(|id| {
                uuid::Uuid::parse_str(&id)
                    .map_err(|e| Status::invalid_argument(format!("invalid {field}: {e}")))
            })
            .transpose()
        };
        let command = Command::PrepareCompose {
            mode: proto_compose_mode(&req.mode).map_err(Status::invalid_argument)?,
            message_id: parse("message_id", req.message_id)?.map(MessageId::from_uuid),
            account_id: parse("account_id", req.account_id)?.map(AccountId::from_uuid),
        };
        match self
            .command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?
        {
            CommandResult::ComposePrepared(view) => Ok(Response::new(compose_view_to_proto(*view))),
            other => Err(Status::internal(format!(
                "unexpected command result: {other:?}"
            ))),
        }
    }

    async fn send_compose(
        &self,
        request: Request<ComposeInput>,
    ) -> Result<Response<SendComposeResponse>, Status> {
        let input =
            proto_compose_input_to_app(request.into_inner()).map_err(Status::invalid_argument)?;
        match self
            .command_bus
            .dispatch(Command::SendCompose { input })
            .await
            .map_err(application_error_to_status)?
        {
            CommandResult::MessageSent { message_id } => Ok(Response::new(SendComposeResponse {
                message_id: message_id.to_string(),
            })),
            other => Err(Status::internal(format!(
                "unexpected command result: {other:?}"
            ))),
        }
    }

    async fn save_draft(
        &self,
        request: Request<ComposeInput>,
    ) -> Result<Response<SaveDraftResponse>, Status> {
        let input =
            proto_compose_input_to_app(request.into_inner()).map_err(Status::invalid_argument)?;
        match self
            .command_bus
            .dispatch(Command::SaveDraft { input })
            .await
            .map_err(application_error_to_status)?
        {
            CommandResult::DraftSaved { message_id } => Ok(Response::new(SaveDraftResponse {
                draft_id: message_id.to_string(),
            })),
            other => Err(Status::internal(format!(
                "unexpected command result: {other:?}"
            ))),
        }
    }

    async fn discard_draft(
        &self,
        request: Request<DiscardDraftRequest>,
    ) -> Result<Response<DiscardDraftResponse>, Status> {
        let message_id = MessageId::from_uuid(
            uuid::Uuid::parse_str(&request.into_inner().draft_id)
                .map_err(|e| Status::invalid_argument(format!("invalid draft_id: {e}")))?,
        );
        self.command_bus
            .dispatch(Command::DiscardDraft { message_id })
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(DiscardDraftResponse {}))
    }

    type WatchMailboxStream = ReceiverStream<Result<MailboxEvent, Status>>;

    async fn watch_mailbox(
        &self,
        request: Request<WatchMailboxRequest>,
    ) -> Result<Response<Self::WatchMailboxStream>, Status> {
        let req = request.into_inner();

        let mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.mailbox_id)
                .map_err(|e| Status::invalid_argument(format!("invalid mailbox_id: {e}")))?,
        );

        // Resolve mailbox to get account_id and mailbox_path.
        let mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox { mailbox_id })
            .await
            .map_err(application_error_to_status)?
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("mailbox not found"))?;

        let account_id = mailbox.account_id;
        let mailbox_id_str = req.mailbox_id.clone();

        // Subscribe to the domain event bus. Filter events that belong to the
        // requested mailbox and map them to proto MailboxEvents.
        let mut subscriber = self.event_bus.subscribe();
        let query_bus = Arc::clone(&self.query_bus);
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            while let Some(envelope) = subscriber.recv().await {
                // Flags belong to the message, not a mailbox: ask where it is.
                if let DomainEvent::MessageFlagged { message_id, .. } = &envelope.payload
                    && !is_in_mailbox(query_bus.as_ref(), *message_id, mailbox_id).await
                {
                    continue;
                }
                let event_opt = domain_event_to_mailbox_event(
                    &envelope.payload,
                    &account_id,
                    &mailbox_id,
                    &mailbox_id_str,
                );
                if let Some(event) = event_opt
                    && tx.send(Ok(event)).await.is_err()
                {
                    break; // Client disconnected
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn expunged(message_id: &MessageId, mailbox_id_str: &str) -> MailboxEvent {
    MailboxEvent {
        mailbox_id: mailbox_id_str.to_owned(),
        event_type: Some(mailbox_event::EventType::MessageExpunged(
            MessageExpungedEvent {
                message_id: message_id.to_string(),
            },
        )),
    }
}

/// A failed lookup counts as "not there": a missed refresh beats one per
/// watcher for every flag change.
async fn is_in_mailbox<Q: QueryBus>(
    query_bus: &Q,
    message_id: MessageId,
    mailbox_id: MailboxId,
) -> bool {
    let Ok(result) = query_bus.dispatch(Query::GetMessage { message_id }).await else {
        return false;
    };
    result
        .into_message()
        .ok()
        .flatten()
        .is_some_and(|m| m.membership(mailbox_id).is_some())
}

/// Maps a domain event to a gRPC [`MailboxEvent`] if it belongs to the given mailbox.
/// Returns `None` if the event is irrelevant to the requested mailbox.
fn domain_event_to_mailbox_event(
    event: &DomainEvent,
    account_id: &AccountId,
    mailbox_id: &MailboxId,
    mailbox_id_str: &str,
) -> Option<MailboxEvent> {
    match event {
        DomainEvent::MessageReceived {
            message_id,
            account_id: evt_account,
            mailbox_id: evt_mailbox,
            remote_id,
            ..
        } if evt_account == account_id && evt_mailbox == mailbox_id => Some(MailboxEvent {
            mailbox_id: mailbox_id_str.to_owned(),
            event_type: Some(mailbox_event::EventType::NewMessage(NewMessageEvent {
                message_id: message_id.to_string(),
                remote_id: remote_id.to_string(),
            })),
        }),

        DomainEvent::MailboxCountsUpdated {
            mailbox_id: evt_mailbox,
            total_messages,
            unread_messages,
        } if evt_mailbox == mailbox_id => Some(MailboxEvent {
            mailbox_id: mailbox_id_str.to_owned(),
            event_type: Some(mailbox_event::EventType::CountsChanged(
                CountsChangedEvent {
                    total: *total_messages,
                    unseen: *unread_messages,
                },
            )),
        }),

        DomainEvent::MessageDeleted {
            message_id,
            mailbox_ids,
        } if mailbox_ids.contains(mailbox_id) => Some(expunged(message_id, mailbox_id_str)),

        // Gaining a mailbox is not announced, as before.
        DomainEvent::MessageMailboxesChanged {
            message_id,
            removed,
            ..
        } if removed.contains(mailbox_id) => Some(expunged(message_id, mailbox_id_str)),

        DomainEvent::MessageFlagged {
            message_id,
            flags_added,
            flags_removed,
        } => {
            let mut flags: Vec<i32> = flags_added
                .iter()
                .map(crate::grpc::mappers::domain_flag_to_proto)
                .collect();
            flags.extend(
                flags_removed
                    .iter()
                    .map(crate::grpc::mappers::domain_flag_to_proto),
            );
            Some(MailboxEvent {
                mailbox_id: mailbox_id_str.to_owned(),
                event_type: Some(mailbox_event::EventType::FlagsChanged(FlagsChangedEvent {
                    message_id: message_id.to_string(),
                    flags,
                })),
            })
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests;
