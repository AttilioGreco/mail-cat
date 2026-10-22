use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{
    application_error_to_status, domain_message_to_detail, domain_message_to_summary,
    compose_view_to_proto, paged_result_to_page_info, proto_compose_input_to_app,
    proto_compose_mode, proto_flags_to_domain, proto_to_domain_pagination,
};
use crate::grpc::proto::{
    BulkChangeResponse, CountsChangedEvent, DeleteMessageRequest, DeleteMessageResponse,
    DeleteMessagesRequest, MoveMessagesRequest, FlagsChangedEvent,
    GetMessageRequest, GetMessageResponse, GetThreadRequest, GetThreadResponse,
    ListConversationsRequest, ListConversationsResponse,
    ListMessagesRequest, ListMessagesResponse, MailboxEvent, MarkMailboxReadRequest,
    MarkMailboxReadResponse, MessageExpungedEvent,
    MoveMessageRequest, MoveMessageResponse, NewMessageEvent,
    RebuildConversationIndexRequest, RebuildConversationIndexResponse,
    ComposeInput, ComposeView, DiscardDraftRequest, DiscardDraftResponse, PrepareComposeRequest,
    SaveDraftResponse, SendComposeResponse, SetFlagsRequest, SetFlagsResponse,
    WatchMailboxRequest, mailbox_event, message_service_server::MessageService,
};
use mc_application::{
    InProcessEventBus,
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::Query,
};
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::Message;
use mc_domain::{AccountId, DomainEvent, MailboxId, MessageId};

/// Parse a bulk request's ids up-front: one malformed id means the caller sent
/// garbage, so reject the whole request rather than half-deleting a selection.
fn parse_message_ids(ids: &[String]) -> Result<Vec<MessageId>, Status> {
    ids.iter()
        .map(|id| {
            uuid::Uuid::parse_str(id)
                .map(MessageId::from_uuid)
                .map_err(|e| Status::invalid_argument(format!("invalid message_id {id}: {e}")))
        })
        .collect()
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
    pub fn new(
        command_bus: Arc<C>,
        query_bus: Arc<Q>,
        event_bus: Arc<InProcessEventBus>,
    ) -> Self {
        Self {
            command_bus,
            query_bus,
            event_bus,
        }
    }

    /// Query the read model to get message + its mailbox context for command enrichment.
    async fn get_message_context(
        &self,
        message_id_str: &str,
    ) -> Result<(Message, Mailbox), Status> {
        let message_id = MessageId::from_uuid(
            uuid::Uuid::parse_str(message_id_str)
                .map_err(|e| Status::invalid_argument(format!("invalid message_id: {e}")))?,
        );

        let message = self
            .query_bus
            .dispatch(Query::GetMessage { message_id })
            .await
            .map_err(application_error_to_status)?
            .into_message()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("message not found"))?;

        let mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox {
                mailbox_id: message.mailbox_id,
            })
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
                    date: Some(crate::grpc::mappers::datetime_to_timestamp(&summary.received_at)),
                    ..Default::default()
                };
                crate::grpc::proto::MessageSummary {
                    id: summary.id.to_string(),
                    account_id: summary.account_id.to_string(),
                    mailbox_id: summary.mailbox_id.to_string(),
                    envelope: Some(envelope),
                    flags: summary
                        .flags
                        .iter()
                        .map(crate::grpc::mappers::domain_flag_to_proto)
                        .collect(),
                    size_bytes: summary.size_bytes,
                    has_attachments: summary.has_attachments,
                    received_at: Some(crate::grpc::mappers::datetime_to_timestamp(
                        &summary.received_at,
                    )),
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

        let (message, mailbox) = self.get_message_context(&req.message_id).await?;

        // The background backfill worker drains newest-first, so a just-arrived
        // message can still be body-less when the user clicks it — which renders
        // as a blank pane. Opening a message is the strongest possible "I want
        // this one now" signal, so jump the queue and fetch it inline. Failure
        // (offline, provider hiccup) falls through to the empty body rather than
        // erroring the whole read: the backfill worker will retry.
        // A message without a known UID still goes: the handler finds it by
        // Message-ID.
        let message = if message.body_text.is_none() && message.body_html.is_none() {
            let message_id = message.id;
            match self
                .command_bus
                .dispatch(Command::FetchMessageBody {
                    message_id,
                    mailbox_path: mailbox.path.clone(),
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

        let query = Query::ListConversations { account_id, mailbox_id, pagination };
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
                thread_id: c.thread_id.value().to_string(),
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

        Ok(Response::new(RebuildConversationIndexResponse { events_replayed }))
    }

    async fn set_flags(
        &self,
        request: Request<SetFlagsRequest>,
    ) -> Result<Response<SetFlagsResponse>, Status> {
        let req = request.into_inner();

        let (message, mailbox) = self.get_message_context(&req.message_id).await?;

        let flags_to_add = proto_flags_to_domain(&req.flags_to_add);
        let flags_to_remove = proto_flags_to_domain(&req.flags_to_remove);

        let message_id = message.id;

        let command = Command::SetMessageFlags {
            message_id: message.id,
            mailbox_path: mailbox.path,
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

        Ok(Response::new(MarkMailboxReadResponse { marked_count: count }))
    }

    async fn move_message(
        &self,
        request: Request<MoveMessageRequest>,
    ) -> Result<Response<MoveMessageResponse>, Status> {
        let req = request.into_inner();

        let (message, from_mailbox) = self.get_message_context(&req.message_id).await?;

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
            from_mailbox_path: from_mailbox.path,
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

        let (message, mailbox) = self.get_message_context(&req.message_id).await?;

        let command = Command::DeleteMessage {
            message_id: message.id,
            mailbox_id: mailbox.id,
            mailbox_path: mailbox.path,
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
        let message_ids = parse_message_ids(&req.message_ids)?;

        let result = self
            .command_bus
            .dispatch(Command::DeleteMessages {
                message_ids,
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
        let message_ids = parse_message_ids(&req.message_ids)?;

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
                message_ids,
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
        match self.command_bus.dispatch(command).await.map_err(application_error_to_status)? {
            CommandResult::ComposePrepared(view) => Ok(Response::new(compose_view_to_proto(*view))),
            other => Err(Status::internal(format!("unexpected command result: {other:?}"))),
        }
    }

    async fn send_compose(
        &self,
        request: Request<ComposeInput>,
    ) -> Result<Response<SendComposeResponse>, Status> {
        let input = proto_compose_input_to_app(request.into_inner()).map_err(Status::invalid_argument)?;
        match self
            .command_bus
            .dispatch(Command::SendCompose { input })
            .await
            .map_err(application_error_to_status)?
        {
            CommandResult::MessageSent { message_id } => Ok(Response::new(SendComposeResponse {
                message_id: message_id.to_string(),
            })),
            other => Err(Status::internal(format!("unexpected command result: {other:?}"))),
        }
    }

    async fn save_draft(
        &self,
        request: Request<ComposeInput>,
    ) -> Result<Response<SaveDraftResponse>, Status> {
        let input = proto_compose_input_to_app(request.into_inner()).map_err(Status::invalid_argument)?;
        match self
            .command_bus
            .dispatch(Command::SaveDraft { input })
            .await
            .map_err(application_error_to_status)?
        {
            CommandResult::DraftSaved { message_id } => Ok(Response::new(SaveDraftResponse {
                draft_id: message_id.to_string(),
            })),
            other => Err(Status::internal(format!("unexpected command result: {other:?}"))),
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
            .dispatch(Query::GetMailbox {
                mailbox_id,
            })
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
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            while let Some(envelope) = subscriber.recv().await {
                let event_opt = domain_event_to_mailbox_event(
                    &envelope.payload,
                    &account_id,
                    &mailbox_id,
                    &mailbox_id_str,
                );
                if let Some(event) = event_opt
                    && tx.send(Ok(event)).await.is_err() {
                        break; // Client disconnected
                    }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
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
            provider_uid,
            ..
        } if evt_account == account_id && evt_mailbox == mailbox_id => Some(MailboxEvent {
            mailbox_id: mailbox_id_str.to_owned(),
            event_type: Some(mailbox_event::EventType::NewMessage(NewMessageEvent {
                message_id: message_id.to_string(),
                provider_uid: provider_uid.value(),
            })),
        }),

        DomainEvent::MailboxCountsUpdated {
            mailbox_id: evt_mailbox,
            total_messages,
            unread_messages,
        } if evt_mailbox == mailbox_id => Some(MailboxEvent {
            mailbox_id: mailbox_id_str.to_owned(),
            event_type: Some(mailbox_event::EventType::CountsChanged(CountsChangedEvent {
                total: *total_messages,
                unseen: *unread_messages,
            })),
        }),

        DomainEvent::MessageDeleted {
            message_id,
            mailbox_id: evt_mailbox,
            ..
        } if evt_mailbox == mailbox_id => Some(MailboxEvent {
            mailbox_id: mailbox_id_str.to_owned(),
            event_type: Some(mailbox_event::EventType::MessageExpunged(
                MessageExpungedEvent {
                    message_id: message_id.to_string(),
                },
            )),
        }),

        DomainEvent::MessageFlagged {
            message_id,
            mailbox_id: evt_mailbox,
            flags_added,
            flags_removed,
        } if evt_mailbox == mailbox_id => {
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
mod tests {
    use super::*;

    use std::sync::Arc;
    use tokio::time::{Duration, timeout};
    use tokio_stream::StreamExt as _;
    use tonic::Request;

    use mc_application::{
        ApplicationError, EventPublisher, InProcessEventBus,
        bus::{CommandBus, QueryBus},
        commands::{Command, CommandResult},
        queries::{Query, QueryResult},
    };
    use mc_domain::{
        AccountId, AggregateType, DomainEvent, EventEnvelope, Flag, Mailbox, MailboxId,
        MailboxPath, MailboxType, MessageId, ProviderUid,
    };

    // ── Minimal mock buses ────────────────────────────────────────────────────

    struct NopCommandBus;

    #[async_trait::async_trait]
    impl CommandBus for NopCommandBus {
        async fn dispatch(&self, _: Command) -> Result<CommandResult, ApplicationError> {
            unreachable!()
        }
    }

    struct SingleMailboxQueryBus(Mailbox);

    #[async_trait::async_trait]
    impl QueryBus for SingleMailboxQueryBus {
        async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
            match query {
                Query::GetMailbox { .. } => Ok(QueryResult::Mailbox(Some(self.0.clone()))),
                _ => Err(ApplicationError::Internal("unexpected".into())),
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn fake_mailbox(account_id: AccountId, mailbox_id: MailboxId) -> Mailbox {
        Mailbox {
            id: mailbox_id,
            account_id,
            path: MailboxPath::inbox(),
            display_name: "INBOX".to_string(),
            mailbox_type: MailboxType::Inbox,
            flags: vec![],
            total_messages: 0,
            unread_messages: 0,
            uid_validity: None,
            uid_next: None,
        }
    }

    fn make_svc(
        bus: Arc<InProcessEventBus>,
        mailbox: Mailbox,
    ) -> MessageServiceImpl<NopCommandBus, SingleMailboxQueryBus> {
        MessageServiceImpl::new(
            Arc::new(NopCommandBus),
            Arc::new(SingleMailboxQueryBus(mailbox)),
            bus,
        )
    }

    fn envelope(event: DomainEvent) -> EventEnvelope {
        EventEnvelope::new("test".into(), AggregateType::Message, 1, event)
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// MessageReceived domain event reaches the gRPC stream as NewMessage.
    #[tokio::test]
    async fn new_message_reaches_watch_stream() {
        let bus = Arc::new(InProcessEventBus::new(16));
        let account_id = AccountId::new();
        let mailbox_id = MailboxId::new();
        let message_id = MessageId::new();

        let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
        let mut stream = svc
            .watch_mailbox(Request::new(WatchMailboxRequest {
                mailbox_id: mailbox_id.as_uuid().to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        bus.publish(envelope(DomainEvent::MessageReceived {
            message_id,
            account_id,
            mailbox_id,
            provider_uid: ProviderUid::new(42),
            envelope: mc_domain::Envelope {
                from: vec![], to: vec![], cc: vec![], bcc: vec![], reply_to: vec![],
                subject: "Hi".into(),
                date: chrono::Utc::now(),
            },
            flags: vec![],
            size_bytes: 0,
            has_attachments: false,
            message_rfc_id: None,
            in_reply_to: None,
            references: vec![],
            received_at: chrono::Utc::now(),
            restored: false,
        }));

        let event = timeout(Duration::from_secs(1), stream.next())
            .await.expect("timeout").expect("ended").expect("error");

        assert_eq!(event.mailbox_id, mailbox_id.as_uuid().to_string());
        assert!(matches!(
            event.event_type,
            Some(mailbox_event::EventType::NewMessage(ref n)) if n.provider_uid == 42
        ));
    }

    /// Events from a different account are filtered out.
    #[tokio::test]
    async fn events_from_other_account_are_filtered() {
        let bus = Arc::new(InProcessEventBus::new(16));
        let account_id = AccountId::new();
        let other_account = AccountId::new();
        let mailbox_id = MailboxId::new();

        let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
        let mut stream = svc
            .watch_mailbox(Request::new(WatchMailboxRequest {
                mailbox_id: mailbox_id.as_uuid().to_string(),
            }))
            .await.unwrap().into_inner();

        // Wrong account → filtered
        bus.publish(envelope(DomainEvent::MessageReceived {
            message_id: MessageId::new(),
            account_id: other_account,
            mailbox_id,
            provider_uid: ProviderUid::new(1),
            envelope: mc_domain::Envelope { from: vec![], to: vec![], cc: vec![], bcc: vec![], reply_to: vec![], subject: "".into(), date: chrono::Utc::now() },
            flags: vec![], size_bytes: 0, has_attachments: false, message_rfc_id: None, in_reply_to: None, references: vec![], received_at: chrono::Utc::now(), restored: false,
        }));
        // Correct account → arrives
        bus.publish(envelope(DomainEvent::MessageReceived {
            message_id: MessageId::new(),
            account_id,
            mailbox_id,
            provider_uid: ProviderUid::new(2),
            envelope: mc_domain::Envelope { from: vec![], to: vec![], cc: vec![], bcc: vec![], reply_to: vec![], subject: "".into(), date: chrono::Utc::now() },
            flags: vec![], size_bytes: 0, has_attachments: false, message_rfc_id: None, in_reply_to: None, references: vec![], received_at: chrono::Utc::now(), restored: false,
        }));

        let event = timeout(Duration::from_secs(1), stream.next())
            .await.expect("timeout").unwrap().unwrap();
        match event.event_type {
            Some(mailbox_event::EventType::NewMessage(n)) => assert_eq!(n.provider_uid, 2),
            other => panic!("unexpected: {:?}", other),
        }
    }

    /// MailboxCountsUpdated reaches the stream as CountsChanged.
    #[tokio::test]
    async fn counts_updated_reaches_watch_stream() {
        let bus = Arc::new(InProcessEventBus::new(16));
        let account_id = AccountId::new();
        let mailbox_id = MailboxId::new();

        let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
        let mut stream = svc
            .watch_mailbox(Request::new(WatchMailboxRequest {
                mailbox_id: mailbox_id.as_uuid().to_string(),
            }))
            .await.unwrap().into_inner();

        bus.publish(EventEnvelope::new(
            "mbx".into(), AggregateType::Mailbox, 1,
            DomainEvent::MailboxCountsUpdated {
                mailbox_id,
                total_messages: 100,
                unread_messages: 5,
            },
        ));

        let event = timeout(Duration::from_secs(1), stream.next())
            .await.expect("timeout").unwrap().unwrap();
        assert!(matches!(
            event.event_type,
            Some(mailbox_event::EventType::CountsChanged(ref c)) if c.total == 100 && c.unseen == 5
        ));
    }

    /// A body-less message whose UID is unknown is still fetched on open:
    /// the handler finds it by Message-ID.
    #[tokio::test]
    async fn opening_a_message_without_uid_fetches_its_body() {
        struct Recording(std::sync::Mutex<Vec<Command>>);
        #[async_trait::async_trait]
        impl CommandBus for Recording {
            async fn dispatch(&self, command: Command) -> Result<CommandResult, ApplicationError> {
                self.0.lock().unwrap().push(command);
                Ok(CommandResult::MessageBodyFetched)
            }
        }
        struct MessageAndMailbox(mc_domain::Message, Mailbox);
        #[async_trait::async_trait]
        impl QueryBus for MessageAndMailbox {
            async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError> {
                match query {
                    Query::GetMessage { .. } => Ok(QueryResult::Message(Box::new(Some(self.0.clone())))),
                    Query::GetMailbox { .. } => Ok(QueryResult::Mailbox(Some(self.1.clone()))),
                    _ => Err(ApplicationError::Internal("unexpected".into())),
                }
            }
        }

        let (account_id, mailbox_id) = (AccountId::new(), MailboxId::new());
        let (mut message, _) = mc_domain::Message::receive(
            account_id,
            mailbox_id,
            ProviderUid::new(1),
            Some("<x@example.com>".into()),
            None,
            vec![],
            mc_domain::Envelope { from: vec![], to: vec![], cc: vec![], bcc: vec![], reply_to: vec![], subject: "".into(), date: chrono::Utc::now() },
            vec![],
            0,
            false,
            chrono::Utc::now(),
        );
        message.provider_uid = None;
        let commands = Arc::new(Recording(std::sync::Mutex::new(vec![])));
        let svc = MessageServiceImpl::new(
            Arc::clone(&commands),
            Arc::new(MessageAndMailbox(message.clone(), fake_mailbox(account_id, mailbox_id))),
            Arc::new(InProcessEventBus::new(16)),
        );

        svc.get_message(Request::new(GetMessageRequest { message_id: message.id.to_string() }))
            .await
            .unwrap();

        assert!(matches!(commands.0.lock().unwrap().as_slice(), [Command::FetchMessageBody { .. }]));
    }

    /// MessageFlagged reaches the stream as FlagsChanged.
    #[tokio::test]
    async fn flags_changed_reaches_watch_stream() {
        let bus = Arc::new(InProcessEventBus::new(16));
        let account_id = AccountId::new();
        let mailbox_id = MailboxId::new();

        let svc = make_svc(Arc::clone(&bus), fake_mailbox(account_id, mailbox_id));
        let mut stream = svc
            .watch_mailbox(Request::new(WatchMailboxRequest {
                mailbox_id: mailbox_id.as_uuid().to_string(),
            }))
            .await.unwrap().into_inner();

        bus.publish(envelope(DomainEvent::MessageFlagged {
            message_id: MessageId::new(),
            mailbox_id,
            flags_added: vec![Flag::Seen],
            flags_removed: vec![],
        }));

        let event = timeout(Duration::from_secs(1), stream.next())
            .await.expect("timeout").unwrap().unwrap();
        assert!(matches!(event.event_type, Some(mailbox_event::EventType::FlagsChanged(_))));
    }
}
