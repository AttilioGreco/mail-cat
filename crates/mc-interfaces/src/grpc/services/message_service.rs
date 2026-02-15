use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{
    application_error_to_status, domain_message_to_detail, domain_message_to_summary,
    paged_result_to_page_info, proto_flags_to_domain, proto_send_request_to_outgoing,
    proto_to_domain_pagination,
};
use crate::grpc::proto::{
    DeleteMessageRequest, DeleteMessageResponse, GetMessageRequest, GetMessageResponse,
    GetThreadRequest, GetThreadResponse, ListMessagesRequest, ListMessagesResponse, MailboxEvent,
    MoveMessageRequest, MoveMessageResponse, SaveDraftRequest, SaveDraftResponse,
    SendMessageRequest, SendMessageResponse, SetFlagsRequest, SetFlagsResponse,
    WatchMailboxRequest, message_service_server::MessageService,
};
use mc_application::{
    bus::{CommandBus, QueryBus},
    commands::Command,
    queries::Query,
};
use mc_domain::model::mailbox::Mailbox;
use mc_domain::model::message::Message;
use mc_domain::{AccountId, MailboxId, MessageId};

/// Message service implementation with CQRS bus integration.
pub struct MessageServiceImpl<C: CommandBus + 'static, Q: QueryBus + 'static> {
    command_bus: Arc<C>,
    query_bus: Arc<Q>,
}

impl<C: CommandBus + 'static, Q: QueryBus + 'static> MessageServiceImpl<C, Q> {
    pub fn new(command_bus: Arc<C>, query_bus: Arc<Q>) -> Self {
        Self {
            command_bus,
            query_bus,
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
                mailbox_id: message.mailbox_id.clone(),
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
impl<C: CommandBus + 'static, Q: QueryBus + 'static> MessageService for MessageServiceImpl<C, Q> {
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
            .map(|summary| crate::grpc::proto::MessageSummary {
                id: summary.id.to_string(),
                account_id: summary.account_id.to_string(),
                mailbox_id: summary.mailbox_id.to_string(),
                envelope: None,
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

        let message_id = MessageId::from_uuid(
            uuid::Uuid::parse_str(&req.message_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let query = Query::GetMessage { message_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let message = result
            .into_message()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("message not found"))?;

        Ok(Response::new(GetMessageResponse {
            message: Some(domain_message_to_detail(&message)),
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

    async fn set_flags(
        &self,
        request: Request<SetFlagsRequest>,
    ) -> Result<Response<SetFlagsResponse>, Status> {
        let req = request.into_inner();

        let (message, mailbox) = self.get_message_context(&req.message_id).await?;

        let flags_to_add = proto_flags_to_domain(&req.flags_to_add);
        let flags_to_remove = proto_flags_to_domain(&req.flags_to_remove);

        let message_id = message.id.clone();

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

        let message_id = message.id.clone();

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

    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(format!("invalid account_id: {e}")))?,
        );

        let outgoing = proto_send_request_to_outgoing(&req).map_err(Status::invalid_argument)?;

        let command = Command::SendMessage {
            account_id,
            outgoing,
        };

        let result = self
            .command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        let message_id = match result {
            mc_application::commands::CommandResult::MessageSent { message_id } => message_id,
            _ => return Err(Status::internal("unexpected command result")),
        };

        Ok(Response::new(SendMessageResponse {
            message_id: message_id.to_string(),
        }))
    }

    async fn save_draft(
        &self,
        _request: Request<SaveDraftRequest>,
    ) -> Result<Response<SaveDraftResponse>, Status> {
        Err(Status::unimplemented("save_draft not yet implemented"))
    }

    type WatchMailboxStream = ReceiverStream<Result<MailboxEvent, Status>>;

    async fn watch_mailbox(
        &self,
        _request: Request<WatchMailboxRequest>,
    ) -> Result<Response<Self::WatchMailboxStream>, Status> {
        Err(Status::unimplemented("watch_mailbox not yet implemented"))
    }
}
