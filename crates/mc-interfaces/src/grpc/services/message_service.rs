use std::sync::Arc;
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;

use mc_application::{
    bus::{CommandBus, QueryBus},
    queries::Query,
};
use mc_domain::{MailboxId, MessageId};
use crate::grpc::proto::{
    message_service_server::MessageService, DeleteMessageRequest, DeleteMessageResponse,
    GetMessageRequest, GetMessageResponse, GetThreadRequest, GetThreadResponse,
    ListMessagesRequest, ListMessagesResponse, MailboxEvent, MoveMessageRequest,
    MoveMessageResponse, SaveDraftRequest, SaveDraftResponse, SendMessageRequest,
    SendMessageResponse, SetFlagsRequest, SetFlagsResponse, WatchMailboxRequest,
};
use crate::grpc::mappers::{
    application_error_to_status, domain_message_to_detail,
    paged_result_to_page_info, proto_to_domain_pagination,
};

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
}

#[tonic::async_trait]
impl<C: CommandBus + 'static, Q: QueryBus + 'static> MessageService for MessageServiceImpl<C, Q> {
    async fn list_messages(
        &self,
        request: Request<ListMessagesRequest>,
    ) -> Result<Response<ListMessagesResponse>, Status> {
        let req = request.into_inner();

        let mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.mailbox_id).map_err(|e| Status::invalid_argument(e.to_string()))?
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

        let messages = paged.items.iter().map(|summary| {
            // Convert MessageSummary to proto MessageSummary
            // For now, we need a helper that works with MessageSummary, not Message
            // Let's use the existing mapper but we need to adapt it
            crate::grpc::proto::MessageSummary {
                id: summary.id.to_string(),
                account_id: summary.account_id.to_string(),
                mailbox_id: summary.mailbox_id.to_string(),
                envelope: None, // MessageSummary doesn't have full envelope
                flags: summary.flags.iter().map(|f| crate::grpc::mappers::domain_flag_to_proto(f)).collect(),
                size_bytes: summary.size_bytes,
                has_attachments: summary.has_attachments,
                received_at: Some(crate::grpc::mappers::datetime_to_timestamp(&summary.received_at)),
            }
        }).collect();

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
            uuid::Uuid::parse_str(&req.message_id).map_err(|e| Status::invalid_argument(e.to_string()))?
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
            uuid::Uuid::parse_str(&req.message_id).map_err(|e| Status::invalid_argument(e.to_string()))?
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

        Ok(Response::new(GetThreadResponse {
            messages: details,
        }))
    }

    async fn set_flags(
        &self,
        _request: Request<SetFlagsRequest>,
    ) -> Result<Response<SetFlagsResponse>, Status> {
        Err(Status::unimplemented("set_flags not yet implemented"))
    }

    async fn move_message(
        &self,
        _request: Request<MoveMessageRequest>,
    ) -> Result<Response<MoveMessageResponse>, Status> {
        Err(Status::unimplemented("move_message not yet implemented"))
    }

    async fn delete_message(
        &self,
        _request: Request<DeleteMessageRequest>,
    ) -> Result<Response<DeleteMessageResponse>, Status> {
        Err(Status::unimplemented("delete_message not yet implemented"))
    }

    async fn send_message(
        &self,
        _request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        Err(Status::unimplemented("send_message not yet implemented"))
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
