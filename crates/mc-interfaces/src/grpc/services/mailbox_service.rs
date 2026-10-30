use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{application_error_to_status, domain_mailbox_to_proto};
use crate::grpc::proto::{
    CreateMailboxRequest, CreateMailboxResponse, DeleteMailboxRequest, DeleteMailboxResponse,
    GetMailboxRequest, GetMailboxResponse, ListMailboxesRequest, ListMailboxesResponse,
    RenameMailboxRequest, RenameMailboxResponse, SetMailboxSubscribedRequest,
    SetMailboxSubscribedResponse, mailbox_service_server::MailboxService,
};
use mc_application::{SyncTrigger, bus::QueryBus, queries::Query};
use mc_domain::MailboxId;

/// Mailbox service implementation with QueryBus integration.
pub struct MailboxServiceImpl<Q: QueryBus + 'static> {
    query_bus: Arc<Q>,
    sync_trigger: Option<Arc<dyn SyncTrigger>>,
}

impl<Q: QueryBus + 'static> MailboxServiceImpl<Q> {
    pub fn new(query_bus: Arc<Q>) -> Self {
        Self {
            query_bus,
            sync_trigger: None,
        }
    }

    #[must_use]
    pub fn with_sync_trigger(mut self, trigger: Arc<dyn SyncTrigger>) -> Self {
        self.sync_trigger = Some(trigger);
        self
    }
}

#[tonic::async_trait]
impl<Q: QueryBus + 'static> MailboxService for MailboxServiceImpl<Q> {
    async fn list_mailboxes(
        &self,
        request: Request<ListMailboxesRequest>,
    ) -> Result<Response<ListMailboxesResponse>, Status> {
        let req = request.into_inner();

        let account_id = mc_domain::AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let query = Query::ListMailboxes { account_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let mailboxes = result
            .into_mailboxes()
            .map_err(|e| Status::internal(e.to_string()))?;

        let summaries = mailboxes.iter().map(domain_mailbox_to_proto).collect();

        Ok(Response::new(ListMailboxesResponse {
            mailboxes: summaries,
        }))
    }

    async fn get_mailbox(
        &self,
        request: Request<GetMailboxRequest>,
    ) -> Result<Response<GetMailboxResponse>, Status> {
        let req = request.into_inner();

        let mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.mailbox_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let query = Query::GetMailbox { mailbox_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let mailbox = result
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("mailbox not found"))?;

        Ok(Response::new(GetMailboxResponse {
            mailbox: Some(domain_mailbox_to_proto(&mailbox)),
        }))
    }

    async fn create_mailbox(
        &self,
        _request: Request<CreateMailboxRequest>,
    ) -> Result<Response<CreateMailboxResponse>, Status> {
        // TODO: Implement when CreateMailbox command is added
        Err(Status::unimplemented("create_mailbox not yet implemented"))
    }

    async fn rename_mailbox(
        &self,
        _request: Request<RenameMailboxRequest>,
    ) -> Result<Response<RenameMailboxResponse>, Status> {
        // TODO: Implement when RenameMailbox command is added
        Err(Status::unimplemented("rename_mailbox not yet implemented"))
    }

    async fn delete_mailbox(
        &self,
        _request: Request<DeleteMailboxRequest>,
    ) -> Result<Response<DeleteMailboxResponse>, Status> {
        // TODO: Implement when DeleteMailbox command is added
        Err(Status::unimplemented("delete_mailbox not yet implemented"))
    }

    async fn set_mailbox_subscribed(
        &self,
        request: Request<SetMailboxSubscribedRequest>,
    ) -> Result<Response<SetMailboxSubscribedResponse>, Status> {
        let req = request.into_inner();
        let mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.mailbox_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );
        let trigger = self
            .sync_trigger
            .as_ref()
            .ok_or_else(|| Status::unavailable("sync service not configured"))?;
        let warning = trigger
            .set_mailbox_subscribed(&mailbox_id, req.subscribed)
            .await
            .map_err(application_error_to_status)?;

        Ok(Response::new(SetMailboxSubscribedResponse {
            warning: warning.unwrap_or_default(),
        }))
    }
}
