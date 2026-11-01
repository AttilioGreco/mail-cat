use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{application_error_to_status, domain_mailbox_to_proto};
use crate::grpc::proto::{
    CreateMailboxRequest, CreateMailboxResponse, DeleteMailboxRequest, DeleteMailboxResponse,
    GetMailboxRequest, GetMailboxResponse, ListMailboxesRequest, ListMailboxesResponse,
    RenameMailboxRequest, RenameMailboxResponse, SetMailboxSubscribedRequest,
    SetMailboxSubscribedResponse, mailbox_service_server::MailboxService,
};
use mc_application::{
    SyncTrigger,
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::Query,
};
use mc_domain::MailboxId;

/// Mailbox service implementation with QueryBus integration.
pub struct MailboxServiceImpl<C: CommandBus + 'static, Q: QueryBus + 'static> {
    command_bus: Arc<C>,
    query_bus: Arc<Q>,
    sync_trigger: Option<Arc<dyn SyncTrigger>>,
}

impl<C: CommandBus + 'static, Q: QueryBus + 'static> MailboxServiceImpl<C, Q> {
    pub fn new(command_bus: Arc<C>, query_bus: Arc<Q>) -> Self {
        Self {
            command_bus,
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
impl<C: CommandBus + 'static, Q: QueryBus + 'static> MailboxService for MailboxServiceImpl<C, Q> {
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
        request: Request<CreateMailboxRequest>,
    ) -> Result<Response<CreateMailboxResponse>, Status> {
        let req = request.into_inner();
        let account_id = mc_domain::AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );
        let result = self
            .command_bus
            .dispatch(Command::CreateLabel {
                account_id,
                name: req.path,
            })
            .await
            .map_err(application_error_to_status)?;
        let CommandResult::LabelCreated { mailbox_id } = result else {
            return Err(Status::internal("unexpected command result"));
        };
        let mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox { mailbox_id })
            .await
            .map_err(application_error_to_status)?
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CreateMailboxResponse {
            mailbox: mailbox.as_ref().map(domain_mailbox_to_proto),
        }))
    }

    async fn rename_mailbox(
        &self,
        request: Request<RenameMailboxRequest>,
    ) -> Result<Response<RenameMailboxResponse>, Status> {
        let req = request.into_inner();
        let mailbox_id = MailboxId::from_uuid(
            uuid::Uuid::parse_str(&req.mailbox_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );
        let result = self
            .command_bus
            .dispatch(Command::RenameMailbox {
                mailbox_id,
                new_name: req.new_path,
            })
            .await
            .map_err(application_error_to_status)?;
        let CommandResult::MailboxRenamed { renamed } = result else {
            return Err(Status::internal("unexpected command result"));
        };
        let mailbox = self
            .query_bus
            .dispatch(Query::GetMailbox { mailbox_id })
            .await
            .map_err(application_error_to_status)?
            .into_mailbox()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RenameMailboxResponse {
            mailbox: mailbox.as_ref().map(domain_mailbox_to_proto),
            renamed,
        }))
    }

    async fn delete_mailbox(
        &self,
        request: Request<DeleteMailboxRequest>,
    ) -> Result<Response<DeleteMailboxResponse>, Status> {
        let req = request.into_inner();
        let parse = |id: &str| {
            uuid::Uuid::parse_str(id)
                .map(MailboxId::from_uuid)
                .map_err(|e| Status::invalid_argument(e.to_string()))
        };
        let result = self
            .command_bus
            .dispatch(Command::DeleteMailbox {
                mailbox_id: parse(&req.mailbox_id)?,
                move_to: (!req.move_to_mailbox_id.is_empty())
                    .then(|| parse(&req.move_to_mailbox_id))
                    .transpose()?,
            })
            .await
            .map_err(application_error_to_status)?;
        let CommandResult::MailboxDeleted { moved } = result else {
            return Err(Status::internal("unexpected command result"));
        };
        Ok(Response::new(DeleteMailboxResponse {
            success: true,
            moved,
        }))
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
