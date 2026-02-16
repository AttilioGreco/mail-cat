use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{
    application_error_to_status, domain_account_summary_to_detail, domain_account_summary_to_proto,
    proto_connection_to_domain, proto_email_to_domain, proto_provider_type_to_domain,
};
use crate::grpc::proto::{
    self, AddAccountRequest, AddAccountResponse, GetAccountRequest, GetAccountResponse,
    ListAccountsRequest, ListAccountsResponse, RemoveAccountRequest, RemoveAccountResponse,
    SyncAccountRequest, UpdateAccountRequest, UpdateAccountResponse,
    account_service_server::AccountService,
};
use mc_application::{
    SyncState, SyncTrigger,
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::Query,
};
use mc_domain::{AccountId, Identity};

/// Account service implementation with CQRS bus integration.
pub struct AccountServiceImpl<C: CommandBus + 'static, Q: QueryBus + 'static> {
    command_bus: Arc<C>,
    query_bus: Arc<Q>,
    sync_trigger: Option<Arc<dyn SyncTrigger>>,
}

impl<C: CommandBus + 'static, Q: QueryBus + 'static> AccountServiceImpl<C, Q> {
    pub fn new(command_bus: Arc<C>, query_bus: Arc<Q>) -> Self {
        Self {
            command_bus,
            query_bus,
            sync_trigger: None,
        }
    }

    pub fn with_sync_trigger(mut self, trigger: Arc<dyn SyncTrigger>) -> Self {
        self.sync_trigger = Some(trigger);
        self
    }
}

#[tonic::async_trait]
impl<C: CommandBus + 'static, Q: QueryBus + 'static> AccountService for AccountServiceImpl<C, Q> {
    async fn add_account(
        &self,
        request: Request<AddAccountRequest>,
    ) -> Result<Response<AddAccountResponse>, Status> {
        let req = request.into_inner();

        // Parse proto inputs to domain types
        let email_addr = proto::EmailAddress {
            email: req.email.clone(),
            name: None,
        };
        let email = proto_email_to_domain(&email_addr)
            .map_err(Status::invalid_argument)?
            .email;

        let provider_type =
            proto_provider_type_to_domain(req.provider_type).map_err(Status::invalid_argument)?;

        let connection_config = proto_connection_to_domain(
            &req.connection_config
                .ok_or_else(|| Status::invalid_argument("connection_config is required"))?,
        )
        .map_err(Status::invalid_argument)?;

        // Create default identity
        let identity = Identity {
            display_name: req.name.clone(),
            email: email.clone(),
            signature_html: None,
            signature_text: None,
        };

        // Dispatch command
        let command = Command::AddAccount {
            name: req.name,
            email,
            provider_type,
            connection_config,
            identity,
        };

        let result = self
            .command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        // Extract account_id from result
        let account_id = match result {
            CommandResult::AccountAdded { account_id } => account_id,
            _ => return Err(Status::internal("unexpected command result")),
        };

        // Query the newly created account
        let query = Query::GetAccount { account_id };
        let query_result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let account = query_result
            .into_account()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("account not found after creation"))?;

        Ok(Response::new(AddAccountResponse {
            account: Some(domain_account_summary_to_detail(&account)),
        }))
    }

    async fn get_account(
        &self,
        request: Request<GetAccountRequest>,
    ) -> Result<Response<GetAccountResponse>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let query = Query::GetAccount { account_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let account = result
            .into_account()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("account not found"))?;

        Ok(Response::new(GetAccountResponse {
            account: Some(domain_account_summary_to_detail(&account)),
        }))
    }

    async fn list_accounts(
        &self,
        _request: Request<ListAccountsRequest>,
    ) -> Result<Response<ListAccountsResponse>, Status> {
        let query = Query::ListAccounts;
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let accounts = result
            .into_accounts()
            .map_err(|e| Status::internal(e.to_string()))?;

        let summaries = accounts
            .iter()
            .map(domain_account_summary_to_proto)
            .collect();

        Ok(Response::new(ListAccountsResponse {
            accounts: summaries,
            page_info: None, // Not paginated yet
        }))
    }

    async fn update_account(
        &self,
        request: Request<UpdateAccountRequest>,
    ) -> Result<Response<UpdateAccountResponse>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let connection_config = if let Some(config) = req.connection_config {
            Some(proto_connection_to_domain(&config).map_err(Status::invalid_argument)?)
        } else {
            None
        };

        let command = Command::UpdateAccount {
            account_id: account_id.clone(),
            name: req.name,
            connection_config,
        };

        self.command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        // Re-query the updated account
        let query = Query::GetAccount { account_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let account = result
            .into_account()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("account not found after update"))?;

        Ok(Response::new(UpdateAccountResponse {
            account: Some(domain_account_summary_to_detail(&account)),
        }))
    }

    async fn remove_account(
        &self,
        request: Request<RemoveAccountRequest>,
    ) -> Result<Response<RemoveAccountResponse>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let command = Command::RemoveAccount { account_id };

        self.command_bus
            .dispatch(command)
            .await
            .map_err(application_error_to_status)?;

        Ok(Response::new(RemoveAccountResponse { success: true }))
    }

    type SyncAccountStream = ReceiverStream<Result<proto::SyncProgressEvent, Status>>;

    async fn sync_account(
        &self,
        request: Request<SyncAccountRequest>,
    ) -> Result<Response<Self::SyncAccountStream>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let sync_trigger = self
            .sync_trigger
            .clone()
            .ok_or_else(|| Status::unavailable("sync service not configured"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            match sync_trigger.trigger_sync(&account_id).await {
                Ok(events) => {
                    for event in events {
                        let proto_event = proto::SyncProgressEvent {
                            account_id: account_id.to_string(),
                            phase: sync_state_to_phase(&event.state),
                            mailbox_name: None,
                            processed: event.processed,
                            total: Some(event.total),
                            message: Some(event.message),
                        };
                        if tx.send(Ok(proto_event)).await.is_err() {
                            break; // client disconnected
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn sync_state_to_phase(state: &SyncState) -> i32 {
    match state {
        SyncState::Idle => proto::SyncPhase::Unspecified as i32,
        SyncState::ListingMailboxes => proto::SyncPhase::DiscoveringMailboxes as i32,
        SyncState::FetchingEnvelopes { .. } => proto::SyncPhase::FetchingEnvelopes as i32,
        SyncState::FetchingMessageBody { .. } => proto::SyncPhase::FetchingBodies as i32,
        SyncState::Completed => proto::SyncPhase::Complete as i32,
        SyncState::Failed => proto::SyncPhase::Error as i32,
    }
}
