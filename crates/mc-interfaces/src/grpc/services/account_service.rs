use std::sync::Arc;
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;

use mc_application::{
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::Query,
};
use mc_domain::{AccountId, Identity};
use crate::grpc::proto::{
    self, account_service_server::AccountService, AddAccountRequest, AddAccountResponse,
    GetAccountRequest, GetAccountResponse, ListAccountsRequest, ListAccountsResponse,
    RemoveAccountRequest, RemoveAccountResponse, SyncAccountRequest, SyncProgressEvent,
    UpdateAccountRequest, UpdateAccountResponse,
};
use crate::grpc::mappers::{
    application_error_to_status, domain_account_summary_to_detail, domain_account_summary_to_proto,
    proto_connection_to_domain, proto_email_to_domain, proto_provider_type_to_domain,
};

/// Account service implementation with CQRS bus integration.
pub struct AccountServiceImpl<C: CommandBus + 'static, Q: QueryBus + 'static> {
    command_bus: Arc<C>,
    query_bus: Arc<Q>,
}

impl<C: CommandBus + 'static, Q: QueryBus + 'static> AccountServiceImpl<C, Q> {
    pub fn new(command_bus: Arc<C>, query_bus: Arc<Q>) -> Self {
        Self {
            command_bus,
            query_bus,
        }
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

        let provider_type = proto_provider_type_to_domain(req.provider_type)
            .map_err(Status::invalid_argument)?;

        let connection_config = proto_connection_to_domain(&req.connection_config.ok_or_else(|| {
            Status::invalid_argument("connection_config is required")
        })?)
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
            .map_err(|e| application_error_to_status(e))?;

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
            .map_err(|e| application_error_to_status(e))?;

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
            uuid::Uuid::parse_str(&req.account_id).map_err(|e| Status::invalid_argument(e.to_string()))?
        );

        let query = Query::GetAccount { account_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(|e| application_error_to_status(e))?;

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
            .map_err(|e| application_error_to_status(e))?;

        let accounts = result
            .into_accounts()
            .map_err(|e| Status::internal(e.to_string()))?;

        let summaries = accounts.iter().map(domain_account_summary_to_proto).collect();

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
            uuid::Uuid::parse_str(&req.account_id).map_err(|e| Status::invalid_argument(e.to_string()))?
        );

        let connection_config = if let Some(config) = req.connection_config {
            Some(
                proto_connection_to_domain(&config).map_err(Status::invalid_argument)?,
            )
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
            .map_err(|e| application_error_to_status(e))?;

        // Re-query the updated account
        let query = Query::GetAccount { account_id };
        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(|e| application_error_to_status(e))?;

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
            uuid::Uuid::parse_str(&req.account_id).map_err(|e| Status::invalid_argument(e.to_string()))?
        );

        let command = Command::RemoveAccount { account_id };

        self.command_bus
            .dispatch(command)
            .await
            .map_err(|e| application_error_to_status(e))?;

        Ok(Response::new(RemoveAccountResponse { success: true }))
    }

    type SyncAccountStream = ReceiverStream<Result<SyncProgressEvent, Status>>;

    async fn sync_account(
        &self,
        request: Request<SyncAccountRequest>,
    ) -> Result<Response<Self::SyncAccountStream>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id).map_err(|e| Status::invalid_argument(e.to_string()))?
        );

        let command = Command::SyncAccount { account_id };

        // Dispatch sync command (fire and forget for now)
        self.command_bus
            .dispatch(command)
            .await
            .map_err(|e| application_error_to_status(e))?;

        // TODO Day 13: Implement progress streaming with EventSubscriber
        // For now, return empty stream
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
