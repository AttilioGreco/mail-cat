use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{
    application_error_to_status, auth_status_to_proto, domain_account_summary_to_detail,
    domain_account_summary_to_proto, proto_connection_to_domain, proto_email_to_domain,
    proto_mailbox_model_to_domain, proto_oauth_provider_to_domain, proto_provider_type_to_domain,
};
use crate::grpc::proto::{
    self, AddAccountRequest, AddAccountResponse, GetAccountRequest, GetAccountResponse,
    ListAccountsRequest, ListAccountsResponse, RemoveAccountRequest, RemoveAccountResponse,
    StartOAuthLoginRequest, SyncAccountRequest, UpdateAccountRequest, UpdateAccountResponse,
    account_service_server::AccountService,
};
use mc_application::{
    AccountProvisioner, AuthGates, OAuthLoginProgress, OAuthLoginTrigger, ProvisionParams,
    SyncProgressEvent as AppSyncProgressEvent, SyncState, SyncTrigger,
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
    oauth_trigger: Option<Arc<dyn OAuthLoginTrigger>>,
    provisioner: Option<Arc<dyn AccountProvisioner>>,
    auth_gates: Option<Arc<AuthGates>>,
}

impl<C: CommandBus + 'static, Q: QueryBus + 'static> AccountServiceImpl<C, Q> {
    pub fn new(command_bus: Arc<C>, query_bus: Arc<Q>) -> Self {
        Self {
            command_bus,
            query_bus,
            sync_trigger: None,
            oauth_trigger: None,
            provisioner: None,
            auth_gates: None,
        }
    }

    pub fn with_sync_trigger(mut self, trigger: Arc<dyn SyncTrigger>) -> Self {
        self.sync_trigger = Some(trigger);
        self
    }

    pub fn with_oauth_trigger(mut self, trigger: Arc<dyn OAuthLoginTrigger>) -> Self {
        self.oauth_trigger = Some(trigger);
        self
    }

    pub fn with_provisioner(mut self, provisioner: Arc<dyn AccountProvisioner>) -> Self {
        self.provisioner = Some(provisioner);
        self
    }

    pub fn with_auth_gates(mut self, gates: Arc<AuthGates>) -> Self {
        self.auth_gates = Some(gates);
        self
    }

    fn auth_status(&self, account_id: &AccountId) -> Option<proto::AccountAuthStatus> {
        let gate = self.auth_gates.as_ref()?.get(account_id)?;
        Some(auth_status_to_proto(&gate.status()))
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

        // Declare the account where the next boot will find it, *before* creating
        // the aggregate: config.toml is what builds providers, so an aggregate
        // without a config entry can never sync. If the aggregate write fails
        // after this, boot reconciles by creating it from config.
        if let Some(provisioner) = &self.provisioner {
            match &connection_config.auth {
                mc_domain::AuthConfig::Plain { username, password } => {
                    provisioner
                        .provision(ProvisionParams {
                            name: req.name.clone(),
                            email: email.as_str().to_string(),
                            incoming: connection_config.incoming.clone(),
                            username: username.clone(),
                            password: password.clone(),
                            smtp: connection_config.smtp.clone(),
                        })
                        .await
                        .map_err(application_error_to_status)?;
                }
                // OAuth accounts are declared by StartOAuthLogin, which owns the
                // token exchange; app passwords have no provisioning path yet.
                _ => {
                    return Err(Status::invalid_argument(
                        "AddAccount supports plain-password credentials only — use StartOAuthLogin for OAuth accounts",
                    ));
                }
            }
        }

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
            .map(|account| proto::AccountSummary {
                auth: self.auth_status(&account.id),
                ..domain_account_summary_to_proto(account)
            })
            .collect();

        Ok(Response::new(ListAccountsResponse {
            accounts: summaries,
            page_info: None, // Not paginated yet
        }))
    }

    async fn set_account_mailbox_model(
        &self,
        request: Request<proto::SetAccountMailboxModelRequest>,
    ) -> Result<Response<proto::SetAccountMailboxModelResponse>, Status> {
        let req = request.into_inner();
        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );
        let model = proto_mailbox_model_to_domain(req.model).map_err(Status::invalid_argument)?;
        self.command_bus
            .dispatch(Command::SetAccountMailboxModel { account_id, model })
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(proto::SetAccountMailboxModelResponse {}))
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
            account_id,
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

        // Resolve the email before removing: the provisioner keys config.toml and
        // the keychain by address, and the read model won't have it afterwards.
        let email = self
            .query_bus
            .dispatch(Query::GetAccount { account_id })
            .await
            .map_err(application_error_to_status)?
            .into_account()
            .map_err(|e| Status::internal(e.to_string()))?
            .map(|a| a.email.as_str().to_string());

        self.command_bus
            .dispatch(Command::RemoveAccount { account_id })
            .await
            .map_err(application_error_to_status)?;

        // Without this the account reappears on the next restart, recreated from
        // config.toml — which is how "removed" accounts came back.
        if let (Some(provisioner), Some(email)) = (&self.provisioner, email) {
            provisioner
                .deprovision(&email)
                .await
                .map_err(application_error_to_status)?;
        }

        Ok(Response::new(RemoveAccountResponse { success: true }))
    }

    async fn get_account_stats(
        &self,
        request: Request<proto::GetAccountStatsRequest>,
    ) -> Result<Response<proto::GetAccountStatsResponse>, Status> {
        let req = request.into_inner();

        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
        );

        let stats = self
            .query_bus
            .dispatch(Query::GetAccountStats { account_id })
            .await
            .map_err(application_error_to_status)?
            .into_account_stats()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::GetAccountStatsResponse {
            total_messages: stats.total_messages,
            synced_messages: stats.synced_messages,
            pending_bodies: stats.pending_bodies,
        }))
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

        // A single-mailbox sync emits no progress events, so the stream's end
        // (tx dropped when the task finishes) is the completion signal.
        if let Some(path) = req.mailbox_path {
            tokio::spawn(async move {
                if let Err(e) = sync_trigger.trigger_mailbox_sync(&account_id, &path).await {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            });
            return Ok(Response::new(ReceiverStream::new(rx)));
        }

        // Subscribe *before* triggering, or the first events race the subscription.
        let progress_rx = sync_trigger.subscribe_progress();

        let forward_tx = tx.clone();
        tokio::spawn(forward_progress(
            progress_rx,
            forward_tx,
            Some(account_id),
            true,
        ));

        tokio::spawn(async move {
            if let Err(e) = sync_trigger.trigger_sync(&account_id).await {
                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type WatchSyncProgressStream = ReceiverStream<Result<proto::SyncProgressEvent, Status>>;

    async fn watch_sync_progress(
        &self,
        request: Request<proto::WatchSyncProgressRequest>,
    ) -> Result<Response<Self::WatchSyncProgressStream>, Status> {
        let req = request.into_inner();

        let account_id = if req.account_id.is_empty() {
            None
        } else {
            Some(AccountId::from_uuid(
                uuid::Uuid::parse_str(&req.account_id)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            ))
        };

        let sync_trigger = self
            .sync_trigger
            .clone()
            .ok_or_else(|| Status::unavailable("sync service not configured"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        // Never stops at Completed: the server syncs again on its own schedule and
        // the watcher is expected to stay open for the life of the client.
        tokio::spawn(forward_progress(
            sync_trigger.subscribe_progress(),
            tx,
            account_id,
            false,
        ));

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type StartOAuthLoginStream = ReceiverStream<Result<proto::OAuthLoginEvent, Status>>;

    async fn start_o_auth_login(
        &self,
        request: Request<StartOAuthLoginRequest>,
    ) -> Result<Response<Self::StartOAuthLoginStream>, Status> {
        let req = request.into_inner();
        let provider =
            proto_oauth_provider_to_domain(req.provider).map_err(Status::invalid_argument)?;

        let trigger = self
            .oauth_trigger
            .clone()
            .ok_or_else(|| Status::unavailable("OAuth login not configured"))?;

        let mut progress_rx = trigger.start_login(req.name, req.email, provider);
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                let proto_event = oauth_login_progress_to_proto(event);
                if tx.send(Ok(proto_event)).await.is_err() {
                    break; // client disconnected
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ReauthAccountStream = ReceiverStream<Result<proto::OAuthLoginEvent, Status>>;

    async fn reauth_account(
        &self,
        request: Request<proto::ReauthAccountRequest>,
    ) -> Result<Response<Self::ReauthAccountStream>, Status> {
        let req = request.into_inner();
        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(format!("invalid account_id: {e}")))?,
        );

        let trigger = self
            .oauth_trigger
            .clone()
            .ok_or_else(|| Status::unavailable("OAuth login not configured"))?;

        let mut progress_rx = trigger.start_reauth(account_id);
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            while let Some(event) = progress_rx.recv().await {
                if tx
                    .send(Ok(oauth_login_progress_to_proto(event)))
                    .await
                    .is_err()
                {
                    break; // client disconnected
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
    async fn set_account_password(
        &self,
        request: Request<proto::SetAccountPasswordRequest>,
    ) -> Result<Response<proto::SetAccountPasswordResponse>, Status> {
        let req = request.into_inner();
        if req.password.is_empty() {
            return Err(Status::invalid_argument("password is required"));
        }
        let account_id = AccountId::from_uuid(
            uuid::Uuid::parse_str(&req.account_id)
                .map_err(|e| Status::invalid_argument(format!("invalid account_id: {e}")))?,
        );
        let provisioner = self
            .provisioner
            .as_ref()
            .ok_or_else(|| Status::unavailable("account provisioning not configured"))?;

        // The keychain is keyed by address, not by account id.
        let email = self
            .query_bus
            .dispatch(Query::GetAccount { account_id })
            .await
            .map_err(application_error_to_status)?
            .into_account()
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("account not found"))?
            .email;

        provisioner
            .set_password(email.as_str(), &req.password)
            .await
            .map_err(application_error_to_status)?;
        // The live gate takes the new password too: a rejected account resumes now.
        if let Some(gate) = self.auth_gates.as_ref().and_then(|g| g.get(&account_id)) {
            gate.set_password(req.password);
        }

        Ok(Response::new(proto::SetAccountPasswordResponse {}))
    }

    type WatchAuthStatusStream = ReceiverStream<Result<proto::AccountAuthStatus, Status>>;

    async fn watch_auth_status(
        &self,
        _request: Request<proto::WatchAuthStatusRequest>,
    ) -> Result<Response<Self::WatchAuthStatusStream>, Status> {
        let gates = self
            .auth_gates
            .clone()
            .ok_or_else(|| Status::unavailable("auth gates not configured"))?;
        // Subscribe before the snapshot so no change falls between the two.
        let mut changes = gates.subscribe();
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            let mut pending = gates.statuses();
            loop {
                for status in &pending {
                    if tx.send(Ok(auth_status_to_proto(status))).await.is_err() {
                        return; // client disconnected
                    }
                }
                pending = match changes.recv().await {
                    Ok(id) => gates.get(&id).map(|g| vec![g.status()]).unwrap_or_default(),
                    // Missed changes: resend everyone, the latest status supersedes them.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => gates.statuses(),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Pump broadcast progress events into one client's stream.
///
/// `account_filter` of `None` forwards every account. `stop_when_done` ends the
/// stream at the first terminal event, which is what the request/response-shaped
/// SyncAccount call wants; a long-lived watcher keeps going.
async fn forward_progress(
    mut progress_rx: tokio::sync::broadcast::Receiver<AppSyncProgressEvent>,
    tx: tokio::sync::mpsc::Sender<Result<proto::SyncProgressEvent, Status>>,
    account_filter: Option<AccountId>,
    stop_when_done: bool,
) {
    loop {
        match progress_rx.recv().await {
            Ok(event) => {
                if account_filter.is_some_and(|id| id != event.account_id) {
                    continue;
                }
                let terminal = matches!(event.state, SyncState::Completed | SyncState::Failed);
                let proto_event = proto::SyncProgressEvent {
                    account_id: event.account_id.to_string(),
                    phase: sync_state_to_phase(&event.state),
                    mailbox_name: None,
                    processed: event.processed,
                    total: Some(event.total),
                    message: Some(event.message),
                };
                if tx.send(Ok(proto_event)).await.is_err() {
                    return; // client disconnected
                }
                if terminal && stop_when_done {
                    return;
                }
            }
            // Lagged: this client was too slow and missed some percentages. The
            // next event supersedes them, so keep going instead of failing.
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

fn oauth_login_progress_to_proto(event: OAuthLoginProgress) -> proto::OAuthLoginEvent {
    use proto::o_auth_login_event::Event;
    let event = match event {
        OAuthLoginProgress::AwaitingBrowser { authorize_url } => {
            Event::AwaitingBrowser(proto::AwaitingBrowser { authorize_url })
        }
        OAuthLoginProgress::Succeeded { email } => {
            Event::Succeeded(proto::OAuthLoginSucceeded { email })
        }
        OAuthLoginProgress::Failed { message } => {
            Event::Failed(proto::OAuthLoginFailed { message })
        }
    };
    proto::OAuthLoginEvent { event: Some(event) }
}

fn sync_state_to_phase(state: &SyncState) -> i32 {
    match state {
        SyncState::Idle => proto::SyncPhase::Unspecified as i32,
        SyncState::ListingMailboxes => proto::SyncPhase::DiscoveringMailboxes as i32,
        SyncState::FetchingEnvelopes { .. } => proto::SyncPhase::FetchingEnvelopes as i32,
        SyncState::FetchingMessageBody => proto::SyncPhase::FetchingBodies as i32,
        SyncState::Completed => proto::SyncPhase::Complete as i32,
        SyncState::Failed => proto::SyncPhase::Error as i32,
    }
}
