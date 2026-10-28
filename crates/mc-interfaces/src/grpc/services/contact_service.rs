use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::application_error_to_status;
use crate::grpc::proto::{
    BackfillContactsRequest, BackfillContactsResponse, ContactDuplicatePair,
    ContactSearchHit as ProtoContactSearchHit, FindContactDuplicatesRequest,
    FindContactDuplicatesResponse, MergeContactsRequest, MergeContactsResponse,
    RebuildContactIndexRequest, RebuildContactIndexResponse, SearchContactsRequest,
    SearchContactsResponse, contact_service_server::ContactService,
};
use mc_application::{
    bus::{CommandBus, QueryBus},
    commands::Command,
    queries::Query,
};
use mc_domain::{AccountId, ContactId};

/// Contact service implementation — exposes address-book search, duplicate
/// detection, and merge over gRPC.
pub struct ContactServiceImpl<Q: QueryBus + 'static, C: CommandBus + 'static> {
    query_bus: Arc<Q>,
    command_bus: Arc<C>,
}

impl<Q: QueryBus + 'static, C: CommandBus + 'static> ContactServiceImpl<Q, C> {
    pub fn new(query_bus: Arc<Q>, command_bus: Arc<C>) -> Self {
        Self {
            query_bus,
            command_bus,
        }
    }
}

#[tonic::async_trait]
impl<Q: QueryBus + 'static, C: CommandBus + 'static> ContactService for ContactServiceImpl<Q, C> {
    async fn search_contacts(
        &self,
        request: Request<SearchContactsRequest>,
    ) -> Result<Response<SearchContactsResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            10
        } else {
            req.limit as usize
        };

        let requesting_account_id = req
            .requesting_account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .query_bus
            .dispatch(Query::SearchContacts {
                query: req.query,
                limit,
                requesting_account_id,
            })
            .await
            .map_err(application_error_to_status)?;

        let hits = result
            .into_contact_hits()
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_hits = hits.into_iter().map(domain_hit_to_proto).collect();
        Ok(Response::new(SearchContactsResponse { hits: proto_hits }))
    }

    async fn find_contact_duplicates(
        &self,
        request: Request<FindContactDuplicatesRequest>,
    ) -> Result<Response<FindContactDuplicatesResponse>, Status> {
        let req = request.into_inner();
        let account_id = req
            .account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .query_bus
            .dispatch(Query::FindContactDuplicates { account_id })
            .await
            .map_err(application_error_to_status)?;

        let pairs = result
            .into_contact_duplicates()
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_pairs = pairs
            .into_iter()
            .map(|(keep, discard)| ContactDuplicatePair {
                keep: Some(domain_hit_to_proto(keep)),
                discard: Some(domain_hit_to_proto(discard)),
            })
            .collect();

        Ok(Response::new(FindContactDuplicatesResponse {
            pairs: proto_pairs,
        }))
    }

    async fn merge_contacts(
        &self,
        request: Request<MergeContactsRequest>,
    ) -> Result<Response<MergeContactsResponse>, Status> {
        let req = request.into_inner();

        let keep_id = parse_contact_id(&req.keep_id)?;
        let discard_id = parse_contact_id(&req.discard_id)?;

        self.command_bus
            .dispatch(Command::MergeContacts {
                keep_id,
                discard_id,
            })
            .await
            .map_err(application_error_to_status)?;

        Ok(Response::new(MergeContactsResponse {}))
    }

    async fn rebuild_contact_index(
        &self,
        request: Request<RebuildContactIndexRequest>,
    ) -> Result<Response<RebuildContactIndexResponse>, Status> {
        let req = request.into_inner();
        let account_id = req
            .account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .command_bus
            .dispatch(Command::RebuildContactIndex { account_id })
            .await
            .map_err(application_error_to_status)?;

        let indexed_count = match result {
            mc_application::commands::CommandResult::ContactIndexRebuilt { indexed_count } => {
                indexed_count
            }
            _ => 0,
        };

        Ok(Response::new(RebuildContactIndexResponse { indexed_count }))
    }

    async fn backfill_contacts_from_messages(
        &self,
        request: Request<BackfillContactsRequest>,
    ) -> Result<Response<BackfillContactsResponse>, Status> {
        let req = request.into_inner();
        let account_id = req
            .account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .command_bus
            .dispatch(Command::BackfillContactsFromMessages { account_id })
            .await
            .map_err(application_error_to_status)?;

        let (new_contacts, already_existed) = match result {
            mc_application::commands::CommandResult::ContactsBackfilled {
                new_contacts,
                already_existed,
            } => (new_contacts, already_existed),
            _ => (0, 0),
        };

        Ok(Response::new(BackfillContactsResponse {
            new_contacts,
            already_existed,
        }))
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn domain_hit_to_proto(h: mc_domain::ContactSearchHit) -> ProtoContactSearchHit {
    ProtoContactSearchHit {
        id: h.id.to_string(),
        display_name: h.display_name,
        primary_email: h.primary_email,
        all_emails: h.all_emails,
        organization: h.organization,
        account_scope: h.account_scope.map(|a| a.to_string()).unwrap_or_default(),
        is_cross_account: h.is_cross_account,
    }
}

fn parse_contact_id(s: &str) -> Result<ContactId, Status> {
    uuid::Uuid::parse_str(s)
        .map(ContactId::from_uuid)
        .map_err(|_| Status::invalid_argument(format!("invalid contact id: {s}")))
}
