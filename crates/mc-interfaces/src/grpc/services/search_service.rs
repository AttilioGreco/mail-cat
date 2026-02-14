use std::sync::Arc;
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;

use mc_application::{
    bus::QueryBus,
    queries::Query,
};
use mc_domain::{AccountId, MailboxId};
use mc_shared::pagination::Pagination;
use crate::grpc::proto::{
    search_service_server::SearchService, RebuildIndexProgress, RebuildIndexRequest,
    SearchRequest, SearchResponse,
};
use crate::grpc::mappers::application_error_to_status;

/// Search service implementation with QueryBus integration.
pub struct SearchServiceImpl<Q: QueryBus + 'static> {
    query_bus: Arc<Q>,
}

impl<Q: QueryBus + 'static> SearchServiceImpl<Q> {
    pub fn new(query_bus: Arc<Q>) -> Self {
        Self { query_bus }
    }
}

#[tonic::async_trait]
impl<Q: QueryBus + 'static> SearchService for SearchServiceImpl<Q> {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();

        let account_id = req.account_id.and_then(|id| {
            uuid::Uuid::parse_str(&id)
                .ok()
                .map(AccountId::from_uuid)
        });

        let mailbox_id = req.mailbox_id.and_then(|id| {
            uuid::Uuid::parse_str(&id)
                .ok()
                .map(MailboxId::from_uuid)
        });

        let pagination = req.pagination
            .map(|p| Pagination {
                page: p.page,
                page_size: p.page_size,
            })
            .unwrap_or_default();

        let query = Query::SearchMessages {
            query: req.query,
            account_id,
            mailbox_id,
            pagination: pagination.clone(),
        };

        let result = self
            .query_bus
            .dispatch(query)
            .await
            .map_err(application_error_to_status)?;

        let search_results = result
            .into_search_results()
            .map_err(|e| Status::internal(e.to_string()))?;

        // Convert SearchResults to proto format
        let hits = search_results.hits.iter().map(|hit| {
            crate::grpc::proto::SearchHit {
                message_id: hit.message_id.to_string(),
                score: hit.score,
                snippet: hit.snippet.clone(),
            }
        }).collect();

        // Create PageInfo from total_count
        let page_info = crate::grpc::proto::PageInfo {
            total_count: search_results.total_count,
            page: pagination.page,
            page_size: pagination.page_size,
            has_next_page: (pagination.page * pagination.page_size as u32) < search_results.total_count as u32,
            total_pages: ((search_results.total_count + pagination.page_size as u64 - 1) / pagination.page_size as u64) as u32,
        };

        Ok(Response::new(SearchResponse {
            hits,
            page_info: Some(page_info),
        }))
    }

    type RebuildIndexStream = ReceiverStream<Result<RebuildIndexProgress, Status>>;

    async fn rebuild_index(
        &self,
        _request: Request<RebuildIndexRequest>,
    ) -> Result<Response<Self::RebuildIndexStream>, Status> {
        // TODO: Implement rebuild with streaming progress
        // Requires SearchIndex::rebuild() which returns progress updates
        Err(Status::unimplemented("rebuild_index streaming not yet implemented"))
    }
}
