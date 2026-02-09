//! Query bus trait — dispatches read-side queries to their handlers.

use crate::error::ApplicationError;
use crate::queries::{Query, QueryResult};

/// Dispatches [`Query`]s to the appropriate handler.
///
/// Implementations wire query variants to concrete handler logic that
/// reads from projected read models (eventually consistent with the
/// event store).
pub trait QueryBus: Send + Sync {
    /// Dispatch a query and return the result.
    ///
    /// The returned [`QueryResult`] variant must match the [`Query`] variant.
    /// Use the `into_*` helpers on `QueryResult` for typed extraction.
    async fn dispatch(&self, query: Query) -> Result<QueryResult, ApplicationError>;
}
