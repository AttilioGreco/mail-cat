//! Projection event handlers — subscribe to domain events and update read models.
//!
//! The [`ProjectionEventHandler`] bridges the event store and the read model:
//! it processes each event through a [`ReadModelProjector`] and tracks its
//! checkpoint so that events are processed exactly once under normal operation,
//! and at-least-once on rebuild (which requires idempotent projectors).

pub mod read_model;
pub mod search_index;

pub use read_model::ProjectionEventHandler;
pub use search_index::SearchProjectionHandler;
