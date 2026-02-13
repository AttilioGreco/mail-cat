use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A UTC timestamp wrapper for domain use.
pub type Timestamp = DateTime<Utc>;

/// Sort direction for queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SortOrder {
    Ascending,
    #[default]
    Descending,
}
