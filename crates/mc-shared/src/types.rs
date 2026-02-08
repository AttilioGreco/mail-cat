use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A UTC timestamp wrapper for domain use.
pub type Timestamp = DateTime<Utc>;

/// Sort direction for queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl Default for SortOrder {
    fn default() -> Self {
        Self::Descending
    }
}
