use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::model::account::AccountId;

macro_rules! remote_string {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

remote_string!(
    /// A server collection (address book or calendar), by the path the
    /// server knows it under. Unique within an account only.
    CollectionId
);
remote_string!(
    /// A resource inside a collection, by the path the server knows it under.
    Href
);
remote_string!(
    /// The server's opaque version tag of a resource: sent back as the
    /// precondition of an overwrite, never interpreted.
    Etag
);

/// Where a contact or event lives on the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteBinding {
    pub account: AccountId,
    pub collection: CollectionId,
    pub href: Href,
    /// The version the local copy is based on. `None` until the server has
    /// accepted the resource once: the push is then a create.
    pub etag: Option<Etag>,
    pub remote_modified: Option<DateTime<Utc>>,
}

/// Who made a change to a remote-bound resource. A pull applies `Remote`
/// events; a user edit is `Local`, and is what makes the resource dirty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Origin {
    #[default]
    Local,
    Remote,
}
