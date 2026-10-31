//! Label discovery: clusters of similar unlabelled mail, proposed to the
//! user as labels they have not created yet.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::model::tagging::Scope;
use crate::value_objects::message_id::MessageId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProposalId(Uuid);

impl ProposalId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for ProposalId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposalStatus {
    Pending,
    Accepted,
    /// Kept, so the same cluster is not proposed again.
    Rejected,
}

impl ProposalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }
}

impl FromStr for ProposalStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Ok(match s {
            "pending" => Self::Pending,
            "accepted" => Self::Accepted,
            "rejected" => Self::Rejected,
            other => return Err(format!("unknown proposal status {other:?}")),
        })
    }
}

/// A message shown to give an idea of what the cluster holds.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProposalSample {
    pub message_id: MessageId,
    pub subject: String,
    pub from: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Proposal {
    pub id: ProposalId,
    pub scope: Scope,
    pub name: String,
    pub keywords: Vec<String>,
    /// The cluster, which is also what identifies it from one run to the next.
    pub members: Vec<MessageId>,
    /// Of the members, those in an Inbox.
    pub inbox: u32,
    pub samples: Vec<ProposalSample>,
    pub status: ProposalStatus,
    pub created_at: DateTime<Utc>,
}

impl Proposal {
    pub fn size(&self) -> usize {
        self.members.len()
    }
}

/// An unlabelled message's embedding, as discovery clusters it.
#[derive(Debug, Clone)]
pub struct DiscoveryPoint {
    pub id: MessageId,
    pub in_inbox: bool,
    pub vector: Vec<f32>,
}
