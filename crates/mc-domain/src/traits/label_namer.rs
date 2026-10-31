//! Proposes a name for a group of similar messages.

use async_trait::async_trait;

/// What is known of a cluster, however it was found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClusterSummary {
    /// Messages in the cluster.
    pub size: usize,
    /// Terms that set the cluster apart from the rest, with their weight.
    pub keywords: Vec<(String, f64)>,
    /// Sender domains (or addresses) and how many messages each sent.
    pub senders: Vec<(String, usize)>,
    pub sample_subjects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedName {
    pub name: String,
    /// What to show next to the name, so the user sees why.
    pub keywords: Vec<String>,
}

#[async_trait]
pub trait LabelNamer: Send + Sync {
    async fn name(&self, cluster: &ClusterSummary) -> ProposedName;
}
