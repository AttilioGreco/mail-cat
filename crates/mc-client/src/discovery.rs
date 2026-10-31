//! Label discovery: proposals of new labels made from similar unlabelled mail.

use crate::client::MailCatClient;
use crate::error::Result;
use crate::proto::mail_cat::v1 as pb;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProposalSample {
    pub message_id: String,
    pub subject: String,
    pub from: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelProposal {
    pub id: String,
    pub name: String,
    pub keywords: Vec<String>,
    pub size: u32,
    /// Of `size`, how many are in an Inbox.
    pub inbox: u32,
    pub samples: Vec<ProposalSample>,
    /// The training group whose mail it is made of.
    pub group: String,
    /// Unix seconds.
    pub created_at: i64,
}

fn proposal(p: pb::LabelProposal) -> LabelProposal {
    LabelProposal {
        id: p.id,
        name: p.name,
        keywords: p.keywords,
        size: p.size,
        inbox: p.inbox,
        samples: p
            .samples
            .into_iter()
            .map(|s| ProposalSample {
                message_id: s.message_id,
                subject: s.subject,
                from: s.from,
            })
            .collect(),
        group: p.group,
        created_at: p.created_at,
    }
}

impl MailCatClient {
    /// Look for new labels in the background, in the account's training group
    /// or in all of them. `false`: a search was already running.
    pub async fn discover_labels(&self, account_id: Option<&str>) -> Result<bool> {
        let resp = self
            .tagging()
            .discover_labels(pb::DiscoverLabelsRequest {
                account_id: account_id.map(str::to_owned),
            })
            .await?;
        Ok(resp.into_inner().started)
    }

    pub async fn list_label_proposals(&self) -> Result<Vec<LabelProposal>> {
        let resp = self
            .tagging()
            .list_label_proposals(pb::ListLabelProposalsRequest {})
            .await?;
        Ok(resp
            .into_inner()
            .proposals
            .into_iter()
            .map(proposal)
            .collect())
    }

    /// Creates the label and labels the cluster's mail in the background;
    /// returns how many messages. `name`: the user's own, `None` keeps the
    /// proposed one. `archive` also takes the mail out of the Inbox.
    pub async fn accept_label_proposal(
        &self,
        id: &str,
        name: Option<&str>,
        archive: bool,
    ) -> Result<u32> {
        let resp = self
            .tagging()
            .accept_label_proposal(pb::AcceptLabelProposalRequest {
                id: id.into(),
                name: name.unwrap_or_default().into(),
                archive,
            })
            .await?;
        Ok(resp.into_inner().messages)
    }

    pub async fn reject_label_proposal(&self, id: &str) -> Result<()> {
        self.tagging()
            .reject_label_proposal(pb::RejectLabelProposalRequest { id: id.into() })
            .await?;
        Ok(())
    }
}
