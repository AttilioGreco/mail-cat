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
    /// `pending`, `accepted` or `rejected`.
    pub status: String,
    /// The training scope: `g:<group id>`, or `a:<account id>` for an account
    /// that learns alone. Proposals merge only within a scope.
    pub scope: String,
    /// Dominant sender domains, most frequent first; empty unless asked for.
    pub senders: Vec<SenderCount>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderCount {
    pub domain: String,
    pub count: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProposalMessage {
    pub message_id: String,
    /// RFC 3339.
    pub received_at: String,
    pub from: String,
    pub subject: String,
    /// Start of the plain text; empty until the body is downloaded.
    pub excerpt: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProposalDetail {
    #[serde(flatten)]
    pub proposal: LabelProposal,
    pub messages: Vec<ProposalMessage>,
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
        status: p.status,
        scope: p.scope,
        senders: p
            .senders
            .into_iter()
            .map(|s| SenderCount {
                domain: s.domain,
                count: s.count,
            })
            .collect(),
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
        self.find_label_proposals(None, "pending", false).await
    }

    /// `status`: `pending`, `accepted`, `rejected` or `all`. `account_id`
    /// limits to that account's training scope. `senders` also counts the
    /// sender domains, which reads every member.
    pub async fn find_label_proposals(
        &self,
        account_id: Option<&str>,
        status: &str,
        senders: bool,
    ) -> Result<Vec<LabelProposal>> {
        let resp = self
            .tagging()
            .list_label_proposals(pb::ListLabelProposalsRequest {
                account_id: account_id.map(str::to_owned),
                status: status.into(),
                with_senders: senders,
            })
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

    /// A proposal of any status with up to `samples` of its messages.
    pub async fn get_label_proposal(&self, id: &str, samples: u32) -> Result<ProposalDetail> {
        let resp = self
            .tagging()
            .get_label_proposal(pb::GetLabelProposalRequest {
                id: id.into(),
                samples,
            })
            .await?
            .into_inner();
        Ok(ProposalDetail {
            proposal: proposal(resp.proposal.unwrap_or_default()),
            messages: resp
                .messages
                .into_iter()
                .map(|m| ProposalMessage {
                    message_id: m.message_id,
                    received_at: m.received_at,
                    from: m.from,
                    subject: m.subject,
                    excerpt: m.excerpt,
                })
                .collect(),
        })
    }

    pub async fn rename_label_proposal(&self, id: &str, name: &str) -> Result<LabelProposal> {
        let resp = self
            .tagging()
            .rename_label_proposal(pb::RenameLabelProposalRequest {
                id: id.into(),
                name: name.into(),
            })
            .await?;
        Ok(proposal(resp.into_inner()))
    }

    /// Replaces the proposals (one training scope) by a new pending one.
    pub async fn merge_label_proposals(
        &self,
        ids: &[String],
        name: Option<&str>,
    ) -> Result<LabelProposal> {
        let resp = self
            .tagging()
            .merge_label_proposals(pb::MergeLabelProposalsRequest {
                ids: ids.to_vec(),
                name: name.unwrap_or_default().into(),
            })
            .await?;
        Ok(proposal(resp.into_inner()))
    }

    /// Labels the proposal's mail with a label its group already has; returns
    /// how many messages.
    pub async fn assign_label_proposal(&self, id: &str, label: &str, archive: bool) -> Result<u32> {
        let resp = self
            .tagging()
            .assign_label_proposal(pb::AssignLabelProposalRequest {
                id: id.into(),
                label: label.into(),
                archive,
            })
            .await?;
        Ok(resp.into_inner().messages)
    }
}
