use super::domain_flag_to_proto;
use crate::grpc::proto::{MailboxSummary, MailboxType as ProtoMailboxType};
use mc_domain::model::mailbox::{Mailbox as DomainMailbox, MailboxType as DomainMailboxType};

/// Convert domain MailboxType to proto MailboxType.
pub fn domain_mailbox_type_to_proto(mailbox_type: &DomainMailboxType) -> i32 {
    match mailbox_type {
        DomainMailboxType::Inbox => ProtoMailboxType::Inbox as i32,
        DomainMailboxType::Sent => ProtoMailboxType::Sent as i32,
        DomainMailboxType::Drafts => ProtoMailboxType::Drafts as i32,
        DomainMailboxType::Trash => ProtoMailboxType::Trash as i32,
        DomainMailboxType::Spam => ProtoMailboxType::Spam as i32,
        DomainMailboxType::Archive => ProtoMailboxType::Archive as i32,
        DomainMailboxType::All => ProtoMailboxType::All as i32,
        DomainMailboxType::Important => ProtoMailboxType::Important as i32,
        DomainMailboxType::Custom => ProtoMailboxType::Custom as i32,
    }
}

/// Convert domain Mailbox to proto MailboxSummary.
pub fn domain_mailbox_to_proto(mailbox: &DomainMailbox) -> MailboxSummary {
    MailboxSummary {
        id: mailbox.id.to_string(),
        account_id: mailbox.account_id.to_string(),
        path: mailbox.path.full_path(),
        display_path: mailbox.display_path.clone(),
        display_name: mailbox.display_name.clone(),
        mailbox_type: domain_mailbox_type_to_proto(&mailbox.mailbox_type),
        total_messages: mailbox.total_messages,
        unread_messages: mailbox.unread_messages,
        flags: mailbox.flags.iter().map(domain_flag_to_proto).collect(),
        subscribed: mailbox.subscribed,
    }
}
