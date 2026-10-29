mod any;
mod event_source;
pub mod imap;
pub mod jmap;
pub mod smtp;

use mc_domain::{AccountId, Envelope, MailboxId, Message, RemoteId};

pub use any::AnyProvider;
pub use event_source::AnyAccountEventSource;
pub use imap::ImapEmailProvider;
pub use jmap::JmapEmailProvider;
pub use smtp::SmtpSender;

/// A message with only its bodies filled: all `SyncService::fetch_message_body`
/// reads from a provider's detail fetch.
fn body_only_message(
    id: &RemoteId,
    body_text: Option<String>,
    body_html: Option<String>,
) -> Message {
    let now = chrono::Utc::now();
    let envelope = Envelope {
        from: vec![],
        to: vec![],
        cc: vec![],
        bcc: vec![],
        reply_to: vec![],
        subject: String::new(),
        date: now,
    };
    let (message, _event) = Message::receive(
        AccountId::new(),
        MailboxId::new(),
        id.clone(),
        None,
        None,
        vec![],
        envelope,
        vec![],
        0,
        false,
        now,
    );
    Message {
        body_text,
        body_html,
        ..message
    }
}
