mod account_event_source;
mod client;
mod mapper;

pub use account_event_source::ImapAccountEventSource;
pub use client::{ImapEmailProvider, asks_the_user_to_act};
