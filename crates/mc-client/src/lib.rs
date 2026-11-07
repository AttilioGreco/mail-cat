mod convert;
#[allow(clippy::enum_variant_names)]
mod proto {
    pub mod mail_cat {
        pub mod v1 {
            tonic::include_proto!("mail_cat.v1");
        }
    }
}

pub mod client;
mod curation;
mod discovery;
pub mod error;
mod server;
mod tagging;
pub mod types;

pub use client::MailCatClient;
pub use curation::*;
pub use discovery::*;
pub use error::{ClientError, Result};
pub use server::*;
pub use tagging::*;
pub use types::*;
