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
pub mod error;
pub mod types;

pub use client::MailCatClient;
pub use error::{ClientError, Result};
pub use types::*;
