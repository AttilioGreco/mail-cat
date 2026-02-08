use serde::{Deserialize, Serialize};

use crate::value_objects::email_address::EmailAddress;

/// Sender identity: the persona used when composing/replying.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub display_name: String,
    pub email: EmailAddress,
    pub signature_html: Option<String>,
    pub signature_text: Option<String>,
}
