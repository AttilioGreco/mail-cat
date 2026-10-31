//! What the application knows of an embedding model: which one made a vector,
//! what a text is embedded for, and the menu offered to the user.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A model and its revision. Vectors of different ids are never compared: a
/// change means the mail is embedded again.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelId {
    pub id: String,
    pub revision: String,
}

impl ModelId {
    pub fn new(id: &str, revision: &str) -> Self {
        Self {
            id: id.to_owned(),
            revision: revision.to_owned(),
        }
    }
}

/// The form in which vectors are stored and looked up.
impl fmt::Display for ModelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.revision)
    }
}

/// What the text is for. Models trained with asymmetric prefixes (e5) treat
/// the two differently; the application never sees how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedPurpose {
    /// Something to be found: a message.
    Document,
    /// Something to search with.
    Query,
}

/// The models on offer, from light to heavy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelProfile {
    #[default]
    Light,
    Balanced,
    Quality,
}

impl ModelProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Balanced => "balanced",
            Self::Quality => "quality",
        }
    }
}

impl FromStr for ModelProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Ok(match s {
            "light" => Self::Light,
            "balanced" => Self::Balanced,
            "quality" => Self::Quality,
            other => return Err(format!("unknown model profile {other:?}")),
        })
    }
}

/// One entry of the menu, for the user to compare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelOption {
    pub profile: ModelProfile,
    pub model: ModelId,
    pub dims: usize,
    pub download_bytes: u64,
    /// An estimate of what each worker holds in memory.
    pub worker_bytes: u64,
    /// 1 (good) to 3 (best).
    pub quality: u8,
}

/// Where inference can run; `available` is false on this build or machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetOption {
    pub id: String,
    pub available: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_survive_their_name_and_the_default_is_light() {
        for p in [
            ModelProfile::Light,
            ModelProfile::Balanced,
            ModelProfile::Quality,
        ] {
            assert_eq!(p.as_str().parse(), Ok(p));
        }
        assert_eq!(ModelProfile::default(), ModelProfile::Light);
        assert!("huge".parse::<ModelProfile>().is_err());
    }

    #[test]
    fn a_model_is_stored_under_id_and_revision() {
        assert_eq!(ModelId::new("m", "r1").to_string(), "m@r1");
        assert_ne!(ModelId::new("m", "r1"), ModelId::new("m", "r2"));
    }
}
