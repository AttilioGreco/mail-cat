//! The models users choose from, and where they may run.
//!
//! All are Xenova's int8-quantized ONNX exports of multilingual-e5 (same
//! tokenizer, mean pooling, `passage:`/`query:` prefixes), 100 languages
//! incl. Italian and English. Each is pinned to a Hugging Face revision and
//! every file to its SHA-256.

use std::fmt;
use std::str::FromStr;

use mc_domain::{DomainError, EmbedPurpose, ModelId, ModelOption, ModelProfile, TargetOption};

/// How a model wants its texts prefixed, per purpose.
pub struct Prefixes {
    pub document: &'static str,
    pub query: &'static str,
}

pub struct ModelSpec {
    pub profile: ModelProfile,
    /// Name in the vector store; the revision completes the `ModelId`.
    pub id: &'static str,
    pub repo: &'static str,
    pub revision: &'static str,
    /// (path in the repository, SHA-256, bytes). The first is the ONNX model.
    pub files: [(&'static str, &'static str, u64); 5],
    pub dims: usize,
    /// Tokens read per text: subject and the start of the body say what a mail is.
    pub max_tokens: usize,
    pub prefixes: Prefixes,
    /// What each worker held in memory when measured with `bench_parallelism` (1 thread pair, rounded).
    pub worker_bytes: u64,
    /// 1 (good) to 3 (best).
    pub quality: u8,
    pub languages: &'static str,
}

const E5: Prefixes = Prefixes {
    document: "passage: ",
    query: "query: ",
};

const TOKENIZER: (&str, &str, u64) = (
    "tokenizer.json",
    "62c24cdc13d4c9952d63718d6c9fa4c287974249e16b7ade6d5a85e7bbb75626",
    17_082_660,
);
const SPECIAL_TOKENS: (&str, &str, u64) = (
    "special_tokens_map.json",
    "06e405a36dfe4b9604f484f6a1e619af1a7f7d09e34a8555eb0b77b66318067f",
    280,
);
const TOKENIZER_CONFIG: (&str, &str, u64) = (
    "tokenizer_config.json",
    "efb5c0d09722e5fe59a462cd2a9976ee216d55b037597d997cd3fe833216da15",
    418,
);

pub const CATALOG: [ModelSpec; 3] = [
    ModelSpec {
        profile: ModelProfile::Light,
        id: "multilingual-e5-small",
        repo: "Xenova/multilingual-e5-small",
        revision: "761b726dd34fb83930e26aab4e9ac3899aa1fa78",
        files: [
            (
                "onnx/model_quantized.onnx",
                "f80102d3f2a1229f387d3c81909990d8945513e347b0eab049f7de3c6f98c193",
                118_308_185,
            ),
            (
                "tokenizer.json",
                "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
                17_082_730,
            ),
            (
                "config.json",
                "cb99455288675345e1a4f411438d5d0adbba5fbd3a67ea4fb03c015433b996c1",
                658,
            ),
            (
                "special_tokens_map.json",
                "d05497f1da52c5e09554c0cd874037a083e1dc1b9cfd48034d1c717f1afc07a7",
                167,
            ),
            (
                "tokenizer_config.json",
                "a1d6bc8734a6f635dc158508bef000f8e2e5a759c7d92f984b2c86e5ff53425b",
                443,
            ),
        ],
        dims: 384,
        max_tokens: 256,
        prefixes: E5,
        worker_bytes: 670_000_000,
        quality: 1,
        languages: "100 lingue, italiano e inglese inclusi",
    },
    ModelSpec {
        profile: ModelProfile::Balanced,
        id: "multilingual-e5-base",
        repo: "Xenova/multilingual-e5-base",
        revision: "1ec9243030a27d1a115d5c340572074c125b58b2",
        files: [
            (
                "onnx/model_quantized.onnx",
                "df7a9a29309e3ad491e1783adf8baee710262cc06079c7cbab63c630277fac94",
                278_647_662,
            ),
            TOKENIZER,
            (
                "config.json",
                "4c27930e59106027abab56f7531c1fa6b14bbf31e8229ec36d68affa4e869bcd",
                686,
            ),
            SPECIAL_TOKENS,
            TOKENIZER_CONFIG,
        ],
        dims: 768,
        max_tokens: 256,
        prefixes: E5,
        worker_bytes: 830_000_000,
        quality: 2,
        languages: "100 lingue, italiano e inglese inclusi",
    },
    ModelSpec {
        profile: ModelProfile::Quality,
        id: "multilingual-e5-large",
        repo: "Xenova/multilingual-e5-large",
        revision: "00fc3aeb3dbb95842de2ac1961d33c6319acf57b",
        files: [
            (
                "onnx/model_quantized.onnx",
                "0a8d65db9a36f810ba5da15249f13145fcdc7890e6656f1fd38cd8b7c4db1fca",
                561_768_762,
            ),
            TOKENIZER,
            (
                "config.json",
                "b509e517b78bc01ce8fba4827592354df3fd1b3e077fe663fb58b6b7bddd0751",
                792,
            ),
            SPECIAL_TOKENS,
            TOKENIZER_CONFIG,
        ],
        dims: 1024,
        max_tokens: 256,
        prefixes: E5,
        worker_bytes: 1_120_000_000,
        quality: 3,
        languages: "100 lingue, italiano e inglese inclusi",
    },
];

impl ModelSpec {
    pub fn for_profile(profile: ModelProfile) -> &'static ModelSpec {
        CATALOG
            .iter()
            .find(|s| s.profile == profile)
            .unwrap_or(&CATALOG[0])
    }

    pub fn model_id(&self) -> ModelId {
        ModelId::new(self.id, self.revision)
    }

    pub fn download_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.2).sum()
    }

    pub fn prefix(&self, purpose: EmbedPurpose) -> &'static str {
        match purpose {
            EmbedPurpose::Document => self.prefixes.document,
            EmbedPurpose::Query => self.prefixes.query,
        }
    }

    pub fn option(&self) -> ModelOption {
        ModelOption {
            profile: self.profile,
            model: self.model_id(),
            dims: self.dims,
            download_bytes: self.download_bytes(),
            worker_bytes: self.worker_bytes,
            quality: self.quality,
        }
    }
}

/// Where inference runs. Only the CPU is wired up; the others are declared so
/// the menu can show them and a later build can light them up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExecutionTarget {
    #[default]
    Cpu,
    Gpu(Gpu),
    Npu(Npu),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gpu {
    Cuda,
    Rocm,
    DirectMl,
    CoreMl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Npu {
    VitisAi,
}

impl ExecutionTarget {
    pub const ALL: [Self; 6] = [
        Self::Cpu,
        Self::Gpu(Gpu::Cuda),
        Self::Gpu(Gpu::Rocm),
        Self::Gpu(Gpu::DirectMl),
        Self::Gpu(Gpu::CoreMl),
        Self::Npu(Npu::VitisAi),
    ];

    pub fn id(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Gpu(Gpu::Cuda) => "gpu-cuda",
            Self::Gpu(Gpu::Rocm) => "gpu-rocm",
            Self::Gpu(Gpu::DirectMl) => "gpu-directml",
            Self::Gpu(Gpu::CoreMl) => "gpu-coreml",
            Self::Npu(Npu::VitisAi) => "npu-vitisai",
        }
    }

    pub fn is_available(self) -> bool {
        self == Self::Cpu
    }

    pub fn ensure_available(self) -> Result<(), DomainError> {
        if self.is_available() {
            return Ok(());
        }
        Err(DomainError::Validation(format!(
            "{}: non disponibile su questa build/macchina",
            self.id()
        )))
    }

    pub fn options() -> Vec<TargetOption> {
        Self::ALL
            .iter()
            .map(|t| TargetOption {
                id: t.id().into(),
                available: t.is_available(),
            })
            .collect()
    }
}

impl FromStr for ExecutionTarget {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, DomainError> {
        Self::ALL
            .iter()
            .find(|t| t.id() == s)
            .copied()
            .ok_or_else(|| DomainError::Validation(format!("unknown execution target {s:?}")))
    }
}

impl fmt::Display for ExecutionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.id())
    }
}
