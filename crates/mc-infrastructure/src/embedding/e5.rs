//! multilingual-e5-small, int8-quantized ONNX (Xenova's export): 384
//! dimensions, 100 languages incl. Italian, ~118 MB. Chosen over
//! paraphrase-multilingual-MiniLM-L12-v2 for its better retrieval quality at
//! the same size and a 512-token window.
//!
//! The files are fetched once from a pinned Hugging Face revision into the
//! data folder and checked against their SHA-256 on every load.

use std::path::{Path, PathBuf};
use std::time::Duration;

use fastembed::{
    InitOptionsUserDefined, Pooling, TextEmbedding, TokenizerFiles, UserDefinedEmbeddingModel,
};
use sha2::{Digest, Sha256};

use mc_domain::DomainError;

use super::{Engine, LazyEmbedder};

const REVISION: &str = "761b726dd34fb83930e26aab4e9ac3899aa1fa78";
/// Changes with the model, its revision or how texts are fed to it.
pub const MODEL: &str = "multilingual-e5-small-q@761b726";
/// Tokens read per text: subject and the start of the body say what a mail is.
const MAX_TOKENS: usize = 256;
/// Texts per forward pass, and ONNX threads: bounded so it never hogs the CPU.
const BATCH: usize = 16;
const THREADS: usize = 2;

/// (path in the repository, SHA-256).
const FILES: [(&str, &str); 5] = [
    (
        "onnx/model_quantized.onnx",
        "f80102d3f2a1229f387d3c81909990d8945513e347b0eab049f7de3c6f98c193",
    ),
    (
        "tokenizer.json",
        "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
    ),
    (
        "config.json",
        "cb99455288675345e1a4f411438d5d0adbba5fbd3a67ea4fb03c015433b996c1",
    ),
    (
        "special_tokens_map.json",
        "d05497f1da52c5e09554c0cd874037a083e1dc1b9cfd48034d1c717f1afc07a7",
    ),
    (
        "tokenizer_config.json",
        "a1d6bc8734a6f635dc158508bef000f8e2e5a759c7d92f984b2c86e5ff53425b",
    ),
];

struct E5(TextEmbedding);

impl Engine for E5 {
    fn embed(&mut self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, DomainError> {
        // The model was trained with a prefix; "query: " is the one for
        // symmetric tasks such as similarity.
        let texts: Vec<String> = texts.iter().map(|t| format!("query: {t}")).collect();
        let mut vectors = self
            .0
            .embed(texts, Some(BATCH))
            .map_err(|e| DomainError::Provider(format!("embedding: {e}")))?;
        vectors.iter_mut().for_each(|v| normalize(v));
        Ok(vectors)
    }
}

fn normalize(v: &mut [f32]) {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
}

/// An embedder that downloads the model into `dir` when first asked, and lets
/// go of it after `idle`. Nothing is fetched before the first `embed`.
pub fn multilingual_e5_small(dir: PathBuf, idle: Duration) -> LazyEmbedder<impl Engine> {
    LazyEmbedder::new(
        MODEL,
        idle,
        Box::new(move || {
            let dir = dir.clone();
            Box::pin(async move { load(&dir).await })
        }),
    )
}

async fn load(dir: &Path) -> Result<E5, DomainError> {
    let mut files = Vec::with_capacity(FILES.len());
    for (name, sha256) in FILES {
        files.push(fetch(dir, name, sha256).await?);
    }
    tokio::task::spawn_blocking(move || {
        let mut files = files.into_iter();
        let onnx = files.next().unwrap_or_default();
        let mut next = || files.next().unwrap_or_default();
        let tokenizer = TokenizerFiles {
            tokenizer_file: next(),
            config_file: next(),
            special_tokens_map_file: next(),
            tokenizer_config_file: next(),
        };
        let model = UserDefinedEmbeddingModel::new(onnx, tokenizer).with_pooling(Pooling::Mean);
        let options = InitOptionsUserDefined::new()
            .with_max_length(MAX_TOKENS)
            .with_intra_threads(THREADS);
        TextEmbedding::try_new_from_user_defined(model, options)
            .map(E5)
            .map_err(|e| DomainError::Provider(format!("loading the embedding model: {e}")))
    })
    .await
    .map_err(|e| DomainError::Provider(format!("loading task: {e}")))?
}

/// The file's bytes: from `dir` if they match the checksum, else downloaded.
async fn fetch(dir: &Path, name: &str, sha256: &str) -> Result<Vec<u8>, DomainError> {
    let path = dir.join(REVISION).join(name);
    if let Ok(bytes) = tokio::fs::read(&path).await
        && matches(&bytes, sha256)
    {
        return Ok(bytes);
    }
    let url =
        format!("https://huggingface.co/Xenova/multilingual-e5-small/resolve/{REVISION}/{name}");
    let bytes = download(&url).await?;
    if !matches(&bytes, sha256) {
        return Err(DomainError::Provider(format!(
            "{name}: checksum mismatch, not using the download"
        )));
    }
    let io = |e: std::io::Error| DomainError::Provider(format!("saving {name}: {e}"));
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(io)?;
    }
    // Written whole before it gets its name, so a crash leaves no half file.
    let part = path.with_extension("part");
    tokio::fs::write(&part, &bytes).await.map_err(io)?;
    tokio::fs::rename(&part, &path).await.map_err(io)?;
    Ok(bytes)
}

async fn download(url: &str) -> Result<Vec<u8>, DomainError> {
    let err = |e: reqwest::Error| DomainError::Provider(format!("downloading {url}: {e}"));
    let mut response = reqwest::get(url)
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(err)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(err)? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(super) fn matches(bytes: &[u8], sha256: &str) -> bool {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
        == sha256
}
