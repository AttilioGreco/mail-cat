//! Runs a catalog model through fastembed/ONNX Runtime.
//!
//! The files are fetched once from the model's pinned Hugging Face revision
//! into the data folder and checked against their SHA-256 on every load.

use std::path::{Path, PathBuf};
use std::time::Duration;

use fastembed::{
    InitOptionsUserDefined, Pooling, TextEmbedding, TokenizerFiles, UserDefinedEmbeddingModel,
};
use sha2::{Digest, Sha256};

use mc_domain::{DomainError, EmbedPurpose};

use super::catalog::ModelSpec;
use super::{Engine, LazyEmbedder};

pub struct Onnx {
    model: TextEmbedding,
    spec: &'static ModelSpec,
}

impl Engine for Onnx {
    fn embed(
        &mut self,
        texts: Vec<String>,
        purpose: EmbedPurpose,
    ) -> Result<Vec<Vec<f32>>, DomainError> {
        let prefix = self.spec.prefix(purpose);
        let texts: Vec<String> = texts.iter().map(|t| format!("{prefix}{t}")).collect();
        let mut vectors = self
            .model
            .embed(texts, None)
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
pub fn onnx_embedder(spec: &'static ModelSpec, dir: PathBuf, idle: Duration) -> LazyEmbedder<Onnx> {
    LazyEmbedder::new(
        spec.model_id(),
        spec.dims,
        idle,
        Box::new(move |workers, threads| {
            let dir = dir.clone();
            Box::pin(async move { load(spec, &dir, workers, threads).await })
        }),
    )
}

/// Each worker's session is built on a niced thread of its own: ONNX Runtime
/// starts its intra-op threads while building, and on Linux they inherit the
/// priority of the thread that did.
async fn load(
    spec: &'static ModelSpec,
    dir: &Path,
    workers: usize,
    threads: usize,
) -> Result<Vec<Onnx>, DomainError> {
    let mut files = Vec::with_capacity(spec.files.len());
    for (name, sha256, _) in spec.files {
        files.push(fetch(spec, dir, name, sha256).await?);
    }
    let mut files = files.into_iter();
    let onnx = files.next().unwrap_or_default();
    let mut next = || files.next().unwrap_or_default();
    // Same order as `ModelSpec::files`.
    let tokenizer = TokenizerFiles {
        tokenizer_file: next(),
        config_file: next(),
        special_tokens_map_file: next(),
        tokenizer_config_file: next(),
    };
    let model = UserDefinedEmbeddingModel::new(onnx, tokenizer).with_pooling(Pooling::Mean);
    let builds = (0..workers).map(|_| {
        let model = model.clone();
        super::niced(move || {
            // ponytail: other execution targets (CUDA/CoreML/DirectML via ort)
            // would be added to these options with `with_execution_providers`,
            // chosen by `ExecutionTarget`. Windows does not hand thread
            // priority down to ONNX Runtime's pool; ort's ThreadManager could,
            // but fastembed does not expose it. Until then Leggero (1 thread)
            // is the quiet choice there.
            let options = InitOptionsUserDefined::new()
                .with_max_length(spec.max_tokens)
                .with_intra_threads(threads);
            TextEmbedding::try_new_from_user_defined(model, options)
                .map(|model| Onnx { model, spec })
                .map_err(|e| DomainError::Provider(format!("loading the embedding model: {e}")))
        })
    });
    futures::future::try_join_all(builds)
        .await?
        .into_iter()
        .collect()
}

/// The file's bytes: from `dir` if they match the checksum, else downloaded.
async fn fetch(
    spec: &ModelSpec,
    dir: &Path,
    name: &str,
    sha256: &str,
) -> Result<Vec<u8>, DomainError> {
    let path = dir.join(spec.revision).join(name);
    if let Ok(bytes) = tokio::fs::read(&path).await
        && matches(&bytes, sha256)
    {
        return Ok(bytes);
    }
    let url = format!(
        "https://huggingface.co/{}/resolve/{}/{name}",
        spec.repo, spec.revision
    );
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
