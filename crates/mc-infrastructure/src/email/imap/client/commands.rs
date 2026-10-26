//! Wire-level command helpers: timeouts, STORE, MOVE/COPY, expunge, capabilities.

use std::sync::OnceLock;
use std::time::Duration;

use async_imap::imap_proto::types::{AttributeValue, Response, SectionPath};

use mc_domain::error::DomainError;
use mc_domain::value_objects::uid::ProviderUid;

use super::fetch::{TextPart, text_parts};
use super::{BODY_FETCH_TIMEOUT, COMMAND_TIMEOUT, ImapEmailProvider, UPLOAD_FLOOR_BYTES_PER_SEC};

/// `fut` within `limit`. A server that stops answering fails as a typed
/// timeout — a transient network fault to the callers' backoff — and the
/// connection, dropped with the caller's `?`, never goes back to the pool.
pub(super) async fn timed<T>(
    limit: Duration,
    what: &str,
    fut: impl std::future::Future<Output = Result<T, DomainError>>,
) -> Result<T, DomainError> {
    tokio::time::timeout(limit, fut).await.unwrap_or_else(|_| {
        Err(DomainError::ConnectionTimeout(format!(
            "{what}: no answer in {}s",
            limit.as_secs()
        )))
    })
}

/// One IMAP exchange under [`timed`].
pub(super) async fn exchange<T>(
    limit: Duration,
    what: &str,
    fut: impl std::future::Future<Output = Result<T, async_imap::error::Error>>,
) -> Result<T, DomainError> {
    timed(limit, what, async {
        fut.await.map_err(|e| exchange_error(what, e))
    })
    .await
}

/// What the backfill reads of one FETCH response, copied out because
/// async-imap keeps its response type to itself.
pub(super) struct Fetched {
    pub(super) uid: Option<u32>,
    pub(super) text_parts: Option<Vec<TextPart>>,
    /// `BODY[..]` data by part number; `None` is the whole message.
    sections: Vec<(Option<Vec<u32>>, Vec<u8>)>,
}

impl Fetched {
    fn new(attributes: &[AttributeValue<'_>]) -> Self {
        let mut fetched = Fetched {
            uid: None,
            text_parts: None,
            sections: Vec::new(),
        };
        for attribute in attributes {
            match attribute {
                AttributeValue::Uid(uid) => fetched.uid = Some(*uid),
                AttributeValue::BodyStructure(structure) => {
                    fetched.text_parts = Some(text_parts(structure))
                }
                AttributeValue::BodySection {
                    section: None,
                    data: Some(data),
                    ..
                } => {
                    fetched.sections.push((None, data.to_vec()));
                }
                AttributeValue::BodySection {
                    section: Some(SectionPath::Part(part, None)),
                    data: Some(data),
                    ..
                } => {
                    fetched.sections.push((Some(part.clone()), data.to_vec()));
                }
                _ => {}
            }
        }
        fetched
    }

    pub(super) fn section(&self, part: Option<&[u32]>) -> Option<&[u8]> {
        self.sections
            .iter()
            .find(|(p, _)| p.as_deref() == part)
            .map(|(_, data)| data.as_slice())
    }
}

/// async-imap's `uid_fetch` drops the tagged reply, so a server that refuses
/// the FETCH looks like one with nothing to return. Reading the reply here
/// keeps the refusal.
pub(super) async fn fetch_checked<T>(
    session: &mut async_imap::Session<T>,
    uid_set: &str,
    query: &str,
) -> Result<Vec<Fetched>, async_imap::error::Error>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    use async_imap::error::Error;
    use async_imap::imap_proto::Status;

    let id = session
        .run_command(format!("UID FETCH {uid_set} {query}"))
        .await?;
    let mut fetched = Vec::new();
    loop {
        let response = session
            .read_response()
            .await?
            .ok_or(Error::ConnectionLost)?;
        match response.parsed() {
            Response::Done {
                tag,
                status,
                code,
                information,
                ..
            } if *tag == id => {
                let reply = format!("code: {code:?}, info: {information:?}");
                return match status {
                    Status::Ok => Ok(fetched),
                    Status::No => Err(Error::No(reply)),
                    _ => Err(Error::Bad(reply)),
                };
            }
            Response::Fetch(_, attributes) => fetched.push(Fetched::new(attributes)),
            _ => {}
        }
    }
}

/// A reply the parser rejects is the only failure that points at one message,
/// so it alone is a `Serialization` error; a NO about quota or bandwidth is
/// the server asking for a pause.
fn exchange_error(what: &str, e: async_imap::error::Error) -> DomainError {
    use async_imap::error::Error;
    match e {
        // The stream reports a response it can't parse as an `Other` io error.
        Error::Parse(_) => DomainError::Serialization(format!("{what}: {e}")),
        Error::Io(ref io)
            if io.kind() == std::io::ErrorKind::Other
                && io.to_string().contains(" during parsing of ") =>
        {
            DomainError::Serialization(format!("{what}: {e}"))
        }
        Error::No(ref reply)
            if ["[OVERQUOTA]", "[LIMIT]", "andwidth limit"]
                .iter()
                .any(|n| reply.contains(n)) =>
        {
            DomainError::Throttled(format!("{what}: {reply}"))
        }
        e => DomainError::Provider(format!("{what} failed: {e}")),
    }
}

/// `UID STORE`, its reply read to the end so the connection can be reused.
pub(super) async fn store<T>(
    session: &mut async_imap::Session<T>,
    uid_set: &str,
    change: &str,
    what: &str,
) -> Result<(), DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    use futures::TryStreamExt;

    exchange(COMMAND_TIMEOUT, what, async {
        session
            .uid_store(uid_set, change)
            .await?
            .try_collect::<Vec<_>>()
            .await
    })
    .await
    .map(drop)
}

/// IMAP extensions the adapter adapts to. IMAP4rev2 includes both.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ServerCaps {
    /// RFC 6851 `MOVE`.
    pub(super) move_cmd: bool,
    /// RFC 4315 `UIDPLUS`: `COPYUID` and `UID EXPUNGE`.
    pub(super) uidplus: bool,
}

impl ServerCaps {
    /// `None` without a protocol version: every server lists one, so its
    /// absence means the answer was lost (async-imap ignores a failed tagged
    /// reply here and hands back an empty set).
    pub(super) fn from_names(has: impl Fn(&str) -> bool) -> Option<Self> {
        let rev2 = has("IMAP4rev2");
        (rev2 || has("IMAP4rev1")).then(|| Self {
            move_cmd: rev2 || has("MOVE"),
            uidplus: rev2 || has("UIDPLUS"),
        })
    }
}

/// The server's extensions, asking again when the login could not learn
/// them. Guessing "none" would take the degraded move/delete path on a
/// server that supports better, so an unknown answer refuses the operation.
pub(super) async fn known_caps<T>(
    cell: &OnceLock<ServerCaps>,
    session: &mut async_imap::Session<T>,
) -> Result<ServerCaps, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    if let Some(caps) = cell.get() {
        return Ok(*caps);
    }
    let unknown = |why: &dyn std::fmt::Display| {
        DomainError::Provider(format!(
            "server capabilities unknown, not moving or deleting: {why}"
        ))
    };
    let names = exchange(COMMAND_TIMEOUT, "CAPABILITY", session.capabilities())
        .await
        .map_err(|e| unknown(&e))?;
    let caps = ServerCaps::from_names(|name| names.has_str(name))
        .ok_or_else(|| unknown(&"no protocol version listed"))?;
    Ok(*cell.get_or_init(|| caps))
}

/// An IMAP quoted string.
pub(super) fn quoted(value: &str) -> Result<String, DomainError> {
    if value.contains(['\r', '\n']) {
        return Err(DomainError::Provider(format!(
            "Not sendable in an IMAP command: {value:?}"
        )));
    }
    Ok(format!(
        "\"{}\"",
        value.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

/// Every message's UID and flags in the selected mailbox, a bounded batch
/// per command so a big mailbox is no single huge reply racing one timeout.
pub(super) async fn uid_flags<T>(
    session: &mut async_imap::Session<T>,
) -> Result<Vec<async_imap::types::Fetch>, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    use futures::TryStreamExt;

    let all = exchange(COMMAND_TIMEOUT, "UID SEARCH ALL", session.uid_search("ALL")).await?;
    let all: Vec<u32> = sorted_uids(all).into_iter().map(|u| u.value()).collect();
    let mut fetches = Vec::with_capacity(all.len());
    for chunk in all.chunks(ImapEmailProvider::FLAGS_BATCH_SIZE) {
        let set = ImapEmailProvider::compress_uid_ranges(chunk);
        let batch: Vec<async_imap::types::Fetch> =
            exchange(BODY_FETCH_TIMEOUT, "UID FETCH FLAGS", async {
                session
                    .uid_fetch(&set, "(FLAGS)")
                    .await?
                    .try_collect()
                    .await
            })
            .await?;
        fetches.extend(batch);
    }
    Ok(fetches)
}

/// APPEND's limit: a slow uplink must still get a big attachment through.
pub(super) fn append_timeout(bytes: usize) -> Duration {
    COMMAND_TIMEOUT + Duration::from_secs(bytes as u64 / UPLOAD_FLOOR_BYTES_PER_SEC)
}

/// `HEADER Message-ID` search for `rfc_id`; `None` for an empty one (`""`,
/// `<>`), which as a substring search would match every message.
pub(super) fn message_id_search(rfc_id: &str) -> Result<Option<String>, DomainError> {
    if rfc_id
        .trim_matches(|c: char| c == '<' || c == '>' || c.is_whitespace())
        .is_empty()
    {
        return Ok(None);
    }
    // A copy marked \Deleted is gone as far as sync is concerned (moves
    // without UIDPLUS leave one behind), so it must not be found either.
    Ok(Some(format!(
        "UNDELETED HEADER Message-ID {}",
        quoted(rfc_id)?
    )))
}

pub(super) fn sorted_uids(found: std::collections::HashSet<u32>) -> Vec<ProviderUid> {
    let mut uids: Vec<u32> = found.into_iter().collect();
    uids.sort_unstable();
    uids.into_iter().map(ProviderUid::new).collect()
}

/// Move `uids` (ascending) out of the selected mailbox into `to`, returning
/// `(old, new)` UID pairs the server reports (UIDPLUS `COPYUID`).
pub(super) async fn move_uids<T>(
    session: &mut async_imap::Session<T>,
    caps: ServerCaps,
    uids: &[u32],
    to: &str,
) -> Result<Vec<(u32, u32)>, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let set = ImapEmailProvider::compress_uid_ranges(uids);
    if caps.move_cmd {
        return transfer(session, "MOVE", &set, to).await;
    }
    let copied = transfer(session, "COPY", &set, to).await?;
    store(session, &set, "+FLAGS.SILENT (\\Deleted)", "Delete flag").await?;
    if caps.uidplus {
        uid_expunge(session, &set).await?;
    }
    Ok(copied)
}

/// Flag `uids` (ascending) `\Deleted` in the selected mailbox and, when
/// `permanently` and the server has UIDPLUS, expunge exactly them.
///
/// Never a plain `EXPUNGE`: it purges every `\Deleted` message in the
/// mailbox, including ones another client merely marked (maybe to undo),
/// and checking first is a race. Without UIDPLUS the marked copies stay
/// until some client expunges them; sync already reads `\Deleted` as gone.
pub(super) async fn delete_uids<T>(
    session: &mut async_imap::Session<T>,
    caps: ServerCaps,
    uids: &[u32],
    permanently: bool,
) -> Result<(), DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    let set = ImapEmailProvider::compress_uid_ranges(uids);
    store(session, &set, "+FLAGS.SILENT (\\Deleted)", "Delete flag").await?;
    if permanently && caps.uidplus {
        uid_expunge(session, &set).await?;
    }
    Ok(())
}

async fn uid_expunge<T>(
    session: &mut async_imap::Session<T>,
    uid_set: &str,
) -> Result<(), DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    use futures::TryStreamExt;

    exchange(COMMAND_TIMEOUT, "Expunge", async {
        session
            .uid_expunge(uid_set)
            .await?
            .try_collect::<Vec<_>>()
            .await
    })
    .await
    .map(drop)
}

/// `UID COPY` / `UID MOVE` keeping the `COPYUID` code async-imap's own
/// methods discard. COPY puts it in the tagged OK; MOVE usually in an
/// untagged OK before the expunges (RFC 6851 §4.3), so both are read.
pub(super) async fn transfer<T>(
    session: &mut async_imap::Session<T>,
    verb: &str,
    uid_set: &str,
    to: &str,
) -> Result<Vec<(u32, u32)>, DomainError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin + std::fmt::Debug + Send,
{
    use async_imap::imap_proto::Status;

    let failed = |e: &dyn std::fmt::Display| DomainError::Provider(format!("{verb} failed: {e}"));
    let tag = session
        .run_command(format!("UID {verb} {uid_set} {}", quoted(to)?))
        .await
        .map_err(|e| failed(&e))?;
    let mut copied = Vec::new();
    loop {
        let response = session
            .read_response()
            .await
            .map_err(|e| failed(&e))?
            .ok_or_else(|| failed(&"connection lost"))?;
        match response.parsed() {
            Response::Data {
                status: Status::Ok,
                code,
                ..
            } => copied.extend(copied_uids(code.as_ref())),
            Response::Done {
                tag: done,
                status,
                code,
                information,
            } if *done == tag => {
                if *status != Status::Ok {
                    return Err(failed(&format!("{status:?} {information:?}")));
                }
                copied.extend(copied_uids(code.as_ref()));
                return Ok(copied);
            }
            _ => {}
        }
    }
}

/// `(source, destination)` UID pairs of a `[COPYUID <validity> <source set>
/// <dest set>]` code: the sets list the UIDs in matching order (RFC 4315).
pub(super) fn copied_uids(code: Option<&async_imap::imap_proto::ResponseCode>) -> Vec<(u32, u32)> {
    use async_imap::imap_proto::{ResponseCode, UidSetMember};

    let Some(ResponseCode::CopyUid(_, from, to)) = code else {
        return Vec::new();
    };
    let expand = |set: &[UidSetMember]| -> Vec<u32> {
        set.iter()
            .flat_map(|member| match member {
                UidSetMember::Uid(uid) => *uid..=*uid,
                UidSetMember::UidRange(range) => range.clone(),
            })
            .collect()
    };
    expand(from).into_iter().zip(expand(to)).collect()
}
