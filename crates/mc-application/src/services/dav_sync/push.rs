//! Local edits to the server: conditional writes, and what a refused
//! precondition means (last write wins, spec §4).

use tracing::{debug, warn};

use mc_domain::{
    DeleteOutcome, DomainError, Etag, Href, PendingChange, PendingPush, PutOutcome, RemoteResource,
    ResourcePut,
};

use super::events::{modified_at, same_body, superseded, uid_of};
use super::{Ctx, DavSyncService, Key, Target, stops};
use crate::error::ApplicationError;

/// How a refused precondition ended.
enum Settled {
    Done,
    /// Send it again, built on this version (`None`: as a create).
    Retry(Option<Etag>),
}

impl DavSyncService {
    /// Sends each of `pending`, or just the aggregate `only` names. A server
    /// that stops answering stops the rest; anything else refused for one
    /// resource is recorded and skipped, to be retried by the next pass.
    pub(super) async fn push_all(
        &self,
        ctx: &Ctx,
        t: &Target,
        pending: Vec<PendingPush>,
        only: Option<&str>,
    ) -> Result<(), ApplicationError> {
        for p in pending
            .iter()
            .filter(|p| only.is_none_or(|id| p.aggregate_id == id))
        {
            let pushed = match &p.change {
                PendingChange::Put { raw } => self.push_put(ctx, t, p, raw).await,
                PendingChange::Delete => self.push_delete(ctx, t, p).await,
            };
            match pushed {
                Err(e) if stops(&e) => return Err(e),
                Err(e) => {
                    warn!(href = %p.binding.href, "push refused: {e}");
                    self.record_push_failure(ctx, t.kind, p, e.to_string())
                        .await?;
                }
                Ok(()) => {}
            }
        }
        Ok(())
    }

    async fn push_put(
        &self,
        ctx: &Ctx,
        t: &Target,
        p: &PendingPush,
        raw: &str,
    ) -> Result<(), ApplicationError> {
        let put = |if_match: Option<Etag>| ResourcePut {
            href: p.binding.href.clone(),
            raw: raw.to_owned(),
            if_match,
        };
        let mut outcome = ctx.put(t, put(p.binding.etag.clone())).await?;
        if outcome == PutOutcome::PreconditionFailed {
            match self.settle_conflict(ctx, t, p, raw).await? {
                Settled::Done => return Ok(()),
                // Once per pass: a second refusal waits for the next one.
                Settled::Retry(etag) => outcome = ctx.put(t, put(etag)).await?,
            }
        }
        match outcome {
            PutOutcome::Stored { href, etag, raw } => {
                self.acknowledge(ctx, t, p, href, etag, raw).await
            }
            PutOutcome::PreconditionFailed => {
                debug!(href = %p.binding.href, "changed again while settling, left for the next pass");
                Ok(())
            }
        }
    }

    async fn push_delete(
        &self,
        ctx: &Ctx,
        t: &Target,
        p: &PendingPush,
    ) -> Result<(), ApplicationError> {
        let href = &p.binding.href;
        let etag =
            p.binding.etag.clone().ok_or_else(|| {
                ApplicationError::Internal(format!("delete of {href} has no etag"))
            })?;
        let mut outcome = ctx.delete(t, href, &etag).await?;
        if outcome == DeleteOutcome::PreconditionFailed {
            match ctx.fetch_one(t, href).await? {
                None => outcome = DeleteOutcome::Gone,
                Some(remote) if self.local_is_newer(t, p, &remote) => {
                    outcome = ctx.delete(t, href, &remote.etag).await?;
                }
                Some(remote) => return self.remote_wins(ctx, t, p, Some(&remote)).await,
            }
        }
        match outcome {
            DeleteOutcome::Deleted | DeleteOutcome::Gone => {
                self.acknowledge(ctx, t, p, href.clone(), etag, None).await
            }
            DeleteOutcome::PreconditionFailed => {
                debug!(%href, "changed again while settling, left for the next pass");
                Ok(())
            }
        }
    }

    /// The server refused an overwrite: look at what it holds now.
    async fn settle_conflict(
        &self,
        ctx: &Ctx,
        t: &Target,
        p: &PendingPush,
        raw: &str,
    ) -> Result<Settled, ApplicationError> {
        let Some(remote) = ctx.fetch_one(t, &p.binding.href).await? else {
            // Deleted on the server. The removal's real time is unknown, so
            // the edit wins whenever it was made: re-creating loses nothing.
            return Ok(Settled::Retry(None));
        };
        if p.binding.etag.is_none() {
            return self.bind_by_uid(ctx, t, p, raw, remote).await;
        }
        if self.local_is_newer(t, p, &remote) {
            return Ok(Settled::Retry(Some(remote.etag)));
        }
        self.remote_wins(ctx, t, p, Some(&remote)).await?;
        Ok(Settled::Done)
    }

    /// Without a timestamp the server's copy wins: the edit is kept in the
    /// history, and nothing says the server's copy is the older one.
    fn local_is_newer(&self, t: &Target, p: &PendingPush, remote: &RemoteResource) -> bool {
        modified_at(t.kind, &remote.raw).is_some_and(|at| p.edited_at > at)
    }

    /// A create the server already holds: the crash left it stored but never
    /// acknowledged, so it is bound, not a conflict.
    async fn bind_by_uid(
        &self,
        ctx: &Ctx,
        t: &Target,
        p: &PendingPush,
        raw: &str,
        remote: RemoteResource,
    ) -> Result<Settled, ApplicationError> {
        let uid = uid_of(t.kind, &remote.raw);
        if uid.is_none() || uid != uid_of(t.kind, raw) {
            return Err(DomainError::Provider(format!(
                "{} is taken by another resource",
                p.binding.href
            ))
            .into());
        }
        if !same_body(&remote.raw, raw) {
            // Edited after the create: what the server holds is the older one.
            return Ok(Settled::Retry(Some(remote.etag)));
        }
        self.acknowledge(ctx, t, p, remote.href, remote.etag, None)
            .await?;
        Ok(Settled::Done)
    }

    /// The server took the local version `p.sequence`. A body it rewrote
    /// replaces the local one, unless an edit has been made since: that one
    /// is still to push, and wins.
    pub(super) async fn acknowledge(
        &self,
        ctx: &Ctx,
        t: &Target,
        p: &PendingPush,
        href: Href,
        etag: Etag,
        rewritten: Option<String>,
    ) -> Result<(), ApplicationError> {
        let key = Key::of_pending(t.kind, ctx.account, p)?;
        let at = self.events.current_sequence(&key.aggregate_id).await?;
        let ack = self.pushed_event(ctx, &key, href.clone(), etag.clone(), p.sequence);
        self.append(&key, Some(at), vec![ack]).await?;
        let Some(raw) = rewritten.filter(|_| at == p.sequence) else {
            return Ok(());
        };
        let stored = RemoteResource { href, etag, raw };
        superseded(
            self.apply_remote(ctx, t, Some(&key), Some(at + 1), &stored)
                .await,
        )
    }

    /// The server's version replaces the local edit, which stays in the
    /// history.
    pub(super) async fn remote_wins(
        &self,
        ctx: &Ctx,
        t: &Target,
        p: &PendingPush,
        remote: Option<&RemoteResource>,
    ) -> Result<(), ApplicationError> {
        let Some(mut at) = self.still_pending(ctx, t, p).await? else {
            return Ok(());
        };
        let key = Key::of_pending(t.kind, ctx.account, p)?;
        let both_deleted = p.change == PendingChange::Delete && remote.is_none();
        if !both_deleted {
            superseded(self.record_loss(ctx, t.kind, p, Some(at)).await)?;
            at += 1;
        }
        let removed = self.removed_event(ctx, &key, &p.binding.href);
        superseded(match (&p.change, remote) {
            (PendingChange::Put { .. }, Some(remote)) => {
                self.apply_remote(ctx, t, Some(&key), Some(at), remote).await
            }
            (_, None) => self.append(&key, Some(at), vec![removed]).await,
            // The removal clears the pending delete; the resource comes back
            // as a new one.
            (PendingChange::Delete, Some(remote)) => {
                self.append(&key, Some(at), vec![removed]).await?;
                self.apply_remote(ctx, t, None, None, remote).await
            }
        })
    }
}
