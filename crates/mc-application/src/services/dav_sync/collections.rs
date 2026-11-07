//! The collection list of one pass: what the server lists against what the
//! account has, and what a vanished collection leaves behind.

use tracing::warn;

use mc_domain::{AggregateType, CollectionId, DomainEvent, PendingPush};

use super::{Ctx, DavSyncService, Key, Kind, Target, stops_the_pass};
use crate::error::ApplicationError;

/// A collection the server lists.
pub(super) struct Listed {
    pub id: CollectionId,
    pub read_only: bool,
    /// A calendar holding only tasks is listed but not synced.
    pub syncs: bool,
}

struct Plan {
    listed: Vec<Listed>,
    /// Discovered or updated collections, with the type to append them under.
    upserts: Vec<(CollectionId, DomainEvent)>,
    gone: Vec<CollectionId>,
}

impl DavSyncService {
    /// Brings the account's list of collections up to date and returns the
    /// server's. `None` when the server could not be asked: a failed listing
    /// must not read as every collection being removed.
    pub(super) async fn reconcile_collections(
        &self,
        ctx: &Ctx,
        kind: Kind,
    ) -> Result<Option<Vec<Listed>>, ApplicationError> {
        let Some(plan) = self.plan_collections(ctx, kind).await? else {
            return Ok(None);
        };
        let ty = match kind {
            Kind::Contacts => AggregateType::AddressBook,
            Kind::Calendar => AggregateType::Calendar,
        };
        let key = |id: &CollectionId| Key::collection(ty, ctx.account, id);
        for (id, event) in plan.upserts {
            self.append(&key(&id), None, vec![event]).await?;
        }
        for id in plan.gone {
            self.release(ctx, &Target::new(kind, id.clone()), key(&id))
                .await?;
        }
        Ok(Some(plan.listed))
    }

    async fn plan_collections(
        &self,
        ctx: &Ctx,
        kind: Kind,
    ) -> Result<Option<Plan>, ApplicationError> {
        let mut plan = Plan {
            listed: Vec::new(),
            upserts: Vec::new(),
            gone: Vec::new(),
        };
        match kind {
            Kind::Contacts => {
                let remote = match ctx.backend.list_address_books().await {
                    Ok(remote) => remote,
                    Err(e) => return skipped(e),
                };
                let local = self.store.list_address_books(&ctx.account).await?;
                for book in &remote {
                    let known = local.iter().find(|l| l.collection == book.collection);
                    if known != Some(book) {
                        let book = book.clone();
                        plan.upserts.push((
                            book.collection.clone(),
                            match known {
                                None => DomainEvent::AddressBookDiscovered { book },
                                Some(_) => DomainEvent::AddressBookUpdated { book },
                            },
                        ));
                    }
                    plan.listed.push(Listed {
                        id: book.collection.clone(),
                        read_only: book.read_only,
                        syncs: true,
                    });
                }
                plan.gone = local
                    .into_iter()
                    .map(|l| l.collection)
                    .filter(|id| !remote.iter().any(|r| r.collection == *id))
                    .collect();
            }
            Kind::Calendar => {
                let remote = match ctx.backend.list_calendars().await {
                    Ok(remote) => remote,
                    Err(e) => return skipped(e),
                };
                let local = self.store.list_calendars(&ctx.account).await?;
                for calendar in &remote {
                    let known = local.iter().find(|l| l.collection == calendar.collection);
                    if known != Some(calendar) {
                        let calendar = calendar.clone();
                        plan.upserts.push((
                            calendar.collection.clone(),
                            match known {
                                None => DomainEvent::CalendarDiscovered { calendar },
                                Some(_) => DomainEvent::CalendarUpdated { calendar },
                            },
                        ));
                    }
                    plan.listed.push(Listed {
                        id: calendar.collection.clone(),
                        read_only: calendar.read_only,
                        syncs: calendar.holds_events(),
                    });
                }
                plan.gone = local
                    .into_iter()
                    .map(|l| l.collection)
                    .filter(|id| !remote.iter().any(|r| r.collection == *id))
                    .collect();
            }
        }
        Ok(Some(plan))
    }

    /// Removes a collection the server no longer lists. Its edits the server
    /// never saw go with it, so each first records what is lost.
    async fn release(
        &self,
        ctx: &Ctx,
        target: &Target,
        collection: Key,
    ) -> Result<(), ApplicationError> {
        for p in self.pending(ctx, target).await? {
            self.record_loss(ctx, target.kind, &p, None).await?;
        }
        let removed = ctx.collection_removed(target);
        self.append(&collection, None, vec![removed]).await?;
        self.store
            .delete_cursor(&ctx.account, &target.cursor_key())
            .await?;
        Ok(())
    }

    /// A local edit that cannot reach the server: lost to its copy.
    pub(super) async fn discard_pending(
        &self,
        ctx: &Ctx,
        target: &Target,
        pending: Vec<PendingPush>,
    ) -> Result<(), ApplicationError> {
        for p in pending {
            let remote = ctx.fetch_one(target, &p.binding.href).await;
            match remote {
                Ok(remote) => self.remote_wins(ctx, target, &p, remote.as_ref()).await?,
                Err(e) if stops_the_pass(&e) => return Err(e.into()),
                Err(e) => {
                    warn!(href = %p.binding.href, "could not fetch for a read-only discard: {e}")
                }
            }
        }
        Ok(())
    }
}

fn skipped(e: mc_domain::DomainError) -> Result<Option<Plan>, ApplicationError> {
    if stops_the_pass(&e) {
        return Err(e.into());
    }
    warn!("DAV collection listing failed, skipping: {e}");
    Ok(None)
}
