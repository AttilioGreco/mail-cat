//! Teaching the model: from the labels mail already has, then message by
//! message as they change.

use std::time::Duration;

use chrono::Utc;
use mc_domain::{
    Example, Lesson, MailboxReadRepository, Scope, SourceFilter, TagSource, TaggingStore,
};

use super::{TaggingService, features};
use crate::bus::CommandBus;
use crate::error::ApplicationError;

/// Unlabelled mail only says "probably not this label": the user may simply
/// not have got round to it.
pub(super) const UNLABELLED_WEIGHT: f64 = 0.3;
/// How much of the newest unlabelled mail is taken as that background.
pub(super) const UNLABELLED_SAMPLE: usize = 2000;
const PAGE: u32 = 200;
/// A label gone from a message this long after we applied it was taken off
/// by someone; sooner, the change may simply not have arrived yet.
const AUTO_SETTLE: chrono::Duration = chrono::Duration::minutes(5);

impl<S, R, C> TaggingService<S, R, C>
where
    S: TaggingStore + 'static,
    R: MailboxReadRepository + 'static,
    C: CommandBus + 'static,
{
    /// Train every scope that has not been trained from its existing labels.
    pub(super) async fn bootstrap(&self) -> Result<(), ApplicationError> {
        for scope in self.store.scopes().await? {
            if !self.store.is_trained(scope).await? {
                self.train_scope(scope).await?;
            }
        }
        Ok(())
    }

    /// Forget the scope's counts and teach them again from the mail as it is.
    pub(crate) async fn train_scope(&self, scope: Scope) -> Result<(), ApplicationError> {
        self.store.reset(scope).await?;
        let accounts = self.store.scope_accounts(scope).await?;
        let mut cursor = None;
        loop {
            let page = self
                .store
                .source_page(&accounts, SourceFilter::Labelled, cursor, false, PAGE)
                .await?;
            let Some(last) = page.last().map(|s| s.id) else {
                break;
            };
            cursor = Some(last);
            for source in &page {
                self.retrain(source, scope, None, false).await?;
            }
            // The writer is shared with sync; leave it room.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut cursor = None;
        let mut taken = 0;
        while taken < UNLABELLED_SAMPLE {
            let page = self
                .store
                .source_page(&accounts, SourceFilter::Unlabelled, cursor, true, PAGE)
                .await?;
            let Some(last) = page.last().map(|s| s.id) else {
                break;
            };
            cursor = Some(last);
            for source in page.iter().take(UNLABELLED_SAMPLE - taken) {
                self.retrain(source, scope, None, true).await?;
                taken += 1;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.store.mark_trained(scope).await?;
        Ok(())
    }

    /// Bring what the model learnt from this message in line with how it is
    /// now: the old lesson is withdrawn and the new one taught. `reject` adds
    /// a label the message was shown to not belong under. `background`
    /// teaches an unlabelled message that has taught nothing before.
    pub(super) async fn retrain(
        &self,
        source: &TagSource,
        scope: Scope,
        reject: Option<&str>,
        background: bool,
    ) -> Result<(), ApplicationError> {
        // A zero weight is a rejection kept through a reset: its lesson is gone.
        let previous = self.store.example(source.id).await?;

        let mut negatives = previous
            .as_ref()
            .map(|e| e.negatives.clone())
            .unwrap_or_default();
        let mut ours = Vec::new();
        for record in self.store.autos(source.id).await? {
            if source.labels.contains(&record.label) {
                ours.push(record.label);
            } else if Utc::now() - record.at > AUTO_SETTLE
                && self.store.take_auto(source.id, &record.label).await?
                // A keyword store the server ignored leaves nothing to
                // remove later; only a server known to keep them can be
                // taken to have had the label taken off.
                && source.can_label()
            {
                negatives.push(record.label);
            }
        }
        negatives.extend(reject.map(str::to_owned));
        // Labels we put there say nothing about what the user wants.
        let positives: Vec<String> = source
            .labels
            .iter()
            .filter(|l| !ours.contains(l))
            .cloned()
            .collect();
        negatives.retain(|l| !positives.contains(l));
        negatives.sort();
        negatives.dedup();

        let next = Example {
            message_id: source.id,
            account_id: source.account_id,
            scope: scope.key(),
            weight: if positives.is_empty() {
                UNLABELLED_WEIGHT
            } else {
                1.0
            },
            with_body: source.body.is_some(),
            positives,
            negatives,
        };
        match &previous {
            Some(old) if *old == next => return Ok(()),
            None if next.positives.is_empty() && next.negatives.is_empty() && !background => {
                return Ok(());
            }
            _ => {}
        }
        if let Some(old) = &previous
            && old.weight != 0.0
            && let Some(old_scope) = Scope::from_key(&old.scope)
        {
            self.teach(old_scope, source, old, -old.weight).await?;
        }
        self.teach(scope, source, &next, next.weight).await?;
        self.store.put_example(&next).await?;
        Ok(())
    }

    async fn teach(
        &self,
        scope: Scope,
        source: &TagSource,
        example: &Example,
        weight: f64,
    ) -> Result<(), ApplicationError> {
        let tokens = features::tokens(source, example.with_body);
        self.store
            .teach(
                scope,
                Lesson {
                    tokens: &tokens,
                    positives: &example.positives,
                    negatives: &example.negatives,
                    weight,
                },
            )
            .await?;
        Ok(())
    }
}
