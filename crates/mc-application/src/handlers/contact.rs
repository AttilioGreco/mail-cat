//! Command handler for Contact aggregate operations.

use std::sync::Arc;

use mc_domain::{
    AccountId, AggregateType, Contact, ContactEmail, ContactEmailLabel, ContactId, ContactIndex,
    ContactReadRepository, ContactSource, ContactSummary, ContactWriteRepository,
    DomainEvent, EmailAddress, EventEnvelope,
};
use mc_shared::pagination::Pagination;
use tracing::info;

use crate::bus::EventPublisher;
use crate::commands::CommandResult;
use crate::error::ApplicationError;

/// Handles Contact-related commands.
///
/// `W` must implement both read and write repository traits so that
/// `EnsureContact` can check for existence before creating.
pub struct ContactCommandHandler<W, CI, B>
where
    W: ContactWriteRepository + ContactReadRepository,
    CI: ContactIndex,
    B: EventPublisher,
{
    write_repo: Arc<W>,
    contact_index: Arc<CI>,
    event_bus: Arc<B>,
}

impl<W, CI, B> ContactCommandHandler<W, CI, B>
where
    W: ContactWriteRepository + ContactReadRepository,
    CI: ContactIndex,
    B: EventPublisher,
{
    pub fn new(write_repo: Arc<W>, contact_index: Arc<CI>, event_bus: Arc<B>) -> Self {
        Self { write_repo, contact_index, event_bus }
    }

    pub async fn handle_add(
        &self,
        display_name: String,
        emails: Vec<ContactEmail>,
        phones: Vec<String>,
        organization: Option<String>,
        notes: Option<String>,
        source: ContactSource,
        external_id: Option<String>,
        account_scope: Option<AccountId>,
    ) -> Result<CommandResult, ApplicationError> {
        let (contact, event) = Contact::create(
            display_name,
            emails,
            phones,
            organization,
            notes,
            source,
            external_id,
            account_scope,
        )
        .map_err(ApplicationError::Domain)?;

        let contact_id = contact.id.clone();
        self.write_repo.save(&contact, vec![event.clone()]).await?;
        self.publish_event(contact_id.to_string(), event);

        let summary = contact_to_summary(&contact);
        if let Err(e) = self.contact_index.index_contact(&summary).await {
            tracing::warn!(%contact_id, "contact index update failed: {e}");
        }

        info!(%contact_id, "contact added");
        Ok(CommandResult::ContactAdded { contact_id })
    }

    /// Idempotent upsert: creates a `LocalEmail` contact for `email` scoped to
    /// `account_id` if one does not already exist.  No-op if already present.
    pub async fn handle_ensure(
        &self,
        email: EmailAddress,
        display_name: Option<String>,
        account_id: AccountId,
    ) -> Result<CommandResult, ApplicationError> {
        // Check for existing contact with this email in this account
        let existing = self
            .write_repo
            .find_by_email_in_account(&email, &account_id)
            .await?;

        if let Some(summary) = existing.into_iter().next() {
            return Ok(CommandResult::ContactEnsured {
                created: false,
                contact_id: summary.id,
            });
        }

        let primary = ContactEmail::new(email, ContactEmailLabel::Other, true);
        let name = display_name.unwrap_or_default();

        let (contact, event) = Contact::create(
            name,
            vec![primary],
            vec![],
            None,
            None,
            ContactSource::LocalEmail,
            None,
            Some(account_id),
        )
        .map_err(ApplicationError::Domain)?;

        let contact_id = contact.id.clone();
        self.write_repo.save(&contact, vec![event.clone()]).await?;
        self.publish_event(contact_id.to_string(), event);

        let summary = contact_to_summary(&contact);
        if let Err(e) = self.contact_index.index_contact(&summary).await {
            tracing::warn!(%contact_id, "contact index update (ensure) failed: {e}");
        }

        info!(%contact_id, "contact auto-created from email activity");
        Ok(CommandResult::ContactEnsured { created: true, contact_id })
    }

    pub async fn handle_update(
        &self,
        contact_id: ContactId,
        display_name: Option<String>,
        emails: Option<Vec<ContactEmail>>,
        phones: Option<Vec<String>>,
        organization: Option<Option<String>>,
        notes: Option<Option<String>>,
    ) -> Result<CommandResult, ApplicationError> {
        let mut contact = self.load_contact(&contact_id).await?;
        let event = contact
            .update(display_name, emails, phones, organization, notes)
            .map_err(ApplicationError::Domain)?;

        self.write_repo.save(&contact, vec![event.clone()]).await?;
        self.publish_event(contact_id.to_string(), event);

        let summary = contact_to_summary(&contact);
        if let Err(e) = self.contact_index.index_contact(&summary).await {
            tracing::warn!(%contact_id, "contact index update failed: {e}");
        }

        info!(%contact_id, "contact updated");
        Ok(CommandResult::ContactUpdated)
    }

    pub async fn handle_remove(
        &self,
        contact_id: ContactId,
    ) -> Result<CommandResult, ApplicationError> {
        let contact = self.load_contact(&contact_id).await?;
        let event = contact.remove();

        self.write_repo.save(&contact, vec![event.clone()]).await?;
        self.publish_event(contact_id.to_string(), event);

        if let Err(e) = self.contact_index.remove_contact(&contact_id).await {
            tracing::warn!(%contact_id, "contact index remove failed: {e}");
        }

        info!(%contact_id, "contact removed");
        Ok(CommandResult::ContactRemoved)
    }

    /// Absorb `discard_id` into `keep_id`: merge emails, then delete the discarded contact.
    pub async fn handle_merge(
        &self,
        keep_id: ContactId,
        discard_id: ContactId,
    ) -> Result<CommandResult, ApplicationError> {
        let mut kept = self.load_contact(&keep_id).await?;
        let discarded = self.load_contact(&discard_id).await?;

        let merge_event = kept.merge_from(&discarded);
        let remove_event = discarded.remove();

        // Save both events
        self.write_repo.save(&kept, vec![merge_event.clone()]).await?;
        self.write_repo
            .save(&discarded, vec![remove_event.clone()])
            .await?;

        self.publish_event(keep_id.to_string(), merge_event);
        self.publish_event(discard_id.to_string(), remove_event);

        // Update index: re-index kept, remove discarded
        let summary = contact_to_summary(&kept);
        if let Err(e) = self.contact_index.index_contact(&summary).await {
            tracing::warn!(%keep_id, "contact index re-index after merge failed: {e}");
        }
        if let Err(e) = self.contact_index.remove_contact(&discard_id).await {
            tracing::warn!(%discard_id, "contact index remove after merge failed: {e}");
        }

        info!(%keep_id, %discard_id, "contacts merged");
        Ok(CommandResult::ContactsMerged)
    }

    /// Rebuild the Tantivy contact index from the stored contact read models.
    ///
    /// Pages through all contacts in the read repository, optionally filtering
    /// by `account_id`, and re-indexes each one. Returns the number of contacts
    /// that were indexed.
    pub async fn handle_rebuild_index(
        &self,
        account_id: Option<AccountId>,
    ) -> Result<CommandResult, ApplicationError> {
        const PAGE_SIZE: u32 = 100;
        let mut page = 1u32;
        let mut indexed_count = 0u32;

        loop {
            let pagination = Pagination::new(page, PAGE_SIZE);
            let paged = self.write_repo.list_contacts(&pagination).await?;

            for summary in &paged.items {
                // Apply account filter when requested
                if let Some(ref aid) = account_id {
                    if summary.account_scope.as_ref() != Some(aid) {
                        continue;
                    }
                }
                if let Err(e) = self.contact_index.index_contact(summary).await {
                    tracing::warn!(contact_id = %summary.id, "rebuild index: failed to index contact: {e}");
                } else {
                    indexed_count += 1;
                }
            }

            if !paged.has_next_page() {
                break;
            }
            page += 1;
        }

        info!(indexed_count, "contact index rebuilt");
        Ok(CommandResult::ContactIndexRebuilt { indexed_count })
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    async fn load_contact(&self, id: &ContactId) -> Result<Contact, ApplicationError> {
        self.write_repo
            .load(id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound("Contact", id.to_string()))
    }

    fn publish_event(&self, aggregate_id: String, event: DomainEvent) {
        let envelope = EventEnvelope::new(aggregate_id, AggregateType::Contact, 0, event);
        self.event_bus.publish(envelope);
    }
}

/// Build a `ContactSummary` from a fully-loaded `Contact` aggregate.
fn contact_to_summary(c: &Contact) -> ContactSummary {
    let primary_email = c
        .primary_email()
        .cloned()
        .unwrap_or_else(|| c.emails.first().map(|e| e.address.clone()).unwrap());

    ContactSummary {
        id: c.id.clone(),
        display_name: c.display_name.clone(),
        primary_email,
        emails: c.emails.clone(),
        organization: c.organization.clone(),
        source: c.source.clone(),
        account_scope: c.account_scope.clone(),
    }
}
