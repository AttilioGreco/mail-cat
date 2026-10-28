//! Command handler for Contact aggregate operations.

use std::sync::Arc;

use mc_domain::{
    AccountId, AggregateType, Contact, ContactEmail, ContactEmailLabel, ContactId,
    ContactReadRepository, ContactSource, ContactWriteRepository, DomainEvent, EmailAddress,
    EventEnvelope,
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
pub struct ContactCommandHandler<W, B>
where
    W: ContactWriteRepository + ContactReadRepository,
    B: EventPublisher,
{
    write_repo: Arc<W>,
    event_bus: Arc<B>,
}

impl<W, B> ContactCommandHandler<W, B>
where
    W: ContactWriteRepository + ContactReadRepository,
    B: EventPublisher,
{
    pub fn new(write_repo: Arc<W>, event_bus: Arc<B>) -> Self {
        Self {
            write_repo,
            event_bus,
        }
    }

    #[allow(clippy::too_many_arguments)]
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

        let contact_id = contact.id;
        self.write_repo.save(&contact, vec![event.clone()]).await?;
        self.publish_event(contact_id.to_string(), event);

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

        let contact_id = contact.id;
        self.write_repo.save(&contact, vec![event.clone()]).await?;
        self.publish_event(contact_id.to_string(), event);

        info!(%contact_id, "contact auto-created from email activity");
        Ok(CommandResult::ContactEnsured {
            created: true,
            contact_id,
        })
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
        self.write_repo
            .save(&kept, vec![merge_event.clone()])
            .await?;
        self.write_repo
            .save(&discarded, vec![remove_event.clone()])
            .await?;

        self.publish_event(keep_id.to_string(), merge_event);
        self.publish_event(discard_id.to_string(), remove_event);

        info!(%keep_id, %discard_id, "contacts merged");
        Ok(CommandResult::ContactsMerged)
    }

    /// Contact search reads the read model directly, so there is no index
    /// left to rebuild; kept so existing clients get a count, not an error.
    pub async fn handle_rebuild_index(
        &self,
        _account_id: Option<AccountId>,
    ) -> Result<CommandResult, ApplicationError> {
        let total = self
            .write_repo
            .list_contacts(&Pagination::new(1, 1))
            .await?
            .total_count;
        Ok(CommandResult::ContactIndexRebuilt {
            indexed_count: u32::try_from(total).unwrap_or(u32::MAX),
        })
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
