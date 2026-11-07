use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{
    address_book_to_proto, application_error_to_status, contact_fields_from_proto,
    domain_error_to_status, harvested_contact_to_card, parse_account_id, remote_contact_to_card,
    remote_contact_to_proto,
};
use crate::grpc::proto::{
    BackfillContactsRequest, BackfillContactsResponse, ContactDuplicatePair,
    ContactSearchHit as ProtoContactSearchHit, CreateRemoteContactRequest,
    CreateRemoteContactResponse, DeleteRemoteContactRequest, DeleteRemoteContactResponse,
    FindContactDuplicatesRequest, FindContactDuplicatesResponse, GetRemoteContactRequest,
    ListAddressBooksRequest, ListAddressBooksResponse, ListHarvestedContactsRequest,
    ListHarvestedContactsResponse, ListRemoteContactsRequest, ListRemoteContactsResponse,
    LookupContactRequest, LookupContactResponse, MergeContactsRequest, MergeContactsResponse,
    RebuildContactIndexRequest, RebuildContactIndexResponse, RemoteContact, SearchContactsRequest,
    SearchContactsResponse, UpdateRemoteContactRequest, UpdateRemoteContactResponse,
    contact_service_server::ContactService,
};
use mc_application::{
    bus::{CommandBus, QueryBus},
    commands::{Command, CommandResult},
    queries::Query,
};
use mc_domain::{
    AccountId, CollectionId, ContactId, ContactReadRepository, DavReadRepository, EmailAddress,
};

/// Contact service implementation — exposes address-book search, duplicate
/// detection, and merge over gRPC.
pub struct ContactServiceImpl<Q: QueryBus + 'static, C: CommandBus + 'static> {
    query_bus: Arc<Q>,
    command_bus: Arc<C>,
    dav: Arc<dyn DavReadRepository>,
    contacts: Arc<dyn ContactReadRepository>,
}

impl<Q: QueryBus + 'static, C: CommandBus + 'static> ContactServiceImpl<Q, C> {
    pub fn new(
        query_bus: Arc<Q>,
        command_bus: Arc<C>,
        dav: Arc<dyn DavReadRepository>,
        contacts: Arc<dyn ContactReadRepository>,
    ) -> Self {
        Self {
            query_bus,
            command_bus,
            dav,
            contacts,
        }
    }

    async fn address_book_name(
        &self,
        account: &AccountId,
        collection: &CollectionId,
    ) -> Result<Option<String>, Status> {
        let books = self
            .dav
            .list_address_books(account)
            .await
            .map_err(domain_error_to_status)?;
        Ok(books
            .into_iter()
            .find(|b| b.collection == *collection)
            .map(|b| b.name))
    }
}

#[tonic::async_trait]
impl<Q: QueryBus + 'static, C: CommandBus + 'static> ContactService for ContactServiceImpl<Q, C> {
    async fn search_contacts(
        &self,
        request: Request<SearchContactsRequest>,
    ) -> Result<Response<SearchContactsResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 {
            10
        } else {
            req.limit as usize
        };

        let requesting_account_id = req
            .requesting_account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .query_bus
            .dispatch(Query::SearchContacts {
                query: req.query,
                limit,
                requesting_account_id,
            })
            .await
            .map_err(application_error_to_status)?;

        let hits = result
            .into_contact_hits()
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_hits = hits.into_iter().map(domain_hit_to_proto).collect();
        Ok(Response::new(SearchContactsResponse { hits: proto_hits }))
    }

    async fn find_contact_duplicates(
        &self,
        request: Request<FindContactDuplicatesRequest>,
    ) -> Result<Response<FindContactDuplicatesResponse>, Status> {
        let req = request.into_inner();
        let account_id = req
            .account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .query_bus
            .dispatch(Query::FindContactDuplicates { account_id })
            .await
            .map_err(application_error_to_status)?;

        let pairs = result
            .into_contact_duplicates()
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_pairs = pairs
            .into_iter()
            .map(|(keep, discard)| ContactDuplicatePair {
                keep: Some(domain_hit_to_proto(keep)),
                discard: Some(domain_hit_to_proto(discard)),
            })
            .collect();

        Ok(Response::new(FindContactDuplicatesResponse {
            pairs: proto_pairs,
        }))
    }

    async fn merge_contacts(
        &self,
        request: Request<MergeContactsRequest>,
    ) -> Result<Response<MergeContactsResponse>, Status> {
        let req = request.into_inner();

        let keep_id = parse_contact_id(&req.keep_id)?;
        let discard_id = parse_contact_id(&req.discard_id)?;

        self.command_bus
            .dispatch(Command::MergeContacts {
                keep_id,
                discard_id,
            })
            .await
            .map_err(application_error_to_status)?;

        Ok(Response::new(MergeContactsResponse {}))
    }

    async fn rebuild_contact_index(
        &self,
        request: Request<RebuildContactIndexRequest>,
    ) -> Result<Response<RebuildContactIndexResponse>, Status> {
        let req = request.into_inner();
        let account_id = req
            .account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .command_bus
            .dispatch(Command::RebuildContactIndex { account_id })
            .await
            .map_err(application_error_to_status)?;

        let indexed_count = match result {
            mc_application::commands::CommandResult::ContactIndexRebuilt { indexed_count } => {
                indexed_count
            }
            _ => 0,
        };

        Ok(Response::new(RebuildContactIndexResponse { indexed_count }))
    }

    async fn backfill_contacts_from_messages(
        &self,
        request: Request<BackfillContactsRequest>,
    ) -> Result<Response<BackfillContactsResponse>, Status> {
        let req = request.into_inner();
        let account_id = req
            .account_id
            .filter(|s| !s.is_empty())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(AccountId::from_uuid);

        let result = self
            .command_bus
            .dispatch(Command::BackfillContactsFromMessages { account_id })
            .await
            .map_err(application_error_to_status)?;

        let (new_contacts, already_existed) = match result {
            mc_application::commands::CommandResult::ContactsBackfilled {
                new_contacts,
                already_existed,
            } => (new_contacts, already_existed),
            _ => (0, 0),
        };

        Ok(Response::new(BackfillContactsResponse {
            new_contacts,
            already_existed,
        }))
    }

    async fn list_address_books(
        &self,
        request: Request<ListAddressBooksRequest>,
    ) -> Result<Response<ListAddressBooksResponse>, Status> {
        let account = parse_account_id(&request.into_inner().account_id)?;
        let books = self
            .dav
            .list_address_books(&account)
            .await
            .map_err(domain_error_to_status)?;
        Ok(Response::new(ListAddressBooksResponse {
            address_books: books.iter().map(address_book_to_proto).collect(),
        }))
    }

    async fn list_remote_contacts(
        &self,
        request: Request<ListRemoteContactsRequest>,
    ) -> Result<Response<ListRemoteContactsResponse>, Status> {
        let req = request.into_inner();
        let account = parse_account_id(&req.account_id)?;
        let book = req.address_book.map(CollectionId::new);
        let contacts = self
            .dav
            .remote_contacts(&account, book.as_ref())
            .await
            .map_err(domain_error_to_status)?;
        Ok(Response::new(ListRemoteContactsResponse {
            contacts: contacts.iter().map(remote_contact_to_proto).collect(),
        }))
    }

    async fn get_remote_contact(
        &self,
        request: Request<GetRemoteContactRequest>,
    ) -> Result<Response<RemoteContact>, Status> {
        let id = parse_contact_id(&request.into_inner().contact_id)?;
        let contact = self
            .dav
            .remote_contact(&id)
            .await
            .map_err(domain_error_to_status)?
            .ok_or_else(|| Status::not_found(format!("contact {id} not found")))?;
        Ok(Response::new(remote_contact_to_proto(&contact)))
    }

    async fn create_remote_contact(
        &self,
        request: Request<CreateRemoteContactRequest>,
    ) -> Result<Response<CreateRemoteContactResponse>, Status> {
        let req = request.into_inner();
        let fields = contact_fields_from_proto(req.fields.unwrap_or_default())
            .map_err(Status::invalid_argument)?;
        let result = self
            .command_bus
            .dispatch(Command::AddRemoteContact {
                account_id: parse_account_id(&req.account_id)?,
                book: CollectionId::new(req.address_book),
                fields,
            })
            .await
            .map_err(application_error_to_status)?;
        let CommandResult::ContactAdded { contact_id } = result else {
            return Err(Status::internal("unexpected command result"));
        };
        Ok(Response::new(CreateRemoteContactResponse {
            contact_id: contact_id.to_string(),
        }))
    }

    async fn update_remote_contact(
        &self,
        request: Request<UpdateRemoteContactRequest>,
    ) -> Result<Response<UpdateRemoteContactResponse>, Status> {
        let req = request.into_inner();
        let fields = contact_fields_from_proto(req.fields.unwrap_or_default())
            .map_err(Status::invalid_argument)?;
        self.command_bus
            .dispatch(Command::UpdateRemoteContact {
                contact_id: parse_contact_id(&req.contact_id)?,
                fields,
            })
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(UpdateRemoteContactResponse {}))
    }

    async fn delete_remote_contact(
        &self,
        request: Request<DeleteRemoteContactRequest>,
    ) -> Result<Response<DeleteRemoteContactResponse>, Status> {
        let contact_id = parse_contact_id(&request.into_inner().contact_id)?;
        self.command_bus
            .dispatch(Command::RemoveRemoteContact { contact_id })
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(DeleteRemoteContactResponse {}))
    }

    async fn lookup_contact(
        &self,
        request: Request<LookupContactRequest>,
    ) -> Result<Response<LookupContactResponse>, Status> {
        let req = request.into_inner();
        let email = EmailAddress::new(req.email.trim()).map_err(|_| {
            Status::invalid_argument(format!("invalid email address: {}", req.email))
        })?;
        let preferred = req
            .account_id
            .filter(|a| !a.is_empty())
            .map(|a| parse_account_id(&a))
            .transpose()?;

        let remote = self
            .dav
            .remote_contacts_by_email(&email)
            .await
            .map_err(domain_error_to_status)?;
        let found = remote
            .iter()
            .find(|c| Some(c.binding.account) == preferred)
            .or(remote.first());
        if let Some(contact) = found {
            let book = self
                .address_book_name(&contact.binding.account, &contact.binding.collection)
                .await?;
            return Ok(Response::new(LookupContactResponse {
                card: Some(remote_contact_to_card(contact, book)),
            }));
        }

        let harvested = self
            .contacts
            .find_by_email(&email)
            .await
            .map_err(domain_error_to_status)?;
        let found = harvested
            .iter()
            .find(|c| c.account_scope.is_some() && c.account_scope == preferred)
            .or(harvested.first());
        Ok(Response::new(LookupContactResponse {
            card: found.map(harvested_contact_to_card),
        }))
    }

    async fn list_harvested_contacts(
        &self,
        _request: Request<ListHarvestedContactsRequest>,
    ) -> Result<Response<ListHarvestedContactsResponse>, Status> {
        let contacts = self
            .contacts
            .list_unsynced_contacts()
            .await
            .map_err(domain_error_to_status)?;
        Ok(Response::new(ListHarvestedContactsResponse {
            contacts: contacts.iter().map(harvested_contact_to_card).collect(),
        }))
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn domain_hit_to_proto(h: mc_domain::ContactSearchHit) -> ProtoContactSearchHit {
    ProtoContactSearchHit {
        id: h.id.to_string(),
        display_name: h.display_name,
        primary_email: h.primary_email,
        all_emails: h.all_emails,
        organization: h.organization,
        account_scope: h.account_scope.map(|a| a.to_string()).unwrap_or_default(),
        is_cross_account: h.is_cross_account,
    }
}

fn parse_contact_id(s: &str) -> Result<ContactId, Status> {
    uuid::Uuid::parse_str(s)
        .map(ContactId::from_uuid)
        .map_err(|_| Status::invalid_argument(format!("invalid contact id: {s}")))
}

#[cfg(test)]
mod tests;
