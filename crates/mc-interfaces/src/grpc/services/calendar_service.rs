use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::grpc::mappers::{
    application_error_to_status, calendar_to_proto, domain_error_to_status, event_edit_from_proto,
    event_to_proto, event_when_from_str, occurrence_to_proto, parse_account_id,
    timestamp_to_datetime,
};
use crate::grpc::proto::{
    CreateEventRequest, CreateEventResponse, DavChange, DavKind, DeleteEventRequest,
    DeleteEventResponse, EventDetail, GetDavStatusRequest, GetDavStatusResponse, GetEventRequest,
    ListCalendarsRequest, ListCalendarsResponse, ListEventsRequest, ListEventsResponse,
    SyncDavNowRequest, SyncDavNowResponse, UpdateEventRequest, UpdateEventResponse,
    WatchDavChangesRequest, calendar_service_server::CalendarService,
};
use mc_application::{
    DavProviders,
    bus::{CommandBus, EventSubscriber, InProcessEventBus},
    commands::{Command, CommandResult},
};
use mc_domain::{
    AccountId, AggregateType, CollectionId, DavReadRepository, EventEnvelope, Href, PendingPush,
};

/// A sync pass appends one event per resource; this is how long the watcher
/// gathers them before telling the client once.
const CHANGE_WINDOW: Duration = Duration::from_millis(750);

/// Calendar service: reads come from the DAV projections, edits go through the
/// command bus so that they are pushed to the server.
pub struct CalendarServiceImpl<C: CommandBus + 'static> {
    command_bus: Arc<C>,
    dav: Arc<dyn DavReadRepository>,
    event_bus: Arc<InProcessEventBus>,
    providers: Arc<DavProviders>,
}

impl<C: CommandBus + 'static> CalendarServiceImpl<C> {
    pub fn new(
        command_bus: Arc<C>,
        dav: Arc<dyn DavReadRepository>,
        event_bus: Arc<InProcessEventBus>,
        providers: Arc<DavProviders>,
    ) -> Self {
        Self {
            command_bus,
            dav,
            event_bus,
            providers,
        }
    }

    /// Every pending change of the account, contacts and events.
    async fn pending(&self, account: &AccountId) -> Result<Vec<PendingPush>, Status> {
        let mut pending = Vec::new();
        for book in self
            .dav
            .list_address_books(account)
            .await
            .map_err(domain_error_to_status)?
        {
            pending.extend(
                self.dav
                    .pending_contacts(account, &book.collection)
                    .await
                    .map_err(domain_error_to_status)?,
            );
        }
        for calendar in self
            .dav
            .list_calendars(account)
            .await
            .map_err(domain_error_to_status)?
        {
            pending.extend(
                self.dav
                    .pending_events(account, &calendar.collection)
                    .await
                    .map_err(domain_error_to_status)?,
            );
        }
        Ok(pending)
    }
}

#[tonic::async_trait]
impl<C: CommandBus + 'static> CalendarService for CalendarServiceImpl<C> {
    async fn list_calendars(
        &self,
        request: Request<ListCalendarsRequest>,
    ) -> Result<Response<ListCalendarsResponse>, Status> {
        let account = parse_account_id(&request.into_inner().account_id)?;
        let calendars = self
            .dav
            .list_calendars(&account)
            .await
            .map_err(domain_error_to_status)?;
        Ok(Response::new(ListCalendarsResponse {
            calendars: calendars.iter().map(calendar_to_proto).collect(),
        }))
    }

    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsResponse>, Status> {
        let req = request.into_inner();
        let account = parse_account_id(&req.account_id)?;
        let (Some(from), Some(to)) = (req.from, req.to) else {
            return Err(Status::invalid_argument("from and to are required"));
        };
        let calendars: Vec<_> = req.calendars.into_iter().map(CollectionId::new).collect();
        let found = self
            .dav
            .events_in_range(
                &account,
                &calendars,
                timestamp_to_datetime(&from),
                timestamp_to_datetime(&to),
            )
            .await
            .map_err(domain_error_to_status)?;
        Ok(Response::new(ListEventsResponse {
            occurrences: found.iter().map(occurrence_to_proto).collect(),
        }))
    }

    async fn get_event(
        &self,
        request: Request<GetEventRequest>,
    ) -> Result<Response<EventDetail>, Status> {
        let req = request.into_inner();
        let account = parse_account_id(&req.account_id)?;
        let href = Href::new(&req.href);
        let event = self
            .dav
            .get_event(&account, &href)
            .await
            .map_err(domain_error_to_status)?
            .ok_or_else(|| Status::not_found(format!("event {} not found", req.href)))?;
        let sync = self
            .dav
            .event_sync(&account, &href)
            .await
            .map_err(domain_error_to_status)?;
        Ok(Response::new(event_to_proto(&event, &sync)))
    }

    async fn create_event(
        &self,
        request: Request<CreateEventRequest>,
    ) -> Result<Response<CreateEventResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .command_bus
            .dispatch(Command::AddCalendarEvent {
                account_id: parse_account_id(&req.account_id)?,
                calendar: CollectionId::new(req.calendar),
                summary: req.summary,
                start: event_when_from_str(&req.start).map_err(Status::invalid_argument)?,
                end: event_when_from_str(&req.end).map_err(Status::invalid_argument)?,
                location: req.location.filter(|l| !l.is_empty()),
                description: req.description.filter(|d| !d.is_empty()),
            })
            .await
            .map_err(application_error_to_status)?;
        let CommandResult::CalendarEventAdded { href } = result else {
            return Err(Status::internal("unexpected command result"));
        };
        Ok(Response::new(CreateEventResponse {
            href: href.to_string(),
        }))
    }

    async fn update_event(
        &self,
        request: Request<UpdateEventRequest>,
    ) -> Result<Response<UpdateEventResponse>, Status> {
        let req = request.into_inner();
        let edit = event_edit_from_proto(&req).map_err(Status::invalid_argument)?;
        self.command_bus
            .dispatch(Command::UpdateCalendarEvent {
                account_id: parse_account_id(&req.account_id)?,
                href: Href::new(req.href),
                edit,
            })
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(UpdateEventResponse {}))
    }

    async fn delete_event(
        &self,
        request: Request<DeleteEventRequest>,
    ) -> Result<Response<DeleteEventResponse>, Status> {
        let req = request.into_inner();
        self.command_bus
            .dispatch(Command::RemoveCalendarEvent {
                account_id: parse_account_id(&req.account_id)?,
                href: Href::new(req.href),
            })
            .await
            .map_err(application_error_to_status)?;
        Ok(Response::new(DeleteEventResponse {}))
    }

    async fn get_dav_status(
        &self,
        request: Request<GetDavStatusRequest>,
    ) -> Result<Response<GetDavStatusResponse>, Status> {
        let account = parse_account_id(&request.into_inner().account_id)?;
        let conflicts_lost = self
            .dav
            .conflicts_lost(&account)
            .await
            .map_err(domain_error_to_status)?;
        let pending = self.pending(&account).await?;
        Ok(Response::new(GetDavStatusResponse {
            conflicts_lost,
            pending_changes: pending.len() as u32,
            failed_changes: pending.iter().filter(|p| p.failure.is_some()).count() as u32,
        }))
    }

    async fn sync_dav_now(
        &self,
        request: Request<SyncDavNowRequest>,
    ) -> Result<Response<SyncDavNowResponse>, Status> {
        let account = request
            .into_inner()
            .account_id
            .as_deref()
            .map(parse_account_id)
            .transpose()?;
        self.providers.wake(account);
        Ok(Response::new(SyncDavNowResponse {}))
    }

    type WatchDavChangesStream = ReceiverStream<Result<DavChange, Status>>;

    async fn watch_dav_changes(
        &self,
        _request: Request<WatchDavChangesRequest>,
    ) -> Result<Response<Self::WatchDavChangesStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(forward_changes(self.event_bus.subscribe(), tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn change_of(envelope: &EventEnvelope) -> Option<(Option<AccountId>, DavKind)> {
    let kind = match envelope.aggregate_type {
        AggregateType::Contact | AggregateType::AddressBook => DavKind::Contacts,
        AggregateType::Calendar | AggregateType::CalendarEvent => DavKind::Calendar,
        _ => return None,
    };
    Some((envelope.payload.account_id(), kind))
}

async fn forward_changes(
    mut events: EventSubscriber,
    tx: tokio::sync::mpsc::Sender<Result<DavChange, Status>>,
) {
    let mut pending = HashSet::new();
    while let Some(first) = events.recv().await {
        pending.extend(change_of(&first));
        if pending.is_empty() {
            continue;
        }
        let flush_at = Instant::now() + CHANGE_WINDOW;
        while let Ok(Some(next)) = timeout_at(flush_at, events.recv()).await {
            pending.extend(change_of(&next));
        }
        for (account, kind) in pending.drain() {
            let change = DavChange {
                account_id: account.map(|a| a.to_string()).unwrap_or_default(),
                kind: kind as i32,
            };
            if tx.send(Ok(change)).await.is_err() {
                return; // client disconnected
            }
        }
    }
}

#[cfg(test)]
mod tests;
