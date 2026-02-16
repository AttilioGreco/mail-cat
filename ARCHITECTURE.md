# mail-cat - Architettura DDD Email Client

## Panoramica
Email client modulare in Rust con architettura DDD, CQRS + Event Sourcing. Backend multi-provider (IMAP, Gmail API, Outlook API) con interfacce pulite basate su trait. Due frontend: Slint (nativo via gRPC) e browser (GraphQL). Storage locale con sled, ricerca full-text con tantivy.

> **Reviewer note (stato attuale)**
> Il documento descrive l'architettura target ("to-be"). Ad oggi (Day 7) la settimana 1 (fondamenta) è completa e la settimana 2 (infrastruttura) è in corso. 96 test passano, 0 warning.

## Stato implementazione (Day 7)
- ✅ `mc-domain`: value objects, aggregati (Account, Mailbox, Message), `DomainEvent` (18 eventi), `EventEnvelope`, `DomainError`, event sourcing rehydration (`apply`/`rehydrate`)
- ✅ `mc-domain/src/traits/*`: EmailProvider, EventStore, Repository (read/write), SearchIndex, ReadModelProjector, ProjectionCheckpoint — tutti con doc-comment e semantica definita
- ✅ `mc-application`: Command enum (11 varianti), Query enum (8 varianti), CommandBus/QueryBus traits, InProcessEventBus (broadcast), InProcessCommandBus/InProcessQueryBus, AccountCommandHandler, MessageCommandHandler, QueryHandler
- ✅ `mc-application/projections`: ProjectionEventHandler (handle, rebuild, catch_up con checkpoint idempotente)
- ✅ `mc-infrastructure`: SledEventStore (3 sled trees, OCC, global_position, subscribe via broadcast), SledRepository (AccountWrite/Read, MailboxRead, MessageRead, ProjectionCheckpoint, ReadModelProjector per tutti i 18 eventi)
- ⚠️ `mc-application/services`: SyncService, SendService (placeholder — Day 14)
- ⚠️ `mc-infrastructure/email`: ImapEmailProvider, SmtpSender (placeholder — Day 8-9)
- ⚠️ `mc-infrastructure/search`: TantivySearchIndex (placeholder — Day 10)
- ⚠️ `mc-interfaces`: gRPC/GraphQL/HTTP (placeholder — Day 11-13)
- ⚠️ `mc-server`: entrypoint placeholder (Day 14)

### Decisioni architetturali prese
- **Generics over dyn**: tutti i trait consumati via generics, `#![allow(async_fn_in_trait)]` a livello di crate
- **mc-infrastructure NON dipende da mc-application**: conforme a Hexagonal/Clean Architecture (reviewer note accolta)
- **EventStore semantica**: `expected_sequence` = last seen (0 per nuovo), append returns last written, global_position monotonica, subscribe at-least-once
- **subscribe via broadcast**: preferito a `sled::watch_prefix` per semplicità; replay via load_all_events + checkpoint per durabilità
- **ReadModelProjector**: trait in mc-domain, implementato da SledRepository; ProjectionEventHandler in mc-application orchestra proiezione + checkpoint

## Stack Tecnologico
| Componente | Libreria |
|------------|----------|
| Async Runtime | tokio |
| RPC | tonic (gRPC) + prost |
| GraphQL | async-graphql + axum |
| Email (IMAP) | async-imap |
| Email (SMTP) | lettre |
| Storage | sled |
| Search | tantivy |
| UI nativa | Slint |
| Logging | tracing + tracing-subscriber |
| Serialization | serde + serde_json |
| Error handling | thiserror + anyhow |
| IDs | uuid (v7) |
| Rust edition | 2024 (async fn in trait nativi) |

---

## Struttura Workspace Cargo

```
mail-cat/
├── Cargo.toml                    # workspace manifest
├── Cargo.lock
├── ARCHITECTURE.md               # questo file
├── proto/                        # protobuf definitions
│   └── mail_cat/v1/
│       ├── common.proto
│       ├── accounts.proto
│       ├── mailboxes.proto
│       ├── messages.proto
│       └── search.proto
└── crates/
    ├── mc-domain/                # Puro dominio: entità, value objects, aggregati, eventi, trait
    ├── mc-application/           # Use cases: comandi, query, event handler, bus, proiezioni
    ├── mc-infrastructure/        # Implementazioni concrete: IMAP, SMTP, sled, tantivy
    ├── mc-interfaces/            # Handler API: gRPC services, GraphQL schema, axum server
    ├── mc-shared/                # Tipi cross-cutting: pagination, errori comuni, telemetria
    ├── mc-server/                # Binario: monolite che avvia gRPC + GraphQL
    └── mc-ui/                    # Binario: UI Slint (client gRPC)
```

---

## Dependency Flow

```
mc-shared  ← nessuna dipendenza interna; solo crate esterne (serde, chrono, uuid, thiserror, tracing)
    ↑
mc-domain  ← dipende da mc-shared; ZERO dipendenze I/O (puro Rust)
    ↑
mc-application  ← dipende da mc-domain, mc-shared
    ↑
mc-infrastructure  ← dipende da mc-domain, mc-application, mc-shared
    ↑                  (implementa i trait del dominio)
mc-interfaces  ← dipende da mc-application, mc-domain, mc-shared
    ↑              (chiama command/query bus dell'application layer)
mc-server  ← dipende da tutto (wiring + dependency injection)

mc-ui  ← solo slint + tonic client; NESSUNA dipendenza backend diretta
```

**Principio chiave**: `mc-domain` non ha dipendenze I/O (no tokio, no sled, no network). È puro Rust con `serde`, `chrono`, `uuid` e `thiserror`. La keyword `async` appare nelle definizioni dei trait ma nessun runtime viene importato.

> **Reviewer note (confini e dipendenze)**
> Valuterei se far dipendere `mc-infrastructure` da `mc-application` sia davvero necessario. In Clean Architecture/Hexagonal di solito:
> - **Domain/Application** definiscono *ports* (trait) e modelli
> - **Infrastructure** implementa quei trait e dipende solo da Domain(+Shared)
> - **Server** fa il wiring
>
> Se `mc-infrastructure` dipende da `mc-application`, rischi di trascinare concetti applicativi dentro i driver (es. EventBus in-process) e rendere più difficile riusare/isolared l’infrastruttura o testarla. Una scelta pratica: spostare tutti i trait di persistenza/search/provider in `mc-domain` (o in `mc-application` se li consideri ports applicativi) e fare in modo che `mc-infrastructure` implementi soltanto quelli.

---

## Domain Model (mc-domain)

### Struttura Moduli
```
mc-domain/src/
├── lib.rs
├── error.rs
├── model/
│   ├── mod.rs
│   ├── account.rs          # Account aggregate root
│   ├── mailbox.rs          # Mailbox aggregate
│   ├── message.rs          # Message aggregate root
│   ├── identity.rs         # Identity value object (persona mittente)
│   └── attachment.rs       # Attachment value object
├── value_objects/
│   ├── mod.rs
│   ├── email_address.rs
│   ├── message_id.rs
│   ├── mailbox_path.rs
│   ├── flag.rs
│   └── uid.rs
├── events/
│   ├── mod.rs
│   ├── account_events.rs
│   ├── mailbox_events.rs
│   └── message_events.rs
└── traits/
    ├── mod.rs
    ├── email_provider.rs   # EmailProvider trait
    ├── event_store.rs      # EventStore trait
    ├── repository.rs       # Repository traits (read + write)
    └── search_index.rs     # SearchIndex trait
```

### Value Objects
- **EmailAddress** — validazione base, lowercase normalization
- **MessageId** — UUID v7 (ordinabile cronologicamente)
- **AccountId**, **MailboxId**, **AttachmentId** — UUID v7
- **ProviderUid(u32)** — UID specifico del provider (es. IMAP UID)
- **MailboxPath** — segmenti + delimiter (es. `["INBOX"]` con delimiter `/`)
- **Flag** — enum: `Seen`, `Answered`, `Flagged`, `Deleted`, `Draft`, `Recent`, `Custom(String)`

### Aggregati

#### Account (aggregate root)
```rust
pub struct Account {
    pub id: AccountId,
    pub name: String,
    pub email: EmailAddress,
    pub provider_type: ProviderType,     // Imap | GmailApi | OutlookApi
    pub connection_config: ConnectionConfig,
    pub identity: Identity,
    pub status: AccountStatus,           // Active | Disabled | Error
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

#### ConnectionConfig
```rust
pub struct ConnectionConfig {
    pub imap_host: Option<String>,
    pub imap_port: Option<u16>,
    pub smtp_host: Option<String>,
    pub smtp_port: Option<u16>,
    pub use_tls: bool,
    pub auth: AuthConfig,               // Plain | OAuth2 | AppPassword
}
```

#### Mailbox (aggregate)
```rust
pub struct Mailbox {
    pub id: MailboxId,
    pub account_id: AccountId,
    pub path: MailboxPath,
    pub display_name: String,
    pub mailbox_type: MailboxType,       // Inbox | Sent | Drafts | Trash | Spam | Archive | Custom
    pub flags: Vec<Flag>,
    pub total_messages: u32,
    pub unread_messages: u32,
    pub uid_validity: Option<u32>,
    pub uid_next: Option<u32>,
}
```

#### Message (aggregate root)
```rust
pub struct Message {
    pub id: MessageId,
    pub account_id: AccountId,
    pub mailbox_id: MailboxId,
    pub provider_uid: Option<ProviderUid>,
    pub message_rfc_id: Option<String>,  // RFC 2822 Message-ID header
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub envelope: Envelope,
    pub flags: Vec<Flag>,
    pub size_bytes: u64,
    pub has_attachments: bool,
    pub attachments: Vec<Attachment>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub raw_headers: Option<String>,
    pub received_at: DateTime<Utc>,
    pub internal_date: DateTime<Utc>,
}
```

#### Envelope
```rust
pub struct Envelope {
    pub from: Vec<Address>,
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    pub reply_to: Vec<Address>,
    pub subject: String,
    pub date: DateTime<Utc>,
}
```

---

## Event Catalog

Ogni evento è wrappato in un `EventEnvelope` con metadata:
```rust
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub aggregate_id: String,
    pub aggregate_type: AggregateType,  // Account | Mailbox | Message
    pub sequence_number: u64,           // monotonicamente crescente per aggregato
    pub occurred_at: DateTime<Utc>,
    pub payload: DomainEvent,
}
```

### DomainEvent enum

**Account Events:**
- `AccountAdded { account_id, name, email, provider_type }`
- `AccountUpdated { account_id, name?, connection_config? }`
- `AccountRemoved { account_id }`
- `AccountSyncStarted { account_id }`
- `AccountSyncCompleted { account_id, mailboxes_synced, messages_synced }`
- `AccountSyncFailed { account_id, error }`

**Mailbox Events:**
- `MailboxDiscovered { mailbox_id, account_id, path, mailbox_type }`
- `MailboxCountsUpdated { mailbox_id, total_messages, unread_messages }`
- `MailboxRenamed { mailbox_id, old_path, new_path }`
- `MailboxDeleted { mailbox_id, account_id }`

**Message Events:**
- `MessageReceived { message_id, account_id, mailbox_id, provider_uid, envelope, flags, size_bytes, has_attachments, received_at }`
- `MessageBodyFetched { message_id, body_text?, body_html?, attachments }`
- `MessageFlagged { message_id, flags_added, flags_removed }`
- `MessageMoved { message_id, from_mailbox_id, to_mailbox_id }`
- `MessageDeleted { message_id, mailbox_id, permanently }`
- `MessageSent { message_id, account_id, envelope, sent_at }`
- `DraftSaved { message_id, account_id, envelope, body_text?, body_html? }`
- `DraftDiscarded { message_id }`

---

## Trait Chiave

### EmailProvider
Astrazione async sui backend email. Implementato da `ImapEmailProvider`, futuro `GmailApiProvider`, ecc.

```rust
pub trait EmailProvider: Send + Sync {
    async fn list_mailboxes(&self) -> Result<Vec<Mailbox>, DomainError>;
    async fn fetch_message_envelopes(&self, mailbox: &MailboxPath, range: FetchRange) -> Result<Vec<MessageEnvelopeSummary>, DomainError>;
    async fn fetch_message_detail(&self, mailbox: &MailboxPath, uid: ProviderUid) -> Result<Message, DomainError>;
    async fn fetch_raw_message(&self, mailbox: &MailboxPath, uid: ProviderUid) -> Result<Vec<u8>, DomainError>;
    async fn set_flags(&self, mailbox: &MailboxPath, uid: ProviderUid, add: &[Flag], remove: &[Flag]) -> Result<(), DomainError>;
    async fn move_message(&self, uid: ProviderUid, from: &MailboxPath, to: &MailboxPath) -> Result<Option<ProviderUid>, DomainError>;
    async fn delete_message(&self, mailbox: &MailboxPath, uid: ProviderUid, permanently: bool) -> Result<(), DomainError>;
    async fn send_message(&self, message: &OutgoingMessage) -> Result<(), DomainError>;
    async fn mailbox_status(&self, mailbox: &MailboxPath) -> Result<MailboxStatus, DomainError>;
    fn watch_mailbox(&self, mailbox: &MailboxPath) -> impl Stream<Item = Result<ProviderNotification, DomainError>> + Send;
}
```

> **Reviewer note (object-safety e ergonomia DI)**
> Le firme con `async fn` nei trait + `-> impl Stream` sono ottime per l’ergonomia, ma diventano problematiche se vuoi usare `dyn EmailProvider` (tipico in DI/wiring):
> - `async fn` in trait e `impl Trait` nel return rendono il trait **non object-safe**.
>
> Prima di andare avanti, conviene decidere il modello di composizione:
> - **Generics** (es. `struct SyncService<P: EmailProvider, ...>`) → più complesso nel wiring ma zero boxing.
> - **Trait object** (`Arc<dyn EmailProvider>`) → allora preferire `async_trait` e/o usare `futures::stream::BoxStream<'static, ...>` o un associated type (`type WatchStream`) per rendere object-safe.
>
> Idem per `EventStore::subscribe()` e `SearchIndex::rebuild(...)` se vuoi poterli mockare con `dyn`.

### EventStore
```rust
pub trait EventStore: Send + Sync {
    async fn append(&self, aggregate_id: &str, aggregate_type: AggregateType, expected_sequence: u64, events: Vec<DomainEvent>) -> Result<u64, EventStoreError>;
    async fn load_events(&self, aggregate_id: &str, from_sequence: u64) -> Result<Vec<EventEnvelope>, EventStoreError>;
    async fn load_all_events(&self, aggregate_type: Option<AggregateType>, from_global_position: u64) -> Result<Vec<EventEnvelope>, EventStoreError>;
    fn subscribe(&self) -> impl Stream<Item = EventEnvelope> + Send;
}
```

> **Reviewer note (semantica da fissare ora)**
> Per evitare ambiguità e bug “fantasma” più avanti, qui servono 2-3 definizioni precise:
> - `expected_sequence`: è **last seen sequence** o **next expected**? (es. 0 per nuovo aggregato?)
> - return di `append`: è **last sequence scritto** o **next sequence**?
> - `load_all_events(... from_global_position)`: qual è il tipo/semantica di `global_position`? È monotonicamente crescente e totale-order? È persistita nell’envelope?
> - `subscribe()`: consegna eventi in **ordine globale** (best effort) o per-aggregate? Almeno una volta? Può duplicare?
>
> Queste scelte impattano direttamente proiezioni, rebuild e idempotenza degli handler.

### Repository (CQRS — read e write separati)
```rust
// Write-side: lavora con event store
pub trait AccountWriteRepository: Send + Sync {
    async fn save(&self, account: &Account, events: Vec<DomainEvent>) -> Result<(), RepositoryError>;
    async fn load(&self, id: &AccountId) -> Result<Option<Account>, RepositoryError>;
}

// Read-side: lavora con proiezioni (sled read model)
pub trait AccountReadRepository: Send + Sync {
    async fn get_account(&self, id: &AccountId) -> Result<Option<AccountSummary>, RepositoryError>;
    async fn list_accounts(&self) -> Result<Vec<AccountSummary>, RepositoryError>;
}

pub trait MailboxReadRepository: Send + Sync { /* ... */ }
pub trait MessageReadRepository: Send + Sync { /* ... */ }
```

### SearchIndex
```rust
pub trait SearchIndex: Send + Sync {
    async fn index_message(&self, message: &Message) -> Result<(), SearchError>;
    async fn remove_message(&self, message_id: &MessageId) -> Result<(), SearchError>;
    async fn search(&self, query: &str, account_id: Option<&AccountId>, mailbox_id: Option<&MailboxId>, pagination: &Pagination) -> Result<SearchResults, SearchError>;
    async fn rebuild(&self, messages: impl Stream<Item = Message> + Send) -> Result<u64, SearchError>;
}
```

---

## Application Layer (mc-application)

### Struttura Moduli
```
mc-application/src/
├── lib.rs
├── error.rs
├── commands/
│   ├── mod.rs
│   ├── account_commands.rs
│   ├── mailbox_commands.rs
│   └── message_commands.rs
├── queries/
│   ├── mod.rs
│   ├── account_queries.rs
│   ├── mailbox_queries.rs
│   ├── message_queries.rs
│   └── search_queries.rs
├── handlers/
│   ├── mod.rs
│   ├── command_handlers.rs
│   ├── query_handlers.rs
│   └── event_handlers.rs
├── services/
│   ├── mod.rs
│   ├── sync_service.rs
│   └── send_service.rs
├── bus/
│   ├── mod.rs
│   ├── command_bus.rs
│   ├── query_bus.rs
│   └── event_bus.rs
└── projections/
    ├── mod.rs
    ├── message_list_projection.rs
    ├── mailbox_tree_projection.rs
    └── account_summary_projection.rs
```

### CQRS Bus
- **CommandBus**: `dispatch(Command) -> CommandResult`
  - `Command::Account(AccountCommand)` — AddAccount, UpdateAccount, RemoveAccount, SyncAccount
  - `Command::Mailbox(MailboxCommand)` — SyncMailboxList, CreateMailbox, RenameMailbox, DeleteMailbox
  - `Command::Message(MessageCommand)` — FetchMessages, SetFlags, MarkAsRead, MoveMessage, DeleteMessage, SendMessage, SaveDraft
- **QueryBus**: `dispatch(Query) -> QueryResult`
  - Account, Mailbox, Message, Search queries
- **EventBus**: `publish(EventEnvelope)` + `subscribe() -> broadcast::Receiver`
  - Implementazione in-process con `tokio::sync::broadcast`

### Event Handlers (side-effects)
- **ProjectionEventHandler** — aggiorna read-model sled su ogni evento dominio
- **SearchIndexEventHandler** — indicizza in tantivy su `MessageBodyFetched`, rimuove su `MessageDeleted`

### Services
- **SyncService** — orchestrazione sync completo account (list mailboxes → fetch envelopes → fetch bodies)
- **SendService** — orchestrazione invio SMTP con generazione evento `MessageSent`

---

## Infrastructure Layer (mc-infrastructure)

### Struttura Moduli
```
mc-infrastructure/src/
├── lib.rs
├── email/
│   ├── mod.rs
│   ├── imap/
│   │   ├── mod.rs
│   │   ├── client.rs              # ImapEmailProvider
│   │   ├── connection_pool.rs
│   │   └── mapper.rs              # IMAP response → domain model
│   └── smtp/
│       ├── mod.rs
│       └── client.rs              # SmtpSender
├── persistence/
│   ├── mod.rs
│   └── sled/
│       ├── mod.rs
│       ├── event_store.rs         # SledEventStore
│       ├── account_repo.rs
│       ├── mailbox_repo.rs
│       ├── message_repo.rs
│       └── projection_store.rs
├── search/
│   ├── mod.rs
│   └── tantivy/
│       ├── mod.rs
│       ├── index.rs               # TantivySearchIndex
│       ├── schema.rs
│       └── mapper.rs
└── config/
    ├── mod.rs
    ├── app_config.rs
    └── account_config.rs
```

### sled Event Store
- **Key schema**: `events:{aggregate_type}:{aggregate_id}:{sequence:010}` (zero-padded per ordinamento lessicografico)
- **Global position counter**: contatore monotonicamente crescente
- **Optimistic concurrency**: compare-and-swap sulla sequenza
- **Subscribe**: `sled::watch_prefix` per stream di nuovi eventi

> **Reviewer note (rischi su sled per Event Store/Proiezioni)**
> `sled` è comodo per un prototipo locale, ma per Event Sourcing (durabilità + scansioni + migrazioni + concorrenza) è facile finire in corner-case.
> Suggerimenti per “affinare” la scelta:
> - Definire chiaramente requisiti: single-writer? multi-writer? crash-consistency? dimensione mailbox? rebuild time accettabile?
> - Se servono transazioni più robuste o query più ricche, valutare presto un’alternativa: **SQLite** (con WAL) o **libsql** per locale; Postgres se mai server-side.
> - `watch_prefix` è utile, ma per proiezioni affidabili di solito serve anche una **coda durabile** o un meccanismo di replay con checkpoint globale.

### sled Projection Store
- **Key schema**:
  - `account:{id}`
  - `mailbox:{account_id}:{id}`
  - `message:{mailbox_id}:{received_at}:{id}`

> **Reviewer note (ordinamento/paginazione stabile)**
> Usare `received_at` come parte della key è ok, ma fissare:
> - precisione e formato (ms? ns?) per evitare collisioni
> - tie-breaker stabile (l’`id` lo è) e ordinamento (asc/desc)
> - semantica “received_at” vs “internal_date” (IMAP): quale guida le liste?

### tantivy Search Index
- **Schema fields**: message_id, account_id, mailbox_id, from, to, subject, body, date, flags, has_attachments
- **Full-text search** con snippet/highlighting

> **Reviewer note (indicizzazione e consistenza)**
> Tantivy è una buona scelta, ma serve decidere l’"source of truth" del testo indicizzato:
> - si indicizza solo dopo `MessageBodyFetched` (eventuale) → molte email restano “searchable” solo per subject/from fino al fetch?
> - come gestire update (flags/move) e delete (tombstone) in indice?
> - rebuild: definire checkpoint, progress reporting e impatto sulla UX.

---

## Interface Layer (mc-interfaces)

### Struttura Moduli
```
mc-interfaces/src/
├── lib.rs
├── grpc/
│   ├── mod.rs
│   ├── server.rs
│   ├── services/
│   │   ├── mod.rs
│   │   ├── account_service.rs
│   │   ├── mailbox_service.rs
│   │   ├── message_service.rs
│   │   └── search_service.rs
│   └── mapper.rs                  # proto ↔ domain mapping
├── graphql/
│   ├── mod.rs
│   ├── schema.rs
│   ├── query.rs
│   ├── mutation.rs
│   ├── subscription.rs
│   └── types/
│       ├── mod.rs
│       ├── account.rs
│       ├── mailbox.rs
│       ├── message.rs
│       └── search.rs
└── http/
    ├── mod.rs
    └── axum_server.rs             # Axum hosting GraphQL
```

### gRPC Services (proto)
4 servizi definiti in proto:
1. **AccountService** — AddAccount, GetAccount, ListAccounts, UpdateAccount, RemoveAccount, SyncAccount (server-streaming per progresso)
2. **MailboxService** — ListMailboxes, GetMailbox, CreateMailbox, RenameMailbox, DeleteMailbox
3. **MessageService** — ListMessages, GetMessage, GetThread, SetFlags, MoveMessage, DeleteMessage, SendMessage, SaveDraft, WatchMailbox (server-streaming)
4. **SearchService** — Search, RebuildIndex (server-streaming per progresso)

### GraphQL Schema
- **QueryRoot**: account(s), mailbox(es), message(s), thread, search
- **MutationRoot**: addAccount, updateAccount, removeAccount, syncAccount, setMessageFlags, markAsRead/Unread, toggleFlagged, moveMessage, deleteMessage, sendMessage, saveDraft, discardDraft, rebuildSearchIndex
- **SubscriptionRoot**: onNewMessage, onMailboxUpdated, onSyncProgress (via tokio broadcast → async-graphql Stream)

---

## Server Wiring (mc-server/main.rs)

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Init tracing
    // 2. Load config (TOML)
    // 3. Open sled DB → EventStore + ProjectionStore
    // 4. Init tantivy SearchIndex
    // 5. Init read repositories (da proiezioni)
    // 6. Init EventBus (broadcast) + registra handlers
    // 7. Init CommandBus + QueryBus
    // 8. Spawn event subscription loop
    // 9. Start gRPC server (porta configurabile)
    // 10. Start GraphQL/axum server (porta configurabile)
    // 11. tokio::select! per attendere entrambi
}
```

> **Reviewer note (wiring e lifecycle)**
> Qui consiglierei di aggiungere nel documento (prima di implementare):
> - modello di **shutdown** (graceful) e cleanup (flush tantivy, close sled)
> - gestione task long-running (watch IMAP, subscription loop) con `tokio::select!` + cancellation
> - un posto unico per config + validation (es. `AppConfig::validate()`), e come iniettare credenziali in modo sicuro.

---

## Proto Files

### common.proto
- `Pagination { page, page_size }`
- `PageInfo { total_count, page, page_size, has_next_page }`
- `Timestamp { seconds, nanos }`

### accounts.proto
- `AccountService` con 6 RPC
- Tipi: `ProviderType`, `AccountStatus`, `ConnectionConfig`, `AuthConfig` (oneof plain/oauth2/app_password), `AccountSummary`
- `SyncAccount` returns `stream SyncProgressEvent`

### mailboxes.proto
- `MailboxService` con 5 RPC
- Tipi: `MailboxType`, `MailboxSummary`

### messages.proto
- `MessageService` con 9 RPC
- Tipi: `EmailAddress`, `MessageEnvelope`, `AttachmentInfo`, `MessageSummary`, `MessageDetail`
- `WatchMailbox` returns `stream MailboxEvent` (oneof: NewMessage, FlagsChanged, MessageExpunged, CountsChanged)

### search.proto
- `SearchService` con 2 RPC
- Tipi: `SearchHit`, `SearchRequest/Response`
- `RebuildIndex` returns `stream RebuildIndexProgress`

---

## Roadmap Implementazione

## Reviewer Table — Considerazioni day-by-day (per definire un piano esecutivo)

Questa tabella è pensata per essere letta da un altro system architect e trasformata in un piano d’implementazione assegnabile ai team. L’obiettivo è fissare **decisioni**, **rischi**, **approccio** e **criteri di uscita** per ciascun giorno della roadmap.

| Giorno | Punti chiave da decidere / rischio | Come lo risolverei (approccio) | Perché (razionale) | Exit criteria (deliverable verificabile) |
|--------|-----------------------------------|--------------------------------|--------------------|------------------------------------------|
| **1** | Confini crate e standard dev (lint/test/CI) | Definire policy dipendenze (domain no I/O), attivare `clippy` + `fmt` in CI, baseline `tracing` + error shared | Evita regressioni architetturali fin dall’inizio | Pipeline minima: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --workspace` |
| **2** | Domain: eventi come “source of truth” vs audit-log | Aggiungere documentazione su `apply(event)` e `rehydrate(events)` + invarianti (dove si validano) | Senza questo, l’Event Sourcing resta nominale e le scelte successive (store/proiezioni) diventano incoerenti | Una pagina/section con semantica di rehydration + almeno 1 aggregato con `apply_*` (anche se non usato subito) |
| **3** | Ports/trait: object-safety + semantica EventStore | Scegliere: generics vs `dyn`. Se `dyn`: usare `async_trait` + `BoxStream`/associated type. Fissare `expected_sequence`, `append` return, `global_position`, duplicati di `subscribe()` | È la decisione che influenza DI, test, mocking e affidabilità CQRS | Trait definitivi (no placeholder) + doc comment con semantica + test di compilazione con `Arc<dyn ...>` (se scelto) |
| **4** | Bus: in-process vs più robusto; ordering eventi | Implementare Command/Query bus come call dirette (no magia), EventBus come broadcast *best effort* ma con event-loop che legge dallo store (non solo publish in memoria) | Se EventBus è solo in memoria, perdi eventi su crash e proiezioni diventano fragili | `mc-application` con bus minimi + loop “event dispatcher” definito (anche stub) + test unitari basilari |
| **5** | Handler: idempotenza e testabilità | Disegnare handler con input “evento + context” e output deterministico; introdurre idempotency-key/checkpoint nel read model | Gli handler verranno rieseguiti (rebuild, retry) | Suite test con mock ports + almeno 1 handler idempotente (proiezione) |
| **6** | EventStore su sled: durabilità/ordinamento/scan | Implementare envelope persistita con `global_position` (o equivalente) + crash-consistency test (append→kill→reopen) + property test su ordering | Event store è il cuore: meglio scoprire limiti subito | `SledEventStore` con append/load/load_all + test integrazione (reopen) + specifica key schema + migrazione versionata |
| **7** | Proiezioni: checkpoint e rebuild deterministico | Proiezioni con `last_global_position` persistito; rebuild = drop + replay; definire chiavi e paginazione stabile | Senza checkpoint, duplicati e inconsistenze diventano inevitabili | `ProjectionStore` + 1 proiezione “account summary” con rebuild e test replay deterministico |
| **8** | IMAP: mapping e limiti operativi (rate/latency) | Layer mapper isolato; definire “sync strategy” (incrementale: UIDVALIDITY/UIDNEXT) + connection pool con timeout | Minimizza bug di mapping e riduce costi di sync | `ImapEmailProvider::list_mailboxes` + fetch envelopes con test su fixture o server test |
| **9** | IMAP flags/move/delete + watch (IDLE) | Separare “watch” da “sync”: watch emette notifiche, ma la fonte di verità resta fetch; introdurre retry/backoff; SMTP sender con error mapping | IDLE è fragile; serve fallback sicuro | Flags/move/delete implementati + watch con fallback + `SmtpSender` minimale con test locale (mock) |
| **10** | Tantivy: update/delete/rebuild e consistenza | Modellare document id stabile (MessageId) + strategia delete (tombstone o delete by term) + commit batching; rebuild con progress e checkpoint | Search è un read-model: deve essere ricostruibile e consistente | `TantivySearchIndex` + test integrazione indicizza→cerca→delete→cerca |
| **11** | Proto: versioning e riuso dal client | Creare crate dedicato (es. `mc-proto`) generato da `tonic-build`; definire policy compatibilità (no breaking changes) | Evita coupling UI↔server e facilita evoluzione API | `mc-proto` buildabile + CI che rigenera/verifica; `mc-ui` consuma solo proto + client |
| **12** | gRPC services: streaming semantics + error mapping | Definire codifica errori (gRPC status + details), correlation-id, backpressure su streaming progress | Streaming senza regole crea client instabili e bug difficili | 1 service completo end-to-end (AccountService) con test `grpcurl`/integration |
| **13** | GraphQL: parità funzionale e auth | Stabilire se GraphQL è “thin layer” sopra use-case; definire subscriptions (eventi di proiezione) + policy auth (anche placeholder) | Riduce duplicazione logica e previene leakage di domain | Schema + resolver minimi (query/list) + subscription basata su stream definito |
| **14** | SyncService: orchestrazione, retry, resume | Implementare sync come state machine (per account) con checkpoint; distinguere “envelopes first” vs “body on demand”; limiti concurrency | Sync è il flusso più complesso e costoso | Sync end-to-end su un account test con progress streaming e resume dopo restart |
| **15** | Error handling cross-layer + sicurezza config | Unificare error taxonomy (Domain vs Infra vs Interface), policy PII logging, storage credenziali (keyring/encrypted) | Evita leak di segreti e rende osservabilità utile | Documento error mapping + config validation + nessuna password/token in chiaro nei log |
| **16** | UI Slint: contract gRPC e paginazione | Definire DTO stabili e paginazione; caching locale UI; gestione stati (loading/error/offline) | UI soffre se API non è stabile | UI mostra lista account + mailboxes via gRPC con pagination baseline |
| **17** | UI: flusso sync e progress | Modellare progress events e UI state machine; cancellazione sync | Migliora UX e riduce bug “stuck syncing” | UX sync: start/stop + progress + errore gestito |
| **18** | UI: lista messaggi e dettaglio | Caricamento lazy body; prefetch controllato; rendering HTML sicuro (sanitization se necessario) | Performance e sicurezza (HTML email) | Lista + dettaglio con fetch on demand e gestione allegati metadata |
| **19** | UI: composizione + invio | Editor testo/HTML, allegati, retry invio, bozze | Completa il ciclo principale “read→reply” | Compose + send + draft save/discard con feedback utente |
| **20** | E2E, performance, packaging | Test E2E scriptati, profiling tantivy/imap, release checklist (config, migrazioni, backup) | Stabilizza la release e previene regressioni | `cargo test` + E2E runbook + binari/package + note migrazione |

### Settimana 1: Fondamenta
| Giorno | Task |
|--------|------|
| **1** | Conversione a workspace Cargo + `mc-shared` (Pagination, PagedResult, error, telemetria) + `mc-domain` value objects con unit test |
| **2** | Domain model completo (aggregati) + eventi (DomainEvent enum + EventEnvelope) + DomainError |
| **3** | Domain trait: EmailProvider, EventStore, Repository (read/write), SearchIndex. Verifica `mc-domain` compila senza I/O |
| **4** | Application layer: bus (Command/Query/Event), InProcessEventBus con broadcast |
| **5** | Command/query handlers + unit test con mock repositories |

### Settimana 2: Infrastruttura
| Giorno | Task |
|--------|------|
| **6** | SledEventStore (append, load, subscribe, concurrency) + integration test |
| **7** | SledProjectionStore + read repositories + ProjectionEventHandler |
| **8** | ImapEmailProvider (list_mailboxes, fetch envelopes/detail, mapper, TLS) |
| **9** | IMAP (flags, move, delete, IDLE watch) + SmtpSender con lettre |
| **10** | TantivySearchIndex + SearchIndexEventHandler + integration test |

### Settimana 3: Interface Layer
| Giorno | Task |
|--------|------|
| **11** | Proto files completi + tonic-build + struct gRPC service |
| **12** | Implementazione gRPC services + streaming + test con grpcurl |
| **13** | GraphQL schema completo (types, query, mutation, subscription) + axum server |
| **14** | SyncService + mc-server wiring completo + end-to-end test |
| **15** | Error handling cross-layer, tracing spans, config TOML, integration test E2E |

### Settimana 4: UI Nativa & Polish
| Giorno | Task |
|--------|------|
| **16-17** | mc-ui con Slint: layout principale, gRPC client stubs, gestione account |
| **18-19** | Slint: lista messaggi, dettaglio, composizione, ricerca, notifiche real-time |
| **20** | Test E2E completo, profiling performance, packaging binari |

---

## Verifica e Testing
- `cargo check --workspace` deve compilare a ogni step
- `cargo test --workspace` per unit + integration test
- Test IMAP con server locale (GreenMail, Dovecot, o account test)
- Test end-to-end: avvia mc-server → aggiungi account via gRPC → sync → cerca → leggi messaggi
- `grpcurl` per testare servizi gRPC manualmente
- GraphQL playground (localhost) per testare query/mutation interattivamente

---

## Reviewer Notes (punti da affinare prima del Day 3)

### 1) Event Sourcing “vero” vs eventi come audit-log
Attualmente il domain produce eventi (`create`, `discover`, `receive`, ecc.) e muta stato direttamente. Per Event Sourcing completo mancano decisioni/documentazione su:
- ricostruzione stato: `apply(event)` per ogni aggregato + `rehydrate(events)`
- invarianti: dove si validano (command handler vs aggregate) e come si gestiscono side-effect/rollback
- idempotenza: come evitare doppi eventi quando una sync riparte o un handler viene rieseguito

### 2) CQRS Read Model e consistenza
Il documento cita proiezioni su sled e handler eventi. Serve fissare:
- ordering: dipende da `global_position` e dal guarantee di `subscribe()`
- semantica di delivery: at-least-once implica proiezioni idempotenti e checkpoint persistito
- rebuild: come rigenerare proiezioni/indice da event store (e dove salvare “last processed”)

### 3) Ports/Adapters: dove stanno i trait?
Allinea i trait (EventStore/Repos/Search/Provider) a un modello coerente:
- **Opzione A (consigliata)**: trait in `mc-domain` (ports) + infra implementa + application orchestration
- **Opzione B**: trait in `mc-application` (ports applicativi) + domain puro senza trait I/O

In entrambi i casi, riduci dipendenze: idealmente `mc-infrastructure` non dovrebbe aver bisogno di `mc-application`.

### 4) Trait async, object-safety, mocking
Decidi subito se userai generics o `dyn`. Se prevedi mocking e DI semplice, considera:
- `async_trait` per i ports
- `BoxFuture` / `BoxStream` per metodi stream
- evitare `impl Stream` nei trait se vuoi trait object

### 5) Sicurezza: credenziali e dati locali
Connessioni e token (OAuth2/IMAP password) non dovrebbero finire in chiaro in TOML.
- Valuta integrazione con keyring (OS) o cifratura a riposo (anche semplice) per config account
- Chiarisci threat model: dispositivo compromesso? multi-utente?

### 6) Scelta storage: sled come base
Se il progetto punta a stabilità, metti esplicitamente “sled è per prototipo” oppure dettaglia come mitigare i rischi (backup/migration, compaction, test di crash-consistency).

### 7) Proto/Interfaces e riuso dal client
Se `mc-ui` deve consumare gRPC, spesso conviene isolare i proto in un crate tipo `mc-proto` (o un modulo dedicato) per evitare che la UI dipenda accidentalmente da `mc-interfaces`.

### 8) Osservabilità
Hai già `tracing`: aggiungi nel documento convenzioni minime:
- naming degli span (es. `sync_account`, `imap_fetch_envelopes`)
- correlation id per richieste gRPC/GraphQL
- logging PII: policy (non loggare body/headers)

