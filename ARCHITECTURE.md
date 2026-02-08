# mail-cat - Architettura DDD Email Client

## Panoramica
Email client modulare in Rust con architettura DDD, CQRS + Event Sourcing. Backend multi-provider (IMAP, Gmail API, Outlook API) con interfacce pulite basate su trait. Due frontend: Slint (nativo via gRPC) e browser (GraphQL). Storage locale con sled, ricerca full-text con tantivy.

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

### EventStore
```rust
pub trait EventStore: Send + Sync {
    async fn append(&self, aggregate_id: &str, aggregate_type: AggregateType, expected_sequence: u64, events: Vec<DomainEvent>) -> Result<u64, EventStoreError>;
    async fn load_events(&self, aggregate_id: &str, from_sequence: u64) -> Result<Vec<EventEnvelope>, EventStoreError>;
    async fn load_all_events(&self, aggregate_type: Option<AggregateType>, from_global_position: u64) -> Result<Vec<EventEnvelope>, EventStoreError>;
    fn subscribe(&self) -> impl Stream<Item = EventEnvelope> + Send;
}
```

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

### sled Projection Store
- **Key schema**:
  - `account:{id}`
  - `mailbox:{account_id}:{id}`
  - `message:{mailbox_id}:{received_at}:{id}`

### tantivy Search Index
- **Schema fields**: message_id, account_id, mailbox_id, from, to, subject, body, date, flags, has_attachments
- **Full-text search** con snippet/highlighting

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
