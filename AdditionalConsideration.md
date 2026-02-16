# mail-cat — Architectural Review & Considerations

> Documento di review architetturale basato sullo stato del progetto (Day 7).  
> Obiettivo: evidenziare punti di forza, rischi reali e decisioni da congelare **prima** che la complessità diventi attrito.

---

## Valutazione Generale

- **Livello architetturale:** Senior / Staff Engineer
- **Coerenza complessiva:** Alta
- **Pulizia dei confini:** Eccellente (Domain realmente puro)
- **Rischio di stallo:** Medio-alto se alcune decisioni restano implicite
- **Riutilizzabilità backend:** Molto alta
- **Probabilità di arrivare a una UI usabile in 30 giorni:** ~60%  
  → ~80% se vengono prese subito 2–3 decisioni chiave

Il progetto non è “overengineered per errore”: è **ambizioso per scelta**.  
Questo è un punto di forza, ma richiede disciplina nelle decisioni.

---

## Punti di Forza Principali

### 1. Confini di dominio estremamente puliti
- `mc-domain` è puro Rust
- Nessuna dipendenza I/O
- Eventi espliciti e modellati correttamente
- Ottima testabilità

Questo è raro e ti protegge da regressioni architetturali future.

---

### 2. Event Catalog ben progettato
- Eventi granulari e semanticamente chiari
- Pensati per CQRS e proiezioni
- Non semplici audit-log “mascherati”

La base per rebuild e read-model è solida.

---

### 3. Ports & Adapters coerenti
- Trait come vere astrazioni (EmailProvider, EventStore, SearchIndex)
- Nessun leakage infrastrutturale nel dominio
- Ottimo allineamento con Hexagonal / Clean Architecture

---

## Punti Critici (Decisioni da Congelare Subito)

### ⚠️ 1. Event Sourcing: completo o “soft”?
Attualmente il progetto si trova in una **zona grigia**:

- Gli aggregati mutano stato direttamente
- Gli eventi vengono emessi
- Esiste un Event Store e una rehydration

Questo è accettabile **solo se dichiarato esplicitamente**.

#### Raccomandazione
Per questa fase del progetto:

- Gli **invarianti** vivono nei command handler
- Gli aggregati **mutano stato direttamente**
- `apply(event)` e `rehydrate(events)` servono per:
  - rebuild
  - proiezioni
  - ripartenze

👉 Event Sourcing usato come **meccanismo di integrazione e ricostruzione**,  
non come unico source of truth dell’aggregato.

Questa scelta va **scritta**, non lasciata implicita.

---

### ⚠️ 2. Trait async, object-safety e Dependency Injection
Le firme attuali (`async fn` + `-> impl Stream`) rendono i trait **non object-safe**.

Questo forza:
- uso intensivo di generics
- wiring complesso
- mocking più difficile

#### Due opzioni valide (sceglierne una):

**Opzione A — Generics ovunque**
- Pro: zero allocazioni, massime performance
- Contro: wiring e test complessi

**Opzione B — Trait object (consigliata)**
- `async_trait`
- `BoxFuture` / `BoxStream`
- `Arc<dyn Trait>`

✔ DI più semplice  
✔ Mocking naturale  
✔ Wiring server/UI più pulito  

👉 Per un client mail **multi-provider**, Opzione B è la scelta più pragmatica.

---

### ⚠️ 3. sled come Event Store: ok ora, ma serve un exit plan
`sled` è una scelta ragionevole per:
- storage locale
- single-user
- prototipo avanzato

Rischi noti:
- replay lunghi
- migrazioni
- corner-case di crash-consistency

#### Raccomandazioni
- Dichiarare esplicitamente che `sled` è **storage iniziale**
- Introdurre:
  - `event_version` nell’EventEnvelope
  - una sezione “Storage Evolution” nel documento
- Garantire che **EventStore sia l’unico punto da sostituire**

Questo rende il rischio **gestibile**, non nascosto.

---

## UI: Slint vs Tauri (alla luce dell’architettura)

Con:
- gRPC + streaming
- GraphQL
- email HTML
- composizione rich text
- search avanzata

👉 **la UI diventa il collo di bottiglia**, non il backend.

### Slint
- Ottimo per UI leggere / embedded
- Poco adatto a:
  - rendering HTML email
  - editor WYSIWYG
  - feature “email client moderno”

### Tauri
- WebView nativa
- HTML/CSS/JS maturi
- Ottimo compromesso performance / DX

#### Raccomandazione
- Tenere `mc-ui-slint` come:
  - viewer minimale
  - client di debug
- Introdurre `mc-ui-tauri` come **client principale**

L’architettura attuale supporta perfettamente entrambi.

---

## Priorità Immediate (Prossimi 2–3 Giorni)

1. **Congelare le decisioni**
   - Event Sourcing “soft” vs completo
   - Generics vs `dyn`
   - Semantica definitiva dell’EventStore

2. **Semplificare il wiring**
   - Meno astrazione dove non serve
   - Bus come orchestrazione, non come magia

3. **Decidere la UI target**
   - Client mail reale → Tauri
   - Slint → opzionale

---

## Conclusione

- Il backend è **molto solido**
- I rischi non sono concettuali, ma **decisionali**
- Ogni scelta non esplicitata oggi diventa attrito domani

Questo progetto può:
- diventare un **client mail che usi davvero**
- oppure fermarsi come **ottimo esercizio architetturale**

La differenza sta nel **tagliare ambiguità ora**, non nel riscrivere codice dopo.
