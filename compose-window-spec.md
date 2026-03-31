# Compose Window — Specifica Implementativa

**Progetto:** Client email — Tauri + React  
**Componente:** `ComposeWindow`  
**Versione:** 1.0

---

## Contesto architetturale

La compose window è una **modale** che gestisce la composizione di email in Markdown.  
Il documento è composto da tre zone in sequenza verticale fissa:

1. **Body** — testo libero in Markdown, editabile
2. **Quoted block** — HTML opaco non editabile, presente solo su reply/forward
3. **Signature block** — HTML opaco non editabile, presente se configurata

Al momento dell'invio, le tre zone vengono assemblate in un messaggio MIME `multipart/alternative` con parti `text/plain` e `text/html`. Se presenti immagini inline, il `text/html` è wrappato in `multipart/related`.

---

## Dipendenze tra task

```
TASK-01 (shell modale)
    └── TASK-02 (header area)
    └── TASK-03 (nodi custom Milkdown)
            └── TASK-04 (editor body + toolbar)
                    └── TASK-05 (immagini inline CID)
            └── TASK-06 (toggle MD/Preview)
    └── TASK-07 (gestione identità e firma)
            └── TASK-08 (dialog cambio identità)
    └── TASK-09 (draft persistence)
    └── TASK-10 (pipeline di invio MIME)
```

I task 02, 03, 07 e 09 possono essere sviluppati in parallelo dopo TASK-01.  
TASK-04 dipende da TASK-03. TASK-05 dipende da TASK-04. TASK-08 dipende da TASK-07.  
TASK-10 dipende da tutti.

---

## TASK-01 — Shell della modale

**Obiettivo:** Scaffolding del componente `ComposeWindow` come modale React, senza contenuto interno.

**Output atteso:**
- Componente `ComposeWindow` montabile/smontabile tramite prop `open: boolean`
- Overlay scuro sul contenuto sottostante, modale centrata
- Dimensioni di default: larghezza `720px`, altezza `90vh`, ridimensionabile
- Pulsante chiusura (`✕`) in alto a destra
- La chiusura con bozza non vuota mostra un confirm nativo (`"Chiudere? Le modifiche non salvate andranno perse."`) — logica di "bozza sporca" delegata ai task successivi, per ora sempre `false`
- Prop `mode: 'new' | 'reply' | 'forward'` ricevuta ma non ancora usata
- Prop `initialData` ricevuta ma non ancora usata (struttura da definire in TASK-02)

**Non incluso:** nessuna logica interna, nessun campo, nessun editor.

---

## TASK-02 — Header area (campi destinatari)

**Obiettivo:** Implementare la zona header con i campi `From`, `To`, `CC`, `BCC`, `Subject`.

**Dipende da:** TASK-01

**Struttura `initialData` definita qui:**

```typescript
interface ComposeInitialData {
  mode: 'new' | 'reply' | 'forward';
  from?: string;           // indirizzo preselezionato
  to?: string[];
  cc?: string[];
  bcc?: string[];
  subject?: string;
  quotedHtml?: string;     // usato da TASK-03
  identities: Identity[];  // usato da TASK-07
}
```

**Campi e comportamenti:**

### From
- Dropdown che lista le identità disponibili (label: `Nome <email>`)
- Preseleziona `initialData.from` se presente, altrimenti prima identità
- Stato interno: `selectedIdentity: Identity`
- Cambio identità: per ora senza dialog (aggiunto in TASK-08), aggiorna solo lo stato

### To
- Chip input: ogni chip rappresenta un indirizzo email
- Separatori accettati per confermare una chip: virgola, punto e virgola, Tab, Invio
- Validazione sintattica al blur (regex RFC-5322 semplificata)
- Chip non valida: bordo rosso, non bloccante
- Precompilato da `initialData.to`

### CC / BCC
- Nascosti di default
- Pulsante `+ CC/BCC` sotto il campo `To` che li espande
- Si aprono automaticamente se `initialData.cc` o `initialData.bcc` sono non vuoti
- Stesso comportamento chip di `To`

### Subject
- Input testo semplice
- Precompilato: `Re: <subject>` per reply, `Fwd: <subject>` per forward
- Editabile liberamente

**Stato esposto al parent:**

```typescript
interface HeaderState {
  from: Identity;
  to: string[];
  cc: string[];
  bcc: string[];
  subject: string;
}
```

**Non incluso:** autocompletamento rubrica (fuori scope), dialog cambio identità (TASK-08).

---

## TASK-03 — Nodi custom Milkdown (QuotedBlock, SignatureBlock)

**Obiettivo:** Definire e registrare in Milkdown due nodi ProseMirror custom non editabili.

**Dipende da:** TASK-01

**QuotedBlock:**
- Contiene HTML arbitrario come dato opaco (attributo `html: string`)
- Non editabile: il cursore non entra nel nodo
- In modalità MD: render come box con label `"Citazione originale"`, freccia collapse/expand (`▼/▶`), contenuto HTML renderato in `iframe` sandboxed o `div` isolato con `pointer-events: none`
- Collassato di default
- Handle per eliminare il blocco (icona `✕` sull'hover)

**SignatureBlock:**
- Contiene HTML arbitrario come dato opaco (attributo `html: string`)
- Non editabile: il cursore non entra nel nodo
- In modalità MD: render come box con label `"Firma"`, contenuto HTML renderato
- Non collassabile
- Non eliminabile dall'editor (la visibilità è controllata dal toggle firma in toolbar — TASK-04)

**Posizionamento:**
- `QuotedBlock` e `SignatureBlock` sono sempre gli ultimi nodi del documento, in quest'ordine
- Il plugin deve impedire che vengano spostati o che venga inserito contenuto dopo di essi

**Serializzazione:**
- In modalità MD raw, i nodi appaiono come commenti HTML speciali:
  - `<!-- quoted-block: <base64 dell'HTML> -->`
  - `<!-- signature-block: <base64 dell'HTML> -->`
- Il parser riconosce questi commenti e ricostruisce i nodi al caricamento del documento

**Non incluso:** il rendering visivo definitivo dei blocchi, popolamento dati reali (TASK-07 per la firma, `initialData` per la citazione).

---

## TASK-04 — Editor body e toolbar

**Obiettivo:** Integrare Milkdown come editor del body e implementare la toolbar.

**Dipende da:** TASK-03

**Editor:**
- Milkdown con preset GFM (tabelle, task list, strikethrough)
- Placeholder `"Scrivi il tuo messaggio..."` visibile quando vuoto
- Focus automatico all'apertura della modale
- Altezza fluida, scroll interno se il contenuto supera lo spazio disponibile
- Inizializzato con `QuotedBlock` se `initialData.quotedHtml` è presente
- `SignatureBlock` inserito se l'identità selezionata ha una firma (coordinato con TASK-07)

**Toolbar — sezione MD:**

| Azione | Comportamento | Shortcut |
|--------|--------------|----------|
| Bold | Wrappa la selezione in `**...**` | `Cmd+B` |
| Italic | Wrappa la selezione in `_..._` | `Cmd+I` |
| Inline code | Wrappa la selezione in `` `...` `` | `Cmd+E` |
| Insert link | Popover inline con campo URL; testo selezionato diventa la label | `Cmd+K` |
| Lista | Toggle lista puntata → numerata → nessuna | — |
| Separatore | Inserisce `\n---\n` alla posizione del cursore | — |

**Toolbar — sezione mail:**

| Azione | Comportamento |
|--------|--------------|
| 📎 Allega | Apre file picker nativo Tauri; i file vengono aggiunti alla lista allegati (non inline) mostrata sotto la toolbar |
| Firma | Toggle: mostra/nasconde il `SignatureBlock` nell'editor |
| 🔍 Palette | Apre la command palette |

**Shortcut globali:**

| Shortcut | Azione |
|----------|--------|
| `Cmd+Shift+K` | Apre command palette |
| `Cmd+Enter` | Invia (evento delegato al parent) |
| `Cmd+S` | Salva bozza (evento delegato al parent) |

**Command palette (`Cmd+Shift+K`):**
- Input di ricerca fuzzy
- Voci: `Invia`, `Salva bozza`, `Allega file`, `Inserisci tabella`, `Inserisci blocco codice`, `Toggle firma`, `Cambia mittente`
- Navigabile con frecce, confermabile con Invio

**Non incluso:** immagini inline (TASK-05), toggle MD/Preview (TASK-06).

---

## TASK-05 — Immagini inline CID

**Obiettivo:** Gestire drag&drop e paste di immagini nell'editor con trasformazione in riferimenti CID.

**Dipende da:** TASK-04

**Flusso:**
1. L'utente trascina o incolla un'immagine nel body dell'editor
2. Viene generato un identificatore `cid:<uuid>@compose`
3. Il blob viene salvato in una mappa in memoria: `Map<cid, InlineImage>`
4. Nel documento MD viene inserito: `![](cid:<uuid>@compose)`
5. Il renderer risolve `cid:...` → `URL.createObjectURL(blob)` per l'anteprima locale

**Formati accettati:** `image/png`, `image/jpeg`, `image/gif`, `image/webp`

**Limite dimensione:** 10 MB per immagine; superato il limite mostrare un toast di errore e non inserire.

**Immagini da URL:** inserite come normali link Markdown `![alt](https://...)`, nessuna gestione CID.

**Stato esposto al parent:**

```typescript
interface InlineImage {
  cid: string;
  blob: Blob;
  mimeType: string;
}
// accessibile via ref o context come Map<string, InlineImage>
```

**Non incluso:** serializzazione MIME (TASK-10).

---

## TASK-06 — Toggle MD / Preview

**Obiettivo:** Implementare il toggle tra modalità editing e modalità preview HTML.

**Dipende da:** TASK-03, TASK-04

**UI:** pulsanti `[MD]` `[Preview]` in alto a destra dell'editor. Stato default: `MD`.

**Modalità MD:**
- Editor Milkdown attivo e interattivo
- Nodi custom visualizzati come descritto in TASK-03

**Modalità Preview:**
- Editor Milkdown nascosto (non smontato, stato preservato)
- Body MD → HTML tramite pipeline remark/unified
- `QuotedBlock` renderato come `<blockquote>` con stile mail (bordo sinistro, testo grigio)
- `SignatureBlock` renderato come `<div class="signature">` con l'HTML della firma
- Immagini CID: `cid:...` sostituiti con `URL.createObjectURL(blob)` per anteprima locale
- Contenuto in sola lettura
- Link cliccabili aprono nel browser esterno tramite `shell.open` Tauri

**Non incluso:** stile email-accurate cross-client della preview.

---

## TASK-07 — Gestione identità e firma

**Obiettivo:** Collegare la selezione del mittente alla firma corrispondente nell'editor.

**Dipende da:** TASK-03, TASK-04

**Struttura `Identity`:**

```typescript
interface Identity {
  id: string;
  displayName: string;
  email: string;
  signatureHtml?: string;  // assente = nessuna firma per questa identità
}
```

**Comportamenti:**
- All'apertura della modale, se l'identità selezionata ha `signatureHtml`, il `SignatureBlock` viene inserito nell'editor
- Se l'identità non ha firma, il `SignatureBlock` non è presente e il toggle firma in toolbar è disabilitato

**Cambio identità senza reply aperta (`mode !== 'reply'`):**
- Nessun dialog
- Il `SignatureBlock` viene sostituito silenziosamente con la firma della nuova identità
- Se la nuova identità non ha firma, il blocco viene rimosso

**Cambio identità con reply aperta (`mode === 'reply'`):**
- Comportamento definito in TASK-08

---

## TASK-08 — Dialog cambio identità su reply

**Obiettivo:** Mostrare un dialog bloccante quando l'utente cambia mittente mentre risponde a una conversazione.

**Dipende da:** TASK-07

**Trigger:** cambio valore dropdown `From` quando `mode === 'reply'`

**Dialog:**

```
┌──────────────────────────────────────────────────────┐
│  Cambia mittente?                                     │
│                                                       │
│  Stai rispondendo come [identità precedente].         │
│  Passando a [nuova identità] la firma verrà           │
│  aggiornata di conseguenza.                           │
│                                                       │
│                         [ Annulla ]  [ Conferma ]     │
└──────────────────────────────────────────────────────┘
```

**Conferma:** aggiorna `selectedIdentity`, sostituisce il `SignatureBlock` con la firma della nuova identità (o rimuove se assente).

**Annulla:** il dropdown torna all'identità precedente, nessuna modifica al documento.

Il dialog è bloccante: non è possibile interagire con la modale sottostante finché non viene chiuso.

---

## TASK-09 — Draft persistence

**Obiettivo:** Salvare e ripristinare le bozze localmente tramite filesystem Tauri.

**Dipende da:** TASK-02, TASK-04

**Formato bozza:**

```typescript
interface Draft {
  id: string;             // uuid generato alla prima apertura della modale
  savedAt: string;        // ISO 8601
  header: HeaderState;
  bodyMd: string;         // MD serializzato inclusi commenti nodi custom
  inlineImages: Array<{
    cid: string;
    filePath: string;     // path relativo a appDataDir/drafts/<id>/
    mimeType: string;
  }>;
}
```

**Salvataggio:**
- Trigger: `Cmd+S`, pulsante `Salva bozza`, chiusura modale con bozza dirty
- Path: `appDataDir/drafts/<id>.json`; blob immagini come file separati in `appDataDir/drafts/<id>/`
- Autosave ogni 60 secondi se il documento è stato modificato dall'ultimo salvataggio (debounce)

**Ripristino:**
- La modale accetta prop `draftId?: string`; se presente, carica la bozza invece di `initialData`
- I blob immagini vengono ricaricati dal filesystem e reinseriti nella mappa CID in memoria

**Stato dirty:**
- `true` se il documento è stato modificato dopo l'ultimo salvataggio
- La chiusura con stato dirty attiva il confirm definito in TASK-01

---

## TASK-10 — Pipeline di invio MIME

**Obiettivo:** Assemblare il messaggio finale in formato MIME e consegnarlo al layer di trasporto.

**Dipende da:** tutti i task precedenti

**Input:**

```typescript
interface SendPayload {
  header: HeaderState;
  bodyMd: string;
  quotedHtml?: string;
  signatureHtml?: string;
  inlineImages: InlineImage[];
  attachments: File[];
}
```

**Pipeline di assemblaggio:**

```
bodyMd → remark + remark-html → htmlBody

assembla HTML completo:
  <div class="body">{htmlBody}</div>
  <blockquote>{quotedHtml}</blockquote>        (se presente)
  <div class="signature">{signatureHtml}</div> (se presente)

plain text = bodyMd raw con commenti nodi custom rimossi

struttura MIME:

  se inlineImages vuoto:
    multipart/alternative
      └── text/plain
      └── text/html

  se inlineImages presenti:
    multipart/alternative
      └── text/plain
      └── multipart/related
            └── text/html  (src="cid:..." già presenti nel markup)
            └── [image/png|jpeg|...] con Content-ID per ogni immagine

  se attachments presenti:
    multipart/mixed
      └── [struttura sopra]
      └── application/octet-stream per ogni allegato
```

**Headers MIME:**
- `From`, `To`, `CC` (BCC escluso dal corpo), `Subject`
- `Message-ID`: `<uuid>@<dominio mittente>`
- `Date`: timestamp UTC di invio
- `In-Reply-To` + `References`: da `initialData` se `mode === 'reply'` (struttura da concordare con il layer IMAP/SMTP, fuori scope di questo task)

**Validazione pre-invio:**

| Condizione | Comportamento |
|------------|--------------|
| `To` vuoto o indirizzi non validi | Errore bloccante, invio impedito |
| `Subject` vuoto | Confirm: `"Inviare senza oggetto?"` |
| Body vuoto (solo nodi custom, nessun testo) | Confirm: `"Inviare messaggio vuoto?"` |

**Output:** oggetto `MimeMessage` o buffer raw consegnato al layer SMTP/API. I dettagli del trasporto sono fuori scope di questo task.
