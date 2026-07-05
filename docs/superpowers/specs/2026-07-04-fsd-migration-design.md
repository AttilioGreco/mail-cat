# Migrazione frontend a Feature-Sliced Design (FSD)

**Data:** 2026-07-04
**Ambito:** `crates/mc-ui/src-ui` (frontend React/Tauri di mail-cat)
**Motivazione:** standardizzazione architetturale su un pattern riconosciuto e documentato (FSD), non guidata da un problema acuto attuale. Adozione **canonica completa** (segmenti standard, public API per slice, enforcement via linter dedicato).

Questo è il primo di due progetti distinti. Il secondo (aggiornamento pacchetti: React RC→stable, Tailwind 3→4, ecc.) è fuori ambito qui e verrà affrontato separatamente dopo che questa migrazione è stabile.

## Stato attuale

Nessun routing: è un'app desktop Tauri a finestra singola. Struttura piatta oggi:
`components/{compose,folders,layout,messages,settings}`, `hooks/`, `lib/`, `store/` — circa 30 file TS/TSX.

## Struttura di destinazione

```
src/
├── app/
│   ├── providers/         QueryProvider.tsx, ThemeProvider.tsx (bootstrap tema, da App.tsx)
│   ├── model/              session-store.ts (ex uiStore: SOLO selezione/navigazione)
│   ├── App.tsx             compone i provider + <MainPage/>
│   └── index.ts
├── pages/
│   └── main/
│       ├── ui/MainPage.tsx  (ex AppShell.tsx — l'unica "pagina" dell'app)
│       └── index.ts
├── widgets/
│   ├── sidebar/            ui/Sidebar.tsx
│   ├── message-list/       ui/MessageList.tsx
│   └── message-pane/       ui/{MessagePane,MessageViewer,ThreadView,EmailContent}.tsx
├── features/
│   ├── compose-message/    ui/{ComposeWindow,ComposeHeader,ComposeEditor,ChipInput,ConfirmIdentityDialog}.tsx (+ milkdown/inlineImagePlugin.ts), model/{composeTypes.ts,compose-store.ts}
│   ├── search-messages/    ui/{SearchBar,SearchResults}.tsx, model/{useSearch.ts,search-store.ts}
│   ├── message-actions/    model/useMessageMutations.ts (mark read/unread, star, move, delete — da useMutations.ts)
│   ├── watch-mailbox/      model/{useMailboxWatcher,useInboxWatcher}.ts
│   ├── manage-settings/    ui/LocalSettingsPanel.tsx, model/settings-store.ts
│   └── toggle-theme/       model/themeStore.ts
├── entities/
│   ├── account/            model/{useAccounts.ts, types.ts}
│   ├── identity/           model/{useIdentities.ts, types.ts}
│   ├── mailbox/            ui/FolderItem.tsx, model/{useMailboxes.ts, types.ts}
│   ├── message/            ui/MessageRow.tsx, model/{useMessages.ts, types.ts}
│   └── conversation/       ui/ConversationRow.tsx, model/{useConversations.ts, types.ts}
├── shared/
│   └── lib/                utils.ts, markdownPreview.ts
└── main.tsx                 entry point Vite (resta a src/, referenziato da index.html)
```

`lib/types.ts` viene smontato: ogni tipo (`Account`, `Mailbox`, `MessageEnvelope`, `ConversationSummary`, ecc.) va nel `model/types.ts` della propria entity.

### Nota: ricerca contatti

`ChipInput.tsx` chiama `invoke('search_contacts', ...)` inline (debounced) solo per l'autocomplete destinatari. Non esiste oggi un hook/entity dedicato e nessun'altra UI la usa: resta dentro `features/compose-message` così com'è, niente `entities/contact` — sarebbe un'astrazione prematura per un solo utilizzo.

### Decisioni prese durante il brainstorming

- **Store di sessione** (`selectedAccountId`, `selectedMailbox`, `selectedMessageId`, `selectedThreadMessageId`, `conversationMode`, `showUnreadOnly`): resta un unico store condiviso, spostato in `app/model/session-store.ts` — è navigazione trasversale a più widget, non appartiene a una singola entity.
- **`composeOpen`/`composeData`** e **`settingsOpen`**: spostati fuori da `uiStore`, dentro il model della rispettiva feature (`features/compose-message/model/compose-store.ts`, `features/manage-settings/model/settings-store.ts`). Stesso trattamento per `searchQuery` → `features/search-messages/model/search-store.ts`.
- **Migrazione big-bang** in un'unica passata (codebase piccola, ~30 file), non incrementale per layer.
- **Rigore FSD canonico completo**: segmenti standard (`ui/`, `model/`, ecc. — creati solo dove servono, niente cartelle vuote), `index.ts` come public API di ogni slice, enforcement via linter dedicato.

## Convenzioni

- Ogni slice espone solo un `index.ts` (public API). **Nessun import profondo tra slice**: `widgets/sidebar` può fare `import { useAccounts } from '@/entities/account'`, mai `from '@/entities/account/model/useAccounts'`.
- Regola di dipendenza standard FSD: `app → pages → widgets → features → entities → shared`. Un layer importa solo da layer sottostanti, mai da un fratello o da un layer superiore.
- Alias `@/*` → `./src/*` (già configurato in `tsconfig.json` e `vite.config.ts`, nessuna modifica necessaria).

## Tooling

- **ESLint**: attualmente non funzionante (`npm run lint` esiste come script ma manca sia il pacchetto `eslint` che qualunque file di config). Aggiunto un flat config minimo (`eslint.config.js`) con `typescript-eslint` + `eslint-plugin-react-hooks`. Non è l'upgrade generale dei pacchetti (progetto separato), solo il minimo per avere linting funzionante.
- **Steiger** (`feature-sliced/steiger`): linter standalone dedicato a FSD (non un plugin ESLint), con proprio `steiger.config.ts` e comando CLI. Nuovo script `"lint:fsd": "steiger ./src"`.
- Nuove devDependencies: `eslint`, `typescript-eslint`, `eslint-plugin-react-hooks`, `globals`, `steiger`.

## Meccanica della migrazione

1. Creare la nuova struttura di cartelle; spostare i file con `git mv` (preserva history), slice per slice.
2. Riscrivere tutti gli import (`@/components/...`, `@/hooks/...`, `@/lib/...`, `@/store/...` → `@/entities/...`, `@/features/...`, `@/widgets/...`, `@/app/...`, `@/pages/...`), poi usare `tsc` per stanare riferimenti rimasti.
3. Aggiungere `index.ts` (public API) a ogni slice.
4. Smontare `lib/types.ts` nei rispettivi `entities/*/model/types.ts`.
5. Aggiungere `eslint.config.js` + `steiger.config.ts`; aggiornare `package.json` (script + devDependencies); `bun install`.

## Verifica

- `bun run build` (tsc + vite build) pulito.
- `steiger ./src` senza violazioni di layer/boundary.
- Smoke test manuale nell'app Tauri: selezione account/mailbox, lista messaggi, apertura messaggio, reply/compose, ricerca, filtro non letti, impostazioni, tema.
- `just build-release` finale come gate di chiusura (già validato funzionante prima di questa migrazione).

## Fuori ambito

- Aggiornamento versioni pacchetti (React RC→stable, Tailwind 3→4, TanStack Query, Zustand, ecc.) — progetto separato successivo.
- Qualunque nuova feature o modifica comportamentale: è una migrazione puramente strutturale, il comportamento dell'app deve restare identico.

## Correzioni emerse durante l'implementazione (enforcement Steiger)

L'attivazione di Steiger con `fsd.configs.recommended` ha trovato 3 violazioni reali di boundary non previste dalla progettazione iniziale. Corrette così:

1. **Store di sessione non può vivere in `app/`.** I widget (Sidebar, MessageList, ecc.) devono leggerlo/scriverlo, ma per la regola di dipendenza FSD i widget non possono importare da `app/` (layer superiore). Spostato in **`entities/session`** (layer basso, importabile da tutti sopra) — pattern standard FSD per stato di sessione condiviso. Inoltre `selectedMailbox: Mailbox | null` è stato ridotto a **`selectedMailboxId: string | null`**, perché tenere l'oggetto intero avrebbe richiesto importare l'entity `mailbox` da dentro `entities/session` (cross-import vietato tra slice dello stesso layer). L'unico consumer che aveva bisogno dell'oggetto completo (`MessageList`, per `name`/`path`) ora lo deriva da `useMailboxes(selectedAccountId).find(...)`.

2. **`entities/identity` era usata solo da `features/compose-message`** (violazione `insignificant-slice`) e importava `entities/account` (cross-import vietato tra entity). Risolto fondendo `useIdentities.ts` e il tipo `Identity` dentro `features/compose-message/model` — non è mai stata una vera entity a sé stante.

3. **`features/toggle-theme` era importata da `features/manage-settings`** (cross-import vietato tra feature dello stesso layer). Il tema è letto sia da un widget (`EmailContent`, per `mailContentMode`) sia da una feature (`LocalSettingsPanel`) — esattamente il profilo di uno stato condiviso di basso livello. Spostato in **`entities/theme`**.

**Regola disattivata con motivazione:** `fsd/insignignificant-slice` è disattivata in `steiger.config.ts` per l'intero progetto. mail-cat ha una sola pagina (`pages/main`), quindi ogni widget top-level (sidebar, message-list, message-pane) è per definizione referenziato da un solo posto — l'euristica non si applica a un'app desktop a finestra singola e seguirla alla lettera avrebbe richiesto fondere widget concettualmente distinti solo per silenziare l'avviso.

**Bonus non pianificato:** rimossi `useSearch`/`SearchHitResult` (dead code in `useMutations.ts`, mai referenziati altrove) invece di trascinarli nella nuova struttura, e ripulite 3 righe `// eslint-disable-next-line react/no-danger` residue in `ComposeEditor.tsx` che puntavano a una regola di un plugin (`eslint-plugin-react`) mai installato in questo progetto.

## Verifica effettivamente eseguita

- `npx tsc --noEmit`: pulito.
- `npx steiger ./src`: nessun problema.
- `bun run build` (tsc + vite build): pulito.
- `bun run lint`: 0 errori, 3 warning preesistenti su `react-hooks/exhaustive-deps` (in `ComposeHeader.tsx`, `ComposeWindow.tsx`, `useInboxWatcher.ts`) — mai stati controllati prima perché ESLint non era mai stato configurato in questo progetto. Lasciati invariati: sono comportamento preesistente, non introdotto da questa migrazione, e correggerli rischierebbe di cambiare comportamento a runtime.
- **`just build-release` (gate finale) NON eseguito con successo**: bloccato da un aggiornamento delle dipendenze Rust (`tonic` 0.13→0.14, `tantivy` 0.22→0.26, `toml` 0.8→1.1, ecc.) in corso in parallelo da parte dell'utente in un altro editor, non ancora completato/adattato — non correlato a questa migrazione frontend. Da rieseguire quando quel lavoro sarà concluso.
- Smoke test manuale nell'app Tauri: **non eseguito**, bloccato dallo stesso problema di build Rust (impossibile lanciare `cargo tauri build`/`just dev` finché il workspace Rust non torna a compilare).
