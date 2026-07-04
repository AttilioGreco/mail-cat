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
