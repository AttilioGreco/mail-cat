# Design — segreti nel keychain di sistema (cross-OS)

Data: 2026-07-06
Stato: approvato (brainstorming), da tradurre in piano di implementazione.

## Obiettivo

Togliere tutti i segreti (password IMAP, password SMTP dedicata, token OAuth2)
dai file su disco — sia il `config.toml` sia l'event store sled — e conservarli
solo nel password manager del sistema operativo, in modo trasparente su
**Linux, macOS, Windows**.

## Scelte confermate col committente

- **Portata**: tutti i segreti, inclusi i token OAuth2.
- **Seeding**: migrazione automatica *live* dal `config.toml` (vedi §5), più un
  comando gRPC per la configurazione futura via UI.
- **Deployment**: il server gira sempre con una sessione desktop presente →
  il Secret Service c'è sempre, **nessun fallback**; se manca, errore netto
  all'avvio.

## 1. Dipendenze nuove

- `keyring` (v3) — unico backend cross-OS: Secret Service (GNOME Keyring /
  KWallet via D-Bus) su Linux, Keychain su macOS, Credential Manager su Windows.
- `notify` — file-watcher cross-OS (inotify / FSEvents / ReadDirectoryChangesW)
  per il `config.toml`. NB: `notify-rust`, già presente, è per le notifiche
  desktop, non c'entra.
- `toml_edit` — editing del TOML *format-preserving*, necessario per riscrivere
  il file mantenendo commenti/formattazione e iniettare il breadcrumb. `toml`
  da solo (`to_string`) azzera i commenti.

## 2. Approccio: port + adapter

Coerente con i port già esistenti in `mc-domain/src/traits/` (`EventStore`,
`ProviderRegistry`, `EmailProvider`).

**Port** — `mc-domain/src/traits/secret_store.rs`:

```rust
pub trait SecretStore: Send + Sync {
    fn load(&self, email: &str) -> Result<Option<AccountSecrets>>;
    fn store(&self, email: &str, secrets: &AccountSecrets) -> Result<()>;
    fn delete(&self, email: &str) -> Result<()>;
}
```

**Tipo bundle** (stesso layer dominio) — un solo item keychain per account,
serializzato in JSON:

```rust
struct AccountSecrets {
    imap_password: Option<String>,
    smtp_password: Option<String>,   // solo se lo SMTP ha credenziali proprie
    app_password: Option<String>,
    oauth: Option<OAuthSecrets>,     // access_token, refresh_token, expires_at
}
```

**Adapter** — `mc-infrastructure/.../secret/keyring_store.rs`,
`KeyringSecretStore`, avvolge `keyring::Entry::new("mail-cat", email)`:
- `store` → `entry.set_password(&json)`
- `load`  → `entry.get_password()` → deserializza; `keyring::Error::NoEntry` → `Ok(None)`
- `delete`→ `entry.delete_credential()`

**Chiave keychain = email dell'account.** Stabile, leggibile, disponibile già
in fase di migrazione (prima che esista un `AccountId`).

## 3. Modello di dominio — segreti fuori dagli eventi

I campi segreti di `AuthConfig` diventano `#[serde(skip)]`:
`Plain.password`, `AppPassword.app_password`,
`OAuth2.access_token`, `OAuth2.refresh_token`.
Restano serializzati gli username e `OAuth2.expires_at`.

Effetto:
- gli eventi `AccountCreated` / `AccountUpdated` in sled **non contengono più
  segreti**; al replay quei campi tornano stringa vuota e vengono idratati dal
  `SecretStore`;
- si chiude anche il leak gRPC: `domain_auth_to_proto`
  (`mc-interfaces/.../mappers/account.rs`) finisce per clonare un campo vuoto,
  quindi `GetAccount`/`ListAccounts` smettono di esporre le password.

## 4. Flusso dati a runtime

In `mc-server/src/wiring.rs`, per ogni account la password oggi presa da
`account_config.password` viene invece risolta da `secret_store.load(email)` e
usata per costruire `ImapEmailProvider` / `SmtpSender`. Il punto di costruzione
del provider non cambia: cambia solo **la fonte** del segreto.

Se `load` restituisce `None` e non c'è plaintext nel TOML → errore di config
chiaro con l'email interessata e come rimediare (vedi §7).

## 5. Migrazione automatica *live* dei segreti

Una sola funzione idempotente, `ingest_plaintext_secrets(config_path)`:

1. Apre il `config.toml` con `toml_edit`.
2. Per ogni tabella account, se trova `password = "..."` (o lo `password`
   dentro `[accounts.smtp]`) in chiaro:
   - scrive il valore nel keychain (`SecretStore.store`, chiave = email, nel
     campo giusto del bundle: `imap_password` o `smtp_password`);
   - rimuove la coppia chiave/valore e lascia un breadcrumb:
     ```toml
     # password → keychain di sistema (service "mail-cat", key "user@example.com")
     ```
   - riscrive il file.
3. Se non trova alcun plaintext → **no-op**.

La funzione viene invocata:
- **all'avvio** del server (la migrazione one-shot dei config esistenti);
- a **ogni evento** del watcher `notify` sul `config.toml` (ingestione live:
  l'utente incolla `password = "..."`, il demone lo sposta nel keychain e
  riscrive con il commento).

### Proprietà che eliminano complessità

- **Niente loop da auto-scrittura, niente flag di soppressione.** La nostra
  riscrittura elimina il plaintext; l'evento di echo che il watcher riceve
  trova zero plaintext → no-op. Idempotente e auto-terminante.
- **Un solo percorso di codice** per avvio e watcher.

### Caveat

- **Scrittura parziale**: alcuni editor troncano-e-riscrivono, il watcher può
  leggere un TOML incompleto. Se il parse fallisce → log + skip; il prossimo
  evento (file completo) lo riprende. Nessun debouncer. Marcare con commento
  `ponytail:` il punto (ceiling: se in pratica si vedono race, aggiungere
  `notify-debouncer-mini`).
- La riscrittura del file da parte del demone modifica un file che l'utente
  possiede: è desiderato e concordato; il breadcrumb documenta cos'è successo.

Cambio a `AccountConfig` in `config.rs`: `password` diventa
`Option<String>` con `#[serde(default, skip_serializing_if = "Option::is_none")]`;
idem per `SmtpAccountConfig.password` (già `Option`).

## 6. gRPC — predisposizione per la UI

`proto/mail_cat/v1/accounts.proto` + handler in
`mc-interfaces/.../services/account_service.rs`.

- **`AddAccount`** (esiste): il client manda `AuthConfig` col segreto in chiaro.
  L'handler scrive il segreto nel `SecretStore` (chiave = email) e l'evento
  persiste `AuthConfig` senza segreto (grazie a §3). È il percorso "crea
  account con credenziali".
- **`SetCredentials(account_id, AuthConfig)`** — nuovo RPC: aggiorna **solo** le
  credenziali (caso UI "cambia password"). Risolve l'email dall'account e fa
  `secret_store.store(email, ...)`. Nessun evento di dominio con segreti; tocca
  `updated_at` sull'account.
- Le risposte `GetAccount`/`ListAccounts` non contengono più segreti.

## 7. Gestione errori

- **Secret Service assente all'avvio** → errore netto, messaggio esplicito
  ("keychain di sistema non disponibile; mail-cat richiede una sessione
  desktop"). Nessun fallback (scelta confermata).
- **Account senza segreto nel keychain e senza plaintext nel TOML** → errore di
  config chiaro con l'email e come rimediare (incollare la password nel
  `config.toml` per l'ingestione, oppure `SetCredentials`).
- **Parse TOML fallito nel watcher** → log a warning + skip, non abortisce il
  demone.

## 8. OAuth2

- I token vivono nel campo `oauth` del bundle `AccountSecrets`.
- Il refresh a runtime scrive i nuovi token via `SecretStore.store` (aggiorna il
  bundle), **non** emette eventi con token.
- `OAuth2.expires_at` resta anche nell'evento, così si sa quando rinfrescare
  senza leggere il keychain.

## 9. Test

- **Handler / wiring**: mock di `SecretStore` (trait ⇒ banale).
- **Adapter `KeyringSecretStore`**: usare il backend mock in-process di
  `keyring` (`keyring::mock`) per verificare store → load → delete senza toccare
  il keychain reale della macchina/CI.
- **Migrazione**: `config.toml` con `password = "..."` → dopo
  `ingest_plaintext_secrets` il keychain (mock) ha il bundle e il TOML riscritto
  non ha più `password` ma ha il commento breadcrumb.
- **Idempotenza watcher**: seconda chiamata su file già migrato → no-op, nessun
  cambiamento al file.

## 10. File toccati (riassunto)

| File | Cambiamento |
|------|-------------|
| `crates/mc-domain/src/traits/secret_store.rs` | nuovo port `SecretStore` + `AccountSecrets` |
| `crates/mc-domain/src/traits/mod.rs` | export |
| `crates/mc-domain/src/model/account.rs` | `#[serde(skip)]` sui campi segreti di `AuthConfig` |
| `crates/mc-infrastructure/.../secret/keyring_store.rs` | adapter `KeyringSecretStore` |
| `crates/mc-server/src/config.rs` | `password: Option<String>` + skip_serializing_if |
| `crates/mc-server/src/wiring.rs` | risoluzione segreti da `SecretStore`; startup ingest |
| `crates/mc-server/src/secret_ingest.rs` (nuovo) | `ingest_plaintext_secrets` + watcher `notify` |
| `proto/mail_cat/v1/accounts.proto` | RPC `SetCredentials` |
| `crates/mc-interfaces/.../services/account_service.rs` | `add_account` → keychain; nuovo `set_credentials` |
| `crates/mc-interfaces/.../mappers/account.rs` | (nessun segreto da mappare, verifica) |
| `Cargo.toml` / `mc-server/Cargo.toml` / `mc-infrastructure/Cargo.toml` | `keyring`, `notify`, `toml_edit` |

## Non incluso (YAGNI)

- Fallback a file cifrato per deployment headless (deployment sempre desktop).
- Backend keyutils.
- Rotazione/scadenza segreti oltre a OAuth2.
- Debouncer del watcher (aggiungere solo se emergono race reali).
