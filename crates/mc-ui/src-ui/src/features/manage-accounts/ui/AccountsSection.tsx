import { useEffect, useState, type ReactNode } from 'react';
import { Plus, RefreshCw, Trash2, AlertCircle, KeyRound, CheckCircle2, RotateCw } from 'lucide-react';
import {
  useAccounts,
  useAccountStats,
  progressPercent,
  useSyncStore,
  authLabel,
  authState,
  useSecondsUntil,
  type Account,
  type AuthState,
  type BodyBatch,
} from '@/entities/account';
import {
  useRemoveAccount,
  useRestartServer,
  useSetAccountPassword,
  useSetMailboxModel,
  useSyncNow,
} from '../model/useAccountMutations';
import { AddAccountDialog } from './AddAccountDialog';

export function AccountsSection({ accountActions }: { accountActions?: (account: Account) => ReactNode }) {
  const { data: accounts = [], isLoading, error } = useAccounts();
  const [addOpen, setAddOpen] = useState(false);
  const [restartNeeded, setRestartNeeded] = useState(false);
  const restartServer = useRestartServer();

  return (
    <section className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h2 className="text-sm font-semibold text-foreground">Account</h2>
          <p className="text-xs text-muted">Stato, avanzamento download e credenziali</p>
        </div>
        <button
          type="button"
          onClick={() => setAddOpen(true)}
          className="flex items-center gap-1.5 rounded-lg bg-accent px-3 py-2 text-xs font-medium text-accent-foreground transition-colors hover:bg-accent/90"
        >
          <Plus size={13} />
          Aggiungi account
        </button>
      </header>

      {restartNeeded && (
        <div className="flex items-start justify-between gap-3 rounded-lg border border-border bg-surface p-3">
          <p className="text-xs text-foreground">
            Le modifiche agli account diventano attive al riavvio del server.
            {restartServer.isError && (
              <span className="mt-1 block text-destructive">{String(restartServer.error)}</span>
            )}
            {restartServer.isSuccess && (
              <span className="mt-1 block text-muted">Server riavviato.</span>
            )}
          </p>
          <button
            type="button"
            disabled={restartServer.isPending}
            onClick={() => restartServer.mutate(undefined, { onSuccess: () => setRestartNeeded(false) })}
            className="flex shrink-0 items-center gap-1.5 rounded border border-border bg-background px-2.5 py-1.5 text-xs text-foreground hover:bg-surface-hover disabled:opacity-50"
          >
            <RotateCw size={12} className={restartServer.isPending ? 'animate-spin' : undefined} />
            Riavvia server
          </button>
        </div>
      )}

      {isLoading && (
        <p className="flex items-center gap-2 text-xs text-muted">
          <RefreshCw size={12} className="animate-spin" />
          Caricamento account…
        </p>
      )}

      {error && (
        <p className="flex items-center gap-2 text-xs text-destructive">
          <AlertCircle size={12} />
          Server non raggiungibile
        </p>
      )}

      {!isLoading && accounts.length === 0 && (
        <p className="rounded-lg border border-dashed border-border p-6 text-center text-xs text-muted">
          Nessun account configurato.
        </p>
      )}

      <div className="space-y-3">
        {accounts.map((account) => (
          <AccountCard
            key={account.id}
            account={account}
            actions={accountActions?.(account)}
            onRestartNeeded={() => setRestartNeeded(true)}
          />
        ))}
      </div>

      <AddAccountDialog
        open={addOpen}
        onClose={() => setAddOpen(false)}
        onAdded={() => {
          setAddOpen(false);
          setRestartNeeded(true);
        }}
      />
    </section>
  );
}

function AccountCard({
  account,
  actions,
  onRestartNeeded,
}: {
  account: Account;
  /** Buttons other features add next to the built-in ones. */
  actions?: ReactNode;
  onRestartNeeded: () => void;
}) {
  const { data: stats } = useAccountStats(account.id);
  const progress = useSyncStore((s) => s.byAccount[account.id]);
  const lastBatch = useSyncStore((s) => s.lastBodyBatch[account.id]);
  const syncNow = useSyncNow();
  const removeAccount = useRemoveAccount();
  const [confirming, setConfirming] = useState(false);
  // A rejected password is what the user came here to fix.
  const [editingPassword, setEditingPassword] = useState(() => authState(account) === 'credentials_rejected');

  const livePercent = progressPercent(progress);
  // The body backfill worker runs continuously in the background and never
  // "finishes", so it must not count as an active sync — otherwise the spinner
  // never stops and the sync button stays disabled forever.
  const syncing =
    !!progress &&
    progress.phase !== 'complete' &&
    progress.phase !== 'error' &&
    progress.phase !== 'fetching_bodies';

  // Envelopes still to fetch vs bodies still to download — two different backlogs.
  const missingEnvelopes = stats ? Math.max(0, stats.totalMessages - stats.syncedMessages) : 0;

  return (
    <article className="rounded-lg border border-border bg-surface p-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className="truncate text-sm font-medium text-foreground">{account.name}</span>
            <StatusBadge account={account} />
          </div>
          <p className="truncate text-xs text-muted">{account.email}</p>
        </div>

        <div className="flex shrink-0 items-center gap-1.5">
          {actions}
          <button
            type="button"
            title="Sincronizza ora"
            disabled={syncNow.isPending || syncing}
            onClick={() => syncNow.mutate(account.id)}
            className="rounded border border-border bg-background p-1.5 text-muted hover:bg-surface-hover hover:text-foreground disabled:opacity-50"
          >
            <RefreshCw size={13} className={syncing ? 'animate-spin' : undefined} />
          </button>
          <button
            type="button"
            title="Aggiorna password"
            onClick={() => setEditingPassword((v) => !v)}
            className="rounded border border-border bg-background p-1.5 text-muted hover:bg-surface-hover hover:text-foreground"
          >
            <KeyRound size={13} />
          </button>
          <button
            type="button"
            title="Rimuovi account"
            onClick={() => setConfirming(true)}
            className="rounded border border-border bg-background p-1.5 text-muted hover:bg-surface-hover hover:text-destructive"
          >
            <Trash2 size={13} />
          </button>
        </div>
      </div>

      {stats && (
        <div className="mt-3 space-y-2">
          <ProgressRow
            label="Messaggi scaricati"
            done={stats.syncedMessages}
            total={stats.totalMessages}
            missingLabel={`${missingEnvelopes.toLocaleString()} da scaricare`}
          />
          <ProgressRow
            label="Contenuti (body)"
            done={stats.syncedMessages - stats.pendingBodies}
            total={stats.syncedMessages}
            missingLabel={`${stats.pendingBodies.toLocaleString()} in coda`}
          />
          {lastBatch && stats.pendingBodies > 0 && <BodyBatchLine batch={lastBatch} />}
        </div>
      )}

      <MailboxModelField account={account} />

      {progress && (
        <p className="mt-2 flex items-center gap-1.5 text-[11px] text-muted">
          {syncing && <RefreshCw size={10} className="animate-spin" />}
          {progress.phase === 'complete' && <CheckCircle2 size={10} className="text-accent" />}
          {progress.phase === 'error' && <AlertCircle size={10} className="text-destructive" />}
          <span className="truncate">{progress.message ?? progress.phase}</span>
          {/* No total means there is nothing left to do — showing 0% would read as stuck. */}
          {livePercent !== null && <span>· {livePercent}%</span>}
        </p>
      )}

      {editingPassword && (
        <PasswordForm
          accountId={account.id}
          onDone={() => {
            setEditingPassword(false);
            onRestartNeeded();
          }}
          onCancel={() => setEditingPassword(false)}
        />
      )}

      {confirming && (
        <div className="mt-3 rounded border border-destructive/40 bg-background p-3">
          <p className="text-xs text-foreground">
            Rimuovere <strong>{account.email}</strong>? Vengono cancellati la configurazione sul
            server e le credenziali dal keychain.
          </p>
          {removeAccount.isError && (
            <p className="mt-1 text-xs text-destructive">{String(removeAccount.error)}</p>
          )}
          <div className="mt-2 flex gap-2">
            <button
              type="button"
              disabled={removeAccount.isPending}
              onClick={() =>
                removeAccount.mutate(account.id, {
                  onSuccess: () => {
                    setConfirming(false);
                    onRestartNeeded();
                  },
                })
              }
              className="rounded bg-destructive px-2.5 py-1.5 text-xs font-medium text-white disabled:opacity-50"
            >
              {removeAccount.isPending ? 'Rimozione…' : 'Rimuovi'}
            </button>
            <button
              type="button"
              onClick={() => setConfirming(false)}
              className="rounded border border-border bg-background px-2.5 py-1.5 text-xs text-foreground hover:bg-surface-hover"
            >
              Annulla
            </button>
          </div>
        </div>
      )}
    </article>
  );
}

function MailboxModelField({ account }: { account: Account }) {
  const setModel = useSetMailboxModel();
  const id = `mailbox-model-${account.id}`;
  return (
    <div className="mt-3">
      <div className="flex items-center gap-2">
        <label htmlFor={id} className="text-xs text-foreground">Organizzazione</label>
        <select
          id={id}
          value={account.mailboxModel}
          disabled={setModel.isPending}
          onChange={(e) => setModel.mutate({ accountId: account.id, labels: e.target.value === 'labels' })}
          className="rounded border border-border bg-background px-2 py-1 text-xs text-foreground"
        >
          <option value="folders">Cartelle</option>
          <option value="labels">Etichette</option>
        </select>
      </div>
      <p className="mt-1 text-[11px] text-muted">
        Cambia solo come vedi la posta (cartelle o etichette sui messaggi); i dati non vengono modificati.
      </p>
      {setModel.isError && <p className="mt-1 text-xs text-destructive">{String(setModel.error)}</p>}
    </div>
  );
}

function PasswordForm({
  accountId,
  onDone,
  onCancel,
}: {
  accountId: string;
  onDone: () => void;
  onCancel: () => void;
}) {
  const [password, setPassword] = useState('');
  const setAccountPassword = useSetAccountPassword();

  return (
    <form
      className="mt-3 rounded border border-border bg-background p-3"
      onSubmit={(e) => {
        e.preventDefault();
        setAccountPassword.mutate({ accountId, password }, { onSuccess: onDone });
      }}
    >
      <label className="block text-xs text-foreground">
        Nuova password IMAP/SMTP (salvata nel keychain)
        <input
          type="password"
          autoFocus
          autoComplete="current-password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          className="mt-1.5 w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground outline-none focus:border-accent"
        />
      </label>
      {setAccountPassword.isError && (
        <p className="mt-1 text-xs text-destructive">{String(setAccountPassword.error)}</p>
      )}
      <div className="mt-2 flex gap-2">
        <button
          type="submit"
          disabled={!password || setAccountPassword.isPending}
          className="rounded bg-accent px-2.5 py-1.5 text-xs font-medium text-accent-foreground disabled:opacity-50"
        >
          {setAccountPassword.isPending ? 'Salvataggio…' : 'Salva'}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="rounded border border-border bg-background px-2.5 py-1.5 text-xs text-foreground hover:bg-surface-hover"
        >
          Annulla
        </button>
      </div>
    </form>
  );
}

/** Bodies arrive in one batch a minute; a ticking "how long ago" shows the
 *  worker is alive between batches instead of looking stuck. */
function BodyBatchLine({ batch }: { batch: BodyBatch }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);
  const seconds = Math.max(0, Math.round((now - batch.at) / 1000));

  return (
    <p className="flex items-center gap-1.5 text-[11px] text-muted">
      <span className="relative flex h-1.5 w-1.5">
        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-accent opacity-60" />
        <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-accent" />
      </span>
      ultimo lotto +{batch.added.toLocaleString()} · {seconds < 60 ? `${seconds}s fa` : `${Math.floor(seconds / 60)} min fa`}
    </p>
  );
}

function ProgressRow({
  label,
  done,
  total,
  missingLabel,
}: {
  label: string;
  done: number;
  total: number;
  missingLabel: string;
}) {
  const percent = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 100;

  return (
    <div>
      <div className="mb-1 flex items-baseline justify-between text-[11px]">
        <span className="text-muted">{label}</span>
        <span className="text-foreground">
          {done.toLocaleString()} / {total.toLocaleString()} · {percent}%
        </span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-background">
        <div className="h-full rounded-full bg-accent transition-[width] duration-700 ease-out" style={{ width: `${percent}%` }} />
      </div>
      <p className="mt-0.5 text-[11px] text-muted">{missingLabel}</p>
    </div>
  );
}

function StatusBadge({ account }: { account: Account }) {
  const state = authState(account);
  const secondsLeft = useSecondsUntil(state === 'offline' ? account.auth?.nextRetry : null);
  const styles: Record<AuthState, string> = {
    active: 'border-accent/40 text-accent',
    offline: 'border-border text-muted',
    needs_reauth: 'border-amber-500/40 text-amber-500',
    credentials_rejected: 'border-amber-500/40 text-amber-500',
    oauth_client_invalid: 'border-destructive/40 text-destructive',
    unknown: 'border-border text-muted',
  };
  const needsUser = state === 'needs_reauth' || state === 'credentials_rejected';

  return (
    <span
      title={account.auth?.lastError ?? undefined}
      className={`flex shrink-0 items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] ${styles[state]}`}
    >
      {needsUser && <KeyRound size={9} />}
      {authLabel(account, secondsLeft)}
    </span>
  );
}
