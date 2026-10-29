import { useEffect, useState } from 'react';
import { ChevronDown, RefreshCw, AlertCircle, Pencil, Settings, MailCheck, KeyRound } from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import { ContextMenu } from '@/shared/ui/ContextMenu';
import { useSessionStore } from '@/entities/session';
import { authLabel, authState, useAccounts, useSecondsUntil, type Account } from '@/entities/account';
import { useMailboxes, FolderItem, type Mailbox } from '@/entities/mailbox';
import { useInboxWatcher } from '@/features/watch-mailbox';
import { useComposeStore } from '@/features/compose-message';
import { useSettingsStore } from '@/features/manage-settings';
import { useOAuthLogin, useRestartServer } from '@/features/manage-accounts';
import { useSearchStore } from '@/features/search-messages';
import { useMarkMailboxRead } from '@/features/message-actions';

export function Sidebar() {
  const { selectedAccountId, selectedMailboxId, selectAccount, selectMailbox } = useSessionStore();
  const openCompose = useComposeStore((s) => s.openCompose);
  const openSettings = useSettingsStore((s) => s.openSettings);
  const resetSearch = useSearchStore((s) => s.setSearchQuery);
  const { data: accounts = [], isLoading, error } = useAccounts();

  const handleSelectAccount = (id: string) => {
    selectAccount(id);
    resetSearch(null);
  };

  const handleSelectMailbox = (mailboxId: string) => {
    selectMailbox(mailboxId);
    resetSearch(null);
  };

  // Land on an account at startup: the persisted one if it still exists, else the first.
  useEffect(() => {
    if (accounts.length === 0) return;
    if (accounts.some((a) => a.id === selectedAccountId)) return;
    selectAccount(accounts[0].id);
  }, [accounts, selectedAccountId, selectAccount]);

  return (
    <aside className="flex h-full w-full flex-col bg-surface">
      <div className="border-b border-border p-2">
        <button
          type="button"
          onClick={() => openCompose({ mode: 'new', accountId: selectedAccountId })}
          className="flex w-full items-center justify-center gap-2 rounded-lg bg-accent px-3 py-2 text-sm font-medium text-accent-foreground transition-colors hover:bg-accent/90 mb-2"
        >
          <Pencil size={14} />
          Scrivi
        </button>
        {isLoading && (
          <div className="flex items-center gap-2 px-2 py-3 text-xs text-muted">
            <RefreshCw size={12} className="animate-spin" />
            Loading accounts…
          </div>
        )}
        {error && (
          <div className="flex items-center gap-2 px-2 py-3 text-xs text-destructive">
            <AlertCircle size={12} />
            Server non raggiungibile
          </div>
        )}
        {accounts.map((account) => (
          <AccountRow
            key={account.id}
            account={account}
            isSelected={selectedAccountId === account.id}
            onSelect={() => handleSelectAccount(account.id)}
          />
        ))}
      </div>

      <div className="flex-1 overflow-hidden p-2">
        {selectedAccountId ? (
          <FolderTree
            accountId={selectedAccountId}
            selectedMailboxId={selectedMailboxId}
            onSelect={handleSelectMailbox}
          />
        ) : (
          <p className="px-2 py-4 text-center text-xs text-muted">Seleziona un account</p>
        )}
      </div>

      <div className="border-t border-border p-2">
        <button
          type="button"
          onClick={openSettings}
          className="flex w-full items-center justify-center gap-2 rounded-md border border-border bg-background px-2.5 py-2 text-xs text-foreground transition-colors hover:bg-surface-hover"
        >
          <Settings size={13} />
          Impostazioni
        </button>
      </div>
    </aside>
  );
}

function AccountRow({
  account,
  isSelected,
  onSelect,
}: {
  account: Account;
  isSelected: boolean;
  onSelect: () => void;
}) {
  return (
    <>
      <button
        type="button"
        onClick={onSelect}
        className={cn(
          'flex w-full items-center gap-2 rounded-md px-2.5 py-2 transition-colors',
          isSelected
            ? 'bg-surface-active text-foreground'
            : 'text-muted-foreground hover:bg-surface-hover hover:text-foreground',
        )}
      >
        <AccountAvatar email={account.email} />
        <div className="flex-1 overflow-hidden text-left">
          <div className="truncate text-xs font-semibold">{account.name}</div>
          <div className="truncate text-[11px] text-muted">{account.email}</div>
        </div>
        <ChevronDown size={12} className="shrink-0 text-muted" />
      </button>
      <AuthBanner account={account} />
      {account.auth?.smtpError && (
        <p className="mx-2.5 mb-1 truncate text-[10px] text-amber-500" title={account.auth.smtpError}>
          Invio bloccato: il server SMTP rifiuta l'accesso
        </p>
      )}
    </>
  );
}

/**
 * Anything that stops mail for an account shows next to it, with its fix —
 * not buried in settings or the logs.
 */
function AuthBanner({ account }: { account: Account }) {
  const state = authState(account);
  const secondsLeft = useSecondsUntil(state === 'offline' ? account.auth?.nextRetry : null);
  const openSettings = useSettingsStore((s) => s.openSettings);
  const restartServer = useRestartServer();
  const label = authLabel(account, secondsLeft);
  const detail = account.auth?.lastError ?? undefined;

  switch (state) {
    case 'offline':
      return (
        <p className="mx-2.5 mb-1 truncate text-[10px] text-muted" title={detail}>
          {label}
        </p>
      );
    case 'needs_reauth':
      // Only an OAuth account can be fixed by logging in again. Without the
      // live status the auth type is unknown, so say nothing about it.
      if (!account.auth) {
        return <AuthAlert label="Accesso richiesto" action="Apri impostazioni" onAction={openSettings} />;
      }
      return account.auth.oauthProvider ? (
        <ReauthBanner accountId={account.id} label={label} />
      ) : (
        <AuthAlert label="Password rifiutata" detail={detail} action="Aggiorna password" onAction={openSettings} />
      );
    case 'credentials_rejected':
      return <AuthAlert label={label} detail={detail} action="Aggiorna password" onAction={openSettings} />;
    case 'oauth_client_invalid':
      return (
        <AuthAlert
          label={label}
          detail={detail}
          action="Riavvia il server"
          busy={restartServer.isPending}
          onAction={() => restartServer.mutate()}
        />
      );
    default:
      return null;
  }
}

function AuthAlert({
  label,
  detail,
  action,
  busy = false,
  onAction,
}: {
  label: string;
  detail?: string;
  action: string;
  busy?: boolean;
  onAction: () => void;
}) {
  return (
    <div className="mx-0.5 mb-1 rounded-md border border-amber-500/40 bg-amber-500/5 px-2 py-1.5">
      <div className="flex items-center gap-1.5 text-[11px] text-amber-500" title={detail}>
        <KeyRound size={11} className="shrink-0" />
        <span>{label}</span>
      </div>
      <button
        type="button"
        disabled={busy}
        onClick={onAction}
        className="mt-1.5 flex w-full items-center justify-center gap-1.5 rounded border border-amber-500/40 px-2 py-1 text-[11px] font-medium text-amber-500 transition-colors hover:bg-amber-500/10 disabled:opacity-60"
      >
        {busy && <RefreshCw size={10} className="animate-spin" />}
        {action}
      </button>
    </div>
  );
}

function ReauthBanner({ accountId, label }: { accountId: string; label: string }) {
  const { reauth, event, pending } = useOAuthLogin();
  // The oauth:event stream is global; this keeps a second stale account's banner
  // from mirroring the progress of the one actually being re-authenticated.
  const [started, setStarted] = useState(false);
  const busy = started && pending;

  return (
    <div className="mx-0.5 mb-1 rounded-md border border-amber-500/40 bg-amber-500/5 px-2 py-1.5">
      <div className="flex items-center gap-1.5 text-[11px] text-amber-500">
        <KeyRound size={11} className="shrink-0" />
        <span>{label}</span>
      </div>
      <button
        type="button"
        disabled={busy}
        onClick={() => {
          setStarted(true);
          reauth(accountId);
        }}
        className="mt-1.5 flex w-full items-center justify-center gap-1.5 rounded border border-amber-500/40 px-2 py-1 text-[11px] font-medium text-amber-500 transition-colors hover:bg-amber-500/10 disabled:opacity-60"
      >
        {busy && <RefreshCw size={10} className="animate-spin" />}
        {busy ? 'Apri il browser…' : 'Ri-autentica'}
      </button>
      {started && event?.kind === 'failed' && (
        <p className="mt-1 text-[10px] text-destructive">{event.message}</p>
      )}
    </div>
  );
}

function AccountAvatar({ email }: { email: string }) {
  const initials = email[0].toUpperCase();
  const hue = [...email].reduce((h, c) => h + c.charCodeAt(0), 0) % 360;
  return (
    <span
      className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-[10px] font-bold text-white"
      style={{ backgroundColor: `hsl(${hue}, 55%, 40%)` }}
    >
      {initials}
    </span>
  );
}

function FolderTree({
  accountId,
  selectedMailboxId,
  onSelect,
}: {
  accountId: string;
  selectedMailboxId: string | null;
  onSelect: (mailboxId: string) => void;
}) {
  const { data: mailboxes = [], isLoading, error } = useMailboxes(accountId);
  const markMailboxRead = useMarkMailboxRead();
  const [menu, setMenu] = useState<{ mailbox: Mailbox; x: number; y: number } | null>(null);

  // Keep INBOX watched permanently so unread badges update even when viewing another folder.
  const inbox = mailboxes.find((m) => m.kind === 'inbox') ?? null;
  useInboxWatcher(accountId, inbox);

  // Same for the folder: restore the persisted one, else default to INBOX.
  useEffect(() => {
    if (mailboxes.length === 0) return;
    if (mailboxes.some((m) => m.id === selectedMailboxId)) return;
    const target = inbox ?? mailboxes[0];
    onSelect(target.id);
  }, [mailboxes, selectedMailboxId, inbox, onSelect]);

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 px-2 py-3 text-xs text-muted">
        <RefreshCw size={12} className="animate-spin" />
        Loading…
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center gap-2 px-2 py-3 text-xs text-destructive">
        <AlertCircle size={12} />
        Errore caricamento mailbox
      </div>
    );
  }

  return (
    <nav className="flex flex-col gap-0.5">
      {mailboxes.map((mailbox) => (
        <FolderItem
          key={mailbox.id}
          mailbox={mailbox}
          isSelected={selectedMailboxId === mailbox.id}
          onSelect={() => onSelect(mailbox.id)}
          onContextMenu={(x, y) => setMenu({ mailbox, x, y })}
        />
      ))}
      {menu && (
        <ContextMenu x={menu.x} y={menu.y} onClose={() => setMenu(null)}>
          <button
            type="button"
            disabled={menu.mailbox.unreadMessages === 0}
            onClick={() => {
              markMailboxRead.mutate(menu.mailbox.id);
              setMenu(null);
            }}
            className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-sm text-foreground transition-colors hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-transparent"
          >
            <MailCheck size={14} className="shrink-0" />
            Segna tutti come letti
          </button>
        </ContextMenu>
      )}
    </nav>
  );
}
