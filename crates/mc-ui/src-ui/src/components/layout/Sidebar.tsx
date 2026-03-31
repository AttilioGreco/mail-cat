import { ChevronDown, RefreshCw, AlertCircle, Pencil, Settings } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useUIStore } from '@/store/uiStore';
import { useAccounts } from '@/hooks/useAccounts';
import { useMailboxes } from '@/hooks/useMailboxes';
import { useInboxWatcher } from '@/hooks/useInboxWatcher';
import { FolderItem } from '@/components/folders/FolderItem';
import type { Account, Mailbox } from '@/lib/types';

export function Sidebar() {
  const { selectedAccountId, selectedMailbox, selectAccount, selectMailbox, openCompose, openSettings } = useUIStore();
  const { data: accounts = [], isLoading, error } = useAccounts();

  return (
    <aside className="flex h-full w-[220px] shrink-0 flex-col border-r border-border bg-surface">
      <div className="border-b border-border p-2">
        <button
          type="button"
          onClick={() => openCompose({ mode: 'new', identities: [] })}
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
            onSelect={() => selectAccount(account.id)}
          />
        ))}
      </div>

      <div className="flex-1 overflow-y-auto p-2">
        {selectedAccountId ? (
          <FolderTree
            accountId={selectedAccountId}
            selectedMailboxId={selectedMailbox?.id ?? null}
            onSelect={selectMailbox}
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
          Impostazioni Locali
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
  onSelect: (mailbox: Mailbox) => void;
}) {
  const { data: mailboxes = [], isLoading, error } = useMailboxes(accountId);

  // Keep INBOX watched permanently so unread badges update even when viewing another folder.
  const inbox = mailboxes.find((m) => m.path.toUpperCase() === 'INBOX') ?? null;
  useInboxWatcher(accountId, inbox);

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
          onSelect={() => onSelect(mailbox)}
        />
      ))}
    </nav>
  );
}
