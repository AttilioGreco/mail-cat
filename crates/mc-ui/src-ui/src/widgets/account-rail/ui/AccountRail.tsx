import { useEffect, useState } from 'react';
import { AlertCircle, Plus, RefreshCw, Settings } from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import { useSessionStore } from '@/entities/session';
import { authState, useAccounts, type Account } from '@/entities/account';
import { subscribedOnly, useMailboxes } from '@/entities/mailbox';
import { useInboxWatcher } from '@/features/watch-mailbox';
import { useSettingsStore } from '@/features/manage-settings';
import { AddAccountDialog } from '@/features/manage-accounts';
import { useSearchStore } from '@/features/search-messages';
import { accountInitials, formatBadge } from '../model/badge';

export function AccountRail() {
  const { selectedAccountId, selectAccount } = useSessionStore();
  const openSettings = useSettingsStore((s) => s.openSettings);
  const resetSearch = useSearchStore((s) => s.setSearchQuery);
  const { data: accounts = [], isLoading, error } = useAccounts();
  const [addOpen, setAddOpen] = useState(false);

  // Land on an account at startup: the persisted one if it still exists, else the first.
  useEffect(() => {
    if (accounts.length === 0) return;
    if (accounts.some((a) => a.id === selectedAccountId)) return;
    selectAccount(accounts[0].id);
  }, [accounts, selectedAccountId, selectAccount]);

  return (
    <nav className="flex h-full w-14 shrink-0 flex-col items-center gap-2 border-r border-border bg-background py-2">
      {isLoading && <RefreshCw size={14} className="mt-2 animate-spin text-muted" />}
      {error && <AlertCircle size={14} className="mt-2 text-destructive" aria-label="Server non raggiungibile" />}
      <div className="flex min-h-0 flex-1 flex-col items-center gap-2 overflow-y-auto overflow-x-hidden p-2">
        {accounts.map((account) => (
          <RailAccount
            key={account.id}
            account={account}
            isSelected={selectedAccountId === account.id}
            onSelect={() => {
              selectAccount(account.id);
              resetSearch(null);
            }}
          />
        ))}
      </div>
      <RailButton label="Aggiungi account" onClick={() => setAddOpen(true)}>
        <Plus size={18} />
      </RailButton>
      <RailButton label="Impostazioni" onClick={openSettings}>
        <Settings size={18} />
      </RailButton>
      <AddAccountDialog open={addOpen} onClose={() => setAddOpen(false)} onAdded={() => setAddOpen(false)} />
    </nav>
  );
}

function RailButton({ label, onClick, children }: { label: string; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      title={label}
      aria-label={label}
      onClick={onClick}
      className="flex h-8 w-8 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
    >
      {children}
    </button>
  );
}

function RailAccount({
  account,
  isSelected,
  onSelect,
}: {
  account: Account;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const { data: mailboxes = [] } = useMailboxes(account.id);
  const inbox = subscribedOnly(mailboxes).find((m) => m.kind === 'inbox') ?? null;
  // Every account keeps its INBOX watched, so the badge is live whichever one is selected.
  useInboxWatcher(account.id, inbox);

  const badge = formatBadge(inbox?.unreadMessages ?? 0);
  const healthy = authState(account) === 'active';
  const hue = [...account.email].reduce((h, c) => h + c.charCodeAt(0), 0) % 360;

  return (
    <div className="relative flex w-full justify-center">
      <button
        type="button"
        onClick={onSelect}
        title={`${account.name} · ${account.email}`}
        className={cn(
          'relative flex h-8 w-8 items-center justify-center rounded-xl text-[11px] font-bold text-white transition-[opacity,box-shadow]',
          isSelected ? 'ring-2 ring-foreground ring-offset-2 ring-offset-background' : 'opacity-70 hover:opacity-100',
        )}
        style={{ backgroundColor: `hsl(${hue}, 55%, 40%)` }}
      >
        {accountInitials(account.name, account.email)}
        {badge && (
          <span className="absolute -right-2 -top-2 min-w-4 rounded-full bg-destructive px-1 ring-2 ring-background text-center text-[10px] font-semibold leading-4 text-white">
            {badge}
          </span>
        )}
        {!healthy && (
          <span className="absolute -bottom-0.5 -right-0.5 h-3 w-3 rounded-full border-2 border-background bg-amber-500" />
        )}
      </button>
    </div>
  );
}
