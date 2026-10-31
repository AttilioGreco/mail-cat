import { Pencil } from 'lucide-react';
import { useSessionStore } from '@/entities/session';
import { useAccounts } from '@/entities/account';
import { useComposeStore } from '@/features/compose-message';
import { useSearchStore } from '@/features/search-messages';
import { AccountAlerts } from './AccountAlerts';
import { FolderTree } from './FolderTree';

export function FolderBar() {
  const { selectedAccountId, selectedMailboxId, selectMailbox } = useSessionStore();
  const openCompose = useComposeStore((s) => s.openCompose);
  const resetSearch = useSearchStore((s) => s.setSearchQuery);
  const { data: accounts = [] } = useAccounts();
  const account = accounts.find((a) => a.id === selectedAccountId);

  return (
    <aside className="flex h-full w-full flex-col bg-surface">
      <div className="border-b border-border p-2">
        <div className="px-1 pb-2">
          <div className="truncate text-sm font-semibold text-foreground">{account?.name ?? ''}</div>
          <div className="truncate text-[11px] text-muted">{account?.email ?? ''}</div>
        </div>
        <button
          type="button"
          onClick={() => openCompose({ mode: 'new', accountId: selectedAccountId })}
          className="mb-1 flex w-full items-center justify-center gap-2 rounded-lg bg-accent px-4 h-8 text-sm font-medium text-accent-foreground transition-colors hover:bg-accent/90"
        >
          <Pencil size={14} />
          Scrivi
        </button>
        {account && <AccountAlerts account={account} />}
      </div>

      <div className="flex-1 overflow-hidden p-2">
        {selectedAccountId ? (
          <FolderTree
            accountId={selectedAccountId}
            labels={account?.mailboxModel === 'labels'}
            selectedMailboxId={selectedMailboxId}
            onSelect={(id) => {
              selectMailbox(id);
              resetSearch(null);
            }}
          />
        ) : (
          <p className="px-2 py-4 text-center text-xs text-muted">Seleziona un account</p>
        )}
      </div>
    </aside>
  );
}
