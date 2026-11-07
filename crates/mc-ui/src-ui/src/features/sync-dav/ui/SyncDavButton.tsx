import { RefreshCw } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { it } from 'date-fns/locale';
import { Button } from '@/shared/ui/button';
import { cn } from '@/shared/lib/utils';
import { useAccounts, type Account } from '@/entities/account';
import { useSyncDav } from '../model/useSyncDav';

function describe(account: Account): string {
  const auth = account.auth;
  if (auth?.davLastError) return `${account.name}: ${auth.davLastError}`;
  if (!auth?.davLastSync) return `${account.name}: non ancora sincronizzato`;
  const ago = formatDistanceToNow(new Date(auth.davLastSync), { addSuffix: true, locale: it });
  return `${account.name}: Ultima sincronizzazione: ${ago}`;
}

/** Syncs contacts and calendars now; `feature` picks the accounts it reports on. */
export function SyncDavButton({ feature }: { feature: 'contacts' | 'calendar' }) {
  const accounts = (useAccounts().data ?? []).filter((a) => !a.auth || a.auth[feature].state === 'enabled');
  const sync = useSyncDav();
  const syncing = sync.isPending || accounts.some((a) => a.auth?.davSyncing);
  const tooltip = ['Aggiorna ora', ...accounts.map(describe)].join('\n');

  return (
    <Button size="icon-sm" variant="ghost" title={tooltip} aria-label="Aggiorna ora" onClick={() => sync.mutate()}>
      <RefreshCw className={cn(syncing && 'animate-spin')} />
    </Button>
  );
}
