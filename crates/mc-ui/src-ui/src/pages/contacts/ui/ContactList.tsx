import { List, type RowComponentProps } from 'react-window';
import { RefreshCw } from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import { useOAuthLogin } from '@/features/manage-accounts';
import type { Row } from '../lib/group';

const HEIGHTS = { account: 30, accountNotice: 58, book: 24, contact: 44 };

const heightOf = (row: Row) =>
  row.kind === 'account' ? (row.notice ? HEIGHTS.accountNotice : HEIGHTS.account) : HEIGHTS[row.kind];

interface RowData {
  rows: Row[];
  selected: string | null;
  onSelect: (key: string) => void;
}

export function ContactList(props: RowData) {
  return (
    <List
      style={{ height: '100%', width: '100%' }}
      rowComponent={ListRow}
      rowCount={props.rows.length}
      rowHeight={(i: number, p: RowData) => heightOf(p.rows[i])}
      rowProps={props}
    />
  );
}

function ListRow({ index, style, rows, selected, onSelect }: RowComponentProps<RowData>) {
  const row = rows[index];
  if (row.kind === 'account') return <AccountHeader style={style} row={row} />;
  if (row.kind === 'book') {
    return (
      <div style={style} className="truncate px-4 pt-1.5 text-[11px] text-muted-foreground">
        {row.label}
      </div>
    );
  }
  const c = row.contact;
  return (
    <button
      type="button"
      style={style}
      onClick={() => onSelect(row.key)}
      className={cn(
        'flex w-full flex-col justify-center px-4 text-left transition-colors hover:bg-surface-hover',
        selected === row.key && 'bg-accent/15',
      )}
    >
      <span className="flex items-center gap-1.5">
        <span className="truncate text-sm text-foreground">{c.displayName}</span>
        {c.dirty && <RefreshCw size={10} className="shrink-0 text-muted-foreground" aria-label="In sincronizzazione" />}
        {c.pushFailed && <span className="size-1.5 shrink-0 rounded-full bg-destructive" title={c.pushFailed} />}
      </span>
      <span className="truncate text-xs text-muted-foreground">{c.organization || c.emails[0] || ' '}</span>
    </button>
  );
}

function AccountHeader({ row, style }: { row: Extract<Row, { kind: 'account' }>; style: React.CSSProperties }) {
  return (
    <div style={style} className="flex flex-col justify-end border-b border-border-subtle px-4 pb-1">
      <span className="truncate text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">{row.label}</span>
      {row.notice === 'missing_permission' && <PermissionNotice accountId={row.accountId} />}
      {row.notice === 'error' && <p className="text-xs text-destructive">Contatti non caricati.</p>}
    </div>
  );
}

function PermissionNotice({ accountId }: { accountId: string }) {
  const { reauth, event, pending } = useOAuthLogin();
  return (
    <p className="truncate text-xs text-amber-500">
      Permesso mancante.{' '}
      <button type="button" className="underline disabled:opacity-60" disabled={pending} onClick={() => reauth(accountId)}>
        {pending ? 'Apri il browser…' : 'Ri-autentica'}
      </button>
      {event?.kind === 'failed' && <span className="ml-1 text-destructive">{event.message}</span>}
    </p>
  );
}
