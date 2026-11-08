import { useState } from 'react';
import { Lock, Plus, UserMinus } from 'lucide-react';
import { toast } from 'sonner';
import { useAccounts } from '@/entities/account';
import {
  calendarColor, calendarKey, useCalendarGroups, useCalendarVisibility, useUnsubscribeCalendar,
  type Calendar, type CalendarGroup,
} from '@/entities/calendar';
import { useSessionStore } from '@/entities/session';
import { useOAuthLogin } from '@/features/manage-accounts';
import { selectedFirst } from '@/shared/lib/utils';
import { ContextMenu } from '@/shared/ui/ContextMenu';
import { ColleagueCalendarDialog } from './ColleagueCalendarDialog';
import { MiniMonth } from './MiniMonth';

export function CalendarSidebar() {
  // The account selected in the mail view leads, as its calendars are the likely ones.
  const selected = useSessionStore((s) => s.selectedAccountId);
  const groups = useCalendarGroups(selectedFirst(useAccounts().data ?? [], selected));
  const empty = groups.length === 0 || groups.every((g) => !g.isLoading && g.calendars.length === 0 && g.feature.state === 'enabled');
  const hasPermissionGap = groups.some((g) => g.feature.state === 'missing_permission');
  const [subscribing, setSubscribing] = useState<string | null>(null);

  return (
    <aside className="flex w-60 shrink-0 flex-col overflow-y-auto border-r border-border bg-surface">
      <MiniMonth />
      <div className="flex flex-col gap-3 px-3 pb-3">
        {groups.map((g) => (
          <Group key={g.account.id} group={g} onAdd={() => setSubscribing(g.account.id)} />
        ))}
        {empty && !hasPermissionGap && (
          <p className="text-xs text-muted-foreground">
            Nessun calendario: gli account collegati non ne hanno, oppure non supportano il calendario.
          </p>
        )}
      </div>
      <ColleagueCalendarDialog accountId={subscribing} onClose={() => setSubscribing(null)} />
    </aside>
  );
}

function Group({ group, onAdd }: { group: CalendarGroup; onAdd: () => void }) {
  const { account, feature, calendars } = group;
  const hidden = useCalendarVisibility((s) => s.hidden);
  const toggle = useCalendarVisibility((s) => s.toggle);
  const unsubscribe = useUnsubscribeCalendar();
  const [menu, setMenu] = useState<{ x: number; y: number; calendar: Calendar } | null>(null);
  if (feature.state === 'enabled' && calendars.length === 0 && !group.error) return null;

  return (
    <div>
      <div className="mb-1 flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">
        <span className="min-w-0 flex-1 truncate" title={account.email}>{account.name}</span>
        {feature.state === 'enabled' && account.auth?.oauthProvider !== 'microsoft' && (
          <button
            type="button"
            className="shrink-0 rounded p-0.5 hover:bg-surface-hover hover:text-foreground"
            title="Aggiungi calendario di un collega"
            aria-label="Aggiungi calendario di un collega"
            onClick={onAdd}
          >
            <Plus size={12} />
          </button>
        )}
      </div>
      {feature.state === 'missing_permission' && <PermissionNotice accountId={account.id} />}
      {group.error != null && <p className="text-xs text-destructive">Calendari non caricati.</p>}
      {calendars.map((c) => {
        const key = calendarKey(account.id, c.collection);
        return (
          <label
            key={key}
            className="flex cursor-pointer items-center gap-2 rounded px-1 py-0.5 text-sm hover:bg-surface-hover"
            onContextMenu={(e) => {
              if (c.ownership === 'own') return;
              e.preventDefault();
              setMenu({ x: e.clientX, y: e.clientY, calendar: c });
            }}
          >
            <input
              type="checkbox"
              checked={!hidden.includes(key)}
              onChange={() => toggle(key)}
              style={{ accentColor: calendarColor(account.id, c) }}
            />
            <span className="min-w-0 flex-1 truncate">{c.name}</span>
            {c.readOnly && <Lock size={11} className="shrink-0 text-muted-foreground" aria-label="Sola lettura" />}
          </label>
        );
      })}
      {menu && (
        <ContextMenu x={menu.x} y={menu.y} onClose={() => setMenu(null)}>
          <button
            type="button"
            className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-foreground transition-colors hover:bg-surface-hover"
            onClick={() => {
              const { collection } = menu.calendar;
              setMenu(null);
              unsubscribe.mutate(
                { accountId: account.id, collection },
                { onError: (err) => toast.error(`Impossibile annullare l'iscrizione: ${String(err)}`) },
              );
            }}
          >
            <UserMinus size={14} className="shrink-0" />
            Annulla iscrizione
          </button>
        </ContextMenu>
      )}
    </div>
  );
}

function PermissionNotice({ accountId }: { accountId: string }) {
  const { reauth, event, pending } = useOAuthLogin();
  return (
    <div className="text-xs text-amber-500">
      <p>Permesso mancante per il calendario.</p>
      <button type="button" className="underline disabled:opacity-60" disabled={pending} onClick={() => reauth(accountId)}>
        {pending ? 'Apri il browser…' : 'Ri-autentica'}
      </button>
      {event?.kind === 'failed' && <p className="mt-1 text-destructive">{event.message}</p>}
    </div>
  );
}
