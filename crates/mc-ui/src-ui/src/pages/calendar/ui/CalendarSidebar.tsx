import { Lock } from 'lucide-react';
import { useAccounts } from '@/entities/account';
import {
  calendarColor, calendarKey, useCalendarGroups, useCalendarVisibility, type CalendarGroup,
} from '@/entities/calendar';
import { useOAuthLogin } from '@/features/manage-accounts';
import { MiniMonth } from './MiniMonth';

export function CalendarSidebar() {
  const groups = useCalendarGroups(useAccounts().data ?? []);
  const empty = groups.length === 0 || groups.every((g) => !g.isLoading && g.calendars.length === 0 && g.feature.state === 'enabled');
  const hasPermissionGap = groups.some((g) => g.feature.state === 'missing_permission');

  return (
    <aside className="flex w-60 shrink-0 flex-col overflow-y-auto border-r border-border bg-surface">
      <MiniMonth />
      <div className="flex flex-col gap-3 px-3 pb-3">
        {groups.map((g) => (
          <Group key={g.account.id} group={g} />
        ))}
        {empty && !hasPermissionGap && (
          <p className="text-xs text-muted-foreground">
            Nessun calendario: gli account collegati non ne hanno, oppure non supportano il calendario.
          </p>
        )}
      </div>
    </aside>
  );
}

function Group({ group }: { group: CalendarGroup }) {
  const { account, feature, calendars } = group;
  const hidden = useCalendarVisibility((s) => s.hidden);
  const toggle = useCalendarVisibility((s) => s.toggle);
  if (feature.state === 'enabled' && calendars.length === 0 && !group.error) return null;

  return (
    <div>
      <div className="mb-1 truncate text-[10px] font-semibold uppercase tracking-wide text-muted-foreground" title={account.email}>
        {account.name}
      </div>
      {feature.state === 'missing_permission' && <PermissionNotice accountId={account.id} />}
      {group.error != null && <p className="text-xs text-destructive">Calendari non caricati.</p>}
      {calendars.map((c) => {
        const key = calendarKey(account.id, c.collection);
        return (
          <label key={key} className="flex cursor-pointer items-center gap-2 rounded px-1 py-0.5 text-sm hover:bg-surface-hover">
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
