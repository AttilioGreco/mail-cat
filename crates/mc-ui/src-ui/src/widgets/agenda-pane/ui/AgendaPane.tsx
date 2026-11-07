import { addDays, format, isSameDay, isToday, startOfDay } from 'date-fns';
import { it } from 'date-fns/locale';
import { X } from 'lucide-react';
import { useAccounts } from '@/entities/account';
import { useAgendaPane, useCalendarGroups, useVisibleEvents, type AccountEvent } from '@/entities/calendar';
import { useEventEditor } from '@/features/edit-event';
import { useOAuthLogin } from '@/features/manage-accounts';
import { AGENDA_DAYS, bucketAgenda } from '../lib/days';

/** Today and the next days across the visible calendars. Mounted only while open, so a closed pane costs nothing. */
export function AgendaPane({ onOpenDay }: { onOpenDay: (day: Date) => void }) {
  const close = useAgendaPane((s) => s.close);
  const { data: accounts = [] } = useAccounts();
  const groups = useCalendarGroups(accounts);
  const today = startOfDay(new Date());
  const range = { from: today, to: addDays(today, AGENDA_DAYS) };
  const { events, failed } = useVisibleEvents(accounts, range);
  const days = bucketAgenda(events, today);
  const missing = groups.filter((g) => g.feature.state === 'missing_permission');
  const calendarName = (e: AccountEvent) =>
    groups.find((g) => g.account.id === e.accountId)?.calendars.find((c) => c.collection === e.calendar)?.name ?? '';

  return (
    <aside className="flex h-full min-w-0 flex-1 flex-col overflow-hidden bg-surface">
      <div className="flex items-center border-b border-border px-3 py-2">
        <button type="button" className="text-sm font-semibold hover:underline" onClick={() => onOpenDay(today)}>
          Agenda
        </button>
        <button
          type="button"
          title="Chiudi l'agenda"
          aria-label="Chiudi l'agenda"
          className="ml-auto rounded p-1 text-muted-foreground hover:bg-surface-hover hover:text-foreground"
          onClick={close}
        >
          <X size={14} />
        </button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-2">
        {missing.map((g) => (
          <PermissionNotice key={g.account.id} accountId={g.account.id} name={g.account.name} />
        ))}
        {failed.length > 0 && <p className="mb-2 text-xs text-destructive">Alcuni eventi non sono stati caricati.</p>}
        {days.map(({ day, events: list }) => (
          <section key={day.getTime()} className="mb-3">
            <button
              type="button"
              className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted-foreground hover:text-foreground"
              onClick={() => onOpenDay(day)}
            >
              {isToday(day) ? 'Oggi' : format(day, 'EEEE d MMMM', { locale: it })}
            </button>
            {list.length === 0 && <p className="text-xs text-muted-foreground">Nessun evento.</p>}
            {list.map((e) => (
              <EventRow key={`${e.accountId}${e.href}${e.start.getTime()}`} event={e} day={day} calendar={calendarName(e)} />
            ))}
          </section>
        ))}
      </div>
    </aside>
  );
}

function EventRow({ event, day, calendar }: { event: AccountEvent; day: Date; calendar: string }) {
  const open = useEventEditor((s) => s.open);
  const hhmm = (d: Date) => format(d, 'HH:mm');
  const time = event.allDay
    ? 'tutto il giorno'
    : isSameDay(event.start, day)
      ? `${hhmm(event.start)}${isSameDay(event.end, day) ? `–${hhmm(event.end)}` : ''}`
      : 'in corso';

  return (
    <button
      type="button"
      onClick={() => open({ mode: 'edit', event })}
      className="flex w-full gap-2 rounded px-1 py-1 text-left hover:bg-surface-hover"
    >
      <span className="w-1 shrink-0 rounded-full" style={{ backgroundColor: event.color }} />
      <span className="min-w-0 flex-1">
        <span className="block truncate text-sm text-foreground">{event.summary || 'Senza titolo'}</span>
        <span className="block truncate text-xs text-muted-foreground">
          {time}
          {calendar && ` · ${calendar}`}
        </span>
      </span>
    </button>
  );
}

function PermissionNotice({ accountId, name }: { accountId: string; name: string }) {
  const { reauth, event, pending } = useOAuthLogin();
  return (
    <p className="mb-2 text-xs text-amber-500">
      Permesso calendario mancante per {name}.{' '}
      <button type="button" className="underline disabled:opacity-60" disabled={pending} onClick={() => reauth(accountId)}>
        {pending ? 'Apri il browser…' : 'Ri-autentica'}
      </button>
      {event?.kind === 'failed' && <span className="ml-1 text-destructive">{event.message}</span>}
    </p>
  );
}
