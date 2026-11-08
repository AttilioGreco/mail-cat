import { useEffect, useMemo, useState } from 'react';
import { useAccounts } from '@/entities/account';
import { usePendingEvent, useVisibleEvents } from '@/entities/calendar';
import { useEventEditor, useReschedule } from '@/features/edit-event';
import type { MenuTarget } from '../lib/menu';
import { rangeOf, viewDays } from '../lib/time';
import { useCalendarView } from '../model/view-store';
import { CalendarMenu, type MenuState } from './CalendarMenu';
import { CalendarSidebar } from './CalendarSidebar';
import { MonthGrid } from './MonthGrid';
import { TimeGrid } from './TimeGrid';
import { Toolbar } from './Toolbar';

/** The Calendario section: it fills the space beside the account rail. */
export function CalendarPage() {
  const { view, date, setDate, setView } = useCalendarView();
  const { data: accounts = [] } = useAccounts();
  const days = useMemo(() => viewDays(view, date), [view, date]);
  const range = useMemo(() => rangeOf(days), [days]);
  const { events, failed, loading } = useVisibleEvents(accounts, range);
  const openEditor = useEventEditor((s) => s.open);
  const reschedule = useReschedule();
  const [menu, setMenu] = useState<MenuState | null>(null);

  const pending = usePendingEvent((s) => s.pending);

  // A notification click: go to the occurrence's day, then open it once its events have loaded.
  // Gone (deleted since) means the day alone is shown.
  useEffect(() => {
    if (pending) setDate(new Date(pending.start * 1000));
  }, [pending, setDate]);
  useEffect(() => {
    if (!pending || loading) return;
    const at = pending.start * 1000;
    // The date change above may not have rendered yet: wait for a range that holds the occurrence.
    if (at < range.from.getTime() || at >= range.to.getTime()) return;
    const found = events.find((e) => e.accountId === pending.accountId && e.href === pending.href && e.start.getTime() === pending.start * 1000);
    if (found) openEditor({ mode: 'edit', event: found });
    usePendingEvent.getState().set(null);
  }, [pending, loading, range, events, openEditor]);

  const onCreate = (start: Date, end: Date, allDay: boolean) => openEditor({ mode: 'create', start, end, allDay });
  const onOpen = (event: (typeof events)[number]) => openEditor({ mode: 'edit', event });

  const onMenu = (e: React.MouseEvent, target: MenuTarget) => {
    e.preventDefault();
    e.stopPropagation();
    setMenu({ x: e.clientX, y: e.clientY, target });
  };

  return (
    <div className="flex min-w-0 flex-1 overflow-hidden">
      <CalendarSidebar />
      <section className="flex min-w-0 flex-1 flex-col">
        <Toolbar days={days} />
        {failed.length > 0 && (
          <p className="border-b border-border px-3 py-1 text-xs text-destructive">
            Alcuni eventi non sono stati caricati: gli altri calendari restano visibili.
          </p>
        )}
        {view === 'month' ? (
          <MonthGrid
            days={days}
            date={date}
            events={events}
            onCreate={onCreate}
            onOpen={onOpen}
            onMenu={onMenu}
            onPickDay={(day) => {
              setDate(day);
              setView('day');
            }}
          />
        ) : (
          <TimeGrid days={days} events={events} onCreate={onCreate} onOpen={onOpen} onReschedule={reschedule} onMenu={onMenu} />
        )}
      </section>
      <CalendarMenu menu={menu} onClose={() => setMenu(null)} />
    </div>
  );
}
