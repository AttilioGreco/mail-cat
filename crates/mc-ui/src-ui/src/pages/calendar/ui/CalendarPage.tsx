import { useMemo } from 'react';
import { useAccounts } from '@/entities/account';
import { useVisibleEvents } from '@/entities/calendar';
import { useEventEditor, useReschedule } from '@/features/edit-event';
import { rangeOf, viewDays } from '../lib/time';
import { useCalendarView } from '../model/view-store';
import { CalendarSidebar } from './CalendarSidebar';
import { MonthGrid } from './MonthGrid';
import { TimeGrid } from './TimeGrid';
import { Toolbar } from './Toolbar';

/** The Calendario section: it fills the space beside the account rail. */
export function CalendarPage() {
  const { view, date, setDate, setView } = useCalendarView();
  const { data: accounts = [] } = useAccounts();
  const days = useMemo(() => viewDays(view, date), [view, date]);
  const { events, failed } = useVisibleEvents(accounts, rangeOf(days));
  const openEditor = useEventEditor((s) => s.open);
  const reschedule = useReschedule();

  const onCreate = (start: Date, end: Date, allDay: boolean) => openEditor({ mode: 'create', start, end, allDay });
  const onOpen = (event: (typeof events)[number]) => openEditor({ mode: 'edit', event });

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
            onPickDay={(day) => {
              setDate(day);
              setView('day');
            }}
          />
        ) : (
          <TimeGrid days={days} events={events} onCreate={onCreate} onOpen={onOpen} onReschedule={reschedule} />
        )}
      </section>
    </div>
  );
}
