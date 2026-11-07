import { ChevronLeft, ChevronRight } from 'lucide-react';
import { addMonths, format, isSameDay, isSameMonth } from 'date-fns';
import { cn } from '@/shared/lib/utils';
import { LOCALE, viewDays } from '../lib/time';
import { useCalendarView } from '../model/view-store';

/** Browsing months moves the main view too: one date drives both, so they never disagree. */
export function MiniMonth() {
  const { date, setDate } = useCalendarView();
  const days = viewDays('month', date);
  const today = new Date();
  return (
    <div className="px-3 pb-3 pt-2">
      <div className="mb-1 flex items-center">
        <span className="flex-1 text-sm font-semibold capitalize">{format(date, 'MMMM yyyy', { locale: LOCALE })}</span>
        <button type="button" aria-label="Mese precedente" className="rounded p-0.5 text-muted-foreground hover:bg-surface-hover" onClick={() => setDate(addMonths(date, -1))}>
          <ChevronLeft size={14} />
        </button>
        <button type="button" aria-label="Mese successivo" className="rounded p-0.5 text-muted-foreground hover:bg-surface-hover" onClick={() => setDate(addMonths(date, 1))}>
          <ChevronRight size={14} />
        </button>
      </div>
      <div className="grid grid-cols-7 text-center text-[10px] text-muted-foreground">
        {days.slice(0, 7).map((d) => (
          <span key={+d} className="py-0.5 uppercase">{format(d, 'EEEEE', { locale: LOCALE })}</span>
        ))}
        {days.map((d) => (
          <button
            key={+d}
            type="button"
            onClick={() => setDate(d)}
            className={cn(
              'mx-auto flex h-6 w-6 items-center justify-center rounded-full text-[11px] hover:bg-surface-hover',
              !isSameMonth(d, date) && 'text-muted-foreground/60',
              isSameDay(d, date) && 'ring-1 ring-foreground/40',
              isSameDay(d, today) && 'bg-accent font-semibold text-accent-foreground hover:bg-accent',
            )}
          >
            {format(d, 'd')}
          </button>
        ))}
      </div>
    </div>
  );
}
