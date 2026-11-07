import { format, isSameDay, isSameMonth } from 'date-fns';
import { cn } from '@/shared/lib/utils';
import type { AccountEvent } from '@/entities/calendar';
import { LOCALE, atMinute, coversDay } from '../lib/time';
import { EventChip } from './EventBlock';

const MAX_CHIPS = 3;

interface Props {
  days: Date[];
  /** The day the toolbar is on: cells of other months are dimmed. */
  date: Date;
  events: AccountEvent[];
  onCreate: (start: Date, end: Date, allDay: boolean) => void;
  onOpen: (event: AccountEvent) => void;
  onPickDay: (day: Date) => void;
}

const byStart = (a: AccountEvent, b: AccountEvent) => Number(b.allDay) - Number(a.allDay) || +a.start - +b.start;

export function MonthGrid({ days, date, events, onCreate, onOpen, onPickDay }: Props) {
  const today = new Date();
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="grid grid-cols-7 border-b border-border text-center text-xs text-muted-foreground">
        {days.slice(0, 7).map((d) => (
          <div key={+d} className="py-1.5">{format(d, 'EEE', { locale: LOCALE })}</div>
        ))}
      </div>
      <div className="grid min-h-0 flex-1 grid-cols-7" style={{ gridTemplateRows: `repeat(${days.length / 7}, minmax(0, 1fr))` }}>
        {days.map((day) => {
          const list = events.filter((e) => coversDay(e, day)).sort(byStart);
          const hidden = list.length - MAX_CHIPS;
          return (
            <div
              key={+day}
              className="flex min-h-0 flex-col gap-px overflow-hidden border-b border-l border-border-subtle p-0.5"
              onDoubleClick={() => onCreate(atMinute(day, 9 * 60), atMinute(day, 10 * 60), false)}
            >
              <span
                className={cn(
                  'ml-auto flex h-5 min-w-5 items-center justify-center rounded-full px-1 text-[11px]',
                  isSameDay(day, today) ? 'bg-accent font-semibold text-accent-foreground' : isSameMonth(day, date) ? 'text-foreground' : 'text-muted-foreground/60',
                )}
              >
                {format(day, 'd')}
              </span>
              {list.slice(0, MAX_CHIPS).map((e) => (
                <EventChip key={`${e.accountId}|${e.href}|${e.calendar}|${+e.start}`} event={e} onOpen={onOpen} />
              ))}
              {hidden > 0 && (
                <button
                  type="button"
                  className="px-1.5 text-left text-[11px] text-muted-foreground hover:text-foreground"
                  onClick={() => onPickDay(day)}
                  onDoubleClick={(ev) => ev.stopPropagation()}
                >
                  +{hidden} altri
                </button>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
