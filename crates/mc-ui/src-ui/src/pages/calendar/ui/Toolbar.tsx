import { ChevronLeft, ChevronRight } from 'lucide-react';
import { format, isSameMonth, isSameYear } from 'date-fns';
import { cn } from '@/shared/lib/utils';
import { Button } from '@/shared/ui/button';
import { LOCALE, type View } from '../lib/time';
import { useCalendarView } from '../model/view-store';

const VIEWS: { id: View; label: string }[] = [
  { id: 'day', label: 'Giorno' },
  { id: 'workweek', label: 'Settimana lavorativa' },
  { id: 'week', label: 'Settimana' },
  { id: 'month', label: 'Mese' },
];

function title(view: View, days: Date[], date: Date): string {
  if (view === 'day') return format(date, 'EEEE d MMMM yyyy', { locale: LOCALE });
  if (view === 'month') return format(date, 'MMMM yyyy', { locale: LOCALE });
  const [first, last] = [days[0], days[days.length - 1]];
  if (isSameMonth(first, last)) return `${format(first, 'd')} – ${format(last, 'd MMMM yyyy', { locale: LOCALE })}`;
  const from = format(first, isSameYear(first, last) ? 'd MMM' : 'd MMM yyyy', { locale: LOCALE });
  return `${from} – ${format(last, 'd MMM yyyy', { locale: LOCALE })}`;
}

export function Toolbar({ days }: { days: Date[] }) {
  const { view, date, setView, today, step } = useCalendarView();
  return (
    <div className="flex shrink-0 items-center gap-2 border-b border-border px-3 py-2">
      <Button variant="outline" size="sm" onClick={today}>Oggi</Button>
      <Button variant="ghost" size="icon-sm" aria-label="Precedente" onClick={() => step(-1)}><ChevronLeft /></Button>
      <Button variant="ghost" size="icon-sm" aria-label="Successivo" onClick={() => step(1)}><ChevronRight /></Button>
      <h2 className="min-w-0 flex-1 truncate text-sm font-semibold capitalize">{title(view, days, date)}</h2>
      <div className="flex rounded-lg border border-border p-0.5">
        {VIEWS.map((v) => (
          <button
            key={v.id}
            type="button"
            onClick={() => setView(v.id)}
            className={cn(
              'rounded-md px-2.5 py-1 text-xs transition-colors',
              view === v.id ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:bg-surface-hover hover:text-foreground',
            )}
          >
            {v.label}
          </button>
        ))}
      </div>
    </div>
  );
}
