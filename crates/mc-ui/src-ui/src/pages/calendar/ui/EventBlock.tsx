import type { CSSProperties } from 'react';
import { AlertTriangle, RefreshCw } from 'lucide-react';
import { format } from 'date-fns';
import { cn } from '@/shared/lib/utils';
import type { AccountEvent } from '@/entities/calendar';

/** The tint is mixed with the page background so it stays readable in light and dark. */
export const eventStyle = (color: string): CSSProperties => ({
  backgroundColor: `color-mix(in srgb, ${color} 28%, hsl(var(--background)))`,
  borderLeft: `3px solid ${color}`,
});

export const timeRange = (e: AccountEvent) => `${format(e.start, 'HH:mm')}–${format(e.end, 'HH:mm')}`;

/** A pending local change shows a spinner; a refused push shows why on hover and stays editable. */
export function SyncMarker({ event }: { event: AccountEvent }) {
  if (event.pushFailed) {
    return <AlertTriangle size={11} className="shrink-0 text-destructive" aria-label={event.pushFailed}><title>{event.pushFailed}</title></AlertTriangle>;
  }
  if (!event.dirty) return null;
  return <RefreshCw size={10} className="shrink-0 animate-spin text-muted-foreground" aria-label="In sincronizzazione"><title>In sincronizzazione</title></RefreshCw>;
}

export const eventTitle = (e: AccountEvent) =>
  [e.summary || 'Senza titolo', e.location, e.pushFailed && `Non inviato: ${e.pushFailed}`].filter(Boolean).join('\n');

/** The one-line chip used in the all-day strip and the month cells. */
export function EventChip({ event, onOpen, className }: { event: AccountEvent; onOpen: (e: AccountEvent) => void; className?: string }) {
  return (
    <button
      type="button"
      title={eventTitle(event)}
      onClick={(ev) => {
        ev.stopPropagation();
        onOpen(event);
      }}
      onDoubleClick={(ev) => ev.stopPropagation()}
      style={eventStyle(event.color)}
      className={cn('flex w-full items-center gap-1 truncate rounded px-1.5 py-px text-left text-[11px] leading-4 hover:brightness-95 dark:hover:brightness-125', className)}
    >
      {!event.allDay && <span className="shrink-0 text-muted-foreground">{format(event.start, 'HH:mm')}</span>}
      <span className="truncate">{event.summary || 'Senza titolo'}</span>
      <SyncMarker event={event} />
    </button>
  );
}
