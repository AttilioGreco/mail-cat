import { format } from 'date-fns';
import { utcDay, type AccountEvent, type Calendar, type EventInput } from '@/entities/calendar';
import { SNAP_MINUTES, atMinute } from './time';

export type MenuTarget =
  | { kind: 'slot'; day: Date; start: Date; end: Date; allDay: boolean }
  | { kind: 'event'; event: AccountEvent };

export type EventAction = 'open' | 'duplicate' | 'move' | 'delete';

/** One hour from the clicked minute snapped down to the grid: what a double click creates too. */
export function slotAt(day: Date, minutes: number): { start: Date; end: Date } {
  const top = Math.floor(minutes / SNAP_MINUTES) * SNAP_MINUTES;
  return { start: atMinute(day, top), end: atMinute(day, top + 60) };
}

/** Duplicate and move would act on a whole series (as a drag would), so a series only opens or goes. */
export function eventActions(e: AccountEvent): EventAction[] {
  if (e.readOnly) return ['open'];
  return e.recurring ? ['open', 'delete'] : ['open', 'duplicate', 'move', 'delete'];
}

/** Writable calendars of the event's own account other than the one it is in. */
export const moveTargets = (e: AccountEvent, calendars: Calendar[]) =>
  calendars.filter((c) => !c.readOnly && c.collection !== e.calendar);

/** The copy's wire form. An all-day event is held as UTC midnights with an exclusive end, and goes out as dates. */
export function copyOf(e: AccountEvent, calendar: string): EventInput {
  const [start, end] = e.allDay
    ? [format(utcDay(e.start), 'yyyy-MM-dd'), format(utcDay(e.end), 'yyyy-MM-dd')]
    : [e.start.toISOString(), e.end.toISOString()];
  return {
    calendar,
    summary: e.summary,
    start,
    end,
    location: e.location ?? undefined,
    description: e.description ?? undefined,
  };
}
