import { addDays, startOfDay } from 'date-fns';
import { coversDay } from '@/entities/calendar';

export const AGENDA_DAYS = 8;

interface Item {
  start: Date;
  end: Date;
  allDay: boolean;
}

export interface AgendaDay<T> {
  day: Date;
  events: T[];
}

/** The key that sorts an event inside its day: all-day first, then by start. */
const sortKey = (e: Item) => (e.allDay ? -1 : e.start.getTime());

/**
 * Today and the days after it, each with the events touching it. A multi-day
 * event shows on every day it covers; today is always listed so the pane never looks broken,
 * the other days only when something happens.
 */
export function bucketAgenda<T extends Item>(events: T[], today: Date): AgendaDay<T>[] {
  const first = startOfDay(today);
  const days: AgendaDay<T>[] = [];
  for (let i = 0; i < AGENDA_DAYS; i++) {
    const day = addDays(first, i);
    const covering = events.filter((e) => coversDay(e, day)).sort((a, b) => sortKey(a) - sortKey(b));
    if (i === 0 || covering.length > 0) days.push({ day, events: covering });
  }
  return days;
}
