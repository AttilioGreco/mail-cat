import { addDays, startOfDay } from 'date-fns';

/**
 * An all-day event's `start`/`end` are UTC midnight of its dates. Reading the
 * local parts would shift the date a day west of UTC, so rebuild the same
 * calendar date at local midnight instead.
 */
export const utcDay = (d: Date): Date => new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());

interface Timed {
  start: Date;
  end: Date;
  allDay: boolean;
}

/** First and last local day an event touches. */
function spanOf(e: Timed): [Date, Date] {
  if (e.allDay) {
    const first = utcDay(e.start);
    const after = utcDay(e.end);
    return [first, after > first ? addDays(after, -1) : first];
  }
  const first = startOfDay(e.start);
  const lastDay = startOfDay(e.end);
  // An event ending exactly at midnight does not touch the next day.
  return [first, e.end > e.start && +lastDay === +e.end ? addDays(lastDay, -1) : lastDay];
}

export function coversDay(e: Timed, day: Date): boolean {
  const [first, last] = spanOf(e);
  return first <= day && day <= last;
}
