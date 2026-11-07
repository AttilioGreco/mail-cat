import { addDays, format } from 'date-fns';
import { utcDay, type EventOccurrence } from '@/entities/calendar';

export interface EventForm {
  summary: string;
  /** `calendarKey` of the target calendar. */
  calendar: string;
  allDay: boolean;
  startDate: string;
  startTime: string;
  /** For an all-day event this is the last day shown, not the exclusive iCal end. */
  endDate: string;
  endTime: string;
  location: string;
  description: string;
}

const DATE = 'yyyy-MM-dd';
const TIME = 'HH:mm';

export function formFromRange(start: Date, end: Date, allDay: boolean, calendar: string): EventForm {
  return {
    summary: '',
    calendar,
    allDay,
    startDate: format(start, DATE),
    startTime: format(start, TIME),
    endDate: format(end, DATE),
    endTime: format(end, TIME),
    location: '',
    description: '',
  };
}

export function formFromOccurrence(e: EventOccurrence, calendar: string): EventForm {
  const base = e.allDay
    ? formFromRange(utcDay(e.start), addDays(utcDay(e.end), -1), true, calendar)
    : formFromRange(e.start, e.end, false, calendar);
  // An all-day event always shows at least its first day.
  if (base.endDate < base.startDate) base.endDate = base.startDate;
  return { ...base, summary: e.summary, location: e.location ?? '', description: e.description ?? '' };
}

/** The wire times for a form, or null when they are missing or the end is not after the start. */
export function wireTimes(f: EventForm): { start: string; end: string } | null {
  if (f.allDay) {
    const first = new Date(`${f.startDate}T00:00`);
    const last = new Date(`${f.endDate}T00:00`);
    if (Number.isNaN(+first) || Number.isNaN(+last) || last < first) return null;
    return { start: f.startDate, end: format(addDays(last, 1), DATE) };
  }
  const start = new Date(`${f.startDate}T${f.startTime}`);
  const end = new Date(`${f.endDate}T${f.endTime}`);
  if (Number.isNaN(+start) || Number.isNaN(+end) || end <= start) return null;
  return { start: start.toISOString(), end: end.toISOString() };
}

export const timesChanged = (a: EventForm, b: EventForm) =>
  a.allDay !== b.allDay ||
  a.startDate !== b.startDate ||
  a.endDate !== b.endDate ||
  (!a.allDay && (a.startTime !== b.startTime || a.endTime !== b.endTime));
