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
  /** Seconds relative to the start, negative is before (iCalendar TRIGGER). */
  reminders: number[];
}

const MIN = 60;
const DAY = 86_400;
/** All-day offsets are from midnight of the first day, so "the day before at 9:00" is -15h. */
export const SAME_DAY_9 = 9 * 3600;
export const DAY_BEFORE_9 = -15 * 3600;
export const defaultReminder = (allDay: boolean) => (allDay ? DAY_BEFORE_9 : -10 * MIN);

export const TIMED_REMINDERS = [0, -5 * MIN, -10 * MIN, -15 * MIN, -30 * MIN, -3600, -7200, -DAY, -2 * DAY, -7 * DAY];
export const ALL_DAY_REMINDERS = [SAME_DAY_9, DAY_BEFORE_9];

export const addReminder = (list: number[], seconds: number) =>
  list.includes(seconds) ? list : [...list, seconds].sort((a, b) => a - b);

const plural = (n: number, one: string, many: string) => `${n} ${n === 1 ? one : many}`;

export function formatReminder(seconds: number, allDay: boolean): string {
  if (allDay && seconds === SAME_DAY_9) return 'Il giorno stesso alle 9:00';
  if (allDay && seconds === DAY_BEFORE_9) return 'Il giorno prima alle 9:00';
  if (seconds === 0) return "All'ora dell'evento";
  const n = Math.abs(seconds);
  const suffix = seconds < 0 ? 'prima' : "dopo l'inizio";
  if (n % (7 * DAY) === 0) return `${plural(n / (7 * DAY), 'settimana', 'settimane')} ${suffix}`;
  if (n % DAY === 0) return `${plural(n / DAY, 'giorno', 'giorni')} ${suffix}`;
  if (n % 3600 === 0) return `${plural(n / 3600, 'ora', 'ore')} ${suffix}`;
  return `${Math.round(n / MIN)} min ${suffix}`;
}

/** A new event starts with the default reminder; flipping all-day swaps it while the user has not touched it. */
export function withAllDay(f: EventForm, allDay: boolean, isNew: boolean): EventForm {
  const untouched = isNew && f.reminders.length === 1 && f.reminders[0] === defaultReminder(f.allDay);
  return { ...f, allDay, reminders: untouched ? [defaultReminder(allDay)] : f.reminders };
}

export const remindersChanged = (a: EventForm, b: EventForm) =>
  a.reminders.length !== b.reminders.length || a.reminders.some((r, i) => r !== b.reminders[i]);

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
    reminders: [defaultReminder(allDay)],
  };
}

export function formFromOccurrence(e: EventOccurrence, calendar: string, reminders: number[]): EventForm {
  const base = e.allDay
    ? formFromRange(utcDay(e.start), addDays(utcDay(e.end), -1), true, calendar)
    : formFromRange(e.start, e.end, false, calendar);
  // An all-day event always shows at least its first day.
  if (base.endDate < base.startDate) base.endDate = base.startDate;
  return { ...base, summary: e.summary, location: e.location ?? '', description: e.description ?? '', reminders };
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

/** Applies a start change and moves the end by the same amount, so the duration survives (as Outlook does). */
export function withStart(f: EventForm, patch: Partial<Pick<EventForm, 'startDate' | 'startTime'>>): EventForm {
  const next = { ...f, ...patch };
  const at = (d: string, t: string) => new Date(`${d}T${f.allDay ? '00:00' : t}`);
  const span = +at(f.endDate, f.endTime) - +at(f.startDate, f.startTime);
  const start = at(next.startDate, next.startTime);
  if (Number.isNaN(span) || Number.isNaN(+start)) return next;
  const end = new Date(+start + span);
  // Date arithmetic in ms would be an hour off across DST, so all-day spans are re-anchored by calendar day.
  const endDay = f.allDay ? addDays(start, Math.round(span / 864e5)) : end;
  return { ...next, endDate: format(endDay, DATE), endTime: format(end, TIME) };
}
