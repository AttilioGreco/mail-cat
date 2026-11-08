// Run with `bun run src/features/edit-event/lib/form.check.ts`.
import { addReminder, formatReminder, formFromOccurrence, formFromRange, wireTimes, withAllDay } from './form';
import type { EventOccurrence } from '@/entities/calendar';

const eq = (a: unknown, b: unknown, what: string) => {
  if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${what}: got ${JSON.stringify(a)}, want ${JSON.stringify(b)}`);
};
const occ = (start: Date, end: Date, allDay: boolean) =>
  ({ summary: 's', start, end, allDay, location: null, description: null }) as EventOccurrence;

// A two-day all-day event, UTC midnight with exclusive end, shows Nov 10 to Nov 11 and round-trips.
const f = formFromOccurrence(occ(new Date(Date.UTC(2026, 10, 10)), new Date(Date.UTC(2026, 10, 12)), true), 'k', []);
eq([f.startDate, f.endDate], ['2026-11-10', '2026-11-11'], 'all-day form');
eq(wireTimes(f), { start: '2026-11-10', end: '2026-11-12' }, 'all-day wire');

const t = formFromOccurrence(occ(new Date(2026, 10, 10, 9), new Date(2026, 10, 10, 10), false), 'k', []);
eq(wireTimes(t), { start: new Date(2026, 10, 10, 9).toISOString(), end: new Date(2026, 10, 10, 10).toISOString() }, 'timed wire');
eq(wireTimes({ ...t, endTime: '09:00' }), null, 'end must be after start');
eq(wireTimes({ ...f, endDate: '2026-11-09' }), null, 'all-day end before start');

eq([0, -600, -3600, -7200, -86400, -604800, -90, 1800].map((r) => formatReminder(r, false)),
  ["All'ora dell'evento", '10 min prima', '1 ora prima', '2 ore prima', '1 giorno prima', '1 settimana prima', '2 min prima', "30 min dopo l'inizio"], 'format');
eq([formatReminder(-54000, true), formatReminder(32400, true)], ['Il giorno prima alle 9:00', 'Il giorno stesso alle 9:00'], 'all-day format');
eq(addReminder([-600, 0], -300), [-600, -300, 0], 'sorted');
eq(addReminder([-600], -600), [-600], 'no duplicates');
const n = formFromRange(new Date(2026, 10, 10, 9), new Date(2026, 10, 10, 10), false, 'k');
eq(withAllDay(n, true, true).reminders, [-54000], 'default swaps');
eq(withAllDay({ ...n, reminders: [-300] }, true, true).reminders, [-300], 'touched stays');
eq(withAllDay(n, true, false).reminders, [-600], 'existing stays');
console.log('form.check: ok');
