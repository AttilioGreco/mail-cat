// Run with `bun run src/features/edit-event/lib/form.check.ts`.
import { formFromOccurrence, wireTimes } from './form';
import type { EventOccurrence } from '@/entities/calendar';

const eq = (a: unknown, b: unknown, what: string) => {
  if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${what}: got ${JSON.stringify(a)}, want ${JSON.stringify(b)}`);
};
const occ = (start: Date, end: Date, allDay: boolean) =>
  ({ summary: 's', start, end, allDay, location: null, description: null }) as EventOccurrence;

// A two-day all-day event, UTC midnight with exclusive end, shows Nov 10 to Nov 11 and round-trips.
const f = formFromOccurrence(occ(new Date(Date.UTC(2026, 10, 10)), new Date(Date.UTC(2026, 10, 12)), true), 'k');
eq([f.startDate, f.endDate], ['2026-11-10', '2026-11-11'], 'all-day form');
eq(wireTimes(f), { start: '2026-11-10', end: '2026-11-12' }, 'all-day wire');

const t = formFromOccurrence(occ(new Date(2026, 10, 10, 9), new Date(2026, 10, 10, 10), false), 'k');
eq(wireTimes(t), { start: new Date(2026, 10, 10, 9).toISOString(), end: new Date(2026, 10, 10, 10).toISOString() }, 'timed wire');
eq(wireTimes({ ...t, endTime: '09:00' }), null, 'end must be after start');
eq(wireTimes({ ...f, endDate: '2026-11-09' }), null, 'all-day end before start');
console.log('form.check: ok');
