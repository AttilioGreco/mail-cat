// Run with `bun run src/pages/calendar/lib/menu.check.ts` (the repo has no frontend test runner).
import type { AccountEvent, Calendar } from '@/entities/calendar';
import { copyOf, eventActions, moveTargets, slotAt } from './menu';

const eq = (actual: unknown, expected: unknown, what: string) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${what}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
  }
};

const ev = (over: Partial<AccountEvent> = {}): AccountEvent => ({
  accountId: 'a', color: '#000', readOnly: false, calendar: '/c1/', href: '/c1/x.ics', summary: 'S',
  start: new Date('2026-11-10T09:00:00Z'), end: new Date('2026-11-10T10:00:00Z'), allDay: false,
  recurring: false, location: null, status: null, description: null, dirty: false, pushFailed: null, ...over,
});

eq(eventActions(ev()), ['open', 'duplicate', 'move', 'delete'], 'plain');
eq(eventActions(ev({ recurring: true })), ['open', 'delete'], 'series');
eq(eventActions(ev({ readOnly: true, recurring: true })), ['open'], 'read-only');

const cal = (collection: string, readOnly = false) => ({ collection, readOnly }) as Calendar;
eq(moveTargets(ev(), [cal('/c1/'), cal('/c2/'), cal('/c3/', true)]).map((c) => c.collection), ['/c2/'], 'targets');

const day = new Date(2026, 10, 10);
const slot = slotAt(day, 9 * 60 + 40);
eq([slot.start.getHours(), slot.start.getMinutes(), slot.end.getHours(), slot.end.getMinutes()], [9, 30, 10, 30], 'slot snaps down');

const allDay = copyOf(ev({ allDay: true, start: new Date(Date.UTC(2026, 10, 10)), end: new Date(Date.UTC(2026, 10, 12)), location: 'X' }), '/c2/');
eq([allDay.start, allDay.end, allDay.location, allDay.calendar], ['2026-11-10', '2026-11-12', 'X', '/c2/'], 'all-day copy');
eq(copyOf(ev(), '/c1/').start, '2026-11-10T09:00:00.000Z', 'timed copy');
