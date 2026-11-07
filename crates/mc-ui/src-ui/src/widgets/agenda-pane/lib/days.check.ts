// Run with `bun run src/widgets/agenda-pane/lib/days.check.ts`.
import { bucketAgenda } from './days';

const eq = (actual: unknown, expected: unknown, what: string) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${what}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
  }
};

const today = new Date(2026, 10, 7, 15, 0);
const timed = (name: string, d: number, h: number, hours = 1) => ({
  name, allDay: false, start: new Date(2026, 10, d, h), end: new Date(2026, 10, d, h + hours),
});
const allDay = (name: string, d: number, days = 1) => ({
  name, allDay: true, start: new Date(Date.UTC(2026, 10, d)), end: new Date(Date.UTC(2026, 10, d + days)),
});

const days = bucketAgenda(
  [timed('late', 7, 18), allDay('trip', 8, 2), timed('early', 7, 9), allDay('holiday', 7), timed('beyond', 20, 9)],
  today,
);
eq(days.map((d) => d.day.getDate()), [7, 8, 9], 'empty days are skipped, today stays, beyond the window is dropped');
eq(days[0].events.map((e) => e.name), ['holiday', 'early', 'late'], 'all-day first, then by start');
eq(days.slice(1).map((d) => d.events.map((e) => e.name)), [['trip'], ['trip']], 'multi-day event covers each day, exclusive end');
eq(bucketAgenda([], today).map((d) => d.day.getDate()), [7], 'today alone when empty');
