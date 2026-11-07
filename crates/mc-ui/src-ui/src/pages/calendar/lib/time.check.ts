// Run with `bun run src/pages/calendar/lib/time.check.ts` (the repo has no frontend test runner).
import { atMinute, coversDay, dayMinutes, layoutColumns, moved, resized, snap, stepDate, viewDays } from './time';

const eq = (actual: unknown, expected: unknown, what: string) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${what}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
  }
};
const d = (y: number, m: number, day: number, h = 0, min = 0) => new Date(y, m - 1, day, h, min);
const iso = (x: Date) => `${x.getFullYear()}-${x.getMonth() + 1}-${x.getDate()}`;

// 2026-11-07 is a Saturday: the week runs Monday 2 to Sunday 8.
eq(viewDays('week', d(2026, 11, 7)).map(iso), ['2026-11-2', '2026-11-3', '2026-11-4', '2026-11-5', '2026-11-6', '2026-11-7', '2026-11-8'], 'week');
eq(viewDays('workweek', d(2026, 11, 7)).length, 5, 'workweek');
const month = viewDays('month', d(2026, 11, 7));
eq([iso(month[0]), month.length], ['2026-10-26', 42], 'month grid');
eq(iso(stepDate('month', d(2026, 1, 31), 1)), '2026-2-28', 'month step clamps');

// All-day: UTC midnight, exclusive end. Nov 10 to Nov 11 inclusive, in any zone.
const allDay = { start: new Date(Date.UTC(2026, 10, 10)), end: new Date(Date.UTC(2026, 10, 12)), allDay: true };
eq([10, 11, 12, 9].map((n) => coversDay(allDay, d(2026, 11, n))), [true, true, false, false], 'all-day cover');
const oneDay = { start: new Date(Date.UTC(2026, 10, 10)), end: new Date(Date.UTC(2026, 10, 11)), allDay: true };
eq([9, 10, 11].map((n) => coversDay(oneDay, d(2026, 11, n))), [false, true, false], 'single all-day');

// Timed: ending at midnight does not touch the next day; crossing midnight is clipped per day.
const late = { start: d(2026, 11, 10, 22), end: d(2026, 11, 11, 0), allDay: false };
eq([10, 11].map((n) => coversDay(late, d(2026, 11, n))), [true, false], 'ends at midnight');
const night = { start: d(2026, 11, 10, 22), end: d(2026, 11, 11, 2), allDay: false };
eq(dayMinutes(night, d(2026, 11, 10)), { top: 1320, bottom: 1440 }, 'night day 1');
eq(dayMinutes(night, d(2026, 11, 11)), { top: 0, bottom: 120 }, 'night day 2');
eq(dayMinutes({ start: d(2026, 11, 10, 9), end: d(2026, 11, 10, 9), allDay: false }, d(2026, 11, 10)), { top: 540, bottom: 555 }, 'zero length gets a step');

// Overlaps: A 9-11, B 10-12 side by side, C 13-14 alone, D 9:30-10 reuses B's column, free until 10.
eq(
  layoutColumns([{ top: 540, bottom: 660 }, { top: 600, bottom: 720 }, { top: 780, bottom: 840 }, { top: 570, bottom: 600 }]),
  [{ col: 0, cols: 2 }, { col: 1, cols: 2 }, { col: 0, cols: 1 }, { col: 1, cols: 2 }],
  'overlap',
);
eq(layoutColumns([{ top: 0, bottom: 60 }, { top: 60, bottom: 120 }]), [{ col: 0, cols: 1 }, { col: 0, cols: 1 }], 'touching blocks do not overlap');

// Gestures.
eq(snap(7), 0, 'snap down');
eq(snap(8), 15, 'snap up');
const ev = { start: d(2026, 11, 10, 9), end: d(2026, 11, 10, 10), allDay: false };
const m = moved(ev, 1, 30);
eq([iso(m.start), m.start.getHours(), m.start.getMinutes(), m.end.getHours()], ['2026-11-11', 9, 30, 10], 'move');
eq(resized(ev, -120).end.getTime(), d(2026, 11, 10, 9, 15).getTime(), 'resize floor');
eq(atMinute(d(2026, 11, 10, 15), 90).getHours(), 1, 'atMinute');

console.log('time.check: ok');
