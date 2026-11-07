import {
  addDays, addMonths, addWeeks, addMinutes, endOfMonth, endOfWeek, getHours, getMinutes,
  isSameDay, startOfDay, startOfMonth, startOfWeek,
} from 'date-fns';
import { it } from 'date-fns/locale';
import { coversDay } from '@/entities/calendar';

export type View = 'day' | 'workweek' | 'week' | 'month';

export const LOCALE = it;
const WEEK = { weekStartsOn: 1 as const };
export const SNAP_MINUTES = 15;
export const DAY_MINUTES = 24 * 60;

/** The days a view shows, as local midnights. The month view pads to whole weeks. */
export function viewDays(view: View, date: Date): Date[] {
  let first: Date;
  let last: Date;
  if (view === 'day') first = last = startOfDay(date);
  else if (view === 'month') {
    first = startOfWeek(startOfMonth(date), WEEK);
    last = endOfWeek(endOfMonth(date), WEEK);
  } else {
    first = startOfWeek(date, WEEK);
    last = view === 'week' ? endOfWeek(date, WEEK) : addDays(first, 4);
  }
  const days: Date[] = [];
  for (let d = startOfDay(first); d <= last; d = addDays(d, 1)) days.push(d);
  return days;
}

export function stepDate(view: View, date: Date, dir: 1 | -1): Date {
  if (view === 'day') return addDays(date, dir);
  if (view === 'month') return addMonths(date, dir);
  return addWeeks(date, dir);
}

/** The query range covering `days`: the end is the start of the day after the last. */
export const rangeOf = (days: Date[]) => ({ from: days[0], to: addDays(days[days.length - 1], 1) });

interface Timed {
  start: Date;
  end: Date;
  allDay: boolean;
}

export { coversDay };

const minutesOf = (d: Date) => getHours(d) * 60 + getMinutes(d);

/** Minutes from midnight of `day` that a timed event occupies there (clipped), at least one snap step tall. */
export function dayMinutes(e: Timed, day: Date): { top: number; bottom: number } {
  const top = isSameDay(e.start, day) ? minutesOf(e.start) : 0;
  const bottom = isSameDay(e.end, day) ? minutesOf(e.end) : DAY_MINUTES;
  return { top, bottom: Math.max(bottom, Math.min(top + SNAP_MINUTES, DAY_MINUTES)) };
}

/**
 * Side-by-side columns for overlapping blocks: a cluster is a run of blocks
 * that chain-overlap, and every block of a cluster shares its column count.
 */
export function layoutColumns(blocks: { top: number; bottom: number }[]): { col: number; cols: number }[] {
  const out = blocks.map(() => ({ col: 0, cols: 1 }));
  const order = blocks.map((_, i) => i).sort((a, b) => blocks[a].top - blocks[b].top || blocks[b].bottom - blocks[a].bottom);

  let cluster: number[] = [];
  let colEnds: number[] = [];
  let clusterEnd = 0;
  const flush = () => {
    for (const i of cluster) out[i].cols = colEnds.length;
    cluster = [];
    colEnds = [];
  };

  for (const i of order) {
    if (cluster.length > 0 && blocks[i].top >= clusterEnd) flush();
    let col = colEnds.findIndex((end) => end <= blocks[i].top);
    if (col < 0) col = colEnds.length;
    colEnds[col] = blocks[i].bottom;
    out[i].col = col;
    cluster.push(i);
    clusterEnd = cluster.length === 1 ? blocks[i].bottom : Math.max(clusterEnd, blocks[i].bottom);
  }
  flush();
  return out;
}

export const snap = (minutes: number) => Math.round(minutes / SNAP_MINUTES) * SNAP_MINUTES;

/** Moving keeps the duration; `days` is whole local days and `minutes` is already snapped. */
export function moved(e: Timed, days: number, minutes: number): { start: Date; end: Date } {
  return { start: addMinutes(addDays(e.start, days), minutes), end: addMinutes(addDays(e.end, days), minutes) };
}

/** Resizing moves only the end, and never below one snap step. */
export function resized(e: Timed, minutes: number): { start: Date; end: Date } {
  const end = addMinutes(e.end, minutes);
  const min = addMinutes(e.start, SNAP_MINUTES);
  return { start: e.start, end: end < min ? min : end };
}

export const atMinute = (day: Date, minutes: number) => addMinutes(startOfDay(day), minutes);
