import { useState, type PointerEvent as ReactPointerEvent, type RefObject } from 'react';
import type { AccountEvent } from '@/entities/calendar';
import { DAY_MINUTES, SNAP_MINUTES, atMinute, moved, resized, snap } from '../lib/time';

export type Preview =
  | { kind: 'create'; day: number; top: number; bottom: number }
  | { kind: 'event'; event: AccountEvent; start: Date; end: Date };

interface Point {
  day: number;
  minutes: number;
}

interface Options {
  days: Date[];
  /** The element spanning every day column, whose box maps pixels to days and minutes. */
  columns: RefObject<HTMLDivElement | null>;
  pxPerMinute: number;
  onCreate: (start: Date, end: Date) => void;
  onOpen: (event: AccountEvent) => void;
  onReschedule: (event: AccountEvent, start: Date, end: Date) => void;
}

const clamp = (n: number, lo: number, hi: number) => Math.min(Math.max(n, lo), hi);

/**
 * Pointer gestures on the time grid: drag on an empty slot to create, drag a
 * block to move it, drag its bottom edge to resize. Listeners live on the
 * window so the gesture survives leaving the column.
 */
export function useGridGestures({ days, columns, pxPerMinute, onCreate, onOpen, onReschedule }: Options) {
  const [preview, setPreview] = useState<Preview | null>(null);

  const locate = (x: number, y: number): Point => {
    const box = columns.current?.getBoundingClientRect();
    if (!box) return { day: 0, minutes: 0 };
    return {
      day: clamp(Math.floor((x - box.left) / (box.width / days.length)), 0, days.length - 1),
      minutes: clamp((y - box.top) / pxPerMinute, 0, DAY_MINUTES),
    };
  };

  // A drag starts after 4px, so a plain click on a block still opens it.
  const track = (e: ReactPointerEvent, move: (p: Point) => void, end: (p: Point, dragged: boolean) => void) => {
    if (e.button !== 0) return;
    const [x0, y0] = [e.clientX, e.clientY];
    let dragged = false;
    const onMove = (ev: PointerEvent) => {
      if (!dragged && Math.hypot(ev.clientX - x0, ev.clientY - y0) < 4) return;
      dragged = true;
      move(locate(ev.clientX, ev.clientY));
    };
    const stop = () => {
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
      window.removeEventListener('pointercancel', stop);
      setPreview(null);
    };
    const onUp = (ev: PointerEvent) => {
      stop();
      end(locate(ev.clientX, ev.clientY), dragged);
    };
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
    window.addEventListener('pointercancel', stop);
  };

  const startCreate = (e: ReactPointerEvent) => {
    const p0 = locate(e.clientX, e.clientY);
    const slot = (p: Point) => {
      const top = Math.floor(Math.min(p0.minutes, p.minutes) / SNAP_MINUTES) * SNAP_MINUTES;
      const bottom = Math.max(Math.ceil(Math.max(p0.minutes, p.minutes) / SNAP_MINUTES) * SNAP_MINUTES, top + SNAP_MINUTES);
      return { top, bottom: Math.min(bottom, DAY_MINUTES) };
    };
    track(
      e,
      (p) => setPreview({ kind: 'create', day: p0.day, ...slot(p) }),
      (p, dragged) => {
        if (!dragged) return;
        const { top, bottom } = slot(p);
        onCreate(atMinute(days[p0.day], top), atMinute(days[p0.day], bottom));
      },
    );
  };

  const startMove = (e: ReactPointerEvent, event: AccountEvent) => {
    e.stopPropagation();
    const p0 = locate(e.clientX, e.clientY);
    const shifted = (p: Point) => moved(event, p.day - p0.day, snap(p.minutes - p0.minutes));
    track(
      e,
      (p) => setPreview({ kind: 'event', event, ...shifted(p) }),
      (p, dragged) => {
        if (!dragged) return onOpen(event);
        const next = shifted(p);
        if (+next.start !== +event.start) onReschedule(event, next.start, next.end);
      },
    );
  };

  const startResize = (e: ReactPointerEvent, event: AccountEvent) => {
    e.stopPropagation();
    const p0 = locate(e.clientX, e.clientY);
    const stretched = (p: Point) => resized(event, snap(p.minutes - p0.minutes));
    track(
      e,
      (p) => setPreview({ kind: 'event', event, ...stretched(p) }),
      (p, dragged) => {
        const next = stretched(p);
        if (dragged && +next.end !== +event.end) onReschedule(event, next.start, next.end);
      },
    );
  };

  return { preview, startCreate, startMove, startResize };
}
