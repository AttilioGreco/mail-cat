import { useEffect, useRef, useState } from 'react';
import { format, isSameDay } from 'date-fns';
import { cn } from '@/shared/lib/utils';
import { isEditable, type AccountEvent } from '@/entities/calendar';
import { LOCALE, atMinute, coversDay, dayMinutes, layoutColumns, snap } from '../lib/time';
import { useGridGestures } from '../model/useGridGestures';
import { EventChip, SyncMarker, eventStyle, eventTitle, timeRange } from './EventBlock';

const HOUR = 48;
const PX_PER_MINUTE = HOUR / 60;
const HOURS = Array.from({ length: 24 }, (_, h) => h);
const GUTTER = 'w-12 shrink-0';

interface Props {
  days: Date[];
  events: AccountEvent[];
  onCreate: (start: Date, end: Date, allDay: boolean) => void;
  onOpen: (event: AccountEvent) => void;
  onReschedule: (event: AccountEvent, start: Date, end: Date) => void;
}

export function TimeGrid({ days, events, onCreate, onOpen, onReschedule }: Props) {
  const columns = useRef<HTMLDivElement>(null);
  const scroller = useRef<HTMLDivElement>(null);
  const now = useNow();
  const { preview, startCreate, startMove, startResize } = useGridGestures({
    days,
    columns,
    pxPerMinute: PX_PER_MINUTE,
    onCreate: (start, end) => onCreate(start, end, false),
    onOpen,
    onReschedule,
  });

  // Working hours are what people look at first; the night is a scroll away.
  useEffect(() => {
    if (scroller.current) scroller.current.scrollTop = 7 * HOUR;
  }, []);

  const shown = events.map((e) => (preview?.kind === 'event' && preview.event === e ? { ...e, start: preview.start, end: preview.end } : e));
  const allDay = shown.filter((e) => e.allDay);
  const timed = shown.filter((e) => !e.allDay);

  return (
    <div ref={scroller} className="min-h-0 flex-1 overflow-y-auto [scrollbar-gutter:stable]">
      <div className="sticky top-0 z-20 border-b border-border bg-background">
        <div className="flex">
          <div className={GUTTER} />
          {days.map((day) => (
            <div key={+day} className="flex-1 border-l border-border-subtle py-1.5 text-center text-xs">
              <span className={cn('rounded-full px-2 py-0.5', isSameDay(day, now) ? 'bg-accent font-semibold text-accent-foreground' : 'text-muted-foreground')}>
                {format(day, 'EEE d', { locale: LOCALE })}
              </span>
            </div>
          ))}
        </div>
        <div className="flex border-t border-border-subtle">
          <div className={cn(GUTTER, 'px-1 py-1 text-right text-[10px] leading-4 text-muted-foreground')}>tutto il giorno</div>
          {days.map((day) => (
            <div
              key={+day}
              className="flex min-h-6 flex-1 flex-col gap-px border-l border-border-subtle p-px"
              onDoubleClick={() => onCreate(day, day, true)}
            >
              {allDay.filter((e) => coversDay(e, day)).map((e) => (
                <EventChip key={`${e.accountId}|${e.href}|${e.calendar}`} event={e} onOpen={onOpen} />
              ))}
            </div>
          ))}
        </div>
      </div>

      <div className="flex">
        <div className={cn(GUTTER, 'relative')} style={{ height: 24 * HOUR }}>
          {HOURS.slice(1).map((h) => (
            <span key={h} className="absolute right-1 -translate-y-1/2 text-[10px] text-muted-foreground" style={{ top: h * HOUR }}>
              {String(h).padStart(2, '0')}:00
            </span>
          ))}
        </div>
        <div ref={columns} className="grid flex-1" style={{ gridTemplateColumns: `repeat(${days.length}, minmax(0, 1fr))` }}>
          {days.map((day, i) => (
            <DayColumn
              key={+day}
              day={day}
              now={now}
              events={timed.filter((e) => coversDay(e, day))}
              ghost={preview?.kind === 'create' && preview.day === i ? preview : null}
              onPointerDown={(e) => e.target === e.currentTarget && startCreate(e)}
              onDoubleClick={(e) => {
                if (e.target !== e.currentTarget) return;
                const y = e.clientY - e.currentTarget.getBoundingClientRect().top;
                const start = atMinute(day, Math.floor(y / PX_PER_MINUTE / 15) * 15);
                onCreate(start, atMinute(day, snap(start.getHours() * 60 + start.getMinutes() + 60)), false);
              }}
              onOpen={onOpen}
              startMove={startMove}
              startResize={startResize}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

interface ColumnProps {
  day: Date;
  now: Date;
  events: AccountEvent[];
  ghost: { top: number; bottom: number } | null;
  onPointerDown: React.PointerEventHandler<HTMLDivElement>;
  onDoubleClick: React.MouseEventHandler<HTMLDivElement>;
  onOpen: (event: AccountEvent) => void;
  startMove: (e: React.PointerEvent, event: AccountEvent) => void;
  startResize: (e: React.PointerEvent, event: AccountEvent) => void;
}

function DayColumn({ day, now, events, ghost, onPointerDown, onDoubleClick, onOpen, startMove, startResize }: ColumnProps) {
  const blocks = events.map((e) => dayMinutes(e, day));
  const placed = layoutColumns(blocks);
  return (
    <div
      className="relative touch-none border-l border-border-subtle"
      style={{
        height: 24 * HOUR,
        backgroundImage: 'linear-gradient(to bottom, hsl(var(--border-subtle)) 1px, transparent 1px)',
        backgroundSize: `100% ${HOUR}px`,
      }}
      onPointerDown={onPointerDown}
      onDoubleClick={onDoubleClick}
    >
      {events.map((e, i) => {
        const { top, bottom } = blocks[i];
        const { col, cols } = placed[i];
        const editable = isEditable(e);
        return (
          <div
            key={`${e.accountId}|${e.href}|${e.calendar}|${+e.start}`}
            role="button"
            tabIndex={0}
            title={eventTitle(e)}
            style={{
              ...eventStyle(e.color),
              top: top * PX_PER_MINUTE,
              height: Math.max((bottom - top) * PX_PER_MINUTE - 1, 12),
              left: `calc(${(col / cols) * 100}% + 1px)`,
              width: `calc(${100 / cols}% - 3px)`,
            }}
            className={cn(
              'absolute overflow-hidden rounded px-1.5 py-0.5 text-[11px] leading-[14px] hover:brightness-95 dark:hover:brightness-125',
              editable ? 'cursor-grab active:cursor-grabbing' : 'cursor-pointer',
            )}
            onPointerDown={(ev) => (editable ? startMove(ev, e) : ev.stopPropagation())}
            onClick={() => !editable && onOpen(e)}
            onDoubleClick={(ev) => ev.stopPropagation()}
            onKeyDown={(ev) => ev.key === 'Enter' && onOpen(e)}
          >
            <div className="flex items-center gap-1 font-medium">
              <span className="truncate">{e.summary || 'Senza titolo'}</span>
              <SyncMarker event={e} />
            </div>
            {bottom - top >= 45 && <div className="truncate text-muted-foreground">{timeRange(e)}</div>}
            {editable && (
              <div className="absolute inset-x-0 bottom-0 h-1.5 cursor-ns-resize" onPointerDown={(ev) => startResize(ev, e)} />
            )}
          </div>
        );
      })}
      {ghost && (
        <div
          className="pointer-events-none absolute inset-x-0.5 rounded bg-accent/30 ring-1 ring-accent"
          style={{ top: ghost.top * PX_PER_MINUTE, height: (ghost.bottom - ghost.top) * PX_PER_MINUTE }}
        />
      )}
      {isSameDay(day, now) && (
        <div className="pointer-events-none absolute inset-x-0 z-10 h-px bg-destructive" style={{ top: (now.getHours() * 60 + now.getMinutes()) * PX_PER_MINUTE }}>
          <span className="absolute -left-1 -top-[3px] h-[7px] w-[7px] rounded-full bg-destructive" />
        </div>
      )}
    </div>
  );
}

function useNow(): Date {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 60_000);
    return () => clearInterval(id);
  }, []);
  return now;
}
