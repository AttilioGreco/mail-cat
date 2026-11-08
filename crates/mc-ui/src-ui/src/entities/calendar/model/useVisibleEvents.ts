import { calendarColor, calendarKey } from '../lib/color';
import { useCalendarVisibility } from './visibility-store';
import { useCalendarGroups, type CalendarAccount } from './useCalendarGroups';
import { useEvents } from './useCalendar';
import type { DateRange, EventOccurrence } from './types';

export interface AccountEvent extends EventOccurrence {
  accountId: string;
  color: string;
  readOnly: boolean;
}

/** Drag and resize are off for a read-only calendar and for a series, so a gesture never moves a whole series. */
export const isEditable = (e: AccountEvent) => !e.readOnly && !e.recurring;

/** Occurrences of the calendars the user left switched on, across accounts. */
export function useVisibleEvents(accounts: CalendarAccount[], range: DateRange) {
  const groups = useCalendarGroups(accounts);
  const hidden = useCalendarVisibility((s) => s.hidden);
  const results = useEvents(
    groups.filter((g) => g.feature.state === 'enabled').map((g) => g.account.id),
    range,
  );

  const events: AccountEvent[] = [];
  for (const { accountId, events: occurrences } of results) {
    const calendars = groups.find((g) => g.account.id === accountId)?.calendars ?? [];
    for (const o of occurrences) {
      const calendar = calendars.find((c) => c.collection === o.calendar);
      if (!calendar || hidden.includes(calendarKey(accountId, o.calendar))) continue;
      events.push({ ...o, accountId, color: calendarColor(accountId, calendar), readOnly: calendar.readOnly });
    }
  }
  return { events, loading: results.some((r) => r.isLoading), failed: results.filter((r) => r.error).map((r) => r.accountId) };
}
