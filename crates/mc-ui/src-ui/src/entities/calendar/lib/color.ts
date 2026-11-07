import type { Calendar } from '../model/types';

/** Collections are only unique within an account. */
export const calendarKey = (accountId: string, collection: string) => `${accountId}::${collection}`;

/** The server's color, or a stable one derived from the id so a calendar never changes color between runs. */
export function calendarColor(accountId: string, calendar: Pick<Calendar, 'collection' | 'color'>): string {
  if (calendar.color) return calendar.color;
  let h = 0;
  for (const c of calendarKey(accountId, calendar.collection)) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  return `hsl(${h % 360} 55% 50%)`;
}
