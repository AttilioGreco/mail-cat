export type CalendarOwnership = 'own' | 'shared' | 'subscription';

export interface Calendar {
  /** Collection path: unique within an account, not across accounts. */
  collection: string;
  name: string;
  color: string | null;
  timezone: string | null;
  readOnly: boolean;
  ownership: CalendarOwnership;
  holdsEvents: boolean;
}

/**
 * One occurrence of an event. A timed event's `start`/`end` are UTC instants;
 * an all-day event's are UTC midnight of its dates with `end` exclusive, so
 * read their UTC date parts and never convert them to the local zone.
 */
export interface EventOccurrence {
  calendar: string;
  href: string;
  summary: string;
  start: Date;
  end: Date;
  allDay: boolean;
  recurring: boolean;
  location: string | null;
  status: string | null;
  description: string | null;
  /** A local edit the server has not acknowledged. */
  dirty: boolean;
  /** Why the server refused the last push. */
  pushFailed: string | null;
}

export interface EventDetail {
  calendar: string;
  href: string;
  raw: string;
  /** Null for a resource without an event (a task). */
  first: EventOccurrence | null;
  /** Seconds relative to the start, iCalendar TRIGGER sign: negative is before. */
  reminders: number[];
}

/** A time is an RFC 3339 instant, or `YYYY-MM-DD` for an all-day event. */
export interface EventInput {
  calendar: string;
  summary: string;
  start: string;
  end: string;
  location?: string;
  description?: string;
  reminders: number[];
}

/** Absent fields are left as they are; an empty location or description clears it. */
export interface EventChanges {
  summary?: string;
  location?: string;
  start?: string;
  end?: string;
  description?: string;
  /** Replaces the reminders; absent leaves them alone. */
  reminders?: number[];
}

export interface DateRange {
  from: Date;
  to: Date;
}
