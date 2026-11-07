/**
 * An all-day event's `start`/`end` are UTC midnight of its dates. Reading the
 * local parts would shift the date a day west of UTC, so rebuild the same
 * calendar date at local midnight instead.
 */
export const utcDay = (d: Date): Date => new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
