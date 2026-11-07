import { useCalendars } from './useCalendar';
import type { Calendar } from './types';

/** The slice of an account this needs; structural so the calendar entity does not import the account one. */
export interface CalendarAccount {
  id: string;
  name: string;
  email: string;
  auth: { calendar: FeatureStatus } | null;
}

interface FeatureStatus {
  state: 'enabled' | 'missing_permission' | 'unavailable';
  missingScopes: string[];
}

export interface CalendarGroup {
  account: CalendarAccount;
  feature: FeatureStatus;
  /** Only the calendars that hold events; empty unless the feature is enabled. */
  calendars: Calendar[];
  isLoading: boolean;
  error: unknown;
}

/**
 * Every account whose calendar feature is usable or fixable. An account with
 * no auth gate is tried anyway, and one whose calendar is unavailable is left out.
 */
export function useCalendarGroups(accounts: CalendarAccount[]): CalendarGroup[] {
  const listed = accounts
    .map((account) => ({ account, feature: account.auth?.calendar ?? { state: 'enabled' as const, missingScopes: [] } }))
    .filter((g) => g.feature.state !== 'unavailable');
  const enabledIds = listed.filter((g) => g.feature.state === 'enabled').map((g) => g.account.id);
  const results = useCalendars(enabledIds);

  return listed.map((g) => {
    const r = results.find((x) => x.accountId === g.account.id);
    return {
      ...g,
      calendars: r?.calendars.filter((c) => c.holdsEvents) ?? [],
      isLoading: r?.isLoading ?? false,
      error: r?.error ?? null,
    };
  });
}
