import { useMutation, useQueries, useQuery, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type {
  Calendar,
  DateRange,
  EventChanges,
  EventDetail,
  EventInput,
  EventOccurrence,
} from './types';

interface OccurrenceDto extends Omit<EventOccurrence, 'start' | 'end'> {
  start: string;
  end: string;
}

interface EventDetailDto extends Omit<EventDetail, 'first'> {
  first: OccurrenceDto | null;
}

const occurrenceFromDto = (d: OccurrenceDto): EventOccurrence => ({
  ...d,
  start: new Date(d.start),
  end: new Date(d.end),
});

/** The account's calendars, one result per account so a failing one never hides the others. */
export function useCalendars(accountIds: string[]) {
  return useQueries({
    queries: accountIds.map((accountId) => ({
      queryKey: ['calendars', accountId],
      queryFn: () => invoke<Calendar[]>('list_calendars', { accountId }),
      staleTime: 60_000,
    })),
    combine: (results) =>
      results.map((r, i) => ({
        accountId: accountIds[i],
        calendars: r.data ?? [],
        isLoading: r.isLoading,
        error: r.error,
      })),
  });
}

/**
 * Occurrences overlapping `range`, fanned out per account in parallel with
 * each account's own loading and error state.
 */
export function useEvents(accountIds: string[], range: DateRange) {
  const from = range.from.toISOString();
  const to = range.to.toISOString();
  return useQueries({
    queries: accountIds.map((accountId) => ({
      queryKey: ['events', accountId, from, to],
      queryFn: async () =>
        (await invoke<OccurrenceDto[]>('list_events', { accountId, from, to })).map(occurrenceFromDto),
      staleTime: 30_000,
    })),
    combine: (results) =>
      results.map((r, i) => ({
        accountId: accountIds[i],
        events: r.data ?? [],
        isLoading: r.isLoading,
        error: r.error,
      })),
  });
}

export function useEvent(accountId: string | null, href: string | null) {
  return useQuery({
    queryKey: ['event', accountId, href],
    enabled: accountId !== null && href !== null,
    queryFn: async (): Promise<EventDetail> => {
      const d = await invoke<EventDetailDto>('get_event', { accountId, href });
      return { ...d, first: d.first && occurrenceFromDto(d.first) };
    },
  });
}

/** Local edits show up at once; the server's `dav:changed` follows once they are pushed. */
function useInvalidateEvents() {
  const queryClient = useQueryClient();
  return (accountId: string) => {
    queryClient.invalidateQueries({ queryKey: ['events', accountId] });
    queryClient.invalidateQueries({ queryKey: ['event', accountId] });
  };
}

/** Resolves to the new event's href. */
export function useCreateEvent() {
  const invalidate = useInvalidateEvents();
  return useMutation({
    mutationFn: ({ accountId, event }: { accountId: string; event: EventInput }) =>
      invoke<string>('create_event', { accountId, event }),
    onSuccess: (_, { accountId }) => invalidate(accountId),
  });
}

export function useUpdateEvent() {
  const invalidate = useInvalidateEvents();
  return useMutation({
    mutationFn: ({ accountId, href, changes }: { accountId: string; href: string; changes: EventChanges }) =>
      invoke<void>('update_event', { accountId, href, changes }),
    onSuccess: (_, { accountId }) => invalidate(accountId),
  });
}

export function useDeleteEvent() {
  const invalidate = useInvalidateEvents();
  return useMutation({
    mutationFn: ({ accountId, href }: { accountId: string; href: string }) =>
      invoke<void>('delete_event', { accountId, href }),
    onSuccess: (_, { accountId }) => invalidate(accountId),
  });
}

/** The calendars appear with the sync pass the server starts, announced by `dav:changed`. */
export function useSubscribeCalendar() {
  return useMutation({
    mutationFn: ({ accountId, email }: { accountId: string; email: string }) =>
      invoke<void>('subscribe_calendar', { accountId, email }),
  });
}

export function useUnsubscribeCalendar() {
  return useMutation({
    mutationFn: ({ accountId, collection }: { accountId: string; collection: string }) =>
      invoke<void>('unsubscribe_calendar', { accountId, collection }),
  });
}
