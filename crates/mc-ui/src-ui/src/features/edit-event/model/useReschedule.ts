import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { useUpdateEvent, type AccountEvent, type EventOccurrence } from '@/entities/calendar';

/**
 * Move or resize an event so the grid follows the pointer at once; the cached
 * lists are put back if the server refuses, since the block would otherwise
 * show a time that was never saved.
 */
export function useReschedule() {
  const queryClient = useQueryClient();
  const { mutateAsync } = useUpdateEvent();

  return async (e: AccountEvent, start: Date, end: Date) => {
    const queryKey = ['events', e.accountId];
    await queryClient.cancelQueries({ queryKey });
    const before = queryClient.getQueriesData<EventOccurrence[]>({ queryKey });
    queryClient.setQueriesData<EventOccurrence[]>({ queryKey }, (old) =>
      old?.map((o) => (o.href === e.href && o.calendar === e.calendar ? { ...o, start, end, dirty: true } : o)),
    );
    try {
      await mutateAsync({
        accountId: e.accountId,
        href: e.href,
        changes: { start: start.toISOString(), end: end.toISOString() },
      });
    } catch (err) {
      for (const [key, data] of before) queryClient.setQueryData(key, data);
      toast.error(`Impossibile spostare l'evento: ${String(err)}`);
    }
  };
}
