import { useEffect } from 'react';
import { listen } from '@tauri-apps/api/event';
import { invoke } from '@tauri-apps/api/core';
import { useQueryClient } from '@tanstack/react-query';
import { create } from 'zustand';

export type SyncPhase =
  | 'discovering_mailboxes'
  | 'fetching_envelopes'
  | 'fetching_bodies'
  | 'indexing'
  | 'complete'
  | 'error';

export interface SyncProgressPayload {
  account_id: string;
  phase: SyncPhase;
  processed: number;
  total: number | null;
  message: string | null;
}

interface SyncState {
  /** Latest progress event per account. */
  byAccount: Record<string, SyncProgressPayload>;
  record: (event: SyncProgressPayload) => void;
}

export const useSyncStore = create<SyncState>((set) => ({
  byAccount: {},
  record: (event) =>
    set((s) => ({ byAccount: { ...s.byAccount, [event.account_id]: event } })),
}));

/**
 * Subscribe to server-side sync progress for every account.
 *
 * The server debounces to one event per percentage point, so this can be fed
 * straight into state — there is no 10k-event flood to throttle here. Most syncs
 * are server-initiated (boot, the 5-minute tick, the body backfill worker), which
 * is why we watch instead of triggering.
 */
export function useSyncProgress() {
  const record = useSyncStore((s) => s.record);
  const queryClient = useQueryClient();

  useEffect(() => {
    invoke('start_sync_watch').catch((e) =>
      console.error('[sync-status] start_sync_watch failed:', e),
    );

    let unlisten: (() => void) | null = null;
    listen<SyncProgressPayload>('sync:progress', (event) => {
      record(event.payload);
      // Body downloads move the stored counts too, and the worker emits at most
      // one event per percentage point (~1/min), so refreshing on them is cheap
      // and keeps the counters honest instead of only after a full sync.
      if (event.payload.phase === 'complete' || event.payload.phase === 'fetching_bodies') {
        queryClient.invalidateQueries({ queryKey: ['account-stats', event.payload.account_id] });
      }
      if (event.payload.phase === 'complete') {
        queryClient.invalidateQueries({ queryKey: ['mailboxes', event.payload.account_id] });
      }
    }).then((fn) => {
      unlisten = fn;
    });

    return () => {
      invoke('stop_sync_watch').catch(() => {});
      unlisten?.();
    };
  }, [record, queryClient]);
}

/** Percentage of `processed / total`, or null when the phase carries no total. */
export function progressPercent(event: SyncProgressPayload | undefined): number | null {
  if (!event || !event.total) return null;
  return Math.min(100, Math.round((event.processed / event.total) * 100));
}
