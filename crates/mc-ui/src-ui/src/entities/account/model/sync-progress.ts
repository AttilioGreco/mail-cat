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

/** The last body batch that actually landed, for the "+N · 12s fa" line. */
export interface BodyBatch {
  added: number;
  at: number;
}

interface SyncState {
  /** Latest progress event per account. */
  byAccount: Record<string, SyncProgressPayload>;
  lastBodyBatch: Record<string, BodyBatch>;
  record: (event: SyncProgressPayload) => void;
}

export const useSyncStore = create<SyncState>((set) => ({
  byAccount: {},
  lastBodyBatch: {},
  record: (event) =>
    set((s) => {
      const prev = s.byAccount[event.account_id];
      // `processed` restarts from 0 with every drain pass (it is measured against
      // that pass's backlog). With no previous event we can't tell a batch from
      // the pass so far, so the first event after opening the app counts nothing.
      const sameRun = event.phase === 'fetching_bodies' && prev?.phase === 'fetching_bodies';
      const added = !sameRun ? 0 : event.total === prev.total ? event.processed - prev.processed : event.processed;
      return {
        byAccount: { ...s.byAccount, [event.account_id]: event },
        lastBodyBatch:
          added > 0
            ? { ...s.lastBodyBatch, [event.account_id]: { added, at: Date.now() } }
            : s.lastBodyBatch,
      };
    }),
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
      // Envelope events are gated to one per percentage point and body events to
      // one per batch (~1/min), so refreshing the counters on each is cheap and
      // is what makes the bars in Settings move live.
      if (event.payload.phase !== 'discovering_mailboxes') {
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
