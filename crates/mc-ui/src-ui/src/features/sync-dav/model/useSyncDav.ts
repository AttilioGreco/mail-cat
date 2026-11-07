import { useMutation } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';

/**
 * Wakes the server's contacts and calendars sync for every account. It returns
 * before the pass runs: progress shows in each account's auth status, and the
 * data refreshes through `dav:changed`.
 */
export function useSyncDav() {
  return useMutation({
    mutationFn: () => invoke('sync_dav_now', { accountId: null }),
  });
}
