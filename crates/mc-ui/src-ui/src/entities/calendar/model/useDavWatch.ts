import { useEffect } from 'react';
import { useQueryClient, type Query } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';

interface DavChange {
  /** Empty when the change does not name an account: treat it as every account. */
  accountId: string;
  kind: 'contacts' | 'calendar';
}

// Keyed `[name, accountId, ...]`, so a change can be narrowed to one account.
const ACCOUNT_KEYED: Record<DavChange['kind'], string[]> = {
  calendar: ['calendars', 'events', 'event'],
  contacts: ['address-books', 'remote-contacts', 'remote-contact'],
};
// Not keyed by account: any change of the kind can affect them.
const ACCOUNT_FREE: Record<DavChange['kind'], string[]> = {
  calendar: [],
  contacts: ['harvested-contacts', 'contact-lookup'],
};

/**
 * Refreshes contacts and calendar queries when the server reports a sync (or a
 * local edit) changed them. Mounted once, above every screen, like the other
 * app-wide watchers: the agenda and the hover card live outside their sections.
 */
export function useDavChangesWatch() {
  const queryClient = useQueryClient();

  useEffect(() => {
    invoke('start_dav_watch').catch((e) => console.error('[dav] start_dav_watch failed:', e));

    // StrictMode unmounts before `listen` resolves: drop the late listener.
    let cancelled = false;
    let unlisten: (() => void) | null = null;
    listen<DavChange>('dav:changed', ({ payload }) => {
      const { accountId, kind } = payload;
      const matches = ({ queryKey }: Query) =>
        ACCOUNT_FREE[kind].includes(queryKey[0] as string) ||
        (ACCOUNT_KEYED[kind].includes(queryKey[0] as string) && (!accountId || queryKey[1] === accountId));
      queryClient.invalidateQueries({ predicate: matches });
    }).then((fn) => {
      if (cancelled) fn();
      else unlisten = fn;
    });

    return () => {
      cancelled = true;
      invoke('stop_dav_watch').catch(() => {});
      unlisten?.();
    };
  }, [queryClient]);
}
