import { useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { usePendingEvent } from '@/entities/calendar';
import { useSectionStore, useSessionStore } from '@/entities/session';

type DeepLink =
  | { kind: 'message'; accountId: string; mailboxId: string; messageId: string }
  // `start` is the occurrence's Unix time in seconds.
  | { kind: 'event'; accountId: string; calendar: string; href: string; start: number };

/**
 * Navigates to the message a desktop notification was clicked for.
 *
 * Two delivery paths, because the webview only exists for one of them:
 *  - cold start: the link is parsed from argv before the window exists, so we
 *    pull it once on mount;
 *  - already running: the single-instance plugin forwards the second launch's
 *    argv and emits "deep-link", which we listen for.
 */
export function useDeepLink() {
  const openMessageAt = useSessionStore((s) => s.openMessageAt);

  useEffect(() => {
    let cancelled = false;

    const apply = (link: DeepLink | null) => {
      if (cancelled || !link) return;
      if (link.kind === 'message') {
        useSectionStore.getState().setSection('mail');
        openMessageAt(link.accountId, link.mailboxId, link.messageId);
        return;
      }
      const session = useSessionStore.getState();
      if (session.selectedAccountId !== link.accountId) session.selectAccount(link.accountId);
      usePendingEvent.getState().set({ accountId: link.accountId, href: link.href, start: link.start });
      useSectionStore.getState().setSection('calendar');
    };

    invoke<DeepLink | null>('take_pending_deep_link')
      .then(apply)
      .catch((e) => console.error('deep link: failed to read startup link', e));

    const unlisten = listen<DeepLink>('deep-link', (event) => apply(event.payload));

    return () => {
      cancelled = true;
      unlisten.then((off) => off()).catch(() => {});
    };
  }, [openMessageAt]);
}
