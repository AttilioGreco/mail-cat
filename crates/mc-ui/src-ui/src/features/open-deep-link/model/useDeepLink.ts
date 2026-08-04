import { useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { useSessionStore } from '@/entities/session';

interface DeepLink {
  accountId: string;
  mailboxId: string;
  messageId: string;
}

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
      openMessageAt(link.accountId, link.mailboxId, link.messageId);
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
