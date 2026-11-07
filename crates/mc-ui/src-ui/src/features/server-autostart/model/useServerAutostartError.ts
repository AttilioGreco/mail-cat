import { useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { toast } from 'sonner';

const show = (message: string) =>
  toast.error('Il server di sincronizzazione non è partito', {
    id: 'server-autostart',
    description: message,
    duration: Infinity,
  });

const showMismatch = (message: string) =>
  toast.warning('Il server non è aggiornato', {
    id: 'server-version',
    description: message,
    duration: Infinity,
  });

/**
 * The app registers and starts its background server on launch. When that
 * fails, say so — otherwise the user just sees an empty, unreachable client.
 * Pulled once (the failure can precede the webview) and listened for.
 */
export function useServerAutostartError() {
  useEffect(() => {
    let cancelled = false;

    invoke<string | null>('take_server_service_error')
      .then((message) => {
        if (!cancelled && message) show(message);
      })
      .catch((e) => console.error('server autostart: failed to read error', e));

    invoke<string | null>('take_server_version_mismatch')
      .then((message) => {
        if (!cancelled && message) showMismatch(message);
      })
      .catch((e) => console.error('server version: failed to read mismatch', e));

    const unlisten = [
      listen<string>('server-service-error', (event) => show(event.payload)),
      listen<string>('server-version-mismatch', (event) => showMismatch(event.payload)),
    ];

    return () => {
      cancelled = true;
      unlisten.forEach((u) => u.then((off) => off()).catch(() => {}));
    };
  }, []);
}
