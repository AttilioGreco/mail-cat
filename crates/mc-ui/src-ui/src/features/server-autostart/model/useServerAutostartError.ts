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

    const unlisten = listen<string>('server-service-error', (event) => show(event.payload));

    return () => {
      cancelled = true;
      unlisten.then((off) => off()).catch(() => {});
    };
  }, []);
}
