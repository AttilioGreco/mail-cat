import { useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { check, type Update } from '@tauri-apps/plugin-updater';
import { relaunch } from '@tauri-apps/plugin-process';
import { openUrl } from '@tauri-apps/plugin-opener';
import { toast } from 'sonner';

const CHECK_EVERY_MS = 6 * 60 * 60 * 1000;
const TOAST_ID = 'app-update';
const RELEASES_URL = 'https://github.com/AttilioGreco/mail-cat/releases/latest';

async function install(update: Update) {
  let total = 0;
  let received = 0;
  toast.loading('Download aggiornamento…', { id: TOAST_ID, duration: Infinity });
  try {
    await update.download((event) => {
      if (event.event === 'Started') total = event.data.contentLength ?? 0;
      if (event.event === 'Progress') {
        received += event.data.chunkLength;
        const pct = total ? ` ${Math.round((received / total) * 100)}%` : '';
        toast.loading(`Download aggiornamento…${pct}`, { id: TOAST_ID, duration: Infinity });
      }
    });
  } catch (e) {
    return fail(e);
  }
  toast.loading('Installazione…', { id: TOAST_ID, duration: Infinity });
  // The server's executable is locked on Windows and would be replaced under a
  // running process elsewhere; the relaunched app starts it from the new files.
  await invoke('stop_server_service').catch((e) => console.error('update: stop server', e));
  try {
    await update.install();
    await relaunch();
  } catch (e) {
    invoke('ensure_server_service').catch(() => {});
    fail(e);
  }
}

function fail(e: unknown) {
  console.error('update failed', e);
  toast.error('Aggiornamento non riuscito', {
    id: TOAST_ID,
    duration: Infinity,
    action: { label: 'Scarica la nuova versione', onClick: () => void openUrl(RELEASES_URL) },
  });
}

/**
 * Checks GitHub for a newer release at startup and every 6 h. Never installs
 * silently: the user confirms from a non-modal toast.
 */
export function useAppUpdate() {
  useEffect(() => {
    let announced: string | null = null;

    const run = async () => {
      try {
        const update = await check();
        if (!update || update.version === announced) return;
        announced = update.version;
        toast(`Versione ${update.version} disponibile`, {
          id: TOAST_ID,
          duration: Infinity,
          action: { label: 'Aggiorna', onClick: () => void install(update) },
        });
      } catch (e) {
        // Offline, or no release published yet: not worth bothering the user.
        console.warn('update check failed', e);
      }
    };

    void run();
    const timer = setInterval(run, CHECK_EVERY_MS);
    return () => clearInterval(timer);
  }, []);
}
