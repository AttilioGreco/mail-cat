import { HotkeysProvider } from '@tanstack/react-hotkeys';
import { MainPage } from '@/pages/main';
import { SettingsPage } from '@/pages/settings';
import { useSettingsStore } from '@/features/manage-settings';
import { useShortcuts, useShortcutsStore, ShortcutsOverlay } from '@/features/keyboard-shortcuts';
import { useComposeStore } from '@/features/compose-message';
import { useSyncNow } from '@/features/manage-accounts';
import { useSessionStore } from '@/entities/session';
import { useSyncProgress } from '@/entities/account';
import { Toaster } from '@/shared/ui/sonner';
import { QueryProvider } from './providers/QueryProvider';
import { ThemeProvider } from './providers/ThemeProvider';

export default function App() {
  return (
    <QueryProvider>
      <ThemeProvider>
        <HotkeysProvider>
          <Router />
        </HotkeysProvider>
      </ThemeProvider>
    </QueryProvider>
  );
}

/**
 * Two screens, one flag — settings is a full page, not an overlay.
 * ponytail: no router dependency for a single either/or; add one if a third
 * screen or deep-linkable URLs show up.
 */
function Router() {
  const { settingsOpen, openSettings, closeSettings } = useSettingsStore();
  const openCompose = useComposeStore((s) => s.openCompose);
  const selectedAccountId = useSessionStore((s) => s.selectedAccountId);
  const syncNow = useSyncNow();
  const toggleHelp = useShortcutsStore((s) => s.toggleHelp);
  // Mounted above both screens: switching to settings must not drop the
  // subscription, or the page would open with no progress to show.
  useSyncProgress();

  // Only the shortcuts that need no list context — the rest are bound where
  // their data lives (message list, message viewer).
  useShortcuts({
    compose: () => openCompose({ mode: 'new', accountId: selectedAccountId }),
    openSettings,
    syncNow: () => {
      if (selectedAccountId) syncNow.mutate(selectedAccountId);
    },
    showShortcuts: toggleHelp,
  });
  return (
    <>
      {settingsOpen ? <SettingsPage onClose={closeSettings} /> : <MainPage />}
      {/* Above both screens: the list of what's bound must be reachable anywhere. */}
      <ShortcutsOverlay />
      <Toaster position="bottom-right" />
    </>
  );
}
