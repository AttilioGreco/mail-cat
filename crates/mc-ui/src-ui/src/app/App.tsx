import { HotkeysProvider } from '@tanstack/react-hotkeys';
import { MainPage } from '@/pages/main';
import { CalendarPage, useCalendarView } from '@/pages/calendar';
import { ContactsPage } from '@/pages/contacts';
import { LabelsPage } from '@/pages/labels';
import { SettingsPage } from '@/pages/settings';
import { useSettingsStore } from '@/features/manage-settings';
import { useShortcuts, useShortcutsStore, ShortcutsOverlay } from '@/features/keyboard-shortcuts';
import { useAppUpdate } from '@/features/app-update';
import { useServerAutostartError } from '@/features/server-autostart';
import { useLabelsPage } from '@/features/manage-labels';
import { useComposeStore } from '@/features/compose-message';
import { useSyncNow } from '@/features/manage-accounts';
import { useSectionStore, useSessionStore } from '@/entities/session';
import { useAuthStatusWatch, useSyncProgress } from '@/entities/account';
import { useAgendaPane, useDavChangesWatch } from '@/entities/calendar';
import { Toaster } from '@/shared/ui/sonner';
import { QueryProvider } from './model/QueryProvider';
import { ThemeProvider } from './model/ThemeProvider';

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
 * Settings and labels are full pages that replace the main layout, not overlays.
 * ponytail: two flags instead of a router; add one if a fourth screen or
 * deep-linkable URLs show up.
 */
function Router() {
  const { settingsOpen, openSettings, closeSettings } = useSettingsStore();
  const { labelsOpen, closeLabels } = useLabelsPage();
  const openCompose = useComposeStore((s) => s.openCompose);
  const selectedAccountId = useSessionStore((s) => s.selectedAccountId);
  const section = useSectionStore((s) => s.section);
  const setSection = useSectionStore((s) => s.setSection);
  const toggleAgenda = useAgendaPane((s) => s.toggle);
  const syncNow = useSyncNow();
  const toggleHelp = useShortcutsStore((s) => s.toggleHelp);
  // Mounted above both screens: switching to settings must not drop the
  // subscription, or the page would open with no progress to show.
  useSyncProgress();
  useAuthStatusWatch();
  useDavChangesWatch();
  useAppUpdate();
  useServerAutostartError();

  // Only the shortcuts that need no list context — the rest are bound where
  // their data lives (message list, message viewer).
  useShortcuts({
    compose: () => openCompose({ mode: 'new', accountId: selectedAccountId }),
    openSettings,
    syncNow: () => {
      if (selectedAccountId) syncNow.mutate(selectedAccountId);
    },
    showShortcuts: toggleHelp,
    toggleAgenda: section === 'mail' ? toggleAgenda : undefined,
    goMail: () => setSection('mail'),
    goCalendar: () => setSection('calendar'),
    goContacts: () => setSection('contacts'),
  });
  const openCalendarDay = (day: Date) => {
    const view = useCalendarView.getState();
    view.setDate(day);
    view.setView('day');
    setSection('calendar');
  };
  return (
    <>
      {settingsOpen ? (
        <SettingsPage onClose={closeSettings} />
      ) : labelsOpen ? (
        <LabelsPage onClose={closeLabels} />
      ) : (
        <MainPage calendar={<CalendarPage />} contacts={<ContactsPage />} openCalendarDay={openCalendarDay} />
      )}
      {/* Above both screens: the list of what's bound must be reachable anywhere. */}
      <ShortcutsOverlay />
      <Toaster position="bottom-right" />
    </>
  );
}
