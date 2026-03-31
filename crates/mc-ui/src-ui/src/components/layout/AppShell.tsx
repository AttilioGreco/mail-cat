import { Sidebar } from './Sidebar';
import { MessageList } from './MessageList';
import { MessagePane } from './MessagePane';
import { ComposeWindow } from '@/components/compose/ComposeWindow';
import { LocalSettingsPanel } from '@/components/settings/LocalSettingsPanel';
import { useUIStore } from '@/store/uiStore';

/**
 * Root layout: 3-column email shell.
 *   [Sidebar 220px] | [MessageList 320px] | [MessagePane flex-1]
 */
export function AppShell() {
  const { composeOpen, composeData, closeCompose, settingsOpen, closeSettings } = useUIStore();

  return (
    <div className="flex h-full overflow-hidden bg-background">
      <Sidebar />
      <div className="flex flex-1 overflow-hidden">
        <div className="w-[320px] shrink-0 border-r border-border overflow-hidden">
          <MessageList />
        </div>
        <main className="flex-1 overflow-hidden">
          <MessagePane />
        </main>
      </div>

      <ComposeWindow
        open={composeOpen}
        onClose={closeCompose}
        initialData={composeData}
      />

      <LocalSettingsPanel open={settingsOpen} onClose={closeSettings} />
    </div>
  );
}
