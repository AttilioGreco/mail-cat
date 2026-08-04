import { Sidebar } from '@/widgets/sidebar';
import { MessageList } from '@/widgets/message-list';
import { MessagePane } from '@/widgets/message-pane';
import { ComposeWindow, useComposeStore } from '@/features/compose-message';
import { useDeepLink } from '@/features/open-deep-link';

/**
 * Root layout: 3-column email shell.
 *   [Sidebar 220px] | [MessageList 320px] | [MessagePane flex-1]
 */
export function MainPage() {
  const { composeOpen, composeData, closeCompose } = useComposeStore();

  // Notification click → jump to that message.
  useDeepLink();

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
    </div>
  );
}
