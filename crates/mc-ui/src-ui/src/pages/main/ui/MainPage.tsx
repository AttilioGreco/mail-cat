import { Sidebar } from '@/widgets/sidebar';
import { MessageList } from '@/widgets/message-list';
import { MessagePane } from '@/widgets/message-pane';
import { ComposeWindow } from '@/features/compose-message';
import { useDeepLink } from '@/features/open-deep-link';
import { useResizableWidth } from '@/shared/lib/useResizableWidth';
import { ResizeHandle } from '@/shared/ui/resize-handle';

/**
 * Root layout: 3-column email shell, the first two columns resizable.
 *   [Sidebar 220px] | [MessageList 320px] | [MessagePane flex-1]
 */
export function MainPage() {
  // Notification click → jump to that message.
  useDeepLink();

  const sidebar = useResizableWidth('mc.sidebarWidth', 220, 160, 400);
  const list = useResizableWidth('mc.messageListWidth', 320, 240, 720);

  return (
    <div className="flex h-full overflow-hidden bg-background">
      <div className="shrink-0 overflow-hidden" style={{ width: sidebar.width }}>
        <Sidebar />
      </div>
      <ResizeHandle
        width={sidebar.width}
        onResize={sidebar.resize}
        onCommit={sidebar.commit}
        onReset={sidebar.reset}
      />
      <div className="flex flex-1 overflow-hidden">
        <div className="shrink-0 overflow-hidden" style={{ width: list.width }}>
          <MessageList />
        </div>
        <ResizeHandle
          width={list.width}
          onResize={list.resize}
          onCommit={list.commit}
          onReset={list.reset}
        />
        <main className="min-w-0 flex-1 overflow-hidden">
          <MessagePane />
        </main>
      </div>

      <ComposeWindow />
    </div>
  );
}
