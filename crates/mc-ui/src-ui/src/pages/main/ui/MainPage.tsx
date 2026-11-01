import { AccountRail } from '@/widgets/account-rail';
import { FolderBar } from '@/widgets/folder-bar';
import { MessageList } from '@/widgets/message-list';
import { MessagePane } from '@/widgets/message-pane';
import { ComposeWindow } from '@/features/compose-message';
import { LabelDialogs } from '@/features/manage-labels';
import { useDeepLink } from '@/features/open-deep-link';
import { useResizableWidth } from '@/shared/lib/useResizableWidth';
import { ResizeHandle } from '@/shared/ui/resize-handle';

/**
 * Root layout: 4-column email shell, the folder bar and list resizable.
 *   [AccountRail 56px] | [FolderBar 176px] | [MessageList 320px] | [MessagePane flex-1]
 */
export function MainPage() {
  // Notification click → jump to that message.
  useDeepLink();

  const sidebar = useResizableWidth('mc.sidebarWidth', 176, 160, 400);
  const list = useResizableWidth('mc.messageListWidth', 320, 240, 720);

  return (
    <div className="flex h-full overflow-hidden bg-background">
      <AccountRail />
      <div className="shrink-0 overflow-hidden" style={{ width: sidebar.width }}>
        <FolderBar />
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
      <LabelDialogs />
    </div>
  );
}
