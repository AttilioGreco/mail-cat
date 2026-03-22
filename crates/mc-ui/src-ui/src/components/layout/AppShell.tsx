import { Sidebar } from './Sidebar';
import { MessageList } from './MessageList';
import { MessagePane } from './MessagePane';

/**
 * Root layout: 3-column email shell.
 *   [Sidebar 220px] | [MessageList 320px] | [MessagePane flex-1]
 */
export function AppShell() {
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
    </div>
  );
}
