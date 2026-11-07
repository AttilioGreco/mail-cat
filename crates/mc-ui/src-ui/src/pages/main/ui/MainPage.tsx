import type { ReactNode } from 'react';
import { AccountRail } from '@/widgets/account-rail';
import { FolderBar } from '@/widgets/folder-bar';
import { MessageList } from '@/widgets/message-list';
import { MessagePane } from '@/widgets/message-pane';
import { AgendaPane } from '@/widgets/agenda-pane';
import { ComposeWindow } from '@/features/compose-message';
import { LabelDialogs } from '@/features/manage-labels';
import { EventDialog } from '@/features/edit-event';
import { ContactDialog } from '@/features/edit-contact';
import { useDeepLink } from '@/features/open-deep-link';
import { useAgendaPane } from '@/entities/calendar';
import { useSectionStore } from '@/entities/session';
import { useResizableWidth } from '@/shared/lib/useResizableWidth';
import { ResizeHandle } from '@/shared/ui/resize-handle';

/**
 * Root layout: 4-column email shell, the folder bar and list resizable. The other
 * sections replace everything right of the rail, which stays put.
 *   [AccountRail 56px] | [FolderBar 176px] | [MessageList 320px] | [MessagePane flex-1]
 */
interface MainPageProps {
  calendar: ReactNode;
  contacts: ReactNode;
  /** Opens the Calendar section on a day; the page that owns the calendar view is not ours to import. */
  openCalendarDay: (day: Date) => void;
}

export function MainPage({ calendar, contacts, openCalendarDay }: MainPageProps) {
  const section = useSectionStore((s) => s.section);
  // Notification click → jump to that message.
  useDeepLink();

  const sidebar = useResizableWidth('mc.sidebarWidth', 176, 160, 400);
  const list = useResizableWidth('mc.messageListWidth', 320, 240, 720);
  const agendaOpen = useAgendaPane((s) => s.open);
  const agenda = useResizableWidth('mc.agendaWidth', 288, 220, 480);

  return (
    <div className="flex h-full overflow-hidden bg-background">
      <AccountRail />
      {section === 'calendar' ? (
        calendar
      ) : section === 'contacts' ? (
        contacts
      ) : (
        <>
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
            {agendaOpen && (
              <>
                {/* The handle drags the pane on its left, so the width is negated: dragging left widens the agenda. */}
                <ResizeHandle
                  width={-agenda.width}
                  onResize={(w) => agenda.resize(-w)}
                  onCommit={(w) => agenda.commit(-w)}
                  onReset={agenda.reset}
                />
                <div className="flex shrink-0 overflow-hidden" style={{ width: agenda.width }}>
                  <AgendaPane onOpenDay={openCalendarDay} />
                </div>
              </>
            )}
          </div>
        </>
      )}

      <EventDialog />
      <ContactDialog />
      <ComposeWindow />
      <LabelDialogs />
    </div>
  );
}
