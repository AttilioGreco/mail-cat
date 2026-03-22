import { FixedSizeList, type ListChildComponentProps } from 'react-window';
import AutoSizer from 'react-virtualized-auto-sizer';
import { RefreshCw, Inbox } from 'lucide-react';
import { useUIStore, mailboxKey } from '@/store/uiStore';
import { useMessages } from '@/hooks/useMessages';
import { useMailboxWatcher } from '@/hooks/useMailboxWatcher';
import { MessageRow } from '@/components/messages/MessageRow';
import type { MessageEnvelope } from '@/lib/types';

const ROW_HEIGHT = 76;

export function MessageList() {
  const { selectedAccountId, selectedMailbox, selectedMessageId, selectMessage } = useUIStore();
  const key = mailboxKey(selectedAccountId, selectedMailbox?.id ?? null);

  // Real-time IMAP IDLE push → auto-invalidates query cache on new/changed messages
  useMailboxWatcher(selectedMailbox?.id ?? null, key, selectedAccountId);
  const { data: messages = [], isLoading } = useMessages(key);

  if (!key) {
    return <EmptyState icon={<Inbox size={32} className="text-muted" />} text="Select a mailbox" />;
  }

  if (isLoading) {
    return (
      <EmptyState
        icon={<RefreshCw size={20} className="animate-spin text-muted" />}
        text="Loading messages…"
      />
    );
  }

  if (messages.length === 0) {
    return <EmptyState icon={<Inbox size={32} className="text-muted" />} text="No messages" />;
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b border-border px-4 py-2.5">
        <span className="text-sm font-semibold text-foreground">
          {selectedMailbox?.name ?? selectedMailbox?.path}
        </span>
        <span className="text-xs text-muted">{messages.length} messages</span>
      </div>

      <div className="flex-1 overflow-hidden">
        <AutoSizer>
          {({ height, width }) => (
            <FixedSizeList
              height={height}
              width={width}
              itemCount={messages.length}
              itemSize={ROW_HEIGHT}
              itemData={{ messages, selectedMessageId, selectMessage }}
            >
              {Row}
            </FixedSizeList>
          )}
        </AutoSizer>
      </div>
    </div>
  );
}

interface RowData {
  messages: MessageEnvelope[];
  selectedMessageId: string | null;
  selectMessage: (id: string | null) => void;
}

function Row({ index, style, data }: ListChildComponentProps<RowData>) {
  const { messages, selectedMessageId, selectMessage } = data;
  const message = messages[index];
  return (
    <MessageRow
      message={message}
      isSelected={selectedMessageId === message.uid}
      onSelect={() => selectMessage(message.uid)}
      style={style}
    />
  );
}

function EmptyState({ icon, text }: { icon: React.ReactNode; text: string }) {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
      {icon}
      <span className="text-sm">{text}</span>
    </div>
  );
}
