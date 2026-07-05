import { List, type RowComponentProps } from 'react-window';
import { RefreshCw, Inbox, MailOpen, MessageSquare, List as ListIcon } from 'lucide-react';
import { useSessionStore, mailboxKey } from '@/entities/session';
import { useMessages, MessageRow, type MessageEnvelope } from '@/entities/message';
import { useMailboxes } from '@/entities/mailbox';
import { useConversations, ConversationRow, type ConversationSummary } from '@/entities/conversation';
import { useMailboxWatcher } from '@/features/watch-mailbox';
import { SearchBar, SearchResults, useSearchStore } from '@/features/search-messages';

const MESSAGE_ROW_HEIGHT = 76;
const CONV_ROW_HEIGHT = 64;

export function MessageList() {
  const {
    selectedAccountId, selectedMailboxId,
    selectedMessageId, selectMessage,
    selectedThreadMessageId, selectThread,
    conversationMode, setConversationMode,
    showUnreadOnly, toggleUnreadOnly,
  } = useSessionStore();
  const searchQuery = useSearchStore((s) => s.searchQuery);

  const { data: mailboxes = [] } = useMailboxes(selectedAccountId);
  const selectedMailbox = mailboxes.find((m) => m.id === selectedMailboxId) ?? null;

  const key = mailboxKey(selectedAccountId, selectedMailboxId);

  // Real-time IMAP IDLE push → auto-invalidates query cache on new/changed messages
  useMailboxWatcher(selectedMailboxId, key, selectedAccountId);

  const { data: rawMessages = [], isLoading: messagesLoading } = useMessages(conversationMode ? null : key);
  const { data: conversations = [], isLoading: convsLoading } = useConversations(
    conversationMode && selectedAccountId ? selectedAccountId : null,
    selectedMailboxId
  );

  const messages = showUnreadOnly ? rawMessages.filter((m) => !m.isRead) : rawMessages;
  const isLoading = conversationMode ? convsLoading : messagesLoading;

  const isSearching = searchQuery !== null && searchQuery.trim().length > 0;
  const unreadCount = conversationMode
    ? conversations.filter((c) => c.unreadCount > 0).length
    : rawMessages.filter((m) => !m.isRead).length;

  const hasContent = selectedAccountId || selectedMailbox;

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-border px-3 py-2">
        <SearchBar />
      </div>

      {/* Toolbar */}
      {hasContent && !isSearching && (
        <div className="flex items-center justify-between border-b border-border px-3 py-1.5 gap-2">
          <span className="text-sm font-semibold text-foreground truncate">
            {selectedMailbox?.name ?? selectedMailbox?.path ?? (conversationMode ? 'Conversations' : 'Messages')}
          </span>
          <div className="flex items-center gap-1 shrink-0">
            {/* Conversation / Message toggle */}
            <button
              onClick={() => setConversationMode(!conversationMode)}
              title={conversationMode ? 'Switch to message list' : 'Switch to conversation view'}
              className="flex items-center gap-1 rounded px-2 py-0.5 text-xs text-muted hover:text-foreground hover:bg-accent transition-colors"
            >
              {conversationMode ? <ListIcon size={12} /> : <MessageSquare size={12} />}
              {conversationMode ? 'Msgs' : 'Threads'}
            </button>

            {!conversationMode && (
              <button
                onClick={toggleUnreadOnly}
                title={showUnreadOnly ? 'Show all' : 'Show unread only'}
                className={[
                  'flex items-center gap-1 rounded px-2 py-0.5 text-xs transition-colors',
                  showUnreadOnly
                    ? 'bg-primary text-primary-foreground'
                    : unreadCount > 0
                      ? 'text-foreground hover:bg-accent'
                      : 'text-muted hover:text-foreground hover:bg-accent',
                ].join(' ')}
              >
                <MailOpen size={12} />
                {unreadCount > 0 ? `${unreadCount}` : '0'}
              </button>
            )}

            <span className="text-xs text-muted">
              {conversationMode ? conversations.length : messages.length}
            </span>
          </div>
        </div>
      )}

      {isSearching ? (
        <SearchResults />
      ) : !hasContent ? (
        <EmptyState icon={<Inbox size={32} className="text-muted" />} text="Select an account" />
      ) : isLoading ? (
        <EmptyState
          icon={<RefreshCw size={20} className="animate-spin text-muted" />}
          text="Loading…"
        />
      ) : conversationMode ? (
        conversations.length === 0 ? (
          <EmptyState icon={<MessageSquare size={32} className="text-muted" />} text="No conversations" />
        ) : (
          <div className="flex-1 overflow-hidden">
            <List
              style={{ height: '100%', width: '100%' }}
              rowComponent={ConvRow}
              rowCount={conversations.length}
              rowHeight={CONV_ROW_HEIGHT}
              rowProps={{ conversations, selectedThreadMessageId, selectThread }}
            />
          </div>
        )
      ) : (
        messages.length === 0 ? (
          <EmptyState
            icon={<Inbox size={32} className="text-muted" />}
            text={showUnreadOnly ? 'No unread messages' : 'No messages'}
          />
        ) : (
          <div className="flex-1 overflow-hidden">
            <List
              style={{ height: '100%', width: '100%' }}
              rowComponent={MsgRow}
              rowCount={messages.length}
              rowHeight={MESSAGE_ROW_HEIGHT}
              rowProps={{ messages, selectedMessageId, selectMessage }}
            />
          </div>
        )
      )}
    </div>
  );
}

// ── Conversation virtual row ──────────────────────────────────────────────────

interface ConvRowData {
  conversations: ConversationSummary[];
  selectedThreadMessageId: string | null;
  selectThread: (id: string | null) => void;
}

function ConvRow({ index, style, conversations, selectedThreadMessageId, selectThread }: RowComponentProps<ConvRowData>) {
  const conv = conversations[index];
  return (
    <div style={style}>
      <ConversationRow
        conversation={conv}
        isSelected={selectedThreadMessageId === conv.sampleMessageId}
        onClick={() => selectThread(conv.sampleMessageId)}
      />
    </div>
  );
}

// ── Message virtual row ───────────────────────────────────────────────────────

interface MsgRowData {
  messages: MessageEnvelope[];
  selectedMessageId: string | null;
  selectMessage: (id: string | null) => void;
}

function MsgRow({ index, style, messages, selectedMessageId, selectMessage }: RowComponentProps<MsgRowData>) {
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
