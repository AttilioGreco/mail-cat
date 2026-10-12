import { List, type RowComponentProps } from 'react-window';
import { RefreshCw, Inbox, MailOpen, MessageSquare, List as ListIcon } from 'lucide-react';
import { useSessionStore, mailboxKey } from '@/entities/session';
import { useMessages, MessageRow, type MessageEnvelope } from '@/entities/message';
import { useMailboxes, type Mailbox } from '@/entities/mailbox';
import { useConversations, ConversationRow, type ConversationSummary } from '@/entities/conversation';
import { useMailboxWatcher } from '@/features/watch-mailbox';
import { SearchBar, SearchResults, useSearchStore } from '@/features/search-messages';
import { useShortcuts } from '@/features/keyboard-shortcuts';
import { useComposeStore } from '@/features/compose-message';
import { useHotkey } from '@tanstack/react-hotkeys';
import {
  useMarkAsRead,
  useMarkAsUnread,
  useToggleStar,
  useDeleteMessage,
  useMoveMessage,
  useMarkMailboxRead,
  useSelectNeighbour,
  useDeleteMessages,
} from '@/features/message-actions';

const MESSAGE_ROW_HEIGHT = 76;
const CONV_ROW_HEIGHT = 64;

export function MessageList() {
  const {
    selectedAccountId, selectedMailboxId,
    selectedMessageId, selectMessage, selectMailbox,
    selectedThreadMessageId, selectThread,
    conversationMode, setConversationMode,
    showUnreadOnly, toggleUnreadOnly,
  } = useSessionStore();
  const searchQuery = useSearchStore((s) => s.searchQuery);
  const requestSearchFocus = useSearchStore((s) => s.requestFocus);

  const { data: mailboxes = [] } = useMailboxes(selectedAccountId);
  const selectedMailbox = mailboxes.find((m) => m.id === selectedMailboxId) ?? null;
  const openCompose = useComposeStore((s) => s.openCompose);
  // Apple Mail: opening a draft resumes editing it rather than reading it.
  const openDraft = selectedMailbox?.kind === 'drafts'
    ? (messageId: string) => openCompose({ mode: 'draft', messageId, accountId: selectedAccountId })
    : undefined;

  const key = mailboxKey(selectedAccountId, selectedMailboxId);

  // Real-time IMAP IDLE push → auto-invalidates query cache on new/changed messages
  useMailboxWatcher(selectedMailboxId, key, selectedAccountId);

  const { data: rawMessages = [], isLoading: messagesLoading } = useMessages(conversationMode ? null : key);
  const { data: rawConversations = [], isLoading: convsLoading } = useConversations(
    conversationMode && selectedAccountId ? selectedAccountId : null,
    selectedMailboxId
  );

  const messages = showUnreadOnly ? rawMessages.filter((m) => !m.isRead) : rawMessages;
  // A thread counts as unread while any message in it is unread — same rule the
  // unread badge already uses.
  const conversations = showUnreadOnly
    ? rawConversations.filter((c) => c.unreadCount > 0)
    : rawConversations;
  const isLoading = conversationMode ? convsLoading : messagesLoading;

  const isSearching = searchQuery !== null && searchQuery.trim().length > 0;
  const unreadCount = conversationMode
    ? rawConversations.filter((c) => c.unreadCount > 0).length
    : rawMessages.filter((m) => !m.isRead).length;

  const hasContent = selectedAccountId || selectedMailbox;

  // ── Keyboard shortcuts ────────────────────────────────────────────────
  // Bound here because this is where the ordered list and the selection live.
  const markAsRead = useMarkAsRead();
  const markAsUnread = useMarkAsUnread();
  const toggleStar = useToggleStar();
  const deleteMessage = useDeleteMessage();
  const moveMessage = useMoveMessage();
  const markFolderRead = useMarkMailboxRead();
  const selectNeighbour = useSelectNeighbour();
  const deleteMany = useDeleteMessages();

  const selected = messages.find((m) => m.uid === selectedMessageId) ?? null;

  /** Move the selection by `delta`, clamped, starting from the top when nothing
   *  is selected. Works on threads too when the list is in conversation mode. */
  const step = (delta: number) => {
    if (conversationMode) {
      if (conversations.length === 0) return;
      const at = conversations.findIndex((c) => c.sampleMessageId === selectedThreadMessageId);
      const next = at === -1 ? 0 : Math.min(conversations.length - 1, Math.max(0, at + delta));
      selectThread(conversations[next].sampleMessageId);
      return;
    }
    if (messages.length === 0) return;
    const at = messages.findIndex((m) => m.uid === selectedMessageId);
    const next = at === -1 ? 0 : Math.min(messages.length - 1, Math.max(0, at + delta));
    selectMessage(messages[next].uid);
  };

  /** Move the selection to the next/previous unread message. */
  const stepUnread = (delta: number) => {
    if (conversationMode || messages.length === 0) return;
    const from = messages.findIndex((m) => m.uid === selectedMessageId);
    // Scan outward from the current position, wrapping is deliberately not done:
    // running off the end should stop, not silently jump back to the top.
    for (let i = from + delta; i >= 0 && i < messages.length; i += delta) {
      if (!messages[i].isRead) {
        selectMessage(messages[i].uid);
        return;
      }
    }
  };

  /** Jump to a folder by special-use type — names are localized, types are not. */
  const goToKind = (kind: Mailbox['kind']) => {
    const target = mailboxes.find((m) => m.kind === kind);
    if (target) selectMailbox(target.id);
  };

  /** The folder an "archive" lands in: Gmail has no \\Archive, its All Mail doubles as one. */
  const archiveMailbox =
    mailboxes.find((m) => m.kind === 'archive') ?? mailboxes.find((m) => m.kind === 'all');

  useHotkey('Enter', () => {
    if (selectedMessageId) openDraft?.(selectedMessageId);
  }, { enabled: !!openDraft && !conversationMode });

  useShortcuts({
    nextMessage: () => step(1),
    prevMessage: () => step(-1),
    nextUnread: () => stepUnread(1),
    prevUnread: () => stepUnread(-1),
    focusSearch: requestSearchFocus,
    toggleUnreadFilter: toggleUnreadOnly,
    toggleConversations: () => setConversationMode(!conversationMode),
    goInbox: () => goToKind('inbox'),
    goSent: () => goToKind('sent'),
    goDrafts: () => goToKind('drafts'),
    goArchive: () => goToKind('archive'),
    goSpam: () => goToKind('spam'),
    goTrash: () => goToKind('trash'),
    markFolderRead: () => {
      if (selectedMailboxId) markFolderRead.mutate(selectedMailboxId);
    },
    // Message-scoped keys are left unbound in conversation mode: ThreadView
    // registers them there, on the whole thread, and two registrations of the
    // same chord would collide.
    toggleRead: conversationMode ? undefined : () => {
      if (!selected) return;
      const key = mailboxKey(selectedAccountId, selectedMailboxId) ?? undefined;
      if (selected.isRead) markAsUnread.mutate(selected.uid, key);
      else markAsRead.mutate(selected.uid, key);
    },
    toggleFlag: conversationMode ? undefined : () => {
      if (!selected) return;
      if (selected.isFlagged) toggleStar.unstar(selected.uid);
      else toggleStar.star(selected.uid);
    },
    // These four take the message out of the list, so each hands the selection
    // on to the next row first — `messages` (not the cache) because it is the
    // filtered list the user is actually looking at.
    deleteMessage: conversationMode ? undefined : () => {
      if (!selected) return;
      selectNeighbour(selected.uid, messages);
      deleteMessage.mutate({ messageId: selected.uid });
    },
    deletePermanently: conversationMode ? undefined : () => {
      if (!selected) return;
      selectNeighbour(selected.uid, messages);
      deleteMessage.mutate({ messageId: selected.uid, permanently: true });
    },
    markSpam: conversationMode ? undefined : () => {
      const spam = mailboxes.find((m) => m.kind === 'spam');
      if (!selected || !spam) return;
      selectNeighbour(selected.uid, messages);
      moveMessage.mutate({ messageId: selected.uid, toMailboxId: spam.id });
    },
    archiveMessage: conversationMode ? undefined : () => {
      if (!selected || !archiveMailbox) return;
      selectNeighbour(selected.uid, messages);
      moveMessage.mutate({ messageId: selected.uid, toMailboxId: archiveMailbox.id });
    },
  });

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

            <span className="text-xs text-muted">
              {conversationMode ? conversations.length : messages.length}
            </span>
          </div>
        </div>
      )}

      {isSearching ? (
        <SearchResults deleteMany={deleteMany} />
      ) : !hasContent ? (
        <EmptyState icon={<Inbox size={32} className="text-muted" />} text="Select an account" />
      ) : isLoading ? (
        <EmptyState
          icon={<RefreshCw size={20} className="animate-spin text-muted" />}
          text="Loading…"
        />
      ) : conversationMode ? (
        conversations.length === 0 ? (
          <EmptyState
            icon={<MessageSquare size={32} className="text-muted" />}
            text={showUnreadOnly ? 'No unread conversations' : 'No conversations'}
          />
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
              rowProps={{ messages, selectedMessageId, selectMessage, openDraft }}
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
  openDraft?: (id: string) => void;
}

function MsgRow({ index, style, messages, selectedMessageId, selectMessage, openDraft }: RowComponentProps<MsgRowData>) {
  const message = messages[index];
  return (
    <MessageRow
      message={message}
      isSelected={selectedMessageId === message.uid}
      onSelect={() => selectMessage(message.uid)}
      onOpen={openDraft && (() => openDraft(message.uid))}
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
