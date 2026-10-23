import { useEffect, useRef } from 'react';
import { List, type RowComponentProps } from 'react-window';
import { RefreshCw, Inbox, MailOpen, MessageSquare, List as ListIcon, Trash2, X } from 'lucide-react';
import { toast } from 'sonner';
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
  useTrashThreads,
} from '@/features/message-actions';

const MESSAGE_ROW_HEIGHT = 76;
const CONV_ROW_HEIGHT = 64;

export function MessageList() {
  const {
    selectedAccountId, selectedMailboxId,
    selectedMessageId, selectMessage, selectMailbox,
    selectedMessageIds, toggleMessageInSelection, selectMessageRange,
    selectedThreadMessageId, selectThread,
    selectedThreadIds, toggleThreadInSelection, selectThreadRange,
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
  const trashThreads = useTrashThreads();

  const selected = messages.find((m) => m.uid === selectedMessageId) ?? null;

  // ── Multi-selection ───────────────────────────────────────────────────
  // Shift-click spans from the last row clicked without Shift, so a second
  // Shift-click re-spans from the same row instead of from the previous end.
  const anchorRef = useRef<string | null>(null);

  // The modifiers as the keyboard last reported them, in case a mouse event
  // comes through without them: the row listens on mousedown because
  // WebKitGTK fires no click at all for a Ctrl-click.
  const heldRef = useRef({ ctrl: false, shift: false });
  useEffect(() => {
    const track = (e: KeyboardEvent) => {
      heldRef.current = { ctrl: e.ctrlKey || e.metaKey, shift: e.shiftKey };
    };
    const release = () => {
      heldRef.current = { ctrl: false, shift: false };
    };
    window.addEventListener('keydown', track);
    window.addEventListener('keyup', track);
    window.addEventListener('blur', release);
    return () => {
      window.removeEventListener('keydown', track);
      window.removeEventListener('keyup', track);
      window.removeEventListener('blur', release);
    };
  }, []);

  // Both list modes share the selection logic, each over its own rows: message
  // uids, or the sample message ids that stand for conversations.
  const rowIds = conversationMode
    ? conversations.map((c) => c.sampleMessageId)
    : messages.map((m) => m.uid);
  const openId = conversationMode ? selectedThreadMessageId : selectedMessageId;
  const multiIds = conversationMode ? selectedThreadIds : selectedMessageIds;
  const openRow = conversationMode ? selectThread : selectMessage;

  const clickRow = (id: string, e: React.MouseEvent | React.KeyboardEvent) => {
    const ctrl = e.ctrlKey || e.metaKey || heldRef.current.ctrl;
    const shift = e.shiftKey || heldRef.current.shift;
    if (shift) {
      const from = rowIds.indexOf(anchorRef.current ?? openId ?? '');
      const to = rowIds.indexOf(id);
      if (from !== -1 && to !== -1) {
        const range = rowIds.slice(Math.min(from, to), Math.max(from, to) + 1);
        if (conversationMode) selectThreadRange(range, id);
        else selectMessageRange(range, id);
        return;
      }
    }
    anchorRef.current = id;
    if (!ctrl) openRow(id);
    else if (conversationMode) toggleThreadInSelection(id);
    else toggleMessageInSelection(id);
  };

  /** Trash every selected row in one round trip, then open the first row
   *  below the selection (or above it, when the selection reached the end). */
  const trashSelection = (permanently = false) => {
    const gone = new Set(multiIds);
    const last = rowIds.reduce((at, id, i) => (gone.has(id) ? i : at), -1);
    const next =
      rowIds.slice(last + 1).find((id) => !gone.has(id)) ??
      rowIds.slice(0, last).reverse().find((id) => !gone.has(id));
    openRow(next ?? null);
    const done = conversationMode
      ? trashThreads.trash([...gone], permanently)
      : deleteMany.mutateAsync({ messageIds: [...gone], permanently });
    done.then(
      ([, failed]) => {
        if (failed > 0) toast.error(`${failed} messaggi non eliminati`);
      },
      (e) => toast.error(`Eliminazione non riuscita: ${String(e)}`),
    );
  };
  const multiSelected = multiIds.length > 1;
  const confirmPermanent = () => {
    if (window.confirm(`Elimini definitivamente ${multiIds.length} ${conversationMode ? 'conversazioni' : 'messaggi'}?`)) {
      trashSelection(true);
    }
  };

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
    // In conversation mode ThreadView owns these keys for the open thread and
    // leaves them unbound while several threads are selected.
    deleteMessage: multiSelected ? () => trashSelection() : conversationMode ? undefined : () => {
      if (!selected) return;
      selectNeighbour(selected.uid, messages);
      deleteMessage.mutate({ messageId: selected.uid });
    },
    deletePermanently: multiSelected ? confirmPermanent : conversationMode ? undefined : () => {
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
      {multiSelected && !isSearching ? (
        <div className="flex items-center justify-between border-b border-border px-3 py-1.5 gap-2">
          <span className="text-sm font-semibold text-foreground truncate">
            {multiIds.length} {conversationMode ? 'conversazioni' : 'messaggi'} selezionati
          </span>
          <div className="flex items-center gap-1 shrink-0">
            <button
              onClick={() => trashSelection()}
              disabled={deleteMany.isPending || trashThreads.isPending}
              title="Sposta nel cestino (Canc)"
              className="flex items-center gap-1 rounded px-2 py-0.5 text-xs text-foreground hover:bg-accent transition-colors disabled:opacity-50"
            >
              <Trash2 size={12} />
              Cestina
            </button>
            <button
              onClick={() => openRow(openId)}
              title="Annulla selezione"
              className="flex items-center rounded p-1 text-muted hover:text-foreground hover:bg-accent transition-colors"
            >
              <X size={12} />
            </button>
          </div>
        </div>
      ) : hasContent && !isSearching && (
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
              rowProps={{ conversations, selectedThreadMessageId, selectedThreadIds, clickRow }}
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
              rowProps={{ messages, selectedMessageId, selectedMessageIds, clickRow, openDraft }}
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
  selectedThreadIds: string[];
  clickRow: (id: string, e: React.MouseEvent | React.KeyboardEvent) => void;
}

function ConvRow({ index, style, conversations, selectedThreadMessageId, selectedThreadIds, clickRow }: RowComponentProps<ConvRowData>) {
  const conv = conversations[index];
  return (
    <div style={style}>
      <ConversationRow
        conversation={conv}
        isSelected={selectedThreadMessageId === conv.sampleMessageId || selectedThreadIds.includes(conv.sampleMessageId)}
        onSelect={(e) => clickRow(conv.sampleMessageId, e)}
      />
    </div>
  );
}

// ── Message virtual row ───────────────────────────────────────────────────────

interface MsgRowData {
  messages: MessageEnvelope[];
  selectedMessageId: string | null;
  selectedMessageIds: string[];
  clickRow: (id: string, e: React.MouseEvent | React.KeyboardEvent) => void;
  openDraft?: (id: string) => void;
}

function MsgRow({ index, style, messages, selectedMessageId, selectedMessageIds, clickRow, openDraft }: RowComponentProps<MsgRowData>) {
  const message = messages[index];
  return (
    <MessageRow
      message={message}
      isSelected={selectedMessageId === message.uid || selectedMessageIds.includes(message.uid)}
      onSelect={(e) => clickRow(message.uid, e)}
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
