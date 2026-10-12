import { useEffect, useRef, useState } from 'react';
import { Archive, ChevronDown, Forward, MailOpen, Reply, ReplyAll, Trash2 } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useThread, type ThreadMessage } from '@/entities/conversation';
import { useSessionStore, mailboxKey } from '@/entities/session';
import { formatDate } from '@/shared/lib/utils';
import { useSetFlags, useDeleteMessage, useMoveMessage, useToggleStar } from '@/features/message-actions';
import { useMailboxes } from '@/entities/mailbox';
import { useShortcuts } from '@/features/keyboard-shortcuts';
import { useComposeStore, type ComposeMode } from '@/features/compose-message';
import { EmailContent } from './EmailContent';

// ── Main component ─────────────────────────────────────────────────────────

export function ThreadView() {
  const {
    selectedThreadMessageId,
    selectedAccountId,
    selectedMailboxId,
    selectThread,
  } = useSessionStore();

  const { data: messages, isLoading, error } = useThread(selectedThreadMessageId);
  const openCompose = useComposeStore((s) => s.openCompose);
  const setFlags = useSetFlags();
  const deleteMsg = useDeleteMessage();
  const moveMsg = useMoveMessage();
  const toggleStar = useToggleStar();
  const { data: mailboxes = [] } = useMailboxes(selectedAccountId);
  const qc = useQueryClient();

  const mailboxCacheKey = mailboxKey(selectedAccountId, selectedMailboxId) ?? undefined;

  // Which messages are expanded
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());

  // Ref to scroll the expanded message into view on load
  const expandedRef = useRef<HTMLDivElement>(null);
  // Guard: fire mark-as-read at most once per thread selection
  const markedThreadRef = useRef<string | null>(null);

  useEffect(() => {
    if (!messages?.length) return;
    const lastId = messages[messages.length - 1].id;
    setExpandedIds(new Set([lastId]));
  }, [selectedThreadMessageId, messages]);

  // Auto-mark-as-read the newest (auto-expanded) message when the thread opens.
  // Uses the same ref-guard pattern as MessageViewer to avoid re-firing on
  // optimistic-update flips of isRead.
  useEffect(() => {
    if (!messages?.length || !selectedThreadMessageId) return;
    if (markedThreadRef.current === selectedThreadMessageId) return;
    markedThreadRef.current = selectedThreadMessageId;

    const lastMsg = messages[messages.length - 1];
    if (!lastMsg.isRead) {
      setFlags.mutate({
        messageId: lastMsg.id,
        flagsToAdd: ['seen'],
        currentIsRead: false,
        mailboxKey: mailboxCacheKey,
      });
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedThreadMessageId, messages]); // only when the selected thread changes

  // Scroll expanded message near the top when thread loads
  useEffect(() => {
    if (!expandedRef.current) return;
    const t = setTimeout(() => {
      expandedRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 80);
    return () => clearTimeout(t);
  }, [selectedThreadMessageId]);

  /** Delete = move to Trash (see `handle_delete` on the server), so this is
   *  recoverable and needs no confirmation, like the single-message toolbar. */
  async function deleteMessages(ids: string[], permanently = false) {
    if (!ids.length) return;
    await Promise.all(ids.map((id) => deleteMsg.mutateAsync({ messageId: id, permanently })));
    // Leave the thread only when nothing is left to show — deleting one message
    // out of five must keep the conversation open.
    if (ids.length >= (messages?.length ?? 0)) selectThread(null);
  }

  /** Move every message of the thread to `kind`, if the account has that folder. */
  async function moveThreadTo(kind: 'archive' | 'all' | 'spam') {
    const target =
      mailboxes.find((m) => m.kind === kind) ??
      // Gmail has no \\Archive; its All Mail doubles as the archive target.
      (kind === 'archive' ? mailboxes.find((m) => m.kind === 'all') : undefined);
    if (!target || !messages?.length) return;
    await Promise.all(
      messages.map((m) => moveMsg.mutateAsync({ messageId: m.id, toMailboxId: target.id })),
    );
    selectThread(null);
  }

  // Bound here, not in MessageList: the flat list only knows a conversation's
  // sample id, while the whole thread's message ids live in this component. Every
  // action here applies to the whole conversation — the labels say so too.
  const threadIds = (messages ?? []).map((m) => m.id);
  const hasUnread = !!messages?.some((m) => !m.isRead);
  useShortcuts({
    deleteMessage: () => void deleteMessages(threadIds),
    deletePermanently: () => void deleteMessages(threadIds, true),
    archiveMessage: () => void moveThreadTo('archive'),
    markSpam: () => void moveThreadTo('spam'),
    toggleRead: () => void markConversation(hasUnread),
    toggleFlag: () => {
      // Star the newest message: starring an entire thread is not a thing IMAP
      // models, and it is what the row indicator reflects.
      const newest = messages?.[messages.length - 1];
      if (newest) toggleStar.star(newest.id);
    },
    reply: () => openOnNewest('reply'),
    replyAll: () => openOnNewest('replyAll'),
    forward: () => openOnNewest('forward'),
  });

  const openOn = (messageId: string, mode: ComposeMode) =>
    openCompose({ mode, messageId, accountId: selectedAccountId });

  /** Shortcuts act on the newest message — the one auto-expanded on open. */
  function openOnNewest(mode: ComposeMode) {
    const newest = messages?.[messages.length - 1];
    if (newest) openOn(newest.id, mode);
  }

  if (!selectedThreadMessageId) return null;

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Caricamento conversazione…
      </div>
    );
  }

  if (error || !messages?.length) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-destructive">
        Impossibile caricare la conversazione.
      </div>
    );
  }

  const subject = messages[0].subject ?? '(senza oggetto)';

  function toggleExpanded(id: string) {
    const isExpanded = expandedIds.has(id);

    // Pure state update — no side effects inside the updater
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (isExpanded) next.delete(id);
      else next.add(id);
      return next;
    });

    if (!isExpanded) {
      // Auto-mark-as-read when expanding an unread message, like Gmail
      const msg = messages!.find((m) => m.id === id);
      if (msg && !msg.isRead) {
        void setFlags.mutateAsync({
          messageId: id,
          flagsToAdd: ['seen'],
          currentIsRead: false,
          mailboxKey: mailboxCacheKey,
        });
      }
    }
  }

  async function markConversation(markAsRead: boolean) {
    const toMark = messages!.filter((m) => m.isRead !== markAsRead);
    if (!toMark.length) return;
    await Promise.all(
      toMark.map((m) =>
        setFlags.mutateAsync({
          messageId: m.id,
          flagsToAdd: markAsRead ? ['seen'] : [],
          flagsToRemove: markAsRead ? [] : ['seen'],
          currentIsRead: m.isRead,
          mailboxKey: mailboxCacheKey,
        })
      )
    );
    void qc.invalidateQueries({ queryKey: ['thread', selectedThreadMessageId] });
  }

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden bg-background">

      {/* ── Sticky subject header ──────────────────────────────────── */}
      <div className="shrink-0 border-b border-border bg-background px-6 py-4">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h2 className="truncate text-lg font-semibold text-foreground">{subject}</h2>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {messages.length} messagg{messages.length === 1 ? 'io' : 'i'}
            </p>
          </div>
          <div className="flex shrink-0 items-center gap-1">
            <button
              type="button"
              title={hasUnread ? 'Segna tutto come letto' : 'Segna tutto come non letto'}
              onClick={() => void markConversation(hasUnread)}
              className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
            >
              <MailOpen size={15} />
            </button>
            <button
              type="button"
              title="Archivia la conversazione"
              disabled={moveMsg.isPending}
              onClick={() => void moveThreadTo('archive')}
              className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground disabled:opacity-50"
            >
              <Archive size={15} />
            </button>
            <button
              type="button"
              title="Sposta la conversazione nel cestino"
              disabled={deleteMsg.isPending}
              onClick={() => void deleteMessages(messages.map((m) => m.id))}
              className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-destructive disabled:opacity-50"
            >
              <Trash2 size={15} />
            </button>
          </div>
        </div>
      </div>

      {/* ── Scrollable feed ───────────────────────────────────────── */}
      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="py-2">
          {messages.map((message, idx) => {
            const isExpanded = expandedIds.has(message.id);
            const isLast = idx === messages.length - 1;

            return (
              <div key={message.id} ref={isLast ? expandedRef : undefined}>
                {/* Separator between messages (not before first) */}
                {idx > 0 && (
                  <div className="mx-6 border-t border-border/60" />
                )}

                {isExpanded ? (
                  <ExpandedMessage
                    message={message}
                    onCollapse={() => toggleExpanded(message.id)}
                    onReply={() => openOn(message.id, 'reply')}
                    onReplyAll={() => openOn(message.id, 'replyAll')}
                    onForward={() => openOn(message.id, 'forward')}
                    onDelete={() => void deleteMessages([message.id])}
                  />
                ) : (
                  <CollapsedMessage
                    message={message}
                    onExpand={() => toggleExpanded(message.id)}
                  />
                )}

              </div>
            );
          })}

          {/* Bottom padding so content never hides under fixed UI */}
          <div className="h-12" />
        </div>
      </div>
    </div>
  );
}

// ── Collapsed strip ────────────────────────────────────────────────────────

function CollapsedMessage({
  message,
  onExpand,
}: {
  message: ThreadMessage;
  onExpand: () => void;
}) {
  const preview =
    (message.textBody ?? message.htmlBody?.replace(/<[^>]+>/g, ' ') ?? '')
      .trim()
      .slice(0, 100)
      .replace(/\s+/g, ' ');

  return (
    <button
      type="button"
      onClick={onExpand}
      className="flex w-full items-center gap-3 px-6 py-3 text-left transition-colors hover:bg-surface-hover"
    >
      <SenderAvatar name={message.fromName || message.fromEmail} size="sm" />

      <span className={[
        'w-36 shrink-0 truncate text-sm',
        message.isRead ? 'font-normal text-foreground' : 'font-semibold text-foreground',
      ].join(' ')}>
        {message.fromName || message.fromEmail}
      </span>

      <span className="min-w-0 flex-1 truncate text-sm text-muted-foreground">
        {preview || '—'}
      </span>

      <div className="flex shrink-0 items-center gap-2">
        {!message.isRead && (
          <span className="h-2 w-2 rounded-full bg-primary" />
        )}
        <span className="text-xs text-muted-foreground">{formatDate(message.date)}</span>
        <ChevronDown size={13} className="text-muted-foreground" />
      </div>
    </button>
  );
}

// ── Expanded message ───────────────────────────────────────────────────────

interface ExpandedProps {
  message: ThreadMessage;
  onCollapse: () => void;
  onReply: () => void;
  onReplyAll: () => void;
  onForward: () => void;
  onDelete: () => void;
}

function ExpandedMessage({
  message,
  onCollapse,
  onReply,
  onReplyAll,
  onForward,
  onDelete,
}: ExpandedProps) {
  return (
    <div className="px-6 py-3">
      {/* Message header — click collapses */}
      <button
        type="button"
        onClick={onCollapse}
        className="flex w-full items-start gap-3 text-left"
      >
        <SenderAvatar name={message.fromName || message.fromEmail} size="md" />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-baseline gap-x-2">
            <span className="text-sm font-semibold text-foreground">
              {message.fromName || message.fromEmail}
            </span>
            {message.fromName && (
              <span className="text-xs text-muted-foreground">&lt;{message.fromEmail}&gt;</span>
            )}
            {!message.isRead && (
              <span className="h-2 w-2 shrink-0 rounded-full bg-primary" />
            )}
            <span className="ml-auto shrink-0 text-xs text-muted-foreground">
              {formatDate(message.date)}
            </span>
            <ChevronDown size={13} className="shrink-0 rotate-180 text-muted-foreground" />
          </div>
          <div className="mt-0.5 text-xs text-muted-foreground">
            <span className="font-medium text-foreground">A:</span>{' '}
            {message.to.length > 0 ? message.to.join(', ') : '—'}
          </div>
        </div>
      </button>

      {/* Email body */}
      <div className="ml-11 mt-4">
        <EmailContent htmlBody={message.htmlBody} textBody={message.textBody} />
      </div>

      {/* Action bar */}
      <div className="ml-11 mt-4 flex items-center gap-2">
        <ActionBtn icon={<Reply size={13} />} label="Rispondi" onClick={onReply} />
        <ActionBtn icon={<ReplyAll size={13} />} label="Rispondi a tutti" onClick={onReplyAll} />
        <ActionBtn icon={<Forward size={13} />} label="Inoltra" onClick={onForward} />
        <ActionBtn icon={<Trash2 size={13} />} label="Elimina" onClick={onDelete} />
      </div>
    </div>
  );
}

// ── Shared sub-components ──────────────────────────────────────────────────

function SenderAvatar({ name, size }: { name: string; size: 'sm' | 'md' }) {
  const letter = name.trim()[0]?.toUpperCase() ?? '?';
  const cls = size === 'md'
    ? 'h-9 w-9 text-sm'
    : 'h-7 w-7 text-xs';
  return (
    <div className={`${cls} flex shrink-0 items-center justify-center rounded-full bg-primary/15 font-semibold text-primary`}>
      {letter}
    </div>
  );
}

function ActionBtn({
  icon,
  label,
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex items-center gap-1.5 rounded-full border border-border px-3 py-1.5 text-xs text-foreground transition-colors hover:bg-surface-hover"
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}
