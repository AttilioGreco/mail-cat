import { useCallback, useEffect, useRef, useState } from 'react';
import { ChevronDown, Forward, MailOpen, Reply, ReplyAll, Send, X } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useThread, type ThreadMessage } from '@/entities/conversation';
import { useSessionStore, mailboxKey } from '@/entities/session';
import { formatDate } from '@/shared/lib/utils';
import { useAccounts } from '@/entities/account';
import { useSetFlags } from '@/features/message-actions';
import { useSendMessage } from '@/features/compose-message';

// ── Main component ─────────────────────────────────────────────────────────

export function ThreadView() {
  const {
    selectedThreadMessageId,
    selectedAccountId,
    selectedMailboxId,
  } = useSessionStore();

  const { data: messages, isLoading, error } = useThread(selectedThreadMessageId);
  const { data: accounts = [] } = useAccounts();
  const setFlags = useSetFlags();
  const qc = useQueryClient();

  const currentAccount = accounts.find((a) => a.id === selectedAccountId) ?? null;
  const mailboxCacheKey = mailboxKey(selectedAccountId, selectedMailboxId) ?? undefined;

  // Which messages are expanded
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  // Which message has the inline reply open, and in which mode
  const [replyingTo, setReplyingTo] = useState<{ id: string; mode: 'reply' | 'replyAll' | 'forward' } | null>(null);

  // Ref to scroll the expanded message into view on load
  const expandedRef = useRef<HTMLDivElement>(null);
  // Guard: fire mark-as-read at most once per thread selection
  const markedThreadRef = useRef<string | null>(null);

  useEffect(() => {
    if (!messages?.length) return;
    const lastId = messages[messages.length - 1].id;
    setExpandedIds(new Set([lastId]));
    setReplyingTo(null);
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
  const hasUnread = messages.some((m) => !m.isRead);

  function toggleExpanded(id: string) {
    const isExpanded = expandedIds.has(id);

    // Pure state update — no side effects inside the updater
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (isExpanded) next.delete(id);
      else next.add(id);
      return next;
    });

    if (isExpanded) {
      if (replyingTo?.id === id) setReplyingTo(null);
    } else {
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
          <button
            type="button"
            title={hasUnread ? 'Segna tutto come letto' : 'Segna tutto come non letto'}
            onClick={() => void markConversation(hasUnread)}
            className="shrink-0 rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
          >
            <MailOpen size={15} />
          </button>
        </div>
      </div>

      {/* ── Scrollable feed ───────────────────────────────────────── */}
      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="py-2">
          {messages.map((message, idx) => {
            const isExpanded = expandedIds.has(message.id);
            const isLast = idx === messages.length - 1;

            // Compute reply-all recipients: sender + To (minus self) → To; CC (minus self) → CC
            const ownEmail = currentAccount?.email?.toLowerCase() ?? '';
            const replyAllTo = [
              message.fromEmail,
              ...message.to.filter((addr) => addr.toLowerCase() !== ownEmail),
            ];
            const replyAllCc = message.cc.filter((addr) => addr.toLowerCase() !== ownEmail);

            return (
              <div key={message.id} ref={isLast ? expandedRef : undefined}>
                {/* Separator between messages (not before first) */}
                {idx > 0 && (
                  <div className="mx-6 border-t border-border/60" />
                )}

                {isExpanded ? (
                  <ExpandedMessage
                    message={message}
                    isReplying={replyingTo?.id === message.id}
                    accountId={selectedAccountId}
                    fromEmail={currentAccount?.email ?? ''}
                    onCollapse={() => toggleExpanded(message.id)}
                    onReply={() => setReplyingTo({ id: message.id, mode: 'reply' })}
                    onReplyAll={() => setReplyingTo({ id: message.id, mode: 'replyAll' })}
                    onForward={() => setReplyingTo({ id: message.id, mode: 'forward' })}
                    onCloseReply={() => setReplyingTo(null)}
                    replyMode={replyingTo?.id === message.id ? replyingTo.mode : null}
                    replyAllTo={replyAllTo}
                    replyAllCc={replyAllCc}
                    inReplyToRfcId={message.rfcMessageId}
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
  isReplying: boolean;
  accountId: string | null;
  fromEmail: string;
  replyMode: 'reply' | 'replyAll' | 'forward' | null;
  replyAllTo: string[];
  replyAllCc: string[];
  inReplyToRfcId: string | null;
  onCollapse: () => void;
  onReply: () => void;
  onReplyAll: () => void;
  onForward: () => void;
  onCloseReply: () => void;
}

function ExpandedMessage({
  message,
  isReplying,
  accountId,
  fromEmail,
  replyMode,
  replyAllTo,
  replyAllCc,
  inReplyToRfcId,
  onCollapse,
  onReply,
  onReplyAll,
  onForward,
  onCloseReply,
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
        <AutoSizedFrame html={message.htmlBody} text={message.textBody} />
      </div>

      {/* Action bar */}
      <div className="ml-11 mt-4 flex items-center gap-2">
        <ActionBtn icon={<Reply size={13} />} label="Rispondi" onClick={onReply} />
        <ActionBtn icon={<ReplyAll size={13} />} label="Rispondi a tutti" onClick={onReplyAll} />
        <ActionBtn icon={<Forward size={13} />} label="Inoltra" onClick={onForward} />
      </div>

      {/* Inline reply box */}
      {isReplying && replyMode !== null && (
        <div className="ml-11 mt-3">
          <InlineReply
            to={replyMode === 'replyAll' ? replyAllTo : replyMode === 'forward' ? [] : [message.fromEmail]}
            cc={replyMode === 'replyAll' ? replyAllCc : []}
            subject={message.subject}
            mode={replyMode}
            accountId={accountId}
            fromEmail={fromEmail}
            inReplyToRfcId={replyMode !== 'forward' ? inReplyToRfcId : null}
            onClose={onCloseReply}
          />
        </div>
      )}
    </div>
  );
}

// ── Auto-sized iframe (isolates email CSS) ─────────────────────────────────

function AutoSizedFrame({ html, text }: { html: string | null; text: string | null }) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [height, setHeight] = useState(200);

  // Build a sandboxed document with CSS reset to neutralize custom styles
  const srcdoc = html
    ? `<!DOCTYPE html><html><head><meta charset="utf-8"><style>
        *{box-sizing:border-box;margin:0;padding:0}
        body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;
             font-size:14px;line-height:1.6;color:#1a1a1a;word-break:break-word;overflow:hidden;
             padding:12px 16px}
        img{max-width:100%;height:auto;display:block}
        a{color:#1a73e8}
        blockquote{border-left:3px solid #dadce0;margin:8px 0;padding-left:12px;color:#555}
        pre,code{font-family:monospace;font-size:13px;background:#f5f5f5;border-radius:3px;padding:2px 4px}
        pre{padding:10px;overflow-x:auto;white-space:pre-wrap}
        table{border-collapse:collapse;max-width:100%}
        td,th{padding:4px 8px}
      </style></head><body>${html}</body></html>`
    : null;

  const measure = useCallback(() => {
    const doc = iframeRef.current?.contentDocument;
    if (!doc) return;
    const h = doc.documentElement.scrollHeight;
    if (h > 0) setHeight(h);
  }, []);

  const onLoad = useCallback(() => {
    measure();
    // Watch for images and lazy content finishing
    const doc = iframeRef.current?.contentDocument;
    if (!doc) return;
    const ro = new ResizeObserver(measure);
    ro.observe(doc.body);
    // Images
    doc.querySelectorAll('img').forEach((img) => img.addEventListener('load', measure));
  }, [measure]);

  if (!srcdoc && !text) {
    return <p className="text-sm text-muted-foreground italic">(nessun contenuto)</p>;
  }

  if (!srcdoc) {
    // Plain text fallback — no iframe needed
    return (
      <pre className="whitespace-pre-wrap break-words font-sans text-sm text-foreground leading-relaxed">
        {text}
      </pre>
    );
  }

  return (
    <iframe
      ref={iframeRef}
      srcDoc={srcdoc}
      sandbox="allow-same-origin"
      onLoad={onLoad}
      style={{ height: `${height}px` }}
      className="w-full border-0 bg-white"
      title="email body"
    />
  );
}

// ── Inline reply compose ───────────────────────────────────────────────────

interface InlineReplyProps {
  to: string[];
  cc: string[];
  subject: string;
  mode: 'reply' | 'replyAll' | 'forward';
  accountId: string | null;
  fromEmail: string;
  inReplyToRfcId: string | null;
  onClose: () => void;
}

function InlineReply({ to, cc, subject, mode, accountId, fromEmail, inReplyToRfcId, onClose }: InlineReplyProps) {
  const [body, setBody] = useState('');
  const [toInput, setToInput] = useState(to.join(', '));
  const [ccInput, setCcInput] = useState(cc.join(', '));
  const send = useSendMessage();

  const isForward = mode === 'forward';
  const subjectPrefix = isForward ? 'Fwd: ' : 'Re: ';
  const replySubject = subject.startsWith(subjectPrefix) ? subject : `${subjectPrefix}${subject}`;
  const hasCc = ccInput.trim().length > 0;

  async function handleSend() {
    if (!body.trim() || !accountId) return;
    const recipients = toInput.split(',').map((s) => s.trim()).filter(Boolean);
    if (!recipients.length) return;
    const ccRecipients = ccInput.split(',').map((s) => s.trim()).filter(Boolean);
    await send.mutateAsync({
      accountId,
      fromEmail,
      to: recipients,
      cc: ccRecipients,
      subject: replySubject,
      bodyText: body,
      inReplyTo: inReplyToRfcId ?? undefined,
      references: inReplyToRfcId ? [inReplyToRfcId] : [],
    });
    onClose();
  }

  return (
    <div className="rounded-xl border border-border bg-background shadow-[0_2px_12px_0_rgba(0,0,0,0.10)] dark:shadow-[0_2px_12px_0_rgba(0,0,0,0.30)]">
      {/* Mini header — editable To field */}
      <div className="flex items-center justify-between border-b border-border px-4 py-2.5">
        <div className="flex min-w-0 flex-1 items-center gap-1.5">
          <span className="shrink-0 text-xs font-medium text-foreground">A:</span>
          <input
            type="text"
            value={toInput}
            onChange={(e) => setToInput(e.target.value)}
            className="min-w-0 flex-1 bg-transparent text-xs text-muted-foreground focus:outline-none focus:text-foreground"
            placeholder="destinatario@example.com"
          />
        </div>
        <button
          type="button"
          onClick={onClose}
          className="ml-2 shrink-0 rounded p-0.5 text-muted-foreground transition-colors hover:text-foreground"
        >
          <X size={13} />
        </button>
      </div>

      {/* CC row — shown when there are CC recipients or in replyAll mode */}
      {(hasCc || mode === 'replyAll') && (
        <div className="flex items-center gap-1.5 border-b border-border px-4 py-2.5">
          <span className="shrink-0 text-xs font-medium text-foreground">CC:</span>
          <input
            type="text"
            value={ccInput}
            onChange={(e) => setCcInput(e.target.value)}
            className="min-w-0 flex-1 bg-transparent text-xs text-muted-foreground focus:outline-none focus:text-foreground"
            placeholder="cc@example.com"
          />
        </div>
      )}

      {/* Subject */}
      <div className="border-b border-border px-4 py-2">
        <span className="text-xs text-muted-foreground">{replySubject}</span>
      </div>

      {/* Textarea */}
      <textarea
        autoFocus
        value={body}
        onChange={(e) => setBody(e.target.value)}
        placeholder="Scrivi una risposta…"
        rows={5}
        className="w-full resize-none bg-transparent px-4 py-3 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none"
      />

      {/* Footer actions */}
      <div className="flex items-center justify-between border-t border-border px-4 py-2.5">
        <button
          type="button"
          onClick={onClose}
          className="text-xs text-muted-foreground transition-colors hover:text-foreground"
        >
          Annulla
        </button>
        <button
          type="button"
          onClick={() => void handleSend()}
          disabled={!body.trim() || !toInput.trim() || send.isPending}
          className="flex items-center gap-1.5 rounded-lg bg-primary px-4 py-1.5 text-xs font-medium text-primary-foreground transition-opacity disabled:opacity-50"
        >
          <Send size={12} />
          {send.isPending ? 'Invio…' : 'Invia'}
        </button>
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
