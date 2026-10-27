import { useEffect, useRef, useState } from 'react';
import {
  RefreshCw, Paperclip, Reply, ReplyAll, Forward, Trash2, Mail, MailOpen,
  MoreHorizontal, Archive, Ban, Star, Flame, Pencil,
} from 'lucide-react';
import { cn, formatDate } from '@/shared/lib/utils';
import { useMessageBody, type MessageEnvelope } from '@/entities/message';
import {
  useSetFlags, useDeleteMessage, useMoveMessage, useToggleStar, useSelectNeighbour,
} from '@/features/message-actions';
import { useMailboxes } from '@/entities/mailbox';
import { useKeymap, type ShortcutBinding } from '@/features/keyboard-shortcuts';
import { formatForDisplay } from '@tanstack/react-hotkeys';
import { useSessionStore, mailboxKey } from '@/entities/session';
import { useComposeStore, type ComposeMode } from '@/features/compose-message';
import { useShortcuts } from '@/features/keyboard-shortcuts';
import { EmailContent } from './EmailContent';

interface MessageViewerProps {
  envelope: MessageEnvelope;
  /** The mailbox the message is listed in: every action acts from it. */
  mailboxId: string;
}

export function MessageViewer({ envelope, mailboxId }: MessageViewerProps) {
  const { data: body, isLoading } = useMessageBody(envelope.uid, mailboxId);
  const setFlags = useSetFlags();
  const deleteMsg = useDeleteMessage();
  const moveMsg = useMoveMessage();
  const toggleStar = useToggleStar();
  const selectNeighbour = useSelectNeighbour();
  const [menuOpen, setMenuOpen] = useState(false);
  const { selectedAccountId, selectedMailboxId } = useSessionStore();
  const openCompose = useComposeStore((s) => s.openCompose);
  const { data: mailboxes = [] } = useMailboxes(selectedAccountId);
  const mbKey = mailboxKey(selectedAccountId, selectedMailboxId);
  const keymap = useKeymap();

  // Auto-mark as read when opening an unread message.
  // Guard with a ref so switching between unread messages doesn't cause loops.
  const markedRef = useRef<string | null>(null);
  useEffect(() => {
    if (envelope.isRead) return; // already read — nothing to do
    if (markedRef.current === envelope.uid) return; // already fired for this message
    markedRef.current = envelope.uid;

    setFlags.mutate({
      messageId: envelope.uid,
      flagsToAdd: ['seen'],
      currentIsRead: false,
      mailboxKey: mbKey ?? undefined,
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [envelope.uid]); // only fire when the selected message changes, not on isRead flip

  const isDraft = mailboxes.find((m) => m.id === selectedMailboxId)?.kind === 'drafts';
  const open = (mode: ComposeMode) =>
    openCompose({ mode, messageId: envelope.uid, accountId: selectedAccountId });

  /** Gmail has no \\Archive; its All Mail doubles as the archive target. */
  const archiveMailbox =
    mailboxes.find((m) => m.kind === 'archive') ?? mailboxes.find((m) => m.kind === 'all');
  const spamMailbox = mailboxes.find((m) => m.kind === 'spam');

  const moveTo = (toMailboxId: string) => {
    // Hand the selection on before mutating: once the cache is invalidated the
    // list no longer knows what came after this message.
    selectNeighbour(envelope.uid);
    moveMsg.mutate({ messageId: envelope.uid, fromMailboxId: mailboxId, toMailboxId });
    setMenuOpen(false);
  };

  useShortcuts({
    reply: () => open('reply'),
    replyAll: () => open('replyAll'),
    forward: () => open('forward'),
  });

  const handleDelete = () => {
    selectNeighbour(envelope.uid);
    deleteMsg.mutate({ messageId: envelope.uid, mailboxId });
  };

  const handleToggleRead = () => {
    if (envelope.isRead) {
      setFlags.mutate({
        messageId: envelope.uid,
        flagsToRemove: ['seen'],
        currentIsRead: true,
        mailboxKey: mbKey ?? undefined,
      });
    } else {
      setFlags.mutate({
        messageId: envelope.uid,
        flagsToAdd: ['seen'],
        currentIsRead: false,
        mailboxKey: mbKey ?? undefined,
      });
    }
  };

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="flex items-center gap-1 border-b border-border px-4 py-2">
        {isDraft && (
          <ToolbarButton
            icon={<Pencil size={15} />}
            label="Modifica bozza"
            onClick={() => open('draft')}
          />
        )}
        <ToolbarButton
          icon={<Reply size={15} />}
          label="Rispondi"
          onClick={() => open('reply')}
        />
        <ToolbarButton
          icon={<ReplyAll size={15} />}
          label="Rispondi a tutti"
          onClick={() => open('replyAll')}
        />
        <ToolbarButton
          icon={<Forward size={15} />}
          label="Inoltra"
          onClick={() => open('forward')}
        />
        <div className="mx-2 h-4 w-px bg-border" />
        <ToolbarButton
          icon={envelope.isRead ? <Mail size={15} /> : <MailOpen size={15} />}
          label={envelope.isRead ? 'Segna come non letto' : 'Segna come letto'}
          onClick={handleToggleRead}
        />
        {archiveMailbox && (
          <ToolbarButton
            icon={<Archive size={15} />}
            label="Archivia"
            onClick={() => moveTo(archiveMailbox.id)}
          />
        )}
        <ToolbarButton
          icon={<Trash2 size={15} />}
          label="Sposta nel cestino"
          variant="destructive"
          onClick={handleDelete}
        />
        <div className="relative ml-auto">
          <ToolbarButton
            icon={<MoreHorizontal size={15} />}
            label="Altre azioni"
            onClick={() => setMenuOpen((v) => !v)}
          />
          {menuOpen && (
            <>
              {/* Click-away layer: cheaper than a document listener for a menu
                  that only exists while open. */}
              <div className="fixed inset-0 z-40" onClick={() => setMenuOpen(false)} aria-hidden />
              <div className="absolute right-0 z-50 mt-1 w-64 overflow-hidden rounded-lg border border-border bg-background py-1 shadow-xl">
                <MenuItem
                  icon={<Star size={13} />}
                  label={envelope.isFlagged ? 'Togli la stella' : 'Aggiungi la stella'}
                  hint={keyHint(keymap.toggleFlag)}
                  onClick={() => {
                    if (envelope.isFlagged) toggleStar.unstar(envelope.uid);
                    else toggleStar.star(envelope.uid);
                    setMenuOpen(false);
                  }}
                />
                {spamMailbox && (
                  <MenuItem
                    icon={<Ban size={13} />}
                    label="Segna come spam"
                    hint={keyHint(keymap.markSpam)}
                    onClick={() => moveTo(spamMailbox.id)}
                  />
                )}
                <div className="my-1 h-px bg-border" />
                <MenuItem
                  icon={<Flame size={13} />}
                  label="Elimina definitivamente"
                  hint={keyHint(keymap.deletePermanently)}
                  destructive
                  onClick={() => {
                    selectNeighbour(envelope.uid);
                    deleteMsg.mutate({ messageId: envelope.uid, mailboxId, permanently: true });
                    setMenuOpen(false);
                  }}
                />
              </div>
            </>
          )}
        </div>
      </div>

      <div className="border-b border-border px-6 py-4">
        <h1 className="mb-3 text-lg font-semibold leading-snug text-foreground">
          {envelope.subject || '(no subject)'}
        </h1>
        <div className="flex items-start gap-3">
          <SenderAvatar name={envelope.fromName || envelope.fromEmail} />
          <div className="flex-1 min-w-0">
            <div className="flex items-baseline gap-2">
              <span className="font-medium text-foreground">
                {envelope.fromName || envelope.fromEmail}
              </span>
              <span className="text-xs text-muted">&lt;{envelope.fromEmail}&gt;</span>
            </div>
            <div className="mt-0.5 flex items-center gap-2 text-xs text-muted">
              <span>To: {envelope.toAddresses.join(', ')}</span>
              <span>·</span>
              <span>{formatDate(envelope.date)}</span>
              {envelope.hasAttachment && (
                <>
                  <span>·</span>
                  <Paperclip size={11} />
                  <span>Attachment</span>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-5">
        {isLoading ? (
          <div className="flex items-center gap-2 text-sm text-muted">
            <RefreshCw size={14} className="animate-spin" />
            Loading message…
          </div>
        ) : (
          <EmailContent htmlBody={body?.htmlBody} textBody={body?.textBody} />
        )}
      </div>
    </div>
  );
}

function SenderAvatar({ name }: { name: string }) {
  const initials = name.slice(0, 2).toUpperCase();
  const hue = [...name].reduce((h, c) => h + c.charCodeAt(0), 0) % 360;
  return (
    <span
      className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xs font-bold text-white"
      style={{ backgroundColor: `hsl(${hue}, 55%, 40%)` }}
    >
      {initials}
    </span>
  );
}

function ToolbarButton({
  icon,
  label,
  variant = 'default',
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  variant?: 'default' | 'destructive';
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      title={label}
      onClick={onClick}
      className={cn(
        'flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-xs transition-colors',
        variant === 'destructive'
          ? 'text-muted-foreground hover:bg-destructive/10 hover:text-destructive'
          : 'text-muted-foreground hover:bg-surface-hover hover:text-foreground',
      )}
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

/** First chord of a binding, for the key hint next to a menu entry. */
function keyHint(binding: ShortcutBinding | undefined): string | null {
  if (!binding) return null;
  if (typeof binding === 'string') return formatForDisplay(binding);
  if (Array.isArray(binding)) return formatForDisplay(binding[0]);
  return binding.sequence.map((step) => formatForDisplay(step)).join(' ');
}

function MenuItem({
  icon,
  label,
  hint,
  destructive,
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  hint: string | null;
  destructive?: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'flex w-full items-center gap-2 px-3 py-1.5 text-left text-xs transition-colors hover:bg-surface-hover',
        destructive ? 'text-destructive' : 'text-foreground',
      ].join(' ')}
    >
      {icon}
      <span className="flex-1">{label}</span>
      {hint && (
        <kbd className="rounded border border-border bg-surface px-1 py-0.5 font-mono text-[10px] text-muted">
          {hint}
        </kbd>
      )}
    </button>
  );
}
