import { useEffect, useRef } from 'react';
import { RefreshCw, Paperclip, Reply, Forward, Trash2, Mail, MailOpen, MoreHorizontal } from 'lucide-react';
import { cn, formatDate } from '@/lib/utils';
import { useMessageBody } from '@/hooks/useMessages';
import { useSetFlags, useDeleteMessage } from '@/hooks/useMutations';
import { useUIStore, mailboxKey } from '@/store/uiStore';
import { useAccounts } from '@/hooks/useAccounts';
import { EmailContent } from '@/components/messages/EmailContent';
import type { MessageEnvelope } from '@/lib/types';

interface MessageViewerProps {
  envelope: MessageEnvelope;
}

export function MessageViewer({ envelope }: MessageViewerProps) {
  const { data: body, isLoading } = useMessageBody(envelope.uid);
  const setFlags = useSetFlags();
  const deleteMsg = useDeleteMessage();
  const { selectMessage, selectedAccountId, selectedMailbox, openCompose } = useUIStore();
  const { data: accounts = [] } = useAccounts();
  const mbKey = mailboxKey(selectedAccountId, selectedMailbox?.id ?? null);

  // The account that received this message — drives identity pre-selection and chip search scope.
  const currentAccount = accounts.find((a) => a.id === selectedAccountId);

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

  const handleDelete = () => {
    deleteMsg.mutate(
      { messageId: envelope.uid },
      { onSuccess: () => selectMessage(null) },
    );
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
        <ToolbarButton
          icon={<Reply size={15} />}
          label="Reply"
          onClick={() => openCompose({
            mode: 'reply',
            from: currentAccount?.email,
            accountId: selectedAccountId ?? undefined,
            // Use fromName + fromEmail so the chip shows the display name immediately
            to: [envelope.fromName
              ? `${envelope.fromName} <${envelope.fromEmail}>`
              : envelope.fromEmail],
            subject: envelope.subject.startsWith('Re:') ? envelope.subject : `Re: ${envelope.subject}`,
            identities: [],
          })}
        />
        <ToolbarButton
          icon={<Forward size={15} />}
          label="Forward"
          onClick={() => openCompose({
            mode: 'forward',
            from: currentAccount?.email,
            accountId: selectedAccountId ?? undefined,
            subject: envelope.subject.startsWith('Fwd:') ? envelope.subject : `Fwd: ${envelope.subject}`,
            identities: [],
          })}
        />
        <div className="mx-2 h-4 w-px bg-border" />
        <ToolbarButton
          icon={envelope.isRead ? <Mail size={15} /> : <MailOpen size={15} />}
          label={envelope.isRead ? 'Mark unread' : 'Mark read'}
          onClick={handleToggleRead}
        />
        <ToolbarButton
          icon={<Trash2 size={15} />}
          label="Delete"
          variant="destructive"
          onClick={handleDelete}
        />
        <div className="ml-auto">
          <ToolbarButton icon={<MoreHorizontal size={15} />} label="More" />
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
