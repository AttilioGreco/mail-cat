import { useEffect, useMemo, useState } from 'react';
import { Forward, Mail, MailOpen, Reply, ReplyAll, X } from 'lucide-react';
import { useThread } from '@/hooks/useConversations';
import { useUIStore } from '@/store/uiStore';
import { formatDate } from '@/lib/utils';
import { EmailContent } from '@/components/messages/EmailContent';
import { useAccounts } from '@/hooks/useAccounts';
import { mailboxKey } from '@/store/uiStore';
import { useSetFlags } from '@/hooks/useMutations';
import type { ThreadMessage } from '@/lib/types';
import type { AddressEntry } from '@/lib/composeTypes';

export function ThreadView() {
  const {
    selectedThreadMessageId,
    selectedAccountId,
    selectedMailbox,
    openCompose,
    selectThread,
  } = useUIStore();
  const { data: messages, isLoading, error } = useThread(selectedThreadMessageId);
  const { data: accounts = [] } = useAccounts();
  const setFlags = useSetFlags();
  const [activeMessageId, setActiveMessageId] = useState<string | null>(null);

  useEffect(() => {
    setActiveMessageId(messages?.[messages.length - 1]?.id ?? null);
  }, [selectedThreadMessageId, messages]);

  if (!selectedThreadMessageId) return null;

  if (isLoading) {
    return (
      <ModalShell onClose={() => selectThread(null)}>
        <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
          Loading thread…
        </div>
      </ModalShell>
    );
  }

  if (error || !messages) {
    return (
      <ModalShell onClose={() => selectThread(null)}>
        <div className="flex h-full items-center justify-center text-sm text-destructive">
          Failed to load thread.
        </div>
      </ModalShell>
    );
  }

  const subject = messages[0]?.subject ?? '(no subject)';
  const activeMessage = messages.find((message) => message.id === activeMessageId) ?? messages[messages.length - 1];
  const currentAccount = accounts.find((account) => account.id === selectedAccountId) ?? null;
  const mailboxCacheKey = mailboxKey(selectedAccountId, selectedMailbox?.id ?? null) ?? undefined;
  const hasUnread = messages.some((message) => !message.isRead);

  const replyTo = (message: ThreadMessage) => {
    openCompose({
      mode: 'reply',
      from: currentAccount?.email,
      accountId: selectedAccountId ?? undefined,
      to: [toAddressEntry(message.fromName, message.fromEmail)],
      subject: message.subject.startsWith('Re:') ? message.subject : `Re: ${message.subject}`,
      identities: [],
    });
  };

  const replyAllTo = (message: ThreadMessage) => {
    const ownEmail = currentAccount?.email?.toLowerCase();
    const recipients = dedupeAddresses([
      message.fromName ? `${message.fromName} <${message.fromEmail}>` : message.fromEmail,
      ...message.to,
    ]).filter((entry) => extractEmail(entry).toLowerCase() !== ownEmail);

    openCompose({
      mode: 'reply',
      from: currentAccount?.email,
      accountId: selectedAccountId ?? undefined,
      to: recipients.map((entry) => addressEntryFromString(entry)),
      subject: message.subject.startsWith('Re:') ? message.subject : `Re: ${message.subject}`,
      identities: [],
    });
  };

  const forwardMessage = (message: ThreadMessage) => {
    openCompose({
      mode: 'forward',
      from: currentAccount?.email,
      accountId: selectedAccountId ?? undefined,
      subject: message.subject.startsWith('Fwd:') ? message.subject : `Fwd: ${message.subject}`,
      identities: [],
    });
  };

  const markConversation = async (markAsRead: boolean) => {
    for (const message of messages) {
      if (markAsRead === message.isRead) continue;
      await setFlags.mutateAsync({
        messageId: message.id,
        flagsToAdd: markAsRead ? ['seen'] : [],
        flagsToRemove: markAsRead ? [] : ['seen'],
        currentIsRead: message.isRead,
        mailboxKey: mailboxCacheKey,
      });
    }
  };

  return (
    <ModalShell onClose={() => selectThread(null)}>
      <div className="flex h-full min-h-0 flex-col overflow-hidden bg-background">
        <div className="flex items-start justify-between border-b border-border px-5 py-4 shrink-0">
          <div className="min-w-0">
            <h2 className="truncate text-lg font-semibold leading-tight text-foreground">{subject}</h2>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {messages.length} messagg{messages.length === 1 ? 'io' : 'i'} nella conversazione
            </p>
          </div>
          <button
            type="button"
            onClick={() => selectThread(null)}
            className="rounded-md p-1.5 text-muted transition-colors hover:bg-surface-hover hover:text-foreground"
            title="Chiudi"
          >
            <X size={16} />
          </button>
        </div>

        <div className="flex min-h-0 flex-1 overflow-hidden">
          <aside className="flex w-[300px] shrink-0 flex-col border-r border-border bg-surface">
            <div className="border-b border-border px-4 py-3 text-xs font-medium uppercase tracking-wide text-muted">
              Conversazione
            </div>
            <div className="flex-1 overflow-y-auto p-2">
              <div className="space-y-2">
                {messages.map((message) => (
                  <ThreadListItem
                    key={message.id}
                    message={message}
                    isActive={message.id === activeMessage?.id}
                    onClick={() => setActiveMessageId(message.id)}
                  />
                ))}
              </div>
            </div>
          </aside>

          <section className="flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="flex flex-wrap items-center gap-1.5 border-b border-border px-5 py-3 shrink-0">
              <ToolbarAction icon={<Reply size={14} />} label="Rispondi" onClick={() => replyTo(activeMessage)} />
              <ToolbarAction icon={<ReplyAll size={14} />} label="Rispondi a tutti" onClick={() => replyAllTo(activeMessage)} />
              <ToolbarAction icon={<Forward size={14} />} label="Inoltra" onClick={() => forwardMessage(activeMessage)} />
              <ToolbarAction
                icon={hasUnread ? <MailOpen size={14} /> : <Mail size={14} />}
                label={hasUnread ? 'Segna come letta' : 'Segna come non letta'}
                onClick={() => void markConversation(hasUnread)}
              />
            </div>

            <div className="flex min-h-0 flex-1 flex-col overflow-hidden px-5 py-4">
              <div className="mb-4 shrink-0">
                <div className="flex items-start gap-3">
                  <SenderAvatar name={activeMessage.fromName || activeMessage.fromEmail} />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-baseline gap-2">
                      <span className="truncate text-sm font-semibold text-foreground">
                        {activeMessage.fromName || activeMessage.fromEmail}
                      </span>
                      <span className="truncate text-xs text-muted">&lt;{activeMessage.fromEmail}&gt;</span>
                      {!activeMessage.isRead && <span className="h-2 w-2 shrink-0 rounded-full bg-primary" />}
                      <span className="ml-auto shrink-0 text-xs text-muted-foreground">
                        {formatDate(activeMessage.date)}
                      </span>
                    </div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      <span className="font-medium text-foreground">A:</span>{' '}
                      {activeMessage.to.length > 0 ? activeMessage.to.join(', ') : '—'}
                    </div>
                  </div>
                </div>
              </div>

              <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border bg-white shadow-sm">
                <EmailContent
                  htmlBody={activeMessage.htmlBody}
                  textBody={activeMessage.textBody}
                  className="h-full w-full border-0 bg-white"
                />
              </div>
            </div>
          </section>
        </div>
      </div>
    </ModalShell>
  );
}

function ModalShell({
  children,
  onClose,
}: {
  children: React.ReactNode;
  onClose: () => void;
}) {
  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/45 backdrop-blur-[1px]" onClick={onClose} aria-hidden />
      <div className="fixed inset-0 z-50 flex items-center justify-center p-3 pointer-events-none sm:p-4">
        <div className="pointer-events-auto h-[94vh] w-[96vw] max-w-[1520px] overflow-hidden rounded-xl border border-border bg-background shadow-2xl">
          {children}
        </div>
      </div>
    </>
  );
}

interface ThreadListItemProps {
  message: ThreadMessage;
  isActive: boolean;
  onClick: () => void;
}

function ThreadListItem({ message, isActive, onClick }: ThreadListItemProps) {
  const preview = useMemo(() => getPreview(message), [message]);

  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'w-full rounded-lg border px-3 py-3 text-left transition-colors',
        isActive
          ? 'border-accent bg-background shadow-sm'
          : 'border-border bg-surface text-foreground hover:bg-surface-hover',
      ].join(' ')}
    >
      <div className="flex items-start gap-3">
        <SenderAvatar name={message.fromName || message.fromEmail} />
        <div className="min-w-0 flex-1">
          <div className="flex items-baseline gap-2">
            <span className={message.isRead ? 'truncate text-sm font-medium text-foreground' : 'truncate text-sm font-semibold text-foreground'}>
              {message.fromName || message.fromEmail}
            </span>
            {!message.isRead && <span className="mt-0.5 h-2 w-2 shrink-0 rounded-full bg-primary" />}
          </div>
          <div className="mt-0.5 text-xs text-muted-foreground">
            {formatDate(message.date)}
          </div>
          <p className="mt-1 line-clamp-2 text-xs text-muted-foreground">
            {preview || 'Apri messaggio'}
          </p>
        </div>
      </div>
    </button>
  );
}

function SenderAvatar({ name }: { name: string }) {
  const letter = name.trim()[0]?.toUpperCase() ?? '?';
  return (
    <div className="w-8 h-8 rounded-full bg-primary/20 text-primary flex items-center justify-center text-sm font-semibold shrink-0">
      {letter}
    </div>
  );
}

function ToolbarAction({
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
      className="flex items-center gap-1.5 rounded-md border border-border bg-surface px-2.5 py-1.5 text-xs text-foreground transition-colors hover:bg-surface-hover"
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

function getPreview(message: ThreadMessage): string {
  const text = message.textBody ?? message.htmlBody?.replace(/<[^>]+>/g, ' ') ?? '';
  return text.trim().slice(0, 120).replace(/\s+/g, ' ');
}

function toAddressEntry(name: string | null, email: string): AddressEntry {
  return name ? { name, email } : { email };
}

function addressEntryFromString(entry: string): AddressEntry {
  const match = entry.match(/^(.*?)\s*<([^>]+)>\s*$/);
  if (match) {
    const name = match[1].trim().replace(/^"|"$/g, '');
    return name ? { name, email: match[2].trim() } : { email: match[2].trim() };
  }
  return { email: entry.trim() };
}

function dedupeAddresses(entries: string[]): string[] {
  const seen = new Set<string>();
  const deduped: string[] = [];

  for (const entry of entries) {
    const email = extractEmail(entry).toLowerCase();
    if (seen.has(email)) continue;
    seen.add(email);
    deduped.push(entry);
  }

  return deduped;
}

function extractEmail(entry: string): string {
  const match = entry.match(/<([^>]+)>/);
  return match?.[1] ?? entry.trim();
}
