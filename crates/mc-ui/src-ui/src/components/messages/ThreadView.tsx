import { useState } from 'react';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { useThread } from '@/hooks/useConversations';
import { useUIStore } from '@/store/uiStore';
import { formatDate } from '@/lib/utils';
import { EmailContent } from '@/components/messages/EmailContent';
import type { ThreadMessage } from '@/lib/types';

export function ThreadView() {
  const { selectedThreadMessageId } = useUIStore();
  const { data: messages, isLoading, error } = useThread(selectedThreadMessageId);

  if (!selectedThreadMessageId) return null;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
        Loading thread…
      </div>
    );
  }

  if (error || !messages) {
    return (
      <div className="flex items-center justify-center h-full text-destructive text-sm">
        Failed to load thread.
      </div>
    );
  }

  const subject = messages[0]?.subject ?? '(no subject)';

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Thread header */}
      <div className="px-6 py-4 border-b border-border shrink-0">
        <h2 className="text-lg font-semibold text-foreground leading-tight">{subject}</h2>
        <p className="text-xs text-muted-foreground mt-0.5">{messages.length} message{messages.length !== 1 ? 's' : ''}</p>
      </div>

      {/* Messages list — newest last, like Gmail */}
      <div className="flex-1 overflow-y-auto px-4 py-3 space-y-2">
        {messages.map((msg, idx) => (
          <ThreadMessageCard
            key={msg.id}
            message={msg}
            defaultExpanded={idx === messages.length - 1}
          />
        ))}
      </div>
    </div>
  );
}

interface CardProps {
  message: ThreadMessage;
  defaultExpanded: boolean;
}

function ThreadMessageCard({ message, defaultExpanded }: CardProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  return (
    <div className="border border-border rounded-lg overflow-hidden bg-card shadow-sm">
      {/* Message header — always visible */}
      <button
        type="button"
        className="w-full flex items-start gap-3 px-4 py-3 text-left hover:bg-muted/30 transition-colors"
        onClick={() => setExpanded((v) => !v)}
      >
        <SenderAvatar name={message.fromName || message.fromEmail} />
        <div className="flex-1 min-w-0">
          <div className="flex items-baseline gap-2">
            <span className={`text-sm font-medium truncate ${message.isRead ? 'text-foreground' : 'text-foreground font-semibold'}`}>
              {message.fromName || message.fromEmail}
            </span>
            {!message.isRead && <span className="w-2 h-2 rounded-full bg-primary shrink-0" />}
            <span className="ml-auto text-xs text-muted-foreground shrink-0">
              {formatDate(message.date)}
            </span>
          </div>
          {!expanded && (
            <p className="text-xs text-muted-foreground truncate mt-0.5">
              {getPreview(message)}
            </p>
          )}
          {expanded && (
            <p className="text-xs text-muted-foreground mt-0.5">
              To: {message.to.join(', ') || '—'}
            </p>
          )}
        </div>
        <span className="text-muted-foreground shrink-0 mt-0.5">
          {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
        </span>
      </button>

      {/* Message body */}
      {expanded && (
        <div className="px-4 pb-4 pt-1 border-t border-border">
          <MessageBody message={message} />
        </div>
      )}
    </div>
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

function MessageBody({ message }: { message: ThreadMessage }) {
  return (
    <EmailContent
      htmlBody={message.htmlBody}
      textBody={message.textBody}
      className="w-full min-h-[260px] border border-border rounded-md bg-white"
    />
  );
}

function getPreview(message: ThreadMessage): string {
  const text = message.textBody ?? message.htmlBody?.replace(/<[^>]+>/g, ' ') ?? '';
  return text.trim().slice(0, 120).replace(/\s+/g, ' ');
}
