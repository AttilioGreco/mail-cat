import { Mail, RefreshCw } from 'lucide-react';
import { useUIStore, mailboxKey } from '@/store/uiStore';
import { useMessages, useMessageBody } from '@/hooks/useMessages';
import { MessageViewer } from '@/components/messages/MessageViewer';
import { ThreadView } from '@/components/messages/ThreadView';

export function MessagePane() {
  const { selectedAccountId, selectedMailbox, selectedMessageId, selectedThreadMessageId, searchQuery, conversationMode } = useUIStore();
  const key = mailboxKey(selectedAccountId, selectedMailbox?.id ?? null);
  const { data: messages = [] } = useMessages(conversationMode ? null : key);

  // Thread mode: show ThreadView when a conversation is selected
  if (conversationMode && selectedThreadMessageId) {
    return <ThreadView />;
  }

  const isSearching = searchQuery !== null && searchQuery.trim().length > 0;
  const envelope = !isSearching
    ? (messages.find((m) => m.uid === selectedMessageId) ?? null)
    : null;

  if (!selectedMessageId && !selectedThreadMessageId) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
        <Mail size={40} strokeWidth={1.2} />
        <span className="text-sm">{conversationMode ? 'Select a conversation' : 'Select a message to read'}</span>
      </div>
    );
  }

  // In search mode, show a simplified body-only viewer
  if (isSearching && !envelope && selectedMessageId) {
    return <SearchBodyViewer messageId={selectedMessageId} />;
  }

  if (!envelope) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
        <Mail size={40} strokeWidth={1.2} />
        <span className="text-sm">Select a message to read</span>
      </div>
    );
  }

  return <MessageViewer envelope={envelope} />;
}

function SearchBodyViewer({ messageId }: { messageId: string }) {
  const { data: body, isLoading } = useMessageBody(messageId);

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center gap-2 text-sm text-muted">
        <RefreshCw size={14} className="animate-spin" />
        Loading message...
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-border px-4 py-2.5">
        <span className="text-xs font-mono text-muted">{messageId.slice(0, 12)}...</span>
      </div>
      <div className="flex-1 overflow-y-auto px-6 py-5">
        {body?.htmlBody ? (
          <div
            className="prose prose-sm max-w-none text-foreground"
            dangerouslySetInnerHTML={{ __html: body.htmlBody }}
          />
        ) : (
          <pre className="whitespace-pre-wrap font-sans text-sm leading-relaxed text-foreground">
            {body?.textBody ?? '(No content)'}
          </pre>
        )}
      </div>
    </div>
  );
}
