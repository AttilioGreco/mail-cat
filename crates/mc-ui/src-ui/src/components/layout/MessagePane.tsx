import { Mail } from 'lucide-react';
import { useUIStore, mailboxKey } from '@/store/uiStore';
import { useMessages } from '@/hooks/useMessages';
import { MessageViewer } from '@/components/messages/MessageViewer';

export function MessagePane() {
  const { selectedAccountId, selectedMailbox, selectedMessageId } = useUIStore();
  const key = mailboxKey(selectedAccountId, selectedMailbox?.id ?? null);
  const { data: messages = [] } = useMessages(key);

  const envelope = messages.find((m) => m.uid === selectedMessageId) ?? null;

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
