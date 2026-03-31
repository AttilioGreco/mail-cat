import { cn, formatDate, truncate } from '@/lib/utils';
import type { ConversationSummary } from '@/lib/types';

interface Props {
  conversation: ConversationSummary;
  isSelected: boolean;
  onClick: () => void;
}

export function ConversationRow({ conversation, isSelected, onClick }: Props) {
  const isUnread = conversation.unreadCount > 0;

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => e.key === 'Enter' && onClick()}
      className={cn(
        'flex flex-col gap-0.5 px-3 py-2.5 border-b border-border cursor-pointer select-none transition-colors',
        isSelected
          ? 'bg-accent text-accent-foreground'
          : 'hover:bg-muted/50',
        isUnread && !isSelected && 'bg-muted/20',
      )}
    >
      {/* Row 1: sender + date */}
      <div className="flex items-baseline gap-2">
        <span className={cn('flex-1 truncate text-sm', isUnread ? 'font-semibold' : 'font-medium')}>
          {conversation.latestSender || '(no sender)'}
        </span>
        <span className="text-xs text-muted shrink-0">
          {formatDate(conversation.lastMessageAt)}
        </span>
      </div>

      {/* Row 2: subject + message count badge */}
      <div className="flex items-center gap-1.5">
        <span className={cn('flex-1 truncate text-sm', isUnread ? 'font-medium text-foreground' : 'text-muted-foreground')}>
          {truncate(conversation.subject || '(no subject)', 48)}
        </span>
        {conversation.messageCount > 1 && (
          <span className="text-xs text-muted shrink-0">
            {conversation.messageCount}
          </span>
        )}
        {isUnread && (
          <span className="w-2 h-2 rounded-full bg-primary shrink-0" />
        )}
      </div>
    </div>
  );
}
