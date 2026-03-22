import {
  Inbox,
  Send,
  FileEdit,
  Trash2,
  Archive,
  AlertOctagon,
  Folder,
  type LucideIcon,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import type { Mailbox } from '@/lib/types';

const ATTR_ICONS: [string, LucideIcon][] = [
  ['\\Sent', Send],
  ['\\Drafts', FileEdit],
  ['\\Trash', Trash2],
  ['\\Junk', AlertOctagon],
  ['\\Archive', Archive],
];

const NAME_ICONS: Record<string, LucideIcon> = {
  INBOX: Inbox,
  Inbox: Inbox,
  Archive: Archive,
  Drafts: FileEdit,
  Sent: Send,
  Trash: Trash2,
  Spam: AlertOctagon,
};

function getIcon(mailbox: Mailbox): LucideIcon {
  for (const [attr, Icon] of ATTR_ICONS) {
    if (mailbox.attributes.includes(attr)) return Icon;
  }
  return NAME_ICONS[mailbox.name] ?? NAME_ICONS[mailbox.path] ?? Folder;
}

interface FolderItemProps {
  mailbox: Mailbox;
  isSelected: boolean;
  onSelect: () => void;
}

export function FolderItem({ mailbox, isSelected, onSelect }: FolderItemProps) {
  const Icon = getIcon(mailbox);

  return (
    <button
      onClick={onSelect}
      className={cn(
        'flex w-full items-center gap-2.5 rounded-md px-2.5 py-1.5 text-sm transition-colors',
        isSelected
          ? 'bg-accent/15 text-accent'
          : 'text-muted-foreground hover:bg-surface-hover hover:text-foreground',
      )}
    >
      <Icon size={15} className="shrink-0" />
      <span className="flex-1 truncate text-left">{mailbox.name}</span>
      {mailbox.unreadMessages > 0 && (
        <span
          className={cn(
            'min-w-[18px] rounded-full px-1.5 py-0.5 text-center text-[10px] font-semibold leading-none tabular-nums',
            isSelected
              ? 'bg-accent text-accent-foreground'
              : 'bg-surface-active text-foreground',
          )}
        >
          {mailbox.unreadMessages > 99 ? '99+' : mailbox.unreadMessages}
        </span>
      )}
    </button>
  );
}
