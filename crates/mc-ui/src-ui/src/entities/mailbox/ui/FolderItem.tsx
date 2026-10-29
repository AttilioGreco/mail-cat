import {
  Inbox,
  Send,
  FileEdit,
  Trash2,
  Archive,
  AlertOctagon,
  Folder,
  Tag,
  type LucideIcon,
} from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import type { Mailbox } from '../model/types';

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

function renderIcon(mailbox: Mailbox, asLabel: boolean) {
  for (const [attr, Icon] of ATTR_ICONS) {
    if (mailbox.attributes.includes(attr)) return <Icon size={15} className="shrink-0" />;
  }
  const Icon =
    NAME_ICONS[mailbox.name] ?? NAME_ICONS[mailbox.displayPath] ?? (asLabel && mailbox.kind === 'custom' ? Tag : Folder);
  return <Icon size={15} className="shrink-0" />;
}

interface FolderItemProps {
  mailbox: Mailbox;
  isSelected: boolean;
  onSelect: () => void;
  /** Labels account: a custom mailbox is shown as a label. */
  asLabel?: boolean;
  onContextMenu?: (x: number, y: number) => void;
}

export function FolderItem({ mailbox, isSelected, onSelect, asLabel = false, onContextMenu }: FolderItemProps) {
  return (
    <button
      onClick={onSelect}
      onContextMenu={(e) => {
        if (!onContextMenu) return;
        e.preventDefault();
        onContextMenu(e.clientX, e.clientY);
      }}
      className={cn(
        'flex w-full items-center gap-2.5 rounded-md px-2.5 py-1.5 text-sm transition-colors',
        isSelected
          ? 'bg-accent/15 text-accent'
          : 'text-muted-foreground hover:bg-surface-hover hover:text-foreground',
      )}
    >
      {renderIcon(mailbox, asLabel)}
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
