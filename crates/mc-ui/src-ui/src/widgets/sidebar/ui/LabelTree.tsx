import { useEffect, useMemo, useState, type KeyboardEvent } from 'react';
import { ChevronRight, Tag } from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import { ancestorKeys, buildLabelTree, visibleRows, type LabelRow, type Mailbox } from '@/entities/mailbox';

const storageKey = (accountId: string) => `sidebar.labels.expanded.${accountId}`;

function loadExpanded(accountId: string): Set<string> {
  try {
    const raw = JSON.parse(localStorage.getItem(storageKey(accountId)) ?? '[]');
    return new Set(Array.isArray(raw) ? raw.filter((k) => typeof k === 'string') : []);
  } catch {
    return new Set();
  }
}

function saveExpanded(accountId: string, expanded: Set<string>) {
  try {
    localStorage.setItem(storageKey(accountId), JSON.stringify([...expanded]));
  } catch {
    // Per-viewer convenience only; the tree works without it.
  }
}

interface LabelTreeProps {
  accountId: string;
  labels: Mailbox[];
  selectedMailboxId: string | null;
  onSelect: (mailboxId: string) => void;
  onContextMenu: (mailbox: Mailbox, x: number, y: number) => void;
}

export function LabelTree({ accountId, labels, selectedMailboxId, onSelect, onContextMenu }: LabelTreeProps) {
  const roots = useMemo(() => buildLabelTree(labels), [labels]);
  const [state, setState] = useState(() => ({ accountId, expanded: loadExpanded(accountId) }));
  const expanded = state.accountId === accountId ? state.expanded : loadExpanded(accountId);
  const selectedPath = labels.find((m) => m.id === selectedMailboxId)?.displayPath;

  const update = (next: Set<string>) => {
    setState({ accountId, expanded: next });
    saveExpanded(accountId, next);
  };
  const toggle = (key: string, open: boolean) => {
    const next = new Set(expanded);
    if (open) next.add(key);
    else next.delete(key);
    update(next);
  };

  // The branch holding the selected label opens whenever the selection moves into it.
  useEffect(() => {
    if (!selectedPath) return;
    const missing = ancestorKeys(selectedPath.split('/').filter(Boolean).join('/')).filter((k) => !expanded.has(k));
    if (missing.length > 0) update(new Set([...expanded, ...missing]));
    // eslint-disable-next-line react-hooks/exhaustive-deps -- only a selection change should force-open
  }, [selectedPath, accountId]);

  const rows = visibleRows(roots, expanded);

  const onKeyDown = (e: KeyboardEvent<HTMLElement>, row: LabelRow, index: number) => {
    const { node } = row;
    const isOpen = expanded.has(node.key);
    const focusRow = (i: number) =>
      (e.currentTarget.parentElement?.querySelectorAll<HTMLElement>('[role=treeitem]')[i])?.focus();
    switch (e.key) {
      case 'ArrowDown':
        focusRow(Math.min(index + 1, rows.length - 1));
        break;
      case 'ArrowUp':
        focusRow(Math.max(index - 1, 0));
        break;
      case 'Home':
        focusRow(0);
        break;
      case 'End':
        focusRow(rows.length - 1);
        break;
      case 'ArrowRight':
        if (node.children.length === 0) return;
        if (isOpen) focusRow(index + 1);
        else toggle(node.key, true);
        break;
      case 'ArrowLeft':
        if (isOpen) toggle(node.key, false);
        else if (row.parentKey) focusRow(rows.findIndex((r) => r.node.key === row.parentKey));
        break;
      case 'Enter':
      case ' ':
        activate(row);
        break;
      default:
        return;
    }
    e.preventDefault();
  };

  const activate = ({ node }: LabelRow) => {
    if (node.mailbox) onSelect(node.mailbox.id);
    else toggle(node.key, !expanded.has(node.key));
  };

  return (
    <div role="tree" aria-label="Etichette" className="flex flex-col gap-0.5">
      {rows.map((row, i) => {
        const { node, depth } = row;
        const hasChildren = node.children.length > 0;
        const isOpen = expanded.has(node.key);
        const isSelected = !!node.mailbox && node.mailbox.id === selectedMailboxId;
        const unread = hasChildren && isOpen ? (node.mailbox?.unreadMessages ?? 0) : node.unreadTotal;
        return (
          <div
            key={node.key}
            role="treeitem"
            aria-level={depth + 1}
            aria-selected={isSelected}
            aria-expanded={hasChildren ? isOpen : undefined}
            tabIndex={isSelected || (!selectedMailboxId && i === 0) ? 0 : -1}
            onClick={() => activate(row)}
            onKeyDown={(e) => onKeyDown(e, row, i)}
            onContextMenu={(e) => {
              if (!node.mailbox) return;
              e.preventDefault();
              onContextMenu(node.mailbox, e.clientX, e.clientY);
            }}
            style={{ paddingLeft: 10 + depth * 14 }}
            className={cn(
              'flex w-full cursor-default items-center gap-1.5 rounded-md py-1.5 pr-2.5 text-sm outline-none transition-colors focus-visible:ring-1 focus-visible:ring-accent',
              isSelected ? 'bg-accent/15 text-accent' : 'text-muted-foreground hover:bg-surface-hover hover:text-foreground',
            )}
          >
            <span
              className="flex h-4 w-4 shrink-0 items-center justify-center"
              onClick={(e) => {
                if (!hasChildren) return;
                e.stopPropagation();
                toggle(node.key, !isOpen);
              }}
            >
              {hasChildren && <ChevronRight size={12} className={cn('transition-transform', isOpen && 'rotate-90')} />}
            </span>
            <Tag size={15} className="shrink-0" />
            <span className="flex-1 truncate text-left">{node.segment}</span>
            {unread > 0 && (
              <span
                className={cn(
                  'min-w-[18px] rounded-full px-1.5 py-0.5 text-center text-[10px] font-semibold leading-none tabular-nums',
                  isSelected ? 'bg-accent text-accent-foreground' : 'bg-surface-active text-foreground',
                )}
              >
                {unread > 99 ? '99+' : unread}
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}
