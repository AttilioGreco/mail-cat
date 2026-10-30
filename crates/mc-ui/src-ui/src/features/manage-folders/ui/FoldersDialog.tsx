import { AlertCircle, RefreshCw, X } from 'lucide-react';
import { useMemo } from 'react';
import { buildLabelTree, useMailboxes, type LabelNode } from '@/entities/mailbox';
import { useRefreshFolders, useSetSubscribed } from '../model/useSubscription';

interface FoldersDialogProps {
  open: boolean;
  accountId: string;
  accountName: string;
  onClose: () => void;
}

/** Thunderbird's Subscribe dialog: every mailbox of the account; the checked
 *  ones are shown in the sidebar and synced, the others hold no local mail. */
export function FoldersDialog({ open, accountId, accountName, onClose }: FoldersDialogProps) {
  const { data: mailboxes = [], isLoading, error } = useMailboxes(open ? accountId : null);
  const setSubscribed = useSetSubscribed(accountId);
  const refresh = useRefreshFolders();
  const roots = useMemo(() => buildLabelTree(mailboxes), [mailboxes]);

  if (!open) return null;

  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/35" onClick={onClose} aria-hidden />
      <section
        role="dialog"
        aria-modal
        aria-label="Gestisci cartelle"
        className="fixed left-1/2 top-1/2 z-50 flex max-h-[85vh] w-[480px] max-w-[95vw] -translate-x-1/2 -translate-y-1/2 flex-col rounded-xl border border-border bg-background shadow-2xl"
      >
        <header className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="min-w-0">
            <h2 className="text-sm font-semibold text-foreground">Gestisci cartelle</h2>
            <p className="truncate text-xs text-muted">{accountName}</p>
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            <button
              type="button"
              title="Aggiorna l’elenco dal server"
              disabled={refresh.isPending}
              onClick={() => refresh.mutate(accountId)}
              className="rounded border border-border bg-background p-1.5 text-muted hover:bg-surface-hover hover:text-foreground disabled:opacity-50"
            >
              <RefreshCw size={13} className={refresh.isPending ? 'animate-spin' : undefined} />
            </button>
            <button
              type="button"
              onClick={onClose}
              title="Chiudi"
              className="rounded-md p-1 text-muted hover:bg-surface-hover hover:text-foreground"
            >
              <X size={16} />
            </button>
          </div>
        </header>

        <p className="px-4 pt-3 text-[11px] text-muted">
          Le cartelle deselezionate non vengono scaricate né mostrate; i messaggi già scaricati che stanno solo
          lì vengono tolti da questo computer. La scelta vale anche per gli altri programmi di posta.
        </p>

        <div className="min-h-0 flex-1 overflow-y-auto px-2 py-3">
          {isLoading && <p className="px-2 text-xs text-muted">Caricamento…</p>}
          {error && (
            <p className="flex items-center gap-2 px-2 text-xs text-destructive">
              <AlertCircle size={12} />
              {String(error)}
            </p>
          )}
          <ul role="tree" aria-label="Cartelle">
            {roots.map((node) => (
              <FolderNode
                key={node.key}
                node={node}
                depth={0}
                disabled={setSubscribed.isPending}
                onToggle={(mailboxId, subscribed) => setSubscribed.mutate({ mailboxId, subscribed })}
              />
            ))}
          </ul>
        </div>

        {setSubscribed.isError && (
          <p role="alert" className="border-t border-border px-4 py-2 text-xs text-destructive">
            {String(setSubscribed.error)}
          </p>
        )}
      </section>
    </>
  );
}

function FolderNode({
  node,
  depth,
  disabled,
  onToggle,
}: {
  node: LabelNode;
  depth: number;
  disabled: boolean;
  onToggle: (mailboxId: string, subscribed: boolean) => void;
}) {
  const { mailbox } = node;
  return (
    <li role="treeitem" aria-expanded={node.children.length > 0 ? true : undefined}>
      <div className="flex items-center gap-2 rounded px-2 py-1 text-xs hover:bg-surface-hover" style={{ paddingLeft: 8 + depth * 16 }}>
        {mailbox ? (
          <label className="flex min-w-0 flex-1 cursor-pointer items-center gap-2">
            <input
              type="checkbox"
              checked={mailbox.subscribed}
              // The Inbox is always subscribed.
              disabled={disabled || mailbox.kind === 'inbox'}
              onChange={(e) => onToggle(mailbox.id, e.target.checked)}
            />
            <span className="truncate text-foreground">{node.segment}</span>
          </label>
        ) : (
          // A parent no mailbox exists for: it only groups its children.
          <span className="truncate pl-5 text-muted">{node.segment}</span>
        )}
      </div>
      {node.children.length > 0 && (
        <ul role="group">
          {node.children.map((child) => (
            <FolderNode key={child.key} node={child} depth={depth + 1} disabled={disabled} onToggle={onToggle} />
          ))}
        </ul>
      )}
    </li>
  );
}
