import { useEffect, useMemo, useState } from 'react';
import { ChevronDown, Pencil, Trash2, ChevronRight, RefreshCw, AlertCircle, MailCheck, Sparkles, Settings2 } from 'lucide-react';
import { ContextMenu } from '@/shared/ui/ContextMenu';
import { useMailboxes, subscribedOnly, FolderItem, type Mailbox, type MailboxKind } from '@/entities/mailbox';
import { LabelSettingsDialog, ProposalsBadge } from '@/features/auto-label';
import { useLabelDialogs, useLabelsPage } from '@/features/manage-labels';
import { useMarkMailboxRead } from '@/features/message-actions';
import { LabelTree } from './LabelTree';

/** Gmail web's order: these first, the rest of the system mailboxes under "Altro". */
const MAIN_KINDS: MailboxKind[] = ['inbox', 'starred', 'important', 'sent', 'drafts', 'archive'];
const MORE_KINDS: MailboxKind[] = ['all', 'spam', 'trash'];

const moreKey = (accountId: string) => `sidebar.more.open.${accountId}`;

function loadMoreOpen(accountId: string): boolean {
  try {
    return localStorage.getItem(moreKey(accountId)) === '1';
  } catch {
    return false;
  }
}

export function FolderTree({
  accountId,
  labels,
  selectedMailboxId,
  onSelect,
}: {
  accountId: string;
  /** Labels account: custom mailboxes are listed apart, as labels. */
  labels: boolean;
  selectedMailboxId: string | null;
  onSelect: (mailboxId: string) => void;
}) {
  const { data: all = [], isLoading, error } = useMailboxes(accountId);
  const mailboxes = useMemo(() => subscribedOnly(all), [all]);
  const markMailboxRead = useMarkMailboxRead();
  const openManage = useLabelDialogs((s) => s.openManage);
  const openLabels = useLabelsPage((s) => s.openLabels);
  const [menu, setMenu] = useState<{ mailbox: Mailbox; x: number; y: number } | null>(null);
  const [autoLabel, setAutoLabel] = useState<Mailbox | null>(null);
  const [more, setMore] = useState(() => ({ accountId, open: loadMoreOpen(accountId) }));
  const moreOpen = more.accountId === accountId ? more.open : loadMoreOpen(accountId);
  const toggleMore = () => {
    setMore({ accountId, open: !moreOpen });
    try {
      localStorage.setItem(moreKey(accountId), moreOpen ? '0' : '1');
    } catch {
      // Per-viewer convenience only.
    }
  };

  const inbox = mailboxes.find((m) => m.kind === 'inbox') ?? null;

  // Restore the persisted one, else default to INBOX.
  useEffect(() => {
    if (mailboxes.length === 0) return;
    if (mailboxes.some((m) => m.id === selectedMailboxId)) return;
    const target = inbox ?? mailboxes[0];
    onSelect(target.id);
  }, [mailboxes, selectedMailboxId, inbox, onSelect]);

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 px-2 py-3 text-xs text-muted">
        <RefreshCw size={12} className="animate-spin" />
        Loading…
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center gap-2 px-2 py-3 text-xs text-destructive">
        <AlertCircle size={12} />
        Errore caricamento mailbox
      </div>
    );
  }

  const ofKinds = (kinds: MailboxKind[]) =>
    kinds.flatMap((kind) => mailboxes.filter((m) => m.kind === kind));
  const system = labels ? ofKinds(MAIN_KINDS) : mailboxes;
  const other = labels ? ofKinds(MORE_KINDS) : [];
  const custom = labels ? mailboxes.filter((m) => m.kind === 'custom') : [];
  const item = (mailbox: Mailbox) => (
    <FolderItem
      key={mailbox.id}
      mailbox={mailbox}
      asLabel={labels}
      isSelected={selectedMailboxId === mailbox.id}
      onSelect={() => onSelect(mailbox.id)}
      onContextMenu={(x, y) => setMenu({ mailbox, x, y })}
    />
  );

  return (
    <nav className="flex h-full min-h-0 flex-col gap-0.5 overflow-y-auto">
      {system.map(item)}
      <ProposalsBadge />
      {other.length > 0 && (
        <>
          <button
            type="button"
            onClick={toggleMore}
            aria-expanded={moreOpen}
            className="flex w-full items-center gap-2 rounded-md px-2 h-8 text-sm text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
          >
            {moreOpen ? <ChevronDown size={15} className="shrink-0" /> : <ChevronRight size={15} className="shrink-0" />}
            Altro
          </button>
          {moreOpen && other.map(item)}
        </>
      )}
      {labels && (
        <div className="flex items-center justify-between px-2 pb-1 pt-4">
          <h2 className="text-[10px] font-semibold uppercase tracking-wide text-muted">Etichette</h2>
          <button
            type="button"
            aria-label="Gestisci etichette"
            title="Gestisci etichette"
            onClick={openLabels}
            className="rounded p-0.5 text-muted transition-colors hover:bg-surface-hover hover:text-foreground"
          >
            <Settings2 size={13} />
          </button>
        </div>
      )}
      {custom.length > 0 && (
        <>
          <LabelTree
            accountId={accountId}
            labels={custom}
            selectedMailboxId={selectedMailboxId}
            onSelect={onSelect}
            onContextMenu={(mailbox, x, y) => setMenu({ mailbox, x, y })}
          />
        </>
      )}
      {menu && (
        <ContextMenu x={menu.x} y={menu.y} onClose={() => setMenu(null)}>
          <button
            type="button"
            disabled={menu.mailbox.unreadMessages === 0}
            onClick={() => {
              markMailboxRead.mutate(menu.mailbox.id);
              setMenu(null);
            }}
            className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-foreground transition-colors hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-transparent"
          >
            <MailCheck size={14} className="shrink-0" />
            Segna tutti come letti
          </button>
          {labels && menu.mailbox.kind === 'custom' && (
            <button
              type="button"
              onClick={() => {
                setAutoLabel(menu.mailbox);
                setMenu(null);
              }}
              className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-foreground transition-colors hover:bg-surface-hover"
            >
              <Sparkles size={14} className="shrink-0" />
              Etichetta automatica…
            </button>
          )}
          {labels && menu.mailbox.kind === 'custom' && (
            <>
              <button
                type="button"
                onClick={() => {
                  openManage(accountId, menu.mailbox.displayPath, 'rename');
                  setMenu(null);
                }}
                className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-foreground transition-colors hover:bg-surface-hover"
              >
                <Pencil size={14} className="shrink-0" />
                Rinomina o sposta…
              </button>
              <button
                type="button"
                onClick={() => {
                  openManage(accountId, menu.mailbox.displayPath, 'delete');
                  setMenu(null);
                }}
                className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-destructive transition-colors hover:bg-surface-hover"
              >
                <Trash2 size={14} className="shrink-0" />
                Elimina…
              </button>
            </>
          )}
        </ContextMenu>
      )}
      {autoLabel && (
        <LabelSettingsDialog
          open
          accountId={accountId}
          label={autoLabel.displayPath}
          onClose={() => setAutoLabel(null)}
        />
      )}
    </nav>
  );
}
