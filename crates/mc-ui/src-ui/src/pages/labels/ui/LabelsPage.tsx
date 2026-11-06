import { useMemo, useState, type FormEvent } from 'react';
import { ArrowLeft, Plus } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Input } from '@/shared/ui/input';
import { labelKey } from '@/shared/lib/label-colors';
import { useSessionStore } from '@/entities/session';
import { useAccounts } from '@/entities/account';
import { buildLabelTree, subscribedOnly, useLabelColors, useMailboxes, type LabelNode } from '@/entities/mailbox';
import { LabelSettingsDialog, useLabelModes, useSetLabelColor } from '@/features/auto-label';
import { LabelDialogs, useCreateLabel, useLabelDialogs } from '@/features/manage-labels';
import { LabelRow } from './LabelRow';

function flatten(nodes: LabelNode[], depth = 0): { node: LabelNode; depth: number }[] {
  return nodes.flatMap((node) => [{ node, depth }, ...flatten(node.children, depth + 1)]);
}

/** Full-page management of the selected account's labels: color, size, auto-label mode,
 *  rename/move and delete. Colors and modes are shared by the account's label group. */
export function LabelsPage({ onClose }: { onClose: () => void }) {
  const accountId = useSessionStore((s) => s.selectedAccountId);
  const account = useAccounts().data?.find((a) => a.id === accountId);
  const { data: all = [] } = useMailboxes(accountId);
  const colors = useLabelColors(accountId);
  const modes = useLabelModes(accountId ?? '');
  const setColor = useSetLabelColor();
  const createLabel = useCreateLabel();
  const openManage = useLabelDialogs((s) => s.openManage);
  const [autoLabel, setAutoLabel] = useState<string | null>(null);
  const [name, setName] = useState('');

  const rows = useMemo(
    () => flatten(buildLabelTree(subscribedOnly(all).filter((m) => m.kind === 'custom'))),
    [all],
  );

  const create = (e: FormEvent) => {
    e.preventDefault();
    const path = name.trim();
    if (!accountId || !path) return;
    createLabel.mutate(
      { accountId, path },
      { onSuccess: () => setName(''), onError: (err) => toast.error(String(err)) },
    );
  };

  return (
    <div className="flex h-full flex-col overflow-hidden bg-background">
      <header className="flex items-center gap-3 border-b border-border px-4 py-2">
        <button
          type="button"
          onClick={onClose}
          className="flex items-center gap-2 rounded-md px-2.5 py-2 text-xs text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
        >
          <ArrowLeft size={14} />
          Torna alla posta
        </button>
        <h1 className="text-sm font-semibold">Etichette</h1>
        {account && <span className="truncate text-xs text-muted">{account.email}</span>}
      </header>

      <main className="flex-1 overflow-y-auto">
        <div className="mx-auto max-w-[860px] p-6">
          {account?.mailboxModel !== 'labels' ? (
            <p className="text-sm text-muted-foreground">Questo account organizza la posta in cartelle, non in etichette.</p>
          ) : (
            <>
              <form onSubmit={create} className="mb-4 flex gap-2">
                <Input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="Nome (usa / per annidare, es. Lavoro/Fatture)"
                  aria-label="Nome della nuova etichetta"
                />
                <Button type="submit" disabled={!name.trim() || createLabel.isPending}>
                  <Plus size={14} />
                  Nuova etichetta
                </Button>
              </form>
              {rows.length === 0 ? (
                <p className="text-sm text-muted-foreground">Nessuna etichetta.</p>
              ) : (
                <ul>
                  {rows.map(({ node, depth }) => (
                    <LabelRow
                      key={node.key}
                      node={node}
                      depth={depth}
                      color={node.mailbox && colors[labelKey(node.mailbox.displayPath)]}
                      mode={(node.mailbox && modes.data?.get(labelKey(node.mailbox.displayPath))) || 'off'}
                      onColor={(color) =>
                        accountId &&
                        node.mailbox &&
                        setColor.mutate(
                          { accountId, label: node.mailbox.displayPath, color },
                          { onError: (err) => toast.error(String(err)) },
                        )
                      }
                      onAuto={() => node.mailbox && setAutoLabel(node.mailbox.displayPath)}
                      onRename={() => accountId && node.mailbox && openManage(accountId, node.mailbox.displayPath, 'rename')}
                      onDelete={() => accountId && node.mailbox && openManage(accountId, node.mailbox.displayPath, 'delete')}
                    />
                  ))}
                </ul>
              )}
            </>
          )}
        </div>
      </main>

      <LabelDialogs />
      {accountId && autoLabel && (
        <LabelSettingsDialog open accountId={accountId} label={autoLabel} onClose={() => setAutoLabel(null)} />
      )}
    </div>
  );
}
