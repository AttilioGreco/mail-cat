import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/shared/ui/dialog';
import { Input } from '@/shared/ui/input';
import { useAccounts } from '@/entities/account';
import { useMailboxes, subscribedOnly } from '@/entities/mailbox';

interface LabelChange {
  label: string;
  into: string | null;
  merge: boolean;
  messages: number;
  accounts: {
    accountId: string;
    kind: string;
    messages: number;
    merges: boolean;
    rehomed: number;
    mailboxes: { label: string; into: string | null; how: string; messages: number }[];
  }[];
}

const HOW: Record<string, string> = {
  rename: 'rinomina',
  merge: 'unisce',
  'create+move': 'crea e sposta',
  delete: 'elimina',
};

interface ManageLabelDialogProps {
  accountId: string;
  label: string;
  mode: 'rename' | 'delete';
  onClose: () => void;
}

/** Rename, move, merge or delete a label on every account of its group: shows the
 *  server's dry-run plan first, then runs it (in the background on the server). */
export function ManageLabelDialog({ accountId, label, mode, onClose }: ManageLabelDialogProps) {
  const qc = useQueryClient();
  const accounts = useAccounts().data ?? [];
  const { data: mailboxes = [] } = useMailboxes(accountId);
  const [target, setTarget] = useState(label);
  const [debounced, setDebounced] = useState(label);
  const [mailOnly, setMailOnly] = useState(false);
  const renaming = mode === 'rename';

  useEffect(() => {
    const t = setTimeout(() => setDebounced(target.trim()), 300);
    return () => clearTimeout(t);
  }, [target]);

  const unchanged = debounced === '' || debounced === label;
  const args = renaming ? { from: label, to: debounced, mailOnly } : { label };
  const command = renaming ? 'rename_label' : 'delete_label';
  const preview = useQuery({
    queryKey: [command, args],
    enabled: !renaming || !unchanged,
    retry: false,
    queryFn: () => invoke<LabelChange>(command, { ...args, dryRun: true }),
  });
  const run = useMutation({
    mutationFn: () => invoke<LabelChange>(command, { ...args, dryRun: false }),
    onSuccess: (r) => {
      toast.success(`${r.messages} messaggi in lavorazione: l’elenco si aggiorna al termine`);
      const refresh = () => {
        for (const key of ['messages', 'conversations', 'thread', 'search', 'mailboxes', 'background-stats', 'label-colors'])
          qc.invalidateQueries({ queryKey: [key] });
      };
      refresh();
      // The server runs the change in the background and no event announces the
      // end, so refetch a few times while it lands.
      // ponytail: fixed polling, replace with a completion event if the server grows one.
      let n = 0;
      const t = setInterval(() => (++n >= 10 ? clearInterval(t) : refresh()), 3000);
      onClose();
    },
  });

  const report = renaming && unchanged ? undefined : preview.data;
  const accountName = (id: string) => accounts.find((a) => a.id === id)?.name ?? id;
  const rehomed = report?.accounts.reduce((n, a) => n + a.rehomed, 0) ?? 0;
  const error = run.error ?? preview.error;

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{renaming ? 'Rinomina o sposta etichetta' : 'Elimina etichetta'}</DialogTitle>
          <DialogDescription>
            {renaming ? 'Scrivi il percorso completo, ad esempio Archivio/Vecchie.' : `«${label}»: le mail restano.`}
          </DialogDescription>
        </DialogHeader>

        {renaming && (
          <>
            <Input
              autoFocus
              list="manage-label-paths"
              value={target}
              onChange={(e) => setTarget(e.target.value)}
              aria-label="Nuovo percorso"
            />
            <datalist id="manage-label-paths">
              {subscribedOnly(mailboxes)
                .filter((m) => m.kind === 'custom' && m.displayPath !== label)
                .map((m) => (
                  <option key={m.id} value={m.displayPath} />
                ))}
            </datalist>
            <label className="flex items-center gap-2 text-xs text-foreground">
              <input type="checkbox" checked={mailOnly} onChange={(e) => setMailOnly(e.target.checked)} />
              Sposta solo le mail di questa etichetta, mantieni l’etichetta e le sottoetichette
            </label>
          </>
        )}

        <div aria-live="polite" className="space-y-2 text-xs">
          {preview.isFetching && <p className="text-muted">Calcolo le modifiche…</p>}
          {report && (
            <>
              {report.merge && (
                <p role="alert" className="rounded-md border border-destructive/40 bg-destructive/10 p-2 text-destructive">
                  Attenzione: «{report.label}» verrà UNITA all’etichetta esistente «{report.into}».
                </p>
              )}
              <p className="text-foreground">
                {report.messages} messaggi coinvolti
                {rehomed > 0 && `, ${rehomed} finiranno in Archivio perché non hanno altre etichette`}.
              </p>
              <ul className="space-y-1.5 text-muted">
                {report.accounts.map((a) => (
                  <li key={a.accountId}>
                    {accountName(a.accountId)}: {a.messages} messaggi
                    {a.merges && ' (unione)'}
                    <ul className="pl-3">
                      {a.mailboxes.map((m) => (
                        <li key={m.label} className="truncate">
                          {HOW[m.how] ?? m.how} {m.label}
                          {m.into && ` → ${m.into}`} ({m.messages})
                        </li>
                      ))}
                    </ul>
                  </li>
                ))}
              </ul>
            </>
          )}
          {error && <p role="alert" className="text-destructive">{String(error)}</p>}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Annulla
          </Button>
          <Button
            variant={renaming && !report?.merge ? 'default' : 'destructive'}
            disabled={!report || preview.isFetching || run.isPending}
            onClick={() => run.mutate()}
          >
            {!renaming ? 'Elimina' : report?.merge ? 'Unisci' : 'Rinomina'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
