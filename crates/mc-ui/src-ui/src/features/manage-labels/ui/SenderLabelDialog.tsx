import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/shared/ui/dialog';
import { LabelChip } from '@/shared/ui/label-chip';
import { useAccounts } from '@/entities/account';
import { useMailboxes, subscribedOnly } from '@/entities/mailbox';
import { LabelCombobox, type LabelOption } from './LabelCombobox';

interface MatchReport {
  toLabel: number;
  toArchive: number;
  already: number;
  creates: boolean;
  accounts: { accountId: string; messages: number; archived: number; already: number; creates: boolean }[];
  sample: { messageId: string; subject: string; from: string }[];
}

interface SenderLabelDialogProps {
  accountId: string;
  email: string;
  onClose: () => void;
}

/** Label all the mail of one sender (or its domain), after showing how much that is. */
export function SenderLabelDialog({ accountId, email, onClose }: SenderLabelDialogProps) {
  const qc = useQueryClient();
  const accounts = useAccounts().data ?? [];
  const { data: mailboxes = [] } = useMailboxes(accountId);
  const [label, setLabel] = useState<string | null>(null);
  const [byDomain, setByDomain] = useState(false);
  const [archive, setArchive] = useState(false);

  const domain = email.split('@')[1] ?? email;
  const args = {
    label,
    from: [byDomain ? domain : email],
    listId: [],
    accountId,
    archive,
  };
  const preview = useQuery({
    queryKey: ['assign-matching', args],
    enabled: label !== null,
    queryFn: () => invoke<MatchReport>('assign_matching_mail', { ...args, dryRun: true }),
  });
  const run = useMutation({
    mutationFn: () => invoke<MatchReport>('assign_matching_mail', { ...args, dryRun: false }),
    onSuccess: (r) => {
      toast.success(`${r.toLabel} messaggi in coda: l’avanzamento è in Impostazioni → Etichette`);
      for (const key of ['messages', 'conversations', 'thread', 'search', 'mailboxes', 'background-stats'])
        qc.invalidateQueries({ queryKey: [key] });
      onClose();
    },
  });

  const options: LabelOption[] = subscribedOnly(mailboxes)
    .filter((m) => m.kind === 'custom')
    .map((m) => ({ id: m.displayPath, path: m.displayPath, state: m.displayPath === label ? 'all' : 'none' }));
  const report = preview.data;
  const accountName = (id: string) => accounts.find((a) => a.id === id)?.name ?? id;

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        className="max-h-[85vh] overflow-y-auto sm:max-w-lg"
        onKeyDownCapture={(e) => {
          if (e.key === 'Escape') onClose();
        }}
      >
        <DialogHeader>
          <DialogTitle>Etichetta le mail di questo mittente</DialogTitle>
          <DialogDescription>{byDomain ? `Tutto il dominio ${domain}` : email}</DialogDescription>
        </DialogHeader>

        {label === null ? (
          <LabelCombobox options={options} onPick={(o) => setLabel(o.id)} onCreate={setLabel} />
        ) : (
          <div className="flex items-center gap-2 text-xs">
            <LabelChip name={label} />
            <button type="button" className="text-muted underline" onClick={() => setLabel(null)}>
              Cambia
            </button>
          </div>
        )}

        <div className="space-y-1.5 text-xs text-foreground">
          <label className="flex items-center gap-2">
            <input type="checkbox" checked={byDomain} onChange={(e) => setByDomain(e.target.checked)} />
            Tutto il dominio {domain}, non solo questo indirizzo
          </label>
          <label className="flex items-center gap-2">
            <input type="checkbox" checked={archive} onChange={(e) => setArchive(e.target.checked)} />
            Togli dalla Posta in arrivo
          </label>
        </div>

        {label !== null && (
          <div aria-live="polite" className="space-y-2 text-xs">
            {preview.isPending && <p className="text-muted">Conto i messaggi…</p>}
            {preview.error && <p className="text-destructive">{String(preview.error)}</p>}
            {report && (
              <>
                <p className="text-foreground">
                  {report.toLabel} da etichettare
                  {report.already > 0 && `, ${report.already} già etichettati`}
                  {archive && `, ${report.toArchive} tolti dalla Posta in arrivo`}.
                  {report.creates && ' L’etichetta verrà creata dove manca.'}
                </p>
                {report.accounts.length > 1 && (
                  <ul className="space-y-0.5 text-muted">
                    {report.accounts.map((a) => (
                      <li key={a.accountId}>
                        {accountName(a.accountId)}: {a.messages}
                        {archive && ` (${a.archived} in archivio)`}
                      </li>
                    ))}
                  </ul>
                )}
                {report.sample.length > 0 && (
                  <ul className="space-y-0.5 rounded-lg border border-border p-2 text-muted">
                    {report.sample.map((s) => (
                      <li key={s.messageId} className="truncate">
                        {s.subject || '(senza oggetto)'} — {s.from}
                      </li>
                    ))}
                  </ul>
                )}
              </>
            )}
          </div>
        )}
        {run.error && <p role="alert" className="text-xs text-destructive">{String(run.error)}</p>}

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Annulla
          </Button>
          <Button disabled={!report || report.toLabel === 0 || run.isPending} onClick={() => run.mutate()}>
            {report ? `Etichetta ${report.toLabel} messaggi` : 'Etichetta'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
