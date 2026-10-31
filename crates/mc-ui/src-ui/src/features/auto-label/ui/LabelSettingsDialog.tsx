import { useState } from 'react';
import { toast } from 'sonner';
import { Button } from '@/shared/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/shared/ui/dialog';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/shared/ui/select';
import {
  useConfirmApply,
  useLabelStatus,
  usePreviewApply,
  useSetLabelSettings,
} from '../model/useAutoLabel';
import { APPLY_MODES, THRESHOLDS, type ApplyPreview, type LabelMode } from '../model/types';

const MODES: { value: LabelMode; label: string }[] = [
  { value: 'off', label: 'Disattivata' },
  { value: 'suggest', label: 'Suggerisci' },
  { value: 'apply', label: 'Applica automaticamente' },
  { value: 'apply_archive', label: 'Applica e archivia' },
];

interface LabelSettingsDialogProps {
  open: boolean;
  onClose: () => void;
  accountId: string;
  /** The label's display path. */
  label: string;
}

/** Per-label behaviour: nothing, suggest, apply, apply and archive. Choosing
 *  an applying mode previews what it would do to existing mail first. */
export function LabelSettingsDialog({ open, onClose, accountId, label }: LabelSettingsDialogProps) {
  const { data: status, isLoading } = useLabelStatus(accountId, label, open);
  const [draft, setDraft] = useState<{ mode: LabelMode; threshold: number } | null>(null);
  const [preview, setPreview] = useState<ApplyPreview | null>(null);
  const save = useSetLabelSettings();
  const previewApply = usePreviewApply();
  const confirm = useConfirmApply();

  const mode = draft?.mode ?? status?.mode ?? 'off';
  const threshold = draft?.threshold ?? status?.threshold ?? 0.9;
  const needsExamples = !!status && status.positives < status.minPositives;
  const params = { accountId, label, mode, threshold };

  const close = () => {
    setDraft(null);
    setPreview(null);
    onClose();
  };
  const fail = (e: Error) => toast.error(String(e));

  const submit = () => {
    if (!APPLY_MODES.includes(mode)) {
      save.mutate(params, { onSuccess: close, onError: fail });
      return;
    }
    previewApply.mutate(params, { onSuccess: setPreview, onError: fail });
  };

  return (
    <Dialog open={open} onOpenChange={(o) => !o && close()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Etichetta automatica</DialogTitle>
          <DialogDescription className="truncate">{label}</DialogDescription>
        </DialogHeader>

        {isLoading || !status ? (
          <p className="text-xs text-muted">Caricamento…</p>
        ) : preview ? (
          <PreviewBody preview={preview} mode={mode} />
        ) : (
          <div className="space-y-3">
            <Field label="Comportamento">
              <Select
                value={mode}
                onValueChange={(v: string | null) => v && setDraft({ mode: v as LabelMode, threshold })}
                items={MODES}
              >
                <SelectTrigger size="sm" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {MODES.map((m) => (
                    <SelectItem key={m.value} value={m.value} disabled={m.value !== 'off' && needsExamples}>
                      {m.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>
            {APPLY_MODES.includes(mode) && (
              <Field label="Sicurezza richiesta per applicarla">
                <Select
                  value={String(threshold)}
                  onValueChange={(v: string | null) => v && setDraft({ mode, threshold: Number(v) })}
                  items={THRESHOLDS.map((t) => ({ value: String(t.value), label: t.label }))}
                >
                  <SelectTrigger size="sm" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {THRESHOLDS.map((t) => (
                      <SelectItem key={t.value} value={String(t.value)}>
                        {t.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </Field>
            )}
            {needsExamples && (
              <p className="text-xs text-muted">
                Servono altri esempi: {status.positives} di {status.minPositives} messaggi con questa etichetta.
              </p>
            )}
          </div>
        )}

        <DialogFooter>
          {preview ? (
            <>
              <Button variant="outline" onClick={() => setPreview(null)}>
                Indietro
              </Button>
              <Button
                variant="outline"
                disabled={save.isPending}
                onClick={() => save.mutate(params, { onSuccess: close, onError: fail })}
              >
                Solo ai nuovi
              </Button>
              <Button
                disabled={confirm.isPending}
                onClick={() =>
                  confirm.mutate(params, {
                    onSuccess: (p) => {
                      toast.success(`Etichettatura di ${p.toLabel} messaggi avviata`);
                      close();
                    },
                    onError: fail,
                  })
                }
              >
                Applica anche agli esistenti
              </Button>
            </>
          ) : (
            <>
              <Button variant="outline" onClick={close}>
                Annulla
              </Button>
              <Button
                disabled={!status || save.isPending || previewApply.isPending || (mode !== 'off' && needsExamples)}
                onClick={submit}
              >
                {APPLY_MODES.includes(mode) ? 'Continua' : 'Salva'}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <div className="text-xs text-muted-foreground">{label}</div>
      {children}
    </div>
  );
}

function PreviewBody({ preview, mode }: { preview: ApplyPreview; mode: LabelMode }) {
  return (
    <div className="space-y-2 text-xs">
      <p className="text-foreground">
        {preview.toLabel} messaggi esistenti verrebbero etichettati
        {mode === 'apply_archive' && `, ${preview.toArchive} archiviati`}.
      </p>
      {preview.sample.length > 0 && (
        <ul className="space-y-1 rounded-md border border-border p-2">
          {preview.sample.map((s) => (
            <li key={s.messageId} className="truncate text-muted-foreground">
              <span className="text-foreground">{s.subject || '(senza oggetto)'}</span> — {s.from}
            </li>
          ))}
        </ul>
      )}
      <p className="text-muted">Le modifiche arrivano al server a piccoli gruppi e si possono annullare dal messaggio.</p>
    </div>
  );
}
