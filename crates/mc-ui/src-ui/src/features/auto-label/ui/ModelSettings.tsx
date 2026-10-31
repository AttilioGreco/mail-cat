import { toast } from 'sonner';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/shared/ui/select';
import { useEmbeddings } from '../model/useAutoLabel';
import type { ModelOption, ModelProfile } from '../model/types';

const PROFILES: { value: ModelProfile; label: string }[] = [
  { value: 'light', label: 'Leggero' },
  { value: 'balanced', label: 'Bilanciato' },
  { value: 'quality', label: 'Qualità' },
];

const TARGETS: Record<string, string> = {
  cpu: 'Processore (CPU)',
  'gpu-cuda': 'GPU NVIDIA (CUDA)',
  'gpu-rocm': 'GPU AMD (ROCm)',
  'gpu-directml': 'GPU (DirectML)',
  'gpu-coreml': 'GPU Apple (CoreML)',
  'npu-vitisai': 'NPU AMD (Vitis AI)',
};

const megabytes = (bytes: number) => Math.round(bytes / 1e6).toLocaleString();

/** Size, memory and quality of a model, to choose by. */
export function modelHint(m: ModelOption): string {
  return `${megabytes(m.downloadBytes)} MB da scaricare · ~${megabytes(m.workerBytes)} MB di memoria per modello · qualità ${'★'.repeat(m.quality)}${'☆'.repeat(3 - m.quality)}`;
}

/** Which model embeds the mail, and on what hardware. */
export function ModelSettings() {
  const { status, setModel, setTarget } = useEmbeddings();
  if (!status?.available || status.models.length === 0) return null;
  const fail = (e: Error) => toast.error(String(e));
  const current = status.models.find((m) => m.profile === status.profile);
  const targets = status.targets.map((t) => ({ value: t.id, label: TARGETS[t.id] ?? t.id, disabled: !t.available }));

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs">
          <div className="text-foreground">Modello di analisi</div>
          <p className="text-muted">
            {current ? modelHint(current) : ''}. Cambiando modello i messaggi vengono analizzati di nuovo in background.
          </p>
        </div>
        <Select
          value={status.profile}
          items={PROFILES}
          onValueChange={(v: string | null) => v !== null && setModel.mutate(v as ModelProfile, { onError: fail })}
        >
          <SelectTrigger size="sm" className="w-36 shrink-0">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {PROFILES.map((p) => (
              <SelectItem key={p.value} value={p.value}>
                {p.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs">
          <div className="text-foreground">Dove viene eseguito</div>
          <p className="text-muted">Le opzioni grigie non sono disponibili su questa versione o su questo computer.</p>
        </div>
        <Select
          value={status.target}
          items={targets}
          onValueChange={(v: string | null) => v !== null && setTarget.mutate(v, { onError: fail })}
        >
          <SelectTrigger size="sm" className="w-44 shrink-0">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {targets.map((t) => (
              <SelectItem key={t.value} value={t.value} disabled={t.disabled}>
                {t.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  );
}
