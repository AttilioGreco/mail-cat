import { Keyboard } from 'lucide-react';
import { useShortcutsStore } from '../model/shortcuts-store';
import { PRESET_LABELS, type PresetId } from '../model/presets';
import { ShortcutList } from './ShortcutList';
import { SimpleSelect } from '@/shared/ui/select';

const PRESET_IDS: PresetId[] = ['vim', 'outlook', 'none'];

export function ShortcutsSection() {
  const { preset, setPreset } = useShortcutsStore();

  return (
    <section className="space-y-4">
      <header>
        <h2 className="text-sm font-semibold text-foreground">Scorciatoie</h2>
        <p className="text-xs text-muted">Valide solo su questo client</p>
      </header>

      <section className="rounded-lg border border-border bg-surface p-3">
        <div className="mb-3 flex items-center gap-2 text-sm font-medium text-foreground">
          <Keyboard size={15} />
          Set di scorciatoie
        </div>
        <SimpleSelect
          value={preset}
          onChange={setPreset}
          items={PRESET_IDS.map((id) => ({ value: id, label: PRESET_LABELS[id] }))}
        />
        <p className="mt-2 text-xs text-muted">
          I tasti singoli non scattano mentre scrivi in un campo di testo, quindi non interferiscono
          con la composizione. Premi F1 in qualsiasi momento per l'elenco completo.
        </p>
      </section>

      {/* Le azioni sul messaggio compaiono qui solo quando sono legate nel
          contesto corrente: in questa pagina la lista messaggi non è montata. */}
      <ShortcutList />
    </section>
  );
}
