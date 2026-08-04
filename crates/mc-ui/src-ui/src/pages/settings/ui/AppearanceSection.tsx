import { Monitor, Moon, Sun, Palette } from 'lucide-react';
import { useThemeStore } from '@/entities/theme';

/** Client-local look & feel. Moved here from the old settings modal. */
export function AppearanceSection() {
  const { appTheme, setAppTheme, mailContentMode, setMailContentMode } = useThemeStore();

  return (
    <section className="space-y-4">
      <header>
        <h2 className="text-sm font-semibold text-foreground">Aspetto</h2>
        <p className="text-xs text-muted">Valide solo su questo client</p>
      </header>

      <section className="rounded-lg border border-border bg-surface p-3">
        <div className="mb-3 flex items-center gap-2 text-sm font-medium text-foreground">
          <Palette size={15} />
          Tema App
        </div>

        <div className="grid grid-cols-3 gap-2">
          <ThemeButton
            label="Sistema"
            icon={<Monitor size={14} />}
            active={appTheme === 'system'}
            onClick={() => setAppTheme('system')}
          />
          <ThemeButton
            label="Scuro"
            icon={<Moon size={14} />}
            active={appTheme === 'dark'}
            onClick={() => setAppTheme('dark')}
          />
          <ThemeButton
            label="Chiaro"
            icon={<Sun size={14} />}
            active={appTheme === 'light'}
            onClick={() => setAppTheme('light')}
          />
        </div>
      </section>

      <section className="rounded-lg border border-border bg-surface p-3">
        <div className="mb-2 text-sm font-medium text-foreground">Lettura Email HTML</div>
        <p className="mb-3 text-xs text-muted">
          Quando il tema app e scuro, alcune email con sfondo neutro possono risultare poco
          leggibili.
        </p>
        <select
          value={mailContentMode}
          onChange={(e) => setMailContentMode(e.target.value as 'auto' | 'light' | 'invert')}
          className="w-full rounded border border-border bg-background px-2.5 py-2 text-sm text-foreground"
        >
          <option value="light">Chiaro (consigliato)</option>
          <option value="auto">Auto</option>
          <option value="invert">Inverti contenuto</option>
        </select>
      </section>
    </section>
  );
}

function ThemeButton({
  label,
  icon,
  active,
  onClick,
}: {
  label: string;
  icon: React.ReactNode;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'flex items-center justify-center gap-1.5 rounded border px-3 py-2 text-sm transition-colors',
        active
          ? 'border-accent bg-accent/10 text-foreground'
          : 'border-border bg-background text-muted hover:bg-surface-hover hover:text-foreground',
      ].join(' ')}
    >
      {icon}
      {label}
    </button>
  );
}
