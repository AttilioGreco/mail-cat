import { X, Monitor, Moon, Sun, Palette } from 'lucide-react';
import { useThemeStore } from '@/store/themeStore';

interface LocalSettingsPanelProps {
  open: boolean;
  onClose: () => void;
}

export function LocalSettingsPanel({ open, onClose }: LocalSettingsPanelProps) {
  const { appTheme, setAppTheme, mailContentMode, setMailContentMode } = useThemeStore();

  if (!open) return null;

  return (
    <>
      <div className="fixed inset-0 z-40 bg-black/35" onClick={onClose} aria-hidden />

      <section
        role="dialog"
        aria-modal
        aria-label="Impostazioni locali"
        className="fixed left-1/2 top-1/2 z-50 w-[560px] max-w-[95vw] -translate-x-1/2 -translate-y-1/2 overflow-hidden rounded-xl border border-border bg-background shadow-2xl"
      >
        <header className="flex items-center justify-between border-b border-border px-4 py-3">
          <div>
            <h2 className="text-sm font-semibold text-foreground">Impostazioni Locali</h2>
            <p className="text-xs text-muted">Valide solo su questo client</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md p-1 text-muted hover:bg-surface-hover hover:text-foreground"
            title="Chiudi"
          >
            <X size={16} />
          </button>
        </header>

        <div className="space-y-5 p-4">
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
              Quando il tema app e scuro, alcune email con sfondo neutro possono risultare poco leggibili.
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
        </div>
      </section>
    </>
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
