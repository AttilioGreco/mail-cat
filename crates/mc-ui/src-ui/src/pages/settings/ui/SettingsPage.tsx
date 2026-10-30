import { useState } from 'react';
import { ArrowLeft, Users, Palette, Keyboard } from 'lucide-react';
import { AccountsSection } from '@/features/manage-accounts';
import { ManageFoldersButton } from '@/features/manage-folders';
import { ShortcutsSection } from '@/features/keyboard-shortcuts';
import { AppearanceSection } from './AppearanceSection';

type Tab = 'accounts' | 'shortcuts' | 'appearance';

const TABS: { id: Tab; label: string; icon: React.ReactNode }[] = [
  { id: 'accounts', label: 'Account', icon: <Users size={14} /> },
  { id: 'shortcuts', label: 'Scorciatoie', icon: <Keyboard size={14} /> },
  { id: 'appearance', label: 'Aspetto', icon: <Palette size={14} /> },
];

/** Full-screen settings shell: section list on the left, content on the right. */
export function SettingsPage({ onClose }: { onClose: () => void }) {
  const [tab, setTab] = useState<Tab>('accounts');

  return (
    <div className="flex h-full overflow-hidden bg-background">
      <aside className="flex w-[220px] shrink-0 flex-col border-r border-border bg-surface">
        <div className="border-b border-border p-2">
          <button
            type="button"
            onClick={onClose}
            className="flex w-full items-center gap-2 rounded-md px-2.5 py-2 text-xs text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
          >
            <ArrowLeft size={14} />
            Torna alla posta
          </button>
        </div>

        <nav className="flex flex-1 flex-col gap-0.5 p-2">
          {TABS.map((t) => (
            <button
              key={t.id}
              type="button"
              onClick={() => setTab(t.id)}
              className={[
                'flex items-center gap-2 rounded-md px-2.5 py-2 text-xs transition-colors',
                tab === t.id
                  ? 'bg-surface-active text-foreground'
                  : 'text-muted-foreground hover:bg-surface-hover hover:text-foreground',
              ].join(' ')}
            >
              {t.icon}
              {t.label}
            </button>
          ))}
        </nav>
      </aside>

      <main className="flex-1 overflow-y-auto">
        <div className="mx-auto max-w-[760px] p-6">
          {tab === 'accounts' && (
            <AccountsSection
              accountActions={(a) => <ManageFoldersButton accountId={a.id} accountName={a.name} />}
            />
          )}
          {tab === 'shortcuts' && <ShortcutsSection />}
          {tab === 'appearance' && <AppearanceSection />}
        </div>
      </main>
    </div>
  );
}
