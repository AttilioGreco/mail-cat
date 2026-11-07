import { useMemo } from 'react';
import { Plus, Search } from 'lucide-react';
import { Input } from '@/shared/ui/input';
import { Button } from '@/shared/ui/button';
import { useAccounts } from '@/entities/account';
import { useContactEditor, useContactGroups, useHarvestedContacts } from '@/entities/contact';
import { buildRows } from '../lib/group';
import { useContactsView } from '../model/contacts-store';
import { ContactDetail } from './ContactDetail';
import { ContactList } from './ContactList';

/** The Contatti section: it fills the space beside the account rail. */
export function ContactsPage() {
  const { query, selected, setQuery, select } = useContactsView();
  const groups = useContactGroups(useAccounts().data ?? []);
  const { data: harvested = [] } = useHarvestedContacts();
  const openEditor = useContactEditor((s) => s.open);

  const rows = useMemo(() => buildRows(groups, harvested, query), [groups, harvested, query]);
  const current = rows.find((r) => r.kind === 'contact' && r.key === selected);
  const hasContacts = rows.some((r) => r.kind === 'contact');
  const hasNotice = rows.some((r) => r.kind === 'account' && r.notice);

  return (
    <div className="flex min-w-0 flex-1 overflow-hidden">
      <aside className="flex w-72 shrink-0 flex-col border-r border-border bg-surface">
        <div className="flex items-center gap-1.5 border-b border-border p-2">
          <div className="relative flex-1">
            <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground" />
            <Input className="pl-7" placeholder="Cerca contatti" value={query} onChange={(e) => setQuery(e.target.value)} />
          </div>
          <Button size="icon" variant="ghost" title="Nuovo contatto" aria-label="Nuovo contatto" onClick={() => openEditor({ mode: 'create' })}>
            <Plus size={15} />
          </Button>
        </div>
        <div className="min-h-0 flex-1">
          {hasContacts || hasNotice ? (
            <ContactList rows={rows} selected={selected} onSelect={select} />
          ) : (
            <p className="p-4 text-xs text-muted-foreground">
              {query.trim() ? 'Nessun contatto trovato.' : 'Nessun contatto: nessun account ha rubriche, oppure non le supporta.'}
            </p>
          )}
        </div>
      </aside>
      <section className="min-w-0 flex-1 overflow-y-auto">
        {current?.kind === 'contact' ? (
          <ContactDetail contact={current.contact} />
        ) : (
          <p className="p-8 text-sm text-muted-foreground">Seleziona un contatto.</p>
        )}
      </section>
    </div>
  );
}
