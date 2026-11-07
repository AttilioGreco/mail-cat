import { Building2, Mail, Pencil, Phone, RefreshCw } from 'lucide-react';
import { Button } from '@/shared/ui/button';
import { useContactEditor } from '@/entities/contact';
import { HARVESTED_LABEL, type Listed } from '../lib/group';

export function ContactDetail({ contact }: { contact: Listed }) {
  const openEditor = useContactEditor((s) => s.open);
  const place = contact.source === 'harvested' ? HARVESTED_LABEL : `${contact.bookName} · ${contact.accountName}`;

  return (
    <div className="mx-auto flex w-full max-w-xl flex-col gap-4 p-8">
      <div className="flex items-start gap-3">
        <div className="min-w-0 flex-1">
          <h2 className="truncate text-xl font-semibold text-foreground">{contact.displayName}</h2>
          <p className="text-xs text-muted-foreground">{place}</p>
        </div>
        {contact.remote && (
          <Button variant="outline" size="sm" onClick={() => openEditor({ mode: 'edit', contact: contact.remote! })}>
            <Pencil size={13} /> Modifica
          </Button>
        )}
      </div>
      {contact.dirty && (
        <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
          <RefreshCw size={11} /> in sincronizzazione
        </p>
      )}
      {contact.pushFailed && <p className="text-xs text-destructive">Il server ha rifiutato la modifica: {contact.pushFailed}</p>}
      {contact.organization && <Line icon={<Building2 size={14} />}>{contact.organization}</Line>}
      {contact.emails.map((e) => (
        <Line key={e} icon={<Mail size={14} />}>{e}</Line>
      ))}
      {contact.phones.map((p) => (
        <Line key={p} icon={<Phone size={14} />}>{p}</Line>
      ))}
      {contact.notes && <p className="whitespace-pre-wrap text-sm text-muted-foreground">{contact.notes}</p>}
      {contact.source === 'harvested' && (
        <p className="text-xs text-muted-foreground">Contatto raccolto dalla posta: sola lettura.</p>
      )}
    </div>
  );
}

function Line({ icon, children }: { icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2 text-sm text-foreground">
      <span className="text-muted-foreground">{icon}</span>
      <span className="min-w-0 break-all">{children}</span>
    </div>
  );
}
