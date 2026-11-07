import type { ReactNode } from 'react';
import { PreviewCard } from '@base-ui/react/preview-card';
import { Building2, Mail, Phone } from 'lucide-react';
import { Button } from '@/shared/ui/button';
import { useAccounts } from '@/entities/account';
import { useContactEditor, useLookupContact } from '@/entities/contact';
import { useComposeStore } from '@/features/compose-message';
import { useSearchStore } from '@/features/search-messages';

interface Props {
  name: string;
  email: string;
  /** The account the message belongs to: its contacts win over another account's. */
  accountId: string | null;
  children: ReactNode;
}

/**
 * Wraps a sender or recipient name. The lookup runs only once the card is
 * open (the popup mounts then), so reading mail never queries contacts.
 */
export function ContactHover({ name, email, accountId, children }: Props) {
  return (
    <PreviewCard.Root>
      <PreviewCard.Trigger
        delay={500}
        render={<span tabIndex={0} className="rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-ring/50" />}
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </PreviewCard.Trigger>
      <PreviewCard.Portal>
        <PreviewCard.Positioner side="bottom" align="start" sideOffset={6} className="z-50">
          <PreviewCard.Popup className="w-72 rounded-lg border border-border bg-surface p-3 text-sm text-foreground shadow-lg outline-none">
            <Card name={name} email={email} accountId={accountId} />
          </PreviewCard.Popup>
        </PreviewCard.Positioner>
      </PreviewCard.Portal>
    </PreviewCard.Root>
  );
}

function Card({ name, email, accountId }: Omit<Props, 'children'>) {
  const { data: card, isLoading } = useLookupContact(email, accountId);
  const { data: accounts = [] } = useAccounts();
  const openCompose = useComposeStore((s) => s.openCompose);
  const setSearchQuery = useSearchStore((s) => s.setSearchQuery);
  const openEditor = useContactEditor((s) => s.open);

  const compose = () =>
    openCompose({ mode: 'new', accountId, to: name ? `${name} <${email}>` : email });
  const recent = () => setSearchQuery(`from:${email}`);
  const place = card?.source === 'harvested'
    ? 'Dalle mail'
    : [card?.addressBookName, accounts.find((a) => a.id === card?.accountId)?.name].filter(Boolean).join(' · ');

  return (
    <div className="flex flex-col gap-2">
      <div>
        <p className="truncate font-semibold">{card?.displayName || name || email}</p>
        {card && place && <p className="truncate text-xs text-muted-foreground">{place}</p>}
      </div>
      {card?.organization && <Line icon={<Building2 size={12} />}>{card.organization}</Line>}
      {(card?.emails ?? [email]).map((e) => (
        <Line key={e} icon={<Mail size={12} />}>{e}</Line>
      ))}
      {card?.phones.map((p) => (
        <Line key={p} icon={<Phone size={12} />}>{p}</Line>
      ))}
      <div className="mt-1 flex flex-wrap gap-1.5">
        <Button size="sm" onClick={compose}>Scrivi</Button>
        <Button size="sm" variant="outline" onClick={recent}>Ultime mail</Button>
        {!card && !isLoading && (
          <Button size="sm" variant="outline" onClick={() => openEditor({ mode: 'create', prefill: { name, email } })}>
            Aggiungi ai contatti
          </Button>
        )}
      </div>
    </div>
  );
}

function Line({ icon, children }: { icon: ReactNode; children: ReactNode }) {
  return (
    <p className="flex items-center gap-1.5 text-xs">
      <span className="text-muted-foreground">{icon}</span>
      <span className="min-w-0 break-all">{children}</span>
    </p>
  );
}
