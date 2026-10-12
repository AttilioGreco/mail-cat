import { useState } from 'react';
import { Input } from '@/shared/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/shared/ui/select';
import { RecipientField } from './RecipientField';
import type { Identity } from '../model/composeTypes';

export interface Recipients {
  to: string[];
  cc: string[];
  bcc: string[];
}

interface ComposeHeaderProps {
  identities: Identity[];
  accountId: string;
  onAccountChange: (accountId: string) => void;
  recipients: Recipients;
  onRecipientsChange: (recipients: Recipients) => void;
  subject: string;
  onSubjectChange: (subject: string) => void;
  focusRecipients: boolean;
}

const identityLabel = (i: Identity) => `${i.displayName} <${i.email}>`;

export function ComposeHeader({
  identities, accountId, onAccountChange,
  recipients, onRecipientsChange,
  subject, onSubjectChange, focusRecipients,
}: ComposeHeaderProps) {
  const [showCcBcc, setShowCcBcc] = useState(recipients.cc.length > 0 || recipients.bcc.length > 0);
  const set = (field: keyof Recipients) => (values: string[]) =>
    onRecipientsChange({ ...recipients, [field]: values });

  return (
    <div className="flex shrink-0 flex-col border-b border-border">
      <div className="flex items-center gap-2 border-b border-border px-4 py-1">
        <span className="w-14 shrink-0 text-xs text-muted">Da</span>
        <Select
          value={accountId}
          onValueChange={(v: string | null) => v && onAccountChange(v)}
          items={identities.map((i) => ({ value: i.accountId, label: identityLabel(i) }))}
        >
          <SelectTrigger size="sm" className="h-7 border-0 bg-transparent px-0 text-xs shadow-none focus-visible:ring-0 dark:bg-transparent dark:hover:bg-transparent">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {identities.map((i) => (
              <SelectItem key={i.accountId} value={i.accountId} className="text-xs">
                {identityLabel(i)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <RecipientField label="A" values={recipients.to} onChange={set('to')} accountId={accountId} autoFocus={focusRecipients} />

      {showCcBcc ? (
        <>
          <RecipientField label="Cc" values={recipients.cc} onChange={set('cc')} accountId={accountId} />
          <RecipientField label="Ccn" values={recipients.bcc} onChange={set('bcc')} accountId={accountId} />
        </>
      ) : (
        <div className="border-b border-border px-4 py-1.5">
          <button
            type="button"
            onClick={() => setShowCcBcc(true)}
            className="text-xs text-muted transition-colors hover:text-foreground"
          >
            + Cc / Ccn
          </button>
        </div>
      )}

      <div className="flex items-center gap-2 px-4 py-1">
        <span className="w-14 shrink-0 text-xs text-muted">Oggetto</span>
        <Input
          value={subject}
          onChange={(e) => onSubjectChange(e.target.value)}
          placeholder="Oggetto..."
          className="h-7 border-0 bg-transparent px-0 text-sm font-medium shadow-none focus-visible:ring-0 placeholder:text-muted dark:bg-transparent"
        />
      </div>
    </div>
  );
}
