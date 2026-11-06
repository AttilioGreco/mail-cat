import { useState } from 'react';
import { Tag } from 'lucide-react';
import { LabelChip } from '@/shared/ui/label-chip';
import { useAccounts } from '@/entities/account';
import { useMailboxes, useLabelColors, labelChips } from '@/entities/mailbox';
import { useLabelDialogs } from '../model/dialogStore';
import { useModifyTags } from '../model/useLabelMutations';
import { labelMode } from './LabelPickerDialog';

interface MessageLabelsProps {
  accountId: string;
  messageId: string;
  mailboxIds: string[];
  /** Absent where keywords are not known (the conversation view): the tag editor is then left out. */
  tags?: string[];
  /** The mailbox on screen: not repeated as a chip. */
  viewedId: string | null;
}

/** Label chips, the button that opens the label picker and (when the server
 *  takes keywords) the tag editor of one message. */
export function MessageLabels({ accountId, messageId, mailboxIds, tags, viewedId }: MessageLabelsProps) {
  const account = useAccounts().data?.find((a) => a.id === accountId);
  const { data: mailboxes = [] } = useMailboxes(accountId);
  const colors = useLabelColors(accountId);
  const openPicker = useLabelDialogs((s) => s.openPicker);
  if (!labelMode(account)) return null;

  const chips = account?.mailboxModel === 'labels' ? labelChips(mailboxes, mailboxIds, viewedId, colors) : [];
  return (
    <div className="mt-3 flex flex-wrap items-center gap-1.5">
      {chips.map((c) => <LabelChip key={c.name} name={c.name} color={c.color} />)}
      <button
        type="button"
        aria-label="Modifica etichette"
        onClick={() => openPicker(accountId, [{ id: messageId, mailboxIds, tags: tags ?? [] }])}
        className="inline-flex items-center gap-1 rounded-full border border-dashed border-border px-1.5 py-px text-[10px] leading-4 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
      >
        <Tag size={10} />
        Etichette
      </button>
      {tags && account?.supportsTags === true && <TagEditor messageId={messageId} tags={tags} />}
    </div>
  );
}

function TagEditor({ messageId, tags }: { messageId: string; tags: string[] }) {
  const modify = useModifyTags();
  const [draft, setDraft] = useState('');

  const add = () => {
    const tag = draft.trim();
    if (!tag || tags.includes(tag)) return;
    modify.mutate({ messageIds: [messageId], add: [tag] }, { onSuccess: () => setDraft('') });
  };

  return (
    <>
      {tags.map((t) => (
        <LabelChip key={t} name={t} tag onRemove={() => modify.mutate({ messageIds: [messageId], remove: [t] })} />
      ))}
      <input
        value={draft}
        aria-label="Aggiungi tag"
        placeholder="+ tag"
        onChange={(e) => setDraft(e.target.value)}
        onKeyDown={(e) => {
          if (e.key !== 'Enter') return;
          e.preventDefault();
          add();
        }}
        className="w-16 rounded border border-transparent bg-transparent px-1 text-[10px] leading-4 outline-none placeholder:text-muted focus:w-28 focus:border-border"
      />
      {modify.error && (
        <span role="alert" className="text-[10px] text-destructive">
          {String(modify.error)}
        </span>
      )}
    </>
  );
}
