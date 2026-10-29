import { useState } from 'react';
import { Tag } from 'lucide-react';
import { LabelChip } from '@/shared/ui/label-chip';
import { Popover, PopoverContent, PopoverTrigger } from '@/shared/ui/popover';
import { useAccounts } from '@/entities/account';
import { useMailboxes, labelNames, type Mailbox } from '@/entities/mailbox';
import { useChangeLabels, useModifyTags } from '../model/useLabelMutations';

interface MessageLabelsProps {
  accountId: string;
  messageId: string;
  mailboxIds: string[];
  tags: string[];
  /** The mailbox on screen: not repeated as a chip. */
  viewedId: string | null;
}

/** Label chips, the label picker and (when the server takes keywords) the tag
 *  editor of one message. Renders nothing on a Folders account. */
export function MessageLabels({ accountId, messageId, mailboxIds, tags, viewedId }: MessageLabelsProps) {
  const account = useAccounts().data?.find((a) => a.id === accountId);
  const { data: mailboxes = [] } = useMailboxes(accountId);
  if (account?.mailboxModel !== 'labels') return null;

  const chips = labelNames(mailboxes, mailboxIds, viewedId);
  return (
    <div className="mt-3 flex flex-wrap items-center gap-1.5">
      {chips.map((name) => <LabelChip key={name} name={name} />)}
      <LabelPicker messageId={messageId} mailboxes={mailboxes} mailboxIds={mailboxIds} />
      {account.supportsTags === true && <TagEditor messageId={messageId} tags={tags} />}
    </div>
  );
}

/** The gRPC status text of a refused last-label removal is not for display. */
function pickerError(e: Error): string {
  return /precondition/i.test(e.message) ? 'Un messaggio deve avere almeno un’etichetta.' : e.message;
}

function LabelPicker({ messageId, mailboxes, mailboxIds }: { messageId: string; mailboxes: Mailbox[]; mailboxIds: string[] }) {
  const change = useChangeLabels();
  const toggle = (id: string, on: boolean) =>
    change.mutate({ messageId, add: on ? [id] : [], remove: on ? [] : [id] });

  return (
    <Popover onOpenChange={(open) => open && change.reset()}>
      <PopoverTrigger
        aria-label="Modifica etichette"
        className="inline-flex items-center gap-1 rounded-full border border-dashed border-border px-1.5 py-px text-[10px] leading-4 text-muted-foreground transition-colors hover:bg-surface-hover hover:text-foreground"
      >
        <Tag size={10} />
        Etichette
      </PopoverTrigger>
      <PopoverContent>
        <ul aria-label="Etichette" className="max-h-64 overflow-y-auto">
          {mailboxes.map((m) => (
            <li key={m.id}>
              <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-xs hover:bg-surface-hover">
                <input
                  type="checkbox"
                  checked={mailboxIds.includes(m.id)}
                  onChange={(e) => toggle(m.id, e.target.checked)}
                />
                <span className="truncate">{m.name}</span>
              </label>
            </li>
          ))}
        </ul>
        {change.error && (
          <p role="alert" className="px-2 py-1.5 text-[11px] text-destructive">
            {pickerError(change.error)}
          </p>
        )}
      </PopoverContent>
    </Popover>
  );
}

function TagEditor({ messageId, tags }: { messageId: string; tags: string[] }) {
  const modify = useModifyTags();
  const [draft, setDraft] = useState('');

  const add = () => {
    const tag = draft.trim();
    if (!tag || tags.includes(tag)) return;
    modify.mutate({ messageId, add: [tag] }, { onSuccess: () => setDraft('') });
  };

  return (
    <>
      {tags.map((t) => (
        <LabelChip key={t} name={t} tag onRemove={() => modify.mutate({ messageId, remove: [t] })} />
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
