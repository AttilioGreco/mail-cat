import { Tag, UserRound } from 'lucide-react';
import { useAccounts } from '@/entities/account';
import { useLabelDialogs, type LabelTarget } from '../model/dialogStore';
import { labelMode } from './LabelPickerDialog';

const ITEM =
  'flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-foreground transition-colors hover:bg-surface-hover';

interface LabelMenuItemsProps {
  accountId: string;
  messages: LabelTarget[];
  /** Offered only when there is one sender to speak of. */
  senderEmail?: string;
  onDone: () => void;
}

/** The label entries of a message's context menu. */
export function LabelMenuItems({ accountId, messages, senderEmail, onDone }: LabelMenuItemsProps) {
  const account = useAccounts().data?.find((a) => a.id === accountId);
  const { openPicker, openSender } = useLabelDialogs();
  const mode = labelMode(account);
  if (!mode) return null;

  return (
    <>
      <button
        type="button"
        className={ITEM}
        onClick={() => {
          openPicker(accountId, messages);
          onDone();
        }}
      >
        <Tag size={14} className="shrink-0" />
        Etichetta…
      </button>
      {mode === 'labels' && senderEmail && (
        <button
          type="button"
          className={ITEM}
          onClick={() => {
            openSender(accountId, senderEmail);
            onDone();
          }}
        >
          <UserRound size={14} className="shrink-0" />
          Etichetta tutte le mail di questo mittente…
        </button>
      )}
    </>
  );
}
