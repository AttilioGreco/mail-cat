import { useLabelDialogs } from '../model/dialogStore';
import { LabelPickerDialog } from './LabelPickerDialog';
import { ManageLabelDialog } from './ManageLabelDialog';
import { SenderLabelDialog } from './SenderLabelDialog';

/** Mounted once on the page: shows whichever label dialog was requested. */
export function LabelDialogs() {
  const { picker, sender, manage, close } = useLabelDialogs();
  if (picker) return <LabelPickerDialog key={picker.messages.map((m) => m.id).join()} {...picker} onClose={close} />;
  if (sender) return <SenderLabelDialog key={sender.email} {...sender} onClose={close} />;
  if (manage) return <ManageLabelDialog key={manage.label + manage.mode} {...manage} onClose={close} />;
  return null;
}
