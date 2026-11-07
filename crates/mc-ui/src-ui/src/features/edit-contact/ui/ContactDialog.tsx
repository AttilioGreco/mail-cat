import { Dialog, DialogContent, DialogTitle } from '@/shared/ui/dialog';
import { useContactEditor } from '@/entities/contact';
import { formFromTarget } from '../lib/form';
import { ContactForm } from './ContactForm';

/** Mounted once by the main page; renders nothing until a contact is opened or created. */
export function ContactDialog() {
  const { target, close } = useContactEditor();
  return (
    <Dialog open={target !== null} onOpenChange={(open) => !open && close()}>
      {target && (
        <DialogContent className="sm:max-w-md">
          <DialogTitle>{target.mode === 'create' ? 'Nuovo contatto' : 'Modifica contatto'}</DialogTitle>
          <ContactForm
            key={target.mode === 'edit' ? target.contact.id : 'new'}
            target={target}
            initial={formFromTarget(target)}
            onClose={close}
          />
        </DialogContent>
      )}
    </Dialog>
  );
}
