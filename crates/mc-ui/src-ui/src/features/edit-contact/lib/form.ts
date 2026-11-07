import type { ContactEditorTarget, ContactFields } from '@/entities/contact';

/** Emails and phones are edited one per line. */
export interface ContactForm {
  displayName: string;
  emails: string;
  phones: string;
  organization: string;
  notes: string;
}

export function formFromTarget(target: ContactEditorTarget): ContactForm {
  if (target.mode === 'edit') {
    const c = target.contact;
    return {
      displayName: c.displayName,
      emails: c.emails.join('\n'),
      phones: c.phones.join('\n'),
      organization: c.organization ?? '',
      notes: c.notes ?? '',
    };
  }
  const { name = '', email = '' } = target.prefill ?? {};
  return { displayName: name, emails: email, phones: '', organization: '', notes: '' };
}

const lines = (text: string) => text.split('\n').map((l) => l.trim()).filter(Boolean);

/** A contact needs something to be found by; empty organization and notes clear them on the server. */
export function fieldsFromForm(f: ContactForm): ContactFields | null {
  const emails = lines(f.emails);
  const displayName = f.displayName.trim();
  if (!displayName && emails.length === 0) return null;
  return {
    displayName: displayName || emails[0],
    emails,
    phones: lines(f.phones),
    organization: f.organization.trim(),
    notes: f.notes.trim(),
  };
}
