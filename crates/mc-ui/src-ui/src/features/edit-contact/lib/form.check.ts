// Run with `bun run src/features/edit-contact/lib/form.check.ts`.
import { fieldsFromForm, formFromTarget } from './form';

const eq = (actual: unknown, expected: unknown, what: string) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${what}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
  }
};

const prefilled = formFromTarget({ mode: 'create', prefill: { email: 'a@x.it' } });
eq(fieldsFromForm(prefilled)?.displayName, 'a@x.it', 'name falls back to the first email');
eq(fieldsFromForm({ ...prefilled, emails: '' }), null, 'nothing to identify the contact');
eq(fieldsFromForm({ ...prefilled, emails: ' a@x.it \n\n b@x.it ', phones: '1\n 2 ' })?.emails, ['a@x.it', 'b@x.it'], 'blank lines dropped');
eq(fieldsFromForm({ ...prefilled, organization: '  ' })?.organization, '', 'blank organization clears it');
