// Run with `bun run src/pages/contacts/lib/group.check.ts`.
import type { ContactCard, ContactGroup, RemoteContact } from '@/entities/contact';
import { buildRows } from './group';

const eq = (actual: unknown, expected: unknown, what: string) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${what}: got ${JSON.stringify(actual)}, want ${JSON.stringify(expected)}`);
  }
};

const remote = (id: string, displayName: string, addressBook: string, emails: string[] = []): RemoteContact => ({
  id, accountId: 'a1', addressBook, href: id, displayName, emails, phones: [], organization: null, notes: null,
  raw: '', dirty: false, pushFailed: null,
});
const feature = { state: 'enabled' as const, missingScopes: [] };
const account = { id: 'a1', name: 'Work', email: 'w@x.it', auth: null };
const group: ContactGroup = {
  account, feature, isLoading: false, error: null,
  books: [{ collection: 'b1', name: 'Rubrica', description: null, readOnly: false }],
  contacts: [remote('2', 'zeta', 'b1'), remote('1', 'Álvaro', 'b1', ['alv@x.it']), remote('3', 'orphan', 'gone')],
};
const harvested: ContactCard = {
  id: 'h', displayName: 'Harvey', organization: null, emails: ['harvey@x.it'], phones: [], addressBook: null,
  addressBookName: null, accountId: '', notes: null, source: 'harvested', raw: null, dirty: false, pushFailed: null,
};
const shape = (rows: ReturnType<typeof buildRows>) =>
  rows.map((r) => (r.kind === 'contact' ? r.contact.displayName : r.label));

eq(shape(buildRows([group], [harvested], '')), ['Work', 'Rubrica', 'Álvaro', 'zeta', 'Dalle mail', 'Harvey'], 'grouped, sorted accent-insensitively');
eq(shape(buildRows([group], [harvested], ' ALV ')), ['Work', 'Rubrica', 'Álvaro'], 'search drops empty groups');
eq(shape(buildRows([group], [harvested], 'zzz')), [], 'nothing found');
const missing: ContactGroup = { ...group, feature: { state: 'missing_permission', missingScopes: ['s'] }, books: [], contacts: [] };
eq(buildRows([missing], [], 'x').map((r) => r.kind === 'account' && r.notice), ['missing_permission'], 'notice survives a search');
