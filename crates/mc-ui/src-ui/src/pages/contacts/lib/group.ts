import type { ContactCard, ContactGroup, RemoteContact } from '@/entities/contact';

export interface Listed {
  /** Unique across accounts and the harvested group. */
  key: string;
  displayName: string;
  organization: string | null;
  emails: string[];
  phones: string[];
  notes: string | null;
  source: 'remote' | 'harvested';
  accountName: string | null;
  bookName: string | null;
  dirty: boolean;
  pushFailed: string | null;
  /** Remote only: what the editor takes. */
  remote: RemoteContact | null;
}

export type Row =
  | { kind: 'account'; key: string; label: string; accountId: string; notice: 'missing_permission' | 'error' | null }
  | { kind: 'book'; key: string; label: string }
  | { kind: 'contact'; key: string; contact: Listed };

export const HARVESTED_LABEL = 'Dalle mail';

const collator = new Intl.Collator('it', { sensitivity: 'base' });
const byName = (a: Listed, b: Listed) => collator.compare(a.displayName, b.displayName);

const matches = (c: Listed, q: string) =>
  [c.displayName, c.organization ?? '', ...c.emails, ...c.phones].some((v) => v.toLowerCase().includes(q));

/**
 * The list as flat rows (headers and contacts) so it can be virtualised. While
 * searching, groups with no match disappear, except an account whose notice must stay.
 */
export function buildRows(groups: ContactGroup[], harvested: ContactCard[], query: string): Row[] {
  const q = query.trim().toLowerCase();
  const keep = (c: Listed) => !q || matches(c, q);
  const rows: Row[] = [];

  for (const g of groups) {
    const { account } = g;
    const notice = g.feature.state === 'missing_permission' ? 'missing_permission' : g.error ? 'error' : null;
    const books = g.books
      .map((b) => ({
        b,
        items: g.contacts
          .filter((c) => c.addressBook === b.collection)
          .map((c): Listed => ({
            key: `${account.id}::${c.id}`,
            displayName: c.displayName,
            organization: c.organization,
            emails: c.emails,
            phones: c.phones,
            notes: c.notes,
            source: 'remote',
            accountName: account.name,
            bookName: b.name,
            dirty: c.dirty,
            pushFailed: c.pushFailed,
            remote: c,
          }))
          .filter(keep)
          .sort(byName),
      }))
      .filter(({ items }) => items.length > 0);
    if (q && !notice && books.length === 0) continue;
    rows.push({ kind: 'account', key: `account:${account.id}`, label: account.name, accountId: account.id, notice });
    for (const { b, items } of books) {
      rows.push({ kind: 'book', key: `book:${account.id}::${b.collection}`, label: b.name });
      for (const contact of items) rows.push({ kind: 'contact', key: contact.key, contact });
    }
  }

  const fromMail = harvested
    .map((c): Listed => ({
      key: `harvested::${c.id}`,
      displayName: c.displayName,
      organization: c.organization,
      emails: c.emails,
      phones: c.phones,
      notes: c.notes,
      source: 'harvested',
      accountName: null,
      bookName: null,
      dirty: false,
      pushFailed: null,
      remote: null,
    }))
    .filter(keep)
    .sort(byName);
  if (fromMail.length > 0) {
    rows.push({ kind: 'account', key: 'harvested', label: HARVESTED_LABEL, accountId: '', notice: null });
    for (const contact of fromMail) rows.push({ kind: 'contact', key: contact.key, contact });
  }
  return rows;
}
