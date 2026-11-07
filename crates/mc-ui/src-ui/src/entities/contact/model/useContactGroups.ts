import { useAddressBooks, useContacts } from './useContact';
import type { AddressBook, RemoteContact } from './types';

/** The slice of an account this needs; structural so the contact entity does not import the account one. */
export interface ContactAccount {
  id: string;
  name: string;
  email: string;
  auth: { contacts: FeatureStatus } | null;
}

interface FeatureStatus {
  state: 'enabled' | 'missing_permission' | 'unavailable';
  missingScopes: string[];
}

export interface ContactGroup {
  account: ContactAccount;
  feature: FeatureStatus;
  /** Empty unless the feature is enabled. */
  books: AddressBook[];
  contacts: RemoteContact[];
  isLoading: boolean;
  error: unknown;
}

/** Every account whose contacts feature is usable or fixable; an unavailable one is left out. */
export function useContactGroups(accounts: ContactAccount[]): ContactGroup[] {
  const listed = accounts
    .map((account) => ({ account, feature: account.auth?.contacts ?? { state: 'enabled' as const, missingScopes: [] } }))
    .filter((g) => g.feature.state !== 'unavailable');
  const ids = listed.filter((g) => g.feature.state === 'enabled').map((g) => g.account.id);
  const books = useAddressBooks(ids);
  const contacts = useContacts(ids);

  return listed.map((g) => {
    const b = books.find((x) => x.accountId === g.account.id);
    const c = contacts.find((x) => x.accountId === g.account.id);
    return {
      ...g,
      books: b?.addressBooks ?? [],
      contacts: c?.contacts ?? [],
      isLoading: (b?.isLoading || c?.isLoading) ?? false,
      error: b?.error ?? c?.error ?? null,
    };
  });
}
