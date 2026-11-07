export interface AddressBook {
  /** Collection path: unique within an account, not across accounts. */
  collection: string;
  name: string;
  description: string | null;
  readOnly: boolean;
}

export interface RemoteContact {
  id: string;
  accountId: string;
  addressBook: string;
  href: string;
  displayName: string;
  emails: string[];
  phones: string[];
  organization: string | null;
  notes: string | null;
  raw: string;
  /** A local edit the server has not acknowledged. */
  dirty: boolean;
  /** Why the server refused the last push. */
  pushFailed: string | null;
}

/**
 * The card behind an address: a remote contact, or one harvested from mail
 * (read-only: no edit or delete).
 */
export interface ContactCard {
  id: string;
  displayName: string;
  organization: string | null;
  emails: string[];
  phones: string[];
  /** Remote only: collection path and its name. */
  addressBook: string | null;
  addressBookName: string | null;
  /** Empty for a harvested contact shared across accounts. */
  accountId: string;
  notes: string | null;
  source: 'remote' | 'harvested';
  /** Remote only. */
  raw: string | null;
  dirty: boolean;
  pushFailed: string | null;
}

/** Absent fields are left as they are; an empty organization or notes clears it. */
export interface ContactFields {
  displayName?: string;
  emails?: string[];
  phones?: string[];
  organization?: string;
  notes?: string;
}
