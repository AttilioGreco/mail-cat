import { useMutation, useQueries, useQuery, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { AddressBook, ContactCard, ContactFields, RemoteContact } from './types';

/** One result per account, so a failing account never hides the others. */
export function useAddressBooks(accountIds: string[]) {
  return useQueries({
    queries: accountIds.map((accountId) => ({
      queryKey: ['address-books', accountId],
      queryFn: () => invoke<AddressBook[]>('list_address_books', { accountId }),
      staleTime: 60_000,
    })),
    combine: (results) =>
      results.map((r, i) => ({
        accountId: accountIds[i],
        addressBooks: r.data ?? [],
        isLoading: r.isLoading,
        error: r.error,
      })),
  });
}

/** Every book of each account; group by `addressBook`. */
export function useContacts(accountIds: string[]) {
  return useQueries({
    queries: accountIds.map((accountId) => ({
      queryKey: ['remote-contacts', accountId],
      queryFn: () => invoke<RemoteContact[]>('list_remote_contacts', { accountId, addressBook: null }),
      staleTime: 30_000,
    })),
    combine: (results) =>
      results.map((r, i) => ({
        accountId: accountIds[i],
        contacts: r.data ?? [],
        isLoading: r.isLoading,
        error: r.error,
      })),
  });
}

export function useContact(accountId: string | null, contactId: string | null) {
  return useQuery({
    queryKey: ['remote-contact', accountId, contactId],
    enabled: accountId !== null && contactId !== null,
    queryFn: () => invoke<RemoteContact>('get_remote_contact', { contactId }),
  });
}

/** The "Dalle mail" group: contacts that live only here. */
export function useHarvestedContacts() {
  return useQuery({
    queryKey: ['harvested-contacts'],
    queryFn: () => invoke<ContactCard[]>('list_harvested_contacts'),
    staleTime: 60_000,
  });
}

/** The card for an address (the hover card), preferring `accountId`'s contacts; null when unknown. */
export function useLookupContact(email: string | null, accountId?: string | null) {
  return useQuery({
    queryKey: ['contact-lookup', email, accountId ?? null],
    enabled: !!email,
    queryFn: () => invoke<ContactCard | null>('lookup_contact', { email, accountId: accountId ?? null }),
    staleTime: 5 * 60_000,
  });
}

/** Local edits show up at once; the server's `dav:changed` follows once they are pushed. */
function useInvalidateContacts() {
  const queryClient = useQueryClient();
  return (accountId: string | null) => {
    const refresh = (name: string) =>
      queryClient.invalidateQueries({
        predicate: ({ queryKey }) => queryKey[0] === name && (!accountId || queryKey[1] === accountId),
      });
    ['remote-contacts', 'remote-contact'].forEach(refresh);
    queryClient.invalidateQueries({ queryKey: ['contact-lookup'] });
  };
}

/** Resolves to the new contact's id. */
export function useCreateContact() {
  const invalidate = useInvalidateContacts();
  return useMutation({
    mutationFn: ({ accountId, addressBook, fields }: { accountId: string; addressBook: string; fields: ContactFields }) =>
      invoke<string>('create_contact', { accountId, addressBook, fields }),
    onSuccess: (_, { accountId }) => invalidate(accountId),
  });
}

export function useUpdateContact() {
  const invalidate = useInvalidateContacts();
  return useMutation({
    mutationFn: ({ contactId, fields }: { accountId: string; contactId: string; fields: ContactFields }) =>
      invoke<void>('update_contact', { contactId, fields }),
    onSuccess: (_, { accountId }) => invalidate(accountId),
  });
}

export function useDeleteContact() {
  const invalidate = useInvalidateContacts();
  return useMutation({
    mutationFn: ({ contactId }: { accountId: string; contactId: string }) =>
      invoke<void>('delete_contact', { contactId }),
    onSuccess: (_, { accountId }) => invalidate(accountId),
  });
}
