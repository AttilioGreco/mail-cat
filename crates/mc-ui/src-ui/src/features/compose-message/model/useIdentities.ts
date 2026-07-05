import { useMemo } from 'react';
import { useAccounts } from '@/entities/account';
import type { Identity } from './composeTypes';

/**
 * Deriva le identità mittente dagli account configurati.
 * Quando il backend esporrà un endpoint dedicato (get_identities),
 * si può sostituire questa derivazione con una query diretta.
 */
export function useIdentities(): { identities: Identity[]; isLoading: boolean } {
  const { data: accounts = [], isLoading } = useAccounts();

  const identities = useMemo<Identity[]>(() =>
    accounts.map((acc) => ({
      id: acc.id,
      displayName: acc.name || acc.email,
      email: acc.email,
      // signatureHtml verrà popolato quando il backend lo esporrà
      signatureHtml: undefined,
    })),
    [accounts],
  );

  return { identities, isLoading };
}

/** Trova l'identità più adatta per rispondere a un messaggio
 *  (quella che corrisponde al dominio del destinatario, o la prima). */
export function bestIdentityForReply(
  identities: Identity[],
  recipientEmail: string,
): Identity | null {
  if (identities.length === 0) return null;
  const domain = recipientEmail.split('@')[1];
  return (
    identities.find((i) => i.email.endsWith('@' + domain)) ??
    identities[0]
  );
}
