import { useQuery } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';

const NONE: Record<string, string> = {};

/** Palette key per label (see `labelKey`) for the account's label group. A failure
 *  just leaves labels uncolored. */
export function useLabelColors(accountId: string | null): Readonly<Record<string, string>> {
  const { data } = useQuery({
    queryKey: ['label-colors', accountId],
    enabled: accountId !== null,
    staleTime: 30_000,
    retry: false,
    queryFn: () => invoke<Record<string, string>>('list_label_colors', { accountId }),
  });
  return data ?? NONE;
}
