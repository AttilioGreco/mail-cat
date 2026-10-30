import { invoke } from '@tauri-apps/api/core';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';

/** Subscribe or unsubscribe a mailbox. The server answers a warning when it
 *  could not pass the choice on: it then holds on this machine only. */
export function useSetSubscribed(accountId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ mailboxId, subscribed }: { mailboxId: string; subscribed: boolean }) =>
      invoke<string | null>('set_mailbox_subscribed', { mailboxId, subscribed }),
    onSuccess: (warning) => {
      if (warning) toast.warning(warning);
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ['mailboxes', accountId] });
      queryClient.invalidateQueries({ queryKey: ['messages'] });
      queryClient.invalidateQueries({ queryKey: ['conversations'] });
    },
  });
}

/** Ask the server to list its mailboxes again (a sync): new ones show up in the dialog. */
export function useRefreshFolders() {
  return useMutation({
    mutationFn: (accountId: string) => invoke('sync_now', { accountId }),
  });
}
