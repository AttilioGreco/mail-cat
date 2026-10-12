import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { ComposeRequest } from './compose-store';
import type { ComposeDto, ComposeInput } from './composeTypes';

export function usePrepareCompose({ mode, messageId, accountId }: ComposeRequest) {
  return useQuery({
    queryKey: ['compose', mode, messageId, accountId],
    queryFn: () => invoke<ComposeDto>('prepare_compose', { mode, messageId, accountId }),
    // One fetch per compose session: a refetch mid-edit would hand the form
    // a new baseline, and a cached one would reopen a stale draft.
    staleTime: Infinity,
    gcTime: 0,
    refetchOnWindowFocus: false,
    retry: false,
  });
}

/** Send, save and discard all touch Sent/Drafts, so they refresh the same views. */
function useInvalidateMail() {
  const qc = useQueryClient();
  return () => {
    for (const key of ['thread', 'conversations', 'messages', 'mailboxes']) {
      void qc.invalidateQueries({ queryKey: [key] });
    }
  };
}

export function useSendCompose() {
  const onSuccess = useInvalidateMail();
  return useMutation({
    mutationFn: (input: ComposeInput) => invoke<string>('send_compose', { input }),
    onSuccess,
  });
}

export function useSaveDraft() {
  const onSuccess = useInvalidateMail();
  return useMutation({
    mutationFn: (input: ComposeInput) => invoke<string>('save_draft', { input }),
    onSuccess,
  });
}

export function useDiscardDraft() {
  const onSuccess = useInvalidateMail();
  return useMutation({
    mutationFn: (draftId: string) => invoke<void>('discard_draft', { draftId }),
    onSuccess,
  });
}
