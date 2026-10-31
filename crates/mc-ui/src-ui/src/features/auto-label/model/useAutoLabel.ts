import { useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type { ApplyPreview, BackgroundStats, EmbeddingsStatus, LabelMode, LabelStatus, MessageTags, TrainingGroup } from './types';

/** Labels changed behind the views' backs: everything that shows them is re-read. */
function requery(qc: QueryClient) {
  for (const key of ['tags', 'messages', 'conversations', 'thread', 'search', 'mailboxes'])
    qc.invalidateQueries({ queryKey: [key] });
}

/** What the classifier offers on a message. Failures are silent: the feature is an extra. */
export function useMessageTags(messageId: string | null) {
  return useQuery({
    queryKey: ['tags', messageId],
    enabled: !!messageId,
    staleTime: 30_000,
    retry: false,
    queryFn: () => invoke<MessageTags>('get_message_tags', { messageId }),
  });
}

interface TagRef {
  messageId: string;
  label: string;
}

function tagMutation(command: 'accept_suggestion' | 'reject_suggestion' | 'undo_auto_label') {
  return function useTagMutation() {
    const qc = useQueryClient();
    return useMutation({
      mutationFn: (ref: TagRef) => invoke<void>(command, { ...ref }),
      onSettled: () => requery(qc),
    });
  };
}

export const useAcceptSuggestion = tagMutation('accept_suggestion');
export const useRejectSuggestion = tagMutation('reject_suggestion');
export const useUndoAutoLabel = tagMutation('undo_auto_label');

export function useLabelStatus(accountId: string, label: string, enabled: boolean) {
  return useQuery({
    queryKey: ['label-status', accountId, label],
    enabled,
    queryFn: () => invoke<LabelStatus>('get_label_settings', { accountId, label }),
  });
}

interface LabelSettingsParams {
  accountId: string;
  label: string;
  mode: LabelMode;
  threshold: number;
}

export function useSetLabelSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (p: LabelSettingsParams) => invoke<LabelStatus>('set_label_settings', { ...p }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['label-status'] }),
    onSettled: () => qc.invalidateQueries({ queryKey: ['tags'] }),
  });
}

export function usePreviewApply() {
  return useMutation({ mutationFn: (p: LabelSettingsParams) => invoke<ApplyPreview>('preview_apply', { ...p }) });
}

/** Saves the mode and starts labelling existing mail in the background. */
export function useConfirmApply() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (p: LabelSettingsParams) => invoke<ApplyPreview>('confirm_apply', { ...p }),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ['label-status'] });
      requery(qc);
    },
  });
}

export function useTrainingGroups() {
  return useQuery({ queryKey: ['training-groups'], queryFn: () => invoke<TrainingGroup[]>('list_training_groups') });
}

export function useGroupMutations() {
  const qc = useQueryClient();
  const done = { onSettled: () => qc.invalidateQueries({ queryKey: ['training-groups'] }) };
  return {
    create: useMutation({ mutationFn: (name: string) => invoke<TrainingGroup>('create_training_group', { name }), ...done }),
    rename: useMutation({
      mutationFn: (p: { id: string; name: string }) => invoke<void>('rename_training_group', { ...p }),
      ...done,
    }),
    remove: useMutation({ mutationFn: (id: string) => invoke<void>('delete_training_group', { id }), ...done }),
    assign: useMutation({
      mutationFn: (p: { accountId: string; groupId: string | null }) => invoke<void>('assign_account_group', { ...p }),
      ...done,
    }),
  };
}

/** Polled only while a component using it is mounted (the settings tab). */
export function useBackgroundStats() {
  return useQuery({
    queryKey: ['background-stats'],
    queryFn: () => invoke<BackgroundStats>('get_background_stats'),
    refetchInterval: 2000,
  });
}

export function useEmbeddings() {
  const qc = useQueryClient();
  const status = useQuery({ queryKey: ['embeddings'], queryFn: () => invoke<EmbeddingsStatus>('get_embeddings') });
  const set = useMutation({
    mutationFn: (enabled: boolean) => invoke<EmbeddingsStatus>('set_embeddings', { enabled }),
    onSuccess: (next) => qc.setQueryData(['embeddings'], next),
  });
  const setSpeed = useMutation({
    mutationFn: (speed: EmbeddingsStatus['speed']) => invoke<EmbeddingsStatus>('set_background_speed', { speed }),
    onSuccess: (next) => qc.setQueryData(['embeddings'], next),
  });
  return { status: status.data, set, setSpeed };
}
