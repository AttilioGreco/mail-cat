import { useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { invoke } from '@tauri-apps/api/core';
import type {
  ApplyAllReport,
  ApplyPreview,
  BackgroundStats,
  EmbeddingsStatus,
  LabelMode,
  LabelProposal,
  LabelStatus,
  MessageTags,
  ModelProfile,
  TrainingGroup,
} from './types';

/** Labels changed behind the views' backs: everything that shows them is re-read. */
function requery(qc: QueryClient) {
  for (const key of ['tags', 'messages', 'conversations', 'thread', 'search', 'mailboxes', 'background-stats'])
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
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['label-status'] });
      qc.invalidateQueries({ queryKey: ['label-modes'] });
    },
    onSettled: () => qc.invalidateQueries({ queryKey: ['tags'] }),
  });
}

/** Mode per label (see `labelKey`) for the account's group; labels the model has
 *  no example of are absent, i.e. off. */
export function useLabelModes(accountId: string) {
  return useQuery({
    queryKey: ['label-modes', accountId],
    queryFn: async () => {
      const [scope] = await invoke<{ labels: LabelStatus[] }[]>('list_labels', { accountId });
      return new Map((scope?.labels ?? []).map((l) => [l.label, l.mode]));
    },
  });
}

/** Colors need no examples: they never change what the classifier does. */
export function useSetLabelColor() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (p: { accountId: string; label: string; color: string | null }) =>
      invoke<LabelStatus>('set_label_color', { ...p }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['label-colors'] }),
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
      qc.invalidateQueries({ queryKey: ['label-modes'] });
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
  const setModel = useMutation({
    mutationFn: (profile: ModelProfile) => invoke<EmbeddingsStatus>('set_embedding_model', { profile }),
    onSuccess: (next) => {
      qc.setQueryData(['embeddings'], next);
      qc.invalidateQueries({ queryKey: ['background-stats'] });
    },
  });
  const setTarget = useMutation({
    mutationFn: (target: string) => invoke<EmbeddingsStatus>('set_execution_target', { target }),
    onSuccess: (next) => qc.setQueryData(['embeddings'], next),
  });
  return { status: status.data, set, setSpeed, setModel, setTarget };
}

/** Labels discovery proposes; the folder bar badge is always mounted, so this polls gently. */
export function useLabelProposals() {
  return useQuery({
    queryKey: ['label-proposals'],
    queryFn: () => invoke<LabelProposal[]>('list_label_proposals'),
    refetchInterval: 30_000,
    retry: false,
  });
}

/** Starts a search in the background; resolves to false when one was already running. */
export function useDiscoverLabels() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => invoke<boolean>('discover_labels', { accountId: null }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['background-stats'] }),
  });
}

export function useProposalMutations() {
  const qc = useQueryClient();
  const done = { onSettled: () => qc.invalidateQueries({ queryKey: ['label-proposals'] }) };
  return {
    accept: useMutation({
      mutationFn: (p: { id: string; name: string; archive: boolean }) => invoke<number>('accept_label_proposal', { ...p }),
      onSettled: () => {
        qc.invalidateQueries({ queryKey: ['label-proposals'] });
        requery(qc);
      },
    }),
    reject: useMutation({ mutationFn: (id: string) => invoke<void>('reject_label_proposal', { id }), ...done }),
  };
}

/** What labelling the existing mail would do, for every label with enough examples.
 *  Never cached: the counts are read when the dialog opens. */
export function useApplyAllPreview() {
  return useQuery({
    queryKey: ['apply-all-preview'],
    gcTime: 0,
    queryFn: () => invoke<ApplyAllReport>('apply_all_labels', { labels: [], accountId: null, dryRun: true }),
  });
}

/** Queues the labelling of the chosen labels; progress shows in the background stats. */
export function useApplyAll() {
  const qc = useQueryClient();
  return useMutation({
    // A label key can exist in several training groups: run each group on its own so an
    // unticked row in one group is not labelled because the same key is ticked in another.
    mutationFn: async (chosen: { scope: string; label: string }[]) => {
      const scopes = await invoke<{ scope: string; accountIds: string[] }[]>('list_labels', { accountId: null });
      const byScope = new Map<string, string[]>();
      for (const c of chosen) byScope.set(c.scope, [...(byScope.get(c.scope) ?? []), c.label]);
      for (const [scope, labels] of byScope) {
        const accountId = scopes.find((s) => s.scope === scope)?.accountIds[0];
        if (!accountId) throw new Error(`Gruppo ${scope} non trovato`);
        await invoke<ApplyAllReport>('apply_all_labels', { labels, accountId, dryRun: false });
      }
    },
    onSettled: () => requery(qc),
  });
}
