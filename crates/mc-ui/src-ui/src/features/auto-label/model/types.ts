/** `off`: nothing happens; `suggest`: shown on the message; `apply`: put on by itself; `apply_archive`: and out of the Inbox. */
export type LabelMode = 'off' | 'suggest' | 'apply' | 'apply_archive';

export interface TrainingGroup {
  id: string;
  name: string;
  accountIds: string[];
  isDefault: boolean;
}

export interface LabelStatus {
  label: string;
  mode: LabelMode;
  threshold: number;
  positives: number;
  minPositives: number;
  color?: string | null;
}

export interface TagSuggestion {
  label: string;
  mailboxId: string;
  name: string;
  confidence: 'high' | 'medium';
}

export interface AutoLabel {
  label: string;
  mailboxId: string;
  name: string;
  archived: boolean;
  /** Unix seconds. */
  at: number;
}

export interface MessageTags {
  suggestions: TagSuggestion[];
  auto: AutoLabel[];
}

export interface ApplyPreview {
  toLabel: number;
  toArchive: number;
  sample: { messageId: string; subject: string; from: string }[];
}

export const APPLY_MODES: LabelMode[] = ['apply', 'apply_archive'];

/** The confidence a label must reach to be applied by itself, as levels people can reason about. */
export const THRESHOLDS = [
  { value: 0.97, label: 'Molto alta' },
  { value: 0.9, label: 'Alta' },
  { value: 0.75, label: 'Media' },
] as const;

export interface EmbeddingsStatus {
  enabled: boolean;
  /** False where the server has no model. */
  available: boolean;
  model: string;
  speed: 'light' | 'normal' | 'max';
  profile: ModelProfile;
  models: ModelOption[];
  /** 'cpu', 'gpu-cuda', 'gpu-rocm', 'gpu-directml', 'gpu-coreml' or 'npu-vitisai'. */
  target: string;
  targets: { id: string; available: boolean }[];
}

export type ModelProfile = 'light' | 'balanced' | 'quality';

export interface ModelOption {
  profile: ModelProfile;
  model: string;
  dims: number;
  downloadBytes: number;
  /** Estimate of the memory each worker holds. */
  workerBytes: number;
  /** 1 (good) to 3 (best). */
  quality: number;
}

export type EmbedderState = 'off' | 'loading' | 'active' | 'paused_for_retrain' | 'idle';

export interface BackgroundStats {
  /** Model in use; empty without one. */
  model: string;
  speed: EmbeddingsStatus['speed'];
  workers: number;
  threads: number;
  batch: number;
  pauseMs: number;
  state: EmbedderState;
  modelLoaded: boolean;
  embedded: number;
  eligible: number;
  perSecond: number;
  /** Zero when unknown. */
  etaSeconds: number;
  /** Zero when unknown. */
  workerBytes: number;
  retrains: { scope: string; done: number; total: number }[];
  /** Set while a search for new labels runs. */
  discovery: DiscoveryProgress | null;
  /** Labelling jobs, queued or running. */
  applying: ApplyProgress[];
}

export interface ApplyProgress {
  label: string;
  /** `undo` takes an accepted proposal's label off again. */
  kind: 'apply' | 'undo';
  done: number;
  total: number;
  /** Of `done`, the messages that left (undo: returned to) the Inbox. */
  archived: number;
}

export interface DiscoveryProgress {
  /** Training group being searched; empty while starting. */
  scope: string;
  phase: 'loading' | 'clustering' | 'naming';
  done: number;
  /** Zero while the phase has nothing to count. */
  total: number;
}

export interface LabelProposal {
  id: string;
  name: string;
  keywords: string[];
  size: number;
  /** Of `size`, how many are in an Inbox. */
  inbox: number;
  samples: { messageId: string; subject: string; from: string }[];
  /** The training group whose mail it is made of. */
  group: string;
  /** Unix seconds. */
  createdAt: number;
  status: 'pending' | 'accepted' | 'rejected';
  /** The training scope: `g:<group id>`, or `a:<account id>` for an account that learns alone. */
  scope: string;
  /** Dominant sender domains, most frequent first; empty unless asked for. */
  senders: { domain: string; count: number }[];
  /** Renamed or merged by hand: discovery keeps it. */
  curated: boolean;
}

export interface LabelApply {
  scope: string;
  /** As the group spells it. */
  name: string;
  /** Lowercase key, what a run is asked for. */
  label: string;
  mode: LabelMode;
  threshold: number;
  positives: number;
  /** Existing mail the classifier would label at the threshold. */
  toLabel: number;
  sample: { messageId: string; subject: string; from: string }[];
}

export interface ApplyAllReport {
  dryRun: boolean;
  toLabel: number;
  labels: LabelApply[];
}
