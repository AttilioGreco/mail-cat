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
