import type { Mailbox } from './types';

export interface LabelNode {
  /** Full path prefix ("A/B"); unique, stable across renders. */
  key: string;
  /** Last path segment. */
  segment: string;
  /** Absent for a grouping node: a parent no real mailbox exists for. */
  mailbox?: Mailbox;
  children: LabelNode[];
  /** Own unread plus every descendant's. */
  unreadTotal: number;
}

const collator = new Intl.Collator(undefined, { sensitivity: 'base', numeric: true });

/** Labels are hierarchical by '/' in displayPath. */
export function buildLabelTree(labels: Mailbox[]): LabelNode[] {
  const roots: LabelNode[] = [];
  const byKey = new Map<string, LabelNode>();

  for (const mailbox of labels) {
    const segments = mailbox.displayPath.split('/').filter(Boolean);
    let siblings = roots;
    let key = '';
    segments.forEach((segment, i) => {
      key = i === 0 ? segment : `${key}/${segment}`;
      let node = byKey.get(key);
      if (!node) {
        node = { key, segment, children: [], unreadTotal: 0 };
        byKey.set(key, node);
        siblings.push(node);
      }
      if (i === segments.length - 1) node.mailbox = mailbox;
      siblings = node.children;
    });
  }

  const finish = (nodes: LabelNode[]) => {
    nodes.sort((a, b) => collator.compare(a.segment, b.segment));
    for (const n of nodes) {
      finish(n.children);
      n.unreadTotal = (n.mailbox?.unreadMessages ?? 0) + n.children.reduce((s, c) => s + c.unreadTotal, 0);
    }
  };
  finish(roots);
  return roots;
}

/** Keys of every ancestor of the node at `key` ("A/B/C" -> "A", "A/B"). */
export function ancestorKeys(key: string): string[] {
  const parts = key.split('/');
  return parts.slice(1).map((_, i) => parts.slice(0, i + 1).join('/'));
}

export interface LabelRow {
  node: LabelNode;
  depth: number;
  parentKey: string | null;
}

/** The rows on screen, in order: children appear only under expanded parents. */
export function visibleRows(roots: LabelNode[], expanded: ReadonlySet<string>): LabelRow[] {
  const rows: LabelRow[] = [];
  const walk = (nodes: LabelNode[], depth: number, parentKey: string | null) => {
    for (const node of nodes) {
      rows.push({ node, depth, parentKey });
      if (expanded.has(node.key)) walk(node.children, depth + 1, node.key);
    }
  };
  walk(roots, 0, null);
  return rows;
}
