/** The search language the server understands (see docs/search.md), for the box's hints. */
export interface OperatorDoc {
  name: string;
  hint: string;
  example: string;
}

export const OPERATORS: OperatorDoc[] = [
  { name: 'from', hint: 'Mittente (indirizzo, nome o dominio)', example: 'from:facile.it' },
  { name: 'to', hint: 'Destinatario', example: 'to:anna@example.com' },
  { name: 'cc', hint: 'In copia', example: 'cc:anna@example.com' },
  { name: 'subject', hint: 'Oggetto (anche sbj:)', example: 'subject:fattura' },
  { name: 'label', hint: 'Etichetta, comprese le sotto-etichette', example: 'label:"Newsletter/The Space Cinema"' },
  { name: 'in', hint: 'Cartella: inbox, sent, trash, spam, drafts, archive o un percorso', example: 'in:inbox' },
  { name: 'account', hint: 'Account (nome, email o id)', example: 'account:lavoro' },
  { name: 'is', hint: 'Stato: unread, read, starred, answered', example: 'is:unread' },
  { name: 'has', hint: 'Ha un allegato', example: 'has:attachment' },
  { name: 'filename', hint: 'Nome di un allegato', example: 'filename:fattura' },
  { name: 'after', hint: 'Ricevuti da questa data', example: 'after:2026-01-31' },
  { name: 'before', hint: 'Ricevuti prima di questa data', example: 'before:2026-02-01' },
  { name: 'newer_than', hint: 'Più recenti di N giorni (d), mesi (m) o anni (y)', example: 'newer_than:7d' },
  { name: 'older_than', hint: 'Più vecchi di N giorni, mesi o anni', example: 'older_than:1y' },
  { name: 'list', hint: 'Lista di distribuzione (List-Id)', example: 'list:github' },
];

const VALUES: Record<string, string[]> = {
  in: ['inbox', 'sent', 'trash', 'spam', 'drafts', 'archive'],
  is: ['unread', 'read', 'starred', 'answered'],
  has: ['attachment'],
};

export interface Completion {
  /** What the list shows. */
  label: string;
  hint?: string;
  /** Replaces the token being typed. */
  insert: string;
}

const quoted = (value: string) => (/\s/.test(value) ? `"${value}"` : value);

/** The token at the end of `input`: `-label:"New` keeps its open quote. */
const LAST_TOKEN = /(?:^|\s)(-?)(\w*)(?::("[^"]*|[^\s"]*))?$/;

/**
 * Operator names for a partly typed word, or the values of the operator
 * before the colon. `start` is where the replaced token begins.
 */
export function completionsFor(input: string, labels: string[]): { start: number; items: Completion[] } {
  const m = LAST_TOKEN.exec(input);
  if (!m) return { start: input.length, items: [] };
  const [token, minus, name, value] = m;
  const start = input.length - token.trimStart().length;

  if (value === undefined) {
    if (name.length === 0) return { start, items: [] };
    const items = OPERATORS.filter((o) => o.name.startsWith(name.toLowerCase())).map((o) => ({
      label: `${o.name}:`,
      hint: o.hint,
      insert: `${minus}${o.name}:`,
    }));
    return { start, items };
  }

  const op = name.toLowerCase();
  const typed = value.replace(/^"/, '').toLowerCase();
  const candidates = op === 'label' ? labels : (VALUES[op] ?? []);
  const items = candidates
    .filter((v) => v.toLowerCase() !== typed && (op === 'label' ? v.toLowerCase().includes(typed) : v.startsWith(typed)))
    .slice(0, 8)
    .map((v) => ({ label: v, insert: `${minus}${op}:${quoted(v)} ` }));
  return { start, items };
}
