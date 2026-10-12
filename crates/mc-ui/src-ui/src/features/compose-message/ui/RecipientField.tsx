import { useEffect, useRef, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { cn } from '@/shared/lib/utils';
import {
  Combobox,
  ComboboxChip,
  ComboboxChips,
  ComboboxChipsInput,
  ComboboxContent,
  ComboboxItem,
  ComboboxList,
  useComboboxAnchor,
} from '@/shared/ui/combobox';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/shared/ui/tooltip';

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

interface ContactHit {
  id: string;
  display_name: string;
  primary_email: string;
  all_emails: string[];
  organization?: string;
  is_cross_account: boolean;
}

/** One row per address: a contact with three emails offers all three. */
interface Suggestion {
  value: string;
  name: string;
  email: string;
  organization?: string;
  crossAccount: boolean;
}

export function parseAddress(value: string): { name: string | null; email: string } {
  const match = value.match(/^(.*?)\s*<([^>]+)>\s*$/);
  if (!match) return { name: null, email: value.trim() };
  const name = match[1].trim().replace(/^"|"$/g, '');
  return { name: name || null, email: match[2].trim() };
}

export const isValidAddress = (value: string) => EMAIL_RE.test(parseAddress(value).email);

const addressKey = (value: string) => parseAddress(value).email.toLowerCase();

/** Splits "a@x.it, B <b@y.it>; c@z" on , ; and newlines, but not inside quotes
 *  or angle brackets ("Rossi, Mario" <m@x.it> stays one address). */
export function splitAddresses(text: string): string[] {
  const out: string[] = [];
  let current = '';
  let quoted = false;
  let angled = false;
  for (const ch of text) {
    if (ch === '"') quoted = !quoted;
    else if (ch === '<' && !quoted) angled = true;
    else if (ch === '>' && !quoted) angled = false;
    if (!quoted && !angled && (ch === ',' || ch === ';' || ch === '\n')) {
      out.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  out.push(current);
  return out.map((s) => s.trim()).filter(Boolean);
}

/** Appends `added`, skipping addresses (case-insensitive) already present. */
export function mergeAddresses(values: string[], added: string[]): string[] {
  const seen = new Set(values.map(addressKey));
  const next = [...values];
  for (const value of added) {
    const key = addressKey(value);
    if (seen.has(key)) continue;
    seen.add(key);
    next.push(value);
  }
  return next;
}

interface RecipientFieldProps {
  label: string;
  values: string[];
  onChange: (values: string[]) => void;
  /** Scopes the contact search to the sending account. */
  accountId: string;
  autoFocus?: boolean;
}

export function RecipientField({ label, values, onChange, accountId, autoFocus }: RecipientFieldProps) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  // Tagged with the query they answer: Enter must not pick a suggestion that
  // belongs to what was typed 150ms ago.
  const [results, setResults] = useState<{ query: string; items: Suggestion[] }>({ query: '', items: [] });
  const highlighted = useRef<string | undefined>(undefined);
  const anchor = useComboboxAnchor();

  useEffect(() => {
    const q = query.trim();
    if (!q) return;
    let live = true;
    const timer = setTimeout(() => {
      invoke<ContactHit[]>('search_contacts', { query: q, limit: 8, requestingAccountId: accountId })
        .then((hits) => live && setResults({ query: q, items: hits.flatMap(toSuggestions) }))
        .catch(() => live && setResults({ query: q, items: [] }));
    }, 150);
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [query, accountId]);

  const taken = new Set(values.map(addressKey));
  // Already-chosen addresses are hidden: in multiple mode, picking one again
  // would toggle it off instead of being a no-op.
  const items = results.query === query.trim() && query.trim()
    ? results.items.filter((s) => !taken.has(s.email.toLowerCase()))
    : [];
  const byValue = new Map(items.map((s) => [s.value, s]));
  const popupOpen = open && items.length > 0;

  const add = (added: string[]) => {
    onChange(mergeAddresses(values, added));
    setQuery('');
  };

  return (
    <div className="flex items-start gap-2 border-b border-border px-4 py-1.5">
      <span className="w-14 shrink-0 pt-1.5 text-xs text-muted">{label}</span>
      <Combobox
        multiple
        items={items.map((s) => s.value)}
        filter={null}
        autoHighlight
        value={values}
        onValueChange={(next: string[]) => onChange(mergeAddresses([], next))}
        inputValue={query}
        onInputValueChange={setQuery}
        open={popupOpen}
        onOpenChange={setOpen}
        onItemHighlighted={(value: string | undefined) => {
          highlighted.current = value;
        }}
      >
        <ComboboxChips
          ref={anchor}
          className="min-h-7 flex-1 gap-1 rounded-none border-0 bg-transparent px-0 py-0.5 focus-within:ring-0 has-data-[slot=combobox-chip]:px-0 dark:bg-transparent"
        >
          {values.map((value) => (
            <AddressChip key={value} value={value} />
          ))}
          <ComboboxChipsInput
            autoFocus={autoFocus}
            className="h-6 min-w-[120px] bg-transparent text-xs text-foreground placeholder:text-muted"
            onKeyDown={(e) => {
              const text = query.trim();
              // Base UI clears every chip on Escape; here Escape belongs to the window.
              if (e.key === 'Escape' && !popupOpen) {
                e.preventBaseUIHandler();
                return;
              }
              // Apple Mail: the first Backspace selects the last chip, the
              // second (handled by the focused chip) removes it.
              if (e.key === 'Backspace' && query === '' && values.length > 0) {
                e.preventBaseUIHandler();
                e.preventDefault();
                const chips = anchor.current?.querySelectorAll<HTMLElement>('[data-slot=combobox-chip]');
                chips?.[chips.length - 1]?.focus();
                return;
              }
              const pick = (e.key === 'Enter' || e.key === 'Tab') && highlighted.current && byValue.has(highlighted.current)
                ? [highlighted.current]
                : ['Enter', 'Tab', ',', ';'].includes(e.key) && text
                  ? splitAddresses(text)
                  : null;
              if (!pick) return;
              e.preventDefault();
              e.preventBaseUIHandler();
              add(pick);
            }}
            onPaste={(e) => {
              const pasted = e.clipboardData.getData('text');
              if (!/[,;\n]/.test(pasted)) return;
              e.preventDefault();
              add(splitAddresses(query + pasted));
            }}
            onBlur={(e) => {
              if (e.relatedTarget?.closest('[data-slot=combobox-content]')) return;
              if (query.trim()) add(splitAddresses(query));
            }}
          />
        </ComboboxChips>
        <ComboboxContent anchor={anchor} className="bg-background">
          <ComboboxList>
            {(value: string) => {
              const s = byValue.get(value);
              if (!s) return null;
              return (
                <ComboboxItem key={value} value={value} className={cn('flex-col items-start gap-0 pr-2 text-xs', s.crossAccount && 'opacity-60')}>
                  <span className="flex w-full items-center gap-1.5">
                    <span className="truncate font-medium">{s.name || s.email}</span>
                    {s.crossAccount && <span className="shrink-0 text-[10px]">altra casella</span>}
                    {s.organization && <span className="ml-auto shrink-0 truncate text-[10px] opacity-70">{s.organization}</span>}
                  </span>
                  {s.name && <span className="truncate opacity-70">{s.email}</span>}
                </ComboboxItem>
              );
            }}
          </ComboboxList>
        </ComboboxContent>
      </Combobox>
    </div>
  );
}

function AddressChip({ value }: { value: string }) {
  const { name, email } = parseAddress(value);
  const valid = EMAIL_RE.test(email);
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <ComboboxChip
            className={cn(
              'h-5 rounded-full px-2 text-xs font-normal outline-none focus:bg-accent focus:text-accent-foreground',
              valid ? 'bg-surface-active' : 'bg-destructive/10 text-destructive ring-1 ring-destructive',
            )}
          />
        }
      >
        {name ?? email}
      </TooltipTrigger>
      <TooltipContent>{valid ? (name ? `${name} <${email}>` : email) : `Indirizzo non valido: ${email}`}</TooltipContent>
    </Tooltip>
  );
}

function toSuggestions(hit: ContactHit): Suggestion[] {
  return hit.all_emails.map((email) => ({
    value: hit.display_name ? `${hit.display_name} <${email}>` : email,
    name: hit.display_name,
    email,
    organization: hit.organization,
    crossAccount: hit.is_cross_account,
  }));
}
