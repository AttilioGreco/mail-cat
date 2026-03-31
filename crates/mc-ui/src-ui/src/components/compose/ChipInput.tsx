import { useEffect, useRef, useState, type KeyboardEvent } from 'react';
import { X } from 'lucide-react';
import { invoke } from '@tauri-apps/api/core';
import { cn } from '@/lib/utils';

// Regex RFC-5322 semplificata
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

interface ContactHit {
  id: string;
  display_name: string;
  primary_email: string;
  all_emails: string[];
  organization?: string;
  account_scope?: string;
  is_cross_account: boolean;
}

interface ChipInputProps {
  label: string;
  values: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
  autoFocus?: boolean;
  /** Account ID of the current user — used to scope contact search. */
  accountId?: string;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Parse an RFC 5322 address string into (name, email).
 * "Mario Rossi <mario@example.com>" → { name: "Mario Rossi", email: "mario@example.com" }
 * "mario@example.com"               → { name: null, email: "mario@example.com" }
 */
function parseAddress(value: string): { name: string | null; email: string } {
  const match = value.match(/^(.*?)\s*<([^>]+)>\s*$/);
  if (match) {
    const name = match[1].trim().replace(/^"|"$/g, '');
    return { name: name || null, email: match[2].trim() };
  }
  return { name: null, email: value.trim() };
}

/** Label shown inside the chip: display name if available, else bare email. */
function chipLabel(value: string): string {
  const { name, email } = parseAddress(value);
  return name ?? email;
}

/** Full tooltip: always "Name <email>" or just email. */
function chipTitle(value: string): string {
  const { name, email } = parseAddress(value);
  return name ? `${name} <${email}>` : email;
}

/** Build a canonical chip value from name + email. */
function makeChipValue(name: string | null, email: string): string {
  return name ? `${name} <${email}>` : email;
}

// ── Component ─────────────────────────────────────────────────────────────────

export function ChipInput({ label, values, onChange, placeholder, autoFocus, accountId }: ChipInputProps) {
  const [input, setInput] = useState('');
  const [suggestions, setSuggestions] = useState<ContactHit[]>([]);
  // activeSuggestion: [contactIdx, emailIdx] — emailIdx -1 means the whole row (single-email contact)
  const [active, setActive] = useState<[number, number]>([-1, -1]);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Debounced contact search
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);

    const trimmed = input.trim();
    if (trimmed.length < 1) {
      setSuggestions([]);
      setActive([-1, -1]);
      return;
    }

    debounceRef.current = setTimeout(async () => {
      try {
        const hits = await invoke<ContactHit[]>('search_contacts', {
          query: trimmed,
          limit: 8,
          requestingAccountId: accountId ?? null,
        });
        setSuggestions(hits);
        setActive([-1, -1]);
      } catch {
        setSuggestions([]);
      }
    }, 150);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [input, accountId]);

  // ── Chip enrichment ─────────────────────────────────────────────────────────
  // For bare-email chips (e.g. pre-populated from a reply), look up the contact
  // in the address book and enrich to "Name <email>" if found.
  useEffect(() => {
    const bareEmails = values.filter((v) => {
      const { name } = parseAddress(v);
      return name === null && EMAIL_RE.test(v.trim());
    });
    if (bareEmails.length === 0) return;

    bareEmails.forEach(async (email) => {
      try {
        const hits = await invoke<ContactHit[]>('search_contacts', {
          query: email,
          limit: 1,
          requestingAccountId: accountId ?? null,
        });
        const hit = hits[0];
        if (!hit) return;
        // Only enrich if the returned contact actually owns this email
        const owns = hit.all_emails.some(
          (e) => e.toLowerCase() === email.toLowerCase(),
        );
        if (!owns || !hit.display_name) return;
        const enriched = makeChipValue(hit.display_name, email);
        onChange(values.map((v) => (v === email ? enriched : v)));
      } catch {
        // leave as bare email
      }
    });
    // We intentionally don't list `values` in deps to avoid an infinite loop;
    // enrichment is a one-shot side-effect when the chip is first added.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [values.join('\0'), accountId]);

  // ── Commit helpers ──────────────────────────────────────────────────────────

  const commit = (raw: string) => {
    const value = raw.trim();
    if (!value) return;
    onChange([...values, value]);
    setInput('');
    setSuggestions([]);
    setActive([-1, -1]);
  };

  const commitContact = (hit: ContactHit, email: string) => {
    const value = makeChipValue(hit.display_name || null, email);
    onChange([...values, value]);
    setInput('');
    setSuggestions([]);
    setActive([-1, -1]);
    inputRef.current?.focus();
  };

  // ── Keyboard navigation ─────────────────────────────────────────────────────
  //
  // Flat navigation order: each (contact, emailIdx) pair in display order.
  // For contacts with 1 email: [(ci, 0)].
  // For contacts with N>1 emails: [(ci, 0), (ci, 1), ..., (ci, N-1)].

  type NavItem = { ci: number; ei: number };

  const navItems = (): NavItem[] => {
    const items: NavItem[] = [];
    suggestions.forEach((hit, ci) => {
      hit.all_emails.forEach((_, ei) => items.push({ ci, ei }));
    });
    return items;
  };

  const activeIndex = (): number => {
    if (active[0] < 0) return -1;
    const items = navItems();
    return items.findIndex((it) => it.ci === active[0] && it.ei === active[1]);
  };

  const activateByIndex = (idx: number) => {
    const items = navItems();
    if (idx < 0 || idx >= items.length) { setActive([-1, -1]); return; }
    const { ci, ei } = items[idx];
    setActive([ci, ei]);
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (suggestions.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        activateByIndex(Math.min(activeIndex() + 1, navItems().length - 1));
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        activateByIndex(Math.max(activeIndex() - 1, -1));
        return;
      }
      if ((e.key === 'Tab' || e.key === 'Enter') && active[0] >= 0) {
        e.preventDefault();
        const hit = suggestions[active[0]];
        if (hit) commitContact(hit, hit.all_emails[active[1]]);
        return;
      }
      if (e.key === 'Escape') {
        setSuggestions([]);
        setActive([-1, -1]);
        return;
      }
    }

    if ([',', ';', 'Tab', 'Enter'].includes(e.key)) {
      e.preventDefault();
      commit(input);
    } else if (e.key === 'Backspace' && input === '' && values.length > 0) {
      onChange(values.slice(0, -1));
    }
  };

  const handleBlur = () => {
    setTimeout(() => {
      if (input.trim()) commit(input);
      setSuggestions([]);
    }, 150);
  };

  const remove = (v: string) => {
    onChange(values.filter((x) => x !== v));
  };

  // ── Render ──────────────────────────────────────────────────────────────────

  return (
    <div className="relative flex items-start gap-2 px-4 py-2 border-b border-border min-h-[40px]">
      <span className="shrink-0 text-xs text-muted pt-1.5 w-14">{label}</span>
      <div
        className="flex flex-1 flex-wrap gap-1 cursor-text"
        onClick={() => inputRef.current?.focus()}
      >
        {values.map((v) => {
          const { email } = parseAddress(v);
          const isValid = EMAIL_RE.test(email);
          return (
            <span
              key={v}
              title={chipTitle(v)}
              className={cn(
                'flex items-center gap-1 rounded-full px-2 py-0.5 text-xs',
                !isValid
                  ? 'border border-destructive bg-destructive/10 text-destructive'
                  : 'bg-surface-active text-foreground',
              )}
            >
              {chipLabel(v)}
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); remove(v); }}
                className="rounded-full opacity-60 hover:opacity-100"
              >
                <X size={10} />
              </button>
            </span>
          );
        })}
        <input
          ref={inputRef}
          value={input}
          autoFocus={autoFocus}
          placeholder={values.length === 0 ? placeholder : undefined}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          onBlur={handleBlur}
          className="min-w-[120px] flex-1 bg-transparent text-xs text-foreground outline-none placeholder:text-muted"
        />
      </div>

      {/* Autocomplete dropdown */}
      {suggestions.length > 0 && (
        <ul className="absolute left-0 top-full z-50 w-full rounded-md border border-border bg-surface shadow-lg overflow-hidden">
          {suggestions.map((hit, ci) => (
            <li key={hit.id} className={cn(hit.is_cross_account && 'opacity-60')}>
              {hit.all_emails.length === 1 ? (
                // ── Single-email contact: one clickable row ──────────────────
                <button
                  type="button"
                  onMouseDown={(e) => { e.preventDefault(); commitContact(hit, hit.all_emails[0]); }}
                  className={cn(
                    'flex w-full flex-col px-3 py-2 text-left text-xs hover:bg-surface-active transition-colors',
                    active[0] === ci && active[1] === 0 && 'bg-surface-active',
                  )}
                >
                  <ContactRow hit={hit} email={hit.all_emails[0]} />
                </button>
              ) : (
                // ── Multi-email contact: header + one sub-row per email ──────
                <div>
                  <div className="flex items-center gap-1.5 px-3 pt-2 pb-1">
                    <span className={cn(
                      'font-medium text-xs truncate',
                      hit.is_cross_account ? 'text-muted' : 'text-foreground',
                    )}>
                      {hit.display_name || hit.primary_email}
                    </span>
                    {hit.is_cross_account && (
                      <span className="shrink-0 rounded px-1 py-0.5 text-[10px] bg-warning/20 text-warning">
                        altra casella
                      </span>
                    )}
                    {hit.organization && (
                      <span className="ml-auto shrink-0 text-[10px] text-muted/70 truncate">{hit.organization}</span>
                    )}
                  </div>
                  {hit.all_emails.map((email, ei) => (
                    <button
                      key={email}
                      type="button"
                      onMouseDown={(e) => { e.preventDefault(); commitContact(hit, email); }}
                      className={cn(
                        'flex w-full items-center gap-2 pl-6 pr-3 py-1.5 text-left text-xs hover:bg-surface-active transition-colors',
                        active[0] === ci && active[1] === ei && 'bg-surface-active',
                      )}
                    >
                      <span className="text-muted truncate">{email}</span>
                      {email === hit.primary_email && (
                        <span className="shrink-0 rounded px-1 py-0.5 text-[10px] bg-surface-hover text-muted">
                          principale
                        </span>
                      )}
                    </button>
                  ))}
                  <div className="h-px bg-border mx-3 mb-1" />
                </div>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ContactRow({ hit, email }: { hit: ContactHit; email: string }) {
  return (
    <>
      <div className="flex items-center gap-1.5">
        <span className={cn(
          'font-medium truncate',
          hit.is_cross_account ? 'text-muted' : 'text-foreground',
        )}>
          {hit.display_name || email}
        </span>
        {hit.is_cross_account && (
          <span className="shrink-0 rounded px-1 py-0.5 text-[10px] bg-warning/20 text-warning">
            altra casella
          </span>
        )}
      </div>
      {hit.display_name && (
        <span className="text-muted truncate">{email}</span>
      )}
      {hit.organization && (
        <span className="text-muted/70 truncate">{hit.organization}</span>
      )}
    </>
  );
}
