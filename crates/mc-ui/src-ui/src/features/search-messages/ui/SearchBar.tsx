import { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { Search, X, Mail, User, FileText, AtSign, CircleHelp } from 'lucide-react';
import { useSearchStore } from '../model/search-store';
import { useSuggestions, useLabelNames } from '../model/useSearch';
import { OPERATORS, completionsFor, type Completion } from '../model/operators';
import { Popover, PopoverTrigger, PopoverContent } from '@/shared/ui/popover';
import { cn } from '@/shared/lib/utils';
import type { Suggestion } from '../model/types';

/** Known prefix scopes: typing "sbj:" scopes to subject, etc. */
const SCOPE_PREFIXES: Record<string, string> = {
  'sbj:': 'subject',
  'from:': 'from',
  'to:': 'to',
};

/** Parse input into { scope, prefix } for the suggest API */
function parseInput(raw: string): { scope: string | null; prefix: string } {
  const trimmed = raw.trimStart();
  for (const [key, scope] of Object.entries(SCOPE_PREFIXES)) {
    if (trimmed.toLowerCase().startsWith(key)) {
      return { scope, prefix: trimmed.slice(key.length).trimStart() };
    }
  }
  return { scope: null, prefix: trimmed };
}

/** Group suggestions by field in display order, with a flat index per item */
interface FlatSuggestion {
  suggestion: Suggestion;
  flatIndex: number;
}

interface SuggestionGroup {
  field: string;
  label: string;
  items: FlatSuggestion[];
}

function buildGroups(suggestions: Suggestion[]): { groups: SuggestionGroup[]; total: number } {
  const fieldOrder: { key: string; label: string }[] = [
    { key: 'from_email', label: 'From (email)' },
    { key: 'from_name', label: 'From (name)' },
    { key: 'to', label: 'To' },
    { key: 'subject', label: 'Subject' },
    { key: 'body_text', label: 'Body' },
  ];

  const groups: SuggestionGroup[] = [];
  let idx = 0;

  for (const { key, label } of fieldOrder) {
    const items = suggestions
      .filter((s) => s.field === key)
      .map((s) => ({ suggestion: s, flatIndex: idx++ }));
    if (items.length > 0) {
      groups.push({ field: key, label, items });
    }
  }

  return { groups, total: idx };
}

function FieldIcon({ field }: { field: string }) {
  switch (field) {
    case 'from_email': return <AtSign size={12} />;
    case 'from_name': return <User size={12} />;
    case 'to': return <Mail size={12} />;
    case 'subject': return <FileText size={12} />;
    default: return <Search size={12} />;
  }
}

export function SearchBar() {
  const { searchQuery, setSearchQuery } = useSearchStore();
  const focusSignal = useSearchStore((s) => s.focusSignal);
  const [input, setInput] = useState(searchQuery ?? '');
  const [showDropdown, setShowDropdown] = useState(false);
  const [highlightIndex, setHighlightIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [debouncedInput, setDebouncedInput] = useState('');

  // The search shortcut lives wherever the keymap is bound, not in here, so it
  // asks for focus through the store.
  useEffect(() => {
    if (focusSignal > 0) inputRef.current?.focus();
  }, [focusSignal]);

  const { scope, prefix } = useMemo(() => parseInput(debouncedInput), [debouncedInput]);

  const { data: suggestions = [] } = useSuggestions(
    prefix.length >= 2 ? prefix : null,
    scope,
  );

  const { groups, total: suggestionCount } = useMemo(() => buildGroups(suggestions), [suggestions]);

  // Operator and value completions come first in the list, then the suggestions.
  const { data: labelNames = [] } = useLabelNames(/label:/i.test(input));
  const completion = useMemo(() => completionsFor(input, labelNames), [input, labelNames]);
  const completions = completion.items;
  const total = completions.length + suggestionCount;

  // Sync external clear (e.g. selecting a mailbox clears query)
  useEffect(() => {
    if (searchQuery === null) {
      // ponytail: reacting to an external zustand store change, not a
      // prop->state derivation within this component's own render.
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setInput('');
      setDebouncedInput('');
    }
  }, [searchQuery]);

  // Debounce input for suggest API (200ms)
  const handleChange = useCallback((value: string) => {
    setInput(value);
    setHighlightIndex(-1);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      setDebouncedInput(value);
    }, 200);
  }, []);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        dropdownRef.current && !dropdownRef.current.contains(target) &&
        inputRef.current && !inputRef.current.contains(target)
      ) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const submitSearch = useCallback((query: string) => {
    const trimmed = query.trim();
    setSearchQuery(trimmed.length > 0 ? trimmed : null);
    setShowDropdown(false);
    setHighlightIndex(-1);
  }, [setSearchQuery]);

  // Build a scoped query string from a selected suggestion
  const buildQueryFromSuggestion = useCallback((s: Suggestion): string => {
    switch (s.field) {
      case 'from_email':
      case 'from_name':
        return `from: ${s.value}`;
      case 'to':
        return `to: ${s.value}`;
      case 'subject':
        return `sbj: ${s.value}`;
      default:
        return s.value;
    }
  }, []);

  const handleComplete = useCallback((c: Completion) => {
    const next = input.slice(0, completion.start) + c.insert;
    setInput(next);
    setDebouncedInput(next);
    setHighlightIndex(-1);
    inputRef.current?.focus();
  }, [input, completion.start]);

  const handleSelect = useCallback((s: Suggestion) => {
    const query = buildQueryFromSuggestion(s);
    setInput(query);
    submitSearch(query);
  }, [buildQueryFromSuggestion, submitSearch]);

  // Find suggestion by flat index
  const getSuggestionByIndex = useCallback((idx: number): Suggestion | null => {
    for (const group of groups) {
      for (const item of group.items) {
        if (item.flatIndex + completions.length === idx) return item.suggestion;
      }
    }
    return null;
  }, [groups, completions.length]);

  const handleSubmit = (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (highlightIndex >= 0 && highlightIndex < completions.length) {
      handleComplete(completions[highlightIndex]);
      return;
    }
    if (highlightIndex >= 0) {
      const s = getSuggestionByIndex(highlightIndex);
      if (s) {
        handleSelect(s);
        return;
      }
    }
    submitSearch(input);
  };

  const handleClear = () => {
    setInput('');
    setDebouncedInput('');
    setSearchQuery(null);
    setShowDropdown(false);
    inputRef.current?.focus();
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!showDropdown || total === 0) return;

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setHighlightIndex((prev) => (prev < total - 1 ? prev + 1 : 0));
        break;
      case 'ArrowUp':
        e.preventDefault();
        setHighlightIndex((prev) => (prev > 0 ? prev - 1 : total - 1));
        break;
      case 'Tab':
        if (completions.length === 0) break;
        e.preventDefault();
        handleComplete(completions[Math.max(highlightIndex, 0)] ?? completions[0]);
        break;
      case 'Escape':
        setShowDropdown(false);
        setHighlightIndex(-1);
        break;
    }
  };

  const shouldShowSuggestions = suggestionCount > 0 && prefix.length >= 2;
  const shouldShowDropdown = showDropdown && (completions.length > 0 || shouldShowSuggestions);

  return (
    <div className="relative">
      <form onSubmit={handleSubmit} className="relative flex items-center">
        <Search size={14} className="absolute left-2.5 text-muted" />
        <input
          ref={inputRef}
          type="text"
          value={input}
          onChange={(e) => handleChange(e.target.value)}
          onFocus={() => setShowDropdown(true)}
          onKeyDown={handleKeyDown}
          placeholder="Cerca… (from: label: in: is:)"
          className="h-8 w-full rounded-md border border-border bg-background pl-8 pr-8 text-xs text-foreground placeholder:text-muted outline-none focus:border-accent focus:ring-1 focus:ring-accent/30"
        />
        <SearchHelp hasClear={input.length > 0} />
        {input.length > 0 && (
          <button
            type="button"
            onClick={handleClear}
            className="absolute right-2 text-muted hover:text-foreground"
          >
            <X size={13} />
          </button>
        )}
      </form>

      {shouldShowDropdown && (
        <div
          ref={dropdownRef}
          className="absolute left-0 right-0 top-full z-50 mt-1 max-h-72 overflow-y-auto rounded-md border border-border bg-background shadow-lg"
        >
          {completions.map((c, i) => (
            <button
              key={c.insert}
              type="button"
              // Keeps the input focused, so the list does not close under the click.
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => handleComplete(c)}
              className={cn(
                'flex w-full items-center justify-between gap-3 px-3 py-1.5 text-xs text-foreground transition-colors',
                i === highlightIndex ? 'bg-accent/15 text-accent' : 'hover:bg-surface-hover',
              )}
            >
              <span className="truncate font-mono">{c.label}</span>
              {c.hint && <span className="truncate text-[10px] text-muted">{c.hint}</span>}
            </button>
          ))}
          {shouldShowSuggestions && groups.map((group) => (
            <div key={group.field}>
              <div className="flex items-center gap-1.5 px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted">
                <FieldIcon field={group.field} />
                {group.label}
              </div>
              {group.items.map(({ suggestion, flatIndex }) => (
                <button
                  key={`${group.field}-${suggestion.value}`}
                  type="button"
                  onClick={() => handleSelect(suggestion)}
                  className={cn(
                    'flex w-full items-center justify-between px-3 py-1.5 text-xs text-foreground transition-colors',
                    flatIndex + completions.length === highlightIndex
                      ? 'bg-accent/15 text-accent'
                      : 'hover:bg-surface-hover',
                  )}
                >
                  <span className="truncate">{suggestion.value}</span>
                  <span className="ml-2 shrink-0 text-[10px] text-muted">
                    {suggestion.docCount}
                  </span>
                </button>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function SearchHelp({ hasClear }: { hasClear: boolean }) {
  return (
    <Popover>
      <PopoverTrigger
        aria-label="Operatori di ricerca"
        className={cn('absolute text-muted hover:text-foreground', hasClear ? 'right-7' : 'right-2')}
      >
        <CircleHelp size={13} />
      </PopoverTrigger>
      <PopoverContent align="end" className="max-h-96 w-96 overflow-y-auto p-3">
        <div className="mb-2 text-xs font-semibold">Operatori di ricerca</div>
        <dl className="space-y-1.5 text-xs">
          {OPERATORS.map((o) => (
            <div key={o.name}>
              <dt className="font-mono text-accent">{o.example}</dt>
              <dd className="text-muted">{o.hint}</dd>
            </div>
          ))}
        </dl>
        <p className="mt-3 text-[11px] leading-snug text-muted">
          Più parole si combinano (AND); <span className="font-mono">OR</span> tra due termini,{' '}
          <span className="font-mono">-</span> davanti per escludere, <span className="font-mono">&quot;frase&quot;</span> tra
          virgolette, parentesi per raggruppare. Esempio:{' '}
          <span className="font-mono">label:Dev -is:read after:2026-01-01</span>
        </p>
      </PopoverContent>
    </Popover>
  );
}
