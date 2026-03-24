import { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { Search, X, Mail, User, FileText, AtSign } from 'lucide-react';
import { useUIStore } from '@/store/uiStore';
import { useSuggestions } from '@/hooks/useMessages';
import { cn } from '@/lib/utils';
import type { Suggestion } from '@/lib/types';

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
  const { searchQuery, setSearchQuery } = useUIStore();
  const [input, setInput] = useState(searchQuery ?? '');
  const [showDropdown, setShowDropdown] = useState(false);
  const [highlightIndex, setHighlightIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [debouncedInput, setDebouncedInput] = useState('');

  const { scope, prefix } = useMemo(() => parseInput(debouncedInput), [debouncedInput]);

  const { data: suggestions = [] } = useSuggestions(
    prefix.length >= 2 ? prefix : null,
    scope,
  );

  const { groups, total } = useMemo(() => buildGroups(suggestions), [suggestions]);

  // Sync external clear (e.g. selecting a mailbox clears query)
  useEffect(() => {
    if (searchQuery === null) {
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

  const handleSelect = useCallback((s: Suggestion) => {
    const query = buildQueryFromSuggestion(s);
    setInput(query);
    submitSearch(query);
  }, [buildQueryFromSuggestion, submitSearch]);

  // Find suggestion by flat index
  const getSuggestionByIndex = useCallback((idx: number): Suggestion | null => {
    for (const group of groups) {
      for (const item of group.items) {
        if (item.flatIndex === idx) return item.suggestion;
      }
    }
    return null;
  }, [groups]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
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
      case 'Escape':
        setShowDropdown(false);
        setHighlightIndex(-1);
        break;
    }
  };

  const shouldShowDropdown = showDropdown && total > 0 && prefix.length >= 2;

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
          placeholder="Search… (sbj: from: to:)"
          className="h-8 w-full rounded-md border border-border bg-background pl-8 pr-8 text-xs text-foreground placeholder:text-muted outline-none focus:border-accent focus:ring-1 focus:ring-accent/30"
        />
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
          {groups.map((group) => (
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
                    flatIndex === highlightIndex
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
