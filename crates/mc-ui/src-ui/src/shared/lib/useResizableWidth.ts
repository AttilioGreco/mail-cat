import { useCallback, useState } from 'react';

function getStoredWidth(key: string, fallback: number, min: number, max: number): number {
  const v = Number(localStorage.getItem(key));
  if (!Number.isFinite(v) || v === 0) return fallback;
  return Math.min(max, Math.max(min, v));
}

/** A pane width the user drags, clamped to [min, max] and persisted under `key`. */
export function useResizableWidth(key: string, fallback: number, min: number, max: number) {
  const [width, setWidth] = useState(() => getStoredWidth(key, fallback, min, max));

  const resize = useCallback(
    (next: number) => setWidth(Math.min(max, Math.max(min, Math.round(next)))),
    [min, max],
  );
  const commit = useCallback((w: number) => localStorage.setItem(key, String(w)), [key]);
  const reset = useCallback(() => {
    setWidth(fallback);
    localStorage.removeItem(key);
  }, [key, fallback]);

  return { width, resize, commit, reset };
}
