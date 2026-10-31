/** Unread count as shown on the rail badge; null hides it. */
export function formatBadge(count: number): string | null {
  if (count <= 0) return null;
  return count > 99 ? '99+' : String(count);
}

/** Two-letter avatar label: initials of the first two words, or the first two characters of a single word. */
export function accountInitials(name: string, email: string): string {
  const words = (name.trim() || email).split(/[\s._@-]+/).filter(Boolean);
  const label = words.length > 1 ? words[0][0] + words[1][0] : (words[0] ?? '?').slice(0, 2);
  return label.toUpperCase();
}
