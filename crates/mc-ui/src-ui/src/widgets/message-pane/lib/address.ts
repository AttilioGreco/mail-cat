export interface Address {
  name: string;
  email: string;
}

/** Parses `Name <a@b.it>`, `"Name" <a@b.it>` or a bare address. */
export function parseAddress(raw: string): Address {
  const m = /^\s*"?([^"<]*?)"?\s*<([^>]+)>\s*$/.exec(raw);
  return m ? { name: m[1], email: m[2].trim() } : { name: '', email: raw.trim() };
}
