const HUES = [
  ['red', 'Rosso'],
  ['orange', 'Arancione'],
  ['amber', 'Ambra'],
  ['yellow', 'Giallo'],
  ['lime', 'Lime'],
  ['green', 'Verde'],
  ['teal', 'Verde acqua'],
  ['cyan', 'Azzurro'],
  ['blue', 'Blu'],
  ['indigo', 'Indaco'],
  ['purple', 'Viola'],
  ['pink', 'Rosa'],
] as const;

export interface LabelColor {
  /** What the server stores; the CSS variable `--label-<key>` holds the shade per theme. */
  key: string;
  name: string;
}

/** The 24 palette keys: each hue vivid, then soft. Must match `LabelSettings::is_color` in mc-domain. */
export const LABEL_COLORS: LabelColor[] = HUES.flatMap(([key, name]) => [
  { key, name },
  { key: `${key}-soft`, name: `${name} tenue` },
]);

/** Inline style for a palette key, or none when the key is absent or unknown to this build. */
export function labelColorStyle(key: string | null | undefined): { backgroundColor: string } | undefined {
  return key && LABEL_COLORS.some((c) => c.key === key) ? { backgroundColor: `hsl(var(--label-${key}))` } : undefined;
}

/** How the server keys a label's settings. */
export const labelKey = (displayPath: string) => displayPath.trim().toLowerCase();
