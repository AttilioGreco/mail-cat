import { v4 as uuidv4 } from 'uuid';
import type { InlineImage } from '../model/composeTypes';

const ACCEPTED_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/svg+xml']);
const MAX_SIZE_BYTES = 10 * 1024 * 1024;

/** Image files in a drop or paste. */
export function extractImageFiles(dataTransfer: DataTransfer): File[] {
  const files = Array.from(dataTransfer.files).filter((f) => ACCEPTED_TYPES.has(f.type));
  if (files.length > 0) return files;
  // A pasted screenshot arrives as an item, not in `files`.
  return Array.from(dataTransfer.items)
    .filter((i) => i.kind === 'file' && ACCEPTED_TYPES.has(i.type))
    .flatMap((i) => i.getAsFile() ?? []);
}

/**
 * Turns image files into inline images, each pointing at an object URL until
 * ComposeWindow swaps it for its cid: on save/send. Rejected files are reported.
 */
export function toInlineImages(files: File[], onError?: (msg: string) => void): InlineImage[] {
  const images: InlineImage[] = [];
  for (const file of files) {
    if (!ACCEPTED_TYPES.has(file.type)) {
      onError?.(`Formato non supportato: ${file.type}`);
    } else if (file.size > MAX_SIZE_BYTES) {
      onError?.(`Immagine troppo grande (max 10 MB): ${(file.size / 1024 / 1024).toFixed(1)} MB`);
    } else {
      images.push({ cid: `${uuidv4()}@compose`, blob: file, mimeType: file.type, url: URL.createObjectURL(file) });
    }
  }
  return images;
}
