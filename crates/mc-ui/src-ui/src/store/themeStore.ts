import { create } from 'zustand';

export type AppTheme = 'system' | 'light' | 'dark';
export type MailContentMode = 'auto' | 'light' | 'invert';

const APP_THEME_KEY = 'mc.appTheme';
const MAIL_MODE_KEY = 'mc.mailContentMode';

function getStoredAppTheme(): AppTheme {
  const v = localStorage.getItem(APP_THEME_KEY);
  if (v === 'light' || v === 'dark' || v === 'system') return v;
  return 'system';
}

function getStoredMailMode(): MailContentMode {
  const v = localStorage.getItem(MAIL_MODE_KEY);
  if (v === 'auto' || v === 'light' || v === 'invert') return v;
  // Default to light for readability in dark app shells.
  return 'light';
}

function resolveTheme(theme: AppTheme): 'light' | 'dark' {
  if (theme === 'light' || theme === 'dark') return theme;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function applyTheme(theme: AppTheme): void {
  const resolved = resolveTheme(theme);
  document.documentElement.setAttribute('data-theme', resolved);
}

interface ThemeState {
  appTheme: AppTheme;
  mailContentMode: MailContentMode;
  setAppTheme: (theme: AppTheme) => void;
  setMailContentMode: (mode: MailContentMode) => void;
  applyAppTheme: () => void;
}

export const useThemeStore = create<ThemeState>((set, get) => ({
  appTheme: getStoredAppTheme(),
  mailContentMode: getStoredMailMode(),

  setAppTheme: (theme) => {
    localStorage.setItem(APP_THEME_KEY, theme);
    set({ appTheme: theme });
    applyTheme(theme);
  },

  setMailContentMode: (mode) => {
    localStorage.setItem(MAIL_MODE_KEY, mode);
    set({ mailContentMode: mode });
  },

  applyAppTheme: () => applyTheme(get().appTheme),
}));
