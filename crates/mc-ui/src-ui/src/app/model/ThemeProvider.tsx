import { useEffect, type ReactNode } from 'react';
import { useThemeStore } from '@/entities/theme';

export function ThemeProvider({ children }: { children: ReactNode }) {
  const { appTheme, applyAppTheme } = useThemeStore();

  useEffect(() => {
    applyAppTheme();

    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const onChange = () => {
      if (useThemeStore.getState().appTheme === 'system') {
        useThemeStore.getState().applyAppTheme();
      }
    };

    mq.addEventListener('change', onChange);
    return () => mq.removeEventListener('change', onChange);
  }, [appTheme, applyAppTheme]);

  return <>{children}</>;
}
