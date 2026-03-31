import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useEffect } from 'react';
import { AppShell } from '@/components/layout/AppShell';
import { useThemeStore } from '@/store/themeStore';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, refetchOnWindowFocus: false },
  },
});

export default function App() {
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

  return (
    <QueryClientProvider client={queryClient}>
      <AppShell />
    </QueryClientProvider>
  );
}
