import { MainPage } from '@/pages/main';
import { QueryProvider } from './providers/QueryProvider';
import { ThemeProvider } from './providers/ThemeProvider';

export default function App() {
  return (
    <QueryProvider>
      <ThemeProvider>
        <MainPage />
      </ThemeProvider>
    </QueryProvider>
  );
}
