import './global.css';
import { RootProvider } from 'fumadocs-ui/provider/next';
import type { ReactNode } from 'react';

export const metadata = {
  title: {
    default: 'Cachet — a read cache that proves its own correctness',
    template: '%s · Cachet',
  },
  description:
    'An integrated read cache for sharded OLTP databases. Exact invalidation, consistency you choose per request, and a verifier that publishes your real consistency as a number.',
};

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex min-h-screen flex-col">
        <RootProvider>{children}</RootProvider>
      </body>
    </html>
  );
}
