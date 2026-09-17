import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { Logo } from '@/components/logo';

export const baseOptions: BaseLayoutProps = {
  nav: {
    title: <Logo />,
  },
  links: [
    {
      text: 'Documentation',
      url: '/docs',
      active: 'nested-url',
    },
    {
      text: 'GitHub',
      url: 'https://github.com/Abhishek-Mallick/cachet',
      external: true,
    },
  ],
};
