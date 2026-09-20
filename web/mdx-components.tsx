import defaultComponents from 'fumadocs-ui/mdx';
import type { MDXComponents } from 'mdx/types';
import { Bars, BeforeAfter, GroupedBars, LogBars, RangeBars } from '@/components/charts';

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultComponents,
    // Charts are available to every page without an import, so a doc that wants to show a figure
    // rather than a table does not have to remember plumbing.
    Bars,
    BeforeAfter,
    GroupedBars,
    LogBars,
    RangeBars,
    ...components,
  };
}
