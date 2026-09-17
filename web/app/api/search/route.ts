import { source } from '@/lib/source';
import { createFromSource } from 'fumadocs-core/search/server';

// Static search index. The docs are a handful of pages, so a server-side search service would be
// infrastructure to run for a problem that does not exist yet.
export const { GET } = createFromSource(source);
