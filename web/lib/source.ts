import { docs } from '@/.source/server';
import { loader } from 'fumadocs-core/source';

// The content source. Docs live in content/docs and are served from /docs.
export const source = loader({
  baseUrl: '/docs',
  source: docs.toFumadocsSource(),
});
