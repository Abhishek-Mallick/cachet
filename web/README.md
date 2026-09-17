# Cachet documentation site

The docs at [`content/docs`](./content/docs), rendered with
[Fumadocs](https://fumadocs.dev) on Next.js.

```bash
npm install
npm run dev     # http://localhost:3000
npm run build
```

## Layout

| Path | What it is |
|---|---|
| `content/docs/*.mdx` | The documentation itself. Editing these is the whole job |
| `content/docs/meta.json` | Sidebar order and section dividers |
| `app/(home)/page.tsx` | The landing page |
| `components/logo.tsx` | The header lockup |
| `lib/source.ts` | Wires the MDX collection to the router |

## A note on the logo

`public/cachet-logo.png` has a **white wordmark on a transparent background**, so it disappears on a
light surface. Both the header and the hero place it on a dark chip rather than shipping a second
asset — see `components/logo.tsx`. If a light-background variant is ever added, that is the one
place to change.

## Keeping it honest

The docs describe behaviour that is covered by tests in this repository. When a claim here stops
matching `make test-consistency` or `make test-e2e`, the docs are what is wrong.
