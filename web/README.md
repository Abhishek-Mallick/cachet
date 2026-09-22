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

There are two files, and the difference is only the wordmark:

| File | Wordmark | Used on |
|---|---|---|
| `public/cachet.png` | White | Dark backgrounds |

Every pixel of the mark — the teal gradient — is identical in both, because it reads on either
ground. The
light variant is generated from the original by `scripts/derive-light-logo.py`, which recolours only
the achromatic pixels — so it stays in sync by re-running a script rather than by someone
remembering to re-export it. Run that script when the source logo changes and commit both outputs.

Both are rendered wherever the lockup appears and one is hidden with `dark:hidden` / `dark:block`,
so the correct one is in the server-rendered HTML and there is no flash of the wrong lockup on load.
Picking in JavaScript would guarantee that flash, since the theme is not known until hydration.

## Keeping it honest

The docs describe behaviour that is covered by tests in this repository. When a claim here stops
matching `make test-consistency` or `make test-e2e`, the docs are what is wrong.
