/**
 * Dependency-free SVG charts.
 *
 * No charting library: every number these draw is a published benchmark, and the whole point of the
 * benchmark discipline is that a reader can trace a figure to the run that produced it. A hundred
 * kilobytes of charting runtime to draw nine bars would be a poor trade, and the CSP on this site
 * forbids most external scripts anyway.
 *
 * Colours come from the fumadocs theme tokens, so the charts follow the light/dark toggle without
 * a second palette to keep in sync.
 */

export type Series = { label: string; values: number[]; muted?: boolean };

function niceMax(values: number[]): number {
  const max = Math.max(...values, 0);
  if (max <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(max));
  return Math.ceil(max / magnitude) * magnitude;
}

/**
 * Grouped horizontal bars.
 *
 * Horizontal because the category labels are words rather than dates: vertical bars would need
 * rotated labels, which nobody reads.
 */
export function GroupedBars({
  categories,
  series,
  unit,
  format,
}: {
  categories: string[];
  series: Series[];
  unit?: string;
  format?: (n: number) => string;
}) {
  const max = niceMax(series.flatMap((s) => s.values));
  const fmt = format ?? ((n: number) => `${n}${unit ?? ''}`);

  return (
    <figure className="my-6 not-prose">
      <div className="mb-3 flex flex-wrap gap-4 text-xs text-fd-muted-foreground">
        {series.map((s, i) => (
          <span key={s.label} className="inline-flex items-center gap-1.5">
            <span
              aria-hidden
              className="inline-block size-2.5 rounded-sm"
              style={{ background: barColor(i, s.muted) }}
            />
            {s.label}
          </span>
        ))}
      </div>

      <div className="space-y-3">
        {categories.map((cat, ci) => (
          <div key={cat} className="grid grid-cols-[4.5rem_1fr] items-center gap-3">
            <div className="text-right font-mono text-xs text-fd-muted-foreground">{cat}</div>
            <div className="space-y-1">
              {series.map((s, si) => {
                const v = s.values[ci];
                const pct = max > 0 ? (v / max) * 100 : 0;
                return (
                  <div key={s.label} className="flex items-center gap-2">
                    <div className="h-4 flex-1 overflow-hidden rounded-sm bg-fd-secondary/60">
                      <div
                        className="h-full rounded-sm"
                        style={{ width: `${pct}%`, background: barColor(si, s.muted) }}
                        role="img"
                        aria-label={`${s.label}, ${cat}: ${fmt(v)}`}
                      />
                    </div>
                    <div className="w-20 shrink-0 font-mono text-xs tabular-nums">{fmt(v)}</div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </figure>
  );
}

/** A single series of bars, for a straight comparison between configurations. */
export function Bars({
  data,
  format,
  highlight,
}: {
  data: { label: string; value: number; note?: string }[];
  format?: (n: number) => string;
  highlight?: string;
}) {
  const max = niceMax(data.map((d) => d.value));
  const fmt = format ?? ((n: number) => String(n));

  return (
    <figure className="my-6 space-y-2 not-prose">
      {data.map((d) => {
        const pct = max > 0 ? (d.value / max) * 100 : 0;
        const isHighlight = d.label === highlight;
        return (
          <div key={d.label} className="grid grid-cols-[11rem_1fr] items-center gap-3">
            <div className="truncate text-right text-xs text-fd-muted-foreground">{d.label}</div>
            <div className="flex items-center gap-2">
              <div className="h-5 flex-1 overflow-hidden rounded-sm bg-fd-secondary/60">
                <div
                  className="h-full rounded-sm transition-[width]"
                  style={{
                    width: `${pct}%`,
                    background: isHighlight ? 'var(--color-fd-primary)' : barColor(1, true),
                  }}
                  role="img"
                  aria-label={`${d.label}: ${fmt(d.value)}`}
                />
              </div>
              <div className="w-24 shrink-0 font-mono text-xs tabular-nums">
                {fmt(d.value)}
                {d.note ? (
                  <span className="ml-1 text-fd-muted-foreground">{d.note}</span>
                ) : null}
              </div>
            </div>
          </div>
        );
      })}
    </figure>
  );
}

/**
 * A range bar: the measured value with the spread it was drawn from.
 *
 * This chart exists because of a specific honesty problem. Three of this project's p99 figures have
 * run-to-run spreads wider than the differences between them, and a plain bar chart would render
 * them as a confident result. Drawing the spread makes "nothing has been measured here" visible
 * rather than something a reader has to find in a footnote.
 */
export function RangeBars({
  data,
  unit = 'ms',
}: {
  data: { label: string; value: number; low: number; high: number }[];
  unit?: string;
}) {
  const max = niceMax(data.map((d) => d.high));

  return (
    <figure className="my-6 space-y-3 not-prose">
      {data.map((d) => {
        const pos = (n: number) => (max > 0 ? (n / max) * 100 : 0);
        return (
          <div key={d.label} className="grid grid-cols-[11rem_1fr] items-center gap-3">
            <div className="truncate text-right text-xs text-fd-muted-foreground">{d.label}</div>
            <div className="flex items-center gap-2">
              <div className="relative h-6 flex-1 rounded-sm bg-fd-secondary/60">
                <div
                  className="absolute inset-y-1.5 rounded-sm opacity-40"
                  style={{
                    left: `${pos(d.low)}%`,
                    width: `${Math.max(pos(d.high) - pos(d.low), 0.5)}%`,
                    background: 'var(--color-fd-primary)',
                  }}
                  aria-hidden
                />
                <div
                  className="absolute inset-y-0 w-0.5"
                  style={{ left: `${pos(d.value)}%`, background: 'var(--color-fd-primary)' }}
                  role="img"
                  aria-label={`${d.label}: ${d.value}${unit}, spread ${d.low}–${d.high}${unit}`}
                />
              </div>
              <div className="w-32 shrink-0 font-mono text-xs tabular-nums">
                {d.value}
                {unit}
                <span className="ml-1 text-fd-muted-foreground">
                  ({d.low}–{d.high})
                </span>
              </div>
            </div>
          </div>
        );
      })}
    </figure>
  );
}

function barColor(index: number, muted?: boolean): string {
  if (muted) return 'color-mix(in oklch, var(--color-fd-muted-foreground) 55%, transparent)';
  return index === 0
    ? 'color-mix(in oklch, var(--color-fd-muted-foreground) 55%, transparent)'
    : 'var(--color-fd-primary)';
}
