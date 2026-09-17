import Link from 'next/link';
import Image from 'next/image';

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col">
      {/* Hero */}
      <section className="mx-auto flex w-full max-w-5xl flex-col items-center px-6 pt-20 pb-16 text-center">
        {/* One asset per theme, swapped by CSS rather than JavaScript so the correct lockup is in
            the server-rendered HTML. See components/logo.tsx. */}
        <Image
          src="/cachet-logo-on-light.png"
          alt="Cachet"
          width={420}
          height={140}
          priority
          className="h-16 w-auto dark:hidden"
        />
        <Image
          src="/cachet-logo.png"
          alt=""
          aria-hidden
          width={420}
          height={140}
          priority
          className="hidden h-16 w-auto dark:block"
        />

        <h1 className="mt-10 max-w-3xl text-4xl font-semibold tracking-tight text-balance sm:text-5xl">
          A read cache that proves its own correctness
        </h1>

        <p className="mt-6 max-w-2xl text-lg text-fd-muted-foreground text-balance">
          Cachet sits inside the read path of a sharded OLTP database, where writes are actually
          visible. Invalidation is exact instead of guessed — and a verifier publishes your real
          consistency as a number.
        </p>

        <div className="mt-10 flex flex-wrap items-center justify-center gap-3">
          <Link
            href="/docs"
            className="rounded-lg bg-fd-primary px-5 py-2.5 text-sm font-medium text-fd-primary-foreground transition-opacity hover:opacity-90"
          >
            Read the docs
          </Link>
          <Link
            href="/docs/quickstart"
            className="rounded-lg border border-fd-border px-5 py-2.5 text-sm font-medium transition-colors hover:bg-fd-accent"
          >
            Quickstart
          </Link>
          <a
            href="https://github.com/Abhishek-Mallick/cachet"
            className="rounded-lg border border-fd-border px-5 py-2.5 text-sm font-medium transition-colors hover:bg-fd-accent"
          >
            GitHub
          </a>
        </div>

        <p className="mt-6 text-xs text-fd-muted-foreground">
          Apache 2.0 · Go 1.27 · MySQL/MyRocks + Valkey or Redis · pre-1.0
        </p>
      </section>

      {/* The question everyone asks first */}
      <section className="mx-auto w-full max-w-4xl px-6 pb-16">
        <div className="rounded-xl border border-fd-border bg-fd-card p-6">
          <h2 className="text-sm font-semibold tracking-wide text-fd-muted-foreground uppercase">
            What you actually run
          </h2>
          <p className="mt-3 text-fd-foreground">
            Cachet is <strong>infrastructure, not a library</strong>. You run a process — as a
            sidecar over a Unix socket, or as a shared service tier over TCP — and your application
            talks to it through a thin client.
          </p>
          <div className="mt-5 grid gap-3 sm:grid-cols-2">
            <div className="rounded-lg bg-fd-secondary/50 p-4">
              <div className="font-mono text-sm font-medium">cachet</div>
              <p className="mt-1 text-sm text-fd-muted-foreground">
                The query engine. The process your app reads through.
              </p>
            </div>
            <div className="rounded-lg bg-fd-secondary/50 p-4">
              <div className="font-mono text-sm font-medium">cachet-go</div>
              <p className="mt-1 text-sm text-fd-muted-foreground">
                A Go module. Carries your session token so you cannot lose the guarantee.
              </p>
            </div>
            <div className="rounded-lg bg-fd-secondary/50 p-4">
              <div className="font-mono text-sm font-medium">flux · sextant</div>
              <p className="mt-1 text-sm text-fd-muted-foreground">
                The CDC tailer and the consistency verifier.
              </p>
            </div>
            <div className="rounded-lg bg-fd-secondary/50 p-4">
              <div className="font-mono text-sm font-medium">cachetctl</div>
              <p className="mt-1 text-sm text-fd-muted-foreground">
                The operator CLI. Health, routing, key inspection, benchmarks.
              </p>
            </div>
          </div>
          <p className="mt-5 text-sm text-fd-muted-foreground">
            The SDK is Go today. The contract is gRPC (<code>cachet.v1</code>), so any language that
            can generate a client can talk to it —{' '}
            <Link href="/docs/what-is-cachet" className="underline underline-offset-4">
              more on that here
            </Link>
            .
          </p>
        </div>
      </section>

      {/* Features */}
      <section className="mx-auto w-full max-w-5xl px-6 pb-24">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <Feature
            title="Exact invalidation"
            body="The write path knows which rows changed and invalidates them before your write is acknowledged. The binlog is the backstop, catching migrations and admin scripts."
          />
          <Feature
            title="Consistency per request"
            body="STRONG, SESSION, BOUNDED(t) or EVENTUAL — chosen per call. Read-own-writes on the hot path without collapsing the hit rate."
          />
          <Feature
            title="Measured, not asserted"
            body="Sextant watches the cache against the database and publishes consistency per level. Every violation carries a trace that says why."
          />
          <Feature
            title="Stampede-proof"
            body="One caller fills a key; everyone else waits briefly. 500 concurrent readers of one hot key produce a single origin read."
          />
          <Feature
            title="Self-tuning"
            body="Per-key read:write ratios decide what is cached. A write-churning key in a read-heavy table stops being cached without anyone filing a ticket."
          />
          <Feature
            title="Shadow mode"
            body="Point it at a deployment you are not reading through and measure what your consistency would have been. No code change, no risk."
          />
        </div>
      </section>
    </main>
  );
}

function Feature({ title, body }: { title: string; body: string }) {
  return (
    <div className="rounded-xl border border-fd-border p-5">
      <h3 className="font-medium">{title}</h3>
      <p className="mt-2 text-sm text-fd-muted-foreground">{body}</p>
    </div>
  );
}
