## What this changes, and why

<!-- The why matters more than the what; the diff already says what. -->

## How it was verified

<!--
The project's standing rule: a test that has never been observed failing is unproven. If this
change is load-bearing, say how you watched the test catch the bug it forbids.
-->

- [ ] `make lint` — 0 issues
- [ ] `make test-unit`
- [ ] Suites relevant to this change (`test-integration`, `test-consistency`, `test-e2e`, `test-chaos`)
- [ ] Watched the new test fail against the defect it exists to catch

## Consistency model

- [ ] This does not change any guarantee in `CONSISTENCY.md`
- [ ] This changes a guarantee, and is therefore a **major** version — the doc and the conformance
      matrix are updated in this PR

## Numbers

<!--
If this claims a performance result, include the spread and the host. When the spread exceeds the
effect, nothing has been measured — say so rather than rounding a non-result into a win.
-->
