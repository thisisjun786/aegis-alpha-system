# Contributing to AAS

Read [POLICY.md](POLICY.md) for integration and release rules. Normal changes use
a short-lived branch and a pull request into `dev`. Keep the problem, resulting
behavior, checks and remaining risks clear. Human review count is zero; automated
review is advisory. Confirmed defects still need resolution.

Prepare the locked development environment with `uv sync --locked --dev`. Run
the focused tests for your change and the relevant style/type checks. CI selects
independent jobs and aggregates one required result; a duplicate full local suite
is not required before opening a PR. See [scripts/AGENTS.md](scripts/AGENTS.md).

### Fast local loop

Tests run whole files in parallel worker processes (pytest-xdist). Each process
gets its own isolated home under `TMPDIR` (see [tests/AGENTS.md](tests/AGENTS.md)),
so a run never reads or writes your `~/.aas`.

- The area you are changing: `uv run --no-sync pytest -n auto --dist loadgroup -m 'not database' <paths>`
  (whole files per worker; see tests/AGENTS.md for serial groups).
- The full database-free lane: `./scripts/verify-lane-test` (the same command each CI
  `tests` shard runs).
- For fsync-heavy storage runs on Linux, set `TMPDIR=/dev/shm/aas-$USER` (create it
  first) to keep the scratch stores in memory. Do not use `/tmp`: a stray `/tmp/.git`
  makes storage refuse paths beneath it.
- `AAS_TEST_WORKERS=0 ./scripts/verify-lane-test` (or `pytest` without `-n`) runs
  serially in one process, for debuggers and `breakpoint()`.

Public tests must work without private strategy data, credentials, live providers
or installed production services. Use synthetic assets, parameters and expected
results. The repository owns generic calculation, schema, parsing and execution
mechanisms. Keep actual strategy records, recipes, performance and holdings in
private storage, including when submitting logs or fixture data.

Before changing dependencies or adapting external code, record the source
revision, license, modifications and relevant behavior checks. Preserve existing
notices. Contributions to AAS-authored code use [Apache-2.0](LICENSE); third-party
terms remain in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

Security reports follow [SECURITY.md](SECURITY.md). A successful development gate
does not authorize a release, deployment, provider call or data migration.
