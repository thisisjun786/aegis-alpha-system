# Local storage

Decision 0014 and `dev-notes/design/backtest-data-foundation.md` own the storage
contract. This is a new embedded implementation, not a port of retired SQLite.

- `workspace` owns installation admission for all connection lifetimes. Never
  bypass it in application commands, including reads and maintenance.
- `state.sqlite3` owns small relational state and visibility; `strategies.sqlite3`
  owns immutable private bundles; `market.duckdb` owns typed observations/results.
- Cross-file writes require a durable state intent, a verifiable target marker,
  then catalog completion. No provider retry follows from a missing state receipt.
- Files live outside Git checkouts. Admit private directories and regular files,
  reject aliases and foreign store identities, and acquire external file locks.
- SQLite readers are query-only. Writers enable FK/WAL/FULL. Check schema checksums
  and reject unknown versions instead of implicitly adopting or upgrading files.
- Backup takes SQLite snapshots and closes DuckDB after checkpoint while retaining
  installation admission. Restore targets a new root; secrets are excluded.
- `research_inputs` publishes a retained source table as a new generation only
  through an exact hashed transform document (`aas-{price,sessions,proxy}-transform-v1`).
  Every common revision field maps to a real source column; never fabricate
  revision links, record identity or row hashes. Decimal admission is exact and
  partial OHLCV rejects. Proxy points are DOUBLE `feature_values` under their own
  contract ID/version and stay non-executable. The raw spec is provenance.
- `market_inputs` reads pinned generations with explicit pins and a caller-owned
  compute budget, replaying the full chain per decision. Strict PIT excludes later
  revisions, unknown knowledge and reference prices; observed snapshot research is
  explicit and uncertified. Coverage reports every requested cell. Nothing here
  promotes a generation to PIT or backtest eligibility.
- `strategy_import.register_strategy` owns workspace strategy registration: state
  intent, the shared private writer, then verified completion. Standalone
  `strategies.import_strategy` retains private-store admission. The v1
  `strategy_requirements` rows are written from `legacy_requirement_rows` and
  keep their meaning. Reimport with identical bytes and lineage is idempotent;
  differing content or lineage fails. `db recover` completes a committed import
  whose state operation was left PREPARED by verifying stored content only; it
  grants no execution eligibility.
- `runs` owns the formal run lifecycle. `open_run` commits the intent before any
  calculation and seals the envelope and preparation; `commit_run` seals the result,
  commits the market marker, then records receipts and ends the run SUCCESS;
  `recover_run` finishes or ends exactly one interrupted run and never recalculates.
  `db recover` reaches it through the `run_commit` kind, and the generic `quarantine`
  refuses a run intent because ending that alone would leave the run RUNNING and
  invisible to the PREPARED-only scan. Every receipt is derived from the sealed files,
  never from caller-supplied values, and a strategy pin must name a version the private
  store admitted. Result `at_us` encodes a session date as midnight UTC, not an instant.
- `strategies.LineageSpec` keeps four exact caller fields. The accepted direct
  parent status is sealed by v2 state/private request hashes at first durable
  acceptance, before PREPARED commits. Retry decodes that commitment, never
  reselects from current existence. Reads authenticate actual status against
  every private receipt before eligibility; load has no hidden state connection.
  No-lineage v1 is unchanged. Interim caller-only lineage v1 is physically
  preserved but rejects verified use/resealing. A later parent never promotes
  an earlier child; corrected lineage needs a new version. The exact protocol,
  compatibility limit and direct-parent policy are in
  [strategy-lineage.md](../../../dev-notes/design/strategy-lineage.md).
- `strategy_requirements.read_execution_definition` is SELECT-only. It loads
  the pinned bundle, checks stored rows against the derived definition, then
  validates optional `ConventionPin` bindings against `input_pins` on the
  explicit state connection. Only a compatible `basis` pin changes the
  definition (price requirements receive its `price_basis`); `capital`
  requirements reject `total_return`. Every other role stays unresolved and
  `executable` remains false. Unresolved lineage fails here, not at import.
- `input_pins.register_convention(state, raw, expected_file_sha256=...)` and
  `read_convention(state, pin)` are the public Python surface for conventions.
  The file hash refers to exact incoming bytes; the pin hash is the SHA-256 of
  the canonical whole-document bytes, which usually differs from the file hash.
  There is no convention CLI yet.
- Tests use private synthetic local directories. Never use the operator's AAS home,
  provider credentials, existing private strategies, or recovered databases.
