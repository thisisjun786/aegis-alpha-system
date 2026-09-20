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
  through an exact hashed transform document
  (`aas-{price,sessions,proxy,observation}-transform-v1`).
  Every common revision field maps to a real source column; never fabricate
  revision links, record identity or row hashes. Decimal admission is exact and
  partial OHLCV rejects. Proxy points are DOUBLE `feature_values` under their own
  contract ID/version and stay non-executable. The raw spec is provenance.
- `research_inputs.register_observation_input` is the explicit observed research
  route for a retained panel that carries real session opens or closes without the
  complete OHLCV an executable bar needs. It writes DOUBLE `feature_values` under
  `aas-observation-definition-v1`, so the exact DECIMAL(38,12) admission and the
  partial-OHLCV refusal keep refusing precisely what they refused before while the
  retained binary64 bits survive unrounded. The contract fixes price_role
  `reference`, `certified: false` and an explicit `value_domain`; its name is
  `series_id/observation_role`, so one series' open and close stay distinct rows
  and carry their own missingness. `observed_source` pins the upstream panel as
  provenance while the transform's own source pins the mapped point table.
  `feature_inputs` pins no single transform, because a panel legitimately arrives
  as several bounded generations; each contributing generation's transform is
  authenticated on read. `publication_at_us` stays as declared, including null: an
  unknown publication time is never filled in from a session date, and a panel
  without knowledge times keeps NULL knowledge columns, so strict PIT selects
  nothing from it. Research return proxies keep their own route.
- An observation extension continues one contract all the way down: each ancestor's
  pinned transform is re-read and must declare the same definition, because matching
  contract columns alone can come from the generic import route. Reads and
  `verify_feature_publications` load each chain once and group rows by
  generation, and that verifier's scan is ordered by `dataset_id, sequence` because
  its one-entry cache depends on a dataset's rows being contiguous. Those are
  correctness-adjacent: without them an ordinary read and `aas db verify` become
  quadratic in the number of chunks a panel was published as.
- `aas data register-observations --spec <file> --sha256 <digest>` is the CLI
  surface, alongside `register-prices`, `register-sessions` and `register-proxy`.
- A native route declares the retained transform it was built from in its sealed
  import document, as the optional `transform_schema` field of
  `aas-market-import-v1`. That document's SHA-256 is the marker `request_hash` the
  generation chain covers, so `verify_feature_publications` classifies a committed
  `feature_values` publication from evidence an edit cannot move, and never from
  `dataset_versions.transform_hash`: that column is `NOT NULL` for every route, and a
  generic offline import commits an opaque digest whose preimage it never retained.
  Discovery starts at the publications rather than the contract table, because a lost
  contract would otherwise hide a committed generation behind an empty scan while the
  integrity checks still pass.
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
- A sealed envelope must carry the `dates` it was exported from: at least two sessions,
  increasing, without repeats, each spelled `YYYY-MM-DD` like every other date the
  runner emits. `open_run` checks them before the envelope becomes an artifact, and
  candidate validation checks the targets and every fill against them, so a target with
  no following session, a decision off the supplied sessions, a reversed or same-day
  pair, a skipped session or a decision without targets seals no result, writes no
  market marker and never reaches SUCCESS. The targets are judged apart from the fills
  because an unchanged or all-cash decision legitimately produces none while its weight
  row is stored anyway. Adjacency is read from the supplied list alone; a weekend or a
  closed venue is the next session, never the next calendar day, and neither a system
  calendar nor a current market lookup may stand in for the sealed list. An alternate
  ISO spelling such as `20240102` is refused rather than normalised, because the
  shipped runner refuses it too. Symbols, quantities, prices
  and fees stay unjudged because storage cannot replay them, and no result is required
  to fill every target day. A run recorded SUCCESS before this check with an envelope
  that names no sessions keeps its recorded status: `read_run`, `verify_run` and
  workspace verification now refuse it explicitly, and `recover_run` leaves it alone
  because it only finishes an interrupted RUNNING run. Correcting such a record means
  opening a new run against a complete envelope; sealed bytes and hashes are never
  rewritten, backfilled from current data, or silently accepted in the old shape.
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
