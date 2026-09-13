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
- Tests use private synthetic local directories. Never use the operator's AAS home,
  provider credentials, existing private strategies, or recovered databases.
