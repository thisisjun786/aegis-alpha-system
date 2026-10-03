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
- State and market carry a core schema version (`migration.CORE_VERSION`). Each store
  records one `schema_migrations` row per applied version from 1 up, the checksum of
  that version's DDL text, and `store_info` and the installation receipt name the last.
  `aas init` applies every version in one transaction; `aas db migrate` is the only
  upgrade (backup, intent, market, state, receipt, completion) and a prepared intent
  makes ordinary admission refuse the installation until the command is repeated. Each
  step's intent has its own operation id and a request bound to that step's versions
  and checksums, so a completed step's intent still matches after later versions exist.
  The v1 DDL bytes are a recorded fact, so v1 is built from the v1 domains alone. A v1 store
  stays usable; writes that need a v2 table or a close-only price name the migration.
  `prices.fields` defaults to `ohlcv`, an OHLCV row reads and hashes in its v1 shape,
  and a generation holding a `close` row hashes `fields` for every row.
- Backup takes SQLite snapshots and closes DuckDB after checkpoint while retaining
  installation admission. Restore targets a new root; secrets are excluded.
- Promotion from the source library to typed generations follows
  `dev-notes/design/data-vertical.md` (Decision 0017). One hashed `aas-promotion-v1`
  spec pins sources, `mapper name@major`, time rules, decimal rules, quality rules
  and the identity snapshot. `record_id` stays `aas-record-v1`; `revision_id`
  hashes `aas-revision-v1` with dataset, record, op, superseded revision and
  `source_row_hash`; op comes from diffing the parent head on domain columns only,
  so re-collecting the same value at another time is no revision. No column takes
  the promotion wall clock, so re-promotion is an empty delta. Time rules are
  conservative upper bounds and ingestion never fills a null. A rule time later
  than the row's ingestion is capped at ingestion and flagged
  `time_clamped_to_ingestion`; a row ingested before the rule's physical base
  (e.g. the session close) is held, not promoted. A correction or deletion is
  never known before the source that carries it: a SUPERSEDE under a record-date
  rule and every TOMBSTONE take that source's ingestion time, and a source row
  older than the head is reported stale instead of superseding it. Strict
  reads use a rule's times only when the binding grants that `id@version`, and the
  run records the grants. Decimal rules that change a source value (`krw_tick@1`
  and others) leave a `quality_flags` row; flags never alter values. One dataset
  holds one provider; consumers join providers with ordered pins and cutover
  intervals. Bulk hashing keeps byte parity with `aas-rowset-v1`; never add a new
  rowset format. Every new hash format (`revision_id`, `source_row_hash`, tombstone,
  `request_hash`, `source_id`) has a frozen expected digest. Source retirement
  requires no references (the source's own `sl:` link is lineage, not a reference),
  an equivalence digest and an other-device backup, and never removes `raw/` bytes.
- `promotion/` implements that path. `spec` parses the document, `mappers` is the registry
  (one module per provider shape; a mapper is SQL over the staged source and declares its
  columns, identity key, partition and time inputs), `time_rules` and `decimal_rules` hold the
  versioned rules with a Python reference beside each SQL form, `formats` the frozen hash
  formats, and `engine` plans, applies, recovers and verifies. All per-row work stays in DuckDB
  temp tables on the workspace connection; only rows SQL cannot hash exactly (escaped text, odd
  natural keys) are computed in Python in bounded key-ordered batches. A plan writes nothing; an
  apply retains spec, request and manifest in `raw/`, records a `promotion` intent whose payload
  is the manifest, commits marker, rows and `quality_flags` in one DuckDB transaction through
  `publish_generation_bulk(companion=...)`, then writes the catalog. `aas db verify` sends a
  promoted chain to `engine.verify_promotion` instead of the import-document verifier, and
  `db recover` finishes a `promotion` intent only when the retained spec recomputes its manifest.
- `calendar_declaration` owns the `aas-calendar-declaration-v1` document (regimes, closed
  regime weekdays, sessions with other hours; one spelling per schedule) and the packaged
  XNYS/XKRX declarations under `calendar_declarations/`, which `scripts/calendar_declarations.py`
  regenerates. `calendar_refresh` commits a declaration's bytes as a content source (one row per
  date, local wall times only) and promotes it with `calendar.declared@1` as the child of
  `sessions.<mic>`'s head: both times are `declared_session_end@1` (basis `record`, a grantable
  rule) on `public_by`, the earlier of the declaration instant and the session's own end, so a
  SUPERSEDE takes the correcting source's evidence time. A declaration older than the head's,
  another one at the same instant, or a plan with stale rows is refused, so an old declaration
  never undoes a newer correction and a correction is never dropped silently.
- `legacy_import` owns `aas import legacy` (`aas-legacy-import-v1`, see the legacy section of
  `dev-notes/design/data-vertical.md`). A registered loader (`<provider>.<shape>@<major>`) fixes
  one format's complete unit, output columns and reconciliation metrics; one unit commits one
  content source per output table through `import_content_arrow`, from the `raw/` copies the
  apply just retained, so rows come only from bytes the ID names (`files.RetainedBytes`, and
  `OriginalBytes` for a plan). Values stay the original text or JSON; a unit whose bytes
  contradict their own index is refused, never repaired. Every file below an entry root is a
  unit file, a retained index or `retain` file, an `exclude` file, or reported as uncovered.
  An entry's retained files are named by one `legacy-retained-files-*` inventory source
  (path, SHA-256, size, reason), so their paths survive deleting the entry root.
  `--plan` opens no installation and writes nothing; `--verify` re-derives the plan, and its
  `complete` (zero unmatched, reconciled, zero uncovered) is the precondition for deleting an
  entry root outside the store. A new format is a new loader with its own synthetic test.
- `bulk_generation` publishes a staged DuckDB table as one generation: plan without
  writing, then one transaction with the marker and `INSERT … SELECT`. DuckDB encodes and
  sorts `aas-rowset-v1` rows and `rowset.RowsetStream` digests them in admitted batches and
  refuses out-of-order rows, so the marker equals what `market.verify_generation` recomputes.
  Row and revision rules are `normalize_rows`' and `_validate_revisions`' in SQL; a change to
  either Python rule changes the SQL in the same change, and the parity tests in
  `tests/storage/test_bulk_generation.py` hold them together. Publication recomputes the plan
  in its transaction: a moved head is `ParentChangedError` (plan again), a reviewed plan that
  no longer matches is `PlanChangedError`, and an existing marker is reused only for identical
  content. `verify_generation_bulk` checks every chain link from recorded hashes and rehashes
  the requested generation; `deep=True` rehashes every delta. Its Python batches fit the
  allocation at any row count; DuckDB's share grows with the domain table's constraint
  indexes and is refused as `ComputeResourceError` after a full rollback (see the bulk
  publication section of `dev-notes/design/data-vertical.md`).
- `source_identity` owns the content source ID (`aas-source-id-v1` over the raw
  addresses, sizes and hashes of the original files plus the output schema major)
  and the `sl:` link. Loader code and transform hashes go to `metadata.lineage`,
  never into the ID or the request hash, so a code-only change reuses the source.
  One ID names one file group, so a loader commits one source per complete
  original unit whose boundary the bytes fix (one job's `complete.json` and the
  files it lists, or one whole original manifest), never per loader-sized batch;
  a loader that regroups files mints new IDs. The explicit-ID path cannot claim
  the content namespace. The link is derived from the commit marker, its completed
  intent and `raw/` alone; it never takes a new wall clock, and a differing
  recorded link is refused. A completed content commit must be linked and keep its
  ID document in raw; `db recover` links one interrupted before its link.
- `identity` owns the native identity registry. Issuer and instrument IDs are minted
  only from a permanent anchor (`mint_issuer`, `mint_instrument`: `sec_cik`,
  `dart_corp_code`, `norgate_assetid`, `krx_isin`, the ISIN of a KRX listing whatever its
  country prefix); a ticker, symbol, path or date is
  an assertion, never an anchor, and a non-canonical token is refused rather than
  repaired. Registration (`aas-identity-registry-v1`) appends only: identical rows are
  reused, a correction is a new assertion naming the one it supersedes, known later
  than it and never before its source's `retrieved_at_us`, and any conflict or missing
  reference refuses the whole document. The instrument row is first-registration
  context; a later issuer or venue is reported, and the time-bounded issuer link is a
  namespace `issuer` assertion (`issuer_link_token`). Snapshots
  project the registry into a chunked manifest (`membership_pins.register_identity_manifest`):
  deterministic v1 parts named `<root>#NNNNN`, a suffix single documents may not use.
  A manifest root holds no members; a root with members is read as a v1 document.
- `kr_identity` builds the KR registry document from three identity mappers
  (`eodhd.kr_symbol@1`, `kind.listings@1`, `dart.corp_codes@1`) over committed sources and
  commits collected KIND and EODHD symbol-list receipts as content sources. Only an ISIN
  with a valid check digit mints an instrument; KIND and DART reach it only through a short
  code EODHD binds to exactly one ISIN, and every missing or ambiguous match (including
  lists that disagree on a symbol's ISIN, type or currency) stays unresolved with its reason
  rather than being derived from a code or a name or chosen from one list. Stock asset types
  stay `unclassified` because the provider types preferred shares as common. Assertions
  are known from their receipt's retrieval instant and cite the source row hash. A build
  must read every source registered KR assertions cite, and reports registered claims its
  sources no longer give (`withdrawn`) instead of closing them.
- `us_identity` builds the US registry document from four identity mappers over committed,
  linked sources (`norgate.master@1`, `eodhd.us_symbol@1`, `fmp.profile@1`, `sec.tickers@1`).
  Every instrument is a Norgate asset ID (venue `XNYS`); a listed row's ticker (class `.`
  spelled `-`) reaches EODHD and FMP symbols only when one listed row has it, and an issuer
  CIK links only when SEC lists the ticker under one CIK and FMP names the same CIK. Every
  disagreement stays unresolved with its reason. A ticker's claims hold only from the
  listing's first date (or the day after a delisted earlier holder's last date) until the
  day after the master's last observed session; later intervals need newer evidence.
  Claims from sources without a row instant
  are known from the source's `sl:` link; an issuer link from the latest of its evidence.
  Builds are cumulative like `kr_identity`'s, and source rows are read through Arrow so
  time-zone-aware timestamps need no time zone database.
- `universe` builds universe documents from committed, linked sources and registers them
  as chunked manifests: `norgate.index_membership@1` compresses each (asset ID, index)
  pair's daily `0`/`1` values into member intervals that expand back to the same values,
  and `norgate.listings@1` spans each master listing. A pair or listing that cannot be
  read exactly is refused whole, members are only instruments the identity registry
  holds, and members are known from their source's `sl:` link. Universe parts fill
  source by source.
- Every contract in that document has a row in its contract/test table. A change
  that implements a `예정` row adds the named test and flips the row to `구현` in
  the same change; `tests/tools/test_data_vertical_contract.py` enforces both directions.
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
- `read_heads` projects pinned chains inside DuckDB with exactly `market.project_heads`
  semantics; the property tests in `tests/storage/test_read_heads.py` hold the two
  together, so a change to either changes both. A `HeadBinding` (`aas-head-binding-v1`)
  holds ordered exact pins with contiguous `[from, to)` cutovers, the time rules it grants
  for strict reads and the quality flags it excludes, and its hash covers all of them. A
  pin never reads outside its interval and an uncovered date is reported, never filled.
  An ungranted rule's time is null to a strict read; an excluded revision is never
  available and removes the head it supersedes once known. Filters on natural-key columns
  run before projection and others on the projected head, because a revision can move a
  non-key date. Pins, chain links and per-generation row counts are checked before any row
  is read (`rehash=True` rehashes every delta), and the result is sized in SQL before it
  is fetched. Every read returns an `aas-head-read-v1` receipt that records whether every
  delta was rehashed. A record without a head reports the reason `market_inputs` reports. `market_inputs.load_pinned_heads`
  is the workspace entry: it checks each generation against the catalog and derives its
  time-rule provenance from retained evidence (promotion spec, research transform or sealed
  import), refusing a generation that has none rather than trusting a caller. A corrupted
  provenance object is an integrity error and one too large for the allocation is
  `ComputeResourceError`, never "not retained".
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
- The run add-on is versioned per store. `run_details.request_schema` carries an explicit
  allow-list, `REQUEST_SCHEMAS`, and `_open_intent` records the schema of the request that
  was actually registered rather than a constant. v1 named `aas-backtest-request-v1` alone,
  so admitting the declared contracts (`aas-research-run-v2` and
  `aas-research-composition-v1`) is `aas db run-migrate`: backup, durable intent, one
  transactional `run_details` rebuild, verification, then completion. `run_schema` receipts
  are append-only, so a migrated store shows `(1, v1), (2, v2)` and a store installed after
  the migration shows `(2, v2)`; the recorded v1 checksum stays exact, because an
  installation on disk is recognised by those bytes. The market add-on is unchanged and
  stays at 1. An interrupted migration is finished by repeating the command, or by
  `db recover`, which completes a rebuild that already landed and never starts one. A run
  recorded under the old CHECK is carried across and stays readable; nothing is rewritten.
  `quarantine` refuses the migration intent, as it already refuses a run intent: a
  quarantined intent can never be prepared again, so ending this one would leave the
  add-on unusable with nothing able to clear it.
- A declared run's three documents are held to agreeing with each other, and to being the
  same kind. Each declared contract has exactly one sealed preparation —
  `aas-prepared-research-run-v1` for a sleeve run and
  `aas-prepared-research-composition-v1` for a sample composition — so the pair is matched
  exactly rather than by family, on schema, `declaration_schema` and `scope` together. A
  composition's preparation under a sleeve declaration, or the reverse, would otherwise
  supply identities for a calculation it never describes. The preparation
  must carry the status its own contract fixes (`certified`, `point_in_time_certified` and
  `executable_prices` all false under `research-uncertified`) and must name its own
  `preparation_source_sha256`, because the engine identity does not cover the code that
  makes the declared path's decisions. The result must name `declared_uncertified_research`
  and carry every fixed claim such a response holds: `non_executable` true, and
  `certified`, `executable_prices`, `source_pins_verified`, `observed_prices_verified`,
  `point_in_time_verified` and `live_orders` all false. All seven, because pinning a
  subset would leave the rest free to say the opposite while the pinned ones still read
  correctly. Both documents are checked on the candidate before anything is sealed and
  again on re-derivation, so a rejected result leaves the run open for a corrected retry
  rather than being authenticated by the manifest built over it. Storage checks those
  claims and not the inputs a document repeats: the declaration is the authority on its
  own pins and is stored beside the run, so a reader compares the two documents instead
  of storage becoming a second copy of that contract.
- `backtest_requests` stores every request contract under one content identity: exactly
  canonical bytes, a hash over those bytes, and bindings that agree with the registered
  bundle. A declaration carries no bindings array, so its bundle is derived from the pins
  it does name. Observation panels cannot be bound, because there is no role for reference
  observations and adding one would put adjusted reference data in the namespace the
  executable price roles use; a calendar cannot be bound either, because it is a declared
  name over the panel's own dates rather than a published generation. An
  `aas-research-run-v2` bundle is therefore required to be exactly the membership it pins.
  An `aas-research-composition-v1` pins one membership per sleeve while the vocabulary
  holds a single membership, so binding one of the two would leave `bundle_id` describing
  half the run while looking complete; a composition binds nothing. Whatever stays unbound
  stays covered by the declaration's own hash. A declaration also has no engine or
  environment, because no certified request stands behind it, so a declared run takes both
  from the `aas-prepared-research-run-v1` preparation sealed beside its envelope, and that
  preparation names `declaration_sha256` where an executable one names `request_hash`.
  Request contract and preparation kind are paired at open and re-checked at verification.
  None of this changes execution admission: `aas-backtest-request-v1` still holds execution
  prices to canonical unadjusted data, and a stored declared run reads back as
  `research_only` under its own schema.
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
