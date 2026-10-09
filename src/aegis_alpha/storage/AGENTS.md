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
  `--to N` runs the steps one at a time; every step without an intent takes its own
  verified backup, and only a prepared step resumes without one. A step's backup alone
  may carry untouched promotion intents (`untouched_promotion_refusal`: no marker, rows,
  flags or catalog trace; a complete manifest matching the intent, its spec and its
  parent's head, sequence and chain hash) as PREPARED; `backup_workspace`
  rechecks the names it is given and every other backup refuses every pending operation.
  The v1 DDL bytes are a recorded fact, so v1 is built from the v1 domains alone. A v1 store
  stays usable; writes that need a v2 table or a close-only price name the migration.
  `prices.fields` defaults to `ohlcv`, an OHLCV row reads and hashes in its v1 shape,
  and a generation holding a `close` row hashes `fields` for every row.
- `verify_workspace`, backup, restore and compact compare stored rows with their recorded
  digests by default: a source table's recorded columns and row count, a promoted chain's links
  and leaf delta. `deep=True` (`--deep`) rehashes every source table and promoted delta; the
  report is the same in both modes. Compact always verifies the rewritten root deep.
- Backup takes SQLite snapshots and closes DuckDB after checkpoint while retaining
  installation admission. Restore targets a new root; secrets are excluded. `backup.json`
  lists every `raw/` and `runs/` file, so it is read with its own bound
  (`backup.MAX_MANIFEST_BYTES`), not the 1 MiB configuration bound.
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
  `request_hash`, `source_id`, `dimensions_hash`) has a frozen expected digest. Source
  retirement requires no references (the source's own `sl:` link is lineage, not a reference),
  an equivalence digest and an other-device backup, and never removes `raw/` bytes.
- `source_retirement` owns `aas db source-retire` (`aas-source-retirement-v1`): a group of sources
  is retired whole only when nothing outside its own `sl:` link refers to it, its compared columns'
  `aas-rowset-v1` multiset digest equals the equivalent sources' (cells in their exact typed form,
  encoded in DuckDB by `bulk_generation.encoded_cell_sql`), and a verified backup on another device
  holds every commit. `--apply` retains the records in `raw/`, prepares a `source-retire` intent,
  drops the tables in one DuckDB transaction and writes `source_retirements`; repeating it or
  `db recover` finishes the intent, and `quarantine` refuses one that already dropped a table.
  Every retired-table column outside the comparison is named in the group's `uncompared` list
  (the plan reports such a group as `partial_columns`, the record's `equivalence_spec` keeps the
  list); the digest is an unkeyed multiset and proves the compared columns only.
  Markers, links and `raw/` stay; `source_library` hides a retired source from listings and
  readers, verifies it from its record, and treats a same-request re-import as reused
  (`committed_source_ids` is the importers' "already committed" set; `legacy_import --verify`
  matches a retired unit as `retired`).
- `compaction` owns `aas db compact --to NEW_ROOT`: verify, rewrite every store into a new root
  (SQLite backup plus `VACUUM`, DuckDB `COPY FROM DATABASE … (SCHEMA)` then rows parent-first
  along foreign keys, a self-referencing table one chain level at a time), copy `raw/`, `runs/` and `secrets/`,
  and require the same logical verification there before the new receipt is `ready`. The original
  installation is never changed; switching `AAS_HOME` is the operator's step.
- `promotion/` implements that path. `spec` parses the document, `mappers` is the registry
  (one module per provider shape, `daily` holding the SQL the daily price mappers share; a
  mapper is SQL over the staged source and declares its columns, identity key, partition
  date, time inputs, any pinned generations of other datasets it joins (which the engine
  verifies and loads as head tables), whether one source row expands into rows numbered by
  `_aas_item`, and the per-row outcome a response source records as coverage instead of
  rows), `time_rules` and `decimal_rules` hold the versioned rules with a Python reference
  beside each SQL form, `formats` the frozen hash formats, and `engine` plans, applies,
  recovers and verifies. All per-row work stays in DuckDB temp tables on the workspace
  connection; only rows SQL cannot hash exactly (escaped text, odd
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
- The macro and FX mappers (`fred.alfred@1`, `bok`/`oecd.observations@1`, `norgate.fx_closes@1`,
  `norgate.fx_history@1`, `fred.fx_series@1`) emit numbers as the source's text for `decimal_text@1` and never repair it.
  A generation holds one revision per record, so ALFRED vintages promote one vintage partition per
  generation in order (`mappers.fred.vintage_partitions`), each later vintage a SUPERSEDE; the closed
  `realtime_end` of a later pull stays in the source row, never on the earlier revision. A text-dated
  source partitions on its `YYYY-MM-DD` text read as a date; a tombstone scope tests the mapper's domain date column, or the
  UTC day of an instant column such as FX `fixing_at_us`. An FX spec's `timezone` must end the day
  after the provider's fixing, and `local_day_end@1` on the fixing date needs the provider to publish
  by then (FRED H.10 does not, so `fred.fx_series@1` takes `unknown_null@1`).
  A mapper whose source shape several providers share declares `source_prefixes`, and the spec
  refuses a pin from another provider's source.
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
- `kr_prices` (`aas data kr-prices`) promotes `prices.kr.eodhd` (or `.ref`) as ordered steps:
  history bars of one lineage per calendar year with `eodhd.bars@1`, the rows those
  downloads held back as invalid in one step with `eodhd.bars_quarantine@1` (canonical only),
  then the KR exchange-wide downloads the provider warned were partial, per session date with
  `eodhd.bulk_quarantine@1`, repeated identical downloads pinned once. Each step's spec is
  canonical (identity snapshot, `sessions.xkrx` head for `session_close_plus_lag@1`,
  `krw_tick@1` OHLC, `float_shortest@1` volume) with the current head as parent. A mapper
  may declare row flags it reads off the source row; the engine attaches
  them under the mapper's `name@major` with a NULL detail. Rows flagged
  `provider_reported_partial` are promoted, never blocked, and the generation records
  `partition_row_count@1` (resolved rows per date against the parent chain's complete dates,
  those with no partial-flagged head) in its manifest
  and `quality_checks`. A row missing a required column is `refused_required` before identity
  resolution, and a partitioned plan refuses rows without a partition date, so malformed rows
  never drop out silently as unresolved. A mapper that needs what a source's commit manifest
  records (the held rows' job symbols) declares `manifest_items`; the engine stages that list
  from the pinned commit as `MANIFEST_ITEMS` after recomputing the source's request hash from
  that manifest, never from outside the pin.
- `collection_ledger` writes the state collection tables: a job per provider request
  (named by its fingerprint, whatever day it is asked), a numbered attempt per ask that is
  `reserved` with a `reserved` usage event before the call, `started` just before it, then
  `succeeded` with a `charged` event naming the retained receipt or `uncertain`. Recovery
  turns a left `reserved` into `failed`/`released` and a left `started` into `uncertain`,
  never into a success or a non-call; a quota counts unreleased reservations in its window.
  A collector may name the reservation's evidence (a Qveris ask's job fingerprint) and
  `release` an ask it never executed.
- `kr_collection` runs `aas collect dart` and `aas collect kind` on a writable workspace:
  the rolling OpenDART cohort planned from every committed `opendart-*` receipts table,
  the newest KIND lists and unanswered attempts; each answer and its canonical receipt go
  to `raw/` before the attempt succeeds, and batches commit as `opendart-receipts` (and
  KIND answers as `kind-listings`) content sources. Receipts a crashed run retained but did
  not commit are committed first by the next run. See the KR collection section of
  `dev-notes/design/data-vertical.md`.
- `provider_collection` holds what the native US collectors share: the ledgered, paced and
  budgeted `Caller.ask`, canonical `aas-<provider>-receipt-v1` receipts (with the document
  selection an ask recorded) and `aas-<provider>-batch-v1` batches whose derived tables are
  content sources of the batch's files, committed before the receipts source that marks the
  batch complete. `sec_collection` (`aas collect sec`) reads daily
  indexes, then each filer's submissions and companyfacts once a run, keeping only the rows
  of the filings it wanted; `fred_collection` (`aas collect fred`) asks ALFRED vintage dates
  after each series' latest collected vintage day and then observations windows of at most
  1990 vintage dates chained on their last vintage, keeping only rows that do not start on a
  window's start (ALFRED clips periods to the window), and commits each CSV download as the
  `fred-series-csv` source `fred.series_csv@1` would import. See the US collection section of
  `dev-notes/design/data-vertical.md`.
- `maintain_promotion` continues catalog dataset chains for `aas maintain`: a route names a
  dataset, its mapper and one collected source shape; each new source becomes the head's child
  under the latest committed spec that used that mapper, with only parent, sources, partition,
  generation pins (to current heads), the maintenance identity snapshot and a `never` tombstone
  advanced, and a `maintain_source@1` quality check marks the source done. It never starts a
  chain and never changes a chain's rules. `maintain_identity` rebuilds and registers the KR
  registry when a newer KR identity source is linked and pins the `maintain-<assertion set>`
  snapshot. See the maintenance section of `dev-notes/design/data-vertical.md`.
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
- `market.budgeted(connection, budget, work)` is the DuckDB capacity boundary of bulk
  publication, the core migration's market step and compaction's market copy: it lowers the
  connection to the lease's share and reports an `OutOfMemoryException` (lowering the limit
  included), or a COMMIT's `TransactionException` whose cause right after DuckDB's fixed
  `Failed to commit: ` prefix is a failed allocation or block pin, as `ComputeResourceError`
  caused by the DuckDB error. Any other failed COMMIT keeps its own error, including a
  constraint violation whose quoted key happens to contain those words. A failed COMMIT has
  already ended its transaction, so the rollback after it must never replace that error with
  ROLLBACK's "no transaction is active"; market writers use `market.rollback` for that.
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
- `qveris_import` commits one completed Qveris job as content sources: the rows table and,
  when it has rows, the held `quarantine` table first, both under the hex of the job's
  `complete.json` and its four page files. The identity document its rows used is lineage
  (`identity_sha256`), so a grown identity document never re-imports a job. A warned
  download is held whole with `provider_reported_partial` and never blocks the import; a
  committed rows source makes the unit `reused`. It never calls a provider.
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
- `us_identity` builds the US registry document from five identity mappers over committed,
  linked sources (`norgate.master@1`, `eodhd.us_symbol@1`, `fmp.profile@1`, `sec.tickers@1`,
  `norgate.export_listing@1`). Every instrument is a Norgate asset ID (venue `XNYS`, or `XXXX`
  for an index or other reference series an export adds); a listed row's ticker (class `.`
  spelled `-`) reaches EODHD and FMP symbols only when one listed row has it, and an issuer
  CIK links only when SEC lists the ticker under one CIK and FMP names the same CIK. Every
  disagreement stays unresolved with its reason. A ticker's claims hold only from the
  listing's first date (or the day after a delisted earlier holder's last date) until the
  day after the master's last observed session; a later Norgate history export adds a
  non-overlapping window up to its own last session, and FMP or SEC rows are judged against
  the claim (master or window) holding when they were retrieved. Later intervals need newer
  evidence.
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
- `promotion/mappers/norgate_actions.py` reads corporate actions back out of Norgate's
  adjusted parts, whose `CAPITAL` close is the unadjusted close over the product of later
  capital events and whose `dividend` sits, in that basis, on the last session before the
  ex-date: `norgate.dividends@1` (cash as paid, ex-date the next session) and
  `norgate.capital_adjustments@1` (the step of `unadjusted_close / close`, beyond one part in
  a million). Both read neighbouring rows, so their partition date is always NULL and a spec
  pins every part. `norgate.status@1` reads master listings and delistings, a delisting
  known no earlier than the last session. `fmp.dividends@1` and `fmp.splits@1` share
  `fmp.revision_runs` with the FMP prices; a key with two values at one instant is never
  selected, because FMP lists two payments on one ex-date that way.
- `adjusted_prices` derives `split_adjusted` and `total_return` prices from unadjusted bars and
  the corporate actions read with the same query (`read_adjusted_prices`, workspace entry
  `load_adjusted_prices`), so an action unknown at the cutoff never reaches an earlier price.
  Only actions inside the bars read apply, the last bar stays as traded, and an action it
  cannot apply leaves every earlier bar `invalid` (`unadjustable_action`) rather than skipping
  it. The actions read uses `read_heads(held=True)`, so an action the cutoff knows but a
  missing grant or missing evidence holds back is unadjustable too, with its reason. The
  derivation reads bars without the query's grid, so a dividend reinvests at the session
  before its ex-date. The receipt (`aas-adjusted-read-v1`) carries both head-read receipts
  and the withheld rules.
- `promotion/mappers/classifications.py` promotes snapshot classifications
  (`norgate.classification@1`, `sec.sic@1`, `kind.industry@1`). A row starts at its
  snapshot's date and is never extended into the past; a later snapshot adds rows of its
  own date. A subject whose source carries its permanent anchor (Norgate asset ID, SEC CIK)
  is minted in SQL exactly as `identity.mint_*` does and pins no identity snapshot; a KRX
  short code resolves `subject_id` through the pinned snapshot. `sec_companies` owns
  `aas import sec-companies`, which commits each CIK document's header from a retained
  `sec-submissions-zip-*` archive as a content source for `sec.sic@1`.
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
- `strategy_registry` registers retained source strategy records as immutable
  `aas-strategy-definition-v1` documents in an independently versioned add-on of the
  private store (`strategy_registry_schema`), never as `strategy_versions` rows: a
  definition is not an engine bundle and grants no eligibility. The version is the
  document's content hash, so changed content is a new version and the old one stays.
  Requirement rows are re-derived from the stored document through the versioned
  requirement map on every verification, and a source whose own dependency tables
  disagree with its requests is refused. One state intent, one private transaction
  ending in the `strategy_registrations` marker, then completion; `db recover` finishes
  a committed marker from stored evidence alone, repeating `--apply` finishes an intent
  with no marker, and `quarantine` refuses every registration intent because the
  operation id is the request hash. Requirement map v1 adds a USD/KRW `fx` row when a
  mapped price's market currency differs from the request's `exchange`.
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
  name over the panel's own dates rather than a published generation. A declaration that
  reads canonical price pins (its root carries `prices` in place of `observations`; each
  research root has exactly those two variants) binds no price either: the binding vocabulary
  has no research price role, and the declaration hash plus the sealed `aas-head-read-v1`
  receipt cover those pins. An
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
