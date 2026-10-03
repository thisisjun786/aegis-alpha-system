# tests/

## Suites and fixtures

Tests mirror current source owners: application, engine, storage, container, data,
collection, identity and metadata. `tools/` checks CI selection, aggregation,
security and verifier behavior. Provider-neutral JSON fixtures are synthetic.
The root `conftest.py` owns network, home, data-root and disposable DB isolation.

## CONVENTIONS
- **PostgreSQL tests are `database`** (current contracts; decisions 0010/0012).
  `tests/conftest.py` tags every test whose fixture closure includes `test_database`; a test
  that opens PostgreSQL any other way must carry `@pytest.mark.database` itself.
  `uv run pytest -m "not database"` needs no PostgreSQL server; native storage tests
  still create disposable SQLite/DuckDB files.
- **`AAS_TEST_DATABASE_URL` is mandatory for `database`.** `test_database` calls `pytest.fail`
  when it is unset; it must use the `postgresql+psycopg` driver, name a disposable `*_test`
  admin database, and the server version must match the pin asserted in `conftest.py`.
  `./scripts/verify-lane-database` provisions the container and exports it.
- Each session creates `aas_owned_<uuid>_test`, stamps ownership via `COMMENT ON DATABASE`, and
  drops it only when prefix, suffix, and owner token all match.
- `clean_postgres` runs `alembic upgrade head`, then deletes every table in reverse dependency
  order *before and after* each test. Schema state is migration-derived, never hand-built.
- **Every test process runs in its own isolated home.** Before `aegis_alpha` imports (that
  ordering is why `tests/conftest.py` carries `# noqa: E402`), `tests/isolation.py` creates one
  `aas-pytest-*` tree under `TMPDIR` per process (per xdist worker) and points `HOME` and
  `XDG_{CONFIG,DATA,STATE,CACHE}_HOME` into it. `AAS_HOME` and `AAS_DATA_ROOT` default into
  the same tree: a caller-supplied root (mounted real-input acceptance) wins, and it must
  still contain only synthetic disposable test inputs, never recovered or production data.
  The process exports its tree as `AAS_PYTEST_ISOLATION_ROOT`; an xdist worker, which starts
  with its controller's environment, replaces any root inside that tree with its own.
- **A run aimed at live state stops before any test.** At session start the guard refuses the
  run (exit 4) when `HOME`'s `.aas` or `.local/share/aegis-alpha`, an `XDG_*_HOME`, or a set
  `AAS_HOME`, `AAS_DATA_ROOT`, `AAS_DATA_CONFIG`, `AAS_INSTALL_CONFIG`, `AAS_COLLECTION_STATE`
  or `AAS_COLLECTION_CONFIG` resolves inside the account's or the starting `HOME`'s `~/.aas`
  or `~/.local/share/aegis-alpha`, or inside `/state/aas`.
- No `__init__.py` anywhere under `tests/`. Support helpers resolve two ways: bare
  (through pytest directory insertion) and dotted (through the configured project Python path).
- `ruff select = ["ALL"]` applies to tests; the only per-file relief is `INP001` and `S101`.
  Subprocess calls need explicit `noqa: S603`, private access `SLF001`.
- `addopts = ["--strict-config", "--strict-markers"]` — an unregistered marker fails the run.
- `tmp_path_retention_policy = "failed"`: a passing test's `tmp_path` is removed at teardown,
  so scratch (memory-backed in CI) holds one test's stores at a time. Failed tests keep theirs.
- **The database-free lane runs whole files in parallel processes.** `verify-lane-test` passes
  `-n "${AAS_TEST_WORKERS:-auto}" --dist loadgroup`, and `scheduling.py` (registered from
  `conftest.py`) schedules an unmarked test by its file: one file runs start to finish in one
  worker, so module fixtures are built once per file, and process-global state (`os.environ`,
  `chdir`, signal handlers, `/proc/self/fd`) is never shared between workers. A file whose
  tests hold a resource shared across processes (a fixed path outside `tmp_path`, a port, an
  abstract Unix socket, a system-wide lock) carries
  `pytestmark = pytest.mark.xdist_group("<resource>")` under a `# Serial: <reason>` comment;
  every file of one group runs in one worker. Today the only group is
  `qveris-account-lease`: the Qveris store binds an abstract socket named by the account, and
  the synthetic accounts are fixed. `conftest.py` fails a test that enters `QverisStore`
  outside that group. A group serializes one run only: two pytest runs on one host still
  contend for the same socket, so a test that starts a nested pytest run points it at
  generated files under `tmp_path`, never at test files that take a host-wide resource.
  Under xdist a grouped test's reported node ID ends in `@<group>`. `AAS_TEST_WORKERS=0` runs
  serially.
  xdist puts `tmp_path` one directory deeper (`popen-gwN/`), so bind an `AF_UNIX` socket by
  a name relative to its directory rather than by its 107-byte-limited absolute path.
- **Fast local loop.** `uv run --no-sync pytest -n auto --dist loadgroup -m 'not database'
  <paths>` for the area you change; `./scripts/verify-lane-test` for the whole lane. For
  fsync-heavy storage tests use `TMPDIR=/dev/shm/aas-$USER`, never `/tmp` (a stray `/tmp/.git`
  makes storage refuse paths). Use `AAS_TEST_WORKERS=0`, or omit `-n`, to debug in one process.
- **Sharding is file-granular and deterministic.** `sharding.py` (registered from `conftest.py`)
  adds `--test-shard INDEX/COUNT`. After marker selection it balances whole test files over the
  shards by the measured seconds in `shard_weights.json`; a file the table lacks is estimated
  from its test count at the table's mean rate. Every CI `tests` shard publishes the seconds it
  measured as the `test-durations-<INDEX>` artifact; when shard durations drift apart, refresh
  the table from one run's artifacts with `python -m tests.sharding merge durations-*.json`
  (it refuses inputs that leave out a weighed file the `-m "not database"` lane still runs,
  such as a missing shard; a deleted, renamed or wholly `database`-marked file leaves the table),
  or locally from an unsharded `-m "not database"` run with
  `--test-durations-out tests/shard_weights.json`.

## ANTI-PATTERNS
- **Never touch the network.** The autouse `block_network` fixture replaces
  `socket.create_connection`, `socket.getaddrinfo`, and `socket.socket.connect` with
  `pytest.fail`. Use the synthetic transports and fake clocks each suite already provides.
- Never use a real credential or a paid provider call; fixtures are provider-neutral and free.
- Do not add a fixed sleep to wait for an assertion. The existing `time.sleep(30)` calls are
  hang-forever *child process* bodies whose termination is under test, and lock-contention
  suites poll `pg_blocking_pids` under an explicit deadline — copy those shapes, not a bare wait.
- Do not run PG-backed suites concurrently against one database; isolation is per session, and
  the repo forbids parallelizing tests that share mutable fixtures. `verify-lane-database`
  runs serially in one process.
