# tests/

## Suites and fixtures

Tests mirror current source owners: application, engine, storage, container, data,
collection, identity and metadata. `tools/` checks CI selection, aggregation,
security and verifier behavior. Provider-neutral JSON fixtures are synthetic.
The root `conftest.py` owns network, data-root and disposable DB isolation.

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
- `AAS_DATA_ROOT` is `setdefault` to a session `TemporaryDirectory` **before** `aegis_alpha`
  imports — that ordering is why `tests/conftest.py` carries `# noqa: E402`. A caller-supplied
  root wins; it must still contain only synthetic disposable test inputs, never
  recovered or production data.
- No `__init__.py` anywhere under `tests/`. Support helpers resolve two ways: bare
  (through pytest directory insertion) and dotted (through the configured project Python path).
- `ruff select = ["ALL"]` applies to tests; the only per-file relief is `INP001` and `S101`.
  Subprocess calls need explicit `noqa: S603`, private access `SLF001`.
- `addopts = ["--strict-config", "--strict-markers"]` — an unregistered marker fails the run.

## ANTI-PATTERNS
- **Never touch the network.** The autouse `block_network` fixture replaces
  `socket.create_connection`, `socket.getaddrinfo`, and `socket.socket.connect` with
  `pytest.fail`. Use the synthetic transports and fake clocks each suite already provides.
- Never use a real credential or a paid provider call; fixtures are provider-neutral and free.
- Do not add a fixed sleep to wait for an assertion. The existing `time.sleep(30)` calls are
  hang-forever *child process* bodies whose termination is under test, and lock-contention
  suites poll `pg_blocking_pids` under an explicit deadline — copy those shapes, not a bare wait.
- Do not run PG-backed suites concurrently against one database; isolation is per session, and
  the repo forbids parallelizing tests that share mutable fixtures.
