# Working on AAS

Read [POLICY.md](POLICY.md) for development, merge, CI, and release rules. It is the repository-owned policy; external control-plane templates are not additional authorities. Read the nearest directory instructions before changing that area. Product decisions live in [architecture](dev-notes/architecture.md) and the [decision index](dev-notes/decisions/README.md). [Decision 0012](dev-notes/decisions/0012-retire-legacy-runtime.md) defines the current retirement boundary.

## Start here

- Inspect the current branch, diff, and worktrees. Preserve existing work and use an isolated worktree for implementation.
- Identify one useful outcome, its owner, and the smallest checks that establish it. Make routine reversible choices without asking for permission again.
- Reuse existing implementations and test fixtures before adding abstractions. Keep changes within the requested outcome.
- Work on a short-lived `<tool>/<change>` branch targeting `dev`. Apply the [merge conditions](POLICY.md#development-and-release); reserve `main` for an explicitly approved release.
- Run focused local checks. Do not require a duplicate full local suite, review-bot response, or extra approval round for every PR.
- Explain what changed, what was actually checked, and what remains. Never report a design, static check, or healthy container as a completed runtime integration.

## Current application

AAS targets a data and research engine consumed by external tools, as defined in [Decision 0015](dev-notes/decisions/0015-research-engine-product-boundary.md). The current package exposes a CLI and generic Python engine API. **Aegis**, **Alpha**, and **Hedge** remain the current preview contracts and research-area names. [Decision 0009](dev-notes/decisions/0009-standalone-three-modules.md) retains those contracts; its CLI-centered product direction is superseded by 0015. External code is reference material; retained imports require provenance and license notices.

- `src/aegis_alpha/application/` owns strict allocation requests, portfolio composition, and the CLI.
- `src/aegis_alpha/modules/` declares the three module responsibilities. `engine/` supplies generic rule evaluation; the allocation-preview CLI does not execute it automatically.
- `examples/portfolio-preview.json` is synthetic executable input, not market evidence or ordinary prose.
- `storage/` owns the native SQLite/DuckDB installation under `AAS_HOME` (default `~/.aas`).
- `Dockerfile` packages the optional single AAS image. Native DB commands need no server or Docker; preview remains DB-independent.

The preview reads input and returns a result; it does not call providers, restore databases, execute strategies, or place orders. `aas init/doctor/db/strategy/data` use the embedded stores. Transitional `aas legacy-db install/adopt` and `legacy-data` retain the old explicit PostgreSQL/Parquet operations. Native generation reads support cutoff-based revision visibility; legacy price inspection is separate. Neither is integrated backtest execution. Python `engine.replay` calculates explicit injected inputs; the CLI does not yet connect stored strategies and market inputs to that calculation. Recheck the current architecture and capability output as the application evolves. Import new domains directly rather than widening the legacy package facade.

## Preserved contracts

Keep current data, collection, identity, metadata and migration contracts intact unless the task explicitly changes them. Decision 0012 retires obsolete implementations; do not restore them from history.

[Decision 0014](dev-notes/decisions/0014-local-embedded-databases.md) now selects local SQLite state/private strategies and native DuckDB market storage. Preserve domain, provenance and temporal contracts while replacing PostgreSQL-specific persistence and mandatory Parquet in the implementation stages. Native commands implement this direction. Transitional collectors and their regression tests still use PostgreSQL until their adapters are ported; they require the `legacy` extra. Do not restore the previously retired SQLite subsystem or silently migrate user data.

Preserve immutable publications, missingness semantics, authority registry pins and source hashes. `engine/` owns reusable calculation with explicit inputs. Actual strategy records, recipes, assets, thresholds, weights, performance and holdings belong in separately managed private storage. Strategy-specific code, if needed, belongs in a private implementation package. Public tests use synthetic inputs and independent expected results. Do not import a private strategy package at installation or in CI.

The historical DAA implementation and tests have a preserved private baseline; they are not the public engine's defaults or the new strategy database. Changes to private data, collector activation, provider calls and trading each need their own defined scope. Never test against recovered or production data.

When adopting an external implementation, record its revision, license, changes, and behavior checks. Reference material does not authorize copying an entire subsystem or reviving superseded requirements.

## Find the owner

| Area | Read first |
|---|---|
| Native installation, storage and backup | [src/aegis_alpha/storage/AGENTS.md](src/aegis_alpha/storage/AGENTS.md) |
| Package structure and imports | [src/aegis_alpha/AGENTS.md](src/aegis_alpha/AGENTS.md) |
| Verification commands and process handling | [scripts/AGENTS.md](scripts/AGENTS.md) |
| Test fixtures, offline execution, and DB isolation | [tests/AGENTS.md](tests/AGENTS.md) |
| Retained data and publication | [src/aegis_alpha/data/AGENTS.md](src/aegis_alpha/data/AGENTS.md) |
| Generic rule engine and external bundles | [src/aegis_alpha/engine/AGENTS.md](src/aegis_alpha/engine/AGENTS.md) |
| Retained migrations | [migrations/AGENTS.md](migrations/AGENTS.md) |
| Architecture, decisions, and operations docs | [dev-notes/AGENTS.md](dev-notes/AGENTS.md) |

## Working commands

Use the repository's Python/uv toolchain, ruff, and ty. Read versions and settings from `.python-version`, `pyproject.toml`, locks, and scripts; do not copy pins into prose. Preserve frozen validated value models and established serialization contracts.

After preparing the locked development environment:

```bash
uv run --no-sync aas status
uv run --no-sync pytest tests/application tests/container
uv run --no-sync pytest -m "not database"
uv run --no-sync ruff format --check .
uv run --no-sync ruff check .
uv run --no-sync ty check src tests
```

Choose the commands relevant to the change; this is not a mandatory sequence. `./scripts/verify` currently runs the full Python regression including PostgreSQL. Its lanes can prepare dependencies and provision a disposable DB, so inspect [scripts/AGENTS.md](scripts/AGENTS.md) before invoking them.

CI uses repository-owned independent jobs with scoped dev coverage and full release coverage, followed by a result-only gate. `scripts/ci_changes.py` owns the conservative selection map. Read [implementation status and transition](POLICY.md#transition) and the PR's actual run evidence before claiming remote activation or faster feedback. Check both classic protection and Rulesets when inspecting branch safeguards.
