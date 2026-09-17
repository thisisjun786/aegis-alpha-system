# Verification and CLI Scripts

Follow [POLICY.md](../POLICY.md) for CI composition and local verification scope. GitHub CI runs selected independent lanes; its required gate only aggregates their results.

## Current entry points

| Entry point | Current behavior |
|---|---|
| `verify` | Full local Python regression: style, types, database-free tests, PostgreSQL tests, package build/install smoke |
| `verify-lib.sh` | Shared locked preparation, validated prepared tokens, owned process handling and synthetic CLI smoke oracle |
| `verify-lane-style` | Combined format/lint; standalone setup installs locked development tools only |
| `verify-lane-format` / `verify-lane-lint` | Standalone ruff checks |
| `verify-lane-types` | ty over `src`, `tests`, the CI Python entry points and the installed-scenario driver |
| `verify-lane-test` | All `not database` tests, with `AAS_TEST_DATABASE_URL` unset |
| `verify-lane-database` | `database` tests using a supplied disposable test DB, or a script-owned pinned PostgreSQL container |
| `verify-lane-build` | Build wheel/sdist, install locked dependencies and wheel in a fresh environment, smoke installed CLI outside checkout, then run the installed end-to-end scenario |
| `verify_installed_scenario.py` | Registration, run, re-read, backup, restore, refusal and recovery against the installed wheel; `seed` under the development interpreter, `scenario` under the installed one |
| `verify-lane-container aas` | Build the selected image and smoke it with no network, host mounts, provider calls or server startup |
| `verify-workflows` | Checksum-pinned actionlint and syntax checks for the verification shell scripts; Linux x86-64 runner tooling |
| `python3 -m scripts.ci_changes` | Complete PR diff selection and candidate parent/tree provenance; see `--help` |
| `python3 -m scripts.ci_gate` | Strict result aggregation, without dependency setup or test execution |
| `python3 -m scripts.ci_docs` | Changed-document links, removed-document references, whitespace and conflict markers |
| `python3 -m scripts.ci_public` | Candidate/distribution checks for private payloads and operating locations |
| `python3 -m scripts.ci_security` | Vulnerability checks for the root locked Python dependency graph |
| `verify-secrets` | Pinned secret scan of available Git history and candidate files |

`AAS_VERIFY_TOPOLOGY` is `serial` by default. `parallel` overlaps style and types, then runs tests, database tests, and build sequentially. An explicit empty or unknown value exits with an error. These are the only supported topology values. The local full Python entry does not build images or validate document diffs; invoke their lanes separately when relevant.

`verify-secrets` has no inherited history exceptions. Validate public migration against the fresh-history candidate; findings in the existing private history are reviewed separately and must not become public scan exclusions.

With `AAS_VERIFY_PREPARED` unset, a standalone lane prepares its own environment. `ensure_verify_toolchain` exports a token binding the repository, environment/interpreter identity, profile and locked inputs. A set token requests validation only: missing/stale preparation fails instead of silently syncing. The full dispatcher prepares once before starting child lanes. The CI setup action persists the token and `AAS_VERIFY_PROFILE` across steps. Unset the token before deliberately preparing a changed environment; never cache it or share `.venv` between jobs. Profiles are `full` (default) and `style` (development tools only).

Download caches are keyed by platform, profile, Python and dependency inputs. Read uv/Python versions, image pins, budgets and patch digests from scripts and locks. The package lane builds/installs in owned temporary directories, so it no longer leaves artifacts in a shared `dist/` directory.

## Changing verification

- Reuse lane logic across local and CI execution. The target is sibling jobs, not a second full verifier behind an aggregate gate.
- Standalone lanes must remain executable from a prepared checkout without relying on another lane's side effects. A parent-prepared mode must validate the environment before skipping setup. Never sync concurrently into one environment.
- Preserve `topology`, `lane-start`, and `lane-end` evidence or update every consumer in the same change. Record exit codes and elapsed time, not just process launch.
- In `lane_bg`, the callsite's trailing `&` owns the tracked job. Do not add an inner `&` that disconnects `wait` from the actual command.
- Reap owned subprocesses and resources on failure, timeout, and cancellation. Keep serial execution available to diagnose parity problems.
- Tests sharing mutable fixtures or databases remain serial. Separate jobs may run concurrently only with separate state. Read [tests/AGENTS.md](../tests/AGENTS.md) before changing DB provisioning or test selection.
- Use disposable test databases only. Never point the database lane at production or recovered data. If the caller supplies a DB, do not destroy resources outside the fixture ownership contract.
- Building a wheel, parsing Compose, running an image, and proving a DB restore are different checks. Report each at its actual scope.

## CLI wrappers

Put application logic and argument handling in the owning package module. Keep script wrappers limited to importing and calling that module's `main`. Inspect existing standalone probes before changing them; their exceptional structure is not a template for new wrappers.

The `ci_*.py` entry points are repository automation, not application commands. Keep them standard-library-only so classification, document checks and aggregation need no application installation. Their contracts are tested under `tests/tools/`.

`verify_installed_scenario.py` is a verification probe, not a `ci_*` entry point and not an
application command, so it may import `aegis_alpha`. Its `seed` mode is the only part that
imports `tests`, because the synthetic input documents come from the repository generator
that the wheel deliberately does not ship. Its `scenario` mode runs under the installed
interpreter with `-I` and must never gain a checkout import.

Preserve `from __future__ import annotations`, strict validation, and justified lint suppressions where the package conventions require them. Do not restore retired runtime environments when changing verification.
