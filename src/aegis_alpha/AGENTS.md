# AAS package

Follow root AGENTS.md, POLICY.md and current decisions before changing a domain.
The installed package is a CLI plus generic engine. The product direction is an
external-tool-used data and research engine (decision 0015). That is a boundary
decision, not an implemented HTTP API or server. Import from the owning module;
the package root has no legacy persistence facade.

| Owner | Responsibility |
| --- | --- |
| `application/` | CLI, strict allocation inputs, portfolio composition, data/provider adapters |
| `modules/` | Aegis, Alpha and Hedge responsibilities |
| `engine/` | Versioned external bundles, generic signals, allocation and receipts |
| `storage/` | Current embedded SQLite/DuckDB installation, private strategy store, typed market history, recovery |
| `data/` | Provider acquisition, immutable files, pinned catalog reads |
| `identity/` | Issuer/instrument/identifier facts and provider mappings |
| `metadata/` | Catalog, feature contracts, installation and validated adoption |
| `collection/` | Provider lifecycle, watermarks and signed usage checkpoints on the retained legacy path |

- Preserve `from __future__ import annotations`, strict frozen value models,
  established hashes, serialization, null meanings and UTC timestamps.
- Validate untrusted inputs and domain invariants. Reject ambiguous identity,
  unauthorized writes and unsupported versions; never substitute a default strategy.
- Native DB commands use `storage/workspace.py` and `runtime.json` under `AAS_HOME`.
  Transitional collectors retain explicit credential/authority and legacy DB contracts.
- Keep source and SQL constraints aligned. Existing migrations are immutable.
  New embedded schemas are independent of the transitional Alembic chain.
- Publications, receipts and ledgers are immutable or append-only; conflicting
  re-registration fails rather than overwriting earlier evidence.
- No private strategies, operating paths, owner keys or real data in source/tests.
- Decision 0012 retired VT, the old SQLite persistence implementation, the old
  canonical publisher and fixed Norgate importers. It did not retire current
  `storage/`. Reuse current contracts instead of restoring the old code.

Run focused pytest plus the repository's ruff and ty checks. Tests must use
synthetic files and disposable databases; provider execution is a separate scope.
