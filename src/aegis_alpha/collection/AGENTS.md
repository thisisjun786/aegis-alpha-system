# src/aegis_alpha/collection

## OVERVIEW
PostgreSQL-backed governance of *provider collection runs* and *signed provider-usage checkpoints* — the ledger that says a run happened and what it spent. 11 modules / 2.6k LOC. Earned its file: distinct domain, `CollectionRegistry` is the top import target in the tree (219 references, 91 files import `aegis_alpha.collection`).

## WHERE TO LOOK
| Task | Module |
|------|--------|
| Start/advance/finish a run, emit events, move watermarks | `registry.py` (910 LOC, `CollectionRegistry`) |
| Run/plan/receipt/event value types | `records.py` (`CollectionMode`, `RunEventType`, `CollectionRunPlan`, `WatermarkAdvance`) |
| Table definitions + CHECK constraints | `schema.py` (7 tables, `collection_*`) |
| Merkle leaves and checkpoint roots | `usage_checkpoint.py` (`UsageRecordLeaf`, `usage_records_root`) |
| Signature verification | `usage_checkpoint_crypto.py` (`Ed25519PublicKeyring`), `usage_checkpoint_schema.py` |
| Persisted checkpoints and provider aggregates | `usage_checkpoint_repository.py`, `provider_usage_repository.py` |
| FMP-specific checkpoint service and advisory lock | `fmp_usage_checkpoint.py` |

## CONVENTIONS
- No module docstrings here; the type names carry the contract. Read `records.py` first — every other module is built on its value types.
- `schema.py` shares the SQLAlchemy `metadata` object imported from `aegis_alpha.metadata.schema`; tables defined here participate in the same Alembic migration chain. Never create a second `MetaData()`.
- Invariants are enforced twice: `records.py` validators (non-empty, tz-aware UTC, frozen JSON objects, SHA-256 shape) and DB `CheckConstraint`s built by `_sha256_check` / `_nonempty_check` / window-pairing checks.
- `_json_type()` yields `JSON().with_variant(JSONB(), "postgresql")` — keep JSON columns going through it.
- Registry methods take an explicit `Connection` and assert it belongs to the owning `Engine` (`_require_same_engine`); they do not open their own transactions.
- Conflicts surface as typed refusals — `CollectionConflictError` (contradictory persisted row) vs `CollectionStateError` (illegal lifecycle transition). Idempotent re-registration compares the persisted projection (`_projection_matches`) instead of rewriting it.
- Quantities are `Decimal` serialized through fixed text form; timestamps are UTC isoformat. Never persist floats for usage counts.
- Checkpoints are content-addressed and signature-verified before use: unknown/stale keys and unsupported contract versions raise rather than degrade (`usage_checkpoint_errors.py` has seven distinct error classes — pick the specific one).
- Cross-run serialization uses a PostgreSQL advisory lock (`acquire_fmp_usage_checkpoint_lock`), not application-level mutexes.

## ANTI-PATTERNS
- Never treat an unsigned or partially verified usage aggregate as trusted; `VerifiedProviderUsage*` types only exist on the verified side of the boundary.
- Never mutate a persisted run, event, receipt, or checkpoint row — the lifecycle is append-only, and lineage checks (`_locked_run_lineage`, `_lineage_allows_dataset`) assume it.
- Never let a caller bypass the registry to insert directly into `collection_*` tables; the constraint set alone does not encode the lifecycle.

## COMMANDS
```bash
uv run --no-sync pytest tests/collection          # 17 modules; uses the clean_postgres session fixture
```
