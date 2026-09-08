# src/aegis_alpha/identity

## Owners

`records.py` owns validated issuer/instrument/identifier records, `registry.py`
owns registration and conflict resolution, and `schema.py` owns PostgreSQL
constraints. Fixed Norgate bootstrap tools have been retired. Import from the
owning module; the package root has no facade.

## CONVENTIONS
- Facts are immutable and idempotent: insert-then-compare. A second registration with the same projection succeeds silently; a divergent projection raises `IdentityConflictError`.
- Provider mappings key on `(provider, namespace, provider_identifier)` over half-open effective intervals; overlap detection uses a PostgreSQL exclusion index plus `intervals_overlap` in Python.
- Identifier normalization is type-specific and lossless: `source_value` is retained verbatim next to the canonical value; input is ASCII-gated *before* NFKC; LEI/CUSIP/ISIN/FIGI checksums are verified.
- Concurrency is handled with advisory locks inside an explicit transaction, not optimistic retries.
- Only `PASS`/`WARN` source snapshots are admissible (`ADMISSIBLE_SNAPSHOT_STATUSES`); admissibility is enforced in `schema.py` constraints as well as `registry.py`.
- Datetimes are timezone-aware; UTC serialization is explicit at every boundary.

## ANTI-PATTERNS
- Never resolve or join identity by ticker alone — provider identifiers are provider-scoped, and ticker-only namespaces are refused.
- Never promote a `BLOCKED` snapshot into an identity fact.
- Never repair, pad, or launder a malformed identifier during normalization; reject it.
- Never resolve provider ambiguity by guessing or ranking. Overlaps persist as `identity_mapping_conflicts` evidence and raise `IdentityAmbiguityError`.

## Verification

Use `tests/identity/` with the disposable database lane. Preserve conflicting
assertions as evidence, effective-time intervals, immutable facts and source hashes.
