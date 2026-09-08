# migrations/

## OVERVIEW

Alembic revision chain for the retained AAS PostgreSQL metadata surfaces — PostgreSQL only, with guarded
destructive downgrades. **Preserved for controlled adoption** (`dev-notes/decisions/0010-backtest-data-foundation.md`).
Existing revisions are immutable historical contracts. A new revision requires a scoped
implementation plan and migration/data-preservation validation on disposable synthetic
databases under [POLICY.md](../POLICY.md) and [test instructions](../tests/AGENTS.md).
Actual restored or production data must not be used for these tests. The design alone does not authorize runtime DDL, collection restart, or deletion.

Decision [0014](../dev-notes/decisions/0014-local-embedded-databases.md) replaces this
PostgreSQL chain as the target for new installations. Implement new embedded schemas
under the storage owner in the [transition plan](../dev-notes/design/backtest-data-foundation.md#전환-계획과-기존-코드).
Keep this existing chain unchanged until callers and regression contracts are replaced;
then retire its unused code and tests together. Do not translate these revisions in place.

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add a revision | `versions/YYYYMMDD_NNNN_slug.py` |
| Register a new schema module | `env.py` import block (`# noqa: F401`) |
| Revision skeleton | `script.py.mako` |
| Runner config | `../alembic.ini` (`script_location = %(here)s/migrations`, `prepend_sys_path = .`) |
| Chain lifecycle tests | `tests/metadata/test_migrations.py`, `tests/metadata/test_registry_extension_chain.py` |

## CONVENTIONS

- Revision identifiers and parent links are owned by `versions/`. Read the actual chain
  and `alembic heads` before extending it; do not copy a historical head from prose.
  Extend only through the scoped change process; never edit an existing revision.
- The chain branched at `0004` (`20260806_0004_canonical_generation_chain` and
  `20260818_0004_signed_usage_checkpoints`) and was merged by
  `20260821_0005_merge_generation_and_usage_heads`. Check `alembic heads` before adding.
- `env.py` must import every metadata module that owns tables — collection, canonical
  generation, identity, eligibility, feature contract — or autogenerate and the downgrade
  guards silently miss them.
- `alembic.ini` carries no `sqlalchemy.url`; the URL resolves through
  `aegis_alpha.metadata.database.load_database_url()`.
- The owned-database guard reads `AAS_MIGRATION_DATABASE_NAME` and
  `AAS_MIGRATION_OWNER_TOKEN` from the environment in containers; tests supply the same
  pair through `config.attributes`.
- `compare_type=True` in both offline and online modes; online adds `include_schemas=True`.
- SQLAlchemy Core defines tables; constraints, triggers, functions, and exclusion indexes
  are raw SQL inside the revision.

## ANTI-PATTERNS

- Every revision ships a real `downgrade()`. Destructive downgrades refuse to run against
  unowned user tables or leftover ledger/extension rows — keep that refusal.
- Never target SQLite here: identity and metadata revisions rely on PostgreSQL triggers,
  generated typed FK columns, and exclusion indexes.
- Never edit a merged revision in place. The chain is verified end to end by
  `alembic downgrade base` -> `alembic upgrade head` -> `alembic check`.
