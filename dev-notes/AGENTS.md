# Development documentation

Current architecture lives in `architecture.md`, runnable procedures in
`operations.md`, and decisions in `decisions/README.md`. Decision 0015 owns the external-tool-facing data/research engine goal. Decisions 0009
through 0014 retain current CLI/module, data, public/private, legacy retirement and
embedded-storage contracts within their documented scope.

- Keep architecture, procedures and rationale in their owning documents.
- Runtime versions, image digests and trust pins belong in code/config/locks;
  link to those owners rather than duplicating their values in prose.
- A new accepted decision requires an owner instruction and an index entry.
- Historical operation records, real strategy definitions, holdings, performance,
  internal host inventories and private data locations stay outside this checkout.
- Preserve generic failure contracts when moving their original evidence privately.
- Earlier VT-only or retired-DB proposals do not override current decisions.
- Decision 0014 replaces PostgreSQL/Parquet as the target storage. Distinguish the
  implemented SQLite/DuckDB commands from transitional PostgreSQL commands and
  accepted-but-unimplemented service designs. An accepted decision is not runtime proof.
- Do not claim implementation, installed behavior or provider success from a design.
- Match the document's existing language. POLICY.md and agent instructions use English.
