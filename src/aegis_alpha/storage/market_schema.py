"""Native DuckDB typed observation and result tables."""

from __future__ import annotations

# One decoded text value costs a fixed object header plus its characters: CPython
# allocates 49 + n bytes for compact ASCII, 74 + 2n for UCS-2 and 76 + 4n for UCS-4.
# These sit above all three, so an estimate built from them stays an upper bound on
# what the values actually hold.
TEXT_OVERHEAD_BYTES = 80
TEXT_CHARACTER_BYTES = 4


def text_bytes(values: int, characters: int) -> int:
    """Bound what decoded text values hold live, counting values and characters apart.

    A flat per-character charge is wrong in both directions. Almost all of a short
    value's cost is its header, so charging only for characters undercharges it: a
    one-character string costs about fifty bytes and a per-character model charges
    tens. Almost none of a long value's cost is its header, so the same charge
    overcharges by more than an order of magnitude: a sixty-four character digest
    costs about a hundred and thirteen bytes. Counting both terms is what makes the
    bound hold at each end, which decides real cases because retained market history
    is mostly digests and identifiers.
    """
    if values < 0 or characters < 0:
        raise ValueError("text estimate counts must be non-negative")
    return TEXT_OVERHEAD_BYTES * values + TEXT_CHARACTER_BYTES * characters


def text_columns(schema: tuple[tuple[str, str], ...]) -> int:
    """How many columns of a domain schema can hold a text value."""
    return sum(1 for _name, kind in schema if kind.rstrip("?") == "VARCHAR")


# Hashing a delta is not free of the text it hashes. _encode_rowset holds three UTF-8
# pictures of the same values at once: the buffer one row's cells encode into, the list
# of encoded rows it sorts, and the joined rowset the digest reads. UTF-8 never spends
# more than four bytes on a code point, and each value carries a tag, a length and a
# bytes header on top of its characters.
ROWSET_ENCODING_COPIES = 3
ROWSET_FRAMING_BYTES = 48


def rowset_encoding_bytes(values: int, characters: int) -> int:
    """Bound the encoding workspace a rowset digest needs beside the retained values.

    This is separate from text_bytes on purpose. A history that is merely held costs
    what its decoded values hold; a history being hashed costs that plus the encoded
    copies the codec builds, and only the read that hashes should be charged for them.
    Four-byte text is the case that decides it, because there the encoded copies cost
    as much as the strings they came from rather than a quarter as much.
    """
    if values < 0 or characters < 0:
        raise ValueError("rowset encoding counts must be non-negative")
    return ROWSET_ENCODING_COPIES * (
        TEXT_CHARACTER_BYTES * characters + ROWSET_FRAMING_BYTES * values
    )


COMMON = (
    ("generation_id", "VARCHAR"),
    ("record_id", "VARCHAR"),
    ("revision_id", "VARCHAR"),
    ("supersedes_revision_id", "VARCHAR?"),
    ("op", "VARCHAR"),
    ("available_at_us", "BIGINT?"),
    ("revision_known_at_us", "BIGINT?"),
    ("ingested_at_us", "BIGINT"),
    ("source_snapshot_id", "VARCHAR"),
    ("source_row_hash", "VARCHAR"),
)
DOMAINS = {
    "prices": (
        ("instrument_id", "VARCHAR"),
        ("session_date", "DATE"),
        ("interval", "VARCHAR"),
        ("bar_end_us", "BIGINT"),
        ("basis", "VARCHAR"),
        ("currency", "VARCHAR"),
        ("open", "DECIMAL(38,12)?"),
        ("high", "DECIMAL(38,12)?"),
        ("low", "DECIMAL(38,12)?"),
        ("close", "DECIMAL(38,12)?"),
        ("volume", "DECIMAL(38,12)?"),
        ("price_role", "VARCHAR"),
        ("value_state", "VARCHAR"),
    ),
    "corporate_actions": (
        ("instrument_id", "VARCHAR"),
        ("action_id", "VARCHAR"),
        ("action_type", "VARCHAR"),
        ("ex_date", "DATE?"),
        ("record_date", "DATE?"),
        ("pay_date", "DATE?"),
        ("effective_date", "DATE"),
        ("amount", "DECIMAL(38,12)?"),
        ("ratio", "DECIMAL(38,12)?"),
        ("currency", "VARCHAR?"),
        ("value_state", "VARCHAR"),
    ),
    "instrument_status": (
        ("instrument_id", "VARCHAR"),
        ("status_event_id", "VARCHAR"),
        ("effective_from_us", "BIGINT"),
        ("effective_to_us", "BIGINT?"),
        ("status", "VARCHAR"),
        ("reason", "VARCHAR"),
    ),
    "fundamentals": (
        ("issuer_id", "VARCHAR"),
        ("instrument_id", "VARCHAR?"),
        ("concept", "VARCHAR"),
        ("period_start", "DATE?"),
        ("period_end", "DATE"),
        ("fiscal_period", "VARCHAR"),
        ("unit", "VARCHAR"),
        ("dimensions_hash", "VARCHAR"),
        ("form", "VARCHAR?"),
        ("accession", "VARCHAR?"),
        ("accepted_at_us", "BIGINT?"),
        ("value", "DECIMAL(38,12)?"),
        ("value_state", "VARCHAR"),
    ),
    "macro_observations": (
        ("series_id", "VARCHAR"),
        ("observation_period", "DATE"),
        ("unit", "VARCHAR"),
        ("source_vintage_start", "DATE?"),
        ("source_vintage_end", "DATE?"),
        ("value", "DECIMAL(38,12)?"),
        ("value_state", "VARCHAR"),
    ),
    "estimates": (
        ("instrument_id", "VARCHAR"),
        ("metric", "VARCHAR"),
        ("target_period", "DATE"),
        ("as_of_us", "BIGINT"),
        ("statistic", "VARCHAR"),
        ("value", "DECIMAL(38,12)?"),
        ("value_state", "VARCHAR"),
        ("analyst_count", "BIGINT?"),
    ),
    "fx_rates": (
        ("base_currency", "VARCHAR"),
        ("quote_currency", "VARCHAR"),
        ("fixing_at_us", "BIGINT"),
        ("rate", "DECIMAL(38,12)?"),
        ("value_state", "VARCHAR"),
    ),
    "calendar_sessions": (
        ("calendar_id", "VARCHAR"),
        ("venue", "VARCHAR"),
        ("session_date", "DATE"),
        ("open_at_us", "BIGINT?"),
        ("close_at_us", "BIGINT?"),
        ("status", "VARCHAR"),
        ("timezone_version", "VARCHAR"),
    ),
    "feature_values": (
        ("contract_id", "VARCHAR"),
        ("contract_version", "VARCHAR"),
        ("contract_hash", "VARCHAR"),
        ("input_bundle_hash", "VARCHAR"),
        ("instrument_id", "VARCHAR"),
        ("feature_at_us", "BIGINT"),
        ("value", "DOUBLE?"),
        ("value_state", "VARCHAR"),
    ),
}
NATURAL_KEYS = {
    "prices": (
        "instrument_id",
        "session_date",
        "interval",
        "bar_end_us",
        "basis",
        "currency",
        "price_role",
    ),
    "corporate_actions": ("instrument_id", "action_id"),
    "instrument_status": ("instrument_id", "status_event_id"),
    "fundamentals": (
        "issuer_id",
        "instrument_id",
        "concept",
        "period_start",
        "period_end",
        "fiscal_period",
        "unit",
        "dimensions_hash",
    ),
    "macro_observations": ("series_id", "observation_period", "unit"),
    "estimates": ("instrument_id", "metric", "target_period", "as_of_us", "statistic"),
    "fx_rates": ("base_currency", "quote_currency", "fixing_at_us"),
    "calendar_sessions": ("calendar_id", "venue", "session_date"),
    "feature_values": (
        "contract_id",
        "contract_version",
        "input_bundle_hash",
        "instrument_id",
        "feature_at_us",
    ),
}
RESULTS = {
    "signals": (
        ("instrument_id", "VARCHAR"),
        ("signal_id", "VARCHAR"),
        ("value", "DOUBLE?"),
        ("value_state", "VARCHAR"),
    ),
    "target_weights": (("instrument_id", "VARCHAR"), ("weight", "DECIMAL(38,12)")),
    "simulated_trades": (
        ("instrument_id", "VARCHAR"),
        ("quantity", "DECIMAL(38,12)"),
        ("price", "DECIMAL(38,12)"),
        ("cost", "DECIMAL(38,12)"),
    ),
    "positions": (
        ("instrument_id", "VARCHAR"),
        ("quantity", "DECIMAL(38,12)"),
        ("value", "DECIMAL(38,12)"),
    ),
    "equity_points": (("equity", "DECIMAL(38,12)"), ("cash", "DECIMAL(38,12)")),
}
BASE_DDL = """
CREATE TABLE store_info (
 store_id VARCHAR PRIMARY KEY, installation_id VARCHAR NOT NULL, schema_version INTEGER NOT
 NULL, kind VARCHAR NOT NULL
);
CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, checksum VARCHAR NOT NULL,
 applied_at_us BIGINT NOT NULL);
CREATE TABLE market_generations (
 generation_id VARCHAR PRIMARY KEY, dataset_id VARCHAR NOT NULL, version VARCHAR NOT NULL,
 parent_id VARCHAR REFERENCES market_generations(generation_id), sequence BIGINT NOT NULL
 CHECK(sequence>=1),
 domain VARCHAR NOT NULL, record_schema VARCHAR NOT NULL,
 delta_hash VARCHAR NOT NULL CHECK(length(delta_hash)=64), chain_hash VARCHAR NOT NULL
 CHECK(length(chain_hash)=64),
 row_count BIGINT NOT NULL CHECK(row_count>=0), operation_id VARCHAR NOT NULL UNIQUE,
 request_hash VARCHAR NOT NULL CHECK(length(request_hash)=64), UNIQUE(dataset_id,version),
 UNIQUE(dataset_id,sequence)
);
CREATE TABLE result_commits (
 run_id VARCHAR PRIMARY KEY, operation_id VARCHAR NOT NULL UNIQUE, request_hash VARCHAR NOT NULL,
 manifest_hash VARCHAR NOT NULL, table_hashes VARCHAR NOT NULL, table_counts VARCHAR NOT NULL
);
"""


def columns_sql(columns: tuple[tuple[str, str], ...]) -> str:
    return ", ".join(
        f'"{name}" {kind.rstrip("?")}' + ("" if kind.endswith("?") else " NOT NULL")
        for name, kind in columns
    )


def domain_ddl(name: str, columns: tuple[tuple[str, str], ...]) -> str:
    checks = """
 PRIMARY KEY(generation_id,record_id,revision_id), UNIQUE(record_id,revision_id),
 FOREIGN KEY(generation_id) REFERENCES market_generations(generation_id),
 CHECK(op IN ('ASSERT','SUPERSEDE','TOMBSTONE')),
 CHECK((op='ASSERT' AND supersedes_revision_id IS NULL) OR
       (op IN ('SUPERSEDE','TOMBSTONE') AND supersedes_revision_id IS NOT NULL)),
 CHECK(available_at_us IS NULL OR available_at_us>=0),
 CHECK(revision_known_at_us IS NULL OR revision_known_at_us>=0), CHECK(ingested_at_us>=0),
 CHECK(length(source_row_hash)=64)
"""
    if any(field == "value_state" for field, _ in columns):
        checks += (
            ", CHECK(value_state IN ('present','missing','not_collected','unsupported','invalid'))"
        )
    if name == "prices":
        checks += (
            ", CHECK(price_role IN ('canonical','reference')), "
            "CHECK(basis='unadjusted' OR price_role='reference')"
        )
    return f'CREATE TABLE "{name}" ({columns_sql(COMMON + columns)}, {checks});'


DDL = BASE_DDL + "\n".join(domain_ddl(name, columns) for name, columns in DOMAINS.items())
RESULT_COMMON = (
    ("run_id", "VARCHAR"),
    ("module", "VARCHAR"),
    ("ordinal", "BIGINT"),
    ("at_us", "BIGINT"),
)
DDL += "\n".join(
    f'CREATE TABLE "{name}" ({columns_sql((*RESULT_COMMON, *columns))}, '
    "PRIMARY KEY(run_id,module,ordinal), FOREIGN KEY(run_id) REFERENCES result_commits(run_id));"
    for name, columns in RESULTS.items()
)
