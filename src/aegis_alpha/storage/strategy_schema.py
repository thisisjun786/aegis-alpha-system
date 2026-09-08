"""STRICT relational schema for the private strategy SQLite store."""

from __future__ import annotations

STRATEGY_KIND = "strategies"

STRATEGY_DDL = """
CREATE TABLE strategies (
    strategy_id TEXT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    lifecycle TEXT NOT NULL,
    CHECK(length(trim(strategy_id)) > 0 AND strategy_id = trim(strategy_id)),
    CHECK(length(trim(name)) > 0 AND name = trim(name)),
    CHECK(lifecycle IN ('active', 'archived'))
) STRICT;
CREATE TABLE strategy_versions (
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    raw_bundle BLOB NOT NULL,
    raw_sha256 TEXT NOT NULL,
    contract_json TEXT NOT NULL,
    contract_sha256 TEXT NOT NULL,
    rule_schema TEXT NOT NULL,
    engine_compatibility TEXT NOT NULL,
    imported_at_us INTEGER NOT NULL,
    PRIMARY KEY (strategy_id, version),
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id),
    CHECK(length(trim(version)) > 0 AND version = trim(version)),
    CHECK(length(raw_sha256) = 64 AND raw_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(length(contract_sha256) = 64 AND contract_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK(length(trim(rule_schema)) > 0 AND rule_schema = trim(rule_schema)),
    CHECK(length(trim(engine_compatibility)) > 0 AND engine_compatibility =
 trim(engine_compatibility)),
    CHECK(length(contract_json) > 0),
    CHECK(imported_at_us >= 0)
) STRICT;
CREATE TABLE strategy_lineage (
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    parent_strategy_id TEXT NOT NULL,
    parent_version TEXT,
    change_kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    reason_hash TEXT NOT NULL,
    parent_status TEXT NOT NULL,
    PRIMARY KEY (strategy_id, version, parent_strategy_id, change_kind),
    FOREIGN KEY (strategy_id, version) REFERENCES strategy_versions(strategy_id, version),
    CHECK(length(trim(parent_strategy_id)) > 0 AND parent_strategy_id = trim(parent_strategy_id)),
    CHECK(
        parent_version IS NULL
        OR (length(trim(parent_version)) > 0 AND parent_version = trim(parent_version))
    ),
    CHECK(length(trim(change_kind)) > 0 AND change_kind = trim(change_kind)),
    CHECK(length(trim(reason)) > 0),
    CHECK(length(reason_hash) = 64 AND reason_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK(parent_status IN ('resolved', 'unresolved')),
    CHECK(
        (parent_status = 'unresolved')
        OR (
            parent_status = 'resolved'
            AND parent_version IS NOT NULL
        )
    )
) STRICT;
CREATE TABLE strategy_sources (
    source_id TEXT NOT NULL PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    origin_ref TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    license TEXT NOT NULL,
    provenance TEXT NOT NULL,
    FOREIGN KEY (strategy_id, version) REFERENCES strategy_versions(strategy_id, version),
    CHECK(length(trim(source_id)) > 0 AND source_id = trim(source_id)),
    CHECK(length(trim(origin_ref)) > 0 AND origin_ref = trim(origin_ref)),
    CHECK(length(source_hash) = 64 AND source_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK(length(trim(license)) > 0 AND license = trim(license)),
    CHECK(length(trim(provenance)) > 0 AND provenance = trim(provenance))
) STRICT;
CREATE TABLE strategy_requirements (
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    role TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    required_schema TEXT NOT NULL,
    required_field TEXT NOT NULL,
    domain TEXT NOT NULL,
    warmup INTEGER NOT NULL,
    basis TEXT NOT NULL,
    cadence TEXT NOT NULL,
    PRIMARY KEY (strategy_id, version, role, ordinal),
    FOREIGN KEY (strategy_id, version) REFERENCES strategy_versions(strategy_id, version),
    CHECK(length(trim(role)) > 0 AND role = trim(role)),
    CHECK(ordinal >= 1),
    CHECK(length(trim(required_schema)) > 0 AND required_schema = trim(required_schema)),
    CHECK(length(trim(required_field)) > 0 AND required_field = trim(required_field)),
    CHECK(length(trim(domain)) > 0 AND domain = trim(domain)),
    CHECK(warmup >= 0),
    CHECK(length(trim(basis)) > 0 AND basis = trim(basis)),
    CHECK(length(trim(cadence)) > 0 AND cadence = trim(cadence))
) STRICT;
CREATE TABLE reference_metrics (
    metric_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    value TEXT,
    value_state TEXT NOT NULL,
    period TEXT,
    universe_ref TEXT,
    risk_free_ref TEXT,
    cost_condition_hash TEXT,
    execution_condition_hash TEXT,
    benchmark_condition_hash TEXT,
    risk_free_condition_hash TEXT,
    comparison_condition_hash TEXT,
    source_id TEXT,
    PRIMARY KEY (strategy_id, version, metric_id),
    FOREIGN KEY (strategy_id, version) REFERENCES strategy_versions(strategy_id, version),
    FOREIGN KEY (source_id) REFERENCES strategy_sources(source_id),
    CHECK(length(trim(metric_id)) > 0 AND metric_id = trim(metric_id)),
    CHECK(value_state IN (
        'present', 'missing', 'not_collected', 'unsupported', 'invalid', 'not_comparable'
    )),
    CHECK((value_state = 'present') = (value IS NOT NULL)),
    CHECK(value_state <> 'present' OR length(trim(value)) > 0),
    CHECK(risk_free_ref IS NULL OR (length(trim(risk_free_ref)) > 0 AND risk_free_ref =
 trim(risk_free_ref))),
    CHECK(universe_ref IS NULL OR (length(trim(universe_ref)) > 0 AND universe_ref =
 trim(universe_ref))),
    CHECK(period IS NULL OR (length(trim(period)) > 0 AND period = trim(period))),
    CHECK(cost_condition_hash IS NULL OR (length(cost_condition_hash) = 64 AND
 cost_condition_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK(execution_condition_hash IS NULL OR (length(execution_condition_hash) = 64 AND
 execution_condition_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK(benchmark_condition_hash IS NULL OR (length(benchmark_condition_hash) = 64 AND
 benchmark_condition_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK(risk_free_condition_hash IS NULL OR (length(risk_free_condition_hash) = 64 AND
 risk_free_condition_hash NOT GLOB '*[^0-9a-f]*')),
    CHECK(comparison_condition_hash IS NULL OR (length(comparison_condition_hash) = 64 AND
 comparison_condition_hash NOT GLOB '*[^0-9a-f]*'))
) STRICT;
CREATE TABLE strategy_imports (
    operation_id TEXT NOT NULL PRIMARY KEY,
    request_hash TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    completed_at_us INTEGER NOT NULL,
    FOREIGN KEY (strategy_id, version) REFERENCES strategy_versions(strategy_id, version),
    CHECK(length(trim(operation_id)) > 0 AND operation_id = trim(operation_id)),
    CHECK(length(request_hash) = 64 AND request_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK(completed_at_us >= 0)
) STRICT;
CREATE INDEX strategy_versions_raw_sha256 ON strategy_versions(raw_sha256);
CREATE INDEX strategy_lineage_parent ON strategy_lineage(parent_strategy_id, parent_version);
CREATE INDEX strategy_sources_version ON strategy_sources(strategy_id, version);
CREATE INDEX strategy_imports_request_hash ON strategy_imports(request_hash);
CREATE INDEX strategy_imports_version ON strategy_imports(strategy_id, version);
CREATE INDEX reference_metrics_risk_free ON reference_metrics(risk_free_ref);
CREATE TRIGGER strategy_versions_reject_update
BEFORE UPDATE ON strategy_versions
BEGIN
    SELECT RAISE(ABORT, 'strategy versions are immutable');
END;
CREATE TRIGGER strategy_lineage_parent_exists
BEFORE INSERT ON strategy_lineage
WHEN NEW.parent_status = 'resolved'
BEGIN
    SELECT RAISE(ABORT, 'resolved parent missing')
    WHERE NOT EXISTS (
        SELECT 1 FROM strategy_versions
        WHERE strategy_id = NEW.parent_strategy_id AND version = NEW.parent_version
    );
END;
CREATE TRIGGER strategy_lineage_reject_cycle
BEFORE INSERT ON strategy_lineage
WHEN NEW.parent_status = 'resolved'
 AND NEW.parent_strategy_id IS NOT NULL
 AND NEW.parent_version IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'strategy lineage cycle')
    WHERE (NEW.strategy_id = NEW.parent_strategy_id AND NEW.version = NEW.parent_version)
       OR EXISTS (
           WITH RECURSIVE walk(strategy_id, version) AS (
               SELECT NEW.parent_strategy_id, NEW.parent_version
               UNION ALL
               SELECT lineage.parent_strategy_id, lineage.parent_version
               FROM strategy_lineage AS lineage
               JOIN walk
                 ON walk.strategy_id = lineage.strategy_id
                AND walk.version = lineage.version
               WHERE lineage.parent_status = 'resolved'
                 AND lineage.parent_strategy_id IS NOT NULL
                 AND lineage.parent_version IS NOT NULL
           )
           SELECT 1 FROM walk
           WHERE walk.strategy_id = NEW.strategy_id AND walk.version = NEW.version
       );
END;
"""

STRATEGY_DDL += "\n".join(
    f"CREATE TRIGGER immutable_{table}_{action.lower()} BEFORE {action} ON {table} "
    "BEGIN SELECT RAISE(ABORT,'immutable strategy evidence'); END;"
    for table in (
        "strategy_versions",
        "strategy_lineage",
        "strategy_sources",
        "strategy_requirements",
        "reference_metrics",
        "strategy_imports",
    )
    for action in ("UPDATE", "DELETE")
)
