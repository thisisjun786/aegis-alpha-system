"""Typed SQLite state schema. Observation payloads belong in DuckDB."""

from __future__ import annotations

_BASE = """
CREATE TABLE source_snapshots (
 snapshot_id TEXT PRIMARY KEY, provider TEXT NOT NULL,
 requested_at_us INTEGER NOT NULL, retrieved_at_us INTEGER NOT NULL,
 publication_at_us INTEGER, status TEXT NOT NULL CHECK(status IN ('raw_verified','quarantined')),
 CHECK(requested_at_us >= 0 AND retrieved_at_us >= requested_at_us),
 CHECK(publication_at_us IS NULL OR publication_at_us >= 0)
) STRICT;
CREATE TABLE source_files (
 snapshot_id TEXT NOT NULL REFERENCES source_snapshots(snapshot_id),
 relative_path TEXT NOT NULL, byte_hash TEXT NOT NULL CHECK(length(byte_hash)=64),
 size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0), PRIMARY KEY(snapshot_id,relative_path)
) STRICT;
CREATE TABLE issuers (issuer_id TEXT PRIMARY KEY, name TEXT NOT NULL) STRICT;
CREATE TABLE instruments (
 instrument_id TEXT PRIMARY KEY, issuer_id TEXT REFERENCES issuers(issuer_id),
 asset_type TEXT NOT NULL, venue TEXT NOT NULL
) STRICT;
CREATE INDEX instruments_issuer ON instruments(issuer_id);
CREATE TABLE identity_assertions (
 assertion_id TEXT PRIMARY KEY, instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
 provider TEXT NOT NULL, namespace TEXT NOT NULL, token TEXT NOT NULL,
 valid_from_us INTEGER NOT NULL, valid_to_us INTEGER, known_from_us INTEGER NOT NULL,
 supersedes_assertion_id TEXT REFERENCES identity_assertions(assertion_id),
 source_snapshot_id TEXT NOT NULL REFERENCES source_snapshots(snapshot_id),
 source_hash TEXT NOT NULL CHECK(length(source_hash)=64),
 CHECK(valid_to_us IS NULL OR valid_to_us > valid_from_us), CHECK(known_from_us >= 0)
) STRICT;
CREATE INDEX identity_provider_key ON identity_assertions(provider,namespace,token,known_from_us);
CREATE TABLE identity_snapshots (
 snapshot_id TEXT PRIMARY KEY, content_hash TEXT NOT NULL CHECK(length(content_hash)=64),
 created_at_us INTEGER NOT NULL
) STRICT;
CREATE TABLE identity_snapshot_members (
 snapshot_id TEXT NOT NULL REFERENCES identity_snapshots(snapshot_id), ordinal INTEGER NOT NULL,
 assertion_id TEXT NOT NULL REFERENCES identity_assertions(assertion_id),
 valid_from_us INTEGER NOT NULL, valid_to_us INTEGER,
 known_from_us INTEGER NOT NULL, known_to_us INTEGER,
 PRIMARY KEY(snapshot_id,ordinal), UNIQUE(snapshot_id,assertion_id),
 CHECK(valid_to_us IS NULL OR valid_to_us > valid_from_us),
 CHECK(known_to_us IS NULL OR known_to_us > known_from_us)
) STRICT;
CREATE TRIGGER identity_projection_no_overlap BEFORE INSERT ON identity_snapshot_members
WHEN EXISTS (
 SELECT 1 FROM identity_snapshot_members m
 JOIN identity_assertions a ON a.assertion_id=m.assertion_id
 JOIN identity_assertions n ON n.assertion_id=NEW.assertion_id
 WHERE m.snapshot_id=NEW.snapshot_id AND a.provider=n.provider
 AND a.namespace=n.namespace AND a.token=n.token
 AND (m.valid_to_us IS NULL OR NEW.valid_from_us<m.valid_to_us)
 AND (NEW.valid_to_us IS NULL OR m.valid_from_us<NEW.valid_to_us)
 AND (m.known_to_us IS NULL OR NEW.known_from_us<m.known_to_us)
 AND (NEW.known_to_us IS NULL OR m.known_from_us<NEW.known_to_us)
) BEGIN SELECT RAISE(ABORT,'identity snapshot interval overlap'); END;
CREATE TABLE universe_versions (
 universe_id TEXT NOT NULL, version TEXT NOT NULL,
 content_hash TEXT NOT NULL CHECK(length(content_hash)=64), PRIMARY KEY(universe_id,version)
) STRICT;
CREATE TABLE universe_members (
 universe_id TEXT NOT NULL, version TEXT NOT NULL,
 instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
 valid_from_us INTEGER NOT NULL, valid_to_us INTEGER, known_from_us INTEGER NOT NULL,
 known_to_us INTEGER, source_snapshot_id TEXT NOT NULL REFERENCES source_snapshots(snapshot_id),
 PRIMARY KEY(universe_id,version,instrument_id,valid_from_us,known_from_us),
 FOREIGN KEY(universe_id,version) REFERENCES universe_versions(universe_id,version),
 CHECK(valid_to_us IS NULL OR valid_to_us>valid_from_us),
 CHECK(known_to_us IS NULL OR known_to_us>known_from_us)
) STRICT;
CREATE TABLE datasets (
 dataset_id TEXT PRIMARY KEY, domain TEXT NOT NULL, record_schema TEXT NOT NULL, owner TEXT NOT
 NULL
) STRICT;
CREATE TABLE dataset_versions (
 dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id), version TEXT NOT NULL,
 generation_id TEXT NOT NULL UNIQUE, parent_generation_id TEXT REFERENCES
 dataset_versions(generation_id),
 sequence INTEGER NOT NULL CHECK(sequence >= 1), chain_hash TEXT NOT NULL
 CHECK(length(chain_hash)=64),
 manifest_hash TEXT NOT NULL CHECK(length(manifest_hash)=64), record_schema TEXT NOT NULL,
 normalizer_version TEXT NOT NULL, transform_hash TEXT NOT NULL CHECK(length(transform_hash)=64),
 identity_snapshot_hash TEXT, authority_policy_hash TEXT, row_count INTEGER NOT NULL
 CHECK(row_count>=0),
 coverage TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('committed','quarantined')),
 PRIMARY KEY(dataset_id,version), UNIQUE(dataset_id,sequence),
 CHECK((parent_generation_id IS NULL AND sequence=1) OR (parent_generation_id IS NOT NULL AND
 sequence>1))
) STRICT;
CREATE TRIGGER dataset_parent BEFORE INSERT ON dataset_versions
WHEN NEW.parent_generation_id IS NOT NULL AND NOT EXISTS (
 SELECT 1 FROM dataset_versions p WHERE p.generation_id=NEW.parent_generation_id
 AND p.dataset_id=NEW.dataset_id AND p.sequence=NEW.sequence-1 AND p.status='committed'
) BEGIN SELECT RAISE(ABORT,'dataset parent mismatch'); END;
CREATE TABLE dataset_sources (
 dataset_id TEXT NOT NULL, version TEXT NOT NULL,
 source_snapshot_id TEXT NOT NULL REFERENCES source_snapshots(snapshot_id),
 PRIMARY KEY(dataset_id,version,source_snapshot_id),
 FOREIGN KEY(dataset_id,version) REFERENCES dataset_versions(dataset_id,version)
) STRICT;
CREATE TABLE quality_checks (
 check_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, version TEXT NOT NULL,
 rule_id TEXT NOT NULL, rule_version TEXT NOT NULL, result TEXT NOT NULL, reason TEXT NOT NULL,
 checked_at_us INTEGER NOT NULL,
 FOREIGN KEY(dataset_id,version) REFERENCES dataset_versions(dataset_id,version)
) STRICT;
CREATE TABLE authority_records (
 authority_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL CHECK(length(payload_hash)=64),
 signer TEXT NOT NULL, scope TEXT NOT NULL, valid_from_us INTEGER NOT NULL,
 valid_to_us INTEGER NOT NULL, signature BLOB NOT NULL, CHECK(valid_to_us>valid_from_us)
) STRICT;
CREATE TABLE authority_revocations (
 revocation_id TEXT PRIMARY KEY, authority_id TEXT NOT NULL REFERENCES
 authority_records(authority_id),
 known_at_us INTEGER NOT NULL, reason TEXT NOT NULL
) STRICT;
CREATE TABLE eligibility_events (
 event_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, version TEXT NOT NULL, scope TEXT NOT NULL,
 authority_id TEXT REFERENCES authority_records(authority_id), known_at_us INTEGER NOT NULL,
 decision TEXT NOT NULL CHECK(decision IN ('allowed','blocked','unverified')), reason TEXT NOT
 NULL,
 FOREIGN KEY(dataset_id,version) REFERENCES dataset_versions(dataset_id,version)
) STRICT;
CREATE TABLE storage_operations (
 operation_id TEXT PRIMARY KEY, kind TEXT NOT NULL, request_hash TEXT NOT NULL
 CHECK(length(request_hash)=64),
 target_id TEXT NOT NULL, expected_parent TEXT, payload_hash TEXT NOT NULL
 CHECK(length(payload_hash)=64),
 phase TEXT NOT NULL CHECK(phase IN ('PREPARED','COMPLETED','QUARANTINED')),
 failure_reason TEXT, created_at_us INTEGER NOT NULL, completed_at_us INTEGER,
 CHECK((phase='COMPLETED') = (completed_at_us IS NOT NULL))
) STRICT;
CREATE TRIGGER operation_identity BEFORE UPDATE ON storage_operations
WHEN NEW.operation_id!=OLD.operation_id OR NEW.kind!=OLD.kind OR
 NEW.request_hash!=OLD.request_hash
 OR NEW.target_id!=OLD.target_id OR NEW.expected_parent IS NOT OLD.expected_parent
 OR NEW.payload_hash!=OLD.payload_hash OR NEW.created_at_us!=OLD.created_at_us
 OR OLD.phase IN ('COMPLETED','QUARANTINED')
BEGIN SELECT RAISE(ABORT,'immutable storage operation'); END;
CREATE TABLE collection_jobs (
 job_id TEXT PRIMARY KEY, provider TEXT NOT NULL, dataset_id TEXT NOT NULL,
 window_start_us INTEGER, window_end_us INTEGER, policy_hash TEXT NOT NULL,
 idempotency_key TEXT NOT NULL UNIQUE, status TEXT NOT NULL,
 CHECK(window_end_us IS NULL OR window_end_us>=window_start_us)
) STRICT;
CREATE TABLE collection_attempts (
 job_id TEXT NOT NULL REFERENCES collection_jobs(job_id), attempt INTEGER NOT NULL
 CHECK(attempt>0),
 status TEXT NOT NULL CHECK(status IN ('reserved','started','succeeded','failed','uncertain')),
 started_at_us INTEGER NOT NULL, completed_at_us INTEGER, request_hash TEXT NOT NULL,
 PRIMARY KEY(job_id,attempt)
) STRICT;
CREATE TABLE usage_events (
 event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, attempt INTEGER NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('reserved','charged','released','uncertain')),
 units TEXT NOT NULL, receipt_hash TEXT NOT NULL CHECK(length(receipt_hash)=64),
 known_at_us INTEGER NOT NULL,
 FOREIGN KEY(job_id,attempt) REFERENCES collection_attempts(job_id,attempt)
) STRICT;
CREATE TABLE watermarks (
 provider TEXT NOT NULL, dataset_id TEXT NOT NULL, partition_id TEXT NOT NULL,
 committed_version TEXT NOT NULL, through_us INTEGER NOT NULL,
 PRIMARY KEY(provider,dataset_id,partition_id),
 FOREIGN KEY(dataset_id,committed_version) REFERENCES dataset_versions(dataset_id,version)
) STRICT;
CREATE TRIGGER watermark_committed BEFORE INSERT ON watermarks WHEN NOT EXISTS (
 SELECT 1 FROM dataset_versions WHERE dataset_id=NEW.dataset_id AND
 version=NEW.committed_version AND status='committed'
) BEGIN SELECT RAISE(ABORT,'watermark requires committed data'); END;
CREATE TRIGGER watermark_monotonic BEFORE UPDATE ON watermarks WHEN NEW.through_us<OLD.through_us
 OR NOT EXISTS (SELECT 1 FROM dataset_versions WHERE dataset_id=NEW.dataset_id AND
 version=NEW.committed_version AND status='committed')
BEGIN SELECT RAISE(ABORT,'watermark must advance to committed data'); END;
CREATE TABLE feature_contracts (
 name TEXT NOT NULL, version TEXT NOT NULL, definition TEXT NOT NULL, record_schema TEXT NOT NULL,
 content_hash TEXT NOT NULL CHECK(length(content_hash)=64), PRIMARY KEY(name,version)
) STRICT;
CREATE TABLE feature_inputs (
 name TEXT NOT NULL, version TEXT NOT NULL, ordinal INTEGER NOT NULL,
 ref_kind TEXT NOT NULL, ref_id TEXT NOT NULL, ref_version TEXT NOT NULL, content_hash TEXT NOT
 NULL,
 PRIMARY KEY(name,version,ordinal), FOREIGN KEY(name,version) REFERENCES
 feature_contracts(name,version)
) STRICT;
CREATE TABLE conventions (
 kind TEXT NOT NULL CHECK(kind IN
 ('calendar','fx','basis','cost','execution','benchmark','risk_free')),
 convention_id TEXT NOT NULL, version TEXT NOT NULL, payload TEXT NOT NULL,
 content_hash TEXT NOT NULL CHECK(length(content_hash)=64), PRIMARY
 KEY(kind,convention_id,version)
) STRICT;
CREATE TABLE input_bundles (
 bundle_id TEXT PRIMARY KEY, content_hash TEXT NOT NULL CHECK(length(content_hash)=64),
 record_schema TEXT NOT NULL
) STRICT;
CREATE TABLE input_bindings (
 bundle_id TEXT NOT NULL REFERENCES input_bundles(bundle_id), role TEXT NOT NULL, ordinal
 INTEGER NOT NULL,
 ref_kind TEXT NOT NULL, ref_id TEXT NOT NULL, ref_version TEXT NOT NULL
 CHECK(ref_version!='latest'),
 content_hash TEXT NOT NULL CHECK(length(content_hash)=64), PRIMARY KEY(bundle_id,role,ordinal)
) STRICT;
CREATE TABLE runs (
 run_id TEXT PRIMARY KEY, prior_run_id TEXT REFERENCES runs(run_id),
 bundle_id TEXT NOT NULL REFERENCES input_bundles(bundle_id), engine_hash TEXT NOT NULL,
 environment_hash TEXT NOT NULL, seed INTEGER, reason TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED','INTERRUPTED','QUARANTINED')),
 created_at_us INTEGER NOT NULL, completed_at_us INTEGER, result_hash TEXT,
 CHECK(status!='SUCCESS' OR (result_hash IS NOT NULL AND completed_at_us IS NOT NULL))
) STRICT;
CREATE TRIGGER completed_run BEFORE UPDATE ON runs WHEN OLD.status!='RUNNING'
BEGIN SELECT RAISE(ABORT,'completed run is immutable'); END;
CREATE TABLE run_events (
 run_id TEXT NOT NULL REFERENCES runs(run_id), sequence INTEGER NOT NULL,
 known_at_us INTEGER NOT NULL, kind TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY
 KEY(run_id,sequence)
) STRICT;
CREATE TABLE run_strategies (
 run_id TEXT NOT NULL REFERENCES runs(run_id), module TEXT NOT NULL, ordinal INTEGER NOT NULL,
 strategy_store_id TEXT NOT NULL, strategy_id TEXT NOT NULL, version TEXT NOT NULL,
 raw_hash TEXT NOT NULL CHECK(length(raw_hash)=64), contract_hash TEXT NOT NULL
 CHECK(length(contract_hash)=64),
 PRIMARY KEY(run_id,module,ordinal)
) STRICT;
CREATE TABLE module_manifests (
 run_id TEXT NOT NULL REFERENCES runs(run_id), module TEXT NOT NULL,
 output_schema TEXT NOT NULL, content_hash TEXT NOT NULL, row_count INTEGER NOT NULL
 CHECK(row_count>=0),
 PRIMARY KEY(run_id,module)
) STRICT;
CREATE TABLE compositions (
 composition_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(run_id),
 decision_at_us INTEGER NOT NULL, input_output_refs TEXT NOT NULL, budgets TEXT NOT NULL,
 content_hash TEXT NOT NULL
) STRICT;
CREATE TABLE run_metrics (
 run_id TEXT NOT NULL REFERENCES runs(run_id), metric TEXT NOT NULL, definition_version TEXT NOT
 NULL,
 value TEXT, value_state TEXT NOT NULL, benchmark_ref TEXT, risk_free_ref TEXT, cost_ref TEXT,
 comparison_condition_hash TEXT, PRIMARY KEY(run_id,metric,definition_version),
 CHECK((value IS NOT NULL)=(value_state='present')),
 CHECK(metric NOT IN ('sharpe','sortino') OR value_state!='present' OR risk_free_ref IS NOT NULL)
) STRICT;
CREATE TABLE artifacts (
 run_id TEXT NOT NULL REFERENCES runs(run_id), relative_path TEXT NOT NULL, media_type TEXT NOT
 NULL,
 size_bytes INTEGER NOT NULL CHECK(size_bytes>=0), content_hash TEXT NOT NULL
 CHECK(length(content_hash)=64),
 PRIMARY KEY(run_id,relative_path)
) STRICT;
"""
_IMMUTABLE = (
    "source_snapshots",
    "source_files",
    "issuers",
    "instruments",
    "identity_assertions",
    "identity_snapshots",
    "identity_snapshot_members",
    "universe_versions",
    "universe_members",
    "datasets",
    "dataset_versions",
    "dataset_sources",
    "quality_checks",
    "authority_records",
    "authority_revocations",
    "eligibility_events",
    "usage_events",
    "feature_contracts",
    "feature_inputs",
    "conventions",
    "input_bundles",
    "input_bindings",
    "run_events",
    "run_strategies",
    "module_manifests",
    "compositions",
    "run_metrics",
    "artifacts",
)
DDL = _BASE + "\n".join(
    f"CREATE TRIGGER immutable_{table}_{action.lower()} BEFORE {action} ON {table} "
    "BEGIN SELECT RAISE(ABORT,'immutable record'); END;"
    for table in _IMMUTABLE
    for action in ("UPDATE", "DELETE")
)
