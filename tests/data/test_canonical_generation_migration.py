from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError

_T0 = datetime(2026, 8, 6, 0, 0, tzinfo=UTC)
_ZERO = "0" * 64
_ONE = "1" * 64
_TWO = "2" * 64
_THREE = "3" * 64


def _seed_series_and_receipt(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_series "
                "(series_id, schema_version, contract_version, created_at_utc) "
                "VALUES ('series-test', 2, 'test-contract', :at)"
            ),
            {"at": _T0},
        )
        connection.execute(
            text(
                "INSERT INTO source_snapshots "
                "(snapshot_id, schema_version, provider, dataset, source_uri, "
                "request_fingerprint, parameters_json, requested_at_utc, retrieved_at_utc, "
                "content_type, raw_byte_length, content_sha256, parser_name, parser_version, "
                "validation_status, license_classification, retention_classification, "
                "manifest_json) "
                "VALUES ('snapshot-test', 1, 'fixture', 'canonical', 'fixture://source', "
                "'sha256:' || :sha, '{}', :at, :at, 'application/json', 0, :content, "
                "'fixture', '1', 'PASS', 'test', 'test', '{}')"
            ),
            {"sha": _ONE, "content": _TWO, "at": _T0},
        )
        connection.execute(
            text(
                "INSERT INTO collection_run_plans "
                "(plan_id, schema_version, provider, dataset, mode, parameters_json, "
                "plan_sha256, created_at_utc) VALUES ('plan-test', 1, 'fixture', 'canonical', "
                "'probe', '{}', :plan, :at)"
            ),
            {"plan": _ONE, "at": _T0},
        )
        connection.execute(
            text(
                "INSERT INTO collection_runs (run_id, plan_id, created_at_utc) "
                "VALUES ('run-test', 'plan-test', :at)"
            ),
            {"at": _T0},
        )
        connection.execute(
            text(
                "INSERT INTO collection_run_receipts "
                "(run_id, attempt_number, source_snapshot_id, row_count, byte_count) "
                "VALUES ('run-test', 1, 'snapshot-test', 1, 1)"
            )
        )


def _insert_genesis(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_generations "
                "(generation_id, series_id, generation_type, seq, history_root, created_at_utc) "
                "VALUES ('generation-0', 'series-test', 'genesis_v1', 0, :root, :at)"
            ),
            {"root": _ZERO, "at": _T0},
        )


def _delta_values() -> dict[str, object]:
    values: dict[str, object] = {
        "generation_id": "generation-1",
        "series_id": "series-test",
        "generation_type": "delta_v2",
        "seq": 1,
        "parent_generation_id": "generation-0",
        "run_id": "run-test",
        "attempt_number": 1,
        "source_snapshot_id": "snapshot-test",
        "expected_parent_history_root": _ZERO,
        "delta_merkle_root": _ONE,
        "manifest_core_sha256": _TWO,
        "history_root": _ONE,
        "plan_sha256": _ONE,
        "source_snapshot_sha256": _TWO,
        "identity_authority_sha256": _ONE,
        "row_count": 1,
        "semantic_count": 1,
        "partition_count": 1,
        "assert_count": 1,
        "supersede_count": 0,
        "tombstone_count": 0,
        "evidence_count": 0,
        "created_at_utc": _T0,
    }
    return values


def _insert_delta(
    engine: Engine,
    generation_id: str = "generation-1",
    *,
    seq: int = 1,
    parent: str = "generation-0",
    history_root: str = _ONE,
    **overrides: object,
) -> None:
    values = _delta_values()
    values.update(
        generation_id=generation_id,
        seq=seq,
        parent_generation_id=parent,
        history_root=history_root,
    )
    values.update(overrides)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_generations "
                "(generation_id, series_id, generation_type, seq, parent_generation_id, "
                "run_id, attempt_number, source_snapshot_id, expected_parent_history_root, "
                "delta_merkle_root, manifest_core_sha256, history_root, plan_sha256, "
                "source_snapshot_sha256, identity_authority_sha256, row_count, semantic_count, "
                "partition_count, assert_count, supersede_count, tombstone_count, evidence_count, "
                "created_at_utc) VALUES "
                "(:generation_id, :series_id, :generation_type, :seq, :parent_generation_id, "
                ":run_id, :attempt_number, :source_snapshot_id, :expected_parent_history_root, "
                ":delta_merkle_root, :manifest_core_sha256, :history_root, :plan_sha256, "
                ":source_snapshot_sha256, :identity_authority_sha256, :row_count, :semantic_count, "
                ":partition_count, :assert_count, :supersede_count, :tombstone_count, "
                ":evidence_count, :created_at_utc)"
            ),
            values,
        )


def test_genesis_is_typed_and_can_become_initial_head(clean_postgres: Engine) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_series_heads "
                "(series_id, generation_id, updated_at_utc) VALUES "
                "('series-test', 'generation-0', :at)"
            ),
            {"at": _T0},
        )


def test_genesis_rejects_delta_only_fields(clean_postgres: Engine) -> None:
    _seed_series_and_receipt(clean_postgres)
    with pytest.raises(DBAPIError), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_generations "
                "(generation_id, series_id, generation_type, seq, run_id, "
                "history_root, created_at_utc) "
                "VALUES ('bad-genesis', 'series-test', 'genesis_v1', 0, 'run-test', :root, :at)"
            ),
            {"root": _ZERO, "at": _T0},
        )


def test_delta_sequence_gap_is_rejected_by_generation_trigger(clean_postgres: Engine) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    with pytest.raises(DBAPIError, match="direct parent sequence"):
        _insert_delta(clean_postgres, seq=2)


def test_delta_parent_history_root_is_exact_and_receipt_lineage_is_required(
    clean_postgres: Engine,
) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    with pytest.raises(DBAPIError):
        _insert_delta(clean_postgres, expected_parent_history_root=_ONE)
    with pytest.raises(DBAPIError):
        _insert_delta(clean_postgres, attempt_number=2)


def test_one_collection_attempt_cannot_admit_multiple_generations(
    clean_postgres: Engine,
) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    _insert_delta(clean_postgres)
    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_series "
                "(series_id, schema_version, contract_version, created_at_utc) "
                "VALUES ('series-other', 2, 'test-contract', :at)"
            ),
            {"at": _T0},
        )
        connection.execute(
            text(
                "INSERT INTO canonical_generations "
                "(generation_id, series_id, generation_type, seq, history_root, created_at_utc) "
                "VALUES ('generation-other-0', 'series-other', 'genesis_v1', 0, :root, :at)"
            ),
            {"root": _TWO, "at": _T0},
        )

    with pytest.raises(DBAPIError, match="uq_canonical_generations_run_attempt"):
        _insert_delta(
            clean_postgres,
            generation_id="generation-other-1",
            parent="generation-other-0",
            history_root=_THREE,
            series_id="series-other",
            expected_parent_history_root=_TWO,
        )


def test_head_must_advance_to_exact_direct_child(clean_postgres: Engine) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    _insert_delta(clean_postgres)
    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO collection_runs (run_id, plan_id, created_at_utc) "
                "VALUES ('run-test-2', 'plan-test', :at)"
            ),
            {"at": _T0},
        )
        connection.execute(
            text(
                "INSERT INTO collection_run_receipts "
                "(run_id, attempt_number, source_snapshot_id, row_count, byte_count) "
                "VALUES ('run-test-2', 1, 'snapshot-test', 1, 1)"
            )
        )
    _insert_delta(
        clean_postgres,
        generation_id="generation-2",
        seq=2,
        parent="generation-1",
        run_id="run-test-2",
        expected_parent_history_root=_ONE,
        history_root=_TWO,
    )
    with clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_series_heads "
                "(series_id, generation_id, updated_at_utc) VALUES "
                "('series-test', 'generation-0', :at)"
            ),
            {"at": _T0},
        )
    with (
        pytest.raises(DBAPIError, match="direct next generation"),
        clean_postgres.begin() as connection,
    ):
        connection.execute(
            text(
                "UPDATE canonical_series_heads SET generation_id = 'generation-2', "
                "updated_at_utc = :at WHERE series_id = 'series-test'"
            ),
            {"at": _T0},
        )


def test_artifact_kind_path_and_sha_are_database_checked(clean_postgres: Engine) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    with pytest.raises(DBAPIError), clean_postgres.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_generation_artifacts "
                "(generation_id, ordinal, relative_path, artifact_kind, "
                "content_sha256, size_bytes) "
                "VALUES ('generation-0', 0, '../escape', 'unknown', :sha, 0)"
            ),
            {"sha": _ONE},
        )


def _insert_delta_artifact_and_partition(  # noqa: PLR0913
    engine: Engine,
    *,
    path: str,
    task_id: str,
    ordinal: int = 0,
    section: str = "prices",
    leaf_kind: str = "state",
    adjustment_basis: str | None = "CAPITAL",
    shard_count: int = 1,
    shard_id: int = 0,
    artifact_kind: str = "partition",
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO canonical_generation_artifacts "
                "(generation_id, ordinal, relative_path, artifact_kind, "
                "content_sha256, size_bytes, row_count, schema_sha256) "
                "VALUES ('generation-1', :ordinal, :path, :artifact_kind, :sha, 1, 1, :sha)"
            ),
            {
                "ordinal": ordinal,
                "path": path,
                "sha": _ONE,
                "artifact_kind": artifact_kind,
            },
        )
        connection.execute(
            text(
                "INSERT INTO canonical_partition_evidence "
                "(generation_id, ordinal, relative_path, section, leaf_kind, adjustment_basis, "
                "shard_count, shard_id, partition_key_json, receipt_projection_json, "
                "receipt_sha256, "
                "task_id, size_bytes, content_sha256, row_count, schema_sha256, "
                "partition_records_sha256, semantic_count, semantic_checksum, "
                "source_snapshot_sha256, "
                "selected_input_sha256, transformation_sha256, plan_sha256, "
                "identity_authority_sha256, "
                "logical_key_min, logical_key_max, assert_count, supersede_count, tombstone_count, "
                "evidence_count) VALUES ('generation-1', :ordinal, :path, :section, :leaf_kind, "
                ":adjustment_basis, :shard_count, :shard_id, '{}', '{}', :sha, :task_id, 1, :sha, "
                "1, :sha, :sha, 1, :sha, :sha, :sha, :sha, :sha, "
                ":sha, '00', 'ff', 1, 0, 0, 0)"
            ),
            {
                "ordinal": ordinal,
                "path": path,
                "sha": _ONE,
                "task_id": task_id,
                "section": section,
                "leaf_kind": leaf_kind,
                "adjustment_basis": adjustment_basis,
                "shard_count": shard_count,
                "shard_id": shard_id,
            },
        )


def test_partition_task_id_is_global_and_invariants_are_strict(clean_postgres: Engine) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    _insert_delta(clean_postgres)
    _insert_delta_artifact_and_partition(clean_postgres, path="part-0.jsonl", task_id="task-1")
    with pytest.raises(DBAPIError):
        _insert_delta_artifact_and_partition(
            clean_postgres,
            path="part-1.jsonl",
            task_id="task-1",
            ordinal=1,
        )


def test_partition_section_and_shard_topology_are_checked(clean_postgres: Engine) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    _insert_delta(clean_postgres)
    with pytest.raises(DBAPIError):
        _insert_delta_artifact_and_partition(
            clean_postgres,
            path="bad-section.jsonl",
            task_id="task-bad-section",
            section="diagnostics",
            adjustment_basis=None,
        )


def test_partition_evidence_must_match_partition_artifact_identity(
    clean_postgres: Engine,
) -> None:
    _seed_series_and_receipt(clean_postgres)
    _insert_genesis(clean_postgres)
    _insert_delta(clean_postgres)
    with pytest.raises(DBAPIError):
        _insert_delta_artifact_and_partition(
            clean_postgres,
            path="manifest-as-partition.jsonl",
            task_id="task-artifact-kind",
            artifact_kind="manifest",
        )
