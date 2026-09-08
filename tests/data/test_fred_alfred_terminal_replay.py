"""Terminal replay must use persisted evidence, not coordinated local rehashes."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fred_alfred_collector_support import CREDENTIAL
from sqlalchemy import Connection, Engine, event, select

from aegis_alpha.collection.schema import collection_run_receipts, collection_usage_records
from aegis_alpha.data import fred_alfred_recovery as recovery_module
from aegis_alpha.data.fred_alfred_collector import CollectorError
from aegis_alpha.data.fred_alfred_runtime import run_runtime
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.records import DatasetArtifact
from aegis_alpha.metadata.schema import dataset_artifacts, dataset_versions
from tests.data.test_fred_alfred_runtime import _setup


@pytest.mark.parametrize("restore_before_compare", [False, True])
def test_terminal_replay_rejects_coordinated_parquet_receipt_ready_rehash(
    tmp_path: Path,
    clean_postgres: Engine,
    monkeypatch: pytest.MonkeyPatch,
    *,
    restore_before_compare: bool,
) -> None:
    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    outcome = run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    artifact = next(path for path in outcome.published_paths if "series_id=" in str(path))
    ready_path = config.receipt_path.with_name(config.receipt_path.name + ".ready.json")
    originals = {path: path.read_bytes() for path in (artifact, config.receipt_path, ready_path)}
    table = pq.ParquetFile(artifact).read()
    rows = table.to_pylist()
    rows[0]["value"] = "999.99"
    changed = type(table).from_pylist(rows, schema=table.schema)
    artifact.chmod(0o600)
    pq.write_table(changed, artifact, compression="zstd")
    receipt = json.loads(config.receipt_path.read_bytes())
    receipt["published_artifacts"][str(artifact)] = hashlib.sha256(
        artifact.read_bytes()
    ).hexdigest()
    config.receipt_path.chmod(0o600)
    config.receipt_path.write_bytes(canonical_json_bytes(receipt))
    ready = json.loads(ready_path.read_bytes())
    ready["receipt_sha256"] = hashlib.sha256(config.receipt_path.read_bytes()).hexdigest()
    ready_path.chmod(0o600)
    ready_path.write_bytes(canonical_json_bytes(ready))
    if restore_before_compare:
        project = recovery_module._artifact_projection  # noqa: SLF001 -- precise file-read interleaving

        def restore_after_decode(
            root: Path, receipt: Mapping[str, object]
        ) -> tuple[list[dict[str, object]], list[Path], tuple[DatasetArtifact, ...]]:
            captured = project(root, receipt)
            assert captured[0][0]["value"] == "999.99"
            for path, content in originals.items():
                path.write_bytes(content)
            return captured

        monkeypatch.setattr(recovery_module, "_artifact_projection", restore_after_decode)
    calls = len(opener.urls)
    statements: list[str] = []

    def capture(_connection: Connection, _cursor: object, statement: str, *_args: object) -> None:
        statements.append(statement)

    event.listen(clean_postgres, "before_cursor_execute", capture)
    try:
        with pytest.raises(CollectorError, match="persisted"):
            run_runtime(
                config=config,
                engine=clean_postgres,
                authority=authority,
                credential=CREDENTIAL,
                clock=clock.now,
                monotonic=clock.time,
                sleep=clock.sleep,
            )
    finally:
        event.remove(clean_postgres, "before_cursor_execute", capture)
    assert len(opener.urls) == calls
    assert not any("settle_fred_budget" in statement for statement in statements)
    assert not any(
        statement.lstrip().upper().startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


@pytest.mark.parametrize(
    "projection", ["usage", "artifact", "missing_artifact", "dataset", "receipt"]
)
def test_terminal_replay_rejects_changed_persisted_projection_without_settlement(
    tmp_path: Path, clean_postgres: Engine, monkeypatch: pytest.MonkeyPatch, projection: str
) -> None:
    config, authority, clock, opener = _setup(tmp_path, clean_postgres, monkeypatch)
    run_runtime(
        config=config,
        engine=clean_postgres,
        authority=authority,
        credential=CREDENTIAL,
        clock=clock.now,
        monotonic=clock.time,
        sleep=clock.sleep,
    )
    calls = len(opener.urls)
    with clean_postgres.begin() as connection:
        if projection == "usage":
            connection.execute(
                collection_usage_records.update()
                .where(
                    (collection_usage_records.c.run_id == config.run_identity)
                    & (collection_usage_records.c.metric == "bytes_received")
                )
                .values(quantity=0)
            )
        elif projection == "artifact":
            connection.execute(dataset_artifacts.update().values(media_type="text/plain"))
        elif projection == "missing_artifact":
            connection.execute(dataset_artifacts.delete())
        elif projection == "dataset":
            connection.execute(
                dataset_versions.update().values(row_count=dataset_versions.c.row_count + 1)
            )
        else:
            connection.execute(collection_run_receipts.update().values(receipt_sha256="f" * 64))
        known_spend = connection.execute(
            select(collection_usage_records).order_by(
                collection_usage_records.c.run_id, collection_usage_records.c.usage_seq
            )
        ).all()
    with pytest.raises(CollectorError, match="persisted"):
        run_runtime(
            config=config,
            engine=clean_postgres,
            authority=authority,
            credential=CREDENTIAL,
            clock=clock.now,
            monotonic=clock.time,
            sleep=clock.sleep,
        )
    assert len(opener.urls) == calls
    with clean_postgres.connect() as connection:
        assert (
            connection.execute(
                select(collection_usage_records).order_by(
                    collection_usage_records.c.run_id, collection_usage_records.c.usage_seq
                )
            ).all()
            == known_spend
        )
