from __future__ import annotations

# ruff: noqa: E501 -- fixed SQL and fixture expectations are intentionally explicit.
import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from psycopg import sql
from sqlalchemy import Engine, text

from aegis_alpha.collection.records import (
    CollectionMode,
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    RunEventType,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.identity.records import (
    EntityType,
    IdentifierAssertion,
    IdentifierType,
    Instrument,
    InstrumentKind,
    Issuer,
)
from aegis_alpha.identity.registry import IdentityRegistry
from aegis_alpha.metadata.registry import MetadataRegistry
from aegis_alpha.metadata.snapshot_import import adopt_snapshot
from aegis_alpha.metadata.snapshot_manifest import SnapshotImportError, load_snapshot
from tests.metadata.snapshot_support import clear_snapshot_fixture, fixture_snapshot
from tests.metadata.test_registry import _dataset_registration, _source_registration


@pytest.fixture
def adoption_db(clean_postgres: Engine) -> Iterator[Engine]:
    with clean_postgres.begin() as connection:
        name = connection.scalar(text("SELECT current_database()"))
        original = connection.scalar(
            text(
                "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()"
            )
        )
        head = connection.scalar(text("SELECT version_num FROM alembic_version"))
        connection.exec_driver_sql(
            sql.SQL("COMMENT ON DATABASE {} IS {}")
            .format(sql.Identifier(name), sql.Literal("aas-runtime-install/v1:" + head))
            .as_string()
        )
    try:
        yield clean_postgres
    finally:
        clear_snapshot_fixture(clean_postgres)
        with clean_postgres.begin() as connection:
            connection.exec_driver_sql(
                sql.SQL("COMMENT ON DATABASE {} IS {}")
                .format(sql.Identifier(name), sql.Literal(original))
                .as_string()
            )


def _populate(engine: Engine) -> None:
    registry = MetadataRegistry(engine)
    registry.register_source_snapshot(_source_registration())
    registry.register_dataset(_dataset_registration())
    identity = IdentityRegistry(engine)
    now = datetime(2026, 7, 30, tzinfo=UTC)
    identity.register_issuer(Issuer("issuer-test", now))
    identity.register_instrument(
        Instrument("instrument-test", "issuer-test", InstrumentKind.EQUITY, now)
    )
    for entity, key, kind, value in (
        (EntityType.ISSUER, "issuer-test", IdentifierType.CIK, "0000000001"),
        (EntityType.INSTRUMENT, "instrument-test", IdentifierType.NORGATE_ASSETID, "123"),
    ):
        identity.assert_identifier(
            IdentifierAssertion(
                "assertion-" + key,
                entity,
                key,
                kind,
                value,
                "norgate-2026-07-29-us-platinum",
                now,
                now,
            )
        )


def test_snapshot_preserves_generated_values_and_is_idempotent(
    adoption_db: Engine, tmp_path: Path
) -> None:
    _populate(adoption_db)
    root = tmp_path / "snapshot"
    digest = fixture_snapshot(adoption_db, root)
    clear_snapshot_fixture(adoption_db)
    assert adopt_snapshot(adoption_db, root, digest)["adopted"] is True
    assert adopt_snapshot(adoption_db, root, digest)["adopted"] is False
    with adoption_db.connect() as connection:
        values = connection.execute(
            text(
                "SELECT issuer_ref,instrument_ref FROM identity_identifier_assertions ORDER BY assertion_id"
            )
        ).all()
        assert set(values) == {("issuer-test", None), (None, "instrument-test")}
        assert connection.scalar(text("SELECT source_run_id FROM dataset_versions")) is None


@pytest.mark.parametrize("defect", ["receipt", "watermark"])
def test_fk_compatible_bad_history_rolls_back(
    adoption_db: Engine, tmp_path: Path, defect: str
) -> None:
    MetadataRegistry(adoption_db).register_source_snapshot(_source_registration())
    registry = CollectionRegistry(adoption_db)
    now = datetime(2026, 7, 30, tzinfo=UTC)
    plan = CollectionRunPlan(
        "plan",
        1,
        "norgate",
        "us_platinum_frozen_export",
        CollectionMode.INCREMENTAL,
        None,
        None,
        {},
        now,
    )
    registry.register_plan(plan)
    registry.start_run(CollectionRun("run", "plan", now))
    if defect == "watermark":
        registry.append_event(
            CollectionRunEvent("run", RunEventType.ATTEMPT_STARTED, now, attempt_number=1)
        )
        registry.append_event(
            CollectionRunEvent(
                "run", RunEventType.ATTEMPT_FAILED, now, attempt_number=1, error_class="fixture"
            )
        )
        registry.append_event(
            CollectionRunEvent("run", RunEventType.RUN_FAILED, now, error_class="fixture")
        )
    with adoption_db.begin() as connection:
        if defect == "receipt":
            connection.execute(
                text(
                    "INSERT INTO collection_run_receipts(run_id,attempt_number,source_snapshot_id) VALUES ('run',1,'norgate-2026-07-29-us-platinum')"
                )
            )
        else:
            connection.execute(
                text(
                    "INSERT INTO collection_watermarks(provider,dataset,stream,watermark_seq,run_id,watermark_value,watermark_position) VALUES ('norgate','us_platinum_frozen_export','test',1,'run','x',now())"
                )
            )
    root = tmp_path / "snapshot"
    digest = fixture_snapshot(adoption_db, root)
    clear_snapshot_fixture(adoption_db)
    with pytest.raises(ValueError, match="collection history"):
        adopt_snapshot(adoption_db, root, digest)
    with adoption_db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM source_snapshots")) == 0
        assert connection.scalar(text("SELECT count(*) FROM engine.data_adoptions")) == 0


def test_tampered_input_and_generated_expression_refused(
    adoption_db: Engine, tmp_path: Path
) -> None:
    _populate(adoption_db)
    root = tmp_path / "snapshot"
    digest = fixture_snapshot(adoption_db, root)
    clear_snapshot_fixture(adoption_db)
    table_file = root / "identity_issuers.input.bin"
    original = table_file.read_bytes()
    table_file.write_bytes(original + b"x")
    with pytest.raises(SnapshotImportError, match="hash/size"):
        load_snapshot(root, digest)
    table_file.write_bytes(original)
    manifest = json.loads((root / "manifest.json").read_bytes())
    table = next(t for t in manifest["tables"] if t["name"] == "identity_identifier_assertions")
    next(c for c in table["columns"] if c["generated"])["generated_expression"] = "NULL::text"
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    (root / "manifest.json").write_bytes(payload)
    with pytest.raises(SnapshotImportError, match="generation expression"):
        adopt_snapshot(adoption_db, root, hashlib.sha256(payload).hexdigest())


def test_nonempty_target_is_preserved(adoption_db: Engine, tmp_path: Path) -> None:
    _populate(adoption_db)
    root = tmp_path / "snapshot"
    digest = fixture_snapshot(adoption_db, root)
    with pytest.raises(SnapshotImportError, match="contains data"):
        adopt_snapshot(adoption_db, root, digest)
    with adoption_db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM identity_instruments")) == 1


def test_concurrent_adoption_lock_refuses_without_writes(
    adoption_db: Engine, tmp_path: Path
) -> None:
    root = tmp_path / "snapshot"
    digest = fixture_snapshot(adoption_db, root)
    with adoption_db.connect() as lock:
        lock.execute(text("SELECT pg_advisory_lock(hashtextextended('aas:data-adoption',0))"))
        try:
            with pytest.raises(SnapshotImportError, match="another adoption"):
                adopt_snapshot(adoption_db, root, digest)
        finally:
            lock.execute(text("SELECT pg_advisory_unlock(hashtextextended('aas:data-adoption',0))"))
    with adoption_db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM engine.data_adoptions")) == 0
