from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from psycopg import Connection as PsycopgConnection
from psycopg import Error as PsycopgError
from psycopg import sql
from sqlalchemy import Connection, Engine, select, text
from sqlalchemy.exc import SQLAlchemyError

from aegis_alpha.collection.registry import CollectionStateError
from aegis_alpha.collection.snapshot_validation import validate_collection_snapshot
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.identity import (
    schema as identity_schema,  # noqa: F401 -- register shared metadata tables
)
from aegis_alpha.metadata.adoption_schema import data_adoptions
from aegis_alpha.metadata.schema import dataset_versions, metadata
from aegis_alpha.metadata.snapshot_manifest import (
    LegacySnapshot,
    SnapshotImportError,
    SnapshotTable,
    load_snapshot,
)

if TYPE_CHECKING:
    from psycopg import Cursor

_BLOCK_BYTES = 1024 * 1024


def _driver(connection: Connection) -> PsycopgConnection:
    driver = connection.connection.driver_connection
    if not isinstance(driver, PsycopgConnection):
        raise SnapshotImportError("snapshot adoption requires psycopg")
    return driver


def _check_target(connection: Connection, snapshot: LegacySnapshot) -> None:
    if connection.scalar(text("SELECT current_database()")) != connection.engine.url.database:
        raise SnapshotImportError("database identity differs from the configured target")
    if connection.scalar(text("SHOW session_replication_role")) != "origin":
        raise SnapshotImportError("target triggers must be enabled during adoption")
    version = connection.scalar(text("SELECT version_num FROM public.alembic_version"))
    marker = connection.scalar(
        text(
            "SELECT shobj_description(oid,'pg_database') FROM pg_database "
            "WHERE datname=current_database()"
        )
    )
    if marker != "aas-runtime-install/v1:" + str(version):
        raise SnapshotImportError("target is not an identified app installation")
    if connection.scalar(text("SHOW server_encoding")) != "UTF8":
        raise SnapshotImportError("snapshot encoding is incompatible with target")
    if int(str(connection.scalar(text("SHOW server_version_num")))) // 10000 != 18:  # noqa: PLR2004 -- snapshot format supports PG18
        raise SnapshotImportError("snapshot server major is incompatible with target")
    for table in snapshot.tables:
        _check_columns(connection, table)


def _check_columns(connection: Connection, table: SnapshotTable) -> None:
    observed = (
        connection.execute(
            text(
                "SELECT a.attname, format_type(a.atttypid,a.atttypmod) AS sql_type, "
                "a.attgenerated, "
                "CASE WHEN a.attgenerated<>'' THEN pg_get_expr(d.adbin,d.adrelid) "
                "ELSE NULL END AS expression "
                "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum "
                "WHERE n.nspname='public' AND c.relname=:table "
                "AND a.attnum>0 AND NOT a.attisdropped"
            ),
            {"table": table.name},
        )
        .mappings()
        .all()
    )
    columns = {r["attname"]: (r["sql_type"], r["attgenerated"], r["expression"]) for r in observed}
    for column in table.columns:
        if columns.get(column.name) != (
            column.sql_type,
            column.generated,
            column.generated_expression,
        ):
            raise SnapshotImportError("source/target column type or generation expression differs")
    extra = set(columns) - {c.name for c in table.columns}
    if extra != ({"source_run_id"} if table.name == "dataset_versions" else set()):
        raise SnapshotImportError("target has an unreviewed source-schema difference")
    primary = (
        connection.execute(
            text(
                "SELECT a.attname FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY AS k(attnum,ordinality) "
                "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum "
                "WHERE n.nspname='public' AND c.relname=:table "
                "AND p.contype='p' ORDER BY k.ordinality"
            ),
            {"table": table.name},
        )
        .scalars()
        .all()
    )
    if tuple(primary) != table.primary_key:
        raise SnapshotImportError("source/target primary key differs")


def _lock_empty_target(connection: Connection, snapshot: LegacySnapshot) -> None:
    relations = connection.execute(
        text(
            "SELECT n.nspname,c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE c.relkind IN ('r','p') AND n.nspname NOT LIKE 'pg_%' "
            "AND n.nspname<>'information_schema' ORDER BY n.nspname,c.relname"
        )
    ).all()
    targets = [
        sql.Identifier(schema, name)
        for schema, name in relations
        if (schema, name) != ("public", "alembic_version")
    ]
    if not targets or len(snapshot.tables) > len(targets):
        raise SnapshotImportError("target schema is incomplete")
    driver = _driver(connection)
    driver.execute(
        sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(sql.SQL(", ").join(targets))
    )
    for table in targets:
        row = driver.execute(sql.SQL("SELECT count(*) FROM {}").format(table)).fetchone()
        if row is None or row[0]:
            raise SnapshotImportError("target contains data; import will not overwrite or merge it")


def _copy_input(cursor: Cursor, tree: DescriptorTree, table: SnapshotTable) -> None:
    statement = sql.SQL("COPY public.{} ({}) FROM STDIN WITH (FORMAT binary)").format(
        sql.Identifier(table.name),
        sql.SQL(",").join(sql.Identifier(c) for c in table.input_columns),
    )
    digest = hashlib.sha256()
    count = 0
    with tree.binary_reader(table.filename("input")) as handle, cursor.copy(statement) as writer:
        while chunk := handle.read(_BLOCK_BYTES):
            digest.update(chunk)
            count += len(chunk)
            writer.write(chunk)
    if count != table.input_bytes or digest.hexdigest() != table.input_sha256:
        raise SnapshotImportError("snapshot input changed during import")


def _compare_projection(cursor: Cursor, table: SnapshotTable) -> None:
    columns = sql.SQL(",").join(sql.Identifier(c.name) for c in table.columns)
    order = sql.SQL(",").join(
        sql.SQL('CAST({} AS text) COLLATE "C"').format(sql.Identifier(c)) for c in table.primary_key
    )
    query = sql.SQL(
        "COPY (SELECT {} FROM public.{} ORDER BY {}) TO STDOUT WITH (FORMAT binary)"
    ).format(
        columns,
        sql.Identifier(table.name),
        order,
    )
    digest = hashlib.sha256()
    size = 0
    with cursor.copy(query) as reader:
        while chunk := reader.read():
            digest.update(chunk)
            size += len(chunk)
    if digest.hexdigest() != table.projection_sha256 or size != table.projection_bytes:
        raise SnapshotImportError("adopted row projection differs from the original snapshot")
    cursor.execute(sql.SQL("SELECT count(*) FROM public.{}").format(sql.Identifier(table.name)))
    row = cursor.fetchone()
    if row is None or row[0] != table.row_count:
        raise SnapshotImportError("adopted row count differs from the source")


def _copy_all(connection: Connection, snapshot: LegacySnapshot) -> None:
    by_name = {table.name: table for table in snapshot.tables}
    ordered = [
        table.name
        for table in metadata.sorted_tables
        if table.schema is None and table.name in by_name
    ]
    if set(ordered) != set(by_name):
        raise SnapshotImportError("source table owner is not registered in shared metadata")
    with DescriptorTree.open_path(snapshot.root) as tree, _driver(connection).cursor() as cursor:
        for name in ordered:
            _copy_input(cursor, tree, by_name[name])
        for table in snapshot.tables:
            _compare_projection(cursor, table)


def _validate_import(connection: Connection) -> None:
    flags = connection.execute(
        select(
            dataset_versions.c.canonical_eligible,
            dataset_versions.c.backtest_eligible,
            dataset_versions.c.paper_eligible,
            dataset_versions.c.order_eligible,
            dataset_versions.c.source_run_id,
        )
    ).all()
    if any(any(row[:4]) or row[4] is not None for row in flags):
        raise SnapshotImportError("v1 adoption cannot infer eligibility or engine lineage")
    validate_collection_snapshot(connection)


def adopt_snapshot(engine: Engine, root: Path, expected_sha256: str) -> dict[str, object]:
    """Atomically adopt a pinned legacy snapshot, or refuse without changing target data."""
    snapshot = load_snapshot(root, expected_sha256)
    try:
        with engine.begin() as connection:
            connection.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            connection.execute(text("SET LOCAL DateStyle TO 'ISO, YMD'"))
            connection.execute(text("SET LOCAL bytea_output TO 'hex'"))
            connection.execute(text("SET LOCAL lock_timeout TO '5s'"))
            if not connection.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtextextended('aas:data-adoption',0))")
            ):
                raise SnapshotImportError("another adoption is active")
            _check_target(connection, snapshot)
            existing = (
                connection.execute(
                    select(data_adoptions).where(
                        data_adoptions.c.source_manifest_sha256 == snapshot.manifest_sha256
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                return {
                    "adoption_id": existing["adoption_id"],
                    "adopted": False,
                    "row_count": existing["row_count"],
                }
            _lock_empty_target(connection, snapshot)
            _copy_all(connection, snapshot)
            _validate_import(connection)
            receipt = {
                "adoption_id": "adoption:" + snapshot.manifest_sha256,
                "source_manifest_sha256": snapshot.manifest_sha256,
                "source_system_identifier": snapshot.source_system_identifier,
                "source_database": snapshot.source_database,
                "source_alembic_head": snapshot.source_alembic_head,
                "adopted_at_utc": datetime.now(UTC),
                "table_count": len(snapshot.tables),
                "row_count": sum(t.row_count for t in snapshot.tables),
                "file_count": len(snapshot.tables) * 2,
            }
            connection.execute(data_adoptions.insert().values(**receipt))
            return {
                "adoption_id": receipt["adoption_id"],
                "adopted": True,
                "row_count": receipt["row_count"],
            }
    except CollectionStateError:
        raise SnapshotImportError(
            "snapshot collection history failed semantic validation"
        ) from None
    except (SQLAlchemyError, PsycopgError):
        raise SnapshotImportError(
            "snapshot database operation failed; transaction rolled back"
        ) from None
