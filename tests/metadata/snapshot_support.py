from __future__ import annotations

# ruff: noqa: E501 -- fixed SQL and fixture expectations are intentionally explicit.
import hashlib
import json
from pathlib import Path
from typing import cast

from psycopg import Connection as PsycopgConnection
from psycopg import sql
from sqlalchemy import Engine, text

from aegis_alpha.metadata.snapshot_manifest import SOURCE_TABLES


def clear_snapshot_fixture(engine: Engine) -> None:
    with engine.begin() as connection:
        driver = cast("PsycopgConnection", connection.connection.driver_connection)
        tables = [sql.Identifier("public", name) for name in sorted(SOURCE_TABLES)]
        tables.append(sql.Identifier("engine", "data_adoptions"))
        driver.execute(sql.SQL("TRUNCATE {} CASCADE").format(sql.SQL(",").join(tables)))


def fixture_snapshot(engine: Engine, root: Path) -> str:
    """Encode synthetic rows in the legacy projection; never access recovered data."""
    root.mkdir()
    tables = []
    with engine.connect() as connection:
        connection.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        connection.execute(text("SET LOCAL DateStyle TO 'ISO,YMD'"))
        driver = cast("PsycopgConnection", connection.connection.driver_connection)
        for name in sorted(SOURCE_TABLES):
            columns = [
                dict(row)
                for row in connection.execute(
                    text(
                        "SELECT a.attname AS name,format_type(a.atttypid,a.atttypmod) AS type, "
                        "a.attgenerated AS generated,CASE WHEN a.attgenerated<>'' "
                        "THEN pg_get_expr(d.adbin,d.adrelid) ELSE NULL END AS generated_expression "
                        "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
                        "JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum "
                        "WHERE n.nspname='public' AND c.relname=:name AND a.attnum>0 "
                        "AND NOT a.attisdropped AND a.attname<>'source_run_id' ORDER BY a.attnum"
                    ),
                    {"name": name},
                ).mappings()
            ]
            primary = list(
                connection.execute(
                    text(
                        "SELECT a.attname FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid "
                        "CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY AS k(attnum,ordinality) "
                        "JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum "
                        "WHERE c.relnamespace='public'::regnamespace AND c.relname=:name "
                        "AND p.contype='p' ORDER BY k.ordinality"
                    ),
                    {"name": name},
                ).scalars()
            )
            inputs = [c["name"] for c in columns if not c["generated"]]
            table: dict[str, object] = {
                "name": name,
                "columns": columns,
                "primary_key": primary,
                "input_columns": inputs,
            }
            order = sql.SQL(",").join(
                sql.SQL('CAST({} AS text) COLLATE "C"').format(sql.Identifier(k)) for k in primary
            )
            with driver.cursor() as cursor:
                for kind, selected in (
                    ("input", inputs),
                    ("projection", [c["name"] for c in columns]),
                ):
                    filename = name + "." + kind + ".bin"
                    query = sql.SQL(
                        "COPY (SELECT {} FROM public.{} ORDER BY {}) TO STDOUT WITH (FORMAT binary)"
                    ).format(
                        sql.SQL(",").join(sql.Identifier(c) for c in selected),
                        sql.Identifier(name),
                        order,
                    )
                    with (root / filename).open("wb") as handle, cursor.copy(query) as reader:
                        while chunk := reader.read():
                            handle.write(chunk)
                    table[kind + "_file"] = filename
                    table[kind + "_sha256"] = hashlib.sha256(
                        (root / filename).read_bytes()
                    ).hexdigest()
                    table[kind + "_bytes"] = (root / filename).stat().st_size
                cursor.execute(
                    sql.SQL("SELECT count(*) FROM public.{}").format(sql.Identifier(name))
                )
                count_row = cursor.fetchone()
                assert count_row is not None
                table["row_count"] = count_row[0]
            tables.append(table)
    manifest = {
        "format": "aas-legacy-copy-snapshot/v1",
        "source_system_identifier": "123456789",
        "source_database": "synthetic_legacy",
        "source_alembic_head": "20260818_0004",
        "source_server_major": 18,
        "source_encoding": "UTF8",
        "order_contract": "pk-as-text-C-v1",
        "tables": tables,
    }
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    (root / "manifest.json").write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()
