"""Report how the KR identity registry resolves a daily-bar source's EODHD symbols.

Review evidence for ``aas identity kr-build``, never an input to it. It opens the market
DuckDB file read-only, reads the DART ``corp_codes`` receipt from ``--dart-source`` and
the provider symbols of every committed table named ``--table`` under
``--symbols-prefix``, reads KIND receipts and EODHD symbol-list jobs from their collected
files (the same bytes ``aas identity kr-import`` would commit, cited by the same content
source IDs), builds the registry document in memory and reports::

    uv run --no-sync python -m scripts.kr_identity_report --market MARKET.duckdb \\
        --dart-source opendart-native-... --kind-receipt KIND/response.json \\
        --eodhd-job JOB_DIR --symbols-prefix qveris-kr-history-62d23e53 --table bars

- the registry summary: issuers, instruments, assertions per provider and namespace,
  each mapper's row report and every unresolved key by reason;
- the resolution of the bar source's symbols: how many resolve to exactly one
  instrument, and every unresolved symbol by reason.

Nothing is written unless ``--output`` names a new file for the full JSON report.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

from aegis_alpha.storage.kr_identity import (
    DART_TABLE,
    SourceRows,
    build_kr_registry,
    read_eodhd_job,
    read_kind_receipt,
)
from aegis_alpha.storage.source_identity import LINK_PREFIX


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _tables(
    connection: duckdb.DuckDBPyConnection, prefix: str, table: str
) -> list[tuple[str, list[str]]]:
    found = []
    for source_id, manifest in connection.execute(
        "SELECT source_id, manifest_json FROM source_library_commits "
        "WHERE starts_with(source_id, ?) ORDER BY source_id",
        [prefix],
    ).fetchall():
        found.extend(
            (str(source_id), [str(item["target"]), *item["columns"]])
            for item in json.loads(manifest)["tables"]
            if item["name"] == table
        )
    if not found:
        raise SystemExit(f"no committed {table} table under source id prefix {prefix}")
    return found


def _dart(connection: duckdb.DuckDBPyConnection, source_id: str) -> SourceRows:
    ((_, (target, *columns)),) = [
        found for found in _tables(connection, source_id, DART_TABLE) if found[0] == source_id
    ]
    rows = connection.execute(
        "SELECT "  # noqa: S608 -- quoted names from the commit manifest
        + ",".join(_quote(column) for column in columns)
        + f" FROM {_quote(target)} ORDER BY _aas_ordinal"
    ).fetchall()
    return SourceRows(LINK_PREFIX + source_id, tuple(columns), tuple(rows))


def _symbols(connection: duckdb.DuckDBPyConnection, prefix: str, table: str) -> list[str]:
    symbols: set[str] = set()
    for _, (target, *_columns) in _tables(connection, prefix, table):
        symbols.update(
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT provider_symbol FROM {_quote(target)}"  # noqa: S608 -- quoted
            ).fetchall()
        )
    return sorted(symbols)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--market", type=Path, required=True)
    parser.add_argument("--dart-source", help="Source ID holding the DART corp_codes receipt")
    parser.add_argument("--kind-receipt", type=Path, action="append", default=[])
    parser.add_argument("--eodhd-job", type=Path, action="append", default=[], required=True)
    parser.add_argument("--symbols-prefix", required=True)
    parser.add_argument("--table", default="bars")
    parser.add_argument("--output", type=Path, help="New file for the full JSON report")
    args = parser.parse_args(argv)
    eodhd = [read_eodhd_job(path) for path in args.eodhd_job]
    kind = [read_kind_receipt(path) for path in args.kind_receipt]
    with duckdb.connect(str(args.market), read_only=True) as connection:
        dart = None if args.dart_source is None else _dart(connection, args.dart_source)
        symbols = _symbols(connection, args.symbols_prefix, args.table)
    registry = build_kr_registry(
        [unit.source_rows() for unit in eodhd], [unit.source_rows() for unit in kind], dart
    )
    full = {
        "sources": {
            "eodhd": [unit.content.source_id for unit in eodhd],
            "kind": [unit.content.source_id for unit in kind],
            "dart": args.dart_source,
        },
        "registry": registry.report(sample=None),
        "resolution": {"source_prefix": args.symbols_prefix, **registry.resolve(symbols)},
    }
    if args.output is not None:
        with args.output.open("x", encoding="utf-8") as handle:
            json.dump(full, handle, ensure_ascii=False, indent=2, sort_keys=True)
    summary = {
        "sources": full["sources"],
        "registry": registry.report(sample=20),
        "resolution": {
            "source_prefix": args.symbols_prefix,
            **registry.resolve(symbols, sample=20),
        },
    }
    json.dump(summary, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
