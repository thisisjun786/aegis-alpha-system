"""Report how the US identity registry resolves EODHD US bulk bars and quarantined rows.

Review evidence for ``aas identity us-build``, never an input to it. It opens the market
DuckDB file read-only (a store without the state file ``us-build`` needs), reads the
Norgate security master committed as ``--master``, builds the US registry from it in
memory and classifies the ``.US`` rows of every committed ``bars`` table under each
``--bars`` source ID prefix and every ``quarantine`` table under each ``--quarantine``
prefix (``scripts/us_identity_report.py`` is the wrapper)::

    uv run --no-sync python -m scripts.us_identity_report --market MARKET.duckdb \\
        --master market-raw-norgate-... --bars qveris-bulk- --quarantine qveris-bulk-

A quarantined row is the provider row in its ``source_row_json`` (``code``, ``date``,
``exchange_short_name``); ``--quarantine-reason`` keeps only rows quarantined for that
reason. Each key ``(symbol, session date)`` resolves as ``eodhd.bars@1`` would resolve it
against a snapshot of this registry: its symbol's claim must hold at the session's New
York start. The report gives the master's ``through`` date, the EODHD claim count and,
per table kind, the resolution of every row and of the distinct keys, with unresolved
rows and symbols by reason.

EODHD symbol claims rest only on the master, so the resolution equals a ``us-build``
registry's for the same master; the document itself is not reported, because its known
instants come from ``sl:`` links in the state file. Nothing is written unless
``--output`` names a new file for the full JSON report.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import duckdb

from aegis_alpha.storage.kr_identity import SourceRows
from aegis_alpha.storage.source_identity import LINK_PREFIX
from aegis_alpha.storage.us_identity import MASTER_TABLE, LinkedRows, UsRegistry, build_us_registry

type Keys = dict[tuple[str, date], int]


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _tables(
    connection: duckdb.DuckDBPyConnection, prefix: str, table: str
) -> list[tuple[str, str, list[str]]]:
    found = []
    for source_id, manifest in connection.execute(
        "SELECT source_id, manifest_json FROM source_library_commits "
        "WHERE starts_with(source_id, ?) ORDER BY source_id",
        [prefix],
    ).fetchall():
        found.extend(
            (str(source_id), str(item["target"]), [str(column) for column in item["columns"]])
            for item in json.loads(manifest)["tables"]
            if item["name"] == table
        )
    if not found:
        raise SystemExit(f"no committed {table} table under source id prefix {prefix}")
    return found


def read_master(connection: duckdb.DuckDBPyConnection, source_id: str) -> LinkedRows:
    matches = [
        found for found in _tables(connection, source_id, MASTER_TABLE) if found[0] == source_id
    ]
    if len(matches) != 1:
        raise SystemExit(f"source {source_id} has no {MASTER_TABLE} table")
    ((_, target, columns),) = matches
    rows = connection.execute(
        "SELECT "  # noqa: S608 -- quoted names from the commit manifest
        + ",".join(_quote(column) for column in columns)
        + f" FROM {_quote(target)} ORDER BY _aas_ordinal"
    ).fetchall()
    # The link instant only sets when claims are known, which resolution does not read.
    return LinkedRows(SourceRows(LINK_PREFIX + source_id, tuple(columns), tuple(rows)), 0)


def _bar_keys(connection: duckdb.DuckDBPyConnection, prefixes: list[str]) -> tuple[Keys, int]:
    keys: Keys = defaultdict(int)
    tables = 0
    for prefix in prefixes:
        for _, target, _columns in _tables(connection, prefix, "bars"):
            tables += 1
            for symbol, day, count in connection.execute(
                "SELECT provider_symbol, date, count(*) FROM "  # noqa: S608 -- quoted
                f"{_quote(target)} WHERE ends_with(provider_symbol, '.US') GROUP BY ALL"
            ).fetchall():
                keys[str(symbol), day] += int(count)
    return keys, tables


def _quarantine_keys(
    connection: duckdb.DuckDBPyConnection, prefixes: list[str], reason: str | None
) -> tuple[Keys, int]:
    keys: Keys = defaultdict(int)
    tables = 0
    field = "json_extract_string(source_row_json, '$.{}')".format
    for prefix in prefixes:
        for _, target, _columns in _tables(connection, prefix, "quarantine"):
            tables += 1
            for symbol, day, count in connection.execute(
                f"SELECT {field('code')} || '.US', CAST({field('date')} AS DATE), count(*) "  # noqa: S608
                f"FROM {_quote(target)} WHERE {field('exchange_short_name')} = 'US' "
                "AND (CAST(? AS VARCHAR) IS NULL OR reason = ?) GROUP BY ALL",
                [reason, reason],
            ).fetchall():
                keys[str(symbol), day] += int(count)
    return keys, tables


def _resolution(
    registry: UsRegistry, keys: Keys, tables: int, sample: int | None
) -> dict[str, object]:
    return {
        "tables": tables,
        "rows": registry.resolve(
            [(symbol, day, count) for (symbol, day), count in sorted(keys.items())], sample=sample
        ),
        "distinct_keys": registry.resolve(
            [(symbol, day, 1) for symbol, day in sorted(keys)], sample=sample
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--market", type=Path, required=True)
    parser.add_argument("--master", required=True, help="Norgate security master source ID")
    parser.add_argument("--bars", action="append", default=[], help="Bars source ID prefix")
    parser.add_argument(
        "--quarantine", action="append", default=[], help="Quarantine source ID prefix"
    )
    parser.add_argument("--quarantine-reason", help="Keep only rows quarantined for this reason")
    parser.add_argument("--output", type=Path, help="New file for the full JSON report")
    args = parser.parse_args(argv)
    with duckdb.connect(str(args.market), read_only=True) as connection:
        registry = build_us_registry(read_master(connection, args.master))
        kinds = {
            "bars": _bar_keys(connection, args.bars) if args.bars else None,
            "quarantine": _quarantine_keys(connection, args.quarantine, args.quarantine_reason)
            if args.quarantine
            else None,
        }

    def report(sample: int | None) -> dict[str, object]:
        through = registry.through
        return {
            "master": args.master,
            "through": None if through is None else through.isoformat(),
            "eodhd_symbols": len(registry.intervals),
            "unresolved_tickers": {
                reason: len(found)
                for reason, found in sorted(registry.unresolved.get("tickers", {}).items())
            },
            "quarantine_reason": args.quarantine_reason,
            **{
                kind: _resolution(registry, *found, sample)
                for kind, found in kinds.items()
                if found is not None
            },
        }

    if args.output is not None:
        with args.output.open("x", encoding="utf-8") as handle:
            json.dump(report(None), handle, ensure_ascii=False, indent=2, sort_keys=True)
    json.dump(report(20), sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
