"""``aas data kr-prices``: KR daily bars from the source library to ``prices.kr.eodhd``.

The backfill is a sequence of promotions, each the next generation of one dataset:

1. the EODHD KR daily-bar history (every ``bars`` table of one source lineage), one
   generation per calendar year, with ``eodhd.bars@1``;
2. the rows the same history downloads held back as invalid (every nonempty
   ``quarantine`` table of that lineage), one generation over their years, with
   ``eodhd.bars_quarantine@1``: each becomes an ``invalid`` bar that keeps no values;
3. the exchange-wide daily downloads the provider answered with a partial-response
   warning (every ``quarantine`` table of one bulk lineage whose rows name an exchange
   of ``CURRENCIES``), one generation per session date, with
   ``eodhd.bulk_quarantine@1``. Each row is promoted with the flag
   ``provider_reported_partial`` and each generation records ``partition_row_count@1``
   against the history it extends.

``--reference`` builds ``prices.kr.eodhd.ref`` from the history and partial tables with
the provider's adjusted close (``eodhd.bars_adjusted@1``,
``eodhd.bulk_quarantine_adjusted@1``); held invalid rows have no adjusted close to keep.

Every step's spec is the canonical document this module writes for the step's partition,
its pinned tables, the pinned identity snapshot, the committed head of ``sessions.xkrx``
for ``session_close_plus_lag@1`` and the dataset's head as parent. Prices take
``krw_tick@1``; volume and an adjusted close take ``float_shortest@1``, because the
provider divides volumes by split factors too (545540.77978275 shares), which no exact
12-decimal value holds; such a row is flagged ``provider_float_storage``.

Tables are ordered by their source's ``sl:`` link time, then source ID. Downloads
repeated with identical content (the same table digest) are pinned once, by the earliest
link, whose time becomes the rows' ingestion. Different tables carrying the same exchange
and date become successive generations of that date in link order: generation ``k`` pins
each exchange's ``k``-th download (or its last, when it has fewer), so a later download
supersedes an earlier one with its own ingestion time and flag. A held lineage whose
nonempty tables have no row with a mapped reason and date plans no held step and reports
those rows as ``held_unmapped_rows``; a history lineage whose nonempty tables have no
dated row is refused. A bulk table whose rows name no exchange at all (malformed JSON or
no ``exchange_short_name``) is not pinned and is listed in ``bulk_unclassified_tables``.

``--plan`` writes nothing and plans every step as the next child of the current head;
a step after an unapplied one is therefore planned against a head that lacks it. An
apply runs the steps in order, each as the child of the head the previous one left, and
stops at the first refusal. A step whose delta is empty publishes nothing, so repeating
the command after a complete run writes nothing.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.promotion.mappers import mapper
from aegis_alpha.storage.source_library import list_sources, list_tables

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

DATASET: Final = "prices.kr.eodhd"
REFERENCE_DATASET: Final = "prices.kr.eodhd.ref"
CALENDAR: Final = ("sessions.xkrx", "XKRX", "XKRX")
TIMEZONE: Final = "Asia/Seoul"
CURRENCIES: Final = {"KO": "KRW", "KQ": "KRW"}
_HISTORY_TABLE: Final = "bars"
_LINK: Final = "sl:"
_BULK_TABLE: Final = "quarantine"
_SUMMARY: Final = (
    "generation_id",
    "parent",
    "published",
    "empty_delta",
    "reused",
    "source_rows",
    "rows",
    "duplicate_keys",
    "unresolved_token_count",
    "unresolved_tokens",
    "operations",
    "unchanged",
    "stale",
    "time_drift",
    "mapped_flags",
    "flags",
    "partition_row_count",
    "blocking",
    "refusals",
)


@dataclass(frozen=True, slots=True)
class Step:
    """One generation of the backfill: a partition, its mapper and the tables it pins."""

    kind: str
    start: date
    end: date
    mapper: str
    sources: tuple[dict[str, str], ...]


def _pin(source: Mapping[str, object], table: Mapping[str, object]) -> dict[str, str]:
    return {
        "source_id": str(source["source_id"]),
        "source_sha256": str(source["sha256"]),
        "table": str(table["name"]),
        "digest": str(table["digest"]),
    }


def _linked_at(workspace: Workspace, source_id: str) -> int | None:
    row = workspace.state.execute(
        "SELECT retrieved_at_us FROM source_snapshots WHERE snapshot_id=?", (_LINK + source_id,)
    ).fetchone()
    return None if row is None else int(row[0])


def _tables(workspace: Workspace, prefix: str, name: str) -> list[tuple[dict[str, str], str, int]]:
    """(pin, market table, rows) of each committed table ``name`` of a lineage.

    The order is the source's ``sl:`` link time (unlinked sources last), then source ID.
    """
    found = [
        (_pin(source, table), str(table["target"]), int(str(table["rows"])))
        for source in list_sources(workspace)
        if source["store"] == "market" and str(source["source_id"]).startswith(prefix + "-")
        for table in list_tables(workspace, str(source["source_id"]))
        if table["name"] == name and table["format"] == "arrow"
    ]
    linked = {item[0]["source_id"]: _linked_at(workspace, item[0]["source_id"]) for item in found}

    def order(item: tuple[dict[str, str], str, int]) -> tuple[bool, int, str]:
        source_id = item[0]["source_id"]
        at = linked[source_id]
        return (at is None, 0 if at is None else at, source_id)

    return sorted(found, key=order)


def _history_steps(workspace: Workspace, lineage: str, *, reference: bool) -> list[Step]:
    tables = _tables(workspace, lineage, _HISTORY_TABLE)
    if not tables:
        raise ValueError(f"no committed {_HISTORY_TABLE} tables in lineage {lineage}")
    day = mapper("eodhd.bars@1").partition_sql
    union = " UNION ALL ".join(
        f"SELECT min({day}), max({day}) FROM {formats.quote_identifier(target)}"  # noqa: S608
        for _, target, _ in tables
    )
    first, last = cast(
        "tuple[date | None, date | None]",
        workspace.market.execute(f"SELECT min(a), max(b) FROM ({union}) t(a, b)").fetchone(),  # noqa: S608
    )
    if first is None or last is None:
        rows = sum(count for _, _, count in tables)
        if rows:
            raise ValueError(f"{rows} {_HISTORY_TABLE} rows of lineage {lineage} have no date")
        return []
    name = "eodhd.bars_adjusted@1" if reference else "eodhd.bars@1"
    pins = tuple(pin for pin, _, _ in tables)
    return [
        Step("history", date(year, 1, 1), date(year + 1, 1, 1), name, pins)
        for year in range(first.year, last.year + 1)
    ]


def _distinct(
    tables: list[tuple[dict[str, str], str, int]], report: dict[str, object], kind: str
) -> list[tuple[dict[str, str], str, int]]:
    """Nonempty tables, a repeated download (the same digest) kept once by earliest link."""
    kept: dict[str, tuple[dict[str, str], str, int]] = {}
    for item in tables:
        if item[2]:
            kept.setdefault(item[0]["digest"], item)
    report[f"{kind}_tables"] = len(kept)
    report[f"{kind}_repeated_tables"] = sum(1 for item in tables if item[2]) - len(kept)
    return list(kept.values())


def _held_steps(workspace: Workspace, lineage: str, report: dict[str, object]) -> list[Step]:
    """One step over the years of the history rows held back as invalid, if there are any."""
    tables = _distinct(_tables(workspace, lineage, _BULK_TABLE), report, "held")
    if not tables:
        return []
    day = mapper("eodhd.bars_quarantine@1").partition_sql
    union = " UNION ALL ".join(
        f"SELECT min({day}), max({day}) FROM {formats.quote_identifier(target)}"  # noqa: S608
        for _, target, _ in tables
    )
    first, last = cast(
        "tuple[date | None, date | None]",
        workspace.market.execute(f"SELECT min(a), max(b) FROM ({union}) t(a, b)").fetchone(),  # noqa: S608
    )
    if first is None or last is None:
        report["held_unmapped_rows"] = sum(rows for _, _, rows in tables)
        return []
    pins = tuple(sorted((pin for pin, _, _ in tables), key=lambda pin: pin["source_id"]))
    return [
        Step(
            "held",
            date(first.year, 1, 1),
            date(last.year + 1, 1, 1),
            "eodhd.bars_quarantine@1",
            pins,
        )
    ]


def _bulk_steps(
    workspace: Workspace, lineage: str, report: dict[str, object], *, reference: bool
) -> list[Step]:
    """One step per session date over the distinct-content partial downloads of KR exchanges."""
    day = mapper("eodhd.bulk_quarantine@1").partition_sql
    exchange = (
        "CASE WHEN json_valid(source_row_json) THEN "
        "json_extract_string(source_row_json, '$.exchange_short_name') END"
    )
    kept: dict[str, tuple[dict[str, str], frozenset[str], list[date]]] = {}
    repeated: list[str] = []
    unclassified: list[dict[str, object]] = []
    for pin, target, rows in _tables(workspace, lineage, _BULK_TABLE):
        if not rows:
            continue
        found = workspace.market.execute(
            f"SELECT DISTINCT {day}, {exchange} FROM {formats.quote_identifier(target)}"  # noqa: S608
        ).fetchall()
        exchanges = {str(row[1]) for row in found if row[1] is not None}
        if not exchanges:
            unclassified.append({"source_id": pin["source_id"], "rows": rows})
            continue
        if not exchanges & set(CURRENCIES):
            continue
        if not exchanges <= set(CURRENCIES):
            raise ValueError(
                f"bulk table {pin['source_id']} mixes KR and other exchanges: {sorted(exchanges)}"
            )
        if pin["digest"] in kept:
            repeated.append(pin["source_id"])
            continue
        kept[pin["digest"]] = (
            pin,
            frozenset(exchanges),
            sorted({cast("date", row[0]) for row in found if row[0]}),
        )
    report["bulk_tables"] = len(kept)
    report["bulk_repeated_tables"] = len(repeated)
    report["bulk_unclassified_tables"] = unclassified
    # Insertion order is link order, so each exchange's downloads of a day are in it too.
    by_day: dict[date, dict[frozenset[str], list[dict[str, str]]]] = {}
    for pin, covered, days in kept.values():
        for session in days:
            by_day.setdefault(session, {}).setdefault(covered, []).append(pin)
    name = "eodhd.bulk_quarantine_adjusted@1" if reference else "eodhd.bulk_quarantine@1"
    steps = []
    for session, groups in sorted(by_day.items()):
        for k in range(max(len(pins) for pins in groups.values())):
            chosen = [pins[min(k, len(pins) - 1)] for pins in groups.values()]
            steps.append(
                Step(
                    "bulk",
                    session,
                    session + timedelta(days=1),
                    name,
                    tuple(sorted(chosen, key=lambda pin: pin["source_id"])),
                )
            )
    report["bulk_superseding_steps"] = len(steps) - len(by_day)
    return steps


def _calendar_pin(workspace: Workspace) -> dict[str, str]:
    dataset = CALENDAR[0]
    row = workspace.state.execute(
        "SELECT version, generation_id, chain_hash, manifest_hash FROM dataset_versions "
        "WHERE dataset_id=? AND status='committed' ORDER BY sequence DESC LIMIT 1",
        (dataset,),
    ).fetchone()
    if row is None:
        raise ValueError(f"no committed {dataset} generation; run aas calendar refresh")
    return {
        "dataset_id": dataset,
        "version": str(row[0]),
        "generation_id": str(row[1]),
        "chain_hash": str(row[2]),
        "manifest_hash": str(row[3]),
    }


def _identity_pin(workspace: Workspace, snapshot_id: str) -> dict[str, str]:
    row = workspace.state.execute(
        "SELECT content_hash FROM identity_snapshots WHERE snapshot_id=?", (snapshot_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"identity snapshot {snapshot_id} is not registered")
    return {"snapshot_id": snapshot_id, "content_hash": str(row[0])}


def step_spec(  # noqa: PLR0913 -- every input the canonical spec spells
    step: Step,
    *,
    dataset: str,
    parent: str | None,
    identity: Mapping[str, str],
    calendar: Mapping[str, str],
    lag_us: int,
) -> bytes:
    """The canonical ``aas-promotion-v1`` spec of one step."""
    rule = {
        "rule": "session_close_plus_lag@1",
        "basis": "record",
        "input": "session_date",
        "args": {
            "calendar": dict(calendar),
            "calendar_id": CALENDAR[1],
            "venue": CALENDAR[2],
            "lag_us": lag_us,
        },
    }
    adjusted = step.mapper.split("@", 1)[0].endswith("_adjusted")
    decimals = (
        {"close": "float_shortest@1"}
        if adjusted
        else {
            **dict.fromkeys(("open", "high", "low", "close"), "krw_tick@1"),
            "volume": "float_shortest@1",
        }
    )
    args: dict[str, object] = {"timezone": TIMEZONE}
    if step.kind in {"held", "bulk"}:
        args["currencies"] = dict(CURRENCIES)
    return formats.canonical(
        {
            "schema_version": "aas-promotion-v1",
            "target": {"domain": "prices", "dataset_id": dataset, "parent": parent},
            "sources": [dict(pin) for pin in step.sources],
            "mapper": {"name": step.mapper, "args": args},
            "partition": {"from": step.start.isoformat(), "to": step.end.isoformat()},
            "time_rules": {"available_at_us": rule, "revision_known_at_us": rule},
            "decimal_rule": decimals,
            "quality_rules": [],
            "tombstone_policy": {"mode": "never"},
            "identity_snapshot": dict(identity),
        }
    )


def kr_price_steps(
    workspace: Workspace,
    *,
    history_lineage: str,
    bulk_lineage: str | None,
    reference: bool,
    report: dict[str, object],
) -> list[Step]:
    """The backfill's steps in order: history years, held history rows, partial bulk dates."""
    steps = _history_steps(workspace, history_lineage, reference=reference)
    if not reference:
        steps += _held_steps(workspace, history_lineage, report)
    if bulk_lineage is not None:
        steps += _bulk_steps(workspace, bulk_lineage, report, reference=reference)
    return steps


def _totals(results: Sequence[Mapping[str, object]]) -> dict[str, object]:
    totals: dict[str, dict[str, int]] = {"rows": {}, "operations": {}, "flags": {}}
    mapped: dict[str, int] = {}
    source_rows = 0
    for result in results:
        source_rows += int(cast("int", result.get("source_rows") or 0))
        for key, total in totals.items():
            for name, count in cast("dict[str, int]", result.get(key) or {}).items():
                total[name] = total.get(name, 0) + int(count)
        flags = cast("dict[str, dict[str, int]]", result.get("mapped_flags") or {})
        for name, count in flags.get("rows", {}).items():
            mapped[name] = mapped.get(name, 0) + int(count)
    return {"source_rows": source_rows, **totals, "mapped_flags": mapped}


def kr_prices(  # noqa: PLR0913 -- the run's explicit inputs
    workspace: Workspace,
    *,
    identity_snapshot: str,
    lag_us: int,
    history_lineage: str,
    bulk_lineage: str | None,
    reference: bool,
    apply: bool,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Plan, or apply in order, every step of the KR price backfill."""
    if isinstance(lag_us, bool) or not isinstance(lag_us, int) or lag_us < 0:
        raise ValueError("lag_us must be a nonnegative integer")
    dataset = REFERENCE_DATASET if reference else DATASET
    identity = _identity_pin(workspace, identity_snapshot)
    calendar = _calendar_pin(workspace)
    report: dict[str, object] = {
        "mode": "apply" if apply else "plan",
        "dataset_id": dataset,
        "identity_snapshot": identity,
        "calendar": calendar,
    }
    steps = kr_price_steps(
        workspace,
        history_lineage=history_lineage,
        bulk_lineage=bulk_lineage,
        reference=reference,
        report=report,
    )
    results = []
    for step in steps:
        parent = dataset_head(workspace, dataset)
        raw = step_spec(
            step,
            dataset=dataset,
            parent=parent,
            identity=identity,
            calendar=calendar,
            lag_us=lag_us,
        )
        sha256 = hashlib.sha256(raw).hexdigest()
        try:
            result = promote(workspace, raw, sha256, apply=apply, budget=budget)
        except ValueError as error:
            raise ValueError(
                f"{step.kind} step {step.start.isoformat()}..{step.end.isoformat()}: {error}"
            ) from error
        results.append(
            {
                "kind": step.kind,
                "from": step.start.isoformat(),
                "to": step.end.isoformat(),
                "mapper": step.mapper,
                "sources": len(step.sources),
                "spec_sha256": sha256,
                **{key: result[key] for key in _SUMMARY if key in result},
            }
        )
    report["steps"] = results
    report["totals"] = _totals(results)
    report["head"] = dataset_head(workspace, dataset)
    return json.loads(formats.canonical(report))
