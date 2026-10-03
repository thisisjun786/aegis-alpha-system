"""Maintenance promotion: each dataset chain continued with the sources collected since.

A route names one dataset of the catalog, the registered mapper that builds it from one
collected source shape (a source ID prefix and table), and, for a shape that several
datasets share, the predicate every row of a routed table satisfies (an exchange's
symbols, partial-response rows of a KR exchange). A table whose rows do not all satisfy
it is left to the other routes of the shape; an empty table is skipped.

A route continues a chain; it never starts one. Its template is the spec of the latest
committed generation of the dataset that used the route's mapper: an operator's
backfill or an earlier maintenance generation. A maintenance spec is that spec with
only these fields advanced:

- ``target.parent``: the dataset head;
- ``sources``: the one new source table;
- ``partition``: null when the template's is null; the source's vintage partitions in
  order (``fred.alfred@1``, one generation each); otherwise ``[first, last + 1 day)`` of the
  mapper's partition dates in the table;
- every generation pin in the mapper, time-rule and quality-rule arguments: the committed
  head of the pinned dataset now (the calendar a refresh advanced, the SEC filings a
  promotion before this one published);
- ``identity_snapshot``: the maintenance identity snapshot when the run has one and the
  template pins a snapshot;
- ``tombstone_policy``: ``never``. One collected source is no full snapshot, so a
  maintenance generation never removes a record.

The mapper and its arguments, the time, numeric and quality rules stay those of the
template, so a chain's rules never change under maintenance. Dataset generations of a
later time-rule generation (``<dataset>.r<N>``) are continued the same way, each from its
own template.

New sources are taken in order of their ``sl:`` link time, then source ID. A source is
done for a dataset once all its steps were promoted or found unchanged: maintenance then
records a ``maintain_source@1`` quality check on the dataset's head version naming the
pin and each step's request and generation (``promoted`` or ``unchanged``). A refused or
blocked step records nothing, so the source is planned again on the next run; a run that
stopped between the steps of one source plans its done steps again as empty deltas.
Sources an operator's promotion pinned are planned once more on the first maintenance
run and recorded as unchanged (an older download than the head is ``stale`` and never
replaces it). A source whose outcome-declaring mapper recorded ``promotion_coverage@1`` is
done as well.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import dataset_head, promote
from aegis_alpha.storage.promotion.mappers import mapper
from aegis_alpha.storage.promotion.mappers.fred import vintage_partitions
from aegis_alpha.storage.provider_collection import committed_tables
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_library import list_sources
from aegis_alpha.storage.state import atomic

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

CHECK_RULE: Final = ("maintain_source", "1")
_LINK: Final = "sl:"
_PIN_KEYS: Final = frozenset({"dataset_id", "version", "generation_id", "chain_hash",
                              "manifest_hash"})  # fmt: skip
_EXCHANGE: Final = (
    "CASE WHEN json_valid(source_row_json) THEN "
    "json_extract_string(source_row_json, '$.exchange_short_name') END"
)
_US_SYMBOL: Final = "provider_symbol LIKE '%.US'"
_KR_SYMBOL: Final = "(provider_symbol LIKE '%.KO' OR provider_symbol LIKE '%.KQ')"
_KR_PARTIAL: Final = f"reason = 'provider_reported_partial' AND {_EXCHANGE} IN ('KO', 'KQ')"
_SUMMARY: Final = (
    "generation_id",
    "published",
    "empty_delta",
    "reused",
    "source_rows",
    "rows",
    "operations",
    "unchanged",
    "stale",
    "time_drift",
    "flags",
    "unresolved_token_count",
    "partition_row_count",
    "coverage_check",
    "blocking",
    "refusals",
)


@dataclass(frozen=True, slots=True)
class Route:
    """One dataset built by one mapper from one collected source shape."""

    dataset_id: str
    mapper: str
    prefix: str
    table: str
    accept: str | None = None


# In dependency order: SEC facts take each filing's acceptance from the filings head.
ROUTES: Final = (
    Route("filings.us.sec", "sec.submissions@1", "sec-submissions-filings-", "filings"),
    Route("fundamentals.us.sec", "sec.companyfacts@1", "sec-companyfacts-facts-", "facts"),
    Route("filings.kr.dart", "dart.fnltt_filings@1", "opendart-receipts-", "receipts"),
    Route("fundamentals.kr.dart", "dart.fnltt@1", "opendart-receipts-", "receipts"),
    Route("macro.us.alfred", "fred.alfred@1", "fred-alfred-observations-", "observations"),
    Route("fx.usdkrw.fred", "fred.fx_series@1", "fred-series-csv-", "observations"),
    Route("classifications.kr.kind", "kind.industry@1", "kind-listings-", "listings"),
    Route("prices.us.eodhd", "eodhd.bars@1", "qveris-bulk-bars-", "bars", _US_SYMBOL),
    Route("prices.kr.eodhd", "eodhd.bars@1", "qveris-bulk-bars-", "bars", _KR_SYMBOL),
    Route("prices.kr.eodhd", "eodhd.bulk_quarantine@1", "qveris-bulk-quarantine-",
          "quarantine", _KR_PARTIAL),
    Route("prices.kr.eodhd.ref", "eodhd.bars_adjusted@1", "qveris-bulk-bars-", "bars",
          _KR_SYMBOL),
    Route("prices.kr.eodhd.ref", "eodhd.bulk_quarantine_adjusted@1", "qveris-bulk-quarantine-",
          "quarantine", _KR_PARTIAL),
)  # fmt: skip

# Collected shapes whose dataset has no registered mapper yet: counted, never promoted.
UNMAPPED: Final = (
    ("actions.us.eodhd", "qveris-splits-", "splits", "provider_symbol LIKE '%.US'"),
    ("actions.us.eodhd", "qveris-dividends-", "dividends", "provider_symbol LIKE '%.US'"),
    ("actions.kr.eodhd", "qveris-splits-", "splits", _KR_SYMBOL),
    ("actions.kr.eodhd", "qveris-dividends-", "dividends", _KR_SYMBOL),
    ("status.kr.kind", "kind-listings-", "listings", None),
    ("prices.us.eodhd", "qveris-bulk-quarantine-", "quarantine",
     f"{_EXCHANGE} = 'US'"),
)  # fmt: skip


@dataclass(frozen=True, slots=True)
class Candidate:
    """A committed source table a route may promote."""

    pin: dict[str, str]
    target: str
    rows: int
    linked_at_us: int | None


@dataclass(slots=True)
class Step:
    partition: tuple[date, date] | None


@dataclass(slots=True)
class DatasetRun:
    dataset_id: str
    mapper: str
    status: str = "current"
    head: str | None = None
    template: str | None = None
    sources: list[dict[str, object]] = field(default_factory=list)
    skipped: dict[str, int] = field(default_factory=dict)

    def report(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "mapper": self.mapper,
            "status": self.status,
            "head": self.head,
            "template_spec_sha256": self.template,
            "sources": self.sources,
            "skipped": self.skipped,
        }


def _quote(name: str) -> str:
    return formats.quote_identifier(name)


def _datasets(workspace: Workspace, base: str) -> list[str]:
    """``base`` and its committed later time-rule generations (``base.r<N>``)."""
    pattern = re.compile(re.escape(base) + r"\.r(?:[2-9]|[1-9][0-9]+)")
    later = sorted(
        (
            str(row[0])
            for row in workspace.state.execute(
                "SELECT DISTINCT dataset_id FROM dataset_versions WHERE status='committed'"
            )
            if pattern.fullmatch(str(row[0]))
        ),
        key=lambda name: int(name.rsplit(".r", 1)[1]),
    )
    return [base, *later]


def _linked(workspace: Workspace) -> dict[str, int]:
    return {
        str(snapshot)[len(_LINK) :]: int(at)
        for snapshot, at in workspace.state.execute(
            "SELECT snapshot_id, retrieved_at_us FROM source_snapshots "
            "WHERE snapshot_id LIKE 'sl:%'"
        )
    }


def candidates(  # noqa: PLR0913 -- the route and the pass's lookups
    workspace: Workspace,
    route: Route,
    linked: Mapping[str, int],
    shas: Mapping[str, str],
    skipped: dict[str, int],
    *,
    done: set[tuple[str, str]] | None = None,
) -> list[Candidate]:
    """The route's committed, nonempty tables not yet done whose every row it accepts.

    They are in ``sl:`` link order, then source ID order.
    """
    found: list[Candidate] = []
    for table in committed_tables(workspace, route.prefix):
        entry = table.entry
        if table.store != "market" or entry["name"] != route.table or entry["format"] != "arrow":
            continue
        if done is not None and (table.source_id, route.table) in done:
            skipped["done"] = skipped.get("done", 0) + 1
            continue
        rows = int(cast("int", entry["rows"]))
        if not rows:
            skipped["empty"] = skipped.get("empty", 0) + 1
            continue
        target = str(entry["target"])
        if route.accept is not None:
            accepted = workspace.market.execute(
                f"SELECT count(*) FILTER (WHERE coalesce(({route.accept}), false)) "  # noqa: S608 -- fixed route predicates over a quoted table
                f"FROM {_quote(target)}"
            ).fetchone()
            matched = 0 if accepted is None else int(accepted[0])
            if matched == 0:
                continue
            if matched != rows:
                skipped["mixed"] = skipped.get("mixed", 0) + 1
                continue
        pin = {
            "source_id": table.source_id,
            "source_sha256": shas[table.source_id],
            "table": route.table,
            "digest": str(entry["digest"]),
        }
        found.append(Candidate(pin, target, rows, linked.get(table.source_id)))
    return sorted(
        found,
        key=lambda item: (
            item.linked_at_us is None,
            item.linked_at_us or 0,
            item.pin["source_id"],
        ),
    )


def _done(workspace: Workspace, dataset_id: str) -> set[tuple[str, str]]:
    """(source ID, table) pins a maintenance record or a coverage check already settles."""
    done: set[tuple[str, str]] = set()
    for rule, reason in workspace.state.execute(
        "SELECT rule_id, reason FROM quality_checks WHERE dataset_id=? "
        "AND rule_id IN ('maintain_source', 'promotion_coverage')",
        (dataset_id,),
    ):
        body = json.loads(str(reason))
        pins = [body["source"]] if rule == CHECK_RULE[0] else body.get("sources", [])
        done.update((str(pin["source_id"]), str(pin["table"])) for pin in pins)
    return done


def _template(workspace: Workspace, dataset_id: str, mapper_name: str) -> tuple[str, dict] | None:
    row = workspace.state.execute(
        "SELECT transform_hash FROM dataset_versions WHERE dataset_id=? AND normalizer_version=? "
        "AND status='committed' ORDER BY sequence DESC LIMIT 1",
        (dataset_id, mapper_name),
    ).fetchone()
    if row is None:
        return None
    digest = str(row[0])
    from aegis_alpha.data.descriptor_tree import DescriptorTree  # noqa: PLC0415

    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        raw = tree.read_bytes(f"{digest[:2]}/{digest}", max_bytes=64 * 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError(f"the retained spec of {dataset_id} differs from its transform hash")
    return digest, cast("dict", json.loads(raw))


def head_pin(workspace: Workspace, dataset_id: str) -> dict[str, str] | None:
    row = workspace.state.execute(
        "SELECT version, generation_id, chain_hash, manifest_hash FROM dataset_versions "
        "WHERE dataset_id=? AND status='committed' ORDER BY sequence DESC LIMIT 1",
        (dataset_id,),
    ).fetchone()
    if row is None:
        return None
    names = ("version", "generation_id", "chain_hash", "manifest_hash")
    return {"dataset_id": dataset_id, **dict(zip(names, map(str, row), strict=True))}


def _refreshed(workspace: Workspace, value: object) -> object:
    """``value`` with every generation pin advanced to its dataset's committed head."""
    if isinstance(value, dict):
        if set(value) == _PIN_KEYS:
            return head_pin(workspace, str(value["dataset_id"])) or value
        return {key: _refreshed(workspace, item) for key, item in value.items()}
    if isinstance(value, list):
        return [_refreshed(workspace, item) for item in value]
    return value


def _steps(
    workspace: Workspace, template: Mapping[str, object], mapper_name: str, item: Candidate
) -> list[Step]:
    if template.get("partition") is None:
        return [Step(None)]
    if mapper_name == "fred.alfred@1":
        return [
            Step(bounds) for bounds in vintage_partitions(workspace.market, _quote(item.target))
        ]
    day = mapper(mapper_name).partition_sql
    bounds = workspace.market.execute(
        f"SELECT min(TRY_CAST(({day}) AS DATE)), max(TRY_CAST(({day}) AS DATE)) "  # noqa: S608 -- registered mapper SQL over a quoted table
        f"FROM {_quote(item.target)}"
    ).fetchone()
    if bounds is None or bounds[0] is None:
        return [Step(None)]
    return [Step((bounds[0], bounds[1] + timedelta(days=1)))]


def maintenance_spec(  # noqa: PLR0913 -- the template and every field maintenance advances
    workspace: Workspace,
    template: Mapping[str, object],
    *,
    parent: str | None,
    pin: Mapping[str, str],
    step: Step,
    identity: Mapping[str, str] | None,
) -> bytes:
    """The template's canonical spec with only the maintenance fields advanced."""
    spec = cast("dict[str, object]", json.loads(json.dumps(template)))
    target = cast("dict[str, object]", spec["target"])
    target["parent"] = parent
    spec["sources"] = [dict(pin)]
    spec["partition"] = (
        None
        if step.partition is None
        else {"from": step.partition[0].isoformat(), "to": step.partition[1].isoformat()}
    )
    for key in ("mapper", "time_rules", "quality_rules"):
        spec[key] = _refreshed(workspace, spec[key])
    if identity is not None and spec.get("identity_snapshot") is not None:
        spec["identity_snapshot"] = dict(identity)
    spec["tombstone_policy"] = {"mode": "never"}
    return formats.canonical(spec)


def _record(
    workspace: Workspace,
    dataset_id: str,
    pin: Mapping[str, str],
    steps: Sequence[Mapping[str, object]],
    *,
    now_us: int,
) -> str | None:
    version = workspace.state.execute(
        "SELECT version FROM dataset_versions WHERE dataset_id=? AND status='committed' "
        "ORDER BY sequence DESC LIMIT 1",
        (dataset_id,),
    ).fetchone()
    if version is None:
        return None
    published = any(step.get("published") for step in steps)
    reason = {
        "source": dict(pin),
        "steps": [
            {
                key: step.get(key)
                for key in ("partition", "spec_sha256", "generation_id", "published")
            }
            for step in steps
        ],
    }
    check_id = (
        "qc-"
        + hashlib.sha256(
            f"maintain/{dataset_id}/{pin['source_id']}/{pin['table']}".encode()
        ).hexdigest()
    )
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO quality_checks(check_id, dataset_id, version, rule_id, rule_version, "
            "result, reason, checked_at_us) VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(check_id) DO NOTHING",
            (
                check_id,
                dataset_id,
                str(version[0]),
                *CHECK_RULE,
                "promoted" if published else "unchanged",
                formats.canonical(reason).decode(),
                now_us,
            ),
        )
    return check_id


def _promote_source(  # noqa: PLR0913 -- one source's steps and the run's inputs
    workspace: Workspace,
    run: DatasetRun,
    template: Mapping[str, object],
    item: Candidate,
    *,
    apply: bool,
    identity: Mapping[str, str] | None,
    budget: ComputeBudget | None,
    now_us: int,
    evaluate: bool = True,
) -> dict[str, object]:
    results: list[dict[str, object]] = []
    outcome: dict[str, object] = {"source_id": item.pin["source_id"], "rows": item.rows}
    for step in _steps(workspace, template, run.mapper, item):
        parent = dataset_head(workspace, run.dataset_id)
        raw = maintenance_spec(
            workspace, template, parent=parent, pin=item.pin, step=step, identity=identity
        )
        sha256 = hashlib.sha256(raw).hexdigest()
        partition = (
            None
            if step.partition is None
            else {"from": step.partition[0].isoformat(), "to": step.partition[1].isoformat()}
        )
        summary: dict[str, object] = {"partition": partition, "spec_sha256": sha256}
        if not evaluate:
            results.append(summary)
            continue
        try:
            result = promote(workspace, raw, sha256, apply=apply, budget=budget)
        except ValueError as error:
            if apply:
                put_raw(workspace.paths.raw, raw)
            results.append({**summary, "error": str(error)})
            return {**outcome, "status": "refused", "steps": results}
        summary.update({key: result[key] for key in _SUMMARY if key in result})
        results.append(summary)
        if result.get("blocking") or result.get("refusals"):
            return {**outcome, "status": "refused", "steps": results}
    published = any(step.get("published") for step in results)
    status = "promoted" if published else "unchanged" if apply else "planned"
    if apply:
        outcome["check_id"] = _record(workspace, run.dataset_id, item.pin, results, now_us=now_us)
    return {**outcome, "status": status, "steps": results}


def promote_datasets(  # noqa: PLR0913 -- the pass's explicit inputs
    workspace: Workspace,
    *,
    apply: bool,
    identity: Mapping[str, str] | None = None,
    budget: ComputeBudget | None = None,
    now_us: int,
    routes: Sequence[Route] = ROUTES,
    evaluate: bool = True,
) -> dict[str, object]:
    """Plan, or apply in order, every route's new sources as the next generations.

    A plan with ``evaluate`` false lists each new source's steps and specs without
    planning their promotions.
    """
    if apply and not evaluate:
        raise ValueError("an applied maintenance promotion evaluates every step")
    linked = _linked(workspace)
    shas = {str(row["source_id"]): str(row["sha256"]) for row in list_sources(workspace)}
    runs: list[DatasetRun] = []
    for route in routes:
        for dataset_id in _datasets(workspace, route.dataset_id):
            run = DatasetRun(dataset_id, route.mapper)
            runs.append(run)
            done = _done(workspace, dataset_id)
            run.skipped["done"] = 0
            pending = candidates(workspace, route, linked, shas, run.skipped, done=done)
            run.head = dataset_head(workspace, dataset_id)
            if run.head is None:
                run.status = "no_head" if pending else "current"
                run.skipped["no_head"] = len(pending)
                continue
            found = _template(workspace, dataset_id, route.mapper)
            if found is None:
                run.status = "no_template" if pending else "current"
                run.skipped["no_template"] = len(pending)
                continue
            run.template, template = found
            for item in pending:
                result = _promote_source(
                    workspace,
                    run,
                    template,
                    item,
                    apply=apply,
                    identity=identity,
                    budget=budget,
                    now_us=now_us,
                    evaluate=evaluate,
                )
                run.sources.append(result)
                if result["status"] == "refused":
                    run.status = "refused"
            if run.status != "refused" and run.sources:
                run.status = "advanced" if apply else "planned"
            run.head = dataset_head(workspace, dataset_id)
    refused = [run.dataset_id for run in runs if run.status == "refused"]
    return {
        "mode": "apply" if apply else "plan",
        "datasets": [run.report() for run in runs],
        "refused": refused,
        "unmapped": unmapped(workspace),
        "provider_calls": 0,
    }


def unmapped(workspace: Workspace) -> dict[str, int]:
    """Nonempty collected tables of each dataset that no registered mapper promotes yet."""
    counts: dict[str, int] = {}
    for dataset_id, prefix, name, accept in UNMAPPED:
        for table in committed_tables(workspace, prefix):
            entry = table.entry
            if table.store != "market" or entry["name"] != name or not entry["rows"]:
                continue
            if accept is not None:
                target = _quote(str(entry["target"]))
                row = workspace.market.execute(
                    f"SELECT bool_and(coalesce(({accept}), false)) FROM {target}"  # noqa: S608 -- fixed predicates over a quoted table
                ).fetchone()
                if row is None or row[0] is not True:
                    continue
            counts[dataset_id] = counts.get(dataset_id, 0) + 1
    return dict(sorted(counts.items()))


def dataset_heads(workspace: Workspace) -> list[dict[str, object]]:
    """Every committed dataset's head version and its watermarks: the run's closing state."""
    heads: list[dict[str, object]] = []
    for dataset_id, version, sequence, rows in workspace.state.execute(
        "SELECT dataset_id, version, sequence, row_count FROM dataset_versions v "
        "WHERE status='committed' AND sequence=(SELECT max(sequence) FROM dataset_versions w "
        "WHERE w.dataset_id=v.dataset_id AND w.status='committed') ORDER BY dataset_id"
    ):
        watermarks = {
            f"{provider}/{partition}": {"through_us": int(through), "version": str(committed)}
            for provider, partition, through, committed in workspace.state.execute(
                "SELECT provider, partition_id, through_us, committed_version FROM watermarks "
                "WHERE dataset_id=? ORDER BY provider, partition_id",
                (dataset_id,),
            )
        }
        heads.append(
            {
                "dataset_id": str(dataset_id),
                "version": str(version),
                "sequence": int(sequence),
                "rows": int(rows),
                "watermarks": watermarks,
            }
        )
    return heads
