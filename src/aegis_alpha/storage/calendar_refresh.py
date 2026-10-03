"""``aas calendar refresh``: one declaration becomes the next generation of its calendar.

For one ``aas-calendar-declaration-v1`` document the refresh

1. checks it against the dataset head (``sessions.<mic>``): the same calendar, venue and
   time zone, a range covering every date the head's declaration states, declared no
   earlier than that declaration, and not at the same instant with other content, so an
   older declaration can never undo a newer one and no stated date is left behind;
2. (apply) retains the document in ``raw/`` and commits its source table
   (``calendar-declared-sessions-<hex>``, one row per date) to the source library, which
   reuses the source when the same bytes were committed before;
3. promotes that table with ``calendar.declared@1`` as the child of the head, both time
   columns under ``declared_session_end@1`` (basis ``record``) on the mapper's
   ``public_by`` instant.

A changed date is a SUPERSEDE in a new generation (a temporary closure, a corrected past
schedule) and a new date an ASSERT (an extended year); every earlier generation stays
exactly as pinned. Under the ``record`` basis a SUPERSEDE takes the time AAS received the
correcting declaration (its source's ``sl:`` link), never the corrected date's own end, so
a correction is never known before it existed and its times never run behind the
revision it replaces. A plan whose changed dates would still be stale is refused before
anything is written rather than published as an empty delta. Refreshing the same declaration
again is an empty delta and writes nothing. ``--plan`` writes nothing: when the source is
already committed it reports the engine's full plan, otherwise the date-level changes
against the head's declaration. No provider is called and no clock reaches a stored value;
the caller's ``now_us`` only refuses a declaration from the future and reports whether the
declaration covers the end of next year.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Final, cast

import duckdb

from aegis_alpha.storage.calendar_declaration import (
    SOURCE_MAJOR,
    SOURCE_PROVIDER,
    SOURCE_SHAPE,
    SOURCE_TABLE,
    Declaration,
    parse_declaration,
    source_table,
)
from aegis_alpha.storage.promotion import formats
from aegis_alpha.storage.promotion.engine import dataset_head, generation_spec, promote
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import SourceContent, SourceFile
from aegis_alpha.storage.source_library import import_content_arrow, list_sources, list_tables
from aegis_alpha.storage.source_reader import SourcePin, resolve_source

if TYPE_CHECKING:
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.storage.workspace import Workspace

MAPPER: Final = "calendar.declared@1"
TIME_RULE: Final = {
    "rule": "declared_session_end@1",
    "basis": "record",
    "input": "public_by",
    "args": {},
}
_SAMPLE: Final = 20
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)

type _Day = tuple[str, datetime | None, datetime | None]


def timezone_version() -> str:
    """The zone data label: DuckDB's bundled ICU data, fixed by the locked DuckDB."""
    return f"duckdb-{duckdb.__version__}-icu"


def declaration_content(declaration: Declaration) -> SourceContent:
    """The content identity of a declaration's source: its exact document bytes."""
    return SourceContent(
        SOURCE_PROVIDER,
        SOURCE_SHAPE,
        SOURCE_MAJOR,
        (SourceFile(declaration.sha256, len(declaration.raw)),),
    )


def promotion_spec(
    declaration: Declaration, pin: SourcePin, parent: str | None, version: str
) -> bytes:
    """The canonical ``aas-promotion-v1`` spec the refresh promotes."""
    return formats.canonical(
        {
            "schema_version": "aas-promotion-v1",
            "target": {
                "domain": "calendar_sessions",
                "dataset_id": declaration.dataset_id,
                "parent": parent,
            },
            "sources": [
                {
                    "source_id": pin.source_id,
                    "source_sha256": pin.source_sha256,
                    "table": pin.table,
                    "digest": pin.table_digest,
                }
            ],
            "mapper": {"name": MAPPER, "args": {"timezone_version": version}},
            "partition": None,
            "time_rules": {"available_at_us": TIME_RULE, "revision_known_at_us": TIME_RULE},
            "decimal_rule": {},
            "quality_rules": [],
            "tombstone_policy": {"mode": "never"},
            "identity_snapshot": None,
        }
    )


class _Head:
    """The head generation's declaration: its spec, source pin and declared rows."""

    def __init__(self, workspace: Workspace, generation_id: str) -> None:
        spec = generation_spec(workspace, generation_id)
        if spec.mapper_name != MAPPER or len(spec.sources) != 1:
            raise ValueError(
                f"dataset {spec.dataset_id} was not promoted from one calendar declaration"
            )
        self.generation_id = generation_id
        self.pin = spec.sources[0]
        self.timezone_version = str(spec.mapper_args["timezone_version"])
        try:
            target = str(resolve_source(workspace, self.pin)["target"])
        except ImportError:
            raise ValueError(
                "aas calendar refresh verifies the head's source table through pyarrow; "
                "install the legacy extra"
            ) from None
        rows = workspace.market.execute(
            "SELECT DISTINCT calendar_id, venue, timezone, epoch_us(declared_at) FROM "  # noqa: S608
            f"{formats.quote_identifier(target)}"
        ).fetchall()
        if len(rows) != 1:
            raise ValueError("the head's declaration source holds more than one declaration")
        self.calendar = tuple(str(value) for value in rows[0][:3])
        self.declared_at_us = int(rows[0][3])
        self.days: dict[date, _Day] = {
            row[0]: (str(row[1]), row[2], row[3])
            for row in workspace.market.execute(
                "SELECT session_date, status, open_local, close_local FROM "  # noqa: S608
                f"{formats.quote_identifier(target)}"
            ).fetchall()
        }


def _committed_pin(workspace: Workspace, content: SourceContent) -> SourcePin | None:
    if content.source_id not in {str(row["source_id"]) for row in list_sources(workspace)}:
        return None
    (table,) = [
        row for row in list_tables(workspace, content.source_id) if row["name"] == SOURCE_TABLE
    ]
    return SourcePin(content.source_id, content.sha256, SOURCE_TABLE, str(table["digest"]))


def _commit(workspace: Workspace, declaration: Declaration) -> SourcePin:
    content = declaration_content(declaration)
    put_raw(workspace.paths.raw, declaration.raw)
    try:
        table = source_table(declaration)
    except ImportError:
        raise ValueError(
            "aas calendar refresh commits its source table through pyarrow; "
            "install the legacy extra"
        ) from None
    result = import_content_arrow(
        workspace,
        content,
        SOURCE_TABLE,
        table.to_reader(),
        lineage={"loader": "aas calendar refresh", "declaration": declaration.sha256},
    )
    (committed,) = cast("list[dict[str, object]]", result["tables"])
    return SourcePin(content.source_id, content.sha256, SOURCE_TABLE, str(committed["digest"]))


def _check_head(declaration: Declaration, head: _Head, content: SourceContent) -> None:
    calendar = (declaration.calendar_id, declaration.venue, declaration.timezone)
    if head.calendar != calendar:
        raise ValueError(f"{declaration.dataset_id} holds calendar {head.calendar}, not {calendar}")
    if declaration.declared_at_us < head.declared_at_us:
        raise ValueError(
            f"the declaration is older than the one {declaration.dataset_id} was promoted "
            "from; declare the change again with a later declared_at"
        )
    if head.days and (declaration.start > min(head.days) or declaration.end <= max(head.days)):
        raise ValueError(
            f"the declaration does not cover every date {declaration.dataset_id} holds "
            f"({min(head.days)}..{max(head.days)}); a declaration states its whole range"
        )
    if (
        declaration.declared_at_us == head.declared_at_us
        and content.source_id != head.pin.source_id
    ):
        raise ValueError("the declaration has the head declaration's declared_at but other content")


def _changes(declaration: Declaration, head: _Head | None) -> dict[str, object]:
    """Date-level changes against the head's declaration, before its source is committed."""
    before = {} if head is None else head.days
    added = changed = unchanged = 0
    sample: list[dict[str, object]] = []
    for item in declaration.days():
        after: _Day = (item.status, item.open_local, item.close_local)
        found = before.get(item.session_date)
        if found is None:
            added += 1
        elif found == after:
            unchanged += 1
        else:
            changed += 1
            if len(sample) < _SAMPLE:
                sample.append(
                    {
                        "date": item.session_date.isoformat(),
                        "before": _day_json(found),
                        "after": _day_json(after),
                    }
                )
    return {
        "added": added,
        "changed": changed,
        "unchanged": unchanged,
        "changed_sample": sample,
        "timezone_version_changed": head is not None
        and head.timezone_version != timezone_version(),
    }


def _day_json(value: _Day) -> dict[str, object]:
    status, opened, closed = value
    return {
        "status": status,
        "open": None if opened is None else opened.strftime("%H:%M"),
        "close": None if closed is None else closed.strftime("%H:%M"),
    }


def refresh_calendar(  # noqa: PLR0913 -- one document, its hash and the run's inputs
    workspace: Workspace,
    raw: bytes,
    sha256: str,
    *,
    apply: bool,
    now_us: int,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """Plan, or apply, one declaration as the next generation of its calendar dataset."""
    declaration = parse_declaration(raw, sha256)
    if declaration.declared_at_us > now_us:
        raise ValueError("the declaration's declared_at is later than now")
    required = date((_EPOCH + timedelta(microseconds=now_us)).year + 1, 12, 31)
    parent = dataset_head(workspace, declaration.dataset_id)
    head = None if parent is None else _Head(workspace, parent)
    content = declaration_content(declaration)
    if head is not None:
        _check_head(declaration, head, content)
    report: dict[str, object] = {
        "calendar_id": declaration.calendar_id,
        "dataset_id": declaration.dataset_id,
        "mode": "apply" if apply else "plan",
        "declaration": declaration.summary(),
        "coverage": {
            "through": (declaration.end - timedelta(days=1)).isoformat(),
            "required_through": required.isoformat(),
            "covers_next_year": declaration.end > required,
        },
        "head": parent,
        "source_id": content.source_id,
    }
    pin = _commit(workspace, declaration) if apply else _committed_pin(workspace, content)
    report["source_committed"] = pin is not None
    if pin is None:
        return {**report, "promotion": None, "changes": _changes(declaration, head)}
    spec = promotion_spec(declaration, pin, parent, timezone_version())
    spec_sha = hashlib.sha256(spec).hexdigest()
    planned = promote(workspace, spec, spec_sha, apply=False, budget=budget)
    if planned.get("stale"):
        raise ValueError(
            f"{planned['stale']} changed dates of the declaration are older than the "
            f"{declaration.dataset_id} head revisions they would replace"
        )
    result = promote(workspace, spec, spec_sha, apply=True, budget=budget) if apply else planned
    return {**report, "spec_sha256": spec_sha, "promotion": result}
