"""``aas import sec-companies``: the company header of each CIK document in an SEC archive.

An SEC submissions archive is retained in ``raw/`` by ``sec.submissions_zip@1``, whose
content source (``sec-submissions-zip-*``) holds the archive's member index. This reads,
through that index, every ``CIK##########.json`` member and commits one row per member as
the content source ``sec-submissions-companies-<hex>`` (table ``companies``). Its original
file is the same archive, so the same archive bytes always give the same source ID and a
rerun reuses it; no provider is called. A rerun that reuses the source does not read the
archive again, so its report has no member counts (``members``, ``companies``,
``with_sic``); ``plan_companies`` reads every member and always reports them.

- The archive must have the size and SHA-256 its source records, and each member the size
  and SHA-256 its index row records. Members other than CIK documents (paginated filing
  histories, placeholders) are not rows.
- A row keeps the document's own values as text: the stated ``cik``, ``name``,
  ``entityType`` (``entity_type``), ``sic`` and ``sicDescription`` (``sic_description``),
  each as the document spells it (a string as itself, any other JSON value as its
  canonical JSON, an absent key as NULL). ``latest_filing_date`` is the greatest
  ``filings.recent.filingDate`` text, the newest filing the document lists, and NULL when it
  lists none. ``member`` and ``member_sha256`` name the member the row came from.
- A member that is not a UTF-8 JSON object refuses the whole import, because the archive
  then is not what its index says it holds.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.storage.source_identity import SourceContent
from aegis_alpha.storage.us_identity import (
    SEC_MEMBER,
    SEC_TABLE,
    open_archive,
    read_member,
    read_rows,
    sec_archive,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    import pyarrow as pa

    from aegis_alpha.storage.us_identity import ArchiveOpener
    from aegis_alpha.storage.workspace import Workspace

PROVIDER: Final = "sec"
SHAPE: Final = "submissions-companies"
TABLE: Final = "companies"
SOURCE_MAJOR: Final = 1
COMMAND: Final = "aas import sec-companies"
COLUMNS: Final = (
    "member",
    "member_sha256",
    "cik",
    "name",
    "entity_type",
    "sic",
    "sic_description",
    "latest_filing_date",
)
_BATCH: Final = 65536


def _text(value: object) -> str | None:
    """A document value as text: a string as itself, other JSON as canonical JSON."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _latest(body: Mapping[str, object]) -> str | None:
    filings = body.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    dates = recent.get("filingDate") if isinstance(recent, dict) else None
    if not isinstance(dates, list):
        return None
    texts = [item for item in cast("list[object]", dates) if isinstance(item, str)]
    return max(texts, default=None)


def company_row(member: str, sha256: str, raw: bytes) -> tuple[str | None, ...]:
    """The ``companies`` row of one CIK document's bytes."""
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError(f"SEC member {member} is not UTF-8 JSON") from None
    if not isinstance(body, dict):
        raise ValueError(f"SEC member {member} is not a JSON object")  # noqa: TRY004 -- content
    document = cast("dict[str, object]", body)
    return (
        member,
        sha256,
        _text(document.get("cik")),
        _text(document.get("name")),
        _text(document.get("entityType")),
        _text(document.get("sic")),
        _text(document.get("sicDescription")),
        _latest(document),
    )


@dataclass(slots=True)
class CompaniesSource:
    """The companies content source of one archive and how to read its rows."""

    members_source: str
    content: SourceContent
    opener: ArchiveOpener
    index: tuple[tuple[object, ...], ...]
    index_columns: tuple[str, ...]
    counts: dict[str, int] = field(default_factory=dict)

    def rows(self) -> Iterator[tuple[str | None, ...]]:
        """Each CIK document's row in index order, every member checked against the index."""
        (archive,) = self.content.files
        self.counts = {"members": 0, "companies": 0, "with_sic": 0}
        with self.opener() as handle, open_archive(handle, archive) as bundle:
            for values in self.index:
                row = dict(zip(self.index_columns, values, strict=True))
                self.counts["members"] += 1
                member = str(row["member"])
                if SEC_MEMBER.fullmatch(member) is None:
                    continue
                company = company_row(member, str(row["sha256"]), read_member(bundle, row))
                self.counts["companies"] += 1
                self.counts["with_sic"] += company[5] not in {None, ""}
                yield company

    def batches(self) -> Iterator[pa.RecordBatch]:
        import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

        schema = pa.schema([(name, pa.string()) for name in COLUMNS])
        rows: list[tuple[str | None, ...]] = []
        for row in self.rows():
            rows.append(row)
            if len(rows) == _BATCH:
                yield _batch(schema, rows)
                rows = []
        if rows:
            yield _batch(schema, rows)

    def reader(self) -> pa.RecordBatchReader:
        import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

        schema = pa.schema([(name, pa.string()) for name in COLUMNS])
        return pa.RecordBatchReader.from_batches(schema, self.batches())


def _batch(schema: pa.Schema, rows: list[tuple[str | None, ...]]) -> pa.RecordBatch:
    import pyarrow as pa  # noqa: PLC0415 -- the legacy extra commits source tables

    return pa.record_batch(
        [pa.array([row[index] for row in rows], pa.string()) for index in range(len(COLUMNS))],
        schema=schema,
    )


def companies_source(workspace: Workspace, members_source: str) -> CompaniesSource:
    """The companies source of a committed ``sec-submissions-zip-*`` content source."""
    archive, opener = sec_archive(workspace, members_source, reader=COMMAND)
    index = read_rows(workspace, members_source, SEC_TABLE)
    missing = {"member", "size", "sha256"} - set(index.columns)
    if missing:
        raise ValueError(f"{members_source} member index lacks {sorted(missing)}")
    content = SourceContent(PROVIDER, SHAPE, SOURCE_MAJOR, (archive,))
    return CompaniesSource(members_source, content, opener, index.rows, index.columns)


def plan_companies(workspace: Workspace, members_source: str) -> dict[str, object]:
    """Read every CIK document as the import would and report it; write nothing."""
    from aegis_alpha.storage.source_library import committed_source_ids  # noqa: PLC0415

    source = companies_source(workspace, members_source)
    for _ in source.rows():
        pass
    committed = committed_source_ids(workspace)
    return {
        "mode": "plan",
        "members_source": members_source,
        "source_id": source.content.source_id,
        "table": TABLE,
        "committed": source.content.source_id in committed,
        **source.counts,
    }


def import_companies(workspace: Workspace, members_source: str) -> dict[str, object]:
    """Commit the companies table as its content source; a rerun reuses it."""
    from aegis_alpha.storage.source_library import import_content_arrow  # noqa: PLC0415

    source = companies_source(workspace, members_source)
    try:
        reader = source.reader()
    except ImportError:
        raise ValueError(f"{COMMAND} commits source tables through pyarrow") from None
    result = import_content_arrow(
        workspace,
        source.content,
        TABLE,
        reader,
        lineage={
            "loader": COMMAND,
            "parser": "sec-submissions-companies-v1",
            "members_source": members_source,
        },
    )
    (committed,) = cast("list[dict[str, object]]", result["tables"])
    reused = bool(result.get("reused", False))
    report: dict[str, object] = {
        "mode": "apply",
        "members_source": members_source,
        "source_id": source.content.source_id,
        "table": TABLE,
        "rows": committed["rows"],
        "digest": committed["digest"],
        "reused": reused,
    }
    if not reused:
        report |= source.counts
    return report
