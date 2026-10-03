"""``aas collect qveris import``: completed Qveris jobs as content-addressed sources.

One collection job is one original unit: its ``complete.json``, the four page files it
pins and, when its rows need an instrument identity, the identity document the rows
were resolved with. Each unit commits under ``qveris-<shape>-<hex>`` where ``hex`` names
exactly those bytes (``aas-source-id-v1``), so the same completed job always lands on
the same source ID whatever the run order, batch or loader version, and a changed
identity document is a new source.

| job | rows table (shape, table) | held rows (shape, table) |
| --- | --- | --- |
| KR/US ``price_history`` | ``<m>-history-bars``, ``bars`` | ``<m>-history-quarantine`` |
| exchange bulk ``prices`` | ``bulk-bars``, ``bars`` | ``bulk-quarantine`` |
| exchange bulk ``splits`` | ``splits``, ``splits`` | ``splits-quarantine`` |
| exchange bulk ``dividends`` | ``dividends``, ``dividends`` | ``dividends-quarantine`` |
| FX ``fx_history`` | ``fx-history-bars``, ``bars`` | ``fx-history-quarantine`` |

Every held table is named ``quarantine``. The rows table is always committed, even
empty (an exchange-day without splits is a fact); the held table is committed only when
it has rows, and before the rows table, so a committed rows table means the unit is
complete. Provider warnings are recorded, never block: every row of a warned download is
held with the reason ``provider_reported_partial`` and the import continues. The
history and bulk tables keep the columns the ``eodhd.*`` mappers read.

Nothing here calls a provider. Code and transform hashes are commit ``lineage`` only.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import KW_ONLY, dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
from aegis_alpha.data.qveris_contracts import (
    ACTION_DATASETS,
    EOD_HISTORY_JSON_TOOL,
    EOD_TOOL,
    FX_DATASET,
    FX_MARKET,
    QverisJob,
    load_json,
    object_value,
)
from aegis_alpha.data.qveris_native import (
    CompletedHistory,
    NormalizedHistory,
    normalize_bulk_actions,
    normalize_bulk_prices,
    normalize_fx_history,
    normalize_price_history,
    read_completed_job,
)
from aegis_alpha.data.serialization import content_sha256
from aegis_alpha.storage.source_identity import SourceContent, SourceFile

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    import pyarrow as pa

    from aegis_alpha.storage.workspace import Workspace

PROVIDER: Final = "qveris"
SCHEMA_MAJOR: Final = 1
LOADER: Final = "aas collect qveris import"
HELD_TABLE: Final = "quarantine"
MAX_IDENTITY_BYTES: Final = 64 * 1024 * 1024
MAX_COMPLETION_BYTES: Final = 1024 * 1024
_IDENTITY_FIELDS: Final = ("instrument_id", "venue", "instrument_type", "currency")
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_CODE: Final = (
    "data/qveris_contracts.py",
    "data/qveris_native.py",
    "data/qveris_payloads.py",
    "storage/qveris_import.py",
)


@dataclass(frozen=True, slots=True)
class Kind:
    """How one job kind is normalized and which sources it commits."""

    name: str
    shape: str
    table: str
    held_shape: str
    _: KW_ONLY
    needs_identity: bool


def kind_of(job: QverisJob) -> Kind | None:
    """The import kind of a completed job, or ``None`` when no importer reads it."""
    if job.tool_id == EOD_HISTORY_JSON_TOOL and job.dataset == "price_history":
        if job.market not in {"KR", "US"}:
            return None
        market = job.market.lower()
        return Kind(
            "price_history",
            f"{market}-history-bars",
            "bars",
            f"{market}-history-quarantine",
            needs_identity=True,
        )
    if job.tool_id == EOD_HISTORY_JSON_TOOL and (job.market, job.dataset) == (
        FX_MARKET,
        FX_DATASET,
    ):
        return Kind(
            FX_DATASET, "fx-history-bars", "bars", "fx-history-quarantine", needs_identity=False
        )
    if job.tool_id == EOD_TOOL and job.dataset == "prices":
        return Kind("bulk_prices", "bulk-bars", "bars", "bulk-quarantine", needs_identity=True)
    if job.tool_id == EOD_TOOL and job.dataset in ACTION_DATASETS:
        return Kind(
            job.dataset,
            job.dataset,
            job.dataset,
            f"{job.dataset}-quarantine",
            needs_identity=True,
        )
    return None


def _schemas() -> dict[str, pa.Schema]:
    import pyarrow as pa  # noqa: PLC0415 -- the source library's Arrow loaders need the legacy extra

    identity = [(name, pa.string()) for name in _IDENTITY_FIELDS]
    prices = [(name, pa.float64()) for name in ("open", "high", "low", "close")]
    adjusted = [("adjusted_close", pa.float64()), ("volume", pa.float64())]
    provenance = [
        ("source_fingerprint", pa.string()),
        ("raw_sha256", pa.string()),
        ("retrieved_at", pa.timestamp("us", tz="UTC")),
        ("source_row", pa.int64()),
    ]
    symbol = [("provider_symbol", pa.string()), ("date", pa.date32())]
    bars = pa.schema(
        [
            *identity,
            *symbol,
            *prices,
            *adjusted,
            *provenance,
            ("calendar_verified", pa.bool_()),
            ("independent_identity_verified", pa.bool_()),
        ]
    )
    pair = [(name, pa.string()) for name in ("pair", "base_currency", "quote_currency")]
    texts = (
        "dividend",
        "dividend_currency",
        "unadjusted_value",
        "declaration_date",
        "record_date",
        "payment_date",
        "period",
    )
    verified = [("independent_identity_verified", pa.bool_())]
    held = [("ordinal", pa.int64()), ("reason", pa.string()), ("source_row_json", pa.string())]
    return {
        "price_history": bars,
        "bulk_prices": bars,
        FX_DATASET: pa.schema([*pair, *symbol, *prices, *adjusted, *provenance]),
        "splits": pa.schema([*identity, *symbol, ("split", pa.string()), *provenance, *verified]),
        "dividends": pa.schema(
            [*identity, *symbol, *((name, pa.string()) for name in texts), *provenance, *verified]
        ),
        "held_job": pa.schema([("source_fingerprint", pa.string()), *held]),
        "held": pa.schema(held),
    }


def held_schema_name(kind: Kind) -> str:
    """History quarantines name their job; exchange-day quarantines are one job already."""
    return "held_job" if kind.name in {"price_history", FX_DATASET} else "held"


# --- identity document ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class IdentityDocument:
    """An explicit symbol → instrument identity document and its exact bytes."""

    raw: bytes
    identities: Mapping[str, Mapping[str, str]]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


def parse_identity(raw: bytes) -> IdentityDocument:
    """Admit ``{"identities": {"<CODE>.<EXCHANGE>": {instrument_id, venue, ...}}}``.

    Each entry must carry the four identity fields as text; other fields (names and
    evidence notes) are kept in the bytes and ignored by the normalizers.
    """
    document = object_value(load_json(raw))
    entries = object_value(document.get("identities"))
    identities: dict[str, Mapping[str, str]] = {}
    for symbol, value in entries.items():
        entry = object_value(value)
        fields = {name: entry.get(name) for name in _IDENTITY_FIELDS}
        if any(not isinstance(item, str) or not item for item in fields.values()):
            raise ValueError(f"identity of {symbol} lacks a text identity field")
        identities[symbol] = cast("dict[str, str]", fields)
    if not identities:
        raise ValueError("identity document holds no identities")
    return IdentityDocument(raw, identities)


# --- completed jobs ------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Completion:
    fingerprint: str
    completion_sha256: str
    job: QverisJob


def _read(tree: DescriptorTree, relative: str) -> bytes:
    return tree.read_bytes(relative, max_bytes=MAX_COMPLETION_BYTES)


def completions(
    raw_root: Path,
    *,
    fingerprints: Iterable[str] | None = None,
    markets: frozenset[str] = frozenset(),
    datasets: frozenset[str] = frozenset(),
) -> tuple[list[Completion], list[dict[str, object]]]:
    """Completed jobs of ``raw_root`` in fingerprint order, and the requested ones not done.

    With ``fingerprints`` only those jobs are read and a job without ``complete.json``
    is reported ``no_completion``; otherwise every completed job is listed. Empty
    ``markets``/``datasets`` select every market/dataset.
    """
    found: list[Completion] = []
    missing: list[dict[str, object]] = []
    try:
        with DescriptorTree.open_path(raw_root) as tree:
            if fingerprints is None:
                names = sorted(tree.listdir("jobs")) if tree.exists("jobs") else []
                selected = [n for n in names if tree.exists(f"jobs/{n}/complete.json")]
            else:
                selected = sorted(set(fingerprints))
            for name in selected:
                if _FINGERPRINT.fullmatch(name) is None:
                    raise ValueError("Qveris job directories are named by their fingerprint")
                path = f"jobs/{name}/complete.json"
                if not tree.exists(path):
                    missing.append({"fingerprint": name, "status": "no_completion"})
                    continue
                body = _read(tree, path)
                job = QverisJob.from_document(object_value(load_json(body)).get("job"))
                if job.fingerprint != name:
                    raise ValueError("Qveris completion is filed under another fingerprint")
                if (markets and job.market not in markets) or (
                    datasets and job.dataset not in datasets
                ):
                    continue
                found.append(Completion(name, hashlib.sha256(body).hexdigest(), job))
    except (OSError, DescriptorTreeError) as error:
        raise ValueError("cannot read the Qveris raw collection root") from error
    return found, missing


# --- units ---------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Unit:
    """One completed job: its content identity and the tables it commits."""

    kind: Kind
    history: CompletedHistory
    files: tuple[bytes, ...]
    rows: pa.Table
    held: pa.Table
    lineage: Mapping[str, object]

    @property
    def content(self) -> SourceContent:
        return content_of(self.kind, self.files)

    @property
    def source_id(self) -> str:
        return f"{PROVIDER}-{self.kind.shape}-{self.content.sha256}"

    @property
    def held_source_id(self) -> str:
        return f"{PROVIDER}-{self.kind.held_shape}-{self.content.sha256}"


def content_of(kind: Kind, files: Sequence[bytes], *, held: bool = False) -> SourceContent:
    return SourceContent(
        PROVIDER,
        kind.held_shape if held else kind.shape,
        SCHEMA_MAJOR,
        tuple(SourceFile(hashlib.sha256(raw).hexdigest(), len(raw)) for raw in files),
    )


def _normalize(
    kind: Kind, history: CompletedHistory, identity: IdentityDocument | None
) -> NormalizedHistory:
    if kind.name == FX_DATASET:
        return normalize_fx_history(history)
    if identity is None:
        raise ValueError(f"{kind.name} rows need an identity document (--identity)")
    if kind.name == "price_history":
        return normalize_price_history(history, identity.identities)
    if kind.name == "bulk_prices":
        return normalize_bulk_prices(history, identity.identities)
    return normalize_bulk_actions(history, identity.identities)


def code_lineage() -> dict[str, object]:
    """SHA-256 of the reading code and the Arrow version: lineage, never identity."""
    package = Path(__file__).resolve().parent.parent
    code: dict[str, object] = {
        name: hashlib.sha256((package / name).read_bytes()).hexdigest() for name in _CODE
    }
    try:
        code["pyarrow_version"] = version("pyarrow")
    except PackageNotFoundError:
        code["pyarrow_version"] = None
    return code


def unit_files(
    kind: Kind, history: CompletedHistory, identity: IdentityDocument | None
) -> tuple[bytes, ...]:
    """The unit's original bytes: the job's evidence, plus the identity its rows used."""
    return history.evidence + ((identity.raw,) if kind.needs_identity and identity else ())


def build_unit(
    completion: Completion,
    history: CompletedHistory,
    identity: IdentityDocument | None,
    *,
    code: Mapping[str, object] | None = None,
) -> Unit:
    """Normalize one verified completed job (``read_completed_job``) into its two tables."""
    import pyarrow as pa  # noqa: PLC0415 -- the source library's Arrow loaders need the legacy extra

    kind = kind_of(completion.job)
    if kind is None or history.job.fingerprint != completion.fingerprint:
        raise ValueError("no importer reads this Qveris job kind")
    normalized = _normalize(kind, history, identity)
    schemas = _schemas()
    rows = pa.Table.from_pylist([dict(row) for row in normalized.rows], schema=schemas[kind.name])
    with_job = held_schema_name(kind) == "held_job"
    held = pa.Table.from_pylist(
        [
            {
                **({"source_fingerprint": completion.fingerprint} if with_job else {}),
                "ordinal": row["ordinal"],
                "reason": row["reason"],
                "source_row_json": json.dumps(row["source_row"], sort_keys=True),
            }
            for row in normalized.quarantine
        ],
        schema=schemas[held_schema_name(kind)],
    )
    files = unit_files(kind, history, identity)
    lineage = {
        "loader": LOADER,
        "kind": kind.name,
        "job_id": completion.job.job_id,
        "fingerprint": completion.fingerprint,
        "completion_sha256": completion.completion_sha256,
        "provider_warning": history.provider_warning,
        "identity_sha256": identity.sha256 if kind.needs_identity and identity else None,
        # One digest keeps every commit's lineage small; the run report carries the map.
        "code_sha256": content_sha256(dict(code) if code is not None else code_lineage()),
        "source_only": True,
        "point_in_time_certified": False,
    }
    return Unit(kind, history, files, rows, held, lineage)


def _digest(table: pa.Table) -> tuple[int, str]:
    from aegis_alpha.storage.source_library_digest import arrow_digest  # noqa: PLC0415

    return arrow_digest(table.to_reader())


def unit_report(unit: Unit) -> dict[str, object]:
    rows, digest = _digest(unit.rows)
    report: dict[str, object] = {
        "fingerprint": unit.history.job.fingerprint,
        "job_id": unit.history.job.job_id,
        "kind": unit.kind.name,
        "source_id": unit.source_id,
        "table": unit.kind.table,
        "rows": rows,
        "digest": digest,
        "held_rows": unit.held.num_rows,
        "provider_warning": unit.history.provider_warning,
    }
    if unit.held.num_rows:
        report["held_source_id"] = unit.held_source_id
        report["held_digest"] = _digest(unit.held)[1]
    return report


def import_unit(workspace: Workspace, unit: Unit) -> dict[str, object]:
    """Retain the unit's files in ``raw/`` and commit its held table, then its rows table."""
    from aegis_alpha.storage.raw import put_raw  # noqa: PLC0415 -- lazy storage imports
    from aegis_alpha.storage.source_library import import_content_arrow  # noqa: PLC0415

    for raw in unit.files:
        put_raw(workspace.paths.raw, raw)
    report = unit_report(unit)
    reused = []
    if unit.held.num_rows:
        held = import_content_arrow(
            workspace,
            content_of(unit.kind, unit.files, held=True),
            HELD_TABLE,
            unit.held.to_reader(),
            lineage=unit.lineage,
        )
        reused.append(bool(held.get("reused", False)))
    result = import_content_arrow(
        workspace, unit.content, unit.kind.table, unit.rows.to_reader(), lineage=unit.lineage
    )
    reused.append(bool(result.get("reused", False)))
    (committed,) = cast("list[dict[str, object]]", result["tables"])
    if (committed["rows"], committed["digest"]) != (report["rows"], report["digest"]):
        raise ValueError("committed Qveris rows differ from the normalized job")
    return {**report, "reused": all(reused)}


# --- runs ----------------------------------------------------------------------------------


def import_completions(  # noqa: PLR0913 -- one ordered, resumable import loop and its report
    raw_root: Path,
    selected: Sequence[Completion],
    identity: IdentityDocument | None,
    *,
    workspace: Workspace | None,
    limit: int | None = None,
    committed: frozenset[str] = frozenset(),
) -> dict[str, object]:
    """Import (``workspace``) or plan (``None``) ``selected`` in order; never call a provider.

    A unit whose rows source is already committed is ``reused`` without being rebuilt.
    A unit that fails to read or normalize is recorded under ``failures`` and the run
    continues; ``limit`` bounds the units built in this run.
    """
    code = code_lineage()
    units: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    unsupported: list[str] = []
    reused = built = attempted = 0
    totals = {"rows": 0, "held_rows": 0, "provider_warnings": 0}
    for completion in selected:
        kind = kind_of(completion.job)
        if kind is None:
            unsupported.append(completion.fingerprint)
            continue
        if kind.needs_identity and identity is None:
            raise ValueError(f"{kind.name} jobs need an identity document (--identity)")
        try:
            history = read_completed_job(
                raw_root, completion.fingerprint, completion.completion_sha256
            )
        except (ValueError, TypeError, OSError, RuntimeError) as error:
            failures.append(_failure(completion, error))
            continue
        source_id = f"{PROVIDER}-{kind.shape}-"
        source_id += content_of(kind, unit_files(kind, history, identity)).sha256
        if source_id in committed:
            reused += 1
            units.append(
                {"fingerprint": completion.fingerprint, "source_id": source_id, "reused": True}
            )
            continue
        if limit is not None and attempted >= limit:
            break
        attempted += 1
        try:
            unit = build_unit(completion, history, identity, code=code)
            report = unit_report(unit) if workspace is None else import_unit(workspace, unit)
        except (ValueError, TypeError, OSError) as error:
            failures.append(_failure(completion, error))
            continue
        totals["rows"] += int(str(report["rows"]))
        totals["held_rows"] += int(str(report["held_rows"]))
        totals["provider_warnings"] += report["provider_warning"] is True
        built += 1
        units.append(report)
    pending = len(selected) - len(unsupported) - len(units) - len(failures)
    return {
        "mode": "plan" if workspace is None else "apply",
        "requested": len(selected),
        "built": built,
        "reused": reused,
        "failed": len(failures),
        "pending": pending,
        "unsupported": len(unsupported),
        **totals,
        "units": units,
        "failures": failures,
        "provider_calls": 0,
        "source_only": True,
        "point_in_time_certified": False,
        "code": code,
    }


def _failure(completion: Completion, error: BaseException) -> dict[str, object]:
    return {
        "fingerprint": completion.fingerprint,
        "job_id": completion.job.job_id,
        "error_type": type(error).__name__,
        "reason": str(error),
    }
