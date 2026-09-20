"""Detached, revision-bearing native research inputs, not execution eligibility.

Load once under workspace admission with a caller-owned compute budget. Each
decision replays the retained immutable chain; no decision queries a latest head.
Strict mode inherits market.project_heads knowledge/availability semantics.
Observed snapshot research is explicit, economically bounded and uncertified.
Coverage describes the entire requested grid, including future/unavailable cells.
Identity/universe pins refer to native immutable snapshots, never current tickers.
Absent pins and unverified catalogs remain reasons, not inferred certification.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import localcontext
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, assert_never

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage import market
from aegis_alpha.storage.import_document import ImportDocument, parse_import
from aegis_alpha.storage.market_schema import COMMON, DOMAINS
from aegis_alpha.storage.membership_pins import (
    IdentityPin,
    UniversePin,
    read_membership_pins,
)
from aegis_alpha.storage.research_inputs import (
    OBSERVATION_DEFINITION_SCHEMA,
    OBSERVATION_TRANSFORM_SCHEMA,
    native_input_document,
)
from aegis_alpha.storage.source_reader import SourcePin

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

type Row = Mapping[str, object]
type History = tuple[Row, ...]
type ReaderMode = Literal["strict_pit", "observed_snapshot_research"]

_PROXY_REFS = ("donor_source", "target_source", "basis_ref", "calendar_ref", "cost_ref")
_OBSERVATION_REFS = ("observed_source", "calendar_ref")
_FEATURE_DOMAINS = frozenset({"proxy", "observation"})


def _text(value: object) -> None:
    if not isinstance(value, str) or not value or value.strip() != value or not value.isprintable():
        raise ValueError("identity/convention requires exact nonempty printable text")


def _digest(value: object) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("pin requires lowercase SHA256 hex")


@dataclass(frozen=True, slots=True)
class GenerationPin:
    """Exact published head identity; its hash commits every ancestor delta."""

    dataset_id: str
    version: str
    generation_id: str
    chain_hash: str
    manifest_hash: str

    def __post_init__(self) -> None:
        for value in (self.dataset_id, self.version, self.generation_id):
            _text(value)
        for value in (self.chain_hash, self.manifest_hash):
            _digest(value)
        if self.version == "latest":
            raise ValueError("generation version must be exact, not latest")


@dataclass(frozen=True, slots=True)
class PriceInputRequest:
    """An explicit daily instrument/session grid and its independent price/calendar pins."""

    pin: GenerationPin
    sessions_pin: GenerationPin | None
    instrument_ids: tuple[str, ...]
    session_dates: tuple[date, ...]
    currency: str
    basis: str
    price_role: Literal["canonical", "reference"]
    calendar_id: str
    venue: str
    timezone_version: str
    interval: Literal["1d"] = "1d"
    mode: ReaderMode = "strict_pit"
    identity_pin: IdentityPin | None = None
    universe_pin: UniversePin | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.pin, GenerationPin) or (
            self.sessions_pin is not None and not isinstance(self.sessions_pin, GenerationPin)
        ):
            raise ValueError("prices and sessions require generation pins")
        if (self.identity_pin is not None and not isinstance(self.identity_pin, IdentityPin)) or (
            self.universe_pin is not None and not isinstance(self.universe_pin, UniversePin)
        ):
            raise ValueError("identity/universe require exact typed pins")
        for values in (self.instrument_ids, self.session_dates):
            if type(values) is not tuple or not values or len(set(values)) != len(values):
                raise ValueError("request requires nonempty unique identities and session dates")
        for value in (
            *self.instrument_ids,
            self.currency,
            self.basis,
            self.calendar_id,
            self.venue,
            self.timezone_version,
        ):
            _text(value)
        if any(type(day) is not date for day in self.session_dates):
            raise ValueError("session dates must be date values")
        if tuple(sorted(self.session_dates)) != self.session_dates:
            raise ValueError("session dates must be increasing")
        if self.price_role not in ("canonical", "reference") or self.interval != "1d":
            raise ValueError("unsupported price role or interval")
        _Decision(0, self.mode)
        if self.price_role == "canonical" and self.basis != "unadjusted":
            raise ValueError("canonical prices require unadjusted basis")


@dataclass(frozen=True, slots=True)
class _Decision:
    at_us: int
    mode: ReaderMode
    ingestion_cutoff_us: int | None = None
    session_date: date | None = None

    def __post_init__(self) -> None:
        if type(self.at_us) is not int or self.at_us < 0:
            raise ValueError("decision must be nonnegative UTC microseconds")
        if self.ingestion_cutoff_us is not None and (
            type(self.ingestion_cutoff_us) is not int or self.ingestion_cutoff_us < 0
        ):
            raise ValueError("ingestion cutoff must be nonnegative UTC microseconds")
        if self.session_date is not None and type(self.session_date) is not date:
            raise ValueError("decision session_date must be a date")
        if self.mode not in ("strict_pit", "observed_snapshot_research"):
            raise ValueError("unsupported pinned reader mode")


@dataclass(frozen=True, slots=True)
class CoverageCell:
    instrument_id: str
    session_date: date | None
    present: bool
    reasons: tuple[str, ...]
    record_id: str | None = None


@dataclass(frozen=True, slots=True)
class CoverageReport:
    """Complete requested-cell accounting; completeness is distinct from certification."""

    cells: tuple[CoverageCell, ...]
    reasons: tuple[str, ...]
    certified: bool = field(default=False, init=False)

    @property
    def expected_count(self) -> int:
        return len(self.cells)

    @property
    def present_count(self) -> int:
        return sum(cell.present for cell in self.cells)

    @property
    def complete(self) -> bool:
        return all(not cell.reasons for cell in self.cells)


@dataclass(frozen=True, slots=True)
class ProjectedInputs:
    rows: History
    coverage: CoverageReport


def _integer(row: Row, key: str) -> int:
    value = row[key]
    if type(value) is not int:
        raise ValueError(f"stored {key} must be integer microseconds")
    return value


def _day(row: Row) -> date:
    value = row["session_date"]
    if type(value) is not date:
        raise ValueError("stored session_date must be a date")
    return value


def _project(history: History, decision: _Decision) -> History:
    match decision.mode:
        case "strict_pit":
            cutoff = decision.at_us
        case "observed_snapshot_research":
            cutoff = None
        case unreachable:
            assert_never(unreachable)
    return tuple(
        MappingProxyType(row)
        for row in market.project_heads(
            history,
            cutoff_us=cutoff,
            ingestion_cutoff_us=decision.ingestion_cutoff_us,
        )
    )


def _unknown(row: Row) -> bool:
    return row["available_at_us"] is None or row["revision_known_at_us"] is None


def _coverage(cells: list[CoverageCell], reasons: list[str]) -> CoverageReport:
    return CoverageReport(
        tuple(cells),
        tuple(
            dict.fromkeys(
                [
                    *reasons,
                    *(reason for cell in cells for reason in cell.reasons),
                ]
            )
        ),
    )


def _absence(history: History, decision: _Decision, domain: str) -> str:
    unknown = False
    for row in reversed(history):
        if (
            decision.ingestion_cutoff_us is not None
            and _integer(row, "ingested_at_us") > decision.ingestion_cutoff_us
        ):
            continue
        if decision.mode == "strict_pit":
            if row["revision_known_at_us"] is None:
                unknown = True
                continue
            if _integer(row, "revision_known_at_us") > decision.at_us:
                continue
            if row["available_at_us"] is None:
                return "unknown_" + domain + "_evidence"
            if _integer(row, "available_at_us") > decision.at_us:
                return domain + "_unavailable"
            if row.get("price_role") == "reference":
                return "reference_price"
        if row["op"] == "TOMBSTONE":
            return "tombstone"
    return "unknown_" + domain + "_evidence" if unknown else "missing_" + domain


def _session_reasons(session: Row | None, absent_reason: str = "missing_session") -> list[str]:
    if session is None:
        return list(dict.fromkeys(["missing_session", absent_reason]))
    return [
        reason
        for applies, reason in (
            (session["status"] != "open", "session_closed"),
            (_unknown(session), "unknown_session_evidence"),
        )
        if applies
    ]


def _history_cells(
    history: History, rows: History, decision: _Decision, domain: str
) -> list[CoverageCell]:
    records = {str(row["record_id"]): row for row in history}
    projected = {str(row["record_id"]): row for row in rows}
    # One pass: an absent record needs its own revision chain, and rescanning the
    # whole history per record costs records times revisions on a large panel.
    chains: dict[str, list[Row]] = {}
    for row in history:
        chains.setdefault(str(row["record_id"]), []).append(row)
    cells = []
    for record, retained in sorted(
        records.items(),
        key=lambda item: (
            _day(item[1]).toordinal()
            if domain == "session"
            else _integer(item[1], "feature_at_us"),
            item[0],
        ),
    ):
        row = projected.get(record)
        reasons = []
        if row is None:
            reasons.append(_absence(tuple(chains[record]), decision, domain))
        elif domain == "session":
            reasons.extend(_session_reasons(row))
        else:
            if _unknown(row):
                reasons.append("unknown_" + domain + "_evidence")
            if row["value_state"] != "present":
                reasons.append(str(row["value_state"]))
        if domain in _FEATURE_DOMAINS and _integer(retained, "feature_at_us") > decision.at_us:
            reasons = ["future_observation"]
        cells.append(
            CoverageCell(
                str(retained.get("instrument_id", retained.get("calendar_id"))),
                _day(retained) if domain == "session" else None,
                row is not None and row.get("value_state", "present") == "present",
                tuple(reasons),
                record_id=record,
            )
        )
    return cells


def _transform(
    workspace: Workspace, generation: str, budget: ComputeBudget
) -> tuple[str, dict[str, object], ComputeBudget]:
    """Decode one pinned transform and hand back the lease with that body charged.

    The decoded body stays live for every step the caller takes next, so the remaining
    allowance is returned charged rather than the original: what is held is charged
    before the next allocation, not after it.
    """
    catalog = workspace.state.execute(
        "SELECT transform_hash FROM dataset_versions WHERE generation_id=?", (generation,)
    ).fetchone()
    digest = str(catalog[0])
    payload = _raw_payload(workspace, digest, budget)
    body = decode_json(payload)
    if not isinstance(body, dict):
        raise TypeError("pinned transform must be an object")
    return (
        digest,
        body,
        replace(budget, reserved_bytes=budget.reserved_bytes + 32 * len(payload)),
    )


def _raw_payload(workspace: Workspace, digest: str, budget: ComputeBudget) -> bytes:
    _digest(digest)
    # JSON objects can cost much more than encoded bytes; bound before reading.
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        payload = tree.read_bytes(
            digest[:2] + "/" + digest,
            max_bytes=(budget.available_bytes) // 32,
        )
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("pinned transform hash mismatch")
    return payload


def _verify_catalog(workspace: Workspace, marker: Row) -> None:
    catalog = workspace.state.execute(
        "SELECT 1 FROM dataset_versions WHERE status='committed' AND dataset_id=? "
        "AND version=? AND generation_id=? AND chain_hash=? AND manifest_hash=? "
        "AND row_count=? AND parent_generation_id IS ? AND sequence=? AND record_schema=?",
        tuple(
            marker[key]
            for key in (
                "dataset_id",
                "version",
                "generation_id",
                "chain_hash",
                "request_hash",
                "row_count",
                "parent_id",
                "sequence",
                "record_schema",
            )
        ),
    ).fetchone()
    if catalog is None:
        raise ValueError("catalog and market generation disagree")


def _load(workspace: Workspace, pin: GenerationPin, budget: ComputeBudget, domain: str) -> History:
    # Admit before any full materialization. read_dataset performs an unbounded
    # rehash; reuse its catalog/marker contract here after the bounded chain read.
    history = verify_sealed_publication(workspace, pin.generation_id, budget=budget)
    chain = market.generation_chain(workspace.market, pin.generation_id)
    for marker in chain:
        if marker["domain"] != domain or marker["dataset_id"] != pin.dataset_id:
            raise ValueError("pin has incompatible dataset/domain")
    head = chain[-1]
    if (
        any(
            head[key] != getattr(pin, key)
            for key in ("dataset_id", "version", "generation_id", "chain_hash")
        )
        or head["request_hash"] != pin.manifest_hash
    ):
        raise ValueError("exact generation pin does not match catalog/marker")
    return history


def _publication_source(workspace: Workspace, document: ImportDocument) -> None:
    digest = document.sha256
    sources = workspace.state.execute(
        "SELECT count(*),sum(source_snapshot_id=?) FROM dataset_sources "
        "WHERE dataset_id=? AND version=?",
        (document.source_id, document.body["dataset_id"], document.body["version"]),
    ).fetchone()
    files = workspace.state.execute(
        "SELECT count(*),sum(relative_path=? AND byte_hash=? AND size_bytes=?) "
        "FROM source_files WHERE snapshot_id=?",
        (digest[:2] + "/" + digest, digest, len(document.payload), document.source_id),
    ).fetchone()
    source = workspace.state.execute(
        "SELECT 1 FROM source_snapshots WHERE snapshot_id=? AND provider=? "
        "AND publication_at_us IS ? AND status='raw_verified'",
        (document.source_id, document.body["provider"], document.body["publication_at_us"]),
    ).fetchone()
    if tuple(sources) != (1, 1) or tuple(files) != (1, 1) or source is None:
        raise ValueError("publication source lineage conflicts with sealed import")


def _sealed_publication(
    workspace: Workspace, marker: Row, history: History, budget: ComputeBudget
) -> ImportDocument:
    """Authenticate existing v1 commitments; opaque transforms require no retained preimage."""
    document = parse_import(_raw_payload(workspace, str(marker["request_hash"]), budget))
    catalog = workspace.state.execute(
        "SELECT 1 FROM dataset_versions WHERE generation_id=? "
        "AND transform_hash=? AND normalizer_version=?",
        (
            marker["generation_id"],
            document.body["transform_sha256"],
            document.body["normalizer_version"],
        ),
    ).fetchone()
    if catalog is None or any(
        document.body[key] != marker[key]
        for key in ("dataset_id", "version", "generation_id", "operation_id", "parent_id", "domain")
    ):
        raise ValueError("transform/catalog conflicts with publication evidence")
    operation = workspace.state.execute(
        "SELECT created_at_us FROM storage_operations WHERE operation_id=? "
        "AND kind='market_publish' AND phase='COMPLETED' AND request_hash=? "
        "AND payload_hash=? AND target_id=? AND expected_parent IS ?",
        (
            marker["operation_id"],
            document.sha256,
            document.sha256,
            marker["generation_id"],
            marker["parent_id"],
        ),
    ).fetchone()
    if operation is None:
        raise ValueError("publication has no matching completed intent")
    _publication_source(workspace, document)
    with localcontext() as context:
        context.prec = 50
        expected = market.normalize_rows(
            str(marker["domain"]),
            str(marker["generation_id"]),
            [{**row, "ingested_at_us": operation["created_at_us"]} for row in document.rows],
        )
    delta = [row for row in history if row["generation_id"] == marker["generation_id"]]
    if sorted(expected, key=lambda row: str(row["record_id"])) != delta:
        raise ValueError("rows conflict with publication evidence")
    return document


def _admit_publication_chain(
    workspace: Workspace, generation_id: str, budget: ComputeBudget
) -> None:
    """Admit variable-width marker identities before the market owner builds its chain."""
    current: object = generation_id
    seen: set[object] = set()
    estimated = 0
    while current is not None:
        if current in seen:
            raise ValueError("market generation cycle")
        size = workspace.market.execute(
            "SELECT 2048+32*(length(generation_id)+length(dataset_id)+length(version)+"
            "coalesce(length(parent_id),0)+length(domain)+length(record_schema)+"
            "length(operation_id)+length(request_hash)+length(delta_hash)+length(chain_hash)) "
            "FROM market_generations WHERE generation_id=?",
            [current],
        ).fetchone()
        if size is None:
            raise ValueError("market generation does not exist")
        estimated += size[0]
        if estimated > (budget.available_bytes) // 8:
            raise ComputeResourceError("publication chain exceeds materialization budget")
        seen.add(current)
        current = market.marker_for(workspace.market, str(current))["parent_id"]


def verify_sealed_publication(
    workspace: Workspace, generation_id: str, *, budget: ComputeBudget
) -> History:
    """Verify an exact generation and every retained sealed delta, with bounded materialization.

    SELECT-only apart from the market owner's connection resource limits. This proves
    content/intent/catalog/row and opaque transform-hash equality, not an unrecorded
    native registration route, provider authority or execution eligibility.
    """
    _admit_publication_chain(workspace, generation_id, budget)
    history = market.read_chain_rows(workspace.market, generation_id, budget=budget)
    deltas: dict[str, list[Row]] = {}
    for row in history:
        deltas.setdefault(str(row["generation_id"]), []).append(row)
    for marker in market.generation_chain(workspace.market, generation_id):
        _verify_catalog(workspace, marker)
        delta = tuple(deltas.get(str(marker["generation_id"]), ()))
        _sealed_publication(workspace, marker, delta, budget)
    return history


@dataclass(frozen=True, slots=True)
class NativeInputAdmission:
    history: History
    source_pins: tuple[SourcePin, ...]


def admit_native_input(
    workspace: Workspace, pin: GenerationPin, *, expected_schema: str, budget: ComputeBudget
) -> NativeInputAdmission:
    """Require retained native price/session evidence, never fall back to opaque content.

    The caller declares its purpose with aas-price-transform-v1 or
    aas-sessions-transform-v1. Reconstruct every delta through the registration
    owner's parser and retained source table, in addition to sealed integrity.
    Return detached history and unique native SourcePins in first-revision order,
    so consumers need no application-local provenance parser.
    No provider authority, PIT eligibility or executable readiness is conferred.
    """
    domains = {"aas-price-transform-v1": "prices", "aas-sessions-transform-v1": "calendar_sessions"}
    if expected_schema not in domains:
        raise ValueError("unsupported native input transform schema")
    # Halve the allocation for one component while carrying the caller's reserve;
    # a fresh ComputeBudget would silently reset it to zero.
    component = budget.component(2)
    history = _load(workspace, pin, component, domains[expected_schema])
    sources: dict[SourcePin, None] = {}
    for marker in market.generation_chain(workspace.market, pin.generation_id):
        catalog = workspace.state.execute(
            "SELECT transform_hash FROM dataset_versions WHERE generation_id=?",
            (marker["generation_id"],),
        ).fetchone()
        document, source = native_input_document(
            workspace,
            _raw_payload(workspace, catalog[0], component),
            expected_schema=expected_schema,
            budget=component,
        )
        if document.sha256 != marker["request_hash"]:
            raise ValueError("native transform/source conflicts with publication evidence")
        sources[source] = None
    return NativeInputAdmission(history, tuple(sources))


@dataclass(frozen=True, slots=True)
class PinnedSessions:
    pin: GenerationPin
    history: History

    def project_as_of(self, at_us: int, *, mode: ReaderMode = "strict_pit") -> ProjectedInputs:
        decision = _Decision(at_us=at_us, mode=mode)
        rows = _project(self.history, decision)
        cells = _history_cells(self.history, rows, decision, "session")
        reasons = ["catalog_unverified"]
        if mode == "observed_snapshot_research":
            reasons.append("observed_snapshot_research")
        return ProjectedInputs(rows, _coverage(cells, reasons))


def load_pinned_sessions(
    workspace: Workspace, pin: GenerationPin, *, budget: ComputeBudget
) -> PinnedSessions:
    """Read the entire verified calendar chain, including unknown knowledge and closed days."""
    history = _load(workspace, pin, budget, "calendar_sessions")
    for row in history:
        match (row["status"], row["open_at_us"], row["close_at_us"]):
            case ("open", int() as opened, int() as closed) if 0 <= opened < closed:
                pass
            case ("closed", None, None):
                pass
            case _:
                raise ValueError("session requires ordered open/close or closed nulls")
    return PinnedSessions(pin, history)


def _members(
    workspace: Workspace, request: PriceInputRequest, budget: ComputeBudget
) -> tuple[History, History]:
    verified = read_membership_pins(
        workspace.state,
        request.identity_pin,
        request.universe_pin,
        max_materialization_bytes=(budget.available_bytes) // 8,
    )
    return (
        verified.identity.members if verified.identity is not None else (),
        verified.universe.members if verified.universe is not None else (),
    )


def _active(members: History, instrument: str, economic_us: int, decision_us: int) -> bool:
    return any(
        row["instrument_id"] == instrument
        and _integer(row, "valid_from_us") <= economic_us
        and (row["valid_to_us"] is None or economic_us < _integer(row, "valid_to_us"))
        and _integer(row, "known_from_us") <= decision_us
        and (row["known_to_us"] is None or decision_us < _integer(row, "known_to_us"))
        for row in members
    )


@dataclass(frozen=True, slots=True)
class PinnedPriceSeries:
    """Complete immutable price/calendar/membership history; no stored latest-head cache."""

    request: PriceInputRequest
    history: History
    sessions: PinnedSessions | None
    identities: History
    universe: History

    def project_as_of(
        self, at_us: int, *, session_date: date, ingestion_cutoff_us: int | None = None
    ) -> ProjectedInputs:
        decision = _Decision(
            at_us=at_us,
            mode=self.request.mode,
            session_date=session_date,
            ingestion_cutoff_us=ingestion_cutoff_us,
        )
        rows = _project(self.history, decision)
        sessions = _project(self.sessions.history, decision) if self.sessions is not None else ()
        selected: list[Row] = []
        cells = []
        reasons = ["catalog_unverified"]
        if self.request.identity_pin is None:
            reasons.append("identity_unpinned")
        if self.request.universe_pin is None:
            reasons.append("universe_unpinned")
        if decision.mode == "observed_snapshot_research":
            reasons.append("observed_snapshot_research")
        for day in self.request.session_dates:
            session = next((row for row in sessions if _day(row) == day), None)
            for instrument in self.request.instrument_ids:
                price = next(
                    (
                        row
                        for row in rows
                        if row["instrument_id"] == instrument and _day(row) == day
                    ),
                    None,
                )
                cell_reasons = self._cell_reasons(instrument, day, price, session, decision)
                present = (
                    price is not None
                    and price["value_state"] == "present"
                    and day <= session_date
                    and _integer(price, "bar_end_us") <= at_us
                )
                if (
                    price is not None
                    and present
                    and not {"identity_unavailable", "outside_universe"}.intersection(cell_reasons)
                ):
                    selected.append(price)
                cells.append(CoverageCell(instrument, day, present, tuple(cell_reasons)))
        return ProjectedInputs(tuple(selected), _coverage(cells, reasons))

    def _cell_reasons(
        self,
        instrument: str,
        day: date,
        price: Row | None,
        session: Row | None,
        decision: _Decision,
    ) -> list[str]:
        session_history = self.sessions.history if self.sessions is not None else ()
        reasons = _session_reasons(
            session,
            _absence(
                tuple(row for row in session_history if _day(row) == day),
                decision,
                "session",
            ),
        )
        if price is None:
            candidates = tuple(
                row
                for row in self.history
                if row["instrument_id"] == instrument and _day(row) == day
            )
            reasons.append(_absence(candidates, decision, "price"))
        else:
            if _unknown(price):
                reasons.append("unknown_price_evidence")
            if price["value_state"] != "present":
                reasons.append(str(price["value_state"]))
            if _integer(price, "bar_end_us") > decision.at_us:
                reasons.append("future_observation")
            if self.request.identity_pin is not None and not _active(
                self.identities, instrument, _integer(price, "bar_end_us"), decision.at_us
            ):
                reasons.append("identity_unavailable")
            if self.request.universe_pin is not None and not _active(
                self.universe, instrument, _integer(price, "bar_end_us"), decision.at_us
            ):
                reasons.append("outside_universe")
        if price is None or price["open"] is None:
            reasons.append("missing_sell_open")
        if decision.session_date is not None and day > decision.session_date:
            reasons.append("future_session")
        return reasons


def load_pinned_prices(
    workspace: Workspace, request: PriceInputRequest, *, budget: ComputeBudget
) -> PinnedPriceSeries:
    """Prepare full histories. Budget is shared across prices, sessions and grid/memberships.

    Half the allocation goes to prices, one quarter to sessions; the remaining
    materialization share covers membership rows and the requested coverage grid.
    Tiny budgets unable to admit component hash workers fail explicitly.
    No provider/identity/universe/authority certification is inferred from hashes.
    """
    cells = len(request.instrument_ids) * len(request.session_dates)
    if cells * 2048 > (budget.available_bytes) // 8:
        raise ComputeResourceError(
            "requested coverage grid exceeds admitted materialization budget"
        )
    history = admit_native_input(
        workspace,
        request.pin,
        expected_schema="aas-price-transform-v1",
        budget=budget.component(2),
    ).history
    sessions = (
        load_pinned_sessions(
            workspace,
            request.sessions_pin,
            budget=budget.component(4),
        )
        if request.sessions_pin
        else None
    )
    for instrument in request.instrument_ids:
        identity = workspace.state.execute(
            "SELECT asset_type, venue FROM instruments WHERE instrument_id=?", (instrument,)
        ).fetchone()
        if (
            identity is None
            or identity["venue"] != request.venue
            or identity["asset_type"] == "proxy"
        ):
            raise ValueError("unknown or incompatible instrument identity/venue")
    for row in history:
        if row["instrument_id"] in request.instrument_ids and any(
            row[key] != getattr(request, key)
            for key in ("currency", "basis", "price_role", "interval")
        ):
            raise ValueError("price currency/basis/role/interval conflicts with request")
    natural = {
        (row["instrument_id"], row["session_date"], row["record_id"])
        for row in history
        if row["instrument_id"] in request.instrument_ids
    }
    if len({item[:2] for item in natural}) != len(natural):
        raise ValueError("duplicate daily price identity")
    if sessions is not None and any(
        row[key] != getattr(request, key)
        for row in sessions.history
        for key in ("calendar_id", "venue", "timezone_version")
    ):
        raise ValueError("session calendar/venue/timezone conflicts with request")
    for marker in market.generation_chain(workspace.market, request.pin.generation_id):
        # One transform is decoded at a time and read immediately, so the charged lease
        # the decoder returns is not needed for a nested step here.
        _, transform, _ = _transform(workspace, str(marker["generation_id"]), budget)
        calendar = transform.get("calendar")
        if not isinstance(calendar, dict) or any(
            calendar.get(key) != getattr(request, key)
            for key in ("calendar_id", "timezone_version")
        ):
            raise ValueError("price transform calendar conflicts with request")
    identities, universe = _members(workspace, request, budget)
    return PinnedPriceSeries(request, history, sessions, identities, universe)


@dataclass(frozen=True, slots=True)
class PinnedProxySeries:
    """Stored binary64 points, not recalibrated or spliced using future levels."""

    pin: GenerationPin
    history: History
    definition: str
    non_executable: bool = field(default=True, init=False)

    def project_as_of(self, at_us: int, *, mode: ReaderMode = "strict_pit") -> ProjectedInputs:
        decision = _Decision(at_us=at_us, mode=mode)
        rows = tuple(
            row
            for row in _project(self.history, decision)
            if _integer(row, "feature_at_us") <= at_us
        )
        cells = _history_cells(self.history, rows, decision, "proxy")
        reasons = ["catalog_unverified", "proxy_non_executable", "conventions_unverified"]
        if mode == "observed_snapshot_research":
            reasons.append("observed_snapshot_research")
        return ProjectedInputs(rows, _coverage(cells, reasons))


def load_pinned_proxy(
    workspace: Workspace, pin: GenerationPin, *, budget: ComputeBudget
) -> PinnedProxySeries:
    """Verify a single external proxy definition against every retained feature revision."""
    history = _load(
        workspace,
        pin,
        budget.component(2),
        "feature_values",
    )
    definition = verify_proxy_content(workspace, history, budget=budget)
    return PinnedProxySeries(pin, history, definition)


def _feature_publication_document(
    workspace: Workspace,
    transform_hash: str,
    transform: dict[str, object],
    budget: ComputeBudget,
    *,
    label: str = "proxy",
) -> tuple[GenerationPin, ImportDocument]:
    """Authenticate the transform pointer with the independently sealed import bytes."""
    destination = transform.get("dataset")
    if not isinstance(destination, dict):
        raise TypeError(label + " transform lacks publication identity")
    catalog = workspace.state.execute(
        "SELECT * FROM dataset_versions WHERE generation_id=? AND status='committed'",
        (destination.get("generation_id"),),
    ).fetchone()
    if catalog is None or catalog["transform_hash"] != transform_hash:
        raise ValueError(label + " transform/catalog mismatch")
    pin = GenerationPin(
        *(
            catalog[key]
            for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
        )
    )
    marker = market.marker_for(workspace.market, pin.generation_id)
    document = parse_import(_raw_payload(workspace, pin.manifest_hash, budget))
    if (
        document.sha256 != pin.manifest_hash
        or destination
        != {
            key: marker[key]
            for key in ("dataset_id", "version", "generation_id", "operation_id", "parent_id")
        }
        or any(document.body[key] != value for key, value in destination.items())
        or document.body["domain"] != "feature_values"
        or document.body["transform_sha256"] != transform_hash
        or document.body["normalizer_version"] != catalog["normalizer_version"]
        or any(
            document.body.get(key) != transform.get(key)
            for key in ("provider", "publication_at_us", "normalizer_version", "instruments")
        )
    ):
        raise ValueError(label + " transform conflicts with publication evidence")
    return pin, document


def _proxy_publication(
    workspace: Workspace,
    transform_hash: str,
    transform: dict[str, object],
    budget: ComputeBudget,
    *,
    label: str = "proxy",
) -> History:
    """Authenticate the pointer, then reload the referencing delta for the proxy reader."""
    pin, document = _feature_publication_document(
        workspace, transform_hash, transform, budget, label=label
    )
    return _proxy_publication_delta(workspace, pin, document, budget)


def _proxy_publication_delta(
    workspace: Workspace, pin: GenerationPin, document: ImportDocument, budget: ComputeBudget
) -> History:
    """Match the whole referencing delta to its sealed import, before selecting rows."""
    history = _load(
        workspace,
        pin,
        budget.component(2),
        "feature_values",
    )
    if document.sha256 != pin.manifest_hash:
        raise ValueError("proxy catalog conflicts with publication evidence")
    return tuple(row for row in history if row["generation_id"] == pin.generation_id)


def _feature_contracts(
    workspace: Workspace, record_schema: str, budget: ComputeBudget
) -> tuple[set[tuple[str, str]], int]:
    """Admit one contract-identity set before fetching it, and report what it holds live.

    feature_contracts.name and .version are TEXT with no length bound, so the set is
    sized from stored bytes and admitted before it is materialized, not after.
    """
    stored = workspace.state.execute(
        "SELECT coalesce(sum(64 + length(CAST(name AS BLOB)) + length(CAST(version AS BLOB))),0) "
        "FROM feature_contracts WHERE record_schema=?",
        (record_schema,),
    ).fetchone()
    live = 32 * int(stored[0])
    if live > budget.available_bytes:
        raise ComputeResourceError("feature contract identities exceed admitted materialization")
    return (
        {
            (row[0], row[1])
            for row in workspace.state.execute(
                "SELECT name,version FROM feature_contracts WHERE record_schema=?",
                (record_schema,),
            )
        },
        live,
    )


def verify_feature_publications(workspace: Workspace, *, budget: ComputeBudget) -> None:
    """Classify and verify every committed feature_values publication from its own seal.

    Discovery starts at the publications, not at the contract table. Scanning contracts
    first and returning when there are none lets a lost SQLite contract hide a committed
    DuckDB generation: integrity checks still pass while the generation can no longer be
    authenticated by the reader that pinned it.

    Classification reads the retained transform schema each publication declared in its
    sealed import. That document's SHA-256 is the marker request hash the generation
    chain covers, so the declaration cannot be edited without breaking the chain. A
    generic offline import declares nothing and commits an opaque transform_sha256 whose
    preimage it never retained, so its commitment is left closed rather than opened as a
    native transform.

    One pass serves both routes because both read the same sealed documents, and each
    dataset's chain is loaded once at its committed head.

    Everything the pass holds live is charged before the step that would allocate next:
    both identity sets, the chain a verified dataset leaves cached, the sealed document,
    and a proxy delta while its definitions are verified.
    """
    proxies, proxy_bytes = _feature_contracts(workspace, "aas-market-rowset-v1", budget)
    observations, observation_bytes = _feature_contracts(
        workspace, OBSERVATION_DEFINITION_SCHEMA, budget
    )
    # Both sets, and the kept subsets built from them, stay live for the whole scan.
    scan = replace(
        budget, reserved_bytes=budget.reserved_bytes + 2 * (proxy_bytes + observation_bytes)
    )
    kept_proxies: set[tuple[str, str]] = set()
    kept_observations: set[tuple[str, str]] = set()
    chains: dict[str, tuple[dict[str, History], int]] = {}
    for catalog in workspace.state.execute(
        "SELECT dataset_id,version,generation_id,chain_hash,manifest_hash "
        "FROM dataset_versions WHERE status='committed' ORDER BY dataset_id, sequence"
    ):
        marker = market.marker_for(workspace.market, str(catalog["generation_id"]))
        if marker["domain"] != "feature_values":
            continue
        # A verified dataset's grouped chain stays cached until the scan reaches another
        # dataset, so it is charged against every later step rather than only the delta
        # it was loaded for.
        lease = replace(
            scan, reserved_bytes=scan.reserved_bytes + sum(live for _, live in chains.values())
        )
        # The marker's original request is independent of both live row fields
        # and catalog transform pointers. _load below also checks the manifest.
        payload = _raw_payload(workspace, str(marker["request_hash"]), lease)
        document = parse_import(payload)
        # The parsed document stays live for every step below, so what is handed down is
        # the remaining allowance rather than the caller's whole lease.
        held = replace(lease, reserved_bytes=lease.reserved_bytes + 32 * len(payload))
        if document.body.get("transform_schema") == OBSERVATION_TRANSFORM_SCHEMA:
            # Classified before any pin is built: a historical generic publication can
            # carry the literal catalog version "latest", which GenerationPin refuses,
            # and an observation destination never does.
            kept_observations.add(
                _verify_observation_publication(
                    workspace, GenerationPin(*catalog), observations, chains, held
                )
            )
            continue
        referenced = proxies.intersection(
            (row["contract_id"], row["contract_version"]) for row in document.rows
        )
        if not referenced:
            continue
        pin = GenerationPin(*catalog)
        delta = _proxy_publication_delta(workspace, pin, document, held)
        # The whole delta stays live while each referencing definition is verified.
        charged = replace(held, reserved_bytes=held.reserved_bytes + _retained_bytes(delta))
        for identity in sorted(referenced):
            rows = tuple(
                row for row in delta if (row["contract_id"], row["contract_version"]) == identity
            )
            verify_proxy_content(workspace, rows, budget=charged)
        kept_proxies.update(referenced)
    if proxies - kept_proxies:
        raise ValueError("proxy definition has no retained feature generation")
    if observations - kept_observations:
        raise ValueError("observation definition has no retained feature generation")


def _verify_proxy_catalog(
    workspace: Workspace, generation: str, transform_hash: str, *, label: str = "proxy"
) -> None:
    _verify_catalog(workspace, market.marker_for(workspace.market, generation))
    catalog = workspace.state.execute(
        "SELECT transform_hash FROM dataset_versions WHERE generation_id=?", (generation,)
    ).fetchone()
    if catalog[0] != transform_hash:
        raise ValueError(label + " transform/catalog mismatch")


def verify_proxy_content(workspace: Workspace, history: History, *, budget: ComputeBudget) -> str:
    """Verify one definition, its publication and the supplied referencing revisions.

    Integrity callers supply only that definition's rows, even in mixed histories.
    The specialized reader supplies its entire pinned history, retaining its
    single-definition compatibility restriction. Neither path changes stored data.
    """
    first = history[0]
    size = workspace.state.execute(
        "SELECT length(CAST(definition AS BLOB)) FROM feature_contracts WHERE name=? AND version=?",
        (first["contract_id"], first["contract_version"]),
    ).fetchone()
    if size is not None and size[0] * 256 > budget.available_bytes:
        raise ComputeResourceError("proxy contract exceeds admitted materialization budget")
    contract = workspace.state.execute(
        "SELECT definition, content_hash, record_schema FROM feature_contracts "
        "WHERE name=? AND version=?",
        (first["contract_id"], first["contract_version"]),
    ).fetchone()
    if (
        contract is None
        or contract["record_schema"] != "aas-market-rowset-v1"
        or hashlib.sha256(contract["definition"].encode()).hexdigest() != contract["content_hash"]
    ):
        raise ValueError("proxy feature contract is absent or corrupt")
    definition = decode_json(contract["definition"].encode())
    if (
        not isinstance(definition, dict)
        or canonical_json_bytes(definition).decode() != contract["definition"]
        or definition.get("proxy_id") != first["contract_id"]
        or definition.get("version") != first["contract_version"]
    ):
        raise ValueError("proxy feature contract identity mismatch")
    registered = workspace.state.execute(
        "SELECT ordinal, ref_kind, ref_id, ref_version, content_hash FROM feature_inputs "
        "WHERE name=? AND version=? ORDER BY ordinal",
        (first["contract_id"], first["contract_version"]),
    ).fetchall()
    if len(registered) != len(_PROXY_REFS) + 1:
        raise ValueError("proxy feature inputs mismatch")
    transform_hash = registered[-1]["ref_id"]
    transition = definition["transition"]
    bundle = [transition[key] for key in _PROXY_REFS]
    digest = hashlib.sha256(canonical_json_bytes(bundle)).hexdigest()
    inputs = [
        (
            key + ":" + source["table"],
            source["source_id"],
            source["source_sha256"],
            source["table_digest"],
        )
        for key in ("donor_source", "target_source")
        for source in (transition[key],)
    ]
    inputs.extend(
        (key, ref["id"], ref["version"], ref["sha256"])
        for key in ("basis_ref", "calendar_ref", "cost_ref")
        for ref in (transition[key],)
    )
    inputs.append(("transform", transform_hash, "aas-proxy-transform-v1", transform_hash))
    if [tuple(row) for row in registered] != [
        (ordinal, *item) for ordinal, item in enumerate(inputs)
    ]:
        raise ValueError("proxy feature inputs mismatch")
    payload = _raw_payload(workspace, transform_hash, budget)
    transform = decode_json(payload)
    if (
        not isinstance(transform, dict)
        or transform.get("proxy") != definition
        or transform.get("schema_version") != "aas-proxy-transform-v1"
    ):
        raise ValueError("proxy transform conflicts with feature contract")
    # The decoded transform stays live while the publication is rebuilt from it.
    delta = _proxy_publication(
        workspace,
        transform_hash,
        transform,
        replace(budget, reserved_bytes=budget.reserved_bytes + 32 * len(payload)),
    )
    expected = {
        "contract_id": definition["proxy_id"],
        "contract_version": definition["version"],
        "contract_hash": contract["content_hash"],
        "input_bundle_hash": digest,
        "instrument_id": transition["logical_exposure_id"],
    }
    if any(row[key] != value for row in (*delta, *history) for key, value in expected.items()):
        raise ValueError("proxy points conflict with pinned definition/inputs")
    for generation in dict.fromkeys(str(row["generation_id"]) for row in history):
        _verify_proxy_catalog(workspace, generation, transform_hash)
    identity = workspace.state.execute(
        "SELECT asset_type FROM instruments WHERE instrument_id=?",
        (transition["logical_exposure_id"],),
    ).fetchone()
    if identity is None or identity[0] != "proxy":
        raise ValueError("proxy requires a non-executable logical exposure identity")
    return contract["definition"]


@dataclass(frozen=True, slots=True)
class PinnedObservationSeries:
    """Stored binary64 observed prices; uncertified research, never executable.

    An observation contract fixes price_role to reference, so strict PIT selects
    none of it no matter how complete its knowledge times are. feature_values rows
    carry no price_role column, so the exclusion the prices reader gets from
    project_heads has to be applied here instead of inferred from a row.
    """

    pin: GenerationPin
    history: History
    definition: str
    non_executable: bool = field(default=True, init=False)
    certified: bool = field(default=False, init=False)

    def project_as_of(self, at_us: int, *, mode: ReaderMode = "strict_pit") -> ProjectedInputs:
        decision = _Decision(at_us=at_us, mode=mode)
        reasons = [
            "catalog_unverified",
            "observation_non_executable",
            "observation_uncertified",
            "calendar_unverified",
        ]
        if mode != "observed_snapshot_research":
            return ProjectedInputs((), _coverage(_reference_cells(self.history), reasons))
        rows = tuple(
            row
            for row in _project(self.history, decision)
            if _integer(row, "feature_at_us") <= at_us
        )
        cells = _history_cells(self.history, rows, decision, "observation")
        reasons.append("observed_snapshot_research")
        return ProjectedInputs(rows, _coverage(cells, reasons))


def load_pinned_observations(
    workspace: Workspace, pin: GenerationPin, *, budget: ComputeBudget
) -> PinnedObservationSeries:
    """Verify one observation definition against every retained feature revision.

    The loaded history stays live for the caller, so it is charged as retained
    state before verification runs. Handing the verifier the whole allowance again
    would let it materialize bytes this history is still holding, which decision
    0016 bounds as charge plus live. Too little left raises ComputeResourceError
    rather than letting the process meet memory pressure instead.
    """
    history = _load(workspace, pin, budget.component(2), "feature_values")
    definition = verify_observation_content(
        workspace,
        history,
        budget=replace(budget, reserved_bytes=budget.reserved_bytes + _retained_bytes(history)),
    )
    return PinnedObservationSeries(pin, history, definition)


def _retained_bytes(history: History) -> int:
    """Estimate what a loaded feature history holds live, as the chain admission does.

    Mirrors market's chain estimate term for term: a fixed base, 1024 per
    contributing generation, and the per-row and per-character terms. The
    per-generation term is not optional here, because the grouped index this reader
    keeps beside the rows also costs one entry per generation, and omitting it
    undercharges a panel published as many bounded chunks by exactly that much.
    """
    schema = COMMON + DOMAINS["feature_values"]
    generations = len({str(row["generation_id"]) for row in history})
    characters = sum(
        len(value) for row in history for value in row.values() if isinstance(value, str)
    )
    return (
        64 * 1024 + 1024 * generations + len(history) * (1024 + 256 * len(schema)) + 32 * characters
    )


def _verify_observation_publication(
    workspace: Workspace,
    pin: GenerationPin,
    contracts: set[tuple[str, str]],
    chains: dict[str, tuple[dict[str, History], int]],
    budget: ComputeBudget,
) -> tuple[str, str]:
    """Authenticate one declared observation publication and return its contract identity.

    The publication declared the retained transform it was built from, so that transform
    is opened here and has to agree: a document cannot claim this route and carry a
    transform from another one. An observation publication whose contract has since been
    lost fails here rather than disappearing from an empty contract scan.
    """
    _, transform, held = _transform(workspace, pin.generation_id, budget)
    if transform.get("schema_version") != OBSERVATION_TRANSFORM_SCHEMA:
        raise ValueError("observation publication declares a transform it was not built from")
    definition = transform.get("observation")
    if not isinstance(definition, dict):
        raise TypeError("observation publication has no usable definition")
    identity = (
        str(definition.get("series_id")) + "/" + str(definition.get("observation_role")),
        str(definition.get("version")),
    )
    if identity not in contracts:
        raise ValueError("observation publication has no registered contract")
    chain, live_bytes = _observation_chain(workspace, pin, chains, held)
    # The cached chain stays live while the delta is re-derived, so it is charged here
    # exactly as the reader charges the history it returns.
    reserved = replace(held, reserved_bytes=held.reserved_bytes + live_bytes)
    rows = tuple(
        row
        for row in chain.get(pin.generation_id, ())
        if (row["contract_id"], row["contract_version"]) == identity
    )
    verify_observation_content(workspace, rows, budget=reserved)
    return identity


def _observation_chain(
    workspace: Workspace,
    pin: GenerationPin,
    chains: dict[str, tuple[dict[str, History], int]],
    budget: ComputeBudget,
) -> tuple[dict[str, History], int]:
    """Load and verify each dataset's chain once, grouped, retaining only the current one.

    The caller owns the materialization allowance, so keeping every dataset's
    history alive would multiply that allowance by the number of datasets even
    though each chain fits on its own. The catalog is walked in dataset order, so
    holding a single chain still costs one load per dataset.

    Rows are grouped by generation once here: the caller looks up one generation per
    catalog row, and rescanning the whole chain each time would cost generations
    times rows.
    """
    cached = chains.get(pin.dataset_id)
    if cached is None:
        chains.clear()
        head = workspace.state.execute(
            "SELECT dataset_id,version,generation_id,chain_hash,manifest_hash "
            "FROM dataset_versions WHERE status='committed' AND dataset_id=? "
            "ORDER BY sequence DESC LIMIT 1",
            (pin.dataset_id,),
        ).fetchone()
        history = _load(workspace, GenerationPin(*head), budget.component(2), "feature_values")
        # The loaded history stays live for the whole walk below, so each transform is
        # read and decoded on what is left rather than on the lease that loaded it.
        walk = replace(budget, reserved_bytes=budget.reserved_bytes + _retained_bytes(history))
        # A chain this route owns must be observations end to end. Another writer can
        # append to an observation head because the domain matches, and each delta
        # then verifies alone while both readers reject the mixed head. The transform
        # must also name this marker, because a hash can be reused from an ancestor.
        for marker in market.generation_chain(workspace.market, str(head["generation_id"])):
            _, transform, _ = _transform(workspace, str(marker["generation_id"]), walk)
            destination = transform.get("dataset")
            if (
                transform.get("schema_version") != OBSERVATION_TRANSFORM_SCHEMA
                or not isinstance(destination, dict)
                or destination.get("generation_id") != marker["generation_id"]
            ):
                raise ValueError("observation chain contains a generation from another route")
        collected: dict[str, list[Row]] = {}
        for row in history:
            collected.setdefault(str(row["generation_id"]), []).append(row)
        grouped = {key: tuple(value) for key, value in collected.items()}
        cached = (grouped, _retained_bytes(history))
        chains[pin.dataset_id] = cached
    return cached


def _reference_cells(history: History) -> list[CoverageCell]:
    """Account for every retained record while selecting none of it under strict PIT."""
    records = {str(row["record_id"]): row for row in history}
    absent = False
    return [
        CoverageCell(
            str(row["instrument_id"]), None, absent, ("reference_observation",), record_id=record
        )
        for record, row in sorted(records.items(), key=lambda item: item[0])
    ]


def _observation_generations(
    workspace: Workspace,
    history: History,
    definition: dict[str, object],
    expected: dict[str, object],
    budget: ComputeBudget,
) -> None:
    """Re-derive every contributing generation from its pinned source and match it.

    The caller has already verified the whole chain once, so each delta is selected
    from that history rather than reloaded. Reloading per generation would replay
    every chain prefix, and rescanning the history per generation would cost
    generations times rows, so the rows are grouped by generation in one pass.

    Matching metadata and a row count is not enough. A transform hash addresses
    immutable bytes that a failed registration can leave behind, and the generic
    import route accepts an existing hash as its pointer, so a publication can
    reference a real observation transform while carrying values that transform
    never produced. Each delta is therefore rebuilt from the pinned source through
    the registration parser and compared by publication bytes.
    """
    grouped: dict[str, list[Row]] = {}
    for row in history:
        grouped.setdefault(str(row["generation_id"]), []).append(row)
    # Every generation of one panel shares the contract's upstream pin, and resolving
    # it recomputes that table's digest, so it is checked once for the whole pass.
    resolved: set[SourcePin] = set()
    for generation, rows in grouped.items():
        transform_hash, transform, held = _transform(workspace, generation, budget)
        destination = transform.get("dataset")
        if (
            transform.get("observation") != definition
            or transform.get("schema_version") != OBSERVATION_TRANSFORM_SCHEMA
            or not isinstance(destination, dict)
            or destination.get("generation_id") != generation
        ):
            raise ValueError("observation transform conflicts with feature contract")
        # Authenticate the pointer, then let that parsed document go before the
        # re-derivation builds another one: the lease reserves the cached chain, not
        # two full documents alive at the same time. The decoded transform itself stays
        # live for both, so both run on the lease that already charges it.
        _feature_publication_document(
            workspace, transform_hash, transform, held, label="observation"
        )
        marker = market.marker_for(workspace.market, generation)
        derived, _ = native_input_document(
            workspace,
            _raw_payload(workspace, transform_hash, held),
            expected_schema=OBSERVATION_TRANSFORM_SCHEMA,
            budget=held,
            resolved=resolved,
        )
        if derived.sha256 != marker["request_hash"]:
            raise ValueError("observation delta was not derived from its pinned source")
        delta = tuple(rows)
        if any(row[key] != value for row in delta for key, value in expected.items()):
            raise ValueError("observation points conflict with pinned definition/inputs")
        _verify_proxy_catalog(workspace, generation, transform_hash, label="observation")


def _observation_identities(workspace: Workspace, history: History) -> None:
    """Every observed instrument must exist and must not be a research return proxy."""
    for instrument in dict.fromkeys(str(row["instrument_id"]) for row in history):
        identity = workspace.state.execute(
            "SELECT asset_type FROM instruments WHERE instrument_id=?", (instrument,)
        ).fetchone()
        if identity is None or identity[0] == "proxy":
            raise ValueError("observation requires a non-proxy observed instrument identity")


def verify_observation_content(
    workspace: Workspace, history: History, *, budget: ComputeBudget
) -> str:
    """Verify one observation definition, its publication and the supplied revisions.

    SELECT-only. This proves contract, transform, publication and row agreement for
    an explicitly uncertified observed research series. It grants no PIT eligibility,
    provider authority or execution readiness, and never promotes the series to an
    executable price. calendar_ref stays a preserved reference, never a resolved
    calendar, so nothing here infers a session, a knowledge time or a publication time.
    """
    first = history[0]
    size = workspace.state.execute(
        "SELECT length(CAST(definition AS BLOB)) FROM feature_contracts WHERE name=? AND version=?",
        (first["contract_id"], first["contract_version"]),
    ).fetchone()
    if size is not None and size[0] * 256 > budget.available_bytes:
        raise ComputeResourceError("observation contract exceeds admitted materialization budget")
    contract = workspace.state.execute(
        "SELECT definition, content_hash, record_schema FROM feature_contracts "
        "WHERE name=? AND version=?",
        (first["contract_id"], first["contract_version"]),
    ).fetchone()
    if (
        contract is None
        or contract["record_schema"] != OBSERVATION_DEFINITION_SCHEMA
        or hashlib.sha256(contract["definition"].encode()).hexdigest() != contract["content_hash"]
    ):
        raise ValueError("observation feature contract is absent or corrupt")
    definition = decode_json(contract["definition"].encode())
    if (
        not isinstance(definition, dict)
        or canonical_json_bytes(definition).decode() != contract["definition"]
    ):
        raise ValueError("observation feature contract identity mismatch")
    identity = str(definition.get("series_id")) + "/" + str(definition.get("observation_role"))
    if identity != first["contract_id"] or definition.get("version") != first["contract_version"]:
        raise ValueError("observation feature contract identity mismatch")
    registered = workspace.state.execute(
        "SELECT ordinal, ref_kind, ref_id, ref_version, content_hash FROM feature_inputs "
        "WHERE name=? AND version=? ORDER BY ordinal",
        (first["contract_id"], first["contract_version"]),
    ).fetchall()
    if len(registered) != len(_OBSERVATION_REFS):
        raise ValueError("observation feature inputs mismatch")
    source = definition["observed_source"]
    reference = definition["calendar_ref"]
    inputs = [
        (
            "observed_source:" + source["table"],
            source["source_id"],
            source["source_sha256"],
            source["table_digest"],
        ),
        ("calendar_ref", reference["id"], reference["version"], reference["sha256"]),
    ]
    if [tuple(row) for row in registered] != [
        (ordinal, *item) for ordinal, item in enumerate(inputs)
    ]:
        raise ValueError("observation feature inputs mismatch")
    expected = {
        "contract_id": identity,
        "contract_version": definition["version"],
        "contract_hash": contract["content_hash"],
        "input_bundle_hash": hashlib.sha256(canonical_json_bytes([source, reference])).hexdigest(),
    }
    if any(row[key] != value for row in history for key, value in expected.items()):
        raise ValueError("observation points conflict with pinned definition/inputs")
    # The decoded definition stays live while every generation is re-derived, so it
    # is charged rather than spent twice on the same lease.
    _observation_generations(
        workspace,
        history,
        definition,
        expected,
        replace(budget, reserved_bytes=budget.reserved_bytes + 256 * len(contract["definition"])),
    )
    _observation_identities(workspace, history)
    return contract["definition"]
