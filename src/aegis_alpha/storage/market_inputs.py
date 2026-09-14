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
from dataclasses import dataclass, field
from datetime import date
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, assert_never

from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.storage import market
from aegis_alpha.storage.import_document import ImportDocument, parse_import
from aegis_alpha.storage.membership_pins import (
    IdentityPin,
    UniversePin,
    read_membership_pins,
)

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

type Row = Mapping[str, object]
type History = tuple[Row, ...]
type ReaderMode = Literal["strict_pit", "observed_snapshot_research"]

_PROXY_REFS = ("donor_source", "target_source", "basis_ref", "calendar_ref", "cost_ref")


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
            chain = tuple(item for item in history if item["record_id"] == record)
            reasons.append(_absence(chain, decision, domain))
        elif domain == "session":
            reasons.extend(_session_reasons(row))
        else:
            if _unknown(row):
                reasons.append("unknown_proxy_evidence")
            if row["value_state"] != "present":
                reasons.append(str(row["value_state"]))
        if domain == "proxy" and _integer(retained, "feature_at_us") > decision.at_us:
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
) -> tuple[str, dict[str, object]]:
    catalog = workspace.state.execute(
        "SELECT transform_hash FROM dataset_versions WHERE generation_id=?", (generation,)
    ).fetchone()
    digest = str(catalog[0])
    body = decode_json(_raw_payload(workspace, digest, budget))
    if not isinstance(body, dict):
        raise TypeError("pinned transform must be an object")
    return digest, body


def _raw_payload(workspace: Workspace, digest: str, budget: ComputeBudget) -> bytes:
    _digest(digest)
    # JSON objects can cost much more than encoded bytes; bound before reading.
    with DescriptorTree.open_path(workspace.paths.raw) as tree:
        payload = tree.read_bytes(
            digest[:2] + "/" + digest,
            max_bytes=(budget.memory_limit_bytes - budget.duckdb_memory_limit_bytes) // 256,
        )
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError("pinned transform hash mismatch")
    return payload


def _verify_catalog(workspace: Workspace, marker: Row) -> None:
    catalog = workspace.state.execute(
        "SELECT * FROM dataset_versions WHERE generation_id=? AND status='committed'",
        (marker["generation_id"],),
    ).fetchone()
    pairs = {
        "dataset_id": "dataset_id",
        "version": "version",
        "generation_id": "generation_id",
        "chain_hash": "chain_hash",
        "manifest_hash": "request_hash",
        "row_count": "row_count",
        "parent_generation_id": "parent_id",
        "sequence": "sequence",
        "record_schema": "record_schema",
    }
    if catalog is None or any(catalog[left] != marker[right] for left, right in pairs.items()):
        raise ValueError("catalog and market generation disagree")


def _load(workspace: Workspace, pin: GenerationPin, budget: ComputeBudget, domain: str) -> History:
    # Admit before any full materialization. read_dataset performs an unbounded
    # rehash; reuse its catalog/marker contract here after the bounded chain read.
    history = market.read_chain_rows(workspace.market, pin.generation_id, budget=budget)
    chain = market.generation_chain(workspace.market, pin.generation_id)
    for marker in chain:
        _verify_catalog(workspace, marker)
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
        max_materialization_bytes=(budget.memory_limit_bytes - budget.duckdb_memory_limit_bytes)
        // 8,
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
    if cells * 2048 > (budget.memory_limit_bytes - budget.duckdb_memory_limit_bytes) // 8:
        raise ComputeResourceError(
            "requested coverage grid exceeds admitted materialization budget"
        )
    history = _load(
        workspace,
        request.pin,
        ComputeBudget(budget.cpu_limit, budget.memory_limit_bytes // 2),
        "prices",
    )
    sessions = (
        load_pinned_sessions(
            workspace,
            request.sessions_pin,
            budget=ComputeBudget(budget.cpu_limit, budget.memory_limit_bytes // 4),
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
        _, transform = _transform(workspace, str(marker["generation_id"]), budget)
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
        ComputeBudget(budget.cpu_limit, budget.memory_limit_bytes // 2),
        "feature_values",
    )
    definition = verify_proxy_content(workspace, history, budget=budget)
    return PinnedProxySeries(pin, history, definition)


def _proxy_publication(
    workspace: Workspace, transform_hash: str, transform: dict[str, object], budget: ComputeBudget
) -> History:
    """Authenticate the transform pointer with the independently sealed import bytes."""
    destination = transform.get("dataset")
    if not isinstance(destination, dict):
        raise TypeError("proxy transform lacks publication identity")
    catalog = workspace.state.execute(
        "SELECT * FROM dataset_versions WHERE generation_id=? AND status='committed'",
        (destination.get("generation_id"),),
    ).fetchone()
    if catalog is None or catalog["transform_hash"] != transform_hash:
        raise ValueError("proxy transform/catalog mismatch")
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
        raise ValueError("proxy transform conflicts with publication evidence")
    return _proxy_publication_delta(workspace, pin, document, budget)


def _proxy_publication_delta(
    workspace: Workspace, pin: GenerationPin, document: ImportDocument, budget: ComputeBudget
) -> History:
    """Match the whole referencing delta to its sealed import, before selecting rows."""
    history = _load(
        workspace,
        pin,
        ComputeBudget(budget.cpu_limit, budget.memory_limit_bytes // 2),
        "feature_values",
    )
    marker = market.marker_for(workspace.market, pin.generation_id)
    catalog = workspace.state.execute(
        "SELECT transform_hash,normalizer_version FROM dataset_versions WHERE generation_id=?",
        (pin.generation_id,),
    ).fetchone()
    if document.body["transform_sha256"] != catalog["transform_hash"]:
        raise ValueError("proxy transform/catalog mismatch with publication evidence")
    if (
        document.sha256 != pin.manifest_hash
        or document.body["normalizer_version"] != catalog["normalizer_version"]
        or any(
            document.body[key] != marker[key]
            for key in (
                "dataset_id",
                "version",
                "generation_id",
                "operation_id",
                "parent_id",
                "domain",
            )
        )
    ):
        raise ValueError("proxy catalog conflicts with publication evidence")
    operation = workspace.state.execute(
        "SELECT kind,phase,request_hash,payload_hash,target_id,expected_parent,created_at_us "
        "FROM storage_operations WHERE operation_id=?",
        (marker["operation_id"],),
    ).fetchone()
    if operation is None or tuple(operation[:6]) != (
        "market_publish",
        "COMPLETED",
        document.sha256,
        document.sha256,
        pin.generation_id,
        marker["parent_id"],
    ):
        raise ValueError("proxy publication has no matching completed intent")
    delta = tuple(row for row in history if row["generation_id"] == pin.generation_id)
    expected = market.normalize_rows(
        "feature_values",
        pin.generation_id,
        [{**row, "ingested_at_us": operation["created_at_us"]} for row in document.rows],
    )
    if sorted(expected, key=lambda row: str(row["record_id"])) != list(delta):
        raise ValueError("proxy rows conflict with publication evidence")
    return delta


def verify_proxy_publications(workspace: Workspace, *, budget: ComputeBudget) -> None:
    """Discover referencing rows from sealed imports, never mutable feature identities."""
    contracts: set[tuple[str, str]] = {
        (row[0], row[1])
        for row in workspace.state.execute(
            "SELECT name,version FROM feature_contracts WHERE record_schema='aas-market-rowset-v1'"
        )
    }
    if not contracts:
        return
    retained: set[tuple[str, str]] = set()
    for catalog in workspace.state.execute(
        "SELECT dataset_id,version,generation_id,chain_hash,manifest_hash "
        "FROM dataset_versions WHERE status='committed'"
    ):
        pin = GenerationPin(*catalog)
        marker = market.marker_for(workspace.market, pin.generation_id)
        if marker["domain"] != "feature_values":
            continue
        # The marker's original request is independent of both live row fields
        # and catalog transform pointers. _load below also checks the manifest.
        document = parse_import(_raw_payload(workspace, str(marker["request_hash"]), budget))
        referenced = contracts.intersection(
            (row["contract_id"], row["contract_version"]) for row in document.rows
        )
        if not referenced:
            continue
        delta = _proxy_publication_delta(workspace, pin, document, budget)
        for identity in sorted(referenced):
            rows = tuple(
                row for row in delta if (row["contract_id"], row["contract_version"]) == identity
            )
            verify_proxy_content(workspace, rows, budget=budget)
        retained.update(referenced)
    if contracts - retained:
        raise ValueError("proxy definition has no retained feature generation")


def _verify_proxy_catalog(workspace: Workspace, generation: str, transform_hash: str) -> None:
    _verify_catalog(workspace, market.marker_for(workspace.market, generation))
    catalog = workspace.state.execute(
        "SELECT transform_hash FROM dataset_versions WHERE generation_id=?", (generation,)
    ).fetchone()
    if catalog[0] != transform_hash:
        raise ValueError("proxy transform/catalog mismatch")


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
    if (
        size is not None
        and size[0] * 256 > budget.memory_limit_bytes - budget.duckdb_memory_limit_bytes
    ):
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
    transform = decode_json(_raw_payload(workspace, transform_hash, budget))
    if (
        not isinstance(transform, dict)
        or transform.get("proxy") != definition
        or transform.get("schema_version") != "aas-proxy-transform-v1"
    ):
        raise ValueError("proxy transform conflicts with feature contract")
    delta = _proxy_publication(workspace, transform_hash, transform, budget)
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
