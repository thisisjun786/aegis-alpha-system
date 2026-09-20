"""SELECT-only preparation of exact stored research inputs, never fills or run storage.

The caller owns the admitted workspace and compute lease. Results are detached;
observed snapshots remain uncertified. T17 owns parsing, semantic identity and
legacy export; the engine receives only explicit, decision-local values.
"""

from __future__ import annotations

import hashlib
import platform
import re
import sys
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, getcontext
from importlib.metadata import version
from importlib.resources import files
from itertools import pairwise
from types import MappingProxyType
from typing import Literal, cast

from aegis_alpha.application.research_run import (
    FILL_CONVENTION,
    PreparationRecord,
    ResearchRunRequest,
    declared_provenance,
)
from aegis_alpha.compute_resources import ComputeBudget, ComputeResourceError
from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.backtest_request import (
    EngineIdentity,
    EnvelopeExport,
    EnvelopeInputs,
    EnvironmentIdentity,
    ParsedPrepareRequest,
    RequestProjection,
    export_envelope,
    parse_prepare_request,
    request_projection,
)
from aegis_alpha.engine.bundle import EngineBundle
from aegis_alpha.engine.codec import decode_json
from aegis_alpha.engine.ensemble import EnsembleMembership
from aegis_alpha.engine.features import (
    AssetFeatures,
    FeatureBuildRequest,
    PricePoint,
    build_feature_matrix,
)
from aegis_alpha.engine.membership import MembershipRow
from aegis_alpha.engine.models import ENGINE_CONTRACT_VERSION_V1
from aegis_alpha.engine.replay import ReplayReceipt, ReplayRequest, replay
from aegis_alpha.engine.requirements import ExecutionDefinition
from aegis_alpha.engine.schedule import DecisionSlot, ScheduleRequest, Session, decision_slots
from aegis_alpha.engine.signals import MacroPoint
from aegis_alpha.storage import market
from aegis_alpha.storage.input_pins import (
    ConventionPin,
    DefinitionPin,
    read_definition,
    read_execution_conventions,
)
from aegis_alpha.storage.market_inputs import (
    GenerationPin,
    History,
    PinnedObservationSeries,
    PinnedPriceSeries,
    PriceInputRequest,
    ReaderMode,
    admit_native_input,
    load_pinned_observations,
    load_pinned_prices,
    load_pinned_proxy,
    load_pinned_sessions,
    verify_sealed_publication,
)
from aegis_alpha.storage.membership_pins import IdentityPin, UniversePin, read_membership_pins
from aegis_alpha.storage.source_library import admit_source_table
from aegis_alpha.storage.source_reader import SourcePin, resolve_source
from aegis_alpha.storage.strategies import load_strategy
from aegis_alpha.storage.strategy_import import verify_strategy_import
from aegis_alpha.storage.strategy_requirements import read_execution_definition
from aegis_alpha.storage.workspace import Workspace

__all__ = [
    "PrepareRequest",
    "PreparedBacktest",
    "PreparedResearchRun",
    "StrategyPin",
    "parse_prepare_request",
    "prepare_backtest",
    "prepare_research_run",
    "research_source_identity",
]

_J = "aas-canonical-json-sha256-v1"
# Closed preparation interpretation/admission inventory plus engine calculation sources.
# New semantic owners require review of this list, not runtime import/glob discovery.
# Hash exact installed bytes, including this fixed source text, never its emitted digest.
CALCULATION_MODULES = (
    "aegis_alpha.application.backtest_prepare",
    "aegis_alpha.data.canonical_records",
    "aegis_alpha.data.serialization",
    "aegis_alpha.engine.__init__",
    "aegis_alpha.engine.allocation",
    "aegis_alpha.engine.backtest_request",
    "aegis_alpha.engine.bundle",
    "aegis_alpha.engine.calendar",
    "aegis_alpha.engine.codec",
    "aegis_alpha.engine.derived",
    "aegis_alpha.engine.ensemble",
    "aegis_alpha.engine.errors",
    "aegis_alpha.engine.etf_candidates",
    "aegis_alpha.engine.execution",
    "aegis_alpha.engine.features",
    "aegis_alpha.engine.membership",
    "aegis_alpha.engine.models",
    "aegis_alpha.engine.numbers",
    "aegis_alpha.engine.pit",
    "aegis_alpha.engine.proxy",
    "aegis_alpha.engine.replay",
    "aegis_alpha.engine.requirements",
    "aegis_alpha.engine.research",
    "aegis_alpha.engine.reset_returns",
    "aegis_alpha.engine.risk",
    "aegis_alpha.engine.schedule",
    "aegis_alpha.engine.signals",
    "aegis_alpha.engine.strategy_config",
    "aegis_alpha.engine.tolerance",
    "aegis_alpha.storage.import_document",
    "aegis_alpha.storage.input_pins",
    "aegis_alpha.storage.market",
    "aegis_alpha.storage.market_inputs",
    "aegis_alpha.storage.market_schema",
    "aegis_alpha.storage.membership_pins",
    "aegis_alpha.storage.research_inputs",
    "aegis_alpha.storage.rowset",
    "aegis_alpha.storage.source_library",
    "aegis_alpha.storage.source_library_digest",
    "aegis_alpha.storage.source_library_schema",
    "aegis_alpha.storage.source_reader",
    "aegis_alpha.storage.state",
    "aegis_alpha.storage.strategies",
    "aegis_alpha.storage.strategy_import",
    "aegis_alpha.storage.strategy_requirements",
)

type Row = Mapping[str, object]


def _row(value: object) -> Row:
    return cast("Row", value)


def _rows(value: object) -> tuple[Row, ...]:
    return cast("tuple[Row, ...]", value)


def _text(value: object) -> str:
    return cast("str", value)


def _day(value: object) -> date:
    return value if type(value) is date else date.fromisoformat(_text(value))


def _utc_day(value: object) -> date:
    return (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=cast("int", value))).date()


def _number(value: object) -> float:
    return float(cast("float | Decimal", value))


def _frozen(value: object) -> object:
    """Detach result collections, not a wire parser or a hash codec."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: _frozen(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_frozen(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class StrategyPin:
    strategy_store_id: str
    strategy_id: str
    version: str
    raw_sha256: str
    contract_sha256: str

    def __post_init__(self) -> None:
        for text in (self.strategy_store_id, self.strategy_id, self.version):
            if (
                not isinstance(text, str)
                or not text
                or text.strip() != text
                or not text.isprintable()
            ):
                raise ValueError("strategy pin requires exact printable identities")
        if self.version == "latest":
            raise ValueError("strategy version must be exact")
        for digest in (self.raw_sha256, self.contract_sha256):
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError("strategy pin requires lowercase SHA256")


@dataclass(frozen=True, slots=True)
class PrepareRequest:
    parsed: ParsedPrepareRequest

    def __post_init__(self) -> None:
        if not isinstance(self.parsed, ParsedPrepareRequest):
            raise TypeError("PrepareRequest wraps a T17 ParsedPrepareRequest")

    @property
    def strategy(self) -> StrategyPin:
        ref = _row(self.parsed.document["strategy"])
        return StrategyPin(
            *(
                _text(ref[key])
                for key in (
                    "strategy_store_id",
                    "strategy_id",
                    "version",
                    "raw_sha256",
                    "contract_sha256",
                )
            )
        )


@dataclass(frozen=True, slots=True)
class PreparedBacktest:
    request: PrepareRequest
    definition: ExecutionDefinition
    slots: tuple[DecisionSlot, ...]
    decisions: tuple[ReplayReceipt, ...]
    features: Mapping[date, Mapping[str, AssetFeatures]]
    inputs: EnvelopeInputs
    projection: RequestProjection
    envelope: EnvelopeExport
    provenance: bytes
    certified: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "slots", tuple(self.slots))
        object.__setattr__(self, "decisions", tuple(self.decisions))
        object.__setattr__(self, "features", _frozen(self.features))
        values = self.inputs
        object.__setattr__(
            self,
            "inputs",
            EnvelopeInputs(
                tuple(values.dates),
                cast("tuple[Mapping[str, float], ...]", _frozen(values.opens)),
                cast("tuple[Mapping[str, float], ...]", _frozen(values.closes)),
                cast("Mapping[date, Mapping[str, float]]", _frozen(values.targets)),
                cast("Mapping[str, str]", _frozen(values.instrument_types)),
                cast("tuple[Mapping[str, str], ...]", _frozen(values.source_pins)),
            ),
        )

    @property
    def targets(self) -> Mapping[date, Mapping[str, float]]:
        return self.inputs.targets

    @property
    def request_hash(self) -> str:
        return self.projection.request_hash


def calculation_identity() -> EngineIdentity:
    root = files("aegis_alpha")
    inventory = [
        {
            "module": module,
            "sha256": hashlib.sha256(
                root.joinpath(
                    module.removeprefix("aegis_alpha.").replace(".", "/") + ".py"
                ).read_bytes()
            ).hexdigest(),
        }
        for module in CALCULATION_MODULES
    ]
    return cast(
        "EngineIdentity",
        _frozen(
            {
                "schema": "aas-engine-identity-v1",
                "hash_format": _J,
                "package_version": version("aegis-alpha-system"),
                "contract_version": ENGINE_CONTRACT_VERSION_V1,
                "calculation_source_hash": content_sha256(
                    {
                        "schema": "aas-calculation-sources-v1",
                        "hash_format": _J,
                        "files": inventory,
                    }
                ),
            }
        ),
    )


def environment_identity() -> EnvironmentIdentity:
    context = getcontext()
    settings = {
        "float_radix": sys.float_info.radix,
        "float_mant_dig": sys.float_info.mant_dig,
        "float_rounds": sys.float_info.rounds,
        "decimal_precision": context.prec,
        "decimal_rounding": context.rounding,
        "decimal_emin": context.Emin,
        "decimal_emax": context.Emax,
        "decimal_capitals": context.capitals,
        "decimal_clamp": context.clamp,
        "decimal_traps": ",".join(
            sorted(signal.__name__ for signal, enabled in context.traps.items() if enabled)
        ),
    }
    return cast(
        "EnvironmentIdentity",
        _frozen(
            {
                "schema": "aas-environment-identity-v1",
                "hash_format": _J,
                "versions": (
                    {"name": "python_implementation", "version": platform.python_implementation()},
                    {"name": "python_version", "version": platform.python_version()},
                ),
                "settings": tuple(
                    {"name": key, "value": value} for key, value in sorted(settings.items())
                ),
            }
        ),
    )


def _generation(ref: Row) -> GenerationPin:
    pin = _row(ref["pin"])
    return GenerationPin(
        *(
            _text(pin[key])
            for key in (
                "dataset_id",
                "version",
                "generation_id",
                "chain_hash",
                "manifest_hash",
            )
        )
    )


def _convention(ref: Row) -> ConventionPin:
    pin = _row(ref["pin"])
    return ConventionPin(*(_text(pin[key]) for key in ("kind", "id", "version", "hash")))


def _definition_pin(ref: Row) -> DefinitionPin:
    pin = _row(ref["pin"])
    return DefinitionPin(*(_text(pin[key]) for key in ("kind", "id", "version", "hash")))


def _bindings(body: Row) -> dict[tuple[str, int], Row]:
    refs = {
        tuple(ref[key] for key in ("ref_kind", "ref_id", "ref_version")): ref
        for ref in _rows(body["refs"])
    }
    return {
        (_text(binding["role"]), cast("int", binding["ordinal"])): refs[
            tuple(binding[key] for key in ("ref_kind", "ref_id", "ref_version"))
        ]
        for binding in _rows(body["bindings"])
    }


def _selection_ref(selection: Row, bindings: Mapping[tuple[str, int], Row]) -> Row:
    key = _row(selection["binding"])
    return bindings[(_text(key["role"]), cast("int", key["ordinal"]))]


@dataclass(frozen=True, slots=True)
class _Visibility:
    mode: ReaderMode
    ceiling: int
    ingestion: int | None
    history_start: date
    history_end: date

    def candidates(self, history: History, cutoff: int) -> History:
        # Snapshot research admits unknown publication, not known future revisions.
        # Keep known-but-unavailable superseders until head projection: they must
        # remove an old value rather than reveal it again.
        if self.mode == "strict_pit":
            return history
        return tuple(
            row
            for row in history
            if row["revision_known_at_us"] is None
            or cast("int", row["revision_known_at_us"]) <= cutoff
        )

    def project(self, history: History, cutoff: int) -> History:
        heads = market.project_heads(
            self.candidates(history, cutoff),
            cutoff_us=cutoff if self.mode == "strict_pit" else None,
            ingestion_cutoff_us=self.ingestion,
        )
        return tuple(
            MappingProxyType(row)
            for row in heads
            if row["available_at_us"] is None or cast("int", row["available_at_us"]) <= cutoff
        )

    def observed(self, row: Row, economic: date) -> date:
        known = tuple(
            cast("int", row[key])
            for key in ("available_at_us", "revision_known_at_us")
            if row[key] is not None
        )
        # Economic dating of unknown snapshot values is explicitly uncertified;
        # it never manufactures publication knowledge from ingestion.
        return _utc_day(max(known)) if known else economic


def _sessions(history: History, visibility: _Visibility, cutoff: int) -> tuple[Session, ...]:
    return tuple(
        Session(
            _text(row["calendar_id"]),
            _text(row["venue"]),
            _day(row["session_date"]),
            cast("int | None", row["open_at_us"]),
            cast("int | None", row["close_at_us"]),
            cast("Literal['open', 'closed']", row["status"]),
            _text(row["timezone_version"]),
        )
        for row in sorted(
            visibility.project(history, cutoff), key=lambda row: _day(row["session_date"])
        )
    )


def _stored_strategy(
    workspace: Workspace, pin: StrategyPin
) -> tuple[EngineBundle, ExecutionDefinition]:
    connection = workspace.strategies
    if connection is None:
        raise ValueError("private strategy database is unavailable")
    store = connection.execute("SELECT store_id FROM store_info").fetchone()
    if store is None or store[0] != pin.strategy_store_id:
        raise ValueError("strategy store pin mismatch")
    definition = read_execution_definition(connection, pin.strategy_id, pin.version, pin.raw_sha256)
    if definition.contract_sha256 != pin.contract_sha256:
        raise ValueError("strategy contract hash mismatch")
    for receipt in connection.execute(
        "SELECT operation_id FROM strategy_imports WHERE strategy_id=? AND version=?",
        (pin.strategy_id, pin.version),
    ):
        operation = workspace.state.execute(
            "SELECT * FROM storage_operations WHERE operation_id=? AND phase='COMPLETED'",
            (receipt[0],),
        ).fetchone()
        if operation is None or not verify_strategy_import(workspace, operation):
            raise ValueError("strategy lineage lacks a completed verified import")
    return load_strategy(connection, pin.strategy_id, pin.version, pin.raw_sha256), definition


@dataclass(slots=True)
class _Loader:
    workspace: Workspace
    budget: ComputeBudget
    bindings: Mapping[tuple[str, int], Row]
    sources: dict[SourcePin, None] = field(default_factory=dict)
    evidence: list[Row] = field(default_factory=list)
    charge: int = 0

    def retain(self, name: str, value: object) -> None:
        self.charge += len(canonical_json_bytes(value)) * 32
        if self.charge > self.budget.available_bytes:
            raise ComputeResourceError("prepared histories exceed aggregate materialization budget")
        self.evidence.append({"name": name, "value": value})

    def native(self, pin: GenerationPin, schema: str) -> History:
        admitted = admit_native_input(
            self.workspace, pin, expected_schema=schema, budget=self.budget
        )
        self.sources.update(dict.fromkeys(admitted.source_pins))
        self.retain(pin.generation_id, admitted.history)
        return admitted.history

    def generation(self, pin: GenerationPin) -> tuple[str, History]:
        history = verify_sealed_publication(self.workspace, pin.generation_id, budget=self.budget)
        marker = market.marker_for(self.workspace.market, pin.generation_id)
        if (
            any(
                marker[key] != getattr(pin, key)
                for key in (
                    "dataset_id",
                    "version",
                    "generation_id",
                    "chain_hash",
                )
            )
            or marker["request_hash"] != pin.manifest_hash
        ):
            raise ValueError("exact auxiliary generation pin mismatch")
        self.retain(pin.generation_id, history)
        return _text(marker["domain"]), history

    def source(self, pin: SourcePin) -> None:
        if pin not in self.sources:
            admit_source_table(
                self.workspace,
                pin.source_id,
                pin.table,
                max_materialization_bytes=self.budget.available_bytes,
            )
            resolve_source(self.workspace, pin)
            self.sources[pin] = None

    def proxy_source(self, definition: Row) -> None:
        # load_pinned_proxy has authenticated this exact transform and its child
        # reference. Extract its SourcePin using the shared codec; do not interpret
        # columns, normalize values, reconstruct or re-splice proxy points here.
        digest = self.workspace.state.execute(
            "SELECT ref_id FROM feature_inputs WHERE name=? AND version=? AND ref_kind='transform'",
            (definition["proxy_id"], definition["version"]),
        ).fetchone()[0]
        with DescriptorTree.open_path(self.workspace.paths.raw) as tree:
            raw = tree.read_bytes(
                digest[:2] + "/" + digest,
                max_bytes=(self.budget.available_bytes) // 32,
            )
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("proxy transform content changed")
        transform = _row(decode_json(raw))
        source = _row(transform["source"])
        self.source(
            SourcePin(
                *(
                    _text(source[key])
                    for key in ("source_id", "source_sha256", "table", "table_digest")
                )
            )
        )
        self.retain("proxy_transform:" + _text(definition["proxy_id"]), transform)

    def definition(self, ref: Row) -> Row:
        pin = _definition_pin(ref)
        raw = read_definition(self.workspace, pin, budget=self.budget)
        doc = _row(decode_json(raw))
        self.retain(pin.kind + ":" + pin.id, doc)
        return doc


def _membership(loader: _Loader, bundle: EngineBundle) -> EnsembleMembership:
    body = loader.definition(loader.bindings["membership", 0])
    membership = EnsembleMembership(
        tuple(
            MembershipRow(_text(row["name"]), Decimal(_text(row["weight"])))
            for row in _rows(body["rows"])
        ),
        _text(body["membership_sha256"]),
    )
    if {row.name for row in membership.rows} != {
        strategy.name for strategy in bundle.contract.pack
    }:
        raise ValueError("ensemble membership must name the exact strategy pack")
    if bundle.contract.ensemble_membership_reference != "ensemble:" + membership.membership_sha256:
        raise ValueError("ensemble membership digest disagrees with strategy")
    return membership


@dataclass(frozen=True, slots=True)
class _Prices:
    role: str
    series: PinnedPriceSeries
    sources: tuple[SourcePin, ...]


def _prices(loader: _Loader, body: Row, calendar: Row, sessions: History) -> tuple[_Prices, ...]:
    ip = _row(loader.bindings["identity", 0]["pin"])
    up = _row(loader.bindings["universe", 0]["pin"])
    identity = IdentityPin(_text(ip["snapshot_id"]), _text(ip["content_hash"]))
    universe = UniversePin(
        _text(up["universe_id"]), _text(up["version"]), _text(up["content_hash"])
    )
    memberships = read_membership_pins(
        loader.workspace.state,
        identity,
        universe,
        max_materialization_bytes=(loader.budget.available_bytes) // 8,
    )
    loader.retain(
        "memberships",
        tuple(
            decode_json(member.canonical_bytes)
            for member in (memberships.identity, memberships.universe)
            if member is not None
        ),
    )
    history, period = _row(body["history"]), _row(body["period"])
    start = _day(history["start"])  # Request validation places this before period.start.
    end = max(_day(history["end"]), _day(period["end"]))
    grid = tuple(
        sorted({day for row in sessions if start <= (day := _day(row["session_date"])) <= end})
    )
    result = []
    for selection in _rows(body["price_inputs"]):
        pin = _generation(_selection_ref(selection, loader.bindings))
        admitted = admit_native_input(
            loader.workspace, pin, expected_schema="aas-price-transform-v1", budget=loader.budget
        )
        loader.sources.update(dict.fromkeys(admitted.source_pins))
        loader.retain(pin.generation_id, admitted.history)
        request = PriceInputRequest(
            pin,
            _generation(loader.bindings["sessions", 0]),
            cast("tuple[str, ...]", selection["instrument_ids"]),
            grid,
            _text(selection["currency"]),
            _text(selection["basis"]),
            cast("Literal['canonical', 'reference']", selection["price_role"]),
            _text(calendar["calendar_id"]),
            _text(calendar["venue"]),
            _text(calendar["timezone_version"]),
            mode=cast("ReaderMode", _row(body["cutoff"])["mode"]),
            identity_pin=identity,
            universe_pin=universe,
        )
        result.append(
            _Prices(
                _text(_row(selection["binding"])["role"]),
                load_pinned_prices(loader.workspace, request, budget=loader.budget),
                admitted.source_pins,
            )
        )
    return tuple(result)


def _eligible_prices(
    series: PinnedPriceSeries, visibility: _Visibility, cutoff: int, day: date
) -> History:
    series = replace(series, history=visibility.candidates(series.history, cutoff))
    if series.sessions is not None:
        series = replace(
            series,
            sessions=replace(
                series.sessions, history=visibility.candidates(series.sessions.history, cutoff)
            ),
        )
    projected = series.project_as_of(
        cutoff, session_date=day, ingestion_cutoff_us=visibility.ingestion
    )
    eligible = {
        (cell.instrument_id, cell.session_date)
        for cell in projected.coverage.cells
        if cell.present
        and not {
            "missing_session",
            "session_closed",
            "future_session",
            "future_observation",
            "identity_unavailable",
            "outside_universe",
        }.intersection(cell.reasons)
    }
    return tuple(
        row
        for row in sorted(projected.rows, key=lambda row: _day(row["session_date"]))
        if (row["instrument_id"], _day(row["session_date"])) in eligible
        and visibility.history_start <= _day(row["session_date"]) <= visibility.history_end
        and (row["available_at_us"] is None or cast("int", row["available_at_us"]) <= cutoff)
    )


def _price_points(
    prices: tuple[_Prices, ...], visibility: _Visibility, slot: DecisionSlot
) -> dict[str, tuple[PricePoint, ...]]:
    result: dict[str, tuple[PricePoint, ...]] = {}
    for selected in prices:
        if selected.role != "signal_prices":
            continue
        rows = _eligible_prices(selected.series, visibility, slot.cutoff_us, slot.decision_date)
        for instrument in selected.series.request.instrument_ids:
            result[instrument] = tuple(
                PricePoint(
                    _day(row["session_date"]),
                    _number(row["close"]),
                    visibility.observed(row, _day(row["session_date"])),
                )
                for row in rows
                if row["instrument_id"] == instrument
            )
    return result


@dataclass(frozen=True, slots=True)
class _Auxiliary:
    name: str
    domain: str
    history: History
    series: str
    field: str
    identity: tuple[str, str]
    prices: PinnedPriceSeries | None = None


def _auxiliary(
    loader: _Loader, body: Row, definition: ExecutionDefinition, template: PriceInputRequest
) -> tuple[_Auxiliary, ...]:
    result = []
    for selection in _rows(body["macro_inputs"]):
        pin = _generation(_selection_ref(selection, loader.bindings))
        domain, history = loader.generation(pin)
        series = _text(selection["series_id"])
        if domain != "macro_observations" or not any(row["series_id"] == series for row in history):
            raise ValueError("macro binding has incompatible domain/series")
        if any(row["unit"] != selection["unit"] for row in history if row["series_id"] == series):
            raise ValueError("macro unit mismatch")
        result.append(
            _Auxiliary(series, domain, history, series, "value", (pin.dataset_id, pin.version))
        )
    specs = {spec.series_id: spec for spec in definition.derived_series}
    for selection in _rows(body["derived_inputs"]):
        doc = loader.definition(_selection_ref(selection, loader.bindings))
        spec = specs[_text(selection["series_id"])]
        if canonical_json_bytes(doc["definition"]) != canonical_json_bytes(spec):
            raise ValueError("derived definition differs from the exact strategy spec")
        for item in _rows(doc["inputs"]):
            binding = spec.input_bindings[cast("int", item["ordinal"])]
            pin = _generation(_row(item["pin"]))
            domain, history = loader.generation(pin)
            _check_derived_domain(domain, history, binding.series, binding.field)
            if domain == "feature_values":
                _reject_observation_contract(loader.workspace, history)
            prices = None
            if domain == "prices":
                history = loader.native(pin, "aas-price-transform-v1")
                prices = load_pinned_prices(
                    loader.workspace,
                    replace(
                        template,
                        pin=pin,
                        instrument_ids=(binding.series,),
                        basis="split_adjusted",
                        price_role="reference",
                    ),
                    budget=loader.budget,
                )
            result.append(
                _Auxiliary(
                    spec.series_id,
                    domain,
                    history,
                    binding.series,
                    binding.field,
                    (pin.dataset_id, pin.version),
                    prices,
                )
            )
    return tuple(result)


def _reject_observation_contract(workspace: Workspace, history: History) -> None:
    """Keep observed research observations out of the generic derived reader.

    An observation contract is always reference data and its own reader selects
    none of it under strict PIT. The derived path projects feature_values directly
    and never consults that contract, so admitting one here would let a
    known-timestamp reference observation reach a replay through the back door.
    """
    from aegis_alpha.storage.research_inputs import (  # noqa: PLC0415 -- contract owner
        OBSERVATION_DEFINITION_SCHEMA,
    )

    first = history[0]
    observed = workspace.state.execute(
        "SELECT 1 FROM feature_contracts WHERE name=? AND version=? AND record_schema=?",
        (first["contract_id"], first["contract_version"], OBSERVATION_DEFINITION_SCHEMA),
    ).fetchone()
    if observed is not None:
        raise ValueError("observed research observations are not derived-series inputs")


def _check_derived_domain(domain: str, history: History, series: str, field_name: str) -> None:
    if domain not in {"prices", "macro_observations", "feature_values"}:
        raise ValueError("derived input domain mismatch")
    key = "series_id" if domain == "macro_observations" else "instrument_id"
    selected = tuple(row for row in history if row[key] == series)
    if not selected:
        raise ValueError("derived input series is absent")
    if domain == "prices" and (
        field_name != "price" or any(row["basis"] != "split_adjusted" for row in selected)
    ):
        raise ValueError("derived price requires capital basis")
    if domain == "macro_observations" and field_name == "price":
        raise ValueError("derived price domain mismatch")
    if (
        domain == "feature_values"
        and len(
            {
                tuple(
                    row[key]
                    for key in (
                        "contract_id",
                        "contract_version",
                        "contract_hash",
                        "input_bundle_hash",
                    )
                )
                for row in history
            }
        )
        != 1
    ):
        raise ValueError("derived feature history has ambiguous contract identity")


def _aux_points(
    item: _Auxiliary, visibility: _Visibility, slot: DecisionSlot
) -> tuple[tuple[date, float, date], ...]:
    cutoff = slot.cutoff_us
    result = []
    rows = (
        visibility.project(item.history, cutoff)
        if item.prices is None
        else _eligible_prices(item.prices, visibility, cutoff, slot.decision_date)
    )
    for row in rows:
        identity = row["series_id"] if item.domain == "macro_observations" else row["instrument_id"]
        if identity != item.series or row["value_state"] != "present":
            continue
        if item.domain == "feature_values":
            economic = _utc_day(row["feature_at_us"])
            if cast("int", row["feature_at_us"]) > cutoff:
                continue
        else:
            economic = _day(
                row["session_date" if item.domain == "prices" else "observation_period"]
            )
        if (
            not visibility.history_start
            <= economic
            <= min(visibility.history_end, slot.decision_date)
        ):
            continue
        value = _number(row["close" if item.domain == "prices" else "value"])
        result.append((economic, value, visibility.observed(row, economic)))
    if len({point[0] for point in result}) != len(result):
        raise ValueError("ambiguous auxiliary observation dates")
    return tuple(sorted(result))


def _aux_inputs(
    items: tuple[_Auxiliary, ...], visibility: _Visibility, slot: DecisionSlot
) -> tuple[dict[str, tuple[MacroPoint, ...]], dict[str, Mapping[str, object]]]:
    macro: dict[str, tuple[MacroPoint, ...]] = {}
    fields: dict[str, dict[str, object]] = {}
    identities: dict[str, dict[str, tuple[str, str]]] = {}
    for item in items:
        points = _aux_points(item, visibility, slot)
        if item.field == "value":
            macro[item.name] = tuple(MacroPoint(*point) for point in points)
        else:
            fields.setdefault(item.name, {})[item.field] = points
            identities.setdefault(item.name, {})[item.series] = item.identity
    return macro, {
        name: {"fields": values, "identities": identities[name]} for name, values in fields.items()
    }


@dataclass(frozen=True, slots=True)
class _Proxy:
    logical: str
    history: History
    transition: Row

    def instrument(self, decision: date) -> str:
        donor = self.transition["mode"] == "observed_instrument_switch" and decision < _day(
            self.transition["switch_decision_date"]
        )
        return _text(self.transition["donor_id" if donor else "target_id"])


def _proxies(loader: _Loader, body: Row, prices: tuple[_Prices, ...]) -> tuple[_Proxy, ...]:
    result = []
    selected_sources = {
        instrument: item.sources
        for item in prices
        if item.role == "execution_prices"
        for instrument in item.series.request.instrument_ids
    }
    for selection in _rows(body["proxy_rules"]):
        pin = _generation(_selection_ref(selection, loader.bindings))
        stored = load_pinned_proxy(loader.workspace, pin, budget=loader.budget)
        doc = _row(decode_json(stored.definition.encode()))
        loader.proxy_source(doc)
        transition = _row(doc["transition"])
        logical = _text(selection["logical_exposure_id"])
        if transition["logical_exposure_id"] != logical:
            raise ValueError("proxy logical exposure mismatch")
        for role in ("basis", "calendar", "cost"):
            convention = _convention(loader.bindings[role, 0])
            if _row(transition[role + "_ref"]) != {
                "id": convention.id,
                "version": convention.version,
                "sha256": convention.hash,
            }:
                raise ValueError("proxy convention reference mismatch")
        _proxy_sources(loader, transition, selected_sources)
        loader.retain("proxy:" + logical, {"definition": doc, "history": stored.history})
        result.append(_Proxy(logical, stored.history, transition))
    return tuple(result)


def _proxy_sources(
    loader: _Loader, transition: Row, selected: Mapping[str, tuple[SourcePin, ...]]
) -> None:
    for side in ("donor", "target"):
        raw = _row(transition[side + "_source"])
        pin = SourcePin(
            *(_text(raw[key]) for key in ("source_id", "source_sha256", "table", "table_digest"))
        )
        loader.source(pin)
        instrument = _text(transition[side + "_id"])
        required = side == "target" or transition["mode"] == "observed_instrument_switch"
        if required and (instrument not in selected or pin not in selected[instrument]):
            raise ValueError(
                "proxy donor/target must match explicitly selected native execution sources"
            )


def _proxy_points(proxy: _Proxy, visibility: _Visibility, cutoff: int) -> tuple[PricePoint, ...]:
    points = tuple(
        PricePoint(
            _utc_day(row["feature_at_us"]),
            _number(row["value"]),
            visibility.observed(row, _utc_day(row["feature_at_us"])),
        )
        for row in visibility.project(proxy.history, cutoff)
        if row["value_state"] == "present"
        and cast("int", row["feature_at_us"]) <= cutoff
        and visibility.history_start <= _utc_day(row["feature_at_us"]) <= visibility.history_end
    )
    if len({point.as_of for point in points}) != len(points):
        raise ValueError("ambiguous proxy observation dates")
    return tuple(sorted(points, key=lambda point: point.as_of))


def _warmup(
    definition: ExecutionDefinition,
    prices: Mapping[str, tuple[PricePoint, ...]],
    slot: DecisionSlot,
    visibility: _Visibility,
) -> None:
    required = next(
        item.minimum_observations for item in definition.input_requirements if item.role == "prices"
    )
    month = slot.decision_date.year * 12 + slot.decision_date.month - 1
    if slot.decision_date.day < definition.calendar.current_month_drop_before_day:
        month -= 1
    start_month = month - required + 1
    required_start = date(start_month // 12, start_month % 12 + 1, 1)
    if visibility.history_start.year * 12 + visibility.history_start.month - 1 > start_month:
        raise ValueError(
            f"history window too narrow: required start {required_start}, "
            f"requested {visibility.history_start}"
        )
    if visibility.history_end < slot.decision_date:
        raise ValueError("history window ends before decision")
    for asset in definition.price_asset_ids:
        months = {
            point.as_of.year * 12 + point.as_of.month - 1
            for point in prices.get(asset, ())
            if point.as_of <= slot.decision_date
        }
        eligible = sorted(value for value in months if value <= month)[
            -definition.calendar.history_observations :
        ]
        if len(eligible) < required:
            raise ValueError(
                f"insufficient eligible buckets for {asset}: available {len(eligible)}, "
                f"required {required}; required start {required_start}"
            )
        if not set(range(start_month, month + 1)) <= set(eligible):
            raise ValueError(f"missing monthly bucket for {asset}; required start {required_start}")


def _outcomes(
    prices: tuple[_Prices, ...], dates: tuple[date, ...], visibility: _Visibility
) -> tuple[tuple[Mapping[str, float], ...], tuple[Mapping[str, float], ...]]:
    opening: dict[date, dict[str, float]] = {day: {} for day in dates}
    closing: dict[date, dict[str, float]] = {day: {} for day in dates}
    for selected in prices:
        if selected.role != "execution_prices":
            continue
        # Outcomes are not signal knowledge at a past decision. They are the
        # separately pinned period observations under the request's global ceiling.
        for row in visibility.project(selected.series.history, visibility.ceiling):
            day = _day(row["session_date"])
            symbol = _text(row["instrument_id"])
            if (
                day not in opening
                or symbol not in selected.series.request.instrument_ids
                or row["value_state"] != "present"
                or cast("int", row["bar_end_us"]) > visibility.ceiling
            ):
                continue
            for name, destination in (("open", opening), ("close", closing)):
                if row[name] is not None:
                    destination[day][symbol] = _number(row[name])
    return tuple(opening[day] for day in dates), tuple(closing[day] for day in dates)


def _types(workspace: Workspace, prices: tuple[_Prices, ...]) -> dict[str, str]:
    return _instrument_types(
        workspace,
        (
            instrument
            for item in prices
            if item.role == "execution_prices"
            for instrument in item.series.request.instrument_ids
        ),
    )


def _instrument_types(workspace: Workspace, instruments: Iterable[str]) -> dict[str, str]:
    """Classify every execution instrument from the store, never from the request."""
    result = {}
    for instrument in instruments:
        row = workspace.state.execute(
            "SELECT asset_type FROM instruments WHERE instrument_id=?", (instrument,)
        ).fetchone()
        if row is None or row[0] != "etf":
            raise ValueError("execution instrument is not an explicitly classified ETF")
        result[instrument] = "ETF"
    return result


def _targets(
    receipt: ReplayReceipt, proxies: tuple[_Proxy, ...], definition: ExecutionDefinition
) -> Mapping[str, float]:
    mapping = {proxy.logical: proxy.instrument(receipt.as_of) for proxy in proxies}
    result: dict[str, float] = {}
    for logical, weight in receipt.ensemble.items():
        if logical not in definition.cash_asset_ids and weight > 0:
            instrument = mapping.get(logical, logical)
            result[instrument] = result.get(instrument, 0.0) + weight
    return MappingProxyType(result)


def _complete_calendar(sessions: tuple[Session, ...], start: date, end: date) -> None:
    days = {session.session_date for session in sessions}
    if sum(start <= day <= end for day in days) != (end - start).days + 1:
        raise ValueError("incomplete pinned calendar: missing session declaration")


def _complete_automatic_months(
    sessions: tuple[Session, ...], request: ScheduleRequest, resolved: set[tuple[int, int]]
) -> None:
    # The outcome projection identifies months needing an explanation, never
    # the dates/cutoffs of historical slots. Only local evidence resolves them.
    outcome_opens = tuple(
        session.session_date
        for session in sessions
        if session.status == "open"
        and request.period_start <= session.session_date <= request.period_end
    )
    required = {
        (left.year, left.month)
        for left, right in pairwise(outcome_opens)
        if (left.year, left.month) != (right.year, right.month)
    }
    if missing := required - resolved:
        raise ValueError(
            f"incomplete decision-local calendar for automatic months: {sorted(missing)}"
        )


def _schedule(
    history: History, visibility: _Visibility, request: ScheduleRequest
) -> tuple[DecisionSlot, ...]:
    # Candidate dates come from retained history, not a future revised latest grid.
    # Every accepted pair is independently selected by T16 at its own ceiling.
    candidates = sorted(
        {
            (_day(row["session_date"]), cast("int", row["close_at_us"]))
            for row in history
            if row["status"] == "open"
            and request.period_start <= _day(row["session_date"]) < request.period_end
        }
    )
    selected: dict[date, DecisionSlot] = {}
    resolved_months: set[tuple[int, int]] = set()
    explicit = request.explicit_decision_dates
    for day, close in candidates:
        if explicit is not None and day not in explicit:
            continue
        cutoff = min(visibility.ceiling, close + request.decision_latency_us)
        grid = _sessions(history, visibility, cutoff)
        opens = tuple(session for session in grid if session.status == "open")
        if not any(
            session.session_date == day and session.close_at_us == close for session in opens
        ):
            continue
        month = (day.year, day.month)
        # A later admitted candidate supersedes earlier completeness evidence in
        # its month. Unmatched historical/future revisions cannot erase that proof.
        resolved_months.discard(month)
        # Omission needs affirmative daily declarations through the endpoint;
        # an absent successor alone may just be unavailable calendar evidence.
        if (
            explicit is None
            and not any(day < session.session_date <= request.period_end for session in opens)
            and sum(day <= session.session_date <= request.period_end for session in grid)
            == (request.period_end - day).days + 1
        ):
            resolved_months.add(month)
        pair = next(
            (
                (left, right)
                for left, right in pairwise(opens)
                if left.session_date == day and left.close_at_us == close
            ),
            None,
        )
        if pair is None:
            continue
        left, right = pair
        if (left.session_date.year, left.session_date.month) == (
            right.session_date.year,
            right.session_date.month,
        ) or right.session_date > request.period_end:
            continue
        _complete_calendar(grid, visibility.history_start, right.session_date)
        (slot,) = decision_slots(
            grid, request=replace(request, request_cutoff_us=cutoff, explicit_decision_dates=(day,))
        )
        if day in selected and selected[day] != slot:
            raise ValueError("ambiguous decision-local session revisions")
        selected[day] = slot
        resolved_months.add(month)
    if explicit is not None and set(selected) != set(explicit):
        raise ValueError("explicit decision has no eligible pinned session pair")
    if explicit is None:
        _complete_automatic_months(
            _sessions(history, visibility, visibility.ceiling), request, resolved_months
        )
    return tuple(selected[day] for day in sorted(selected))


def _execution_selection(
    prices: tuple[_Prices, ...], proxies: tuple[_Proxy, ...], definition: ExecutionDefinition
) -> None:
    expected = (
        set(definition.asset_ids)
        - set(definition.cash_asset_ids)
        - {proxy.logical for proxy in proxies}
    )
    for proxy in proxies:
        expected.add(_text(proxy.transition["target_id"]))
        if proxy.transition["mode"] == "observed_instrument_switch":
            expected.add(_text(proxy.transition["donor_id"]))
    selected = {
        instrument
        for item in prices
        if item.role == "execution_prices"
        for instrument in item.series.request.instrument_ids
    }
    if selected != expected:
        raise ValueError("execution selection must match exact assets and proxy transitions")


def _active(members: History, instrument: str, economic: int, known: int) -> bool:
    return any(
        row["instrument_id"] == instrument
        and cast("int", row["valid_from_us"]) <= economic
        and (row["valid_to_us"] is None or economic < cast("int", row["valid_to_us"]))
        and cast("int", row["known_from_us"]) <= known
        and (row["known_to_us"] is None or known < cast("int", row["known_to_us"]))
        for row in members
    )


def _target_memberships(
    targets: Mapping[date, Mapping[str, float]],
    slots: tuple[DecisionSlot, ...],
    prices: tuple[_Prices, ...],
    sessions: History,
    visibility: _Visibility,
) -> None:
    selected = {
        instrument: item.series
        for item in prices
        if item.role == "execution_prices"
        for instrument in item.series.request.instrument_ids
    }
    for slot in slots:
        # Economic validity uses the execution open admitted at this cutoff,
        # independently of membership knowledge and later outcome revisions.
        opening = next(
            cast("int", session.open_at_us)
            for session in _sessions(sessions, visibility, slot.cutoff_us)
            if session.session_date == slot.execution_date
        )
        for instrument in targets[slot.decision_date]:
            if instrument not in selected:
                raise ValueError("target is not an explicitly selected instrument")
            series = selected[instrument]
            if not _active(series.identities, instrument, opening, slot.cutoff_us):
                raise ValueError("selected target identity unavailable at decision")
            if not _active(series.universe, instrument, opening, slot.cutoff_us):
                raise ValueError("selected target outside pinned universe at decision")


@dataclass(frozen=True, slots=True)
class _Period:
    """What a preparation asks of the calendar, apart from the sessions themselves."""

    sessions_pin: GenerationPin
    calendar: Row | None
    start: date
    end: date
    latency: int
    explicit: tuple[date, ...] | None


@dataclass(frozen=True, slots=True)
class _Plan:
    """The calendar every preparation shares: sessions, the grid, the slots, the period."""

    sessions: History
    grid: tuple[Session, ...]
    slots: tuple[DecisionSlot, ...]
    dates: tuple[date, ...]


def _plan(
    loader: _Loader,
    definition: ExecutionDefinition,
    visibility: _Visibility,
    period: _Period,
) -> _Plan:
    """Resolve the pinned sessions into a validated schedule and outcome calendar.

    Both preparations read the same registered sessions generation and hold the same
    calendar to the same checks. Only the price side differs between them, so the
    schedule lives here once rather than as two copies that could drift apart.
    """
    loader.native(period.sessions_pin, "aas-sessions-transform-v1")
    sessions = load_pinned_sessions(
        loader.workspace, period.sessions_pin, budget=loader.budget
    ).history
    # A declared run names no calendar convention, so the pinned sessions supply their
    # own identity rather than a caller asserting one over them.
    calendar = period.calendar if period.calendar is not None else _session_calendar(sessions)
    schedule = ScheduleRequest(
        definition.calendar,
        _text(calendar["calendar_id"]),
        _text(calendar["venue"]),
        _text(calendar["timezone_version"]),
        period.start,
        period.end,
        period.latency,
        visibility.ceiling,
        period.explicit,
    )
    grid = _sessions(sessions, visibility, visibility.ceiling)
    slots = _schedule(sessions, visibility, schedule)
    dates = tuple(
        session.session_date
        for session in grid
        if session.status == "open"
        and schedule.period_start <= session.session_date <= schedule.period_end
    )
    # Legacy accounting fills on the next grid date, including all-cash targets.
    # Reject an unrepresentable slot rather than rewriting either calendar.
    next_open = dict(pairwise(dates))
    for slot in slots:
        if next_open.get(slot.decision_date) != slot.execution_date:
            message = f"incompatible outcome calendar projection: decision {slot.decision_date} "
            message += f"requires next open {slot.execution_date}, "
            raise ValueError(message + f"projected {next_open.get(slot.decision_date)}")
    # Validate even an intentionally empty schedule and all supplied session values.
    decision_slots(grid, request=replace(schedule, explicit_decision_dates=()))
    _complete_calendar(grid, visibility.history_start, schedule.period_end)
    return _Plan(sessions, grid, slots, dates)


def _session_calendar(sessions: History) -> Row:
    """Take the calendar from the registered sessions generation, never from prose."""
    named = {
        tuple(_text(row[key]) for key in ("calendar_id", "venue", "timezone_version"))
        for row in sessions
    }
    if len(named) != 1:
        raise ValueError("the pinned sessions generation names more than one calendar")
    calendar_id, venue, timezone_version = next(iter(named))
    return {"calendar_id": calendar_id, "venue": venue, "timezone_version": timezone_version}


def prepare_backtest(
    workspace: Workspace, request: PrepareRequest, *, budget: ComputeBudget
) -> PreparedBacktest:
    """Prepare decision-keyed targets/provenance using SELECT-only stored owners.

    Does not install schemas, register requests, write output files, discover a
    default strategy, infer holdings, execute accounting or certify market data.
    Keep the captured environment unchanged when consuming the result.
    """
    body = _row(request.parsed.document)
    engine, environment = calculation_identity(), environment_identity()
    bundle, definition = _stored_strategy(workspace, request.strategy)
    bindings = _bindings(body)
    conventions = read_execution_conventions(
        workspace.state,
        tuple(
            _convention(ref)
            for ref in _rows(body["refs"])
            if _text(ref["ref_kind"]).startswith("convention:")
        ),
        definition=definition,
    )
    projection = request_projection(
        request.parsed,
        definition=definition,
        convention_documents=conventions,
        engine_identity=engine,
        environment_identity=environment,
    )
    calendar = next(
        _row(_row(decode_json(raw))["payload"])
        for raw in conventions
        if _row(decode_json(raw))["kind"] == "calendar"
    )
    cutoff, history = (_row(body[key]) for key in ("cutoff", "history"))
    visibility = _Visibility(
        cast("ReaderMode", cutoff["mode"]),
        cast("int", cutoff["knowledge_cutoff_us"]),
        cast("int | None", cutoff["ingestion_cutoff_us"]),
        _day(history["start"]),
        _day(history["end"]),
    )
    loader = _Loader(workspace, budget, bindings)
    period = _row(body["period"])
    plan = _plan(
        loader,
        definition,
        visibility,
        _Period(
            _generation(bindings["sessions", 0]),
            calendar,
            _day(period["start"]),
            _day(period["end"]),
            cast("int", body["decision_latency_us"]),
            None
            if body["explicit_decision_dates"] is None
            else tuple(
                _day(day) for day in cast("tuple[str, ...]", body["explicit_decision_dates"])
            ),
        ),
    )
    slots, dates = plan.slots, plan.dates
    membership = _membership(loader, bundle)
    prices = _prices(loader, body, calendar, plan.sessions)
    auxiliary = _auxiliary(loader, body, definition, prices[0].series.request)
    proxies = _proxies(loader, body, prices)
    _execution_selection(prices, proxies, definition)
    decisions, features = _decisions(
        bundle, definition, slots, visibility, (membership, prices, auxiliary, proxies)
    )
    opening, closing = _outcomes(prices, dates, visibility)
    sources = tuple(
        asdict(pin)
        for pin in sorted(
            loader.sources,
            key=lambda pin: (pin.source_id, pin.source_sha256, pin.table, pin.table_digest),
        )
    )
    inputs = EnvelopeInputs(
        dates,
        opening,
        closing,
        {receipt.as_of: _targets(receipt, proxies, definition) for receipt in decisions},
        _types(workspace, prices),
        sources,
    )
    _target_memberships(inputs.targets, slots, prices, plan.sessions, visibility)
    envelope = export_envelope(request.parsed, projection=projection, inputs=inputs)
    if environment_identity() != environment:
        raise ValueError("calculation context changed during preparation")
    provenance = canonical_json_bytes(
        {
            "schema": "aas-prepared-backtest-v1",
            "hash_format": _J,
            "request_hash": projection.request_hash,
            "request": decode_json(projection.canonical_bytes),
            "envelope_sha256": envelope.envelope_sha256,
            "definition": definition,
            "conventions": tuple(decode_json(raw) for raw in conventions),
            "stored_inputs": loader.evidence,
            "source_pins": sources,
            "slots": slots,
            "decisions": decisions,
            "features": {
                day.isoformat(): {
                    asset: {
                        "as_of": value.as_of,
                        "latest_price": value.latest_price,
                        "returns": tuple(sorted(value.returns.items())),
                        "ma_ratios": tuple(sorted(value.ma_ratios.items())),
                        "momentum": value.momentum,
                    }
                    for asset, value in values.items()
                }
                for day, values in features.items()
            },
            "certified": False,
        }
    )
    return PreparedBacktest(
        request, definition, slots, decisions, features, inputs, projection, envelope, provenance
    )


# A declared run is named by its own content, so two identical declarations over one
# installation name one run rather than looking like two results.
RESEARCH_RUN_ID_SCHEMA = "aas-research-run-id-v1"
# The declared path's own decisions are made here and in the contract module, and the
# engine identity covers neither, so a declared run names their source itself.
RESEARCH_SOURCE_SCHEMA = "aas-research-sources-v1"
RESEARCH_SOURCE_MODULES = (
    "aegis_alpha.application.backtest_prepare",
    "aegis_alpha.application.research_run",
)
# What this installation can actually carry out. The engine contract evaluates at the
# prior calendar month end and the accounting fills at the next supplied session open,
# so a declaration naming anything else would be sealed over a different calculation.
_HONOURED_BASIS = "M"


def research_source_identity() -> str:
    """Hash the modules that decide a declared run, so its identity tracks its code."""
    root = files("aegis_alpha")
    return content_sha256(
        {
            "schema": RESEARCH_SOURCE_SCHEMA,
            "hash_format": _J,
            "files": [
                {
                    "module": module,
                    "sha256": hashlib.sha256(
                        root.joinpath(
                            module.removeprefix("aegis_alpha.").replace(".", "/") + ".py"
                        ).read_bytes()
                    ).hexdigest(),
                }
                for module in RESEARCH_SOURCE_MODULES
            ],
        }
    )


def _require_honourable(declaration: ResearchRunRequest) -> None:
    """Refuse a declaration this installation cannot actually carry out.

    A declaration that runs but is not what ran is worse than a refusal: the sealed
    document would describe one calculation while the envelope performed another. The
    engine evaluates at the prior calendar month end, so a daily basis would be sealed
    over a monthly schedule, and the accounting fills at the next supplied session
    open, so a decision-close fill would be sealed over a next-open execution.
    """
    if declaration.semantics.data_basis != _HONOURED_BASIS:
        raise ValueError(
            "this installation evaluates at month end; a "
            + declaration.semantics.data_basis
            + " basis cannot be honoured"
        )
    if declaration.semantics.fill_price != FILL_CONVENTION:
        raise ValueError(
            "the accounting fills at the next supplied session open; "
            + declaration.semantics.fill_price
            + " cannot be honoured"
        )


@dataclass(frozen=True, slots=True)
class PreparedResearchRun:
    """A declared uncertified research run: its decisions, its envelope, its declaration."""

    declaration: ResearchRunRequest
    definition: ExecutionDefinition
    slots: tuple[DecisionSlot, ...]
    decisions: tuple[ReplayReceipt, ...]
    inputs: EnvelopeInputs
    envelope: EnvelopeExport
    provenance: bytes
    certified: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Freeze what a caller could otherwise edit after the bytes were sealed.

        The envelope and the declaration are already immutable bytes. The decision and
        price mappings beside them were not, so a caller could change what the result
        appears to have run on while its sealed hashes stayed the same.
        """
        object.__setattr__(self, "slots", tuple(self.slots))
        object.__setattr__(self, "decisions", tuple(self.decisions))
        values = self.inputs
        object.__setattr__(
            self,
            "inputs",
            EnvelopeInputs(
                tuple(values.dates),
                cast("tuple[Mapping[str, float], ...]", _frozen(values.opens)),
                cast("tuple[Mapping[str, float], ...]", _frozen(values.closes)),
                cast("Mapping[date, Mapping[str, float]]", _frozen(values.targets)),
                cast("Mapping[str, str]", _frozen(values.instrument_types)),
                tuple(values.source_pins),
            ),
        )

    @property
    def run_id(self) -> str:
        """The run's own content, not a fresh name.

        A generated identifier would make two identical runs look like two results. This
        one is derived from the declaration, the envelope it produced and the sealed
        provenance, so the same declaration over the same installation always names the
        same run, and any change to inputs, conventions or engine identity names a
        different one. That is what makes a requery meaningful without a run record.
        """
        return "research-" + content_sha256(
            {
                "schema": RESEARCH_RUN_ID_SCHEMA,
                "hash_format": _J,
                "declaration_sha256": self.declaration.request_sha256,
                "envelope_sha256": self.envelope.envelope_sha256,
                "provenance_sha256": hashlib.sha256(self.provenance).hexdigest(),
            }
        )


@dataclass(frozen=True, slots=True)
class _Observed:
    """One pinned observation panel, already resolved onto declared instruments.

    The engine never sees an aas-obs- series identifier. Mapping happens here, once,
    against the declaration, so an unmapped series is a refusal rather than a silently
    dropped asset the calculation would then run without.
    """

    role: str
    values: Mapping[str, Mapping[date, float]]
    observed: Mapping[str, Mapping[date, date]]
    known_us: Mapping[str, Mapping[date, int]]


def _declared_pin(reference: object) -> GenerationPin:
    return GenerationPin(
        *(
            getattr(reference, name)
            for name in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
        )
    )


def _mapped_panel(
    series: PinnedObservationSeries,
    declaration: ResearchRunRequest,
    visibility: _Visibility,
    role: str,
) -> _Observed:
    """Project one panel at the declared ceiling and resolve its series onto instruments."""
    values: dict[str, dict[date, float]] = {}
    observed: dict[str, dict[date, date]] = {}
    known: dict[str, dict[date, int]] = {}
    # Observed-snapshot projection deliberately ignores knowledge times, so the declared
    # ceiling has to be applied before head selection rather than after. Filtering later
    # would drop a future-known revision and lose the value it superseded; filtering here
    # leaves the older revision as the head, which is what was knowable at the ceiling.
    admissible = replace(series, history=visibility.candidates(series.history, visibility.ceiling))
    for row in admissible.project_as_of(visibility.ceiling, mode=visibility.mode).rows:
        name = _text(row["instrument_id"])
        instrument = declaration.instrument_map.get(name)
        if instrument is None:
            raise ValueError("observation series " + name + " has no declared instrument mapping")
        if row["value_state"] != "present":
            continue
        if row["available_at_us"] is not None and cast("int", row["available_at_us"]) > (
            visibility.ceiling
        ):
            # A row the panel itself says was unavailable at the declared instant is not
            # admitted by the declaration, which claims every row precedes it.
            continue
        at_us = cast("int", row["feature_at_us"])
        session = _utc_day(at_us)
        if session in values.setdefault(instrument, {}):
            raise ValueError("observation panel repeats one session for " + instrument)
        values[instrument][session] = _number(row["value"])
        # No knowledge time is manufactured: the panel carries none, so each row's own
        # economic session stands and the declaration records that axis as uncertified.
        observed.setdefault(instrument, {})[session] = visibility.observed(row, session)
        known.setdefault(instrument, {})[session] = at_us
    return _Observed(role, values, observed, known)


def _observation_panels(
    loader: _Loader, declaration: ResearchRunRequest, visibility: _Visibility
) -> dict[str, _Observed]:
    """Read every declared observation generation through its own uncertified reader."""
    loaded: dict[str, _Observed] = {}
    semantics: dict[str, tuple[str, ...]] = {}
    for declared in declaration.observations:
        pin = _declared_pin(declared)
        series = load_pinned_observations(loader.workspace, pin, budget=loader.budget)
        contract = _row(decode_json(series.definition.encode()))
        role = _text(contract["observation_role"])
        if role != declared.observation_role:
            raise ValueError("observation role disagrees with the declared pin")
        if (contract["price_role"], contract["certified"]) != ("reference", False):
            # A certified or canonical series belongs on the strict path, which admits
            # it through the native price transform this preparation never calls.
            raise ValueError("a research run admits only uncertified reference observations")
        if _text(contract["currency"]) != declaration.conventions.currency:
            raise ValueError("observation currency is not the declared account currency")
        loader.retain(pin.generation_id, series.history)
        if role in loaded:
            raise ValueError("two declared observations carry the same role")
        loaded[role] = _mapped_panel(series, declaration, visibility, role)
        semantics[role] = (
            *(_text(contract[key]) for key in ("basis", "adjustment", "value_domain")),
            canonical_json_bytes(contract["calendar_ref"]).decode(),
        )
    if sorted(loaded) != ["close", "open"]:
        # Signals read the close panel and fills read the open one. Without both, the
        # accounting would have to reuse one for the other and call it an execution.
        raise ValueError("a declared research run needs one open and one close panel")
    if len(set(semantics.values())) != 1:
        # Signals come from one panel and fills from the other, into one account. If the
        # two disagree on basis, adjustment or calendar, that account is marked in a
        # mixture nobody declared and the envelope would not say so.
        raise ValueError("the open and close panels disagree on basis, adjustment or calendar")
    return loaded


def _research_membership(
    loader: _Loader, declaration: ResearchRunRequest, bundle: EngineBundle
) -> EnsembleMembership:
    """Resolve the membership the strategy's own contract already names."""
    reference = declaration.membership
    raw = read_definition(
        loader.workspace,
        DefinitionPin(reference.kind, reference.id, reference.version, reference.hash),
        budget=loader.budget,
    )
    body = _row(decode_json(raw))
    membership = EnsembleMembership(
        tuple(
            MembershipRow(_text(row["name"]), Decimal(_text(row["weight"])))
            for row in _rows(body["rows"])
        ),
        _text(body["membership_sha256"]),
    )
    if {row.name for row in membership.rows} != {
        strategy.name for strategy in bundle.contract.pack
    }:
        raise ValueError("ensemble membership must name the exact strategy pack")
    if bundle.contract.ensemble_membership_reference != "ensemble:" + membership.membership_sha256:
        raise ValueError("ensemble membership digest disagrees with strategy")
    loader.retain("membership:" + reference.id, body)
    return membership


def _research_decisions(
    bundle: EngineBundle,
    definition: ExecutionDefinition,
    slots: tuple[DecisionSlot, ...],
    visibility: _Visibility,
    loaded: tuple[EnsembleMembership, Mapping[str, _Observed]],
) -> tuple[ReplayReceipt, ...]:
    """Evaluate the registered strategy at each decision through the ordinary engine.

    Each decision sees only the sessions at or before its own cutoff, so nothing later
    than the decision reaches its features. The knowledge axis stays declared and the
    economic axis stays honest, which is exactly the split the declaration records.
    """
    membership, panels = loaded
    close = panels["close"]
    receipts = []
    for slot in slots:
        points = {
            instrument: tuple(
                PricePoint(session, value, close.observed[instrument][session])
                for session, value in sorted(series.items())
                if close.known_us[instrument][session] <= slot.cutoff_us
            )
            for instrument, series in close.values.items()
        }
        _warmup(definition, points, slot, visibility)
        receipts.append(
            replay(
                bundle,
                ReplayRequest(slot.decision_date, slot.decision_date, points, {}, {}, membership),
                knowledge_as_of=_utc_day(slot.cutoff_us),
            )
        )
    return tuple(receipts)


def _research_outcomes(
    panels: Mapping[str, _Observed], dates: tuple[date, ...]
) -> tuple[tuple[Mapping[str, float], ...], tuple[Mapping[str, float], ...]]:
    """Mark the period from the separately pinned open and close panels."""
    marked = {
        role: tuple(
            {
                instrument: series[day]
                for instrument, series in panels[role].values.items()
                if day in series
            }
            for day in dates
        )
        for role in ("open", "close")
    }
    return marked["open"], marked["close"]


def _research_targets(
    receipt: ReplayReceipt, definition: ExecutionDefinition
) -> Mapping[str, float]:
    """Take the ensemble weights as instruments. No proxy stands in for a logical asset."""
    result: dict[str, float] = {}
    for logical, weight in receipt.ensemble.items():
        if logical not in definition.cash_asset_ids and weight > 0:
            result[logical] = result.get(logical, 0.0) + weight
    return MappingProxyType(result)


def _research_envelope(declaration: ResearchRunRequest, inputs: EnvelopeInputs) -> EnvelopeExport:
    """Write the accounting envelope directly, because no executable request can hold it.

    An aas-backtest-request-v1 refuses to name a reference series as an execution input,
    which is correct and stays that way. The declaration is what stands behind these
    bytes instead, and the sealed provenance says so.
    """
    document = {
        "schema_version": "aas-etf-backtest-v1",
        "module": "aegis",
        "instrument_types": dict(sorted(inputs.instrument_types.items())),
        "dates": [day.isoformat() for day in inputs.dates],
        "opens": [dict(sorted(row.items())) for row in inputs.opens],
        "closes": [dict(sorted(row.items())) for row in inputs.closes],
        "targets": {
            day.isoformat(): dict(sorted(weights.items()))
            for day, weights in sorted(inputs.targets.items())
        },
        "initial_cash": declaration.execution.initial_cash,
        "cost": declaration.execution.cost,
        "source_pins": [],
        "research_mode": "observed_etf_research",
    }
    raw = canonical_json_bytes(document)
    return EnvelopeExport(raw, hashlib.sha256(raw).hexdigest())


def _require_fillable(inputs: EnvelopeInputs) -> None:
    """Refuse a target the panel cannot fill, before the accounting has to discover it.

    A sparse panel produces an envelope the accounting rejects, which is the right
    outcome reached the wrong way: the message names an arithmetic failure rather than
    the missing observation. Nothing is filled in here; the run is refused instead.
    """
    following = dict(pairwise(inputs.dates))
    for day, weights in sorted(inputs.targets.items()):
        execution = following.get(day)
        if execution is None:
            # The last decision has no session to fill on, which the accounting treats
            # as no trade rather than as an error.
            continue
        prices = inputs.opens[inputs.dates.index(execution)]
        missing = sorted(set(weights) - set(prices))
        if missing:
            raise ValueError(
                "the open panel has no observation on "
                + execution.isoformat()
                + " for: "
                + ", ".join(missing)
            )


def prepare_research_run(
    workspace: Workspace, declaration: ResearchRunRequest, *, budget: ComputeBudget
) -> PreparedResearchRun:
    """Prepare one declared uncertified research run over pinned observations.

    This is the opt-in counterpart of prepare_backtest, and it is opt-in in the only way
    that matters: it is a separate entry point the executable path never reaches.
    admit_native_input still refuses the observation transform, the price reader still
    refuses the pin for domain, and _reject_observation_contract still closes the derived
    route. None of them is called here. The panel is read through
    load_pinned_observations, the reader written for reference data, which returns
    nothing at all under strict PIT.

    The declaration is the whole provenance. An aas-backtest-request-v1 cannot describe
    this run, because that contract requires execution prices to be canonical and
    unadjusted and a reference observation is neither, so nothing here pretends one
    stands behind it. Does not register a request, record a run, install a schema,
    execute accounting, or fabricate a price, a knowledge time or a session.

    Not every declared field is checkable, and the ones that are not stay assertions
    rather than being presented as verified. Checked against stored evidence: the
    knowledge time against the projection ceiling, the currency and role against each
    observation contract, the instrument map against the series the panel actually
    carries, the strategy pin against the private store, the membership digest against
    the strategy contract, and the basis and fill convention against what this
    installation can carry out. Recorded and not checked: the prose conventions and the
    strategy semantics the engine does not consume, which is why the sealed document
    keeps source_parity at unknown.
    """
    engine, environment = calculation_identity(), environment_identity()
    _require_honourable(declaration)
    bundle, definition = _stored_strategy(
        workspace,
        StrategyPin(
            declaration.strategy_store_id,
            declaration.strategy_id,
            declaration.strategy_version,
            declaration.strategy_raw_sha256,
            declaration.strategy_contract_sha256,
        ),
    )
    visibility = _Visibility(
        "observed_snapshot_research",
        declaration.conventions.knowledge_time_us,
        None,
        declaration.history.start,
        declaration.history.end,
    )
    loader = _Loader(workspace, budget, {})
    plan = _plan(
        loader,
        definition,
        visibility,
        _Period(
            _declared_pin(declaration.sessions),
            None,
            declaration.period.start,
            declaration.period.end,
            0,
            None,
        ),
    )
    membership = _research_membership(loader, declaration, bundle)
    if definition.derived_series or bundle.contract.macro_signals:
        # The declaration pins observations and nothing else, so a strategy that reads a
        # macro series or a derived one would reach the engine short of an input it was
        # told to expect. Refused here rather than failing inside replay.
        raise ValueError(
            "a declared research run supplies only observed prices; "
            "this strategy also requires macro or derived inputs"
        )
    panels = _observation_panels(loader, declaration, visibility)
    decisions = _research_decisions(
        bundle, definition, plan.slots, visibility, (membership, panels)
    )
    opening, closing = _research_outcomes(panels, plan.dates)
    inputs = EnvelopeInputs(
        plan.dates,
        opening,
        closing,
        {receipt.as_of: _research_targets(receipt, definition) for receipt in decisions},
        _instrument_types(workspace, sorted(set(declaration.instrument_map.values()))),
        (),
    )
    _require_fillable(inputs)
    envelope = _research_envelope(declaration, inputs)
    if environment_identity() != environment:
        raise ValueError("calculation context changed during preparation")
    provenance = declared_provenance(
        declaration,
        PreparationRecord(
            envelope_sha256=envelope.envelope_sha256,
            engine=engine,
            environment=environment,
            preparation_source_sha256=research_source_identity(),
            resolved_calendar=_session_calendar(plan.sessions),
        ),
    )
    return PreparedResearchRun(
        declaration, definition, plan.slots, decisions, inputs, envelope, provenance
    )


def _decisions(
    bundle: EngineBundle,
    definition: ExecutionDefinition,
    slots: tuple[DecisionSlot, ...],
    visibility: _Visibility,
    loaded: tuple[
        EnsembleMembership, tuple[_Prices, ...], tuple[_Auxiliary, ...], tuple[_Proxy, ...]
    ],
) -> tuple[tuple[ReplayReceipt, ...], Mapping[date, Mapping[str, AssetFeatures]]]:
    membership, prices, auxiliary, proxies = loaded
    receipts = []
    features = {}
    for slot in slots:
        points = _price_points(prices, visibility, slot)
        points.update(
            {proxy.logical: _proxy_points(proxy, visibility, slot.cutoff_us) for proxy in proxies}
        )
        _warmup(definition, points, slot, visibility)
        macro, derived = _aux_inputs(auxiliary, visibility, slot)
        knowledge_as_of = _utc_day(slot.cutoff_us)
        features[slot.decision_date] = build_feature_matrix(
            points,
            FeatureBuildRequest(
                definition.feature_matrix,
                slot.decision_date,
                definition.stale_gates,
                definition.calendar.current_month_drop_before_day,
                definition.calendar.history_observations,
            ),
            knowledge_as_of=knowledge_as_of,
        )
        receipts.append(
            replay(
                bundle,
                ReplayRequest(
                    slot.decision_date, slot.decision_date, points, macro, derived, membership
                ),
                knowledge_as_of=knowledge_as_of,
            )
        )
    return tuple(receipts), features
