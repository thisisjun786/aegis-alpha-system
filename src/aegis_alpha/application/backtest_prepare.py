"""SELECT-only preparation of exact stored research inputs, never fills or run storage.

The caller owns the admitted workspace and compute lease. Results are detached;
observed snapshots remain uncertified. T17 owns parsing, semantic identity and
legacy export; the engine receives only explicit, decision-local values.
"""

from __future__ import annotations

import hashlib
import math
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

from aegis_alpha.application.backtest_cli import DECLARED_RESEARCH_MODE
from aegis_alpha.application.research_run import (
    FILL_CONVENTION,
    MacroGrant,
    MembershipRef,
    PreparationRecord,
    ResearchRunRequest,
    SleeveRef,
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
from aegis_alpha.engine.fx_conversion import FxConversion, FxFixing, FxRates
from aegis_alpha.engine.membership import MembershipRow
from aegis_alpha.engine.models import ENGINE_CONTRACT_VERSION_V1
from aegis_alpha.engine.replay import ReplayReceipt, ReplayRequest, replay
from aegis_alpha.engine.requirements import ExecutionDefinition
from aegis_alpha.engine.schedule import DecisionSlot, ScheduleRequest, Session, decision_slots
from aegis_alpha.engine.signals import MacroPoint
from aegis_alpha.storage import market
from aegis_alpha.storage.adjusted_prices import AdjustedRead, load_adjusted_prices
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
    check_sessions,
    load_pinned_heads,
    load_pinned_observations,
    load_pinned_prices,
    load_pinned_proxy,
    load_pinned_revisions,
    load_pinned_sessions,
    verify_head_binding,
    verify_sealed_publication,
)
from aegis_alpha.storage.membership_pins import IdentityPin, UniversePin, read_membership_pins
from aegis_alpha.storage.read_heads import (
    BINDING_SCHEMA,
    HeadBinding,
    HeadQuery,
    HeadRead,
    RevisionRead,
    head_binding,
    project_revisions,
)
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
    "ResearchDecision",
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
    "aegis_alpha.engine.fx_conversion",
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
    "aegis_alpha.storage.adjusted_prices",
    "aegis_alpha.storage.import_document",
    "aegis_alpha.storage.input_pins",
    "aegis_alpha.storage.market",
    "aegis_alpha.storage.market_inputs",
    "aegis_alpha.storage.market_schema",
    "aegis_alpha.storage.membership_pins",
    "aegis_alpha.storage.read_heads",
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

    def project_revisions(self, read: RevisionRead, cutoff: int) -> History:
        """``project`` over a revision read, with its binding's grants and exclusions."""
        strict = self.mode == "strict_pit"
        if read.strict != strict:
            raise ValueError("revision read mode disagrees with the request cutoff mode")
        revisions = (
            read.revisions
            if strict
            else tuple(
                item
                for item in read.revisions
                if item.values["revision_known_at_us"] is None
                or cast("int", item.values["revision_known_at_us"]) <= cutoff
            )
        )
        heads = project_revisions(
            revisions,
            strict=strict,
            cutoff_us=cutoff if strict else None,
            ingestion_cutoff_us=self.ingestion,
        )
        return tuple(
            MappingProxyType(dict(head.values))
            for head in heads
            if head.values["available_at_us"] is None
            or cast("int", head.values["available_at_us"]) <= cutoff
        )

    def query(
        self,
        cutoff: int,
        subjects: tuple[str, ...],
        start: date,
        end: date,
        roles: tuple[str, ...] | None = None,
    ) -> HeadQuery:
        """One head read at ``cutoff``: strict PIT, or research under a knowledge ceiling."""
        strict = self.mode == "strict_pit"
        return HeadQuery(
            cutoff_us=cutoff if strict else None,
            ingestion_cutoff_us=self.ingestion,
            known_ceiling_us=None if strict else cutoff,
            subjects=subjects,
            from_date=start,
            to_date=end + timedelta(days=1),
            price_roles=roles,
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


@dataclass(frozen=True, slots=True)
class _Calendar:
    """The pinned sessions, which every decision projects at its own cutoff.

    ``history`` is every revision (the schedule's candidate sessions). A head-bound
    calendar also keeps its revision read, so each projection applies the binding's
    grants and exclusions exactly as ``read_heads`` would at that cutoff.
    """

    history: History
    revisions: RevisionRead | None = None
    pin: GenerationPin | None = None

    def project(self, visibility: _Visibility, cutoff: int) -> History:
        if self.revisions is None:
            return visibility.project(self.history, cutoff)
        return visibility.project_revisions(self.revisions, cutoff)


def _sessions(calendar: _Calendar, visibility: _Visibility, cutoff: int) -> tuple[Session, ...]:
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
            calendar.project(visibility, cutoff), key=lambda row: _day(row["session_date"])
        )
    )


def _calendar(
    loader: _Loader, convention: Row, visibility: _Visibility, period_end: date
) -> _Calendar:
    """Load the pinned sessions: a native sessions generation, or a head-bound calendar.

    A head-bound calendar is read once as its revisions over the request's window (the
    history start through the period end), so each decision projects the calendar known at
    its own cutoff under the binding's grants. Its rows must be the convention's calendar.
    """
    if loader.bindings["sessions", 0]["ref_kind"] == "generation":
        pin = _generation(loader.bindings["sessions", 0])
        loader.native(pin, "aas-sessions-transform-v1")
        history = load_pinned_sessions(loader.workspace, pin, budget=loader.budget).history
        return _Calendar(history, pin=pin)
    binding = loader.binding(("sessions", 0))
    read = load_pinned_revisions(
        loader.workspace,
        binding,
        HeadQuery(
            ingestion_cutoff_us=visibility.ingestion,
            subjects=(_text(convention["calendar_id"]),),
            from_date=visibility.history_start,
            to_date=period_end + timedelta(days=1),
        ),
        strict=visibility.mode == "strict_pit",
        budget=loader.budget,
    )
    loader.head_read(("sessions", 0), "calendar", None, read.receipt, read.receipt_hash)
    history = tuple(item.values for item in read.revisions)
    # Every revision stays live for each decision's projection.
    loader.hold(history)
    for row in history:
        if any(row[key] != convention[key] for key in ("calendar_id", "venue", "timezone_version")):
            raise ValueError("session calendar/venue/timezone conflicts with request")
    check_sessions(history)
    return _Calendar(history, read)


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
    reads: list[Row] = field(default_factory=list)
    charge: int = 0

    def retain(self, name: str, value: object) -> None:
        self._charge(value)
        self.evidence.append({"name": name, "value": value})

    def hold(self, value: object) -> None:
        """Charge a history the preparation keeps live but does not seal as evidence."""
        self._charge(value)

    def _charge(self, value: object) -> None:
        self.charge += len(canonical_json_bytes(value)) * 32
        if self.charge > self.budget.available_bytes:
            raise ComputeResourceError("prepared histories exceed aggregate materialization budget")

    def head_read(  # noqa: PLR0913 -- one read: where, why, when, its receipt and notes
        self,
        key: tuple[str, int],
        purpose: str,
        decision: date | None,
        receipt: Mapping[str, object],
        receipt_hash: str,
        *,
        notes: Mapping[str, object] | None = None,
    ) -> None:
        """Record one head read's receipt exactly as the reader returned it.

        ``notes`` adds what the preparation concluded from the read beside its receipt.
        """
        entry = {
            "role": key[0],
            "ordinal": key[1],
            "purpose": purpose,
            "decision_date": None if decision is None else decision.isoformat(),
            "receipt": dict(receipt),
            "receipt_sha256": receipt_hash,
            **(notes or {}),
        }
        self._charge(entry)
        self.reads.append(entry)

    def binding(self, key: tuple[str, int]) -> HeadBinding:
        """The head binding a ``heads`` reference names, already admitted by the request."""
        ref = self.bindings[key]
        binding = head_binding({"schema": BINDING_SCHEMA, **_row(ref["pin"])})
        if binding.binding_hash != ref["hash"]:
            raise ValueError("head binding reference disagrees with its document")
        verify_head_binding(self.workspace, binding, budget=self.budget)
        return binding

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
class _HeadPrices:
    """One price selection read through a head binding, decision by decision.

    ``actions`` is set for a canonical selection whose basis is adjusted: those prices are
    derived from the binding's unadjusted bars and the corporate actions known at each
    cutoff, never read from a provider's adjusted series. ``actions_key`` is the request
    binding it came from.
    """

    binding: HeadBinding
    actions: HeadBinding | None
    actions_key: tuple[str, int] | None
    instruments: tuple[str, ...]
    currency: str
    basis: str
    price_role: str


@dataclass(frozen=True, slots=True)
class _Prices:
    role: str
    ordinal: int
    series: PinnedPriceSeries | None
    sources: tuple[SourcePin, ...]
    identities: History
    universe: History
    heads: _HeadPrices | None = None

    @property
    def instruments(self) -> tuple[str, ...]:
        if self.heads is not None:
            return self.heads.instruments
        return cast("PinnedPriceSeries", self.series).request.instrument_ids

    @property
    def currency(self) -> str:
        if self.heads is not None:
            return self.heads.currency
        return cast("PinnedPriceSeries", self.series).request.currency


@dataclass(frozen=True, slots=True)
class _Fx:
    """One granted FX conversion and the ``fx_conversion`` head binding of its fixings."""

    conversion: FxConversion
    binding: HeadBinding
    key: tuple[str, int]
    # A declared research run's own price binding for this currency's bars, if it names one.
    prices: HeadBinding | None = None


@dataclass(slots=True)
class _Conversions:
    """The request's FX conversions by price currency, and what each actually converted.

    Signal reads are converted per decision and recorded beside each read's receipt. The
    execution prices are converted once over the period; the fixing applied to each
    session and the cells no fixing could convert are kept for the sealed record.
    """

    account: str
    by_currency: Mapping[str, _Fx]
    applied: dict[str, dict[date, FxFixing]] = field(default_factory=dict)
    unconverted: dict[str, set[tuple[str, date]]] = field(default_factory=dict)

    def record(self) -> list[dict[str, object]]:
        return [
            {
                "binding": {"role": fx.key[0], "ordinal": fx.key[1]},
                "binding_hash": fx.binding.binding_hash,
                "conversion": fx.conversion.document(),
                "fixings": [
                    [day.isoformat(), fixing.day.isoformat(), fixing.rate]
                    for day, fixing in sorted(self.applied.get(currency, {}).items())
                ],
                "unconverted": [
                    [instrument, day.isoformat()]
                    for instrument, day in sorted(self.unconverted.get(currency, set()))
                ],
            }
            for currency, fx in sorted(self.by_currency.items())
        ]


def _conversions(loader: _Loader, body: Row) -> _Conversions:
    """The FX conversions the request grants, each with its admitted head binding."""
    account = _text(_row(body["account"])["currency"])
    result = {}
    for row in _rows(body.get("fx_conversions", ())):
        key = _row(row["binding"])
        position = (_text(key["role"]), cast("int", key["ordinal"]))
        conversion = FxConversion(
            _text(row["currency"]),
            account,
            _text(row["series_id"]),
            cast("int", row["max_fixing_age_days"]),
            _text(row["signal_basis"]),
        )
        result[conversion.currency] = _Fx(conversion, loader.binding(position), position)
    return _Conversions(account, MappingProxyType(result))


def _fx_rates(
    loader: _Loader, fx: _Fx, visibility: _Visibility, cutoff: int, window: tuple[date, date]
) -> tuple[FxRates, HeadRead]:
    """The fixings a conversion may apply, as known at ``cutoff``.

    The read reaches back the conversion's maximum fixing age before the window, so the
    window's first value can use the fixing before it. A fixing is used only when it is
    present, available by the cutoff and fixed by it: a rate fixed after a decision is
    not one that decision could have applied.
    """
    conversion = fx.conversion
    start, end = window
    read = load_pinned_heads(
        loader.workspace,
        fx.binding,
        visibility.query(
            cutoff,
            (conversion.series_id,),
            start - timedelta(days=conversion.max_fixing_age_days),
            end,
        ),
        budget=loader.budget,
    )
    fixings = []
    for head in read.rows:
        row = head.values
        if _text(row["base_currency"]) + "/" + _text(row["quote_currency"]) != (
            conversion.series_id
        ):
            raise ValueError("an FX read returned a fixing of another series")
        if (
            row["value_state"] != "present"
            or cast("int", row["fixing_at_us"]) > cutoff
            or (row["available_at_us"] is not None and cast("int", row["available_at_us"]) > cutoff)
        ):
            continue
        fixings.append(FxFixing(_utc_day(row["fixing_at_us"]), _number(row["rate"])))
    return FxRates(conversion, fixings), read


def _record_fx(  # noqa: PLR0913 -- one fixing read and what it converted
    loader: _Loader,
    fx: _Fx,
    purpose: tuple[str, date | None],
    read: HeadRead,
    *,
    converted: int,
    unconverted: Iterable[tuple[str, date]],
) -> None:
    loader.head_read(
        fx.key,
        purpose[0],
        purpose[1],
        read.receipt,
        read.receipt_hash,
        notes={
            "fx_conversion": fx.conversion.document(),
            "converted": converted,
            "unconverted": [
                [instrument, day.isoformat()] for instrument, day in sorted(set(unconverted))
            ],
        },
    )


def _memberships(loader: _Loader) -> tuple[IdentityPin, UniversePin, History, History]:
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
    return (
        identity,
        universe,
        memberships.identity.members if memberships.identity is not None else (),
        memberships.universe.members if memberships.universe is not None else (),
    )


def _head_instruments(loader: _Loader, instruments: tuple[str, ...], calendar: Row) -> None:
    """The same identity and venue admission ``load_pinned_prices`` applies to its request."""
    for instrument in instruments:
        identity = loader.workspace.state.execute(
            "SELECT asset_type, venue FROM instruments WHERE instrument_id=?", (instrument,)
        ).fetchone()
        if (
            identity is None
            or identity["venue"] != calendar["venue"]
            or identity["asset_type"] == "proxy"
        ):
            raise ValueError("unknown or incompatible instrument identity/venue")


def _prices(loader: _Loader, body: Row, calendar: Row, sessions: _Calendar) -> tuple[_Prices, ...]:
    identity, universe, identities, members = _memberships(loader)
    history, period = _row(body["history"]), _row(body["period"])
    start = _day(history["start"])  # Request validation places this before period.start.
    end = max(_day(history["end"]), _day(period["end"]))
    grid = tuple(
        sorted(
            {day for row in sessions.history if start <= (day := _day(row["session_date"])) <= end}
        )
    )
    result = []
    # Derived selections take the actions bindings by ordinal, in binding order.
    derived_count = 0
    for selection in _rows(body["price_inputs"]):
        key = _row(selection["binding"])
        role, ordinal = _text(key["role"]), cast("int", key["ordinal"])
        instruments = cast("tuple[str, ...]", selection["instrument_ids"])
        if loader.bindings[role, ordinal]["ref_kind"] == "heads":
            _head_instruments(loader, instruments, calendar)
            actions = None
            if (
                role == "signal_prices"
                and selection["price_role"] == "canonical"
                and selection["basis"] != "unadjusted"
            ):
                actions = ("actions", derived_count)
                derived_count += 1
            heads = _HeadPrices(
                loader.binding((role, ordinal)),
                None if actions is None else loader.binding(actions),
                actions,
                instruments,
                _text(selection["currency"]),
                _text(selection["basis"]),
                _text(selection["price_role"]),
            )
            result.append(_Prices(role, ordinal, None, (), identities, members, heads))
            continue
        if sessions.pin is None:
            raise ValueError(
                "a native price generation is read against a sessions generation, "
                "not a head-bound calendar"
            )
        pin = _generation(_selection_ref(selection, loader.bindings))
        admitted = admit_native_input(
            loader.workspace, pin, expected_schema="aas-price-transform-v1", budget=loader.budget
        )
        loader.sources.update(dict.fromkeys(admitted.source_pins))
        loader.retain(pin.generation_id, admitted.history)
        request = PriceInputRequest(
            pin,
            sessions.pin,
            instruments,
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
        series = load_pinned_prices(loader.workspace, request, budget=loader.budget)
        result.append(
            _Prices(
                role,
                ordinal,
                series,
                admitted.source_pins,
                series.identities,
                series.universe,
            )
        )
    return tuple(result)


def _head_bars(
    loader: _Loader, item: _Prices, read: tuple[str, HeadQuery], decision: date | None
) -> tuple[Row, ...]:
    """One head read of a price selection: provider bars, or bars derived at the cutoff."""
    heads = cast("_HeadPrices", item.heads)
    purpose, query = read
    if heads.actions is not None:
        adjusted = load_adjusted_prices(
            loader.workspace,
            heads.binding,
            heads.actions,
            replace(query, price_roles=("canonical",)),
            basis=heads.basis,
            budget=loader.budget,
        )
        absent = _no_action_source(adjusted, heads, strict=query.cutoff_us is not None)
        loader.head_read(
            (item.role, item.ordinal),
            purpose,
            decision,
            adjusted.receipt,
            adjusted.receipt_hash,
            notes={
                "actions": dict(
                    zip(
                        ("role", "ordinal"), cast("tuple[str, int]", heads.actions_key), strict=True
                    )
                ),
                "no_action_source": absent,
            },
        )
        return tuple(row.values for row in adjusted.rows)
    result = load_pinned_heads(
        loader.workspace,
        heads.binding,
        replace(query, price_roles=(heads.price_role,)),
        budget=loader.budget,
    )
    loader.head_read(
        (item.role, item.ordinal), purpose, decision, result.receipt, result.receipt_hash
    )
    # A dataset may carry several bases of one bar (a provider's split-adjusted and total
    # return references); the selection names one of them.
    return tuple(row.values for row in result.rows if row.values["basis"] == heads.basis)


NO_ACTION_GRANT = "absent_actions_as_none@1"


def _no_action_source(adjusted: AdjustedRead, heads: _HeadPrices, *, strict: bool) -> list[str]:
    """The instruments whose bars the actions read holds no evidence about.

    An instrument with bars but no corporate action, held or not, in the actions read is
    not known to have had none: the binding may simply not cover it. Its derived series is
    its unadjusted bars, so a strict preparation reads it only when the actions binding
    grants ``absent_actions_as_none@1``; every preparation records the instruments.
    """
    priced = {str(row.values["instrument_id"]) for row in adjusted.series.rows}
    evidenced = {str(row.values["instrument_id"]) for row in adjusted.actions.rows}
    evidenced |= {cell.instrument_id for cell in adjusted.actions.held}
    absent = sorted(priced - evidenced)
    granted = NO_ACTION_GRANT in cast("HeadBinding", heads.actions).granted_rules
    if absent and strict and not granted:
        raise ValueError(
            "no corporate action evidence for "
            + ", ".join(absent)
            + "; the actions binding grants "
            + NO_ACTION_GRANT
            + " to read an absent action as none"
        )
    return absent


def _admit_bar(row: Row, heads: _HeadPrices, seen: set[tuple[object, date]]) -> None:
    """Refuse a bar the selection cannot read as its one daily bar per session."""
    if row["currency"] != heads.currency:
        raise ValueError("price currency/basis/role/interval conflicts with request")
    if row["interval"] != "1d":
        raise ValueError("price currency/basis/role/interval conflicts with request")
    key = (row["instrument_id"], _day(row["session_date"]))
    if key in seen:
        raise ValueError("duplicate daily price identity")
    seen.add(key)


def _head_signal(
    loader: _Loader,
    item: _Prices,
    visibility: _Visibility,
    slot: DecisionSlot,
    calendar: _Calendar,
) -> History:
    """The signal bars a decision may use, read at its own cutoff.

    The same cells ``_eligible_prices`` admits: a present bar of an open session of the
    calendar known at the cutoff, no later than the decision and ended by the cutoff,
    inside the history window, whose instrument the identity and universe pins hold at
    the bar's end as known at the cutoff.
    """
    heads = cast("_HeadPrices", item.heads)
    cutoff = slot.cutoff_us
    end = min(visibility.history_end, slot.decision_date)
    if end < visibility.history_start:
        return ()
    opened = {
        session.session_date
        for session in _sessions(calendar, visibility, cutoff)
        if session.status == "open"
    }
    query = visibility.query(cutoff, heads.instruments, visibility.history_start, end)
    if heads.actions is not None:
        # The derivation needs the open sessions: a dividend whose prior session has no bar
        # cannot be reinvested at an older close.
        grid = tuple(sorted(day for day in opened if visibility.history_start <= day <= end))
        if not grid:
            return ()
        query = replace(query, grid=grid)
    seen: set[tuple[object, date]] = set()
    selected = []
    for row in _head_bars(loader, item, ("decision", query), slot.decision_date):
        _admit_bar(row, heads, seen)
        day = _day(row["session_date"])
        instrument = _text(row["instrument_id"])
        bar_end = cast("int", row["bar_end_us"])
        if (
            row["value_state"] == "present"
            and day in opened
            and day <= slot.decision_date
            and bar_end <= cutoff
            and (row["available_at_us"] is None or cast("int", row["available_at_us"]) <= cutoff)
            and visibility.history_start <= day <= visibility.history_end
            and _active(item.identities, instrument, bar_end, cutoff)
            and _active(item.universe, instrument, bar_end, cutoff)
        ):
            selected.append(row)
    return tuple(sorted(selected, key=lambda row: _day(row["session_date"])))


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


def _price_points(  # noqa: PLR0913 -- one decision's signal reads and their conversions
    loader: _Loader,
    prices: tuple[_Prices, ...],
    calendar: _Calendar,
    visibility: _Visibility,
    slot: DecisionSlot,
    *,
    conversions: _Conversions,
) -> dict[str, tuple[PricePoint, ...]]:
    result: dict[str, tuple[PricePoint, ...]] = {}
    for selected in prices:
        if selected.role != "signal_prices":
            continue
        rows = (
            _eligible_prices(
                cast("PinnedPriceSeries", selected.series),
                visibility,
                slot.cutoff_us,
                slot.decision_date,
            )
            if selected.heads is None
            else _head_signal(loader, selected, visibility, slot, calendar)
        )
        closes = _signal_closes(loader, selected, rows, visibility, slot, conversions=conversions)
        for instrument in selected.instruments:
            result[instrument] = tuple(
                PricePoint(day, value, visibility.observed(row, day))
                for row in rows
                if row["instrument_id"] == instrument
                and (value := closes.get((instrument, day := _day(row["session_date"]))))
                is not None
            )
    return result


def _signal_closes(  # noqa: PLR0913 -- one selection's signal bars and their conversion
    loader: _Loader,
    selected: _Prices,
    rows: History,
    visibility: _Visibility,
    slot: DecisionSlot,
    *,
    conversions: _Conversions,
) -> dict[tuple[str, date], float]:
    """Each signal bar's close in the currency the strategy's signals read it in.

    A selection in the account currency, or one whose conversion keeps signals in the
    price currency, reads its closes as stored. Otherwise the fixings known at the
    decision's cutoff convert each close; a bar no fixing converts is not a signal point,
    and the decision's read record names it.
    """
    closes = {
        (_text(row["instrument_id"]), _day(row["session_date"])): _number(row["close"])
        for row in rows
    }
    fx = conversions.by_currency.get(selected.currency)
    if fx is None or fx.conversion.signal_basis == "price_currency" or not closes:
        return closes
    window = (min(day for _, day in closes), max(day for _, day in closes))
    rates, read = _fx_rates(loader, fx, visibility, slot.cutoff_us, window)
    result = {}
    for cell, value in closes.items():
        converted = rates.convert(value, cell[1])
        if converted is not None:
            result[cell] = converted[0]
    _record_fx(
        loader,
        fx,
        ("decision", slot.decision_date),
        read,
        converted=len(result),
        unconverted=closes.keys() - result.keys(),
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
    # A head-bound macro or FX selection: read at each decision's cutoff.
    binding: HeadBinding | None = None
    key: tuple[str, int] = ("macro", 0)
    unit: str = ""


_FX_SERIES = re.compile(r"[A-Z]{3}/[A-Z]{3}")


def _macro_value(item: _Auxiliary, row: Row) -> tuple[object, date, object]:
    """A head-bound row's series, economic date and value, with its unit checked.

    A macro observation is keyed by series and period. An FX fixing is the series
    ``BASE/QUOTE`` dated by the UTC day of its fixing instant, and the unit a selection
    declares for it is the quote currency it is stated in.
    """
    if item.domain == "fx_rates":
        if row["quote_currency"] != item.unit:
            raise ValueError("an FX selection's unit is its quote currency")
        series = _text(row["base_currency"]) + "/" + _text(row["quote_currency"])
        return series, _utc_day(row["fixing_at_us"]), row["rate"]
    if row["unit"] != item.unit:
        raise ValueError("macro unit mismatch")
    return row["series_id"], _day(row["observation_period"]), row["value"]


def _head_macro(
    loader: _Loader, item: _Auxiliary, query: HeadQuery, decision: date | None
) -> History:
    read = load_pinned_heads(
        loader.workspace, cast("HeadBinding", item.binding), query, budget=loader.budget
    )
    purpose = "admission" if decision is None else "decision"
    loader.head_read(item.key, purpose, decision, read.receipt, read.receipt_hash)
    return tuple(row.values for row in read.rows)


def _head_auxiliary(loader: _Loader, selection: Row, visibility: _Visibility) -> _Auxiliary:
    key = _row(selection["binding"])
    position = (_text(key["role"]), cast("int", key["ordinal"]))
    binding = loader.binding(position)
    series = _text(selection["series_id"])
    if binding.domain == "fx_rates" and _FX_SERIES.fullmatch(series) is None:
        raise ValueError("an FX selection names its series BASE/QUOTE")
    item = _Auxiliary(
        series,
        binding.domain,
        (),
        series,
        "value",
        ("heads", binding.binding_hash),
        binding=binding,
        key=position,
        unit=_text(selection["unit"]),
    )
    _admit_head_macro(loader, item, visibility)
    return item


def _admit_head_macro(loader: _Loader, item: _Auxiliary, visibility: _Visibility) -> None:
    """Hold a head-bound series to carrying the series, in its unit, by the ceiling."""
    series = item.series
    # The pin must carry the series, as a native macro generation must; a series it does
    # not hold by the knowledge cutoff would otherwise reach replay as silence.
    rows = _head_macro(
        loader,
        item,
        visibility.query(
            visibility.ceiling, (series,), visibility.history_start, visibility.history_end
        ),
        None,
    )
    if not rows:
        raise ValueError("macro binding holds no head of " + series + " by the knowledge cutoff")
    for row in rows:
        _macro_value(item, row)


def _auxiliary(
    loader: _Loader,
    body: Row,
    definition: ExecutionDefinition,
    template: PriceInputRequest | None,
    visibility: _Visibility,
) -> tuple[_Auxiliary, ...]:
    result = []
    for selection in _rows(body["macro_inputs"]):
        if _selection_ref(selection, loader.bindings)["ref_kind"] == "heads":
            result.append(_head_auxiliary(loader, selection, visibility))
            continue
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
    return (*result, *_derived_auxiliary(loader, body, definition, template))


def _derived_auxiliary(
    loader: _Loader,
    body: Row,
    definition: ExecutionDefinition,
    template: PriceInputRequest | None,
) -> tuple[_Auxiliary, ...]:
    result = []
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
                if template is None:
                    raise ValueError(
                        "a derived price input is read like a native price generation, "
                        "which needs a native price selection and sessions generation"
                    )
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


def _head_aux_points(
    loader: _Loader, item: _Auxiliary, visibility: _Visibility, slot: DecisionSlot
) -> tuple[tuple[date, float, date], ...]:
    """A head-bound series as known at the decision's cutoff, within the history window."""
    cutoff = slot.cutoff_us
    end = min(visibility.history_end, slot.decision_date)
    if end < visibility.history_start:
        return ()
    rows = _head_macro(
        loader,
        item,
        visibility.query(cutoff, (item.series,), visibility.history_start, end),
        slot.decision_date,
    )
    result = []
    for row in rows:
        series, economic, value = _macro_value(item, row)
        if (
            series != item.series
            or row["value_state"] != "present"
            or (row["available_at_us"] is not None and cast("int", row["available_at_us"]) > cutoff)
            or not visibility.history_start <= economic <= end
        ):
            continue
        result.append((economic, _number(value), visibility.observed(row, economic)))
    if len({point[0] for point in result}) != len(result):
        raise ValueError("ambiguous auxiliary observation dates")
    return tuple(sorted(result))


def _aux_points(
    loader: _Loader, item: _Auxiliary, visibility: _Visibility, slot: DecisionSlot
) -> tuple[tuple[date, float, date], ...]:
    if item.binding is not None:
        return _head_aux_points(loader, item, visibility, slot)
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
    loader: _Loader, items: tuple[_Auxiliary, ...], visibility: _Visibility, slot: DecisionSlot
) -> tuple[dict[str, tuple[MacroPoint, ...]], dict[str, Mapping[str, object]]]:
    macro: dict[str, tuple[MacroPoint, ...]] = {}
    fields: dict[str, dict[str, object]] = {}
    identities: dict[str, dict[str, tuple[str, str]]] = {}
    for item in items:
        points = _aux_points(loader, item, visibility, slot)
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
        for instrument in item.instruments
    }
    currencies = {
        instrument: item.currency
        for item in prices
        if item.role == "execution_prices"
        for instrument in item.instruments
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
        _proxy_currency(transition, currencies, _text(_row(body["account"])["currency"]))
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


def _proxy_currency(transition: Row, currencies: Mapping[str, str], account: str) -> None:
    """Hold a proxy's selected donor and target to the account currency.

    A proxy's feature values carry no currency, so no conversion can state them in the
    account currency; a donor or target selected in another currency is refused by name.
    """
    for side in ("donor", "target"):
        instrument = _text(transition[side + "_id"])
        currency = currencies.get(instrument, account)
        if currency != account:
            raise ValueError(
                "proxy "
                + side
                + " "
                + instrument
                + " is selected in "
                + currency
                + ", not the account currency "
                + account
                + "; a proxy carries no currency"
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


def _execution_rows(
    loader: _Loader, selected: _Prices, dates: tuple[date, ...], visibility: _Visibility
) -> History:
    """The execution bars of the period, as known at the request's ceiling."""
    if selected.heads is None:
        return visibility.project(
            cast("PinnedPriceSeries", selected.series).history, visibility.ceiling
        )
    if not dates:
        return ()
    query = visibility.query(visibility.ceiling, selected.instruments, dates[0], dates[-1])
    rows = _head_bars(loader, selected, ("outcomes", query), None)
    seen: set[tuple[object, date]] = set()
    for row in rows:
        _admit_bar(row, selected.heads, seen)
    return tuple(
        row
        for row in rows
        if row["available_at_us"] is None
        or cast("int", row["available_at_us"]) <= visibility.ceiling
    )


def _outcomes(
    loader: _Loader,
    prices: tuple[_Prices, ...],
    dates: tuple[date, ...],
    visibility: _Visibility,
    conversions: _Conversions,
) -> tuple[tuple[Mapping[str, float], ...], tuple[Mapping[str, float], ...]]:
    """The period's execution prices in the account currency.

    A selection in another currency is converted with the fixings known at the request's
    ceiling, read once per currency over the period. A session no fixing converts has no
    price for that instrument, exactly as a missing bar has none; the sealed record lists
    those cells and the fixing applied to every other session.
    """
    opening: dict[date, dict[str, float]] = {day: {} for day in dates}
    closing: dict[date, dict[str, float]] = {day: {} for day in dates}
    outcome = _OutcomeFx(
        loader, conversions, visibility, (dates[0], dates[-1]) if dates else None, "outcomes"
    )
    for selected in prices:
        if selected.role != "execution_prices":
            continue
        # Outcomes are not signal knowledge at a past decision. They are the
        # separately pinned period observations under the request's global ceiling.
        for row in _execution_rows(loader, selected, dates, visibility):
            day = _day(row["session_date"])
            symbol = _text(row["instrument_id"])
            if (
                day not in opening
                or symbol not in selected.instruments
                or row["value_state"] != "present"
                or cast("int", row["bar_end_us"]) > visibility.ceiling
            ):
                continue
            for name, destination in (("open", opening), ("close", closing)):
                if row[name] is None:
                    continue
                value = outcome.value(selected.currency, symbol, day, _number(row[name]))
                if value is not None:
                    destination[day][symbol] = value
    outcome.record()
    return tuple(opening[day] for day in dates), tuple(closing[day] for day in dates)


@dataclass(slots=True)
class _OutcomeFx:
    """Conversions over one window at the request's ceiling: a fixing read per currency.

    The strict path converts its execution prices over the period this way; a declared
    research run converts its price panels over its whole read window.
    """

    loader: _Loader
    conversions: _Conversions
    visibility: _Visibility
    window: tuple[date, date] | None
    purpose: str
    rates: dict[str, tuple[FxRates, HeadRead]] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)

    def value(self, currency: str, symbol: str, day: date, value: float) -> float | None:
        """``value`` in the account currency, or None when no fixing converts it."""
        fx = self.conversions.by_currency.get(currency)
        if fx is None:
            return value
        if currency not in self.rates:
            self.rates[currency] = _fx_rates(
                self.loader,
                fx,
                self.visibility,
                self.visibility.ceiling,
                cast("tuple[date, date]", self.window),
            )
        converted = self.rates[currency][0].convert(value, day)
        if converted is None:
            self.conversions.unconverted.setdefault(currency, set()).add((symbol, day))
            return None
        self.conversions.applied.setdefault(currency, {})[day] = converted[1]
        self.counts[currency] = self.counts.get(currency, 0) + 1
        return converted[0]

    def record(self) -> None:
        for currency, (_, read) in sorted(self.rates.items()):
            _record_fx(
                self.loader,
                self.conversions.by_currency[currency],
                (self.purpose, None),
                read,
                converted=self.counts.get(currency, 0),
                unconverted=self.conversions.unconverted.get(currency, ()),
            )


def _types(workspace: Workspace, prices: tuple[_Prices, ...]) -> dict[str, str]:
    return _instrument_types(
        workspace,
        (
            instrument
            for item in prices
            if item.role == "execution_prices"
            for instrument in item.instruments
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


def _observation_types(
    workspace: Workspace, mapping: Mapping[str, str], observed: set[str]
) -> dict[str, str]:
    """Classify a declared run's instruments from the series they were read from.

    The declaration renames an observation series to the asset id a strategy knows, but
    it cannot change what the series is. The type therefore travels with the source: the
    store says research_observation and the envelope says OBSERVATION. Nothing is
    relabelled as an ETF to make the accounting accept it.

    A mapping entry naming a registered series that neither panel carries would put an
    instrument in the envelope that no observation stands behind, so the map has to name
    what was actually read and nothing else.
    """
    absent = sorted(set(mapping) - observed)
    if absent:
        raise ValueError(
            "instrument_map names series no pinned panel carries: " + ", ".join(absent)
        )
    result = {}
    for series, instrument in sorted(mapping.items()):
        row = workspace.state.execute(
            "SELECT asset_type FROM instruments WHERE instrument_id=?", (series,)
        ).fetchone()
        if row is None or row[0] != "research_observation":
            raise ValueError(
                "observation series is not a classified research observation: " + series
            )
        result[instrument] = "OBSERVATION"
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
    calendar: _Calendar, visibility: _Visibility, request: ScheduleRequest
) -> tuple[DecisionSlot, ...]:
    # Candidate dates come from retained history, not a future revised latest grid.
    # Every accepted pair is independently selected by T16 at its own ceiling.
    candidates = sorted(
        {
            (_day(row["session_date"]), cast("int", row["close_at_us"]))
            for row in calendar.history
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
        grid = _sessions(calendar, visibility, cutoff)
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
            _sessions(calendar, visibility, visibility.ceiling), request, resolved_months
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
        for instrument in item.instruments
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
    sessions: _Calendar,
    visibility: _Visibility,
) -> None:
    selected = {
        instrument: item
        for item in prices
        if item.role == "execution_prices"
        for instrument in item.instruments
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
    sessions = _calendar(loader, calendar, visibility, _day(period["end"]))
    schedule = ScheduleRequest(
        definition.calendar,
        _text(calendar["calendar_id"]),
        _text(calendar["venue"]),
        _text(calendar["timezone_version"]),
        _day(period["start"]),
        _day(period["end"]),
        cast("int", body["decision_latency_us"]),
        visibility.ceiling,
        None
        if body["explicit_decision_dates"] is None
        else tuple(_day(day) for day in cast("tuple[str, ...]", body["explicit_decision_dates"])),
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
    membership = _membership(loader, bundle)
    prices = _prices(loader, body, calendar, sessions)
    template = next((item.series.request for item in prices if item.series is not None), None)
    auxiliary = _auxiliary(loader, body, definition, template, visibility)
    proxies = _proxies(loader, body, prices)
    _execution_selection(prices, proxies, definition)
    conversions = _conversions(loader, body)
    decisions, features = _decisions(
        bundle,
        definition,
        slots,
        visibility,
        (loader, sessions, membership, prices, auxiliary, proxies, conversions),
    )
    opening, closing = _outcomes(loader, prices, dates, visibility, conversions)
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
    _target_memberships(inputs.targets, slots, prices, sessions, visibility)
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
            # Every head read the preparation made, with the receipt its reader returned.
            "head_reads": loader.reads,
            "source_pins": sources,
            # Present only when the request grants a conversion, so a single-currency
            # preparation keeps the sealed bytes it had before conversions existed.
            **({"fx_conversions": conversions.record()} if conversions.by_currency else {}),
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
# A decision needs a session to fill on, so one observed session is not a period.
_MINIMUM_RESEARCH_SESSIONS = 2
RESEARCH_SOURCE_MODULES = (
    "aegis_alpha.application.backtest_prepare",
    "aegis_alpha.application.research_run",
    # The price route selects its bars here, so a change to that selection is a change
    # to what a price-pinned run decides on.
    "aegis_alpha.storage.read_heads",
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
    # Which sleeve supplied each decision, positionally against decisions. A sleeve run
    # is all offense; only a composition ever reads defense here.
    sleeve_roles: tuple[str, ...]
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
        object.__setattr__(self, "sleeve_roles", tuple(self.sleeve_roles))
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
    series: frozenset[str]
    calendar_ref: str
    # A signal panel's instruments whose closes each decision converts, by the series name
    # the store carries them under and the conversion that applies.
    converts: Mapping[str, tuple[str, _Fx]] = field(default_factory=dict)


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
    calendar_ref: str,
) -> _Observed:
    """Project one panel at the declared ceiling and resolve its series onto instruments."""
    values: dict[str, dict[date, float]] = {}
    observed: dict[str, dict[date, date]] = {}
    known: dict[str, dict[date, int]] = {}
    read: set[str] = set()
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
        read.add(name)
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
    return _Observed(role, values, observed, known, frozenset(read), calendar_ref)


def _price_panels(
    loader: _Loader,
    declaration: ResearchRunRequest,
    visibility: _Visibility,
    conversions: _Conversions,
) -> tuple[dict[str, _Observed], HeadRead]:
    """Derive the open and close panels from canonical price pins through read_heads.

    One read serves both panels, so they cannot disagree on basis, adjustment, calendar
    or sessions: each canonical unadjusted bar supplies its own open and its own close.
    The read is a research read (no strict cutoff, so time-rule grants decide nothing)
    bounded by the declared knowledge time exactly as the observation route bounds its
    panel: a revision recorded as known after the ceiling is not read, one with no
    recorded knowledge time is. A head the store itself says became available after the
    ceiling is skipped rather than replaced by the value it superseded.

    The query is pushed down to the declared instruments and to the dates the run can
    use, from the earlier of the history and period starts, so the panel starts where
    the pinned data starts for each instrument and not where a retained snapshot did.

    A bar in a currency the declaration grants a conversion for is stated in the account
    currency with the fixings known at the ceiling, read once over the same window; a bar
    no fixing converts is left out of both marking panels like a bar that is not present.
    The signal panel holds each close in its own currency. Where the conversion states
    signals in the account currency, each decision converts the closes it reads with the
    fixings known at its own cutoff, so a fixing fixed or known after a decision never
    reaches that decision's signals.
    """
    binding = cast("HeadBinding", declaration.prices)
    start = min(declaration.history.start, declaration.period.start)
    end = max(declaration.history.end, declaration.period.end)
    query = HeadQuery(
        known_ceiling_us=visibility.ceiling,
        subjects=tuple(sorted(declaration.instrument_map)),
        from_date=start,
        to_date=end + timedelta(days=1),
        price_roles=("canonical",),
    )
    read = load_pinned_heads(loader.workspace, binding, query, budget=loader.budget)
    sources = _panel_sources(loader, read, query, conversions)
    reference = canonical_json_bytes(
        {"schema": "aas-head-binding-v1", "binding_hash": binding.binding_hash}
    ).decode()
    values: dict[str, dict[str, dict[date, float]]] = {"open": {}, "close": {}, "signal": {}}
    observed: dict[str, dict[date, date]] = {}
    known: dict[str, dict[date, int]] = {}
    seen: set[str] = set()
    sessions: set[tuple[str, date]] = set()
    converts: dict[str, tuple[str, _Fx]] = {}
    converting = _OutcomeFx(loader, conversions, visibility, (start, end), "panels")
    for head, required in (
        (head, required) for source, required in sources for head in source.rows
    ):
        row = head.values
        name = _text(row["instrument_id"])
        instrument = declaration.instrument_map[name]
        seen.add(name)
        currency = _text(row["currency"])
        fx = _panel_fx(row, required, declaration, conversions)
        if not _panel_bar(row, visibility):
            continue
        session = _day(row["session_date"])
        if (instrument, session) in sessions:
            raise ValueError("price panel repeats one session for " + instrument)
        sessions.add((instrument, session))
        stated = {
            role: converting.value(currency, name, session, _number(row[role]))
            for role in ("open", "close")
        }
        if stated["open"] is not None and stated["close"] is not None:
            for role in ("open", "close"):
                values[role].setdefault(instrument, {})[session] = cast("float", stated[role])
        values["signal"].setdefault(instrument, {})[session] = _number(row["close"])
        if fx is not None and fx.conversion.signal_basis == "account_currency":
            converts[instrument] = (name, fx)
        observed.setdefault(instrument, {})[session] = visibility.observed(row, session)
        # The bar's own end instant orders sessions for the date-only schedule. It is the
        # economic axis of the bar, not a knowledge time, exactly as the observation
        # panel's feature instant is.
        known.setdefault(instrument, {})[session] = cast("int", row["bar_end_us"])
    absent = sorted(set(declaration.instrument_map) - seen)
    if absent:
        raise ValueError(
            "instrument_map names instruments no pinned price carries: " + ", ".join(absent)
        )
    converting.record()
    loader.retain(
        "prices:" + binding.binding_hash,
        {
            "head_read": dict(read.receipt),
            "panels": {
                role: {
                    instrument: {day.isoformat(): value for day, value in sorted(series.items())}
                    for instrument, series in sorted(panel.items())
                }
                for role, panel in values.items()
                if role != "signal" or conversions.by_currency
            },
        },
    )
    panels = {
        role: _Observed(
            role,
            values[role],
            observed,
            known,
            frozenset(seen),
            reference,
            MappingProxyType(converts) if role == "signal" else MappingProxyType({}),
        )
        for role in ("open", "close", "signal")
    }
    return panels, read


def _panel_sources(
    loader: _Loader, read: HeadRead, query: HeadQuery, conversions: _Conversions
) -> list[tuple[HeadRead, str | None]]:
    """The declaration's price read, then each FX grant's own price binding read.

    A grant's binding contributes that chain's bars, which must all be in the grant's
    currency; a bar both bindings carry repeats its session and is refused.
    """
    sources: list[tuple[HeadRead, str | None]] = [(read, None)]
    for fx in conversions.by_currency.values():
        if fx.prices is not None:
            extra = load_pinned_heads(loader.workspace, fx.prices, query, budget=loader.budget)
            loader.head_read(fx.key, "prices", None, extra.receipt, extra.receipt_hash)
            sources.append((extra, fx.conversion.currency))
    return sources


def _panel_fx(
    row: Row, required: str | None, declaration: ResearchRunRequest, conversions: _Conversions
) -> _Fx | None:
    """Admit one panel bar's shape; the conversion it takes, or None in the account currency."""
    if (row["basis"], row["price_role"]) != ("unadjusted", "canonical"):
        raise ValueError("a price-pinned research run reads only canonical unadjusted bars")
    if row["interval"] != "1d":
        # The panels are session panels: any other bar would stand in for a session's
        # open and close, or meet the daily bar of its session as a repeat.
        raise ValueError("a price-pinned research run reads only daily bars")
    currency = _text(row["currency"])
    if required is not None and currency != required:
        raise ValueError("an FX grant's price binding carries a bar not in " + required)
    fx = conversions.by_currency.get(currency)
    if currency != declaration.conventions.currency and fx is None:
        raise ValueError(
            "price currency is not the declared account currency and no FX conversion "
            "grants it: " + currency
        )
    return fx


def _panel_bar(row: Row, visibility: _Visibility) -> bool:
    """A present bar the store says was available by the declared ceiling."""
    return row["value_state"] == "present" and (
        row["available_at_us"] is None or cast("int", row["available_at_us"]) <= visibility.ceiling
    )


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
        loaded[role] = _mapped_panel(
            series,
            declaration,
            visibility,
            role,
            canonical_json_bytes(contract["calendar_ref"]).decode(),
        )
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
    loader: _Loader, reference: MembershipRef, bundle: EngineBundle
) -> EnsembleMembership:
    """Resolve the membership the strategy's own contract already names."""
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


@dataclass(frozen=True, slots=True)
class _Sleeve:
    """One registered sleeve, loaded: what it is and what it needs to be evaluated."""

    role: str
    bundle: EngineBundle
    definition: ExecutionDefinition
    membership: EnsembleMembership


@dataclass(frozen=True, slots=True)
class ResearchDecision:
    """One decision and the sleeve that actually supplied it."""

    receipt: ReplayReceipt
    sleeve: _Sleeve


def _load_sleeve(workspace: Workspace, loader: _Loader, role: str, sleeve: SleeveRef) -> _Sleeve:
    """Load one registered sleeve and refuse one this path cannot feed.

    A sleeve's macro series are fed from the declaration's macro grants, which
    ``_research_macro`` holds to exactly the series the sleeves require. A derived series
    has no declared source on this path, so a sleeve reading one would reach the engine
    short of an input it was told to expect. Refused here rather than failing in replay.
    """
    bundle, definition = _stored_strategy(
        workspace,
        StrategyPin(
            sleeve.strategy_store_id,
            sleeve.strategy_id,
            sleeve.version,
            sleeve.raw_sha256,
            sleeve.contract_sha256,
        ),
    )
    if definition.derived_series:
        raise ValueError(
            "a declared research run supplies prices and granted macro series; the "
            + role
            + " sleeve also requires derived inputs"
        )
    return _Sleeve(
        role, bundle, definition, _research_membership(loader, sleeve.membership, bundle)
    )


def _require_composable(offense: _Sleeve, defense: _Sleeve) -> None:
    """Hold a composition to exactly two levels and one calendar.

    A defensive sleeve that declares its own canary would make the switch recursive, and
    this contract names one switch. Two sleeves evaluated on different calendar
    conventions would also be two different runs reported as one, so they have to agree.

    The offensive sleeve may declare no regular signals either. evaluate_signals folds
    them into the same master switch the composition routes on, so a sleeve carrying one
    would send decisions to the defensive sleeve for a condition that is not the canary,
    and the sealed record would name a switch that is not the one that fired.

    Any declared signal is refused, not only one that would have evaluated true. Reading
    the enablement rule here would copy an engine internal into application code, and a
    copy that drifts turns the contract's own claim false in the one place where being
    wrong is worst. What the declaration reaches for is what this checks.
    """
    for strategy in offense.bundle.contract.pack:
        if strategy.signals_config:
            raise ValueError(
                "the offensive sleeve declares regular signals, enabled or not; "
                "this composition switches on the canary alone"
            )
    if offense.bundle.contract.macro_signals:
        # A macro signal folds into the same master switch, so the switch that routed a
        # decision to the defensive sleeve would not be the canary the record names.
        raise ValueError(
            "the offensive sleeve declares macro signals; this composition switches on "
            "the canary alone"
        )
    # Every pack member, not the first: replay evaluates all of them, so a canary on a
    # later strategy would fire inside the defensive sleeve just the same.
    for strategy in defense.bundle.contract.pack:
        declared_canary = strategy.canary_config.get("assets", ())
        if not isinstance(declared_canary, (list, tuple)) or declared_canary:
            raise ValueError(
                "the defensive sleeve declares its own canary; this composition names one switch"
            )
    if canonical_json_bytes(offense.definition.calendar) != canonical_json_bytes(
        defense.definition.calendar
    ):
        raise ValueError("the two sleeves declare different calendar conventions")


def _research_decisions(  # noqa: PLR0913 -- the sleeves, their schedule and every input
    loader: _Loader,
    sleeves: tuple[_Sleeve, ...],
    slots: tuple[DecisionSlot, ...],
    visibility: _Visibility,
    panels: Mapping[str, _Observed],
    *,
    macro: tuple[_Auxiliary, ...],
) -> tuple[ResearchDecision, ...]:
    """Evaluate the registered strategy at each decision through the ordinary engine.

    Each decision sees only the sessions at or before its own cutoff, so nothing later
    than the decision reaches its features. The knowledge axis stays declared and the
    economic axis stays honest, which is exactly the split the declaration records.

    A composition adds one step and no new logic: the offense sleeve is evaluated first
    and the engine's own master switch, computed from that sleeve's declared canary
    configuration, decides whether the defense sleeve supplies the decision instead.
    Nothing here interprets a condition a declaration wrote.

    Each granted macro series is read at the decision's own cutoff (never past the
    declared knowledge time), so a decision sees the vintage known when it was made where
    the store records one, and each sleeve receives the series its contract reads. A
    signal converted into the account currency is converted the same way: with the
    fixings known at the decision's cutoff.
    """
    close = panels.get("signal", panels["close"])
    offense = sleeves[0]
    receipts = []
    for slot in slots:
        known = replace(slot, cutoff_us=min(slot.cutoff_us, visibility.ceiling))
        series = {
            item.name: tuple(
                MacroPoint(*point) for point in _head_aux_points(loader, item, visibility, known)
            )
            for item in macro
        }
        readable = {
            instrument: {
                session: value
                for session, value in series.items()
                # The declared history window bounds what a signal may look back on, as
                # it does on the executable path. It does not bound the marking panels:
                # a period legitimately extends past the lookback window it warmed up on.
                if visibility.history_start <= session <= visibility.history_end
                and close.known_us[instrument][session] <= slot.cutoff_us
            }
            for instrument, series in close.values.items()
        }
        _convert_signals(loader, close.converts, readable, visibility, known)
        points = {
            instrument: tuple(
                PricePoint(session, value, close.observed[instrument][session])
                for session, value in sorted(values.items())
            )
            for instrument, values in readable.items()
        }
        chosen, receipt = offense, _replay_sleeve(offense, points, slot, visibility, series)
        if len(sleeves) > 1 and any(receipt.master_switch.values()):
            # The switch fired, so the defensive sleeve supplies this decision. The
            # offense receipt is discarded rather than blended: a sample takes one
            # sleeve's weights at a time, which is what the private runner does.
            chosen = sleeves[1]
            receipt = _replay_sleeve(chosen, points, slot, visibility, series)
        receipts.append(ResearchDecision(receipt, chosen))
    return tuple(receipts)


def _convert_signals(
    loader: _Loader,
    converts: Mapping[str, tuple[str, _Fx]],
    readable: dict[str, dict[date, float]],
    visibility: _Visibility,
    slot: DecisionSlot,
) -> None:
    """Convert one decision's signal closes, in place, with the fixings its cutoff knows.

    One fixing read per currency serves every instrument in it, over the sessions the
    decision reads. A close no fixing known at the cutoff converts is not a signal point,
    and the decision's read record names it.
    """
    by_currency: dict[str, list[tuple[str, str]]] = {}
    for instrument, (name, fx) in sorted(converts.items()):
        if readable.get(instrument):
            by_currency.setdefault(fx.conversion.currency, []).append((instrument, name))
    for _, members in sorted(by_currency.items()):
        fx = converts[members[0][0]][1]
        days = [day for instrument, _ in members for day in readable[instrument]]
        rates, read = _fx_rates(loader, fx, visibility, slot.cutoff_us, (min(days), max(days)))
        converted = 0
        unconverted = []
        for instrument, name in members:
            values = readable[instrument]
            for day, value in list(values.items()):
                stated = rates.convert(value, day)
                if stated is None:
                    del values[day]
                    unconverted.append((name, day))
                else:
                    values[day] = stated[0]
                    converted += 1
        _record_fx(
            loader,
            fx,
            ("decision", slot.decision_date),
            read,
            converted=converted,
            unconverted=unconverted,
        )


def _replay_sleeve(
    sleeve: _Sleeve,
    points: Mapping[str, tuple[PricePoint, ...]],
    slot: DecisionSlot,
    visibility: _Visibility,
    macro: Mapping[str, tuple[MacroPoint, ...]],
) -> ReplayReceipt:
    _warmup(sleeve.definition, points, slot, visibility)
    required = _macro_series(sleeve)
    return replay(
        sleeve.bundle,
        ReplayRequest(
            slot.decision_date,
            slot.decision_date,
            points,
            {name: macro[name] for name in required},
            {},
            sleeve.membership,
        ),
        knowledge_as_of=_utc_day(slot.cutoff_us),
    )


def _macro_series(sleeve: _Sleeve) -> tuple[str, ...]:
    """The macro series a sleeve's contract reads, as its execution definition lists them."""
    return tuple(
        sorted(
            identity
            for requirement in sleeve.definition.input_requirements
            if requirement.role == "macro"
            for identity in requirement.identifiers
        )
    )


def _research_macro(
    loader: _Loader,
    declaration: ResearchRunRequest,
    sleeves: tuple[_Sleeve, ...],
    visibility: _Visibility,
) -> tuple[_Auxiliary, ...]:
    """Admit the declaration's macro grants against the series its sleeves read.

    The grant is exact: every series a sleeve reads is granted, and nothing else is, so a
    sleeve never reaches replay short of a series and a declaration never records a series
    no decision used. Each binding is verified against the store and must hold the series,
    in its declared unit, by the declared knowledge time.
    """
    required = sorted({name for sleeve in sleeves for name in _macro_series(sleeve)})
    granted = [grant.series_id for grant in declaration.macro]
    if required != granted:
        raise ValueError(
            "the sleeves read macro series ["
            + ", ".join(required)
            + "]; the declaration grants ["
            + ", ".join(granted)
            + "]"
        )
    items = []
    for ordinal, grant in enumerate(declaration.macro):
        items.append(_granted_macro(loader, grant, ordinal, visibility))
    return tuple(items)


def _granted_macro(
    loader: _Loader, grant: MacroGrant, ordinal: int, visibility: _Visibility
) -> _Auxiliary:
    verify_head_binding(loader.workspace, grant.binding, budget=loader.budget)
    item = _Auxiliary(
        grant.series_id,
        grant.binding.domain,
        (),
        grant.series_id,
        "value",
        ("heads", grant.binding.binding_hash),
        binding=grant.binding,
        key=("macro", ordinal),
        unit=grant.unit,
    )
    _admit_head_macro(loader, item, visibility)
    return item


def _research_conversions(loader: _Loader, declaration: ResearchRunRequest) -> _Conversions:
    """The declaration's FX conversions, each binding verified against the store."""
    result = {}
    for ordinal, grant in enumerate(declaration.fx_conversions):
        for binding in (grant.binding, grant.prices):
            if binding is not None:
                verify_head_binding(loader.workspace, binding, budget=loader.budget)
        result[grant.conversion.currency] = _Fx(
            grant.conversion, grant.binding, ("fx_conversion", ordinal), grant.prices
        )
    return _Conversions(declaration.conventions.currency, MappingProxyType(result))


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
        # The declared mode, which the strict request schema does not list, so the
        # executable path cannot emit this envelope even by accident.
        "research_mode": DECLARED_RESEARCH_MODE,
    }
    raw = canonical_json_bytes(document)
    return EnvelopeExport(raw, hashlib.sha256(raw).hexdigest())


def _require_fillable(inputs: EnvelopeInputs) -> None:
    """Refuse what the panel cannot fill or mark, before the accounting discovers it.

    A newly targeted symbol is not the whole requirement. On a rebalance session the
    accounting also needs an open for everything already held, because a position
    leaving the book is sold at that open, and on every session it needs a close for
    everything still held, because that is what marks the account. A panel missing any
    of those fails deep inside the replay with an arithmetic message that names neither
    the session nor the instrument. This walks the same holdings the replay walks and
    refuses with both. Nothing is filled in.
    """
    held: set[str] = set()
    for index in range(1, len(inputs.dates)):
        session, decision = inputs.dates[index], inputs.dates[index - 1]
        weights = inputs.targets.get(decision)
        if weights is not None:
            wanted = {symbol for symbol, weight in weights.items() if weight > 0}
            # Exactly the set the replay demands at a rebalance: what is on the book
            # plus what is being bought, because the difference is what gets sold.
            _require_observed(inputs.opens[index], held | wanted, session, "open")
            held = wanted
        _require_observed(inputs.closes[index], held, session, "close")


def _require_observed(
    prices: Mapping[str, float], symbols: set[str], session: date, role: str
) -> None:
    """Hold the panel to what the accounting calls usable: present, finite and positive."""
    missing = sorted(
        symbol
        for symbol in symbols
        if not isinstance(prices.get(symbol), (int, float))
        or not math.isfinite(prices[symbol])
        or prices[symbol] <= 0
    )
    if missing:
        raise ValueError(
            "the "
            + role
            + " panel has no usable observation on "
            + session.isoformat()
            + " for: "
            + ", ".join(missing)
        )


@dataclass(frozen=True, slots=True)
class _ResearchPlan:
    """A schedule built from dates alone, because the panel carries no clock times."""

    dates: tuple[date, ...]
    slots: tuple[DecisionSlot, ...]


def _panel_sessions(panel: _Observed) -> set[date]:
    return {session for series in panel.values.values() for session in series}


def _panel_cutoff(panel: _Observed, day: date) -> int:
    """The last instant the panel actually recorded on this session.

    Taken from the observations rather than from a clock nobody supplied. It is what
    makes a decision see its own session and nothing after it.
    """
    return max(
        at_us
        for series in panel.known_us.values()
        for session, at_us in series.items()
        if session == day
    )


def _research_plan(
    declaration: ResearchRunRequest, panels: Mapping[str, _Observed]
) -> _ResearchPlan:
    """Schedule month-end decisions over the sessions the panel itself observed.

    engine.schedule requires an open and a close instant for every open session, and the
    retained panel has neither; the declared calendar convention supplies none either.
    Inventing hours would be wrong twice over, once because they are not observed and
    again because daylight saving moves them. So the research path schedules on dates,
    and each decision's cutoff is the last observation the panel recorded that day.
    Nothing about the executable schedule changes: this path simply does not use it.
    """
    period = declaration.period
    # Compared inside the declared period only. That is where the two panels are used
    # together, so a difference outside it says nothing about this run.
    observed = {
        role: {day for day in _panel_sessions(panel) if period.start <= day <= period.end}
        for role, panel in panels.items()
    }
    if observed["open"] != observed["close"]:
        # Signals and fills would otherwise run on two different calendars.
        raise ValueError("the open and close panels observe different sessions in the period")
    dates = tuple(sorted(observed["close"]))
    if len(dates) < _MINIMUM_RESEARCH_SESSIONS:
        raise ValueError("the declared period holds fewer than two observed sessions")
    following = dict(pairwise(dates))
    month_end: dict[tuple[int, int], date] = {}
    for day in dates:
        month_end[day.year, day.month] = day
    slots = tuple(
        DecisionSlot(decision, following[decision], _panel_cutoff(panels["close"], decision))
        for decision in sorted(month_end.values())
        # The last observed session has nothing to fill on, and the accounting treats a
        # decision without a following session as no trade rather than as an error.
        if decision in following
    )
    if not slots:
        raise ValueError("the declared period holds no month end with a session to fill on")
    return _ResearchPlan(dates, slots)


def _research_sleeves(
    workspace: Workspace, loader: _Loader, declaration: ResearchRunRequest
) -> tuple[_Sleeve, ...]:
    """Load the one sleeve a run declares, or the two a composition does."""
    offense = _load_sleeve(
        workspace,
        loader,
        "offense",
        SleeveRef(
            declaration.strategy_store_id,
            declaration.strategy_id,
            declaration.strategy_version,
            declaration.strategy_raw_sha256,
            declaration.strategy_contract_sha256,
            declaration.membership,
        ),
    )
    if declaration.composition is None:
        return (offense,)
    defense = _load_sleeve(workspace, loader, "defense", declaration.composition.defense)
    _require_composable(offense, defense)
    return (offense, defense)


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
    visibility = _Visibility(
        "observed_snapshot_research",
        declaration.conventions.knowledge_time_us,
        None,
        declaration.history.start,
        declaration.history.end,
    )
    loader = _Loader(workspace, budget, {})
    sleeves = _research_sleeves(workspace, loader, declaration)
    macro = _research_macro(loader, declaration, sleeves, visibility)
    conversions = _research_conversions(loader, declaration)
    head_read = None
    if declaration.prices is None:
        panels = _observation_panels(loader, declaration, visibility)
    else:
        panels, head_read = _price_panels(loader, declaration, visibility, conversions)
    read = {series for panel in panels.values() for series in panel.series}
    plan = _research_plan(declaration, panels)
    decisions = _research_decisions(loader, sleeves, plan.slots, visibility, panels, macro=macro)
    opening, closing = _research_outcomes(panels, plan.dates)
    inputs = EnvelopeInputs(
        plan.dates,
        opening,
        closing,
        {
            decision.receipt.as_of: _research_targets(decision.receipt, decision.sleeve.definition)
            for decision in decisions
        },
        _observation_types(workspace, declaration.instrument_map, read)
        if head_read is None
        else {
            declaration.instrument_map[name]: kind
            for name, kind in _instrument_types(workspace, sorted(read)).items()
        },
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
            defensive_decisions=tuple(
                decision.receipt.as_of.isoformat()
                for decision in decisions
                if decision.sleeve.role == "defense"
            ),
            resolved_calendar={
                "calendar_id": declaration.calendar.calendar_id,
                "basis": declaration.calendar.basis,
                # The caller names the research calendar; this is the reference the
                # panels themselves carry, so the label cannot stand in for it.
                "observed_calendar_ref": panels["close"].calendar_ref,
                "sessions": len(plan.dates),
                "first_session": plan.dates[0].isoformat(),
                "last_session": plan.dates[-1].isoformat(),
                "decisions": len(plan.slots),
            },
            head_read=None if head_read is None else head_read.receipt,
            macro_reads=tuple(item for item in loader.reads if item["role"] == "macro"),
            fx_conversions=tuple(conversions.record()),
            fx_reads=tuple(item for item in loader.reads if item["role"] == "fx_conversion"),
        ),
    )
    return PreparedResearchRun(
        declaration,
        sleeves[0].definition,
        plan.slots,
        tuple(decision.receipt for decision in decisions),
        tuple(decision.sleeve.role for decision in decisions),
        inputs,
        envelope,
        provenance,
    )


def _decisions(
    bundle: EngineBundle,
    definition: ExecutionDefinition,
    slots: tuple[DecisionSlot, ...],
    visibility: _Visibility,
    loaded: tuple[
        _Loader,
        _Calendar,
        EnsembleMembership,
        tuple[_Prices, ...],
        tuple[_Auxiliary, ...],
        tuple[_Proxy, ...],
        _Conversions,
    ],
) -> tuple[tuple[ReplayReceipt, ...], Mapping[date, Mapping[str, AssetFeatures]]]:
    loader, calendar, membership, prices, auxiliary, proxies, conversions = loaded
    receipts = []
    features = {}
    for slot in slots:
        points = _price_points(loader, prices, calendar, visibility, slot, conversions=conversions)
        points.update(
            {proxy.logical: _proxy_points(proxy, visibility, slot.cutoff_us) for proxy in proxies}
        )
        _warmup(definition, points, slot, visibility)
        macro, derived = _aux_inputs(loader, auxiliary, visibility, slot)
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
