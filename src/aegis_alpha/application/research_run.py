"""The declared uncertified research-run request.

No executable path exists for a retained observation panel, and none is added here.
admit_native_input accepts only the price and sessions transforms, the price route
refuses an observation pin for domain, _reject_observation_contract closes the
derived reader, and strict PIT selects nothing from rows without knowledge times.
Those refusals are load-bearing and stay exactly as they are.

What this module adds is the opposite move: instead of promoting reference data to
executable, it makes the caller declare everything the strict path would otherwise
have to infer, and records that declaration with the run. A declaration buys
legibility, never eligibility. Nothing here converts an observation into a price,
fills in a missing knowledge time, rounds a value to fit DECIMAL, or relaxes a
tolerance.

The execution mode is fixed at parse time and carried into storage so a downstream
consumer cannot flip it: a request that does not say research-uncertified is refused
rather than defaulted.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.codec import decode_json

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "CALENDAR_BASIS",
    "EXECUTION_MODE",
    "FILL_CONVENTION",
    "OBSERVATION_NAMESPACE",
    "PREPARED_COMPOSITION_SCHEMA",
    "PREPARED_SCHEMA",
    "REQUIRED_UNSETTLED",
    "RESEARCH_COMPOSITION_SCHEMA",
    "RESEARCH_RUN_SCHEMA",
    "SWITCH_RULE",
    "TIE_RULE",
    "Composition",
    "DeclaredConventions",
    "DeclaredSemantics",
    "ExecutionTerms",
    "MembershipRef",
    "ObservationPinRef",
    "PreparationRecord",
    "ResearchCalendar",
    "ResearchRunError",
    "ResearchRunRequest",
    "SleeveRef",
    "Window",
    "declared_provenance",
    "parse_research_composition_request",
    "parse_research_run_request",
]

RESEARCH_RUN_SCHEMA = "aas-research-run-v2"
# The retained panel carries session dates and no clock times, and the calendar
# convention declares none. The engine's session schedule requires an open and a close
# instant per session, so a declared run schedules on the panel's own observed dates
# instead. Fixed literal: no other basis can be claimed under this schema.
CALENDAR_BASIS = "observed-sessions-date-only"
# What a prepared declared run seals beside its envelope. The declaration is the whole
# provenance, so the sealed document names its own source rather than a certified one.
PREPARED_SCHEMA = "aas-prepared-research-run-v1"
# A sample is not a sleeve. Composing two sleeves is a different declaration with its
# own identity, so a sleeve run and a sample composition can never be read as each
# other even when every other input matches.
RESEARCH_COMPOSITION_SCHEMA = "aas-research-composition-v1"
PREPARED_COMPOSITION_SCHEMA = "aas-prepared-research-composition-v1"
# The condition between the two sleeves, named rather than expressed. The installed
# engine already computes it from the offense sleeve's own declared canary
# configuration, so a composition selects that rule; it does not carry one.
SWITCH_RULE = "offense-master-switch-v1"
# Fixed, not a default. A request that omits or changes it is refused, so the mode a
# stored run reports is the mode its author actually wrote down.
EXECUTION_MODE = "research-uncertified"
OBSERVATION_NAMESPACE = "aas-obs-"
# Resolved as research policy, not as Snowball parity: the public documentation and
# frontend code carry no server tie rule, so this names our own deterministic choice
# and says so. engine.allocation._top already orders by descending score then
# ascending asset id, which is exactly this rule; nothing is reimplemented here.
TIE_RULE = "momentum-tie-canonical-id-asc-v1"
# Resolved as research policy for the monthly samples: a month-end observation signal
# fills at the next trading session open, and a nontrading date rolls forward to the
# next trading session. Named and versioned because it is a decision rather than a
# finding; source_parity stays unknown, so this claims no Snowball equivalence.
FILL_CONVENTION = "fill-next-session-open-v1"
# D and M are a confirmed identity, not an equivalence: the exact backend sampling,
# lookback and lag behind each remain unverified, so a request names one of them.
_DATA_BASIS = frozenset({"D", "M"})
# What a daily-basis run may still say. The audit probe compared next-session-open
# against decision-close only, and did not test next-session close or exhaust the
# alternatives, so a D run names a hypothesis and lists it as unsettled.
_FILL_PRICE = frozenset({"next-session-open", "decision-close"})
_EXPAND = frozenset({"extended-history-used", "extended-history-not-used"})
# What a daily-basis run must still list as open. The monthly path no longer carries
# fill_price here because the owner resolved it into FILL_CONVENTION; the daily path
# does, so a stored D run cannot present a fill convention as settled.
REQUIRED_UNSETTLED = frozenset({"fill_price"})

_ROOT = frozenset(
    {
        "schema_version",
        "execution_mode",
        "strategy",
        "observations",
        "calendar",
        "membership",
        "period",
        "history",
        "execution",
        "instrument_map",
        "conventions",
        "uncertainty",
        "semantics",
        "unsettled",
    }
)
_STRATEGY = frozenset(
    {"strategy_store_id", "strategy_id", "version", "raw_sha256", "contract_sha256"}
)
_OBSERVATION = frozenset(
    {"dataset_id", "version", "generation_id", "chain_hash", "manifest_hash", "observation_role"}
)
_CALENDAR = frozenset({"calendar_id", "basis"})
_MEMBERSHIP = frozenset({"kind", "id", "version", "hash"})
_WINDOW = frozenset({"start", "end"})
# The two numbers the retained panel cannot supply and the engine will not guess.
_EXECUTION = frozenset({"cost", "initial_cash"})
_SLEEVE = frozenset(
    {
        "strategy_store_id",
        "strategy_id",
        "version",
        "raw_sha256",
        "contract_sha256",
        "membership",
    }
)
_SLEEVES = frozenset({"offense", "defense"})
_COMPOSITION = frozenset({"sample_id", "switch", "sleeves"})
_COMPOSITION_ROOT = (_ROOT - {"strategy", "membership"}) | {"composition"}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# A proportional rate of one consumes the whole fill, and anything at or above it makes
# the accounting produce a number no account could have reached.
_MAX_COST = 1.0
# Every one of these is a value the strict path would otherwise take from a certified
# source. Each must be written down; none is inferred and none defaults.
_CONVENTIONS = frozenset({"knowledge_time", "calendar", "cost", "capital", "currency"})
_ROLES = frozenset({"open", "close"})
_SEMANTICS = frozenset(
    {
        "data_basis",
        "abs_compare",
        "defensive_rule",
        "expand",
        "expand_source",
        "rebalance_timing",
        "fill_price",
        "tie_rule",
    }
)
_HEX = frozenset("0123456789abcdef")
_DIGEST_CHARS = 64


class ResearchRunError(ValueError):
    """A declared research-run request is incomplete, inconsistent or not research."""


@dataclass(frozen=True, slots=True)
class ObservationPinRef:
    """One pinned observation generation and the role its rows carry."""

    dataset_id: str
    version: str
    generation_id: str
    chain_hash: str
    manifest_hash: str
    observation_role: str


@dataclass(frozen=True, slots=True)
class ResearchCalendar:
    """A date-only calendar named by the caller and supplied by the panel itself.

    Nothing here asserts an exchange calendar. The sessions are whatever the pinned
    observations were recorded on, and the identifier exists so a stored run can say
    which research calendar it meant rather than leaving it unnamed.
    """

    calendar_id: str
    basis: str


@dataclass(frozen=True, slots=True)
class MembershipRef:
    """The registered ensemble membership the strategy's own contract already names."""

    kind: str
    id: str
    version: str
    hash: str


@dataclass(frozen=True, slots=True)
class SleeveRef:
    """One registered sleeve: the strategy the store admitted and its membership."""

    strategy_store_id: str
    strategy_id: str
    version: str
    raw_sha256: str
    contract_sha256: str
    membership: MembershipRef


@dataclass(frozen=True, slots=True)
class Composition:
    """Two pinned sleeves and the named rule that chooses between them.

    The rule is a literal, not an expression. Nothing in a declaration can describe a
    new condition, so composing a sample cannot become a way to run arbitrary logic:
    the only switch available is the one the installed engine already computes from the
    offense sleeve's declared canary configuration.
    """

    sample_id: str
    switch: str
    defense: SleeveRef


@dataclass(frozen=True, slots=True)
class Window:
    """An inclusive date window, start before end."""

    start: date
    end: date


@dataclass(frozen=True, slots=True)
class ExecutionTerms:
    """The account terms a declaration supplies because no retained input carries them."""

    cost: float
    initial_cash: float


@dataclass(frozen=True, slots=True)
class PreparationRecord:
    """What one preparation produced, beside what its declaration asserted.

    Kept together because these travel as one fact: the run's own outputs and the
    identities of everything that decided them. A caller that forgets one of these is
    sealing a record that cannot be checked.
    """

    envelope_sha256: str
    engine: Mapping[str, object]
    environment: Mapping[str, object]
    preparation_source_sha256: str
    resolved_calendar: Mapping[str, object]
    # Which sleeve supplied each decision, for a composition. Empty for a sleeve run,
    # so the sealed record always states what actually ran rather than what could have.
    defensive_decisions: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DeclaredConventions:
    """What the caller asserts about a calculation the strict path would not admit."""

    knowledge_time: str
    calendar: str
    cost: str
    capital: str
    currency: str

    @property
    def knowledge_time_us(self) -> int:
        """The declared knowledge moment in microseconds, as the request cutoff states it.

        The declaration exists to put every retained row before one named instant, so it
        has to be the same instant the registered request cuts at. Returning it as an
        integer is what lets the preparation compare the two rather than trust the prose.
        """
        return _knowledge_us(self.knowledge_time)


def _knowledge_us(value: str) -> int:
    """Read an exact UTC instant. A local or second-precision time is refused.

    A declared knowledge time is compared against a stored microsecond cutoff, so an
    offset-free timestamp would silently name a different moment depending on who reads
    it. Precision below the offset is the caller's to choose: this value is the
    projection ceiling itself, so a coarser declaration declares a coarser ceiling, and
    the sealed document records the microseconds it resolved to.
    """
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise ResearchRunError("knowledge_time must be an ISO-8601 instant") from error
    if moment.utcoffset() != timedelta(0):
        raise ResearchRunError("knowledge_time must carry a UTC offset")
    # Integer arithmetic throughout: seconds since the epoch multiplied as a float
    # rounds a distant instant, and a cutoff that moves by a microsecond can move
    # which revision a projection admits.
    return (moment - _EPOCH) // timedelta(microseconds=1)


@dataclass(frozen=True, slots=True)
class DeclaredSemantics:
    """Strategy semantics the caller declares, with the unverified ones named.

    Each field was established from public documentation and frontend code rather
    than from the original backend, so the declaration records what this run assumes
    and never asserts parity with the source system.
    """

    data_basis: str
    abs_compare: str
    defensive_rule: str
    expand: str
    expand_source: str
    rebalance_timing: str
    fill_price: str
    tie_rule: str


@dataclass(frozen=True, slots=True)
class ResearchRunRequest:
    """A parsed request. Its mode is fixed and its declarations are complete."""

    strategy_store_id: str
    strategy_id: str
    strategy_version: str
    strategy_raw_sha256: str
    strategy_contract_sha256: str
    observations: tuple[ObservationPinRef, ...]
    calendar: ResearchCalendar
    membership: MembershipRef
    period: Window
    history: Window
    execution: ExecutionTerms
    instrument_map: Mapping[str, str]
    conventions: DeclaredConventions
    semantics: DeclaredSemantics
    unsettled: tuple[str, ...]
    uncertainty: tuple[str, ...]
    execution_mode: str
    request_sha256: str
    schema_version: str = RESEARCH_RUN_SCHEMA
    # Absent for a sleeve run. Present only under the composition schema, which is what
    # separates a sample-level record from a sleeve-level one.
    composition: Composition | None = None

    @property
    def certified(self) -> bool:
        """Always false. A research run states its own status rather than carrying one."""
        return False

    @property
    def scope(self) -> str:
        """What this declaration is a record of, named rather than inferred."""
        return "sample-composition" if self.composition is not None else "sleeve"


def _object(value: object, field: str, allowed: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ResearchRunError(field + " must be an object")
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ResearchRunError(field + " has unknown keys: " + ", ".join(unknown))
    missing = sorted(allowed - set(value))
    if missing:
        raise ResearchRunError(field + " is missing: " + ", ".join(missing))
    return dict(value)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchRunError(field + " must be nonempty text")
    return value


def _digest(value: object, field: str) -> str:
    text = _text(value, field)
    if len(text) != _DIGEST_CHARS or any(character not in _HEX for character in text):
        raise ResearchRunError(field + " must be a lowercase sha256 digest")
    return text


def _observations(value: object) -> tuple[ObservationPinRef, ...]:
    if not isinstance(value, list) or not value:
        raise ResearchRunError("observations must be a non-empty array")
    pins = []
    for index, entry in enumerate(value):
        row = _object(entry, "observations[" + str(index) + "]", _OBSERVATION)
        role = _text(row["observation_role"], "observation_role")
        if role not in _ROLES:
            raise ResearchRunError("observation_role must be open or close")
        pins.append(
            ObservationPinRef(
                _text(row["dataset_id"], "dataset_id"),
                _exact_version(row["version"], "observation version"),
                _text(row["generation_id"], "generation_id"),
                _digest(row["chain_hash"], "chain_hash"),
                _digest(row["manifest_hash"], "manifest_hash"),
                role,
            )
        )
    seen = [(pin.generation_id, pin.observation_role) for pin in pins]
    if len(set(seen)) != len(seen):
        raise ResearchRunError("observations repeat a generation and role")
    return tuple(pins)


def _instrument_map(value: object) -> Mapping[str, str]:
    if not isinstance(value, dict) or not value:
        raise ResearchRunError("instrument_map must be a non-empty object")
    mapped = {}
    for series, instrument in sorted(value.items()):
        key = _text(series, "instrument_map key")
        if not key.startswith(OBSERVATION_NAMESPACE):
            raise ResearchRunError(
                "instrument_map key must name an " + OBSERVATION_NAMESPACE + " series"
            )
        mapped[key] = _text(instrument, "instrument_map[" + key + "]")
    if len(set(mapped.values())) != len(mapped):
        # Two series collapsing onto one instrument would silently pick a winner
        # inside the calculation, so refuse the ambiguity before it gets there.
        raise ResearchRunError("instrument_map maps two series onto one instrument")
    # Freezing a dataclass does not freeze what its fields hold. A caller that edited
    # this mapping after parsing would have the run sealed under a declaration digest
    # and a run identity computed from inputs it no longer uses.
    return MappingProxyType(mapped)


def _uncertainty(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        # An uncertified result whose author recorded no reservation is the shape
        # this path exists to prevent, so an empty list is refused.
        raise ResearchRunError("uncertainty must record at least one note")
    return tuple(_text(note, "uncertainty note") for note in value)


def _semantics(value: object) -> DeclaredSemantics:
    row = _object(value, "semantics", _SEMANTICS)
    basis = _text(row["data_basis"], "data_basis")
    if basis not in _DATA_BASIS:
        raise ResearchRunError("data_basis must be D or M")
    # null is not a disabled comparator: the public tooltip describes abs_compare as
    # replacing the default sign test, so an absent value would silently choose one.
    compare = _text(row["abs_compare"], "abs_compare")
    expand = _text(row["expand"], "expand")
    if expand not in _EXPAND:
        raise ResearchRunError("expand must name extended-history use")
    source = row["expand_source"]
    if expand == "extended-history-used":
        # Storing the flag is not implementing the feature, so claiming the extended
        # history requires naming where it came from.
        source_text = _text(source, "expand_source")
    else:
        if source not in (None, ""):
            raise ResearchRunError("expand_source belongs only to extended-history-used")
        source_text = ""
    fill = _text(row["fill_price"], "fill_price")
    if basis == "M":
        # The owner resolved the monthly convention, so a monthly run names it rather
        # than picking one of the hypotheses the audit left standing.
        if fill != FILL_CONVENTION:
            raise ResearchRunError("a monthly run must declare fill_price " + FILL_CONVENTION)
    elif fill not in _FILL_PRICE:
        raise ResearchRunError("fill_price must be next-session-open or decision-close")
    if row["tie_rule"] != TIE_RULE:
        raise ResearchRunError("tie_rule must be " + TIE_RULE)
    return DeclaredSemantics(
        data_basis=basis,
        abs_compare=compare,
        defensive_rule=_text(row["defensive_rule"], "defensive_rule"),
        expand=expand,
        expand_source=source_text,
        rebalance_timing=_text(row["rebalance_timing"], "rebalance_timing"),
        fill_price=fill,
        tie_rule=TIE_RULE,
    )


def _unsettled(value: object, semantics: DeclaredSemantics) -> tuple[str, ...]:
    """Hold a run to the axes its own basis leaves open.

    A monthly run may not list fill_price, because it is settled for that path and
    carrying it would keep reporting an open question the owner already answered. A
    daily run must list it, because nothing settled it there.
    """
    if not isinstance(value, list):
        raise ResearchRunError("unsettled must be an array")
    named = tuple(_text(item, "unsettled entry") for item in value)
    if semantics.data_basis == "M":
        if "fill_price" in named:
            raise ResearchRunError(
                "a monthly run declares " + FILL_CONVENTION + " and cannot list fill_price"
            )
        return named
    missing = sorted(REQUIRED_UNSETTLED - set(named))
    if missing:
        # The audit did not settle these, so a run may not present them as settled.
        raise ResearchRunError("unsettled must list: " + ", ".join(missing))
    return named


def _calendar(value: object) -> ResearchCalendar:
    row = _object(value, "calendar", _CALENDAR)
    if row["basis"] != CALENDAR_BASIS:
        # Fixed literal, so a declaration cannot claim an exchange calendar it does not
        # have. The only calendar this path can supply is the panel's own dates.
        raise ResearchRunError("calendar basis must be " + CALENDAR_BASIS)
    return ResearchCalendar(_text(row["calendar_id"], "calendar_id"), CALENDAR_BASIS)


def _membership(value: object) -> MembershipRef:
    row = _object(value, "membership", _MEMBERSHIP)
    if row["kind"] != "membership":
        raise ResearchRunError("membership kind must be membership")
    return MembershipRef(
        "membership",
        _text(row["id"], "membership id"),
        _exact_version(row["version"], "membership version"),
        _digest(row["hash"], "membership hash"),
    )


def _exact_version(value: object, field: str) -> str:
    version = _text(value, field)
    if version == "latest":
        raise ResearchRunError(field + " must be exact, not latest")
    return version


def _window(value: object, field: str) -> Window:
    row = _object(value, field, _WINDOW)
    bounds = []
    for name in ("start", "end"):
        text = _text(row[name], field + " " + name)
        try:
            bounds.append(date.fromisoformat(text))
        except ValueError as error:
            raise ResearchRunError(field + " " + name + " must be an ISO date") from error
    if bounds[0] > bounds[1]:
        raise ResearchRunError(field + " start must not follow its end")
    return Window(bounds[0], bounds[1])


def _execution(value: object) -> ExecutionTerms:
    """The cost and capital a declaration supplies, refused rather than defaulted.

    Zero capital is not an account and a negative cost is a subsidy. Both would run and
    produce a number, which is why each is checked here rather than left to accounting.
    """
    row = _object(value, "execution", _EXECUTION)
    terms = []
    for name in ("cost", "initial_cash"):
        number = row[name]
        if isinstance(number, bool) or not isinstance(number, (int, float)):
            raise ResearchRunError("execution " + name + " must be a number")
        try:
            finite = math.isfinite(number)
        except OverflowError:
            # An integer too large for a float is not a rejected value, it is an
            # unhandled exception leaking out of the parser. Refuse it like any other
            # number the accounting could not use.
            finite = False
        if not finite:
            # NaN compares false against every bound below, so it would pass each check
            # and then fail somewhere inside the accounting instead of here.
            raise ResearchRunError("execution " + name + " must be finite")
        terms.append(float(number))
    if terms[0] < 0:
        raise ResearchRunError("execution cost must not be negative")
    if terms[0] >= _MAX_COST:
        raise ResearchRunError("execution cost must be below 1")
    if terms[1] <= 0:
        raise ResearchRunError("execution initial_cash must be positive")
    return ExecutionTerms(terms[0], terms[1])


def parse_research_run_request(raw: bytes) -> ResearchRunRequest:
    """Parse and validate an exact request. Refuses before anything durable is written."""
    decoded = decode_json(raw)
    body = _object(decoded, "request", _ROOT)
    if body["schema_version"] != RESEARCH_RUN_SCHEMA:
        raise ResearchRunError("request is not " + RESEARCH_RUN_SCHEMA)
    strategy = _object(body["strategy"], "strategy", _STRATEGY)
    offense = SleeveRef(
        _text(strategy["strategy_store_id"], "strategy_store_id"),
        _text(strategy["strategy_id"], "strategy_id"),
        _exact_version(strategy["version"], "strategy version"),
        _digest(strategy["raw_sha256"], "raw_sha256"),
        _digest(strategy["contract_sha256"], "contract_sha256"),
        _membership(body["membership"]),
    )
    return _declared(decoded, body, RESEARCH_RUN_SCHEMA, offense, None)


def parse_research_composition_request(raw: bytes) -> ResearchRunRequest:
    """Parse a sample composition: two pinned sleeves and the named switch between them.

    Its own schema rather than a flag on a run, so a sleeve record and a sample record
    cannot be mistaken for one another and a consumer admitting one does not silently
    admit the other.
    """
    decoded = decode_json(raw)
    body = _object(decoded, "request", _COMPOSITION_ROOT)
    if body["schema_version"] != RESEARCH_COMPOSITION_SCHEMA:
        raise ResearchRunError("request is not " + RESEARCH_COMPOSITION_SCHEMA)
    block = _object(body["composition"], "composition", _COMPOSITION)
    if block["switch"] != SWITCH_RULE:
        # A literal, so a declaration cannot describe a condition of its own.
        raise ResearchRunError("composition switch must be " + SWITCH_RULE)
    sleeves = _object(block["sleeves"], "composition sleeves", _SLEEVES)
    offense, defense = (_sleeve(sleeves[role], role) for role in ("offense", "defense"))
    if offense.strategy_id == defense.strategy_id:
        # One sleeve named twice would compose a sample out of a single strategy and
        # report a switch that could never change the outcome.
        raise ResearchRunError("a composition needs two distinct sleeves")
    composition = Composition(
        _text(block["sample_id"], "composition sample_id"), SWITCH_RULE, defense
    )
    return _declared(decoded, body, RESEARCH_COMPOSITION_SCHEMA, offense, composition)


def _sleeve(value: object, role: str) -> SleeveRef:
    row = _object(value, "sleeve " + role, _SLEEVE)
    return SleeveRef(
        _text(row["strategy_store_id"], role + " strategy_store_id"),
        _text(row["strategy_id"], role + " strategy_id"),
        _exact_version(row["version"], role + " strategy version"),
        _digest(row["raw_sha256"], role + " raw_sha256"),
        _digest(row["contract_sha256"], role + " contract_sha256"),
        _membership(row["membership"]),
    )


def _declared(
    decoded: object,
    body: dict[str, object],
    schema: str,
    offense: SleeveRef,
    composition: Composition | None,
) -> ResearchRunRequest:
    """Everything both schemas declare. Only the strategy block differs between them."""
    if body["execution_mode"] != EXECUTION_MODE:
        raise ResearchRunError("execution_mode must be " + EXECUTION_MODE)
    conventions = _object(body["conventions"], "conventions", _CONVENTIONS)
    semantics = _semantics(body["semantics"])
    return ResearchRunRequest(
        strategy_store_id=offense.strategy_store_id,
        strategy_id=offense.strategy_id,
        strategy_version=offense.version,
        strategy_raw_sha256=offense.raw_sha256,
        strategy_contract_sha256=offense.contract_sha256,
        observations=_observations(body["observations"]),
        calendar=_calendar(body["calendar"]),
        membership=offense.membership,
        period=_window(body["period"], "period"),
        history=_window(body["history"], "history"),
        execution=_execution(body["execution"]),
        instrument_map=_instrument_map(body["instrument_map"]),
        conventions=DeclaredConventions(
            knowledge_time=_knowledge_time(conventions["knowledge_time"]),
            calendar=_text(conventions["calendar"], "calendar"),
            cost=_text(conventions["cost"], "cost"),
            capital=_text(conventions["capital"], "capital"),
            currency=_text(conventions["currency"], "currency"),
        ),
        semantics=semantics,
        unsettled=_unsettled(body["unsettled"], semantics),
        uncertainty=_uncertainty(body["uncertainty"]),
        execution_mode=EXECUTION_MODE,
        request_sha256=content_sha256(decoded),
        schema_version=schema,
        composition=composition,
    )


def _knowledge_time(value: object) -> str:
    """Refuse an unreadable knowledge time while parsing, not when a run reads it."""
    declared = _text(value, "knowledge_time")
    _knowledge_us(declared)
    return declared


def _sealed_composition(
    request: ResearchRunRequest, prepared: PreparationRecord
) -> Mapping[str, object] | None:
    """What the composition was, and which decisions the switch actually moved.

    Counts alone would let a record agree with a different computation, so the dates
    the defense sleeve supplied are listed. A reader can hold the sealed document
    against the envelope's own targets and see that they describe the same run.
    """
    if request.composition is None:
        return None
    defense = request.composition.defense
    return {
        "sample_id": request.composition.sample_id,
        "switch": request.composition.switch,
        "sleeves": {
            "offense": {
                "strategy_store_id": request.strategy_store_id,
                "strategy_id": request.strategy_id,
                "version": request.strategy_version,
                "raw_sha256": request.strategy_raw_sha256,
                "contract_sha256": request.strategy_contract_sha256,
                "membership": {
                    "kind": request.membership.kind,
                    "id": request.membership.id,
                    "version": request.membership.version,
                    "hash": request.membership.hash,
                },
            },
            "defense": {
                "strategy_store_id": defense.strategy_store_id,
                "strategy_id": defense.strategy_id,
                "version": defense.version,
                "raw_sha256": defense.raw_sha256,
                "contract_sha256": defense.contract_sha256,
                "membership": {
                    "kind": defense.membership.kind,
                    "id": defense.membership.id,
                    "version": defense.membership.version,
                    "hash": defense.membership.hash,
                },
            },
        },
        "defensive_decisions": list(prepared.defensive_decisions),
        "defensive_decision_count": len(prepared.defensive_decisions),
    }


def declared_provenance(request: ResearchRunRequest, prepared: PreparationRecord) -> bytes:
    """The sealed record of one declared run: what was asserted, and what it produced.

    The declaration is the whole provenance here. No certified request stands behind
    this calculation, so the document names its own hash, the envelope it produced and
    the engine and environment that produced it, and states in its own bytes that it is
    uncertified. A reader who has only this document can still tell what it is.
    """
    return canonical_json_bytes(
        {
            "schema": (
                PREPARED_COMPOSITION_SCHEMA if request.composition is not None else PREPARED_SCHEMA
            ),
            "declaration_schema": request.schema_version,
            "scope": request.scope,
            "declaration_sha256": request.request_sha256,
            "envelope_sha256": _digest(prepared.envelope_sha256, "envelope_sha256"),
            "execution_mode": request.execution_mode,
            "certified": request.certified,
            "point_in_time_certified": False,
            "executable_prices": False,
            "engine": dict(prepared.engine),
            # The engine identity covers the calculation modules. The declared path's
            # own decisions are made in the preparation, so its source is named too;
            # otherwise a stored run would keep its identity across a change to the
            # code that produced it.
            "preparation_source_sha256": _digest(
                prepared.preparation_source_sha256, "preparation_source_sha256"
            ),
            "environment": dict(prepared.environment),
            "strategy": {
                "strategy_store_id": request.strategy_store_id,
                "strategy_id": request.strategy_id,
                "version": request.strategy_version,
                "raw_sha256": request.strategy_raw_sha256,
                "contract_sha256": request.strategy_contract_sha256,
            },
            "composition": _sealed_composition(request, prepared),
            "observations": [
                {
                    "dataset_id": pin.dataset_id,
                    "version": pin.version,
                    "generation_id": pin.generation_id,
                    "chain_hash": pin.chain_hash,
                    "manifest_hash": pin.manifest_hash,
                    "observation_role": pin.observation_role,
                }
                for pin in request.observations
            ],
            "calendar": {
                "calendar_id": request.calendar.calendar_id,
                "basis": request.calendar.basis,
            },
            "membership": {
                "kind": request.membership.kind,
                "id": request.membership.id,
                "version": request.membership.version,
                "hash": request.membership.hash,
            },
            "period": {
                "start": request.period.start.isoformat(),
                "end": request.period.end.isoformat(),
            },
            "history": {
                "start": request.history.start.isoformat(),
                "end": request.history.end.isoformat(),
            },
            "execution": {
                "cost": request.execution.cost,
                "initial_cash": request.execution.initial_cash,
            },
            "instrument_map": dict(sorted(request.instrument_map.items())),
            # The declared calendar is prose. This is the identity the run actually used,
            # taken from the pinned sessions, so a reader can hold one against the other.
            "resolved_calendar": dict(sorted(prepared.resolved_calendar.items())),
            "conventions": {
                "knowledge_time": request.conventions.knowledge_time,
                "knowledge_time_us": request.conventions.knowledge_time_us,
                "calendar": request.conventions.calendar,
                "cost": request.conventions.cost,
                "capital": request.conventions.capital,
                "currency": request.conventions.currency,
            },
            "semantics": {
                "data_basis": request.semantics.data_basis,
                "abs_compare": request.semantics.abs_compare,
                "defensive_rule": request.semantics.defensive_rule,
                "expand": request.semantics.expand,
                "expand_source": request.semantics.expand_source,
                "rebalance_timing": request.semantics.rebalance_timing,
                "fill_price": request.semantics.fill_price,
                "tie_rule": request.semantics.tie_rule,
                "source_parity": "unknown",
            },
            "unsettled": list(request.unsettled),
            "uncertainty": list(request.uncertainty),
        }
    )
