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

from dataclasses import dataclass
from typing import TYPE_CHECKING

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.codec import decode_json

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "EXECUTION_MODE",
    "OBSERVATION_NAMESPACE",
    "RESEARCH_RUN_SCHEMA",
    "DeclaredConventions",
    "ObservationPinRef",
    "ResearchRunError",
    "ResearchRunRequest",
    "declared_provenance",
    "parse_research_run_request",
]

RESEARCH_RUN_SCHEMA = "aas-research-run-v1"
# Fixed, not a default. A request that omits or changes it is refused, so the mode a
# stored run reports is the mode its author actually wrote down.
EXECUTION_MODE = "research-uncertified"
OBSERVATION_NAMESPACE = "aas-obs-"

_ROOT = frozenset(
    {
        "schema_version",
        "execution_mode",
        "strategy",
        "observations",
        "instrument_map",
        "conventions",
        "uncertainty",
    }
)
_STRATEGY = frozenset({"strategy_id", "version", "raw_sha256", "contract_sha256"})
_OBSERVATION = frozenset(
    {"dataset_id", "version", "generation_id", "chain_hash", "manifest_hash", "observation_role"}
)
# Every one of these is a value the strict path would otherwise take from a certified
# source. Each must be written down; none is inferred and none defaults.
_CONVENTIONS = frozenset({"knowledge_time", "calendar", "cost", "capital", "currency"})
_ROLES = frozenset({"open", "close"})
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
class DeclaredConventions:
    """What the caller asserts about a calculation the strict path would not admit."""

    knowledge_time: str
    calendar: str
    cost: str
    capital: str
    currency: str


@dataclass(frozen=True, slots=True)
class ResearchRunRequest:
    """A parsed request. Its mode is fixed and its declarations are complete."""

    strategy_id: str
    strategy_version: str
    strategy_raw_sha256: str
    strategy_contract_sha256: str
    observations: tuple[ObservationPinRef, ...]
    instrument_map: Mapping[str, str]
    conventions: DeclaredConventions
    uncertainty: tuple[str, ...]
    execution_mode: str
    request_sha256: str

    @property
    def certified(self) -> bool:
        """Always false. A research run states its own status rather than carrying one."""
        return False


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
                _text(row["version"], "version"),
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


def _instrument_map(value: object) -> dict[str, str]:
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
    return mapped


def _uncertainty(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        # An uncertified result whose author recorded no reservation is the shape
        # this path exists to prevent, so an empty list is refused.
        raise ResearchRunError("uncertainty must record at least one note")
    return tuple(_text(note, "uncertainty note") for note in value)


def parse_research_run_request(raw: bytes) -> ResearchRunRequest:
    """Parse and validate an exact request. Refuses before anything durable is written."""
    decoded = decode_json(raw)
    body = _object(decoded, "request", _ROOT)
    if body["schema_version"] != RESEARCH_RUN_SCHEMA:
        raise ResearchRunError("request is not " + RESEARCH_RUN_SCHEMA)
    if body["execution_mode"] != EXECUTION_MODE:
        raise ResearchRunError("execution_mode must be " + EXECUTION_MODE)
    strategy = _object(body["strategy"], "strategy", _STRATEGY)
    conventions = _object(body["conventions"], "conventions", _CONVENTIONS)
    return ResearchRunRequest(
        strategy_id=_text(strategy["strategy_id"], "strategy_id"),
        strategy_version=_text(strategy["version"], "strategy version"),
        strategy_raw_sha256=_digest(strategy["raw_sha256"], "raw_sha256"),
        strategy_contract_sha256=_digest(strategy["contract_sha256"], "contract_sha256"),
        observations=_observations(body["observations"]),
        instrument_map=_instrument_map(body["instrument_map"]),
        conventions=DeclaredConventions(
            knowledge_time=_text(conventions["knowledge_time"], "knowledge_time"),
            calendar=_text(conventions["calendar"], "calendar"),
            cost=_text(conventions["cost"], "cost"),
            capital=_text(conventions["capital"], "capital"),
            currency=_text(conventions["currency"], "currency"),
        ),
        uncertainty=_uncertainty(body["uncertainty"]),
        execution_mode=EXECUTION_MODE,
        request_sha256=content_sha256(decoded),
    )


def declared_provenance(request: ResearchRunRequest) -> bytes:
    """The sidecar a stored run carries, so the record states its own status."""
    return canonical_json_bytes(
        {
            "schema": RESEARCH_RUN_SCHEMA,
            "execution_mode": request.execution_mode,
            "certified": request.certified,
            "point_in_time_certified": False,
            "executable_prices": False,
            "strategy": {
                "strategy_id": request.strategy_id,
                "version": request.strategy_version,
                "raw_sha256": request.strategy_raw_sha256,
                "contract_sha256": request.strategy_contract_sha256,
            },
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
            "instrument_map": dict(sorted(request.instrument_map.items())),
            "conventions": {
                "knowledge_time": request.conventions.knowledge_time,
                "calendar": request.conventions.calendar,
                "cost": request.conventions.cost,
                "capital": request.conventions.capital,
                "currency": request.conventions.currency,
            },
            "uncertainty": list(request.uncertainty),
            "request_sha256": request.request_sha256,
        }
    )
