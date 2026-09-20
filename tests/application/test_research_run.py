"""The declared research-run contract refuses before anything durable is written.

Every case here is a way an uncertified result could quietly acquire more standing
than its inputs earned: an absent mode, an inferred convention, an unmapped series,
a silent instrument collision, or no recorded reservation at all.
"""

from __future__ import annotations

import json
from typing import cast

import pytest

from aegis_alpha.application.research_run import (
    CALENDAR_BASIS,
    EXECUTION_MODE,
    FILL_CONVENTION,
    RESEARCH_RUN_SCHEMA,
    TIE_RULE,
    PreparationRecord,
    ResearchRunError,
    ResearchRunRequest,
    declared_provenance,
    parse_research_run_request,
)
from aegis_alpha.engine.errors import ContractParseError

DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64
EXPECTED_PINS = 2
# The later of the two ingest moments the retained pins carry, so every retained row
# precedes it. Declaring it is a convention and creates no time certification.
KNOWLEDGE_TIME = "2026-09-20T00:53:46.583764+00:00"
KNOWLEDGE_US = 1789865626583764


def _provenance(request: object) -> dict[str, object]:
    """Build the sidecar with fixed links, so a test compares declarations not hashes."""
    return json.loads(
        declared_provenance(
            cast("ResearchRunRequest", request),
            PreparationRecord(
                envelope_sha256=DIGEST_B,
                engine={"schema": "aas-engine-identity-v1"},
                environment={"schema": "aas-environment-identity-v1"},
                preparation_source_sha256=DIGEST_C,
                resolved_calendar={"calendar_id": "synthetic-calendar"},
            ),
        )
    )


def _pin(generation: str = "obs-synthetic-open", role: str = "open") -> dict[str, object]:
    return {
        "dataset_id": "obs-synthetic",
        "version": "1",
        "generation_id": generation,
        "chain_hash": DIGEST_C,
        "manifest_hash": DIGEST_D,
        "observation_role": role,
    }


def _strategy() -> dict[str, str]:
    return {
        "strategy_store_id": "synthetic-store",
        "strategy_id": "synthetic-strategy",
        "version": "1",
        "raw_sha256": DIGEST_A,
        "contract_sha256": DIGEST_B,
    }


def _conventions() -> dict[str, str]:
    return {
        "knowledge_time": KNOWLEDGE_TIME,
        "calendar": "synthetic-month-end",
        "cost": "0.0003 per side",
        "capital": "10000",
        "currency": "USD",
    }


def _semantics() -> dict[str, object]:
    return {
        "data_basis": "M",
        "abs_compare": "default-sign",
        "defensive_rule": (
            "DUAL_SWITCH: equal weight across the top relative-momentum defensive "
            "assets; a chosen asset with negative absolute momentum has its share "
            "held as cash"
        ),
        "expand": "extended-history-not-used",
        "expand_source": None,
        "rebalance_timing": "previous-month result applied at the following month start",
        "fill_price": FILL_CONVENTION,
        "tie_rule": TIE_RULE,
    }


def _body() -> dict[str, object]:
    return {
        "schema_version": RESEARCH_RUN_SCHEMA,
        "execution_mode": EXECUTION_MODE,
        "strategy": _strategy(),
        "observations": [_pin(), _pin("obs-synthetic-close", "close")],
        "calendar": {"calendar_id": "synthetic-research-calendar", "basis": CALENDAR_BASIS},
        "membership": {
            "kind": "membership",
            "id": "synthetic-membership",
            "version": "1",
            "hash": DIGEST_A,
        },
        "period": {"start": "2026-01-29", "end": "2026-03-30"},
        "history": {"start": "2025-11-28", "end": "2026-02-26"},
        "execution": {"cost": 0.0003, "initial_cash": 10000.0},
        "instrument_map": {"aas-obs-1": "SYN1", "aas-obs-2": "SYN2"},
        "conventions": _conventions(),
        "semantics": _semantics(),
        "unsettled": [],
        "uncertainty": ["reference observations, not executable prices"],
    }


def _raw(body: dict[str, object]) -> bytes:
    return json.dumps(body).encode()


def test_a_complete_request_parses_and_is_never_certified() -> None:
    request = parse_research_run_request(_raw(_body()))
    assert request.execution_mode == EXECUTION_MODE
    assert request.certified is False
    assert len(request.observations) == EXPECTED_PINS
    assert request.instrument_map["aas-obs-1"] == "SYN1"
    assert request.conventions.currency == "USD"


def test_the_recorded_sidecar_states_its_own_uncertified_status() -> None:
    request = parse_research_run_request(_raw(_body()))
    recorded = _provenance(request)
    assert recorded["execution_mode"] == EXECUTION_MODE
    assert recorded["certified"] is False
    assert recorded["point_in_time_certified"] is False
    assert recorded["executable_prices"] is False
    assert recorded["uncertainty"]
    assert recorded["envelope_sha256"] == DIGEST_B
    assert recorded["declaration_sha256"] == request.request_sha256


def test_the_declared_knowledge_time_is_an_exact_utc_instant() -> None:
    request = parse_research_run_request(_raw(_body()))
    assert request.conventions.knowledge_time_us == KNOWLEDGE_US


@pytest.mark.parametrize(
    "declared",
    [
        "declared: the panel carries no knowledge times",
        "2026-09-20T00:53:46.583764",
        "2026-09-20",
        "2026-09-20T09:53:46.583764+09:00",
    ],
)
def test_a_knowledge_time_that_is_not_an_exact_utc_instant_is_refused(declared: str) -> None:
    """A declaration compared against a microsecond cutoff has to be one.

    Prose, a naive timestamp and a local offset all read as plausible and none of them
    can be compared against the stored cutoff the run is supposed to match.
    """
    body = _body()
    body["conventions"] = _conventions() | {"knowledge_time": declared}
    with pytest.raises(ResearchRunError, match="knowledge_time must"):
        parse_research_run_request(_raw(body))


def test_a_missing_execution_mode_is_refused_rather_than_defaulted() -> None:
    body = _body()
    del body["execution_mode"]
    with pytest.raises(ResearchRunError, match="missing"):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize("mode", ["certified", "production", "backtest", ""])
def test_only_the_research_mode_is_accepted(mode: str) -> None:
    body = _body()
    body["execution_mode"] = mode
    with pytest.raises(ResearchRunError, match="execution_mode must be"):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize(
    "convention", ["knowledge_time", "calendar", "cost", "capital", "currency"]
)
def test_every_convention_must_be_declared(convention: str) -> None:
    body = _body()
    body["conventions"] = {k: v for k, v in _conventions().items() if k != convention}
    with pytest.raises(ResearchRunError, match="conventions is missing"):
        parse_research_run_request(_raw(body))


def test_an_empty_uncertainty_list_is_refused() -> None:
    body = _body()
    body["uncertainty"] = []
    with pytest.raises(ResearchRunError, match="at least one note"):
        parse_research_run_request(_raw(body))


def test_observations_cannot_be_empty() -> None:
    body = _body()
    body["observations"] = []
    with pytest.raises(ResearchRunError, match="non-empty array"):
        parse_research_run_request(_raw(body))


def test_a_repeated_generation_and_role_is_refused() -> None:
    body = _body()
    body["observations"] = [_pin(), _pin()]
    with pytest.raises(ResearchRunError, match="repeat a generation"):
        parse_research_run_request(_raw(body))


def test_an_instrument_map_key_outside_the_observation_namespace_is_refused() -> None:
    body = _body()
    body["instrument_map"] = {"SPY": "SPY"}
    with pytest.raises(ResearchRunError, match="must name an aas-obs- series"):
        parse_research_run_request(_raw(body))


def test_two_series_cannot_collapse_onto_one_instrument() -> None:
    body = _body()
    body["instrument_map"] = {"aas-obs-1": "SYN1", "aas-obs-2": "SYN1"}
    with pytest.raises(ResearchRunError, match="two series onto one instrument"):
        parse_research_run_request(_raw(body))


def test_an_empty_instrument_map_is_refused() -> None:
    body = _body()
    body["instrument_map"] = {}
    with pytest.raises(ResearchRunError, match="non-empty object"):
        parse_research_run_request(_raw(body))


def test_a_wrong_schema_version_is_refused() -> None:
    body = _body()
    body["schema_version"] = "aas-backtest-v1"
    with pytest.raises(ResearchRunError, match="is not " + RESEARCH_RUN_SCHEMA):
        parse_research_run_request(_raw(body))


def test_an_unknown_root_key_is_refused() -> None:
    body = _body()
    body["certified"] = True
    with pytest.raises(ResearchRunError, match="unknown keys"):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize("field", ["raw_sha256", "contract_sha256"])
def test_a_strategy_pin_needs_real_digests(field: str) -> None:
    body = _body()
    body["strategy"] = _strategy() | {field: "not-a-digest"}
    with pytest.raises(ResearchRunError, match="lowercase sha256 digest"):
        parse_research_run_request(_raw(body))


def test_an_unknown_observation_role_is_refused() -> None:
    body = _body()
    body["observations"] = [_pin(role="settlement")]
    with pytest.raises(ResearchRunError, match="open or close"):
        parse_research_run_request(_raw(body))


def test_the_same_request_hashes_the_same_way_twice() -> None:
    first = parse_research_run_request(_raw(_body()))
    second = parse_research_run_request(_raw(_body()))
    assert first.request_sha256 == second.request_sha256
    assert _provenance(first) == _provenance(second)


def test_the_sidecar_records_the_declared_semantics_and_unknown_parity() -> None:
    recorded = _provenance(parse_research_run_request(_raw(_body())))
    assert cast("dict[str, object]", recorded["semantics"])["tie_rule"] == TIE_RULE
    assert cast("dict[str, object]", recorded["semantics"])["data_basis"] == "M"
    assert cast("dict[str, object]", recorded["semantics"])["source_parity"] == "unknown"
    assert recorded["unsettled"] == []
    assert cast("dict[str, object]", recorded["semantics"])["fill_price"] == FILL_CONVENTION
    assert cast("dict[str, object]", recorded["semantics"])["source_parity"] == "unknown"


@pytest.mark.parametrize("basis", ["either", "d", "daily", "DM"])
def test_data_basis_names_one_confirmed_identity(basis: str) -> None:
    body = _body()
    body["semantics"] = _semantics() | {"data_basis": basis}
    with pytest.raises(ResearchRunError, match="data_basis must be D or M"):
        parse_research_run_request(_raw(body))


def test_a_null_abs_compare_is_refused_because_null_does_not_disable_it() -> None:
    body = _body()
    body["semantics"] = _semantics() | {"abs_compare": None}
    with pytest.raises(ResearchRunError, match="abs_compare must be nonempty"):
        parse_research_run_request(_raw(body))


def test_claiming_extended_history_requires_naming_its_source() -> None:
    body = _body()
    body["semantics"] = _semantics() | {"expand": "extended-history-used"}
    with pytest.raises(ResearchRunError, match="expand_source must be nonempty"):
        parse_research_run_request(_raw(body))


def test_an_expand_source_without_extended_history_is_refused() -> None:
    body = _body()
    body["semantics"] = _semantics() | {"expand_source": "some-index"}
    with pytest.raises(ResearchRunError, match="belongs only to extended-history-used"):
        parse_research_run_request(_raw(body))


def test_the_tie_rule_is_fixed_and_cannot_be_renamed() -> None:
    body = _body()
    body["semantics"] = _semantics() | {"tie_rule": "holding-preference"}
    with pytest.raises(ResearchRunError, match="tie_rule must be " + TIE_RULE):
        parse_research_run_request(_raw(body))


def _daily(body: dict[str, object]) -> dict[str, object]:
    """The same declaration on the daily basis, where nothing settled the fill."""
    body["semantics"] = _semantics() | {"data_basis": "D", "fill_price": "next-session-open"}
    body["unsettled"] = ["fill_price"]
    return body


def test_a_daily_run_still_cannot_present_the_fill_convention_as_settled() -> None:
    """R1 resolved the monthly path only. The daily one keeps the question open."""
    body = _daily(_body())
    body["unsettled"] = []
    with pytest.raises(ResearchRunError, match="unsettled must list: fill_price"):
        parse_research_run_request(_raw(body))


def test_a_daily_run_declares_a_hypothesis_and_records_it_as_open() -> None:
    request = parse_research_run_request(_raw(_daily(_body())))
    assert request.semantics.data_basis == "D"
    assert request.unsettled == ("fill_price",)


@pytest.mark.parametrize("price", ["next-session-close", "vwap", "open", FILL_CONVENTION])
def test_an_unsupported_daily_fill_price_is_refused(price: str) -> None:
    body = _daily(_body())
    body["semantics"] = _semantics() | {"data_basis": "D", "fill_price": price}
    with pytest.raises(ResearchRunError, match="fill_price must be"):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize("price", ["next-session-open", "decision-close", "vwap"])
def test_a_monthly_run_must_name_the_adopted_convention(price: str) -> None:
    """A monthly run that still names a hypothesis is refused, not quietly upgraded."""
    body = _body()
    body["semantics"] = _semantics() | {"fill_price": price}
    with pytest.raises(ResearchRunError, match="must declare fill_price " + FILL_CONVENTION):
        parse_research_run_request(_raw(body))


def test_a_monthly_run_cannot_keep_reporting_the_settled_axis_as_open() -> None:
    body = _body()
    body["unsettled"] = ["fill_price"]
    with pytest.raises(ResearchRunError, match="cannot list fill_price"):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize(
    ("terms", "message"),
    [
        ({"cost": -0.0001, "initial_cash": 10000.0}, "cost must not be negative"),
        ({"cost": 0.0003, "initial_cash": 0.0}, "initial_cash must be positive"),
        ({"cost": 0.0003, "initial_cash": -1.0}, "initial_cash must be positive"),
        ({"cost": True, "initial_cash": 10000.0}, "cost must be a number"),
    ],
)
def test_execution_terms_are_refused_rather_than_corrected(
    terms: dict[str, object], message: str
) -> None:
    """A run with no account is still a run that produces a number, so it is refused."""
    body = _body()
    body["execution"] = terms
    with pytest.raises(ResearchRunError, match=message):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize("field", ["period", "history"])
def test_a_window_that_ends_before_it_starts_is_refused(field: str) -> None:
    body = _body()
    body[field] = {"start": "2026-03-30", "end": "2026-01-29"}
    with pytest.raises(ResearchRunError, match="must not follow its end"):
        parse_research_run_request(_raw(body))


def test_a_membership_of_another_kind_is_refused() -> None:
    """The reference has to be the ensemble membership, not some other definition."""
    body = _body()
    body["membership"] = {
        "kind": "derived",
        "id": "synthetic-membership",
        "version": "1",
        "hash": DIGEST_A,
    }
    with pytest.raises(ResearchRunError, match="membership kind must be membership"):
        parse_research_run_request(_raw(body))


def test_a_floating_membership_version_is_refused() -> None:
    """latest is not a pin: the same declaration would name different bytes over time."""
    body = _body()
    body["membership"] = cast("dict[str, object]", _body()["membership"]) | {"version": "latest"}
    with pytest.raises(ResearchRunError, match="must be exact, not latest"):
        parse_research_run_request(_raw(body))


def test_a_calendar_that_claims_another_basis_is_refused() -> None:
    """The only calendar this path can supply is the dates the panel was recorded on."""
    body = _body()
    body["calendar"] = {"calendar_id": "XNYS", "basis": "exchange-certified"}
    with pytest.raises(ResearchRunError, match="calendar basis must be"):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize("cost", [1.0, 1.5, 42.0])
def test_a_cost_that_consumes_the_whole_fill_is_refused(cost: float) -> None:
    """A rate at or above one produces a number no account could have reached."""
    body = _body()
    body["execution"] = {"cost": cost, "initial_cash": 10000.0}
    with pytest.raises(ResearchRunError, match="cost must be below 1"):
        parse_research_run_request(_raw(body))


def test_the_knowledge_time_keeps_every_microsecond() -> None:
    """The cutoff decides which revisions a projection admits, so it must be exact.

    Seconds since the epoch multiplied as a float rounds a distant instant. These two
    declarations are one microsecond apart and must stay one microsecond apart.
    """
    body = _body()
    conventions = _conventions()
    pairs = []
    for moment, expected in (
        ("2262-04-11T23:47:16.854775+00:00", 9223372036854775),
        ("2262-04-11T23:47:16.854776+00:00", 9223372036854776),
    ):
        body["conventions"] = conventions | {"knowledge_time": moment}
        pairs.append(
            (parse_research_run_request(_raw(body)).conventions.knowledge_time_us, expected)
        )
    assert [observed for observed, _ in pairs] == [expected for _, expected in pairs]


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_account_terms_never_reach_the_contract(token: str) -> None:
    """The codec refuses a non-finite constant before the declaration is parsed.

    Worth pinning because NaN compares false against every bound, so if one ever did
    arrive it would pass each check in turn and fail somewhere inside the accounting.
    The contract keeps its own finiteness guard for a caller that does not come through
    JSON; this test records where the refusal actually happens today.
    """
    raw = _raw(_body()).replace(b'"cost": 0.0003', b'"cost": ' + token.encode())
    with pytest.raises(ContractParseError, match="non-finite JSON constant"):
        parse_research_run_request(raw)


def test_a_floating_strategy_version_is_refused() -> None:
    """An exact request cannot name a version whose bytes change underneath it."""
    body = _body()
    body["strategy"] = _strategy() | {"version": "latest"}
    with pytest.raises(ResearchRunError, match="strategy version must be exact"):
        parse_research_run_request(_raw(body))


def test_a_floating_observation_version_is_refused() -> None:
    body = _body()
    body["observations"] = [_pin() | {"version": "latest"}, _pin("obs-synthetic-close", "close")]
    with pytest.raises(ResearchRunError, match="observation version must be exact"):
        parse_research_run_request(_raw(body))


def test_an_account_term_too_large_for_a_float_is_refused_not_raised() -> None:
    """A huge JSON integer overflows the finiteness check; that is a refusal, not a leak."""
    raw = _raw(_body()).replace(b'"initial_cash": 10000.0', b'"initial_cash": ' + b"9" * 400)
    with pytest.raises(ResearchRunError, match="initial_cash must be finite"):
        parse_research_run_request(raw)
