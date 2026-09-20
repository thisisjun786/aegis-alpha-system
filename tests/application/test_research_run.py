"""The declared research-run contract refuses before anything durable is written.

Every case here is a way an uncertified result could quietly acquire more standing
than its inputs earned: an absent mode, an inferred convention, an unmapped series,
a silent instrument collision, or no recorded reservation at all.
"""

from __future__ import annotations

import json

import pytest

from aegis_alpha.application.research_run import (
    EXECUTION_MODE,
    RESEARCH_RUN_SCHEMA,
    TIE_RULE,
    ResearchRunError,
    declared_provenance,
    parse_research_run_request,
)

DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64
EXPECTED_PINS = 2


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
        "strategy_id": "synthetic-strategy",
        "version": "1",
        "raw_sha256": DIGEST_A,
        "contract_sha256": DIGEST_B,
    }


def _conventions() -> dict[str, str]:
    return {
        "knowledge_time": "declared: panel carries no knowledge times",
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
        "fill_price": "next-session-open",
        "tie_rule": TIE_RULE,
    }


def _body() -> dict[str, object]:
    return {
        "schema_version": RESEARCH_RUN_SCHEMA,
        "execution_mode": EXECUTION_MODE,
        "strategy": _strategy(),
        "observations": [_pin(), _pin("obs-synthetic-close", "close")],
        "instrument_map": {"aas-obs-1": "SYN1", "aas-obs-2": "SYN2"},
        "conventions": _conventions(),
        "semantics": _semantics(),
        "unsettled": ["fill_price"],
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
    recorded = json.loads(declared_provenance(request))
    assert recorded["execution_mode"] == EXECUTION_MODE
    assert recorded["certified"] is False
    assert recorded["point_in_time_certified"] is False
    assert recorded["executable_prices"] is False
    assert recorded["uncertainty"]


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
    assert declared_provenance(first) == declared_provenance(second)


def test_the_sidecar_records_the_declared_semantics_and_unknown_parity() -> None:
    recorded = json.loads(declared_provenance(parse_research_run_request(_raw(_body()))))
    assert recorded["semantics"]["tie_rule"] == TIE_RULE
    assert recorded["semantics"]["data_basis"] == "M"
    assert recorded["semantics"]["source_parity"] == "unknown"
    assert recorded["unsettled"] == ["fill_price"]


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


def test_a_run_cannot_present_the_fill_convention_as_settled() -> None:
    body = _body()
    body["unsettled"] = []
    with pytest.raises(ResearchRunError, match="unsettled must list: fill_price"):
        parse_research_run_request(_raw(body))


@pytest.mark.parametrize("price", ["next-session-close", "vwap", "open"])
def test_an_unsupported_fill_price_is_refused(price: str) -> None:
    body = _body()
    body["semantics"] = _semantics() | {"fill_price": price}
    with pytest.raises(ResearchRunError, match="fill_price must be"):
        parse_research_run_request(_raw(body))
