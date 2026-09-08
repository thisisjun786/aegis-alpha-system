from __future__ import annotations

import copy
import hashlib
import importlib
import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
probe = importlib.import_module("scripts.probe_finimpulse_earnings")
SCRIPT = ROOT / "scripts" / "probe_finimpulse_earnings.py"
FIXTURE = ROOT / "tests" / "fixtures" / "provider_neutral" / "finimpulse_earnings_probe.json"
ERROR_EXIT_CODE = 2
EXPECTED_SYMBOL_COUNT = 2
EXPECTED_TYPE_SYMBOL_CELLS = 4
EXPECTED_RATE_LIMIT = 2000
EXPECTED_MINIMUM_REMAINING = 1996
EXPECTED_PARTIAL_CALL_COUNT = 4


def run_probe(
    *arguments: str,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT), *arguments],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )


def write_mutated_fixture(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
) -> Path:
    raw = json.loads(FIXTURE.read_text())
    mutate(raw)
    fixture_path = tmp_path / "mutated.json"
    fixture_path.write_text(json.dumps(raw))
    return fixture_path


def mutate_response(
    raw: dict[str, Any],
    mutate: Callable[[dict[str, Any]], None],
    *,
    call_index: int = 0,
) -> None:
    calls = raw["calls"]
    assert isinstance(calls, list)
    call = calls[call_index]
    assert isinstance(call, dict)
    response_envelope = call["response"]
    assert isinstance(response_envelope, dict)
    response = json.loads(response_envelope["body_text"])
    mutate(response)
    body_text = json.dumps(response, separators=(",", ":"), sort_keys=True)
    response_envelope["body_text"] = body_text
    response_envelope["body_sha256"] = hashlib.sha256(body_text.encode()).hexdigest()


def test_offline_raw_fixture_produces_fail_closed_coverage_receipt(tmp_path: Path) -> None:
    receipt_path = tmp_path / "receipt.json"

    result = run_probe(
        "--offline-raw",
        str(FIXTURE),
        "--receipt-output",
        str(receipt_path),
    )

    assert result.returncode == 0, result.stderr
    receipt = json.loads(receipt_path.read_text())
    assert receipt["task_id"] == "AAS-DATA-008A"
    assert receipt["probe_status"] == "PROBE_ONLY_NOT_PROMOTED"
    assert receipt["coverage"]["observed_symbol_count"] == EXPECTED_SYMBOL_COUNT
    assert receipt["coverage"]["requested_type_symbol_cells"] == EXPECTED_TYPE_SYMBOL_CELLS
    assert receipt["coverage"]["missing_type_symbol_cells"] == 1
    assert receipt["coverage"]["missing_type_symbol_rate"] == "0.25"
    assert receipt["cost"]["provider_reported_total_usd"] == "0.00425"
    assert receipt["pagination"]["status"] == "PASS"
    assert receipt["rate_limit"]["observed_limit"] == EXPECTED_RATE_LIMIT
    assert receipt["rate_limit"]["minimum_remaining"] == EXPECTED_MINIMUM_REMAINING
    assert receipt["rate_limit"]["window"] == {
        "seconds": None,
        "status": "NOT_OBSERVED",
    }
    assert receipt["credential_handling"] == {
        "artifact_schema_validation": "PASS",
        "console_disclosure": "NOT_OBSERVED",
        "external_persistence": "NOT_OBSERVED",
        "source_name": "environment:FINIMPULSE_API_TOKEN",
    }
    assert receipt["raw_fixture"] == {
        "byte_length": len(FIXTURE.read_bytes()),
        "kind": "synthetic_provider_neutral_contract",
        "logical_id": f"sha256:{hashlib.sha256(FIXTURE.read_bytes()).hexdigest()}",
    }
    assert receipt["pit_assessment"]["status"] == "NOT_PROVEN"
    assert receipt["pit_assessment"]["backtest_eligible"] is False
    assert receipt["eligibility"] == {
        "backtest": False,
        "canonical": False,
        "order": False,
        "paper": False,
    }


def test_same_raw_bytes_at_different_paths_produce_byte_identical_receipts(
    tmp_path: Path,
) -> None:
    first_raw = tmp_path / "first" / "raw.json"
    second_raw = tmp_path / "second" / "renamed.json"
    first_raw.parent.mkdir()
    second_raw.parent.mkdir()
    first_raw.write_bytes(FIXTURE.read_bytes())
    second_raw.write_bytes(FIXTURE.read_bytes())
    first_receipt = tmp_path / "first-receipt.json"
    second_receipt = tmp_path / "second-receipt.json"

    first_result = run_probe(
        "--offline-raw",
        str(first_raw),
        "--receipt-output",
        str(first_receipt),
    )
    second_result = run_probe(
        "--offline-raw",
        str(second_raw),
        "--receipt-output",
        str(second_receipt),
    )

    assert first_result.returncode == 0, first_result.stderr
    assert second_result.returncode == 0, second_result.stderr
    assert first_receipt.read_bytes() == second_receipt.read_bytes()


def test_live_probe_requires_explicit_opt_in_before_network(tmp_path: Path) -> None:
    marker_token = "credential-must-not-appear"  # noqa: S105
    env = os.environ.copy()
    env["FINIMPULSE_API_TOKEN"] = marker_token

    result = run_probe(
        "--raw-output",
        str(tmp_path / "raw.json"),
        "--receipt-output",
        str(tmp_path / "receipt.json"),
        env=env,
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert "--live is required" in result.stderr
    assert marker_token not in result.stdout
    assert marker_token not in result.stderr


def test_live_probe_rejects_raw_output_inside_git_repository(tmp_path: Path) -> None:
    marker_token = "credential-must-not-appear"  # noqa: S105
    env = os.environ.copy()
    env["FINIMPULSE_API_TOKEN"] = marker_token

    result = run_probe(
        "--live",
        "--symbols",
        "AAPL",
        "--raw-output",
        str(ROOT / "would-persist-live-raw.json"),
        "--receipt-output",
        str(tmp_path / "receipt.json"),
        env=env,
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert "outside the Git repository" in result.stderr
    assert marker_token not in result.stdout
    assert marker_token not in result.stderr


def test_live_probe_fails_closed_without_environment_credential(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.pop("FINIMPULSE_API_TOKEN", None)
    raw_path = tmp_path / "raw.json"
    receipt_path = tmp_path / "receipt.json"

    result = run_probe(
        "--live",
        "--raw-output",
        str(raw_path),
        "--receipt-output",
        str(receipt_path),
        env=env,
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert "FINIMPULSE_API_TOKEN is required" in result.stderr
    assert not raw_path.exists()
    assert not receipt_path.exists()


def test_live_probe_rejects_more_than_twenty_symbols_without_echoing_token(
    tmp_path: Path,
) -> None:
    marker_token = "credential-must-not-appear"  # noqa: S105
    env = os.environ.copy()
    env["FINIMPULSE_API_TOKEN"] = marker_token
    symbols = [f"SYM{index}" for index in range(21)]

    result = run_probe(
        "--live",
        "--symbols",
        *symbols,
        "--raw-output",
        str(tmp_path / "raw.json"),
        "--receipt-output",
        str(tmp_path / "receipt.json"),
        env=env,
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert "at most 20 symbols" in result.stderr
    assert marker_token not in result.stdout
    assert marker_token not in result.stderr


def test_offline_fixture_rejects_credential_like_response_fields(tmp_path: Path) -> None:
    raw = json.loads(FIXTURE.read_text())
    response = json.loads(raw["calls"][0]["response"]["body_text"])
    response["api_key"] = "must-not-persist"
    body_text = json.dumps(response)
    raw["calls"][0]["response"]["body_text"] = body_text
    raw["calls"][0]["response"]["body_sha256"] = hashlib.sha256(body_text.encode()).hexdigest()
    unsafe_fixture = tmp_path / "unsafe.json"
    unsafe_fixture.write_text(json.dumps(raw))

    result = run_probe(
        "--offline-raw",
        str(unsafe_fixture),
        "--receipt-output",
        str(tmp_path / "receipt.json"),
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert "unexpected response fields" in result.stderr
    assert "must-not-persist" not in result.stdout
    assert "must-not-persist" not in result.stderr


def _set_provider_status_error(response: dict[str, Any]) -> None:
    response["status_code"] = 50000


def _set_response_symbol_mismatch(response: dict[str, Any]) -> None:
    data = response["data"]
    assert isinstance(data, dict)
    data["symbol"] = "MSFT"


def _set_response_types_mismatch(response: dict[str, Any]) -> None:
    data = response["data"]
    assert isinstance(data, dict)
    data["types"] = ["eps_trend"]


def _set_items_count_mismatch(response: dict[str, Any]) -> None:
    result = response["result"]
    assert isinstance(result, dict)
    result["items_count"] = 99


def _set_total_count_mismatch(response: dict[str, Any]) -> None:
    result = response["result"]
    assert isinstance(result, dict)
    result["total_count"] = 0


def _set_limit_mismatch(response: dict[str, Any]) -> None:
    data = response["data"]
    assert isinstance(data, dict)
    data["limit"] = 19


def _set_malformed_items(response: dict[str, Any]) -> None:
    result = response["result"]
    assert isinstance(result, dict)
    result["items"] = {}


def _add_duplicate_item(response: dict[str, Any]) -> None:
    result = response["result"]
    assert isinstance(result, dict)
    items = result["items"]
    assert isinstance(items, list)
    items.append(copy.deepcopy(items[0]))
    result["items_count"] = len(items)
    result["total_count"] = len(items)


def _add_unknown_response_field(response: dict[str, Any]) -> None:
    response["vendor_note"] = "schema drift"


@pytest.mark.parametrize(
    ("mutate", "expected_error"),
    [
        (_set_provider_status_error, "provider status_code"),
        (_set_response_symbol_mismatch, "response symbol"),
        (_set_response_types_mismatch, "response types"),
        (_set_items_count_mismatch, "items_count"),
        (_set_total_count_mismatch, "total_count"),
        (_set_limit_mismatch, "response limit"),
        (_set_malformed_items, "result.items"),
        (_add_duplicate_item, "duplicate record"),
        (_add_unknown_response_field, "unexpected response fields"),
    ],
)
def test_offline_replay_rejects_provider_semantic_mismatch(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
    expected_error: str,
) -> None:
    fixture_path = write_mutated_fixture(
        tmp_path,
        lambda raw: mutate_response(raw, mutate),
    )

    result = run_probe(
        "--offline-raw",
        str(fixture_path),
        "--receipt-output",
        str(tmp_path / "receipt.json"),
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert expected_error in result.stderr


def test_offline_replay_rejects_incomplete_call_set(tmp_path: Path) -> None:
    def remove_last_call(raw: dict[str, Any]) -> None:
        calls = raw["calls"]
        assert isinstance(calls, list)
        calls.pop()

    fixture_path = write_mutated_fixture(tmp_path, remove_last_call)

    result = run_probe(
        "--offline-raw",
        str(fixture_path),
        "--receipt-output",
        str(tmp_path / "receipt.json"),
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert "call set" in result.stderr


def test_offline_replay_rejects_non_finite_or_negative_cost(tmp_path: Path) -> None:
    fixture_path = write_mutated_fixture(
        tmp_path,
        lambda raw: mutate_response(raw, lambda response: response.__setitem__("cost", "NaN")),
    )

    result = run_probe(
        "--offline-raw",
        str(fixture_path),
        "--receipt-output",
        str(tmp_path / "receipt.json"),
    )

    assert result.returncode == ERROR_EXIT_CODE
    assert "finite non-negative cost" in result.stderr


def test_quota_exhaustion_preserves_partial_call_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_fixture = json.loads(FIXTURE.read_text())
    template_call = copy.deepcopy(raw_fixture["calls"][0])
    response_envelope = template_call["response"]
    response = json.loads(response_envelope["body_text"])
    response["live"] = True
    body_text = json.dumps(response, separators=(",", ":"), sort_keys=True)
    response_envelope["body_text"] = body_text
    response_envelope["body_sha256"] = hashlib.sha256(body_text.encode()).hexdigest()
    response_envelope["headers"]["x-ratelimit-remaining"] = "0"

    def fake_execute_call(
        body: Mapping[str, object],
        _token: str,
    ) -> dict[str, object]:
        call = copy.deepcopy(template_call)
        call["request"]["body"] = body
        return call

    raw_output = tmp_path / "partial.json"
    receipt_output = tmp_path / "receipt.json"
    args = probe.parse_args(
        [
            "--live",
            "--symbols",
            "AAPL",
            "--raw-output",
            str(raw_output),
            "--receipt-output",
            str(receipt_output),
        ]
    )
    monkeypatch.setenv("FINIMPULSE_API_TOKEN", "marker")

    with pytest.raises(probe.ProbeError, match="remaining quota"):
        probe.execute_probe(args, call_executor=fake_execute_call)

    partial = json.loads(raw_output.read_text())
    assert partial["probe_status"] == "PARTIAL_FAILED"
    assert partial["completed_call_count"] == 1
    assert len(partial["calls"]) == 1
    assert not receipt_output.exists()


def completed_live_raw() -> dict[str, Any]:
    raw = json.loads(FIXTURE.read_text())
    calls = raw["calls"]
    raw["fixture_kind"] = "live_provider_probe"
    raw["probe_status"] = "COMPLETED"
    raw["completed_call_count"] = len(calls)
    raw.pop("failure", None)
    return raw


def build_receipt_from_raw(raw: dict[str, Any]) -> dict[str, Any]:
    raw_bytes = json.dumps(raw, separators=(",", ":"), sort_keys=True).encode()
    return probe.build_receipt(raw, raw_bytes)


def test_all_calls_partial_live_raw_cannot_become_success_receipt() -> None:
    raw = completed_live_raw()
    raw["probe_status"] = "PARTIAL_FAILED"
    raw["failure"] = {"message": "last call quota failure"}

    with pytest.raises(probe.ProbeError, match=r"probe_status.*COMPLETED"):
        build_receipt_from_raw(raw)


def test_live_raw_without_completion_status_cannot_create_receipt() -> None:
    raw = completed_live_raw()
    raw.pop("probe_status")

    with pytest.raises(probe.ProbeError, match=r"probe_status.*COMPLETED"):
        build_receipt_from_raw(raw)


def test_completed_live_raw_with_failure_field_cannot_create_receipt() -> None:
    raw = completed_live_raw()
    raw["failure"] = {"message": "must not coexist with COMPLETED"}

    with pytest.raises(probe.ProbeError, match="failure field"):
        build_receipt_from_raw(raw)


@pytest.mark.parametrize(("completed_delta", "plan_delta"), [(-1, 0), (0, -1)])
def test_live_raw_requires_completed_plan_and_actual_call_counts_to_match(
    completed_delta: int,
    plan_delta: int,
) -> None:
    raw = completed_live_raw()
    calls = raw["calls"]
    raw["completed_call_count"] = len(calls) + completed_delta
    raw["probe_plan"]["call_count"] = len(calls) + plan_delta

    with pytest.raises(probe.ProbeError, match=r"completed_call_count.*probe_plan.*calls"):
        build_receipt_from_raw(raw)


def test_completed_live_raw_over_budget_cannot_create_receipt() -> None:
    raw = completed_live_raw()
    mutate_response(raw, lambda response: response.__setitem__("cost", 0.1))

    with pytest.raises(probe.ProbeError, match=r"cost exceeds.*budget"):
        build_receipt_from_raw(raw)


def live_call_executor_with_final_mutation(
    mutate_final: Callable[[dict[str, Any]], None],
) -> Callable[[Mapping[str, object], str], dict[str, object]]:
    templates = json.loads(FIXTURE.read_text())["calls"]
    call_index = 0

    def execute(body: Mapping[str, object], _token: str) -> dict[str, object]:
        nonlocal call_index
        call = copy.deepcopy(templates[call_index])
        call["request"]["body"] = dict(body)
        response_envelope = call["response"]
        response = json.loads(response_envelope["body_text"])
        response["live"] = True
        if call_index == len(templates) - 1:
            mutate_final(call)
            response = json.loads(response_envelope["body_text"])
            response["live"] = True
        body_text = json.dumps(response, separators=(",", ":"), sort_keys=True)
        response_envelope["body_text"] = body_text
        response_envelope["body_sha256"] = hashlib.sha256(body_text.encode()).hexdigest()
        call_index += 1
        return call

    return execute


def remove_remaining_quota(call: dict[str, Any]) -> None:
    call["response"]["headers"].pop("x-ratelimit-remaining")


def set_over_budget_cost(call: dict[str, Any]) -> None:
    response_envelope = call["response"]
    response = json.loads(response_envelope["body_text"])
    response["cost"] = 0.1
    response_envelope["body_text"] = json.dumps(response, separators=(",", ":"), sort_keys=True)


@pytest.mark.parametrize(
    "mutate_final",
    [remove_remaining_quota, set_over_budget_cost],
    ids=["last-call-quota-missing", "last-call-cost-over-budget"],
)
def test_last_call_failure_partial_ledger_cannot_create_success_receipt(
    mutate_final: Callable[[dict[str, Any]], None],
) -> None:
    executor = live_call_executor_with_final_mutation(mutate_final)

    with pytest.raises(probe.PartialProbeError) as partial_error:
        probe.run_live_probe(["AAPL", "PLAB"], "marker", call_executor=executor)

    partial = partial_error.value.raw
    assert partial["probe_status"] == "PARTIAL_FAILED"
    partial_calls = partial["calls"]
    assert isinstance(partial_calls, list)
    assert partial["completed_call_count"] == len(partial_calls) == EXPECTED_PARTIAL_CALL_COUNT
    with pytest.raises(probe.ProbeError, match=r"probe_status.*COMPLETED"):
        build_receipt_from_raw(partial)
