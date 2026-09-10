from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.data import qveris_acquisition as acquisition
from aegis_alpha.data.qveris_acquisition_cli import main
from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.data.qveris_contracts import (
    EOD_TOOL,
    FRED_TOOL,
    QverisJob,
    load_jobs,
    object_value,
)
from aegis_alpha.data.qveris_store import QverisStore
from aegis_alpha.data.serialization import canonical_json_bytes

NOW = datetime(2026, 9, 6, tzinfo=UTC)
PRICE = {
    "code": "AAA",
    "exchange_short_name": "US",
    "date": "2026-08-31",
    "open": 10,
    "high": 12,
    "low": 9,
    "close": 11,
    "adjusted_close": 11,
    "volume": 100,
}


def eod_job() -> QverisJob:
    return QverisJob(
        "us-bulk",
        EOD_TOOL,
        "eodhd",
        "US",
        "prices",
        canonical_json_bytes({"exchange": "US", "date": "2026-08-31", "fmt": "json"}).decode(),
        date(2026, 9, 6),
    )


def fred_job() -> QverisJob:
    return QverisJob(
        "korea-macro",
        FRED_TOOL,
        "stlouisfed_fred",
        "KR",
        "macro",
        canonical_json_bytes(
            {
                "series_id": "SYNTHETIC",
                "file_type": "json",
                "observation_start": "2026-01-01",
                "observation_end": "2026-08-31",
                "realtime_start": "2026-08-31",
                "realtime_end": "2026-08-31",
                "limit": 2,
                "offset": 0,
            }
        ).decode(),
        date(2026, 9, 6),
    )


class FakeQveris:
    account_key = "synthetic-credential-digest"

    def __init__(self) -> None:
        self.balance = Decimal(100)
        self.price = Decimal("2.81")
        self.usage: list[dict[str, object]] = []
        self.ledger: list[dict[str, object]] = []
        self.calls: list[str] = []
        self.execute_count = 0
        self.failure: str | None = None
        self.usage_ready = True
        self.ledger_ready = True

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        self.calls.append(path)
        body, query = body or {}, query or {}
        if path == "/tools/by-ids":
            tool = cast("list[str]", body["tool_ids"])[0]
            doc: dict[str, object] = {
                "search_id": f"search-{len(self.calls)}",
                "results": [
                    {
                        "tool_id": tool,
                        "provider_id": "eodhd" if tool == EOD_TOOL else "stlouisfed_fred",
                        "params": [{"name": "synthetic", "type": "string"}],
                    }
                ],
            }
        elif path == "/tools/probe":
            doc = {
                "schema": {"valid": self.failure != "schema"},
                "quote": {
                    "estimate_credits": str(self.price),
                    "exact": True,
                    "currency": "credits",
                    "basis": "per_call",
                },
            }
        elif path == "/auth/credits":
            doc = {"status": "success", "data": {"remaining_credits": str(self.balance)}}
        elif path == "/auth/credits/ledger":
            doc = self._page(self.ledger if self.ledger_ready else [], query)
        elif path == "/auth/usage/history/v2":
            rows = [row for row in self.usage if row["search_id"] == query.get("search_id")]
            doc = self._page(rows if self.usage_ready else [], query)
        elif path == "/tools/execute":
            doc = self._execute(body, query)
        else:
            raise AssertionError(f"unexpected fake endpoint {path}")
        return QverisResponse(
            200, (("content-type", "application/json"),), canonical_json_bytes(doc), NOW, NOW
        )

    @staticmethod
    def _page(rows: list[dict[str, object]], query: dict[str, str | int]) -> dict[str, object]:
        return {
            "status": "success",
            "data": {
                "items": rows,
                "total": len(rows),
                "page": query["page"],
                "page_size": query["page_size"],
            },
        }

    def _execute(self, body: dict[str, object], query: dict[str, str | int]) -> dict[str, object]:
        self.execute_count += 1
        execution = f"exec-{self.execute_count}"
        self.balance -= self.price
        usage = {
            "id": execution,
            "execution_id": execution,
            "tool_id": query["tool_id"],
            "session_id": body["session_id"],
            "search_id": body["search_id"],
            "success": True,
            "charge_outcome": "charged",
            "billing_unit": "call",
            "quantity": 1,
            "actual_amount_credits": str(self.price),
            "settled_amount_credits": str(self.price),
        }
        self.usage.append(usage)
        self.ledger.append(
            {"id": execution, "amount_credits": str(-self.price), "execution_id": None}
        )
        if self.failure == "timeout":
            raise TimeoutError("synthetic lost response")
        parameters = object_value(body["parameters"])
        payload: object = self._fred(parameters) if query["tool_id"] == FRED_TOOL else [dict(PRICE)]
        result: dict[str, object] = {"status_code": 200, "data": payload}
        if self.failure == "truncated":
            result["truncated_content"] = "partial"
        if self.failure == "wrong-session":
            usage["session_id"] = "another-client"
        if self.failure == "included":
            usage.update(
                charge_outcome="included", outcome="partial_success", reason_code="provider.error"
            )
        if self.failure == "failed-not-charged":
            self.balance += self.price
            self.ledger.pop()
            usage.update(
                charge_outcome="failed_not_charged",
                actual_amount_credits="0",
                settled_amount_credits="0",
                quantity=None,
                success=False,
                billable_success=False,
            )
            return {
                "execution_id": execution,
                "success": False,
                "result": {"status_code": 200, "data": "unusable provider text"},
            }
        if self.failure == "duplicate-usage":
            self.usage.append({**usage, "id": execution + "-duplicate"})
        if self.failure == "over-quote":
            self.balance -= 1
            usage["actual_amount_credits"] = usage["settled_amount_credits"] = str(self.price + 1)
            self.ledger[-1]["amount_credits"] = str(-self.price - 1)
        if self.failure == "deposit":
            self.balance += 100
            self.ledger.append({"id": "deposit", "amount_credits": "100"})
        return {"execution_id": execution, "success": True, "result": result}

    def _fred(self, parameters: dict[str, object]) -> dict[str, object]:
        offset = cast("int", parameters["offset"])
        dates = ["2026-01-01", "2026-02-01", "2026-03-01"]
        rows = [
            {
                "date": day,
                "realtime_start": "2026-08-31",
                "realtime_end": "2026-08-31",
                "value": "." if day == dates[0] else "10.5",
            }
            for day in dates[offset : offset + 2]
        ]
        if self.failure == "short-page":
            rows = rows[:1]
        if self.failure == "future-vintage":
            rows[0]["realtime_start"] = "2026-09-06"
        if self.failure == "repeated-page" and offset:
            rows[0]["date"] = dates[0]
        return {
            "count": len(dates),
            "offset": offset,
            "limit": 2,
            "observations": rows,
            "realtime_start": "2026-08-31",
            "realtime_end": "2026-08-31",
        }


def test_complete_reuses_raw_without_any_http_even_when_job_is_renamed(tmp_path: Path) -> None:
    client = FakeQveris()
    job = eod_job()
    first = acquisition.acquire_jobs((job,), tmp_path, client)
    calls = len(client.calls)
    second = acquisition.acquire_jobs(
        (replace(job, job_id="renamed", max_response_bytes=64 * 1024 * 1024),), tmp_path, client
    )
    assert first["provider_calls_this_run"] == 1
    assert second["provider_calls_this_run"] == 0
    assert client.execute_count == 1
    assert len(client.calls) == calls


@pytest.mark.parametrize("condition", ["balance", "schema"])
def test_failed_preflight_never_executes(tmp_path: Path, condition: str) -> None:
    client = FakeQveris()
    if condition == "balance":
        client.balance = Decimal(0)
    else:
        client.failure = "schema"
    with pytest.raises((ValueError, RuntimeError)):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 0
    assert not list(tmp_path.rglob("*.intent.json"))


def test_charged_lost_response_is_not_executed_again(tmp_path: Path) -> None:
    client = FakeQveris()
    client.failure = "timeout"
    for _ in range(2):
        with pytest.raises(RuntimeError, match="RAW_MISSING"):
            acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 1
    assert not list(tmp_path.rglob("complete.json"))
    assert len(list(tmp_path.rglob("*.billing.json"))) == 1


def test_intent_before_execute_crash_remains_ambiguous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeQveris()

    def crash(*_args: object) -> None:
        raise RuntimeError("synthetic process death")

    with monkeypatch.context() as patch:
        patch.setattr(acquisition, "_execute_once", crash)
        with pytest.raises(RuntimeError, match="process death"):
            acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    with pytest.raises(RuntimeError, match="PENDING_SETTLEMENT"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 0


def test_raw_before_billing_crash_recovers_without_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeQveris()

    def crash(*_args: object) -> dict[str, object]:
        raise RuntimeError("synthetic process death")

    with monkeypatch.context() as patch:
        patch.setattr(acquisition, "reconcile_page", crash)
        with pytest.raises(RuntimeError, match="process death"):
            acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 1
    assert len(list(tmp_path.rglob("complete.json"))) == 1


@pytest.mark.parametrize("lag", ["usage_ready", "ledger_ready"])
def test_pending_settlement_blocks_next_paid_job_until_reconciled(tmp_path: Path, lag: str) -> None:
    client = FakeQveris()
    setattr(client, lag, False)
    with pytest.raises(RuntimeError, match="PENDING_SETTLEMENT"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    with pytest.raises(RuntimeError, match="PENDING_SETTLEMENT"):
        acquisition.acquire_jobs((fred_job(),), tmp_path, client)
    assert client.execute_count == 1
    setattr(client, lag, True)
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 1


@pytest.mark.parametrize("failure", ["truncated", "wrong-session", "short-page"])
def test_incomplete_or_unattributed_data_does_not_complete(tmp_path: Path, failure: str) -> None:
    client = FakeQveris()
    client.failure = failure
    job = fred_job() if failure == "short-page" else eod_job()
    with pytest.raises((ValueError, RuntimeError)):
        acquisition.acquire_jobs((job,), tmp_path, client)
    assert client.execute_count == 1
    assert not list(tmp_path.rglob("complete.json"))


def test_fred_paging_retains_missing_value_and_settles_each_call(tmp_path: Path) -> None:
    client = FakeQveris()
    client.price = Decimal(1)
    acquisition.acquire_jobs((fred_job(),), tmp_path, client)
    with QverisStore(tmp_path, client.account_key) as store:
        marker = store.document(f"jobs/{fred_job().fingerprint}/complete.json")
        assert (marker["rows"], marker["pages"], marker["settled_credits"]) == (3, 2, "2")
        assert b'"value":"."' in store.read(f"jobs/{fred_job().fingerprint}/0000.raw")
    assert client.execute_count == marker["pages"]


def test_tampered_raw_is_not_reused(tmp_path: Path) -> None:
    client = FakeQveris()
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    next(tmp_path.rglob("*.raw")).write_bytes(b"tampered")
    before = len(client.calls)
    with pytest.raises(ValueError, match="pin differs"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert len(client.calls) == before


def test_symlink_root_rejected_before_network(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    link = tmp_path / "alias"
    link.symlink_to(actual, target_is_directory=True)
    client = FakeQveris()
    with pytest.raises(RuntimeError, match="symlink"):
        acquisition.acquire_jobs((eod_job(),), link, client)
    assert not client.calls
    assert not list(actual.iterdir())


def test_second_writer_cannot_execute(tmp_path: Path) -> None:
    client = FakeQveris()
    with QverisStore(tmp_path, client.account_key), pytest.raises(BlockingIOError):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert not client.calls


def test_contract_roundtrip_and_unknown_fields_fail() -> None:
    document = {"schema_version": 1, "jobs": [eod_job().document()]}
    assert load_jobs(canonical_json_bytes(document)) == (eod_job(),)
    invalid = eod_job().document()
    invalid["token"] = "synthetic"  # noqa: S105 -- rejected fixture field, not a credential
    with pytest.raises(ValueError, match="fields"):
        QverisJob.from_document(invalid)
    with pytest.raises(ValueError, match="unreviewed"):
        replace(eod_job(), tool_id="unreviewed")


def test_cli_plan_does_not_create_output_or_read_credential(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    jobs = tmp_path / "jobs.json"
    jobs.write_bytes(canonical_json_bytes({"schema_version": 1, "jobs": [eod_job().document()]}))
    output = tmp_path / "not-created"
    assert (
        main(["--jobs", str(jobs), "--output-root", str(output), "--key-file", "/does/not/exist"])
        == 0
    )
    assert not output.exists()
    assert '"provider_calls": 0' in capsys.readouterr().out


def test_over_quote_survives_restart_and_blocks_another_job(tmp_path: Path) -> None:
    client = FakeQveris()
    client.failure = "over-quote"
    with pytest.raises(RuntimeError, match="QUOTE_EXCEEDED"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    client.failure = None
    with pytest.raises(RuntimeError, match="QUOTE_EXCEEDED"):
        acquisition.acquire_jobs((fred_job(),), tmp_path, client)
    assert client.execute_count == 1


def test_prepaid_deposit_is_separate_from_dataset_charge(tmp_path: Path) -> None:
    client = FakeQveris()
    client.failure = "deposit"
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    with QverisStore(tmp_path, client.account_key) as store:
        billing = store.document(f"jobs/{eod_job().fingerprint}/0000.billing.json")
    assert billing["settled_credits"] == "2.81"
    assert Decimal(str(billing["other_account_delta"])) == Decimal(100)


@pytest.mark.parametrize("failure", ["duplicate-usage", "future-vintage", "repeated-page"])
def test_usage_ambiguity_or_invalid_vintage_pages_are_preserved_without_completion(
    tmp_path: Path, failure: str
) -> None:
    client = FakeQveris()
    client.failure = failure
    with pytest.raises((RuntimeError, ValueError)):
        acquisition.acquire_jobs((fred_job(),), tmp_path, client)
    assert not list(tmp_path.rglob("complete.json"))
    assert list(tmp_path.rglob("*.raw"))


def test_free_tool_has_verified_zero_cost(tmp_path: Path) -> None:
    client = FakeQveris()
    client.price = Decimal(0)
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    with QverisStore(tmp_path, client.account_key) as store:
        assert (
            store.document(f"jobs/{eod_job().fingerprint}/0000.billing.json")["settled_credits"]
            == "0"
        )


def test_account_activity_during_preflight_stops_before_intent_and_can_retry(
    tmp_path: Path,
) -> None:
    class ActivityClient(FakeQveris):
        def request(
            self,
            path: str,
            *,
            body: dict[str, object] | None = None,
            query: dict[str, str | int] | None = None,
        ) -> QverisResponse:
            if path == "/auth/credits" and self.calls.count(path) == 1:
                self.balance += 100
                self.ledger.append({"id": "external-deposit", "amount_credits": "100"})
            return super().request(path, body=body, query=query)

    client = ActivityClient()
    with pytest.raises(RuntimeError, match="ACCOUNT_CHANGED"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 0
    assert not list(tmp_path.rglob("*.intent.json"))
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 1


@pytest.mark.parametrize("field", ["files", "rows", "settled_credits"])
def test_completion_claims_are_recomputed_from_all_pinned_pages(tmp_path: Path, field: str) -> None:
    client = FakeQveris()
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    with QverisStore(tmp_path, client.account_key) as store:
        relative = f"jobs/{eod_job().fingerprint}/complete.json"
        marker = store.document(relative)
    marker[field] = {"files": [], "rows": 999999, "settled_credits": "0"}[field]
    (tmp_path / relative).write_bytes(canonical_json_bytes(marker))
    before = len(client.calls)
    with pytest.raises(ValueError, match="Qveris completion"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert len(client.calls) == before


def test_explicit_quarantine_keeps_reservation_and_permits_other_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeQveris()

    def crash(*_args: object) -> None:
        raise RuntimeError("synthetic process death")

    with monkeypatch.context() as patch:
        patch.setattr(acquisition, "_execute_once", crash)
        with pytest.raises(RuntimeError, match="process death"):
            acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    page = f"jobs/{eod_job().fingerprint}/0000"
    result = acquisition.quarantine_pending(
        tmp_path, page, "operator investigates dead intent", client
    )
    assert result["status"] == "SETTLEMENT_UNKNOWN"
    with QverisStore(tmp_path, client.account_key) as store:
        assert store.reserved_credits() == client.price
        assert store.exists(f"{page}.intent.json")
    acquisition.acquire_jobs((fred_job(),), tmp_path, client)
    before = client.execute_count
    with pytest.raises(RuntimeError, match="QUARANTINED"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == before


def test_quarantine_does_not_spend_reserved_credits(tmp_path: Path) -> None:
    client = FakeQveris()
    client.balance = Decimal(6)
    client.failure = "timeout"
    client.usage_ready = False
    with pytest.raises(RuntimeError, match="PENDING_SETTLEMENT"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    page = f"jobs/{eod_job().fingerprint}/0000"
    acquisition.quarantine_pending(tmp_path, page, "operator isolates uncertain response", client)
    client.failure = None
    with pytest.raises(RuntimeError, match="INSUFFICIENT_CREDITS"):
        acquisition.acquire_jobs((fred_job(),), tmp_path, client)
    assert client.execute_count == 1
    client.usage_ready = True
    acquisition.reconcile_pending(tmp_path, client)
    with QverisStore(tmp_path, client.account_key) as store:
        assert store.reserved_credits() == 0


def test_reconcile_refuses_ignored_jobs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--reconcile", "--jobs", "/irrelevant", "--output-root", str(tmp_path)]) == 2  # noqa: PLR2004 -- CLI precondition exit
    assert "does not accept --jobs" in capsys.readouterr().err


def test_lowered_byte_limit_rejects_large_cache_without_http(tmp_path: Path) -> None:
    class LargeClient(FakeQveris):
        def _execute(
            self, body: dict[str, object], query: dict[str, str | int]
        ) -> dict[str, object]:
            return {**super()._execute(body, query), "padding": "x" * 2048}

    client = LargeClient()
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    before = len(client.calls)
    with pytest.raises(ValueError, match="cached response exceeds"):
        acquisition.acquire_jobs((replace(eod_job(), max_response_bytes=1024),), tmp_path, client)
    assert len(client.calls) == before


def test_zero_included_charge_preserves_gateway_warning_without_reexecution(tmp_path: Path) -> None:
    client = FakeQveris()
    client.price = Decimal(0)
    client.failure = "included"
    result = acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert result["status"] == "RAW_ACQUIRED_WITH_WARNINGS"
    with QverisStore(tmp_path, client.account_key) as store:
        marker = store.document(f"jobs/{eod_job().fingerprint}/complete.json")
        assert marker["provider_completeness_verified"] is False
        assert marker["settled_credits"] == "0"
    before = len(client.calls)
    acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert len(client.calls) == before


def test_included_with_nonzero_amount_cannot_settle(tmp_path: Path) -> None:
    client = FakeQveris()
    client.failure = "included"
    with pytest.raises(RuntimeError, match="included charge requires"):
        acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert not list(tmp_path.rglob("complete.json"))


def test_failed_not_charged_releases_reservation_but_never_marks_data_complete(
    tmp_path: Path,
) -> None:
    client = FakeQveris()
    client.failure = "failed-not-charged"
    for _ in range(2):
        with pytest.raises(ValueError, match="did not succeed"):
            acquisition.acquire_jobs((eod_job(),), tmp_path, client)
    assert client.execute_count == 1
    with QverisStore(tmp_path, client.account_key) as store:
        billing = store.document(f"jobs/{eod_job().fingerprint}/0000.billing.json")
        assert billing["settled_credits"] == "0"
        assert billing["quantity"] is None
        assert not store.pending_pages()
    client.failure = None
    acquisition.acquire_jobs((fred_job(),), tmp_path, client)
