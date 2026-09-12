"""Exact synthetic wire amounts exercise decoding, admission, and durable billing."""

from __future__ import annotations

import hashlib
import re
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from aegis_alpha.data.qveris import InvocationBudget
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_billing import account_credits
from aegis_alpha.data.qveris_client import QverisClient, QverisDocumentError, QverisResponse
from aegis_alpha.data.qveris_contracts import credit_value, object_value
from aegis_alpha.data.qveris_store import QverisStore
from tests.data.test_qveris_acquisition import NOW, FakeQveris, eod_job
from tests.data.test_qveris_client import FakeOpener, FakeResponse


class WireCredits(FakeQveris):
    """Keep the stateful fake's accounting, but emit numeric monetary JSON tokens."""

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        response = super().request(path, body=body, query=query)
        raw = re.sub(
            rb'("(?:estimate_credits|remaining_credits|actual_amount_credits|'
            rb'settled_amount_credits|amount_credits)":)"([^"]+)"',
            rb"\1\2",
            response.body,
        )
        return replace(response, body=raw)


def test_admission_refuses_execution_when_wire_quote_exceeds_cap(tmp_path: Path) -> None:
    # Given an amount whose binary float representation equals the smaller cap.
    client = WireCredits()
    client.price = Decimal("0.10000000000000001")
    budget = InvocationBudget(1, Decimal("0.1"))
    # When the real acquisition path decodes and reserves the wire quote.
    with pytest.raises(RuntimeError, match="INVOCATION_CREDIT_LIMIT"):
        acquire_jobs((eod_job(),), tmp_path, client, budget=budget)
    # Then no paid request, intent, or reservation exists, and the audit stays exact.
    assert client.execute_count == 0
    assert "/tools/execute" not in client.calls
    assert (budget.reserved_calls, budget.reserved_credits) == (0, Decimal(0))
    assert not list(tmp_path.glob("jobs/*/*.intent.json"))
    with QverisStore(tmp_path, client.account_key) as store:
        audits = [store.document(f"audits/{p.name}") for p in (tmp_path / "audits").iterdir()]
    (probe,) = [audit for audit in audits if audit["path"] == "/tools/probe"]
    quote = object_value(object_value(probe["response"])["quote"])
    assert credit_value(quote["estimate_credits"]) == Decimal("0.10000000000000001")


@pytest.mark.parametrize("price", ["0.10000000000000001", "0.1", "2", "0"])
def test_settlement_is_exact_when_wire_amounts_are_numeric(tmp_path: Path, price: str) -> None:
    # Given stateful wire responses for quote, balance, usage, and ledger.
    client = WireCredits()
    client.price = Decimal(price)
    budget = InvocationBudget(1, Decimal(price))
    # When one admitted job executes and reconciles through the real adapter.
    result = acquire_jobs((eod_job(),), tmp_path, client, budget=budget)
    # Then every consumed monetary value survives durable serialization.
    assert result["provider_calls_this_run"] == 1
    assert client.execute_count == 1
    with QverisStore(tmp_path, client.account_key) as store:
        page = f"jobs/{eod_job().fingerprint}/0000"
        intent = store.document(f"{page}.intent.json")
        billing = store.document(f"{page}.billing.json")
        response = store.document(f"{page}.response.json")
        raw = store.read(f"{page}.raw")
    expected = Decimal(price)
    assert budget.reserved_credits == expected
    assert credit_value(intent["quoted_credits"]) == expected
    assert credit_value(billing["settled_credits"]) == expected
    assert credit_value(billing["credits_after"]) == Decimal(100) - expected
    assert Decimal(str(billing["account_ledger_delta"])) == -expected
    assert Decimal(str(billing["other_account_delta"])) == 0
    usage = object_value(billing["usage"])
    assert credit_value(usage["actual_amount_credits"]) == expected
    assert credit_value(usage["settled_amount_credits"]) == expected
    assert billing["over_quote"] is False
    assert object_value(response["raw"])["sha256"] == hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        (b"0.10000000000000001", "0.10000000000000001"),
        (b"1.0000000000000001e-1", "0.10000000000000001"),
        (b"1e-400", "1e-400"),
        (b"1e400", "1e400"),
        (b'"0.10000000000000001"', "0.10000000000000001"),
        (b"7", "7"),
    ],
)
def test_balance_preserves_amount_when_http_body_is_decoded(
    tmp_path: Path, token: bytes, expected: str
) -> None:
    # Given raw HTTP bytes, not a pre-decoded monetary mock.
    raw = b'{"status":"success","data":{"remaining_credits":' + token + b"}}"
    opener = FakeOpener([FakeResponse(body=raw)])
    key = tmp_path / "key"
    key.write_bytes(b"synthetic-exact-credit-key")
    key.chmod(0o600)
    client = QverisClient(key, opener=opener, clock=lambda: NOW)
    with QverisStore(tmp_path / "evidence", client.account_key) as store:
        # When the real client and balance adapter decode and audit the body.
        balance = account_credits(client, store)
        # Then both the returned balance and reopened JSON audit remain exact.
        assert balance == Decimal(expected)
        (audit,) = (store.root / "audits").iterdir()
        saved = store.document(f"audits/{audit.name}")
        data = object_value(object_value(saved["response"])["data"])
        assert credit_value(data["remaining_credits"]) == Decimal(expected)
    assert len(opener.calls) == 1


@pytest.mark.parametrize(
    "token",
    [b'"invalid"', b'"NaN"', b'"Infinity"', b'"-Infinity"', b"-0.1", b"true", b"null"],
)
def test_admission_rejects_invalid_amount_when_quote_is_decoded(
    tmp_path: Path, token: bytes
) -> None:
    # Given an invalid numeric or string credit value on the wire.
    class InvalidQuote(WireCredits):
        def request(
            self,
            path: str,
            *,
            body: dict[str, object] | None = None,
            query: dict[str, str | int] | None = None,
        ) -> QverisResponse:
            response = super().request(path, body=body, query=query)
            return replace(
                response,
                body=response.body.replace(
                    b'"estimate_credits":2.81', b'"estimate_credits":' + token
                ),
            )

    client = InvalidQuote()
    # When admission consumes the raw quote through its normal decoder.
    with pytest.raises((TypeError, ValueError)):
        acquire_jobs((eod_job(),), tmp_path, client, budget=InvocationBudget(1, Decimal(1)))
    # Then malformed credits cannot authorize an intent or paid execution.
    assert client.execute_count == 0
    assert not list(tmp_path.glob("jobs/*/*.intent.json"))


@pytest.mark.parametrize(
    "token", [b"NaN", b"Infinity", b"-Infinity", b"0.1.2", b"1e", b'0.1,"x":1,"x":2']
)
def test_audit_retains_hash_when_numeric_json_is_malformed(tmp_path: Path, token: bytes) -> None:
    # Given malformed/nonfinite JSON delivered by the real HTTP client seam.
    raw = b'{"status":"success","data":{"remaining_credits":' + token + b"}}"
    key = tmp_path / "key"
    key.write_bytes(b"synthetic-exact-credit-key")
    key.chmod(0o600)
    opener = FakeOpener([FakeResponse(body=raw)])
    client = QverisClient(key, opener=opener, clock=lambda: NOW)
    with QverisStore(tmp_path / "evidence", client.account_key) as store:
        # When the balance adapter tries to decode the body.
        with pytest.raises(QverisDocumentError):
            account_credits(client, store)
        # Then durable failure evidence binds the unchanged wire bytes.
        (audit,) = (store.root / "audits").iterdir()
        saved = store.document(f"audits/{audit.name}")
        assert saved["response"] is None
        assert saved["body_sha256"] == hashlib.sha256(raw).hexdigest()
        assert saved["body_bytes"] == len(raw)
    assert len(opener.calls) == 1


@pytest.mark.parametrize("field", ["actual_amount_credits", "amount_credits"])
def test_reconciliation_refuses_completion_when_wire_amounts_differ(
    tmp_path: Path, field: str
) -> None:
    # Given a usage or ledger discrepancy smaller than binary float can represent.
    class DiscrepantCredits(WireCredits):
        def request(
            self,
            path: str,
            *,
            body: dict[str, object] | None = None,
            query: dict[str, str | int] | None = None,
        ) -> QverisResponse:
            response = super().request(path, body=body, query=query)
            amount = b"-0.1" if field == "amount_credits" else b"0.1"
            source = b'"' + field.encode() + b'":' + amount
            return replace(
                response, body=response.body.replace(source, source + b"0000000000000001")
            )

    client = DiscrepantCredits()
    client.price = Decimal("0.1")
    # When the real reconciliation adapter compares independently decoded amounts.
    with pytest.raises(RuntimeError, match="PENDING_SETTLEMENT"):
        acquire_jobs((eod_job(),), tmp_path, client)
    # Then the paid attempt is preserved, but inconsistent billing is not certified.
    assert client.execute_count == 1
    assert list(tmp_path.glob("jobs/*/*.raw"))
    assert not list(tmp_path.glob("jobs/*/*.billing.json"))
    assert not list(tmp_path.glob("jobs/*/complete.json"))
