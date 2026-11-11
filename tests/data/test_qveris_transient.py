"""An execute answered 5xx with no execution ID and no usage event: a transient error.

Replayed through the scripted gateway: the page stays unsettled and blocks every later
job by default; ``tolerate`` (``continue_past_transient``) steps past it while its quote
stays reserved. Nothing is quarantined and no provider is called.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aegis_alpha.application.maintain_qveris import raw_asks
from aegis_alpha.data.qveris_acquisition import acquire_jobs
from aegis_alpha.data.qveris_batch import collect_cohort
from aegis_alpha.data.qveris_client import QverisResponse
from aegis_alpha.data.qveris_contracts import load_json, object_value
from tests.data.qveris_support import ScriptedQveris, account_key, bulk_job
from tests.data.test_qveris_acquisition import NOW

# Serial: these tests take the host-wide Qveris account lease (an abstract Unix socket named by
# the account), so every file that takes it runs in one xdist worker.
pytestmark = pytest.mark.xdist_group("qveris-account-lease")

MAINTENANCE = b"<html><title>System Under Maintenance</title>Error type: 50x</html>"


class MaintenanceQveris(ScriptedQveris):
    """Answers execute with the gateway's 502 maintenance page while ``down``."""

    def __init__(self, key: str) -> None:
        super().__init__({}, key)
        self.down = True

    def request(
        self,
        path: str,
        *,
        body: dict[str, object] | None = None,
        query: dict[str, str | int] | None = None,
    ) -> QverisResponse:
        if path == "/tools/execute" and self.down:
            self.calls.append(path)
            return QverisResponse(502, (("content-type", "text/html"),), MAINTENANCE, NOW, NOW)
        return super().request(path, body=body, query=query)


def _down_page(root: Path) -> tuple[MaintenanceQveris, str]:
    client = MaintenanceQveris(account_key(root))
    job = bulk_job("KO", "splits", "2026-08-31")
    with pytest.raises(RuntimeError, match="PENDING_SETTLEMENT"):
        acquire_jobs((job,), root, client)
    client.down = False
    return client, f"jobs/{job.fingerprint}/0000"


def test_a_5xx_page_without_usage_is_a_transient_candidate(tmp_path: Path) -> None:
    _, page = _down_page(tmp_path)
    ((ask,),) = raw_asks(tmp_path).values()
    assert ask.state == "unsettled"
    (transient,) = ask.transient
    assert (transient.page, transient.http_status, transient.candidate) == (page, 502, True)
    assert transient.empty_reads >= 1
    assert not (tmp_path / f"{page}.quarantine.json").exists()


def test_the_page_stops_later_jobs_unless_tolerated_and_stays_reserved(tmp_path: Path) -> None:
    client, page = _down_page(tmp_path)
    later = bulk_job("KO", "dividends", "2026-08-31")
    client.rows = {
        later.parameters_json: [
            {
                "code": "123456",
                "exchange": "KO",
                "date": "2026-08-31",
                "dividend": "150",
                "currency": "KRW",
            }
        ]
    }
    stopped = collect_cohort((later,), tmp_path, client)
    # Default: the current rule, the unsettled page stops the cohort before any execution.
    assert (stopped["stopped"], stopped["completed"]) == ("PENDING_OR_PREFLIGHT_FAILURE", 0)
    assert client.execute_count == 0
    stepped = collect_cohort((later,), tmp_path, client, tolerate=frozenset({page}))
    assert (stepped["stopped"], stepped["completed"]) == (None, 1)
    assert client.execute_count == 1
    intent = object_value(load_json(
        (tmp_path / "jobs" / later.fingerprint / "0000.intent.json").read_bytes()))  # fmt: skip
    # The tolerated page's full quote stays reserved against the balance.
    assert intent["other_reserved_credits"] == str(client.price)
    ((_, ask),) = sorted(
        (ask.job.dataset, ask) for asks in raw_asks(tmp_path).values() for ask in asks
        if ask.state == "unsettled"
    )  # fmt: skip
    assert [item.page for item in ask.transient] == [page]
