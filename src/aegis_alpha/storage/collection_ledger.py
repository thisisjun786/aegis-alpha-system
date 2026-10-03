"""The state collection ledger: jobs, attempts and usage events of provider calls.

A job is one provider request, named by its fingerprint (``idempotency_key``), whatever
day it is asked; each ask is a numbered attempt. Before a call the collector commits a
``reserved`` attempt with a ``reserved`` usage event, so the quota counts the call before
it can happen. The attempt becomes ``started`` immediately before the call, and after it:

- ``succeeded`` with a ``charged`` usage event naming the retained receipt's SHA-256,
  whatever the provider answered (an error answer is still an answer that was retained);
- ``uncertain`` with an ``uncertain`` usage event when no answer was retained (a transport
  failure), since the provider may have counted the call.

``recover`` settles attempts an interrupted run left behind: a ``reserved`` attempt never
reached the provider and is ``failed`` with a ``released`` event; a ``started`` attempt
may have, and is ``uncertain`` with an ``uncertain`` event. Neither becomes a success or
disappears, and nothing is retried here: the planner decides what to ask again.

The quota is the number of attempts reserved in the window and not released. Usage events
are append-only (state forbids their update and delete); attempt rows move forward only.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.storage.state import atomic

if TYPE_CHECKING:
    import sqlite3

LEDGER_FORMAT: Final = "aas-collection-ledger-v1"
_SHA256_LENGTH: Final = 64
_FORWARD: Final = {
    "reserved": frozenset({"started", "failed"}),
    "started": frozenset({"succeeded", "uncertain"}),
}


def _digest(*parts: object) -> str:
    text = json.dumps([LEDGER_FORMAT, *parts], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Job:
    """One provider request as a ledger job."""

    provider: str
    dataset_id: str
    fingerprint: str
    policy_hash: str
    window_start_us: int | None = None
    window_end_us: int | None = None

    def __post_init__(self) -> None:
        for value in (self.fingerprint, self.policy_hash):
            if len(value) != _SHA256_LENGTH or value.strip("0123456789abcdef"):
                raise ValueError("ledger jobs are named by lowercase SHA-256 digests")
        if not self.provider.isidentifier() or not self.dataset_id:
            raise ValueError("ledger jobs name a provider and a dataset")

    @property
    def job_id(self) -> str:
        return f"{self.provider}:{self.fingerprint}"


@dataclass(frozen=True, slots=True)
class Attempt:
    job_id: str
    attempt: int


def _event(connection: sqlite3.Connection, attempt: Attempt, kind: str, receipt: str,
           at_us: int) -> None:  # fmt: skip
    connection.execute(
        "INSERT INTO usage_events(event_id,job_id,attempt,kind,units,receipt_hash,known_at_us) "
        "VALUES(?,?,?,?,?,?,?)",
        (
            _digest("usage", attempt.job_id, attempt.attempt, kind),
            attempt.job_id,
            attempt.attempt,
            kind,
            "1",
            receipt,
            at_us,
        ),
    )


def _move(connection: sqlite3.Connection, attempt: Attempt, status: str, at_us: int) -> None:
    row = connection.execute(
        "SELECT status FROM collection_attempts WHERE job_id=? AND attempt=?",
        (attempt.job_id, attempt.attempt),
    ).fetchone()
    if row is None or status not in _FORWARD.get(str(row[0]), frozenset()):
        raise ValueError(f"ledger attempt {attempt} cannot move to {status}")
    completed = None if status == "started" else at_us
    connection.execute(
        "UPDATE collection_attempts SET status=?,completed_at_us=? WHERE job_id=? AND attempt=?",
        (status, completed, attempt.job_id, attempt.attempt),
    )


def reserve(
    connection: sqlite3.Connection, job: Job, *, at_us: int, receipt_sha256: str | None = None
) -> Attempt:
    """Commit the job (once), its next attempt and a ``reserved`` usage event.

    ``receipt_sha256`` names the evidence the collector writes before the call, when it
    has one (a Qveris intent's job fingerprint); otherwise the event names the attempt.
    """
    if receipt_sha256 is not None and (
        len(receipt_sha256) != _SHA256_LENGTH or receipt_sha256.strip("0123456789abcdef")
    ):
        raise ValueError("a reservation receipt is a lowercase SHA-256 digest")
    with atomic(connection):
        connection.execute(
            "INSERT INTO collection_jobs(job_id,provider,dataset_id,window_start_us,"
            "window_end_us,policy_hash,idempotency_key,status) VALUES(?,?,?,?,?,?,?,'open') "
            "ON CONFLICT(job_id) DO NOTHING",
            (
                job.job_id,
                job.provider,
                job.dataset_id,
                job.window_start_us,
                job.window_end_us,
                job.policy_hash,
                job.job_id,
            ),
        )
        recorded = connection.execute(
            "SELECT provider,dataset_id FROM collection_jobs WHERE job_id=?", (job.job_id,)
        ).fetchone()
        if tuple(recorded) != (job.provider, job.dataset_id):
            raise ValueError(f"ledger job {job.job_id} is recorded for another dataset")
        number = cast(
            "int",
            connection.execute(
                "SELECT coalesce(max(attempt),0)+1 FROM collection_attempts WHERE job_id=?",
                (job.job_id,),
            ).fetchone()[0],
        )
        attempt = Attempt(job.job_id, number)
        connection.execute(
            "INSERT INTO collection_attempts(job_id,attempt,status,started_at_us,"
            "completed_at_us,request_hash) VALUES(?,?,'reserved',?,NULL,?)",
            (job.job_id, number, at_us, job.fingerprint),
        )
        receipt = receipt_sha256 or _digest("reserve", job.job_id, number)
        _event(connection, attempt, "reserved", receipt, at_us)
    return attempt


def start(connection: sqlite3.Connection, attempt: Attempt, *, at_us: int) -> None:
    with atomic(connection):
        _move(connection, attempt, "started", at_us)


def succeed(
    connection: sqlite3.Connection, attempt: Attempt, *, receipt_sha256: str, outcome: str,
    at_us: int,
) -> None:  # fmt: skip
    """The provider answered and the receipt naming that answer is retained."""
    with atomic(connection):
        _move(connection, attempt, "succeeded", at_us)
        _event(connection, attempt, "charged", receipt_sha256, at_us)
        connection.execute(
            "UPDATE collection_jobs SET status=? WHERE job_id=?",
            (outcome.lower(), attempt.job_id),
        )


def release(connection: sqlite3.Connection, attempt: Attempt, *, at_us: int) -> None:
    """A ``reserved`` attempt never reached the provider: ``failed`` with a ``released`` event."""
    with atomic(connection):
        _move(connection, attempt, "failed", at_us)
        _event(connection, attempt, "released", _digest("release", *_key(attempt)), at_us)


def uncertain(connection: sqlite3.Connection, attempt: Attempt, *, at_us: int) -> None:
    """No answer was retained; the provider may still have counted the call."""
    with atomic(connection):
        _move(connection, attempt, "uncertain", at_us)
        _event(connection, attempt, "uncertain", _digest("uncertain", *_key(attempt)), at_us)
        connection.execute(
            "UPDATE collection_jobs SET status='uncertain' WHERE job_id=?", (attempt.job_id,)
        )


def _key(attempt: Attempt) -> tuple[str, int]:
    return attempt.job_id, attempt.attempt


def recover(connection: sqlite3.Connection, provider: str, *, at_us: int) -> dict[str, int]:
    """Settle the provider's attempts an interrupted run left ``reserved`` or ``started``."""
    settled = {"released": 0, "uncertain": 0}
    with atomic(connection):
        rows = connection.execute(
            "SELECT a.job_id,a.attempt,a.status FROM collection_attempts a JOIN collection_jobs j "
            "ON j.job_id=a.job_id WHERE j.provider=? AND a.status IN ('reserved','started') "
            "ORDER BY a.job_id,a.attempt",
            (provider,),
        ).fetchall()
        for job_id, number, status in rows:
            attempt = Attempt(str(job_id), int(number))
            if status == "reserved":
                release(connection, attempt, at_us=at_us)
                settled["released"] += 1
            else:
                uncertain(connection, attempt, at_us=at_us)
                settled["uncertain"] += 1
    return settled


def used(connection: sqlite3.Connection, provider: str, *, since_us: int) -> int:
    """Attempts of the provider reserved at or after ``since_us`` and never released."""
    row = connection.execute(
        "SELECT count(*) FROM usage_events r JOIN collection_jobs j ON j.job_id=r.job_id "
        "WHERE j.provider=? AND r.kind='reserved' AND r.known_at_us>=? AND NOT EXISTS ("
        "SELECT 1 FROM usage_events x WHERE x.job_id=r.job_id AND x.attempt=r.attempt "
        "AND x.kind='released')",
        (provider, since_us),
    ).fetchone()
    return int(row[0])


def unanswered(connection: sqlite3.Connection, provider: str) -> dict[str, int]:
    """Request fingerprint -> latest settle instant of attempts with no retained answer.

    These are ``uncertain`` attempts (a ``failed`` attempt never reached the provider).
    """
    rows = connection.execute(
        "SELECT a.request_hash,max(a.completed_at_us) FROM collection_attempts a "
        "JOIN collection_jobs j ON j.job_id=a.job_id WHERE j.provider=? "
        "AND a.status='uncertain' GROUP BY a.request_hash",
        (provider,),
    ).fetchall()
    return {str(fingerprint): int(at) for fingerprint, at in rows}


def charged_receipts(connection: sqlite3.Connection, provider: str) -> list[str]:
    """SHA-256 of every receipt a succeeded attempt of the provider retained, oldest first."""
    rows = connection.execute(
        "SELECT u.receipt_hash FROM usage_events u JOIN collection_jobs j ON j.job_id=u.job_id "
        "WHERE j.provider=? AND u.kind='charged' ORDER BY u.known_at_us,u.job_id,u.attempt",
        (provider,),
    ).fetchall()
    return [str(row[0]) for row in rows]
