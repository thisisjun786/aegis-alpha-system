"""Daily exchange-bulk Qveris jobs from the declared session calendar; nothing is called.

A request is one exchange, one dataset (``prices``, ``splits`` or ``dividends``) and one
session date of the exchange's declared calendar (``US`` on XNYS, ``KO``/``KQ`` on XKRX)
before the observation date. A request is left out when a completed job of the raw root
already holds it (whatever its observation date or status, so a provider warning
completes it) and is reported ``held`` when an attempt without a completion exists
there: an uncertain or failed attempt is never planned again automatically. A settled,
uncharged empty answer for an exchange day of actions (``empty_result``) is neither: it is
reported under ``empty`` and the request is planned again for any later observation date.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING, Final

from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
from aegis_alpha.data.qveris_contracts import EOD_TOOL, QverisJob, load_json, object_value
from aegis_alpha.data.qveris_payloads import empty_action_day
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.calendar_declaration import (
    Declaration,
    packaged_declaration,
    parse_declaration,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from pathlib import Path

EXCHANGES: Final = {"US": ("US", "XNYS"), "KO": ("KR", "XKRX"), "KQ": ("KR", "XKRX")}
DATASETS: Final = ("prices", "splits", "dividends")
MAX_DAYS: Final = 366
_MAX_DOCUMENT: Final = 1024 * 1024

type Request = tuple[str, str, str, str, str]


def _request(job: QverisJob) -> Request:
    """What a job asks the provider for; the observation date and job ID are not part of it."""
    return (job.tool_id, job.upstream, job.market, job.dataset, job.parameters_json)


@dataclass(frozen=True, slots=True)
class RawAttempts:
    """The requests a raw root completed, and those it attempted without completing.

    ``fingerprints`` are the completed job directories themselves: ``run`` reuses a job
    only under its own fingerprint, which includes the observation date.
    """

    completed: frozenset[Request]
    held: Mapping[Request, tuple[str, ...]]
    fingerprints: frozenset[str] = frozenset()
    empty: Mapping[Request, date] = field(default_factory=dict)
    """Requests answered only with settled empty answers: the latest one's observation date."""


def _settled_empty(tree: DescriptorTree, base: str, job: QverisJob, intents: list[str]) -> bool:
    """Whether a job's one page is Qveris's settled, uncharged empty answer."""
    page = f"{base}/{intents[0].removesuffix('.intent.json')}"
    if len(intents) != 1 or not all(
        tree.exists(f"{page}.{suffix}") for suffix in ("raw", "billing.json")
    ):
        return False
    try:
        billing = object_value(
            load_json(tree.read_bytes(f"{page}.billing.json", max_bytes=_MAX_DOCUMENT))
        )
        response = object_value(load_json(tree.read_bytes(f"{page}.raw", max_bytes=_MAX_DOCUMENT)))
    except (ValueError, TypeError):
        return False  # an unreadable answer is not shown to be empty; the request stays held
    usage = billing.get("usage")
    return isinstance(usage, dict) and empty_action_day(job, response, usage)


def raw_attempts(raw_root: Path | None) -> RawAttempts:
    """Read every job directory of ``raw_root`` (absent root: nothing attempted)."""
    completed: set[Request] = set()
    fingerprints: set[str] = set()
    held: dict[Request, list[str]] = {}
    empty: dict[Request, date] = {}
    if raw_root is None or not raw_root.exists():
        return RawAttempts(frozenset(), {})
    try:
        with DescriptorTree.open_path(raw_root) as tree:
            names = sorted(tree.listdir("jobs")) if tree.exists("jobs") else []
            for name in names:
                base = f"jobs/{name}"
                if tree.exists(f"{base}/complete.json"):
                    body = tree.read_bytes(f"{base}/complete.json", max_bytes=_MAX_DOCUMENT)
                    document = object_value(load_json(body)).get("job")
                    completed.add(_request(QverisJob.from_document(document)))
                    fingerprints.add(name)
                    continue
                intents = sorted(n for n in tree.listdir(base) if n.endswith(".intent.json"))
                if intents:
                    body = tree.read_bytes(f"{base}/{intents[0]}", max_bytes=_MAX_DOCUMENT)
                    job = QverisJob.from_document(object_value(load_json(body)).get("job"))
                    request = _request(job)
                    if _settled_empty(tree, base, job, intents):
                        empty[request] = max(empty.get(request, job.observation_date),
                                             job.observation_date)  # fmt: skip
                    else:
                        held.setdefault(request, []).append(name)
    except (OSError, DescriptorTreeError) as error:
        raise ValueError("cannot read the Qveris raw collection root") from error
    return RawAttempts(
        frozenset(completed),
        {key: tuple(value) for key, value in held.items() if key not in completed},
        frozenset(fingerprints),
        {key: day for key, day in empty.items() if key not in completed and key not in held},
    )


def declaration_for(calendar_id: str, override: Declaration | None = None) -> Declaration:
    if override is not None:
        if override.calendar_id != calendar_id:
            raise ValueError(f"declaration is {override.calendar_id}, not {calendar_id}")
        return override
    raw = packaged_declaration(calendar_id)
    return parse_declaration(raw, hashlib.sha256(raw).hexdigest())


def sessions(declaration: Declaration, start: date, through: date) -> list[date]:
    """Open session dates of ``[start, through]``; the window must lie inside the declaration."""
    if start > through or (through - start).days >= MAX_DAYS:
        raise ValueError(f"the window must be ordered and at most {MAX_DAYS} days")
    if start < declaration.start or through >= declaration.end:
        raise ValueError(
            f"{declaration.calendar_id} is declared for [{declaration.start}, {declaration.end})"
        )
    return [
        item.session_date
        for item in declaration.days()
        if start <= item.session_date <= through and item.status == "open"
    ]


def daily_job(exchange: str, dataset: str, day: date, observation_date: date) -> QverisJob:
    market = EXCHANGES[exchange][0]
    parameters: dict[str, object] = {"date": day.isoformat(), "exchange": exchange, "fmt": "json"}
    if dataset != "prices":
        parameters["type"] = dataset
    return QverisJob(
        f"daily-{exchange}-{dataset}-{day.isoformat()}",
        EOD_TOOL,
        "eodhd",
        market,
        dataset,
        canonical_json_bytes(parameters).decode(),
        observation_date,
    )


def plan_daily_jobs(  # noqa: C901, PLR0913 -- the explicit request window and its evidence
    raw_root: Path | None,
    *,
    exchanges: Sequence[str],
    datasets: Sequence[str],
    start: date,
    through: date,
    observation_date: date,
    declarations: Iterable[Declaration] = (),
) -> dict[str, object]:
    """The jobs document of every unheld, uncompleted request in the window."""
    if not exchanges or not set(exchanges) <= EXCHANGES.keys():
        raise ValueError("exchanges must be among " + ", ".join(EXCHANGES))
    if not datasets or not set(datasets) <= set(DATASETS):
        raise ValueError("datasets must be among " + ", ".join(DATASETS))
    if through >= observation_date:
        raise ValueError("a session is requested only after its date: through < observation")
    overrides = {item.calendar_id: item for item in declarations}
    attempts = raw_attempts(raw_root)
    jobs: list[QverisJob] = []
    covered = 0
    held: list[dict[str, object]] = []
    empty: list[dict[str, object]] = []
    calendars: dict[str, str] = {}
    for exchange in dict.fromkeys(exchanges):
        calendar = EXCHANGES[exchange][1]
        declaration = declaration_for(calendar, overrides.get(calendar))
        calendars[calendar] = declaration.sha256
        for day in sessions(declaration, start, through):
            for dataset in dict.fromkeys(datasets):
                job = daily_job(exchange, dataset, day, observation_date)
                request = _request(job)
                if request in attempts.completed:
                    covered += 1
                elif request in attempts.held:
                    held.append(
                        {"job_id": job.job_id, "attempted_fingerprints": attempts.held[request]}
                    )
                elif request in attempts.empty:
                    # Asked again on a later observation date; the same day's job is the one
                    # already answered.
                    again = attempts.empty[request] < observation_date
                    empty.append({"job_id": job.job_id, "planned": again,
                                  "empty_on": attempts.empty[request].isoformat()})  # fmt: skip
                    if again:
                        jobs.append(job)
                else:
                    jobs.append(job)
    document = {"schema_version": 1, "jobs": [job.document() for job in jobs]}
    return {
        "document": canonical_json_bytes(document) if jobs else None,
        "planned": len(jobs),
        "covered": covered,
        "held": held,
        "empty": empty,
        "calendars": calendars,
        "window": {"from": start.isoformat(), "through": through.isoformat()},
        "observation_date": observation_date.isoformat(),
        "provider_calls": 0,
    }


def default_window(observation_date: date, days: int) -> tuple[date, date]:
    """The ``days`` calendar days that end the day before ``observation_date``."""
    if type(days) is not int or not 1 <= days <= MAX_DAYS:
        raise ValueError(f"lookback days must be from 1 to {MAX_DAYS}")
    through = observation_date - timedelta(days=1)
    return through - timedelta(days=days - 1), through
