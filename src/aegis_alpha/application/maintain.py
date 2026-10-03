"""``aas maintain plan|run``: one bounded daily pass from provider answers to promoted heads.

A run opens the installation writable once and keeps its storage locks for the whole pass,
so maintenance is the single market writer while it runs; another command (or a second
run) is refused with ``installation_busy``. The stages, in order:

1. ``recover``: finish or end interrupted operations (promotions, source imports) from
   their retained evidence, without any provider call;
2. ``runtime``: the running interpreter and package beside the install receipt;
3. ``calendars``: every packaged declaration refreshed as the next session generation;
4. ``collect``: when ``jobs.enabled``, each enabled provider under its per-run caps: KIND
   lists, the OpenDART rolling cohort, SEC EDGAR, FRED/ALFRED, then Qveris (paid credits,
   checked against the account balance before every page);
5. ``import``: completed Qveris jobs and KR symbol lists as content sources;
6. ``identity``: new KR listings registered and the maintenance identity snapshot pinned;
7. ``promote``: every catalog dataset chain continued with its new sources;
8. ``report``: dataset heads with their watermarks, kept in ``raw/`` and as
   ``<runtime>/maintain-report.json``.

A failing stage (a database error included) is recorded and the later stages still run
on what is committed; a provider refusal or failure never stops the other providers. When
the identity stage fails, chains that resolve identities wait for the next run instead of
resolving new listings against an older snapshot. Each collector settles the
attempts an interrupted run left before it plans, so a killed run is recovered by the next
one without asking any answered request again. ``plan`` opens the installation read-only
and reports what each stage would do; it reads no credential and calls no provider.

The exit code is 0 when every stage completed (a spent budget is a completion), 1 when a
stage failed or a provider stopped on a refusal, and 2 when Qveris stopped on an unsettled
paid call that needs settlement or quarantine.
"""

from __future__ import annotations

# ruff: noqa: PLC0415 -- provider clients, Arrow and stores load with the stage that uses them.
import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from aegis_alpha.application.maintain_config import MaintainConfig, QverisSettings
    from aegis_alpha.compute_resources import ComputeBudget
    from aegis_alpha.data.fred_collect import FredClient, FredPolicy
    from aegis_alpha.data.opendart import OpenDartClient, Transport
    from aegis_alpha.data.opendart_cohort import CohortPolicy
    from aegis_alpha.data.qveris_billing import QverisPort
    from aegis_alpha.data.sec_collect import SecClient
    from aegis_alpha.storage.workspace import Workspace

REPORT_SCHEMA: Final = "aas-maintain-report-v1"
REPORT_NAME: Final = "maintain-report.json"
_LOCAL_ERRORS: Final = (ValueError, TypeError, OSError, RuntimeError, KeyError)

type Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class Ports:
    """Provider clients, each built only when the run asks that provider."""

    kind: Callable[[], Transport] | None = None
    dart: Callable[[], OpenDartClient] | None = None
    sec: Callable[[], SecClient] | None = None
    fred: Callable[[], FredClient] | None = None
    qveris: Callable[[], QverisPort] | None = None


@dataclass(frozen=True, slots=True)
class Policies:
    """Collector policies; the defaults are the collectors' own."""

    dart: CohortPolicy | None = None
    sec_lookback_days: int | None = None
    fred: FredPolicy | None = None
    sleep: Callable[[float], None] = time.sleep


def config_ports(config: MaintainConfig) -> Ports:
    """Production clients from the configured credential files, read on first use."""
    from aegis_alpha.application.data_config import read_secret
    from aegis_alpha.data import fred_collect, sec_collect
    from aegis_alpha.data.opendart import OpenDartClient, urllib_transport

    dart, sec, fred, qveris = config.dart, config.sec, config.fred, config.qveris

    def qveris_client() -> QverisPort:
        from aegis_alpha.data.qveris_client import QverisClient

        settings = cast("QverisSettings", qveris)  # built only for a configured provider
        return QverisClient(settings.key_file.absolute(), timeout_seconds=settings.timeout_seconds)

    return Ports(
        kind=urllib_transport if config.kind else None,
        dart=None
        if dart is None
        else lambda: OpenDartClient(read_secret(dart.key_file.absolute()), urllib_transport()),
        sec=None
        if sec is None
        else lambda: sec_collect.SecClient(
            read_secret(sec.user_agent_file.absolute()),
            urllib_transport(max_bytes=sec_collect.MAX_RESPONSE_BYTES),
        ),
        fred=None
        if fred is None
        else lambda: fred_collect.FredClient(
            read_secret(fred.key_file.absolute()),
            urllib_transport(max_bytes=fred_collect.MAX_RESPONSE_BYTES),
        ),
        qveris=None if qveris is None else qveris_client,
    )


def _us(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


@dataclass(slots=True)
class _Run:
    clock: Clock
    stages: dict[str, object] = field(default_factory=dict)
    failed: list[str] = field(default_factory=list)
    stopped: list[str] = field(default_factory=list)
    uncertain: list[str] = field(default_factory=list)

    def local(self, name: str, action: Callable[[], dict[str, object]]) -> dict[str, object] | None:
        """A stage that reads or writes only the installation: errors keep their message.

        Database errors (DuckDB, SQLite, out of memory among them) are local errors too.
        """
        import sqlite3

        import duckdb

        try:
            result = action()
        except (*_LOCAL_ERRORS, duckdb.Error, sqlite3.Error) as error:
            self.stages[name] = {"status": "failed", "error_type": type(error).__name__,
                                 "error": str(error)}  # fmt: skip
            self.failed.append(name)
            return None
        self.stages[name] = result
        return result

    def provider(self, name: str, action: Callable[[], dict[str, object]]) -> None:
        """A provider stage: a failure is recorded by type only, never with its message."""
        try:
            result = action()
        except Exception as error:  # noqa: BLE001 -- keep other providers; no credential-bearing text
            self.stages[name] = {
                "status": "failed",
                "error_type": type(error).__name__,
                "message": "provider stage failed; inspect the ledger and retained receipts",
            }
            self.failed.append(name)
            return
        self.stages[name] = result


def _calendars(
    workspace: Workspace, *, apply: bool, now_us: int, budget: ComputeBudget | None
) -> dict[str, object]:
    from aegis_alpha.storage.calendar_declaration import (
        PACKAGED,
        packaged_declaration,
        parse_declaration,
    )
    from aegis_alpha.storage.calendar_refresh import refresh_calendar

    results: list[dict[str, object]] = []
    for name in PACKAGED:
        raw = packaged_declaration(name)
        declaration = parse_declaration(raw, hashlib.sha256(raw).hexdigest())
        if declaration.declared_at_us > now_us:
            # A clock behind the package's declaration: nothing is refreshed, nothing fails.
            results.append({"calendar_id": name, "status": "declared_after_now"})
            continue
        result = refresh_calendar(
            workspace, raw, hashlib.sha256(raw).hexdigest(), apply=apply, now_us=now_us,
            budget=budget,
        )  # fmt: skip
        promotion = cast("dict[str, object] | None", result.get("promotion"))
        results.append(
            {
                "calendar_id": result["calendar_id"],
                "dataset_id": result["dataset_id"],
                "coverage": result["coverage"],
                "source_id": result["source_id"],
                "head": result["head"],
                "published": None if promotion is None else promotion.get("published"),
                "operations": None if promotion is None else promotion.get("operations"),
                "generation_id": None if promotion is None else promotion.get("generation_id"),
            }
        )
    renewal = [
        item["calendar_id"]
        for item in results
        if "coverage" in item
        and not cast("dict[str, object]", item["coverage"])["covers_next_year"]
    ]
    return {"calendars": results, "renewal_due": renewal}


def _stop_code(result: dict[str, object]) -> bool:
    return result.get("stopped") not in {None, "budget"}


def _collect(  # noqa: C901 -- one stage per provider, in order
    run: _Run,
    workspace: Workspace,
    config: MaintainConfig,
    ports: Ports,
    policies: Policies,
) -> None:
    from aegis_alpha.data import sec_collect

    if not config.jobs_enabled:
        run.stages["collect"] = {"status": "jobs_disabled", "provider_calls": 0}
        return
    clock, sleep = run.clock, policies.sleep
    if config.kind and ports.kind is not None:
        kind = ports.kind

        def collect_kind() -> dict[str, object]:
            from aegis_alpha.storage.kr_collection import collect_kind as collect

            result = collect(workspace, kind(), clock=clock)
            lists = cast("list[dict[str, object]]", result["lists"])
            if any(item.get("status") != "committed" for item in lists):
                run.stopped.append("kind")
            return result

        run.provider("kind", collect_kind)
    if config.dart is not None and ports.dart is not None:
        dart, client = config.dart, ports.dart

        def collect_dart() -> dict[str, object]:
            from aegis_alpha.storage.kr_collection import collect_dart as collect

            result = collect(workspace, client(), policy=policies.dart, clock=clock, sleep=sleep,
                             max_calls=dart.max_calls, daily_quota=dart.daily_quota)  # fmt: skip
            if _stop_code(result):
                run.stopped.append("dart")
            return result

        run.provider("dart", collect_dart)
    if config.sec is not None and ports.sec is not None:
        sec, sec_client = config.sec, ports.sec

        def collect_sec() -> dict[str, object]:
            from aegis_alpha.storage.sec_collection import collect_sec as collect

            extra = {} if policies.sec_lookback_days is None else {
                "lookback_days": policies.sec_lookback_days}  # fmt: skip
            policy = sec_collect.SecPolicy(issuers=sec.issuers, **extra)  # ty: ignore[invalid-argument-type]
            result = collect(workspace, sec_client(), policy=policy, clock=clock, since=sec.since,
                             sleep=sleep, max_calls=sec.max_calls)  # fmt: skip
            if _stop_code(result):
                run.stopped.append("sec")
            return result

        run.provider("sec", collect_sec)
    if config.fred is not None and ports.fred is not None:
        fred, fred_client = config.fred, ports.fred

        def collect_fred() -> dict[str, object]:
            from aegis_alpha.storage.fred_collection import collect_fred as collect

            result = collect(workspace, fred_client(), policy=policies.fred, clock=clock,
                             sleep=sleep, max_calls=fred.max_calls)  # fmt: skip
            if _stop_code(result):
                run.stopped.append("fred")
            return result

        run.provider("fred", collect_fred)
    if config.qveris is not None and ports.qveris is not None:
        qveris, port = config.qveris, ports.qveris

        def collect_qveris() -> dict[str, object]:
            from aegis_alpha.application.maintain_qveris import collect

            result = collect(workspace, qveris.raw_root, qveris.policy, qveris.caps, port,
                             today=clock().astimezone(UTC).date(), clock=clock)  # fmt: skip
            if result["status"] == "stopped":
                run.uncertain.append("qveris")
            elif result["status"] == "partial":
                run.stopped.append("qveris")
            return result

        run.provider("qveris", collect_qveris)


def _import(workspace: Workspace, config: MaintainConfig) -> dict[str, object]:
    from aegis_alpha.application.maintain_qveris import import_completed

    qveris = config.qveris
    if qveris is None:
        return {"status": "not_configured"}
    result = import_completed(workspace, qveris.raw_root, qveris.identity)
    if result["failed"] or result["missing"]:
        unreadable = len(cast("list[object]", result["missing"]))
        raise ValueError(
            f"{result['failed']} Qveris completions failed to import; {unreadable} unreadable"
        )
    return result


def _write_report(workspace: Workspace, report: dict[str, object]) -> str:
    from aegis_alpha.data.descriptor_tree import DescriptorTree
    from aegis_alpha.storage.locks import private_directory
    from aegis_alpha.storage.raw import put_raw

    raw = canonical_json_bytes(report)
    _, digest, _ = put_raw(workspace.paths.raw, raw)
    private_directory(workspace.paths.runtime, create=True)
    with DescriptorTree.open_path(workspace.paths.runtime) as tree:
        tree.atomic_write_bytes(REPORT_NAME, raw)
    return digest


def run_maintenance(  # noqa: PLR0913 -- the run's explicit inputs
    workspace: Workspace,
    config: MaintainConfig,
    ports: Ports,
    *,
    clock: Clock,
    policies: Policies | None = None,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """One bounded maintenance pass over a writable workspace; see the module docstring."""
    from aegis_alpha.application.install_receipt import runtime_report
    from aegis_alpha.storage import maintain_identity, maintain_promotion
    from aegis_alpha.storage.publication import recover_operations

    policies = policies or Policies()
    run = _Run(clock)
    started = clock()
    run.local("recover", lambda: recover_operations(workspace, budget=budget))
    run.local("runtime", lambda: runtime_report(workspace.paths))
    run.local("calendars", lambda: _calendars(workspace, apply=True, now_us=_us(clock()),
                                              budget=budget))  # fmt: skip
    _collect(run, workspace, config, ports, policies)
    run.local("import", lambda: _import(workspace, config))
    identity = run.local(
        "identity", lambda: maintain_identity.advance(workspace, apply=True, now_us=_us(clock()))
    )
    pin = None if identity is None else identity.get("snapshot")
    promoted = run.local(
        "promote",
        lambda: maintain_promotion.promote_datasets(
            workspace,
            apply=True,
            identity=cast("dict[str, str] | None", pin),
            identity_failed=identity is None,
            budget=budget,
            now_us=_us(clock()),
        ),
    )
    if promoted is not None and promoted["refused"]:
        run.failed.append("promote")
    run.local("heads", lambda: {"datasets": maintain_promotion.dataset_heads(workspace)})
    code = 2 if run.uncertain else 1 if run.failed or run.stopped else 0
    report: dict[str, object] = {
        "schema": REPORT_SCHEMA,
        "mode": "run",
        "started_at_utc": started.isoformat(),
        "finished_at_utc": clock().isoformat(),
        "installation_id": workspace.installation_id,
        "status": "succeeded" if code == 0 else "stopped" if code == 2 else "partial",  # noqa: PLR2004 -- exit code
        "failed_stages": run.failed,
        "stopped_providers": [*run.stopped, *run.uncertain],
        "stages": run.stages,
        "exit_code": code,
    }
    report["report_sha256"] = _write_report(workspace, report)
    return report


def _plan_providers(
    workspace: Workspace, config: MaintainConfig, policies: Policies, now: datetime
) -> dict[str, object]:
    from aegis_alpha.application.maintain_qveris import plan_requests
    from aegis_alpha.data import fred_collect, sec_collect
    from aegis_alpha.data.opendart_cohort import CohortPolicy, seoul_day
    from aegis_alpha.storage.fred_collection import plan_fred
    from aegis_alpha.storage.kr_collection import plan_dart
    from aegis_alpha.storage.sec_collection import plan_sec

    plans: dict[str, object] = {"jobs_enabled": config.jobs_enabled, "enabled": config.enabled()}
    if config.kind:
        plans["kind"] = {"lists": ["kind-kospi", "kind-kosdaq"]}
    if config.dart is not None:
        plans["dart"] = plan_dart(workspace, today=seoul_day(now),
                                  policy=policies.dart or CohortPolicy())  # fmt: skip
    if config.sec is not None:
        policy = sec_collect.SecPolicy(issuers=config.sec.issuers)
        plans["sec"] = plan_sec(workspace, now=now, policy=policy, since=config.sec.since)
    if config.fred is not None:
        plans["fred"] = plan_fred(workspace, today=fred_collect.fred_day(now),
                                  policy=policies.fred or fred_collect.FredPolicy())  # fmt: skip
    if config.qveris is not None:
        qveris = config.qveris
        planned = plan_requests(qveris.raw_root, qveris.policy, today=now.astimezone(UTC).date())
        plans["qveris"] = {
            **planned.report(),
            "jobs": [
                {"job_id": item.job.job_id, "fingerprint": item.job.fingerprint,
                 "reason": item.reason}
                for item in planned.planned
            ],
            "max_calls": qveris.caps.max_calls,
            "max_credits": qveris.caps.max_credits,
        }  # fmt: skip
    return plans


def plan_maintenance(  # noqa: PLR0913 -- the plan's explicit inputs
    workspace: Workspace,
    config: MaintainConfig,
    *,
    now: datetime,
    policies: Policies | None = None,
    promotions: bool = False,
    budget: ComputeBudget | None = None,
) -> dict[str, object]:
    """What a run at ``now`` would do, from a read-only workspace; no key, no call."""
    from aegis_alpha.application.install_receipt import runtime_report
    from aegis_alpha.storage import maintain_identity, maintain_promotion

    policies = policies or Policies()
    run = _Run(lambda: now)
    run.local("runtime", lambda: runtime_report(workspace.paths))
    run.local("calendars", lambda: _calendars(workspace, apply=False, now_us=_us(now),
                                              budget=budget))  # fmt: skip
    run.local("collect", lambda: _plan_providers(workspace, config, policies, now))
    identity = run.local(
        "identity", lambda: maintain_identity.advance(workspace, apply=False, now_us=_us(now))
    )
    snapshot = None if identity is None else identity.get("snapshot")
    run.local(
        "promote",
        lambda: maintain_promotion.promote_datasets(
            workspace,
            apply=False,
            identity=cast("dict[str, str] | None", snapshot),
            identity_failed=identity is None,
            budget=budget,
            now_us=_us(now),
            evaluate=promotions,
        ),
    )
    run.local("heads", lambda: {"datasets": maintain_promotion.dataset_heads(workspace)})
    return {
        "schema": REPORT_SCHEMA,
        "mode": "plan",
        "planned_at_utc": now.isoformat(),
        "failed_stages": run.failed,
        "stages": run.stages,
        "provider_calls": 0,
        "exit_code": 1 if run.failed else 0,
    }
