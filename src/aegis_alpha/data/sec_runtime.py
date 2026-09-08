"""Durable SEC runtime over the existing metadata and collection registries."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import Engine, text

from aegis_alpha.collection.records import (
    CollectionRun,
    CollectionRunEvent,
    CollectionRunPlan,
    CollectionUsageRecord,
    RunEventType,
    WatermarkAdvance,
)
from aegis_alpha.collection.registry import CollectionRegistry
from aegis_alpha.data.sec_collector import (
    CollectorConfig,
    CollectorError,
    CollectorOutcome,
    PublicationInputs,
    SecCollector,
)
from aegis_alpha.data.sec_disagreement import ComparableFact
from aegis_alpha.data.sec_evidence import (
    file_pin,
    open_directory,
    publish_bytes,
    read_bytes,
)
from aegis_alpha.data.sec_finalization import finish_run, outcome_from_marker, verified_marker
from aegis_alpha.data.sec_identity import AdmissionState, IdentityPort, admit_universe
from aegis_alpha.data.sec_policy import require_live_policy
from aegis_alpha.data.sec_rate_limit import MINIMUM_INTERVAL_SECONDS, RateLimiter
from aegis_alpha.data.sec_registration import register_response, register_run_source
from aegis_alpha.data.sec_runtime_config import (
    FrozenSecIdentity,
    run_config,
    validate_runtime_config,
)
from aegis_alpha.data.sec_transport import (
    CollectorRequest,
    CollectorResponse,
    Transport,
    assert_user_agent_absent,
    validate_user_agent,
)
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.metadata.registry import MetadataRegistry

# One provider-wide session lock across profiles on the configured application DB.
SEC_PROVIDER_LOCK = int.from_bytes(
    hashlib.sha256(b"aas:sec:provider-runtime:v1").digest()[:8], "big", signed=True
)


@contextmanager
def provider_lock(engine: Engine) -> Iterator[None]:
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        locked = connection.scalar(
            text("SELECT pg_try_advisory_lock(:key)"), {"key": SEC_PROVIDER_LOCK}
        )
        if locked is not True:
            raise CollectorError("SEC provider is already running")
        try:
            yield
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": SEC_PROVIDER_LOCK})


def _new_run_directories(config: CollectorConfig) -> None:
    for path in (config.dataset_root, config.raw_store_root):
        with open_directory(path.parent, create=True) as parent:
            parent.mkdir(path.name)
    with open_directory(config.receipt_path.parent, create=True):
        pass


class _DurableSecCollector(SecCollector):
    def __init__(  # noqa: PLR0913 - explicit injected runtime dependencies
        self,
        *,
        registry: CollectionRegistry,
        metadata: MetadataRegistry,
        request_sha256: str,
        run_id: str,
        preflight: Callable[[], None],
        config: CollectorConfig,
        transport: Transport,
        limiter: RateLimiter,
        clock: Callable[[], datetime],
        user_agent: str | None,
    ) -> None:
        super().__init__(config, transport, registry, limiter, clock, user_agent)
        self.registry = registry
        self.metadata = metadata
        self.request_sha256 = request_sha256
        self.run_id = run_id
        self.preflight = preflight
        self.sources: list[dict[str, object]] = []
        self.attempts: list[dict[str, object]] = []
        self.usage: list[CollectionUsageRecord] = []
        self.publication: PublicationInputs | None = None
        self.published: tuple[Path, ...] = ()
        self.started_at = clock()

    def account_unfinished_failure(self, error: Exception) -> None:
        state = self.registry.current_run_state(self.run_id)
        if (
            state is not None
            and not state.terminal
            and not (self.config.dataset_root / "finalization.json").exists()
        ):
            self._record_usage(run_id=self.run_id)
            self._fail(
                run_id=self.run_id,
                error_class=type(error).__name__,
                error_message="SEC collection failed",
            )

    def _register(self, plan: CollectionRunPlan, *, run_id: str) -> None:
        self.registry.register_plan(plan)
        self.registry.start_run(CollectionRun(run_id, plan.plan_id, self.started_at))
        self.registry.append_event(
            CollectionRunEvent(
                run_id, RunEventType.ATTEMPT_STARTED, self.started_at, attempt_number=1
            )
        )

    def _request_preflight(self) -> None:
        self.preflight()

    def _before_transport(self, request: CollectorRequest) -> None:
        path = self.config.dataset_root / "attempts" / f"{self.limiter.calls_attempted:08d}.json"
        payload = canonical_json_bytes(
            {
                "run_id": self.run_id,
                "request_sha256": self.request_sha256,
                "attempt": self.limiter.calls_attempted,
                "source_uri": request.source_uri,
                "request_fingerprint": request.request_fingerprint,
                "admitted_at": self.clock(),
                "disposition": "attempt_admitted_outcome_unknown",
            }
        )
        publish_bytes(path, payload)
        self.attempts.append(file_pin(self.config.dataset_root, path))

    def _capture(self, request: CollectorRequest, response: CollectorResponse) -> str:
        snapshot_id = super()._capture(request, response)
        self.sources.append(
            register_response(
                self.metadata,
                self.config,
                request,
                response,
                snapshot_id=snapshot_id,
                provenance=self._provenance_records[-1],
            )
        )
        return snapshot_id

    def _publish(self, publication: PublicationInputs) -> tuple[Path, ...]:
        self.publication = publication
        self.published = super()._publish(publication)
        return self.published

    def _record_usage(self, *, run_id: str) -> None:
        if not self.usage:
            self.usage = [
                CollectionUsageRecord(
                    run_id, index, metric, Decimal(quantity), unit, recorded_at_utc=self.clock()
                )
                for index, (metric, quantity, unit) in enumerate(
                    self.limiter.ledger().as_usage_records(), 1
                )
            ]
            publish_bytes(self.config.dataset_root / "usage.json", canonical_json_bytes(self.usage))
        for record in self.usage:
            self.registry.record_usage(record)

    def _succeed(self, *, run_id: str) -> None:
        # Success is committed together with watermarks by _advance_watermarks.
        if run_id != self.run_id:
            raise CollectorError("SEC run binding changed")

    def _fail(self, *, run_id: str, error_class: str, error_message: str) -> None:
        del error_message
        if (self.config.dataset_root / "finalization.json").exists():
            raise CollectorError("SEC finalization pending; verified recovery required")
        finished_at = self.clock()
        with self.registry.begin_registration() as connection:
            self.registry.append_event(
                CollectionRunEvent(
                    run_id,
                    RunEventType.ATTEMPT_FAILED,
                    finished_at,
                    attempt_number=1,
                    error_class=error_class,
                    error_message="SEC collection failed",
                ),
                connection=connection,
            )
            self.registry.append_event(
                CollectionRunEvent(
                    run_id,
                    RunEventType.RUN_FAILED,
                    finished_at,
                    error_class=error_class,
                    error_message="SEC collection failed",
                ),
                connection=connection,
            )

    def _advance_watermarks(
        self, *, run_id: str, advances: Sequence[WatermarkAdvance]
    ) -> tuple[tuple[str, str, str], ...]:
        publication = self.publication
        if publication is None:
            raise CollectorError("SEC finalization requires durable outputs")
        finished_at = self.clock()
        marker = {
            "version": 1,
            "run_id": run_id,
            "request_sha256": self.request_sha256,
            "plan_id": publication.plan.plan_id,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "sources": self.sources,
            "outputs": [
                file_pin(self.config.dataset_root, path)
                for path in self.published
                if path != self.config.receipt_path
            ],
            "receipt": file_pin(self.config.receipt_path.parent, self.config.receipt_path),
            "usage": self.usage,
            "watermarks": advances,
            "admissions": publication.admissions,
            "skips": publication.skips,
            "calls_attempted": self.limiter.calls_attempted,
            "disagreement_count": len(publication.disagreements),
            "attempts": [
                *self.attempts,
                file_pin(self.config.dataset_root, self.config.dataset_root / "usage.json"),
            ],
        }
        payload = canonical_json_bytes(marker)
        if self.user_agent is not None:
            assert_user_agent_absent(self.user_agent, payload)
        publish_bytes(self.config.dataset_root / "finalization.json", payload)
        register_run_source(self.metadata, self.config, run_id, payload, finished_at=finished_at)
        verified = verified_marker(
            self.registry, self.config, run_id=run_id, request_sha256=self.request_sha256
        )
        return finish_run(self.registry, self.metadata, self.config, verified)


def _freeze_identity(
    identity: IdentityPort, instrument_ids: Sequence[str] | None, as_of: datetime
) -> FrozenSecIdentity:
    admissions = admit_universe(identity, instrument_ids, as_of)
    if len({item.instrument_id for item in admissions}) != len(admissions):
        raise CollectorError("SEC runtime refuses duplicate instrument IDs")
    if not any(item.state is AdmissionState.ADMITTED for item in admissions):
        raise CollectorError("SEC requires pinned CIK admissions; zero calls")
    return FrozenSecIdentity(admissions, as_of)


def run_sec_collection(  # noqa: PLR0913 - explicit provider runtime and fixture seam
    config: CollectorConfig,
    *,
    engine: Engine,
    identity: IdentityPort,
    transport: Transport,
    limiter: RateLimiter,
    clock: Callable[[], datetime],
    instrument_ids: Sequence[str] | None = None,
    user_agent: str | None = None,
    synthetic: bool = False,
    registry_path: Path | None = None,
    fmp_facts: Sequence[ComparableFact] = (),
    fmp_source_bytes: Mapping[Path, bytes] | None = None,
) -> CollectorOutcome:
    """Execute/recover the exact configured run identity.

    The caller supplies an invocation-specific receipt path. Raw and dataset
    roots gain runs/<run_identity>; live policy is rechecked before transport.
    """
    validate_runtime_config(config, limiter)
    if type(synthetic) is not bool:
        raise CollectorError("SEC synthetic mode must be explicit")

    def preflight() -> None:
        if not synthetic:
            validate_user_agent(user_agent)
            require_live_policy(registry_path)

    preflight()
    frozen_identity = _freeze_identity(identity, instrument_ids, config.as_of)
    admissions = frozen_identity.admissions
    run_id = config.run_identity
    spec = {
        "run_id": run_id,
        "mode": config.mode.value,
        "as_of": config.as_of,
        "max_calls": config.max_calls,
        "normalized_version": config.normalized_version,
        "raw_root": str(config.raw_store_root),
        "dataset_root": str(config.dataset_root),
        "receipt_path": str(config.receipt_path),
        "admissions": admissions,
        "fmp_facts": fmp_facts,
        "fmp_inputs": {
            str(path): hashlib.sha256(body).hexdigest()
            for path, body in (fmp_source_bytes or {}).items()
        },
    }
    request = canonical_json_bytes(spec)
    if user_agent is not None:
        assert_user_agent_absent(user_agent, request)
    request_sha256 = hashlib.sha256(request).hexdigest()
    selected = run_config(config, run_id)
    registry = CollectionRegistry(engine)
    metadata = MetadataRegistry(engine)
    with provider_lock(engine):
        preflight()
        if selected.dataset_root.exists():
            if read_bytes(selected.dataset_root / "request.json") != request:
                raise CollectorError("SEC recovery request differs from the immutable run")
            if not (selected.dataset_root / "finalization.json").exists():
                raise CollectorError(
                    "SEC run has uncertain/failed attempts; automatic HTTP replay refused"
                )
            marker = verified_marker(
                registry, selected, run_id=run_id, request_sha256=request_sha256
            )
            finish_run(registry, metadata, selected, marker)
            return outcome_from_marker(selected, marker.document)
        if selected.receipt_path.exists():
            raise CollectorError("SEC invocation receipt already exists outside verified recovery")
        _new_run_directories(selected)
        publish_bytes(selected.dataset_root / "request.json", request)
        collector = _DurableSecCollector(
            registry=registry,
            metadata=metadata,
            request_sha256=request_sha256,
            run_id=run_id,
            preflight=preflight,
            config=selected,
            transport=transport,
            limiter=limiter,
            clock=clock,
            user_agent=user_agent,
        )
        # Preserve the stricter cap across sequential invocations, not just within one limiter.
        limiter.sleeper(MINIMUM_INTERVAL_SECONDS)
        try:
            return collector.collect(
                identity=frozen_identity,
                run_id=run_id,
                fmp_facts=fmp_facts,
                fmp_source_bytes=fmp_source_bytes,
            )
        except Exception as error:
            collector.account_unfinished_failure(error)
            raise CollectorError(
                "SEC runtime failed; inspect durable run evidence before recovery"
            ) from error
