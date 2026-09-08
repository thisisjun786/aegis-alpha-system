"""AAS-DATA-008C round-3 fixes: boundary authority, secrets, durability.

Every test runs against synthetic fixtures with zero provider calls, zero
credential access, and zero cost.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import urllib.error
from collections.abc import Mapping
from pathlib import Path

import pytest
from finimpulse_collector_support import (
    OBSERVED_AT,
    SYNTHETIC_CREDENTIAL,
    RecordingTransport,
    build_response,
    make_config,
    make_gate_evidence,
    make_identity_export,
    no_sleep,
    snapshot_items,
    synthetic_pair,
)

from aegis_alpha.data.finimpulse_collector import (
    CREDENTIAL_REDACTION,
    AuthorityReferenceError,
    CollectionResult,
    CollectorError,
    ContractError,
    PublicationError,
    RawCall,
    SocketAccessError,
    SyntheticExecution,
    SyntheticTransport,
    TransportFailureError,
    collect_snapshot,
    committed_marker_path,
    make_https_transport,
    publish_lease,
    publish_lease_path,
    read_partition_rows,
    recover_uncommitted_partitions,
    redact_credential,
)

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"


def run(
    tmp_path: Path,
    *,
    transport: RecordingTransport | None = None,
    forge_synthetic: bool = False,
    live_transport: bool = False,
    symbols: tuple[str, ...] = ("AAPL",),
) -> CollectionResult:
    identity = make_identity_export(symbols=symbols)
    offline, capability = synthetic_pair(
        RecordingTransport("baseline") if transport is None else transport
    )
    if forge_synthetic:
        capability = None
    return collect_snapshot(
        snapshot_id="snap-r3-001",
        config=make_config(
            symbols,
            identity_export_sha256=identity.export_sha256,
            identity_as_of=identity.as_of_utc,
        ),
        credential=SYNTHETIC_CREDENTIAL,
        transport=make_https_transport() if live_transport else offline,
        raw_store_root=tmp_path / "raw",
        dataset_root=tmp_path / "dataset",
        identity_export=identity,
        gate_evidence=make_gate_evidence(tmp_path / "gates", symbols=symbols),
        observed_at=OBSERVED_AT,
        sleeper=no_sleep,
        synthetic=capability,
    )


# AAS008C-R3-F1 — authority enforced at the collector boundary


def test_direct_api_use_without_the_synthetic_capability_makes_zero_calls(
    tmp_path: Path,
) -> None:
    """Bypassing the CLI cannot bypass the empty owner authority sets."""

    transport = RecordingTransport("baseline")

    with pytest.raises(
        (AuthorityReferenceError, CollectorError),
        match=r"no owner-authorized|standing authority is required",
    ):
        run(tmp_path, transport=transport, forge_synthetic=True)

    assert transport.calls == []
    assert not (tmp_path / "raw").exists()
    assert not list(tmp_path.rglob("*.parquet"))


def test_synthetic_capability_cannot_be_constructed_directly() -> None:
    """A production caller cannot forge the capability object."""

    offline = SyntheticTransport(RecordingTransport("baseline"))

    with pytest.raises(AuthorityReferenceError, match="cannot be constructed directly"):
        SyntheticExecution(reason="forged", transport=offline, _token=object())

    with pytest.raises(ValueError, match="stated reason"):
        offline.capability("   ")


def test_synthetic_transport_is_sealed_against_override_bypasses() -> None:
    """A subclass cannot replace the socket-blocked call implementation."""

    def mutate(target: object, name: str, value: object) -> None:
        setattr(target, name, value)

    with pytest.raises(TypeError, match="sealed and cannot be subclassed"):
        type("OverridingTransport", (SyntheticTransport,), {})

    offline = SyntheticTransport(RecordingTransport("baseline"))
    with pytest.raises(TypeError, match="instance is immutable"):
        mutate(offline, "_responder", RecordingTransport("baseline"))
    with pytest.raises(TypeError, match="class is sealed"):
        mutate(SyntheticTransport, "__call__", lambda *_args: None)


def test_forged_capability_with_live_transport_makes_zero_calls(tmp_path: Path) -> None:
    """A capability issued elsewhere cannot admit the live HTTPS transport."""

    elsewhere = SyntheticTransport(RecordingTransport("baseline"))
    stolen = elsewhere.capability("issued for a different transport")
    identity = make_identity_export(symbols=("AAPL",))

    with pytest.raises(AuthorityReferenceError, match="offline SyntheticTransport"):
        collect_snapshot(
            snapshot_id="snap-forge-001",
            config=make_config(
                ("AAPL",),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=make_https_transport(),
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates", symbols=("AAPL",)),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=stolen,
        )

    assert not (tmp_path / "raw").exists()
    assert not list(tmp_path.rglob("*.parquet"))


def test_capability_bound_to_another_offline_transport_is_rejected(tmp_path: Path) -> None:
    other = SyntheticTransport(RecordingTransport("baseline"))
    stolen = other.capability("issued for a different transport")
    used = SyntheticTransport(RecordingTransport("baseline"))
    identity = make_identity_export(symbols=("AAPL",))

    with pytest.raises(AuthorityReferenceError, match="issued for a different transport"):
        collect_snapshot(
            snapshot_id="snap-forge-002",
            config=make_config(
                ("AAPL",),
                identity_export_sha256=identity.export_sha256,
                identity_as_of=identity.as_of_utc,
            ),
            credential=SYNTHETIC_CREDENTIAL,
            transport=used,
            raw_store_root=tmp_path / "raw",
            dataset_root=tmp_path / "dataset",
            identity_export=identity,
            gate_evidence=make_gate_evidence(tmp_path / "gates", symbols=("AAPL",)),
            observed_at=OBSERVED_AT,
            sleeper=no_sleep,
            synthetic=stolen,
        )

    assert not (tmp_path / "raw").exists()


def test_offline_transport_blocks_socket_access() -> None:
    """A responder that reaches for a socket fails instead of doing I/O."""

    def networking_responder(_body: Mapping[str, object], _credential: str) -> RawCall:
        socket.socket()
        raise AssertionError("unreachable: socket creation must be denied")

    offline = SyntheticTransport(networking_responder)

    with pytest.raises(SocketAccessError, match="must not open a socket"):
        offline({"symbol": "AAPL"}, SYNTHETIC_CREDENTIAL)


def test_offline_transport_blocks_prebound_socket_and_subprocess_aliases() -> None:
    prebound_socket = socket.socket

    def socket_responder(_body: Mapping[str, object], _credential: str) -> RawCall:
        prebound_socket()
        raise AssertionError("unreachable")

    with pytest.raises(SocketAccessError, match="external I/O is blocked"):
        SyntheticTransport(socket_responder)({"symbol": "AAPL"}, SYNTHETIC_CREDENTIAL)

    def subprocess_responder(_body: Mapping[str, object], _credential: str) -> RawCall:
        subprocess.run([sys.executable, "-c", "pass"], check=True)
        raise AssertionError("unreachable")

    with pytest.raises(SocketAccessError, match="external I/O is blocked"):
        SyntheticTransport(subprocess_responder)({"symbol": "AAPL"}, SYNTHETIC_CREDENTIAL)


def test_ambiguous_url_error_keeps_the_reservation_charged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def ambiguous_failure(*_args: object, **_kwargs: object) -> None:
        raise urllib.error.URLError(TimeoutError("post-send timeout"))

    monkeypatch.setattr("urllib.request.urlopen", ambiguous_failure)

    with pytest.raises(TransportFailureError) as failure:
        make_https_transport()({"symbol": "AAPL"}, SYNTHETIC_CREDENTIAL)

    assert failure.value.charged is True


# AAS008C-R3-F2 — no plaintext credential is ever persisted


def test_redaction_replaces_every_occurrence() -> None:
    payload = b'{"a":"secret","b":"secret"}'

    redacted, found = redact_credential(payload, "secret")

    assert found is True
    assert b"secret" not in redacted
    assert redacted.count(CREDENTIAL_REDACTION) == 2  # noqa: PLR2004 - both occurrences


def test_reflected_credential_is_never_written_to_any_artifact(tmp_path: Path) -> None:
    reflected = build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
    reflected["echo"] = SYNTHETIC_CREDENTIAL
    transport = RecordingTransport("baseline", overrides={"AAPL": reflected})

    with pytest.raises(ContractError) as failure:
        run(tmp_path, transport=transport)

    secret = SYNTHETIC_CREDENTIAL.encode()
    assert SYNTHETIC_CREDENTIAL not in str(failure.value)
    written = [path for path in tmp_path.rglob("*") if path.is_file()]
    assert written, "quarantined evidence must still exist"
    for path in written:
        assert secret not in path.read_bytes(), f"credential leaked into {path}"
    quarantined = [
        path
        for path in (tmp_path / "raw" / "blobs").rglob("*.raw")
        if CREDENTIAL_REDACTION in path.read_bytes()
    ]
    assert quarantined, "the redacted body must be retained as evidence"


def test_reflected_credential_evidence_is_marked_blocked(tmp_path: Path) -> None:
    reflected = build_response("AAPL", snapshot_items("baseline", "AAPL"), cost=0.0014)
    reflected["echo"] = SYNTHETIC_CREDENTIAL
    transport = RecordingTransport("baseline", overrides={"AAPL": reflected})

    with pytest.raises(ContractError):
        run(tmp_path, transport=transport)

    provenance = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "raw" / "snapshots").glob("*.json")
    ]
    assert provenance
    assert all(entry["validation_status"] == "BLOCKED" for entry in provenance)
    assert not list((tmp_path / "dataset").rglob("*.parquet"))


# AAS008C-R3-F5 — durability, hash verification, and leased recovery


def test_committed_partitions_are_hash_verified_on_read(tmp_path: Path) -> None:
    result = run(tmp_path)
    assert result.manifest is not None
    target = tmp_path / "dataset" / result.manifest.partitions[0].relative_path
    target.write_bytes(target.read_bytes() + b"corruption")

    with pytest.raises(PublicationError, match="does not match its recorded hash"):
        read_partition_rows(tmp_path / "dataset", result.manifest)


def test_missing_committed_partition_is_detected_on_read(tmp_path: Path) -> None:
    result = run(tmp_path)
    assert result.manifest is not None
    (tmp_path / "dataset" / result.manifest.partitions[0].relative_path).unlink()

    with pytest.raises(PublicationError, match="committed partition is missing"):
        read_partition_rows(tmp_path / "dataset", result.manifest)


def test_recovery_cannot_delete_an_active_publishers_files(tmp_path: Path) -> None:
    """A held publish lease blocks a concurrent recovery sweep."""

    result = run(tmp_path)
    assert result.manifest is not None
    committed_marker_path(tmp_path / "dataset", "snap-r3-001").unlink()
    partitions = sorted((tmp_path / "dataset").rglob("*.parquet"))
    assert partitions

    with publish_lease(tmp_path / "dataset", "snap-r3-001"):
        with pytest.raises(PublicationError, match="another live publisher holds the lease"):
            recover_uncommitted_partitions(tmp_path / "dataset", "snap-r3-001")
        assert sorted((tmp_path / "dataset").rglob("*.parquet")) == partitions


def test_a_crashed_publishers_lease_does_not_block_recovery(tmp_path: Path) -> None:
    """Process death releases the advisory lock, so no stale lease persists.

    A crashed publisher is simulated by a child process that acquires the lease
    and exits without releasing it. The kernel drops the flock on exit, so the
    same snapshot can be recovered and retried.
    """

    result = run(tmp_path)
    assert result.manifest is not None
    dataset_root = tmp_path / "dataset"
    committed_marker_path(dataset_root, "snap-r3-001").unlink()

    crashed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            (
                "import sys; sys.path.insert(0, sys.argv[1]);"
                "from pathlib import Path;"
                "from aegis_alpha.data.finimpulse_collector import publish_lease;"
                "ctx = publish_lease(Path(sys.argv[2]), 'snap-r3-001');"
                "ctx.__enter__();"
                "print('acquired', flush=True)"
            ),
            str(SRC_ROOT),
            str(dataset_root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert crashed.returncode == 0, crashed.stderr
    assert "acquired" in crashed.stdout
    assert publish_lease_path(dataset_root, "snap-r3-001").exists()

    # The lease file survives the crash, but the advisory lock does not.
    removed = recover_uncommitted_partitions(dataset_root, "snap-r3-001")
    assert removed
    with publish_lease(dataset_root, "snap-r3-001"):
        pass


def test_recovery_proceeds_once_the_lease_is_released(tmp_path: Path) -> None:
    result = run(tmp_path)
    assert result.manifest is not None
    committed_marker_path(tmp_path / "dataset", "snap-r3-001").unlink()

    removed = recover_uncommitted_partitions(tmp_path / "dataset", "snap-r3-001")

    assert removed
    assert not [
        path for path in (tmp_path / "dataset").rglob("*.parquet") if ".staging" not in path.parts
    ]


def test_publication_refuses_a_concurrently_leased_snapshot(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir(parents=True)

    with publish_lease(dataset_root, "snap-r3-001"):
        transport = RecordingTransport("baseline")
        with pytest.raises(PublicationError, match="another live publisher holds the lease"):
            run(tmp_path, transport=transport)
