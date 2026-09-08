"""AAS-DATA-008C CLI fail-closed tests; zero provider calls in every case."""

from __future__ import annotations

import io
import subprocess
import sys
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path

import pytest
from finimpulse_authority_support import (
    AUTHORITY_NOW,
    StandingAuthorityFixture,
    authority_argv,
    make_standing_authority,
    write_revocation,
)
from finimpulse_collector_support import (
    SYNTHETIC_CREDENTIAL,
    RecordingTransport,
    write_gate_evidence,
    write_identity_export,
)

from aegis_alpha.data import finimpulse_collector as collector
from aegis_alpha.data.finimpulse_collector import (
    CREDENTIAL_ENVIRONMENT_VARIABLE,
    Transport,
)
from aegis_alpha.data.finimpulse_collector_cli import PRECONDITION_EXIT, main
from aegis_alpha.data.finimpulse_owner_authority import OWNER_AUTHORITY_ENV
from aegis_alpha.data.finimpulse_recurring_authority import (
    RecurringAuthorityError,
    verify_recurring_authority,
)
from aegis_alpha.data.finimpulse_recurring_revocation import require_not_revoked, revocation_path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "collect_finimpulse_estimates.py"


def write_universe(tmp_path: Path, symbols: tuple[str, ...] = ("AAPL", "PLAB")) -> Path:
    path = tmp_path / "universe.txt"
    path.write_text("# synthetic universe\n" + "\n".join(symbols) + "\n", encoding="utf-8")
    return path


def cli_arguments(
    tmp_path: Path,
    outside: Path,
    *,
    standing: StandingAuthorityFixture | None = None,
) -> list[str]:
    identity_path, _ = write_identity_export(
        tmp_path / "evidence", {"AAPL": ["INST-AAPL"], "PLAB": []}
    )
    gate_path, _ = write_gate_evidence(tmp_path / "evidence")
    if standing is None:
        standing = make_standing_authority(tmp_path / "authority", destinations=outside)
    return [
        "--snapshot-id",
        "snap-cli-001",
        "--universe-file",
        str(write_universe(tmp_path)),
        "--raw-store-root",
        str(outside / "raw"),
        "--dataset-root",
        str(outside / "dataset"),
        "--receipt-path",
        str(outside / "receipt.json"),
        "--budget-usd",
        "0.10",
        "--identity-export",
        str(identity_path),
        "--gate-evidence",
        str(gate_path),
        *authority_argv(standing),
    ]


def authorize(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Commit the synthetic artifacts into the owner authority reference.

    Production ships both sets empty, so this patch stands in for the separate
    owner PR that would commit an exact authorized digest.
    """

    _, identity_digest = write_identity_export(
        tmp_path / "evidence", {"AAPL": ["INST-AAPL"], "PLAB": []}
    )
    _, gate_digest = write_gate_evidence(tmp_path / "evidence")
    monkeypatch.setattr(
        collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({identity_digest})
    )
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({gate_digest}))


def _admit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    destinations: Path | None = None,
    document_changes: Mapping[str, object] | None = None,
) -> StandingAuthorityFixture:
    dest = tmp_path / "outside" if destinations is None else destinations
    fixture = make_standing_authority(
        tmp_path / "authority",
        destinations=dest,
        document_changes=document_changes,
    )
    monkeypatch.setattr(
        "aegis_alpha.data.finimpulse_collector_cli.load_owner_authority",
        lambda *_args, **_kwargs: fixture.owner_authority,
    )
    return fixture


def _environ(
    extra: dict[str, str] | None = None,
    *,
    standing: StandingAuthorityFixture | None = None,
) -> dict[str, str]:
    environ = {CREDENTIAL_ENVIRONMENT_VARIABLE: SYNTHETIC_CREDENTIAL}
    if standing is not None:
        environ[OWNER_AUTHORITY_ENV] = str(standing.owner_authority_path)
    if extra:
        environ.update(extra)
    return environ


def run_cli(
    arguments: list[str],
    *,
    environ: dict[str, str] | None = None,
    transport: Transport | None = None,
) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        arguments,
        environ={} if environ is None else environ,
        transport=transport,
        stdout=stdout,
        stderr=stderr,
        now=AUTHORITY_NOW,
    )
    return code, stdout.getvalue(), stderr.getvalue()


def test_cli_requires_explicit_live_opt_in(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        cli_arguments(tmp_path, tmp_path / "outside"),
        environ={CREDENTIAL_ENVIRONMENT_VARIABLE: SYNTHETIC_CREDENTIAL},
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "--live is required" in stderr
    assert transport.calls == []


def test_cli_fails_closed_without_the_environment_credential(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside")],
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "no provider calls were attempted" in stderr
    assert CREDENTIAL_ENVIRONMENT_VARIABLE in stderr
    assert transport.calls == []


def test_cli_rejects_destinations_inside_a_git_repository(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    (repository / ".git").mkdir(parents=True)
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, repository / "evidence")],
        environ={CREDENTIAL_ENVIRONMENT_VARIABLE: SYNTHETIC_CREDENTIAL},
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "must be outside a Git repository" in stderr
    assert transport.calls == []


def test_unauthorized_gate_artifact_blocks_with_zero_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A locally fabricated artifact cannot self-authorize a paid run."""

    fixture = _admit(tmp_path, monkeypatch)
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "no owner-authorized G-B gate artifact digest is committed" in stderr
    assert transport.calls == []
    assert not (tmp_path / "outside").exists()


def test_artifact_outside_the_authority_set_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(collector, "AUTHORIZED_GATE_ARTIFACT_SHA256", frozenset({"c" * 64}))
    monkeypatch.setattr(collector, "AUTHORIZED_IDENTITY_EXPORT_SHA256", frozenset({"d" * 64}))
    fixture = _admit(tmp_path, monkeypatch)
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "is not owner-authorized" in stderr
    assert transport.calls == []


def test_missing_registry_target_blocks_with_zero_calls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Registration is mandatory, so an absent database precedes any call."""

    authorize(monkeypatch, tmp_path)
    fixture = _admit(tmp_path, monkeypatch)
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "AAS-DATA-005 registration is required" in stderr
    assert transport.calls == []
    assert not (tmp_path / "outside").exists()


def test_unreachable_registry_blocks_with_zero_calls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    authorize(monkeypatch, tmp_path)
    fixture = _admit(tmp_path, monkeypatch)
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(
            {
                "AAS_DATABASE_URL": "postgresql+psycopg://nobody@127.0.0.1:1/none",
            },
            standing=fixture,
        ),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "unreachable" in stderr
    assert transport.calls == []
    assert not (tmp_path / "outside").exists()


def test_preexisting_receipt_target_blocks_with_zero_calls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The receipt target is reserved before transport, never after."""

    authorize(monkeypatch, tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir(parents=True)
    (outside / "receipt.json").write_text("{}", encoding="utf-8")
    fixture = _admit(tmp_path, monkeypatch, destinations=outside)
    transport = RecordingTransport("baseline")

    code, _, _ = run_cli(
        ["--live", *cli_arguments(tmp_path, outside, standing=fixture)],
        environ=_environ(
            {
                "AAS_DATABASE_URL": "postgresql+psycopg://nobody@127.0.0.1:1/none",
            },
            standing=fixture,
        ),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert transport.calls == []


def test_cli_rejects_a_nonpositive_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    authorize(monkeypatch, tmp_path)
    fixture = _admit(tmp_path, monkeypatch)
    arguments = ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)]
    arguments[arguments.index("--budget-usd") + 1] = "0"

    code, _, stderr = run_cli(
        arguments,
        environ=_environ(
            {
                "AAS_DATABASE_URL": "postgresql+psycopg://nobody@127.0.0.1:1/none",
            },
            standing=fixture,
        ),
        transport=RecordingTransport("baseline"),
    )

    assert code == PRECONDITION_EXIT
    assert "positive finite amount" in stderr


def test_script_wrapper_fails_closed_without_opt_in(tmp_path: Path) -> None:
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(SCRIPT),
            *cli_arguments(tmp_path, tmp_path / "outside"),
        ],
        check=False,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(ROOT / "src")},
        text=True,
    )

    assert result.returncode == PRECONDITION_EXIT
    assert "--live is required" in result.stderr


@pytest.mark.parametrize("missing", ["--snapshot-id", "--receipt-path"])
def test_cli_requires_its_evidence_arguments(missing: str, tmp_path: Path) -> None:
    arguments = ["--live", *cli_arguments(tmp_path, tmp_path / "outside")]
    index = arguments.index(missing)
    del arguments[index : index + 2]

    with pytest.raises(SystemExit):
        run_cli(arguments, environ={CREDENTIAL_ENVIRONMENT_VARIABLE: SYNTHETIC_CREDENTIAL})


def test_live_requires_standing_authority(tmp_path: Path) -> None:
    transport = RecordingTransport("baseline")
    arguments = cli_arguments(tmp_path, tmp_path / "outside")
    authority_index = arguments.index("--recurring-authority")
    del arguments[authority_index : authority_index + 4]

    code, _, stderr = run_cli(
        ["--live", *arguments],
        environ={CREDENTIAL_ENVIRONMENT_VARIABLE: SYNTHETIC_CREDENTIAL},
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "--recurring-authority and --recurring-authority-signature are required" in stderr
    assert transport.calls == []


def test_per_probe_owner_approval_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _admit(tmp_path, monkeypatch)
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        [
            "--live",
            *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture),
            "--owner-approval",
            str(tmp_path / "approval.json"),
        ],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "does not accept per-probe owner-approval inputs" in stderr
    assert transport.calls == []


def test_expired_standing_scope_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _admit(
        tmp_path,
        monkeypatch,
        document_changes={
            "issued_at_utc": "2026-08-19T11:59:00.000000Z",
            "valid_from_utc": "2026-08-19T12:00:00.000000Z",
        },
    )
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "not currently valid" in stderr
    assert transport.calls == []


def test_revoked_standing_scope_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _admit(tmp_path, monkeypatch)
    verified = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=AUTHORITY_NOW
    )
    write_revocation(verified, AUTHORITY_NOW)
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "recurring authority was revoked" in stderr
    assert transport.calls == []


def test_malformed_revocation_marker_fails_closed(tmp_path: Path) -> None:
    fixture = make_standing_authority(tmp_path / "authority", destinations=tmp_path / "outside")
    verified = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=AUTHORITY_NOW
    )
    destination = revocation_path(verified)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(b"not-json")

    with pytest.raises(RecurringAuthorityError, match="revocation is invalid"):
        require_not_revoked(verified, AUTHORITY_NOW)


def test_bad_signature_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = make_standing_authority(tmp_path / "authority", destinations=tmp_path / "outside")
    other = make_standing_authority(tmp_path / "other", destinations=tmp_path / "outside")
    monkeypatch.setattr(
        "aegis_alpha.data.finimpulse_collector_cli.load_owner_authority",
        lambda *_args, **_kwargs: other.owner_authority,
    )
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "signature verification failed" in stderr
    assert transport.calls == []


def test_wrong_contract_version_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _admit(tmp_path, monkeypatch, document_changes={"version": 2})
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "contract is unsupported" in stderr
    assert transport.calls == []


def test_spend_bound_exhaustion_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _admit(tmp_path, monkeypatch, document_changes={"max_spend_micros": 1})
    transport = RecordingTransport("baseline")

    code, _, stderr = run_cli(
        ["--live", *cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)],
        environ=_environ(standing=fixture),
        transport=transport,
    )

    assert code == PRECONDITION_EXIT
    assert "max_spend_micros" in stderr
    assert transport.calls == []


def test_standing_scope_expires_with_its_signing_key(tmp_path: Path) -> None:
    fixture = make_standing_authority(tmp_path / "authority", destinations=tmp_path / "outside")
    verified = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=AUTHORITY_NOW
    )

    with pytest.raises(RecurringAuthorityError, match="signing key is expired"):
        verified.require_request(fixture.owner_authority.valid_until_utc)
    verified.require_request(fixture.owner_authority.valid_until_utc - timedelta(microseconds=1))


def test_credential_rotation_does_not_block_zero_call_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery detection precedes transport-only credential preconditions."""

    authorize(monkeypatch, tmp_path)
    fixture = _admit(tmp_path, monkeypatch)
    arguments = cli_arguments(tmp_path, tmp_path / "outside", standing=fixture)
    receipt_path = Path(arguments[arguments.index("--receipt-path") + 1])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text("{}", encoding="utf-8")
    resume_called: list[object] = []

    class _DummyEngine:
        def dispose(self) -> None:
            return None

    engine = _DummyEngine()

    def fake_resume(*_args: object, **_kwargs: object) -> tuple[str, str, bool]:
        resume_called.append(True)
        return "RESUMED", "0" * 64, True

    monkeypatch.setattr(
        "aegis_alpha.data.finimpulse_collector_cli._resume_interrupted_lifecycle",
        fake_resume,
    )
    monkeypatch.setattr(
        "aegis_alpha.data.finimpulse_collector_cli.require_registry_target",
        lambda *_args, **_kwargs: engine,
    )

    code, stdout, stderr = run_cli(
        ["--live", *arguments],
        environ=_environ(standing=fixture) | {CREDENTIAL_ENVIRONMENT_VARIABLE: ""},
    )

    assert resume_called == [True], stderr
    assert code == 0, stderr
    assert "RESUMED" in stdout
    assert stderr == ""
