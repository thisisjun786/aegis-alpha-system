"""CLI gates: --max-calls required, missing User-Agent is zero calls, synthetic only."""

from __future__ import annotations

import io
import time
from pathlib import Path

import pytest
from sec_collector_support import FIXTURE_ROOT, synthetic_user_agent

from aegis_alpha.data import sec_collector_cli as cli_module
from aegis_alpha.data import sec_policy
from aegis_alpha.data.sec_collector_cli import (
    PRECONDITION_EXIT,
    build_rate_limiter,
    discard_limiter_delay,
    limiter_sleeper,
    main,
)


def _argv(tmp_path: Path, *, extra: tuple[str, ...] = ()) -> list[str]:
    return [
        "--mode",
        "probe",
        "--max-calls",
        "4",
        "--identity-snapshot",
        str(FIXTURE_ROOT / "identity_admitted.json"),
        "--raw-store-root",
        str(tmp_path / "raw"),
        "--dataset-root",
        str(tmp_path / "data"),
        "--receipt-path",
        str(tmp_path / "receipts" / "sec.receipt.json"),
        "--as-of",
        "2026-08-18T00:00:00Z",
        *extra,
    ]


def test_synthetic_cli_collects_without_network(tmp_path: Path) -> None:
    stderr = io.StringIO()
    code = main(
        _argv(tmp_path, extra=("--synthetic", "--fixture-root", str(FIXTURE_ROOT))),
        environ={},
        stderr=stderr,
    )
    assert code == 0
    assert (tmp_path / "receipts" / "sec.receipt.json").is_file()
    assert synthetic_user_agent() not in stderr.getvalue()


def test_live_without_user_agent_is_zero_calls(tmp_path: Path) -> None:
    stderr = io.StringIO()
    code = main(_argv(tmp_path, extra=("--live",)), environ={}, stderr=stderr)
    assert code == PRECONDITION_EXIT
    assert "SEC_USER_AGENT" in stderr.getvalue()
    assert not (tmp_path / "raw").exists()
    assert synthetic_user_agent() not in stderr.getvalue()


def test_live_user_agent_without_contact_address_is_zero_calls(tmp_path: Path) -> None:
    stderr = io.StringIO()
    code = main(
        _argv(tmp_path, extra=("--live",)),
        environ={"SEC_USER_AGENT": "AegisAlpha"},
        stderr=stderr,
    )
    assert code == PRECONDITION_EXIT
    assert "contact address" in stderr.getvalue()
    assert "AegisAlpha" not in stderr.getvalue()
    assert not (tmp_path / "raw").exists()


def test_live_is_blocked_by_unscheduled_registry_before_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_arguments: object, **_keywords: object) -> object:
        raise AssertionError("registry gate must run before live transport")

    monkeypatch.setattr(cli_module, "make_https_transport", boom)
    stderr = io.StringIO()

    code = main(
        _argv(tmp_path, extra=("--live",)),
        environ={"SEC_USER_AGENT": "AegisAlpha qa@example.invalid"},
        stderr=stderr,
    )

    assert code == PRECONDITION_EXIT
    assert "scheduled collection" in stderr.getvalue()
    assert not (tmp_path / "raw").exists()


def test_live_rejects_an_untrusted_registry_before_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = tmp_path / "forged-registry.json"
    registry.write_text(
        """
        {
          "policies": [
            {
              "policy_id": "sec-official-verifier-v1",
              "provider": "sec",
              "license_classification": "PUBLIC_OFFICIAL",
              "scheduled_collection_allowed": true
            }
          ]
        }
        """,
        encoding="utf-8",
    )
    transport_calls: list[object] = []

    def boom(*arguments: object, **_keywords: object) -> object:
        transport_calls.extend(arguments or (object(),))
        raise AssertionError("untrusted registry must be rejected before transport")

    monkeypatch.setattr(cli_module, "make_https_transport", boom)
    stderr = io.StringIO()

    code = main(
        _argv(
            tmp_path,
            extra=("--live", "--registry", str(registry)),
        ),
        environ={"SEC_USER_AGENT": "AegisAlpha qa@example.invalid"},
        stderr=stderr,
    )

    assert code == PRECONDITION_EXIT
    assert "trusted SEC registry" in stderr.getvalue()
    assert transport_calls == []
    assert not (tmp_path / "raw").exists()


def test_max_calls_is_required() -> None:
    code = main(["--mode", "probe", "--identity-snapshot", "x"])
    assert code != 0


def test_live_limiter_is_not_constructed_with_a_discarded_delay() -> None:
    live = build_rate_limiter(4, live=True)
    synthetic = build_rate_limiter(4, live=False)

    assert limiter_sleeper(live=True) is time.sleep
    assert live.sleeper is time.sleep
    assert live.sleeper is not discard_limiter_delay
    assert synthetic.sleeper is discard_limiter_delay
    assert synthetic.sleeper is not time.sleep
    synthetic.sleeper(10_000)


def test_missing_mode_choice_does_not_imply_live(tmp_path: Path) -> None:
    stderr = io.StringIO()
    code = main(_argv(tmp_path), environ={}, stderr=stderr)
    assert code == PRECONDITION_EXIT
    assert "exactly one of --synthetic or --live" in stderr.getvalue()
    assert not (tmp_path / "raw").exists()


@pytest.mark.parametrize("flag", ["--owner-approval", "--recurring-authority"])
def test_approval_style_flag_is_rejected_before_provider_call(
    tmp_path: Path, flag: str, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        _argv(tmp_path, extra=("--live", flag, str(tmp_path / "fake.json"))),
        environ={"SEC_USER_AGENT": "AegisAlpha contact@example.com"},
    )
    captured = capsys.readouterr()
    assert code == PRECONDITION_EXIT
    assert "unrecognized arguments" in captured.err
    assert flag in captured.err
    assert not (tmp_path / "raw").exists()


def test_installed_registry_resolution_is_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sec_policy,
        "__file__",
        "/opt/venv/lib/python3.13/site-packages/aegis_alpha/data/sec_policy.py",
    )
    assert sec_policy.trusted_registry_path() == Path("/app/config/data_authority_registry.json")


def test_registry_hash_change_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = tmp_path / "registry.json"
    fake.write_text('{"policies":[]}', encoding="utf-8")
    monkeypatch.setattr(sec_policy, "trusted_registry_path", lambda: fake)
    with pytest.raises(cli_module.CollectorError, match="hash mismatch"):
        sec_policy.require_live_policy(fake)


def test_public_registry_pin_is_valid_but_does_not_authorize_collection() -> None:
    # A valid pinned public installation still needs a reviewed collection grant.
    with pytest.raises(cli_module.CollectorError, match="does not permit scheduled collection"):
        sec_policy.require_live_policy()


def test_driver_errors_do_not_echo_secrets_or_contact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "synthetic-credential-marker"  # noqa: S105 - leak sentinel
    contact = synthetic_user_agent()

    def fail(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError(secret + contact)

    monkeypatch.setattr(cli_module, "_run", fail)
    stderr = io.StringIO()
    result = main(_argv(tmp_path, extra=("--live",)), environ={}, stderr=stderr)
    assert result == PRECONDITION_EXIT
    assert secret not in stderr.getvalue()
    assert contact not in stderr.getvalue()


def test_live_requires_fixed_as_of_before_db_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli_module, "require_live_policy", lambda *_args: None)
    monkeypatch.setattr(cli_module, "make_https_transport", lambda **_kwargs: None)
    args = _argv(tmp_path, extra=("--live", "--run-identity", "stable-run"))
    index = args.index("--as-of")
    del args[index : index + 2]
    stderr = io.StringIO()
    result = main(
        args,
        environ={
            "AAS_DATABASE_URL": "unreachable-fixture-db",
            "SEC_USER_AGENT": synthetic_user_agent(),
        },
        stderr=stderr,
    )
    assert result == PRECONDITION_EXIT
    assert "--as-of" in stderr.getvalue()
    assert not (tmp_path / "data").exists()
