from __future__ import annotations

import io
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fred_alfred_authority_support import (
    AUTHORITY_NOW,
    StandingAuthorityFixture,
    make_standing_authority,
    write_revocation,
)
from fred_alfred_collector_support import FIXTURE_ROOT, MAX_CALLS, default_script
from sqlalchemy import select

from aegis_alpha.collection.schema import collection_usage_records
from aegis_alpha.data import fred_alfred_collector as collector_module
from aegis_alpha.data import fred_alfred_collector_cli as cli_module
from aegis_alpha.data.fred_alfred_collector import DestinationError
from aegis_alpha.data.fred_alfred_collector_cli import (
    PRECONDITION_EXIT,
    load_fred_alfred_policy,
    main,
    policy_blocks_live,
    run_preflight,
)
from aegis_alpha.data.fred_alfred_owner_authority import (
    OWNER_AUTHORITY_ENV,
    OwnerAuthorityError,
    load_owner_authority,
)
from aegis_alpha.data.fred_alfred_recurring_authority import (
    MAX_CALLS_PER_MINUTE,
    RecurringAuthorityError,
    verify_recurring_authority,
)
from aegis_alpha.data.fred_alfred_recurring_revocation import (
    require_not_revoked,
    revocation_path,
)
from aegis_alpha.data.fred_alfred_series import (
    DEFAULT_SERIES_IDS,
    DEFAULT_SERIES_UNIVERSE_SHA256,
    MACRO_SERIES_IDS,
    MACRO_SERIES_UNIVERSE_SHA256,
    PENDING_LICENSE,
    POLICY_ID,
    series_universe_sha256,
)
from aegis_alpha.data.fred_alfred_usage_budget import CONTROL_PLANE_URL_ENV

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy import Engine

NOW = AUTHORITY_NOW
REPO_REGISTRY = Path(__file__).resolve().parents[2] / "config" / "data_authority_registry.json"
CLEARED_REGISTRY = FIXTURE_ROOT / "registry_cleared.json"


def _destinations(tmp_path: Path) -> dict[str, Path]:
    return {
        "raw_store_root": tmp_path / "raw",
        "dataset_root": tmp_path / "normalized",
        "receipt_path": tmp_path / "receipts" / "run.receipt.json",
    }


def _cli(  # noqa: PLR0913 - one keyword per explicit CLI input
    tmp_path: Path,
    *,
    extra: Sequence[str] = (),
    environ: dict[str, str] | None = None,
    registry: Path = REPO_REGISTRY,
    max_calls: int = MAX_CALLS,
    dry_run: bool = False,
) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    destinations = _destinations(tmp_path)
    argv = [
        "--mode",
        "incremental",
        "--max-calls",
        str(max_calls),
        "--registry",
        str(registry),
        "--raw-store-root",
        str(destinations["raw_store_root"]),
        "--dataset-root",
        str(destinations["dataset_root"]),
        "--receipt-path",
        str(destinations["receipt_path"]),
        *extra,
    ]
    if dry_run:
        argv.append("--dry-run")
    scripted = default_script()

    def transport(
        request: collector_module.CollectorRequest, credential: str
    ) -> collector_module.CollectorResponse:
        response = scripted(request, credential)
        timestamp = datetime.now(UTC)
        return replace(response, requested_at_utc=timestamp, retrieved_at_utc=timestamp)

    code = main(
        argv,
        environ={"FRED_API_KEY": "SYNTH-FRED-KEY"} if environ is None else environ,
        stdout=stdout,
        stderr=stderr,
        now=NOW,
        transport=transport,
    )
    return (code, stdout.getvalue(), stderr.getvalue())


def _admit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    document_changes: Mapping[str, object] | None = None,
) -> StandingAuthorityFixture:
    """Build a signed standing authority and anchor the CLI to its real public key.

    Only the externally-owned-file boundary is substituted; signature
    verification, validity, revocation, domain, root, and budget gates all run.
    """

    fixture = make_standing_authority(tmp_path, document_changes=document_changes)
    monkeypatch.setattr(cli_module, "load_owner_authority", lambda *_args: fixture.owner_authority)
    return fixture


def _authority_argv(fixture: StandingAuthorityFixture) -> tuple[str, ...]:
    return (
        "--recurring-authority",
        str(fixture.authority_path),
        "--recurring-authority-signature",
        str(fixture.signature_path),
    )


def _authority_environ(
    fixture: StandingAuthorityFixture,
    *,
    database_url: str | None = None,
) -> dict[str, str]:
    environ = {
        "FRED_API_KEY": "SYNTH-FRED-KEY",
        OWNER_AUTHORITY_ENV: str(fixture.owner_authority_path),
    }
    if database_url is not None:
        environ[CONTROL_PLANE_URL_ENV] = database_url
    return environ


def test_repository_registry_still_pending_and_unscheduled() -> None:
    policy = load_fred_alfred_policy(REPO_REGISTRY)
    assert policy.policy_id == POLICY_ID
    assert policy.license_classification == PENDING_LICENSE
    assert policy.scheduled_collection_allowed is False
    assert policy_blocks_live(policy) is True


def test_dry_run_prints_frozen_universe_and_makes_zero_calls(tmp_path: Path) -> None:
    # Given an unsigned dry-run invocation
    # When the CLI runs
    code, stdout, stderr = _cli(tmp_path, dry_run=True, environ={})
    # Then only the frozen plan prints and no authority is consulted
    assert code == 0
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["command"] == "dry-run"
    assert payload["provider_calls"] == 0
    assert payload["series_ids"] == list(DEFAULT_SERIES_IDS)
    assert payload["default_series_universe_sha256"] == DEFAULT_SERIES_UNIVERSE_SHA256
    assert payload["series_universe_sha256"] == DEFAULT_SERIES_UNIVERSE_SHA256
    assert payload["macro_series_universe_sha256"] == MACRO_SERIES_UNIVERSE_SHA256
    assert payload["series_selection"] == "legacy4"
    assert payload["mode"] == "incremental"


def test_dry_run_rejects_standing_authority_flags(tmp_path: Path) -> None:
    # Given a signed standing authority
    fixture = make_standing_authority(tmp_path)
    # When a dry-run presents live admission artifacts
    code, stdout, stderr = _cli(
        tmp_path, extra=list(_authority_argv(fixture)), dry_run=True, environ={}
    )
    # Then the run fails closed; dry-run stays unsigned
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "--recurring-authority is only accepted for live gated runs" in stderr
    assert "no provider calls were attempted" in stderr


def test_live_fails_closed_while_registry_is_unscheduled(tmp_path: Path) -> None:
    # Given the shipped registry, which has not scheduled FRED collection
    # When a live run is attempted
    code, stdout, stderr = _cli(tmp_path)
    # Then the registry gate fails closed before any authority is consulted
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "scheduled_collection_allowed" in stderr
    assert "no provider calls were attempted" in stderr


def test_standing_authority_cannot_open_an_unscheduled_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a fully valid signed standing authority
    fixture = _admit(tmp_path, monkeypatch)
    # When a live run presents it against the unscheduled shipped registry
    code, stdout, stderr = _cli(
        tmp_path,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then the registry gate still fails closed
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "scheduled_collection_allowed" in stderr
    assert "no provider calls were attempted" in stderr


def test_scheduled_registry_requires_recurring_authority(tmp_path: Path) -> None:
    # Given a scheduled, license-cleared registry
    # When a live run presents no standing authority
    code, stdout, stderr = _cli(tmp_path, registry=CLEARED_REGISTRY)
    # Then admission fails closed naming the required artifacts
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "--recurring-authority and --recurring-authority-signature are required" in stderr
    assert "no provider calls were attempted" in stderr


def test_scheduled_registry_requires_owner_authority_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a scheduled registry and a signed standing authority
    fixture = _admit(tmp_path, monkeypatch)
    # When the external public-key authority path is not configured
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ={"FRED_API_KEY": "SYNTH-FRED-KEY"},
    )
    # Then admission fails closed naming the environment variable
    assert code == PRECONDITION_EXIT
    assert f"{OWNER_AUTHORITY_ENV} is required" in stderr
    assert "no provider calls were attempted" in stderr


def test_live_admission_requires_control_plane_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _admit(tmp_path, monkeypatch)
    code, stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert CONTROL_PLANE_URL_ENV in stderr
    assert "no provider calls were attempted" in stderr


def test_valid_standing_authority_executes_fixture_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_postgres: Engine
) -> None:
    # Given a scheduled registry and a fully valid signed standing authority
    fixture = _admit(tmp_path, monkeypatch)
    # When a live run is admitted by that standing authority alone
    code, stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(
            fixture,
            database_url=clean_postgres.url.render_as_string(hide_password=False),
        ),
    )
    assert code == 0
    assert stderr == ""
    result = json.loads(stdout)
    assert result["provider_calls"] > 0
    assert result["run_id"].startswith("fred-alfred-run-")
    assert result["status"] == "run_succeeded"
    assert Path(result["published_paths"][-1]).name == f"run.receipt-{result['run_id']}.json"


def test_preflight_admits_live_run_on_standing_authority_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_postgres: Engine
) -> None:
    # Given a scheduled registry and a valid signed standing authority
    fixture = _admit(tmp_path, monkeypatch)
    # When preflight runs for live gates with no per-run approval artifact
    result = run_preflight(
        registry_path=CLEARED_REGISTRY,
        recurring_authority_path=fixture.authority_path,
        recurring_signature_path=fixture.signature_path,
        environment=_authority_environ(
            fixture,
            database_url=clean_postgres.url.render_as_string(hide_password=False),
        ),
        max_calls=MAX_CALLS,
        **_destinations(tmp_path),
        series_ids=DEFAULT_SERIES_IDS,
        now=NOW,
        require_live_gates=True,
    )
    # Then the verified standing authority is the only admission evidence
    assert result.authority is not None
    assert result.authority.calls_per_minute == MAX_CALLS_PER_MINUTE
    assert result.authority.allowed_domains == ("api.stlouisfed.org",)
    assert result.artifact_hashes["recurring_authority"] == result.authority.payload_sha256
    assert set(result.artifact_hashes) == {
        "registry",
        "recurring_authority",
        "recurring_authority_signature",
    }


def test_not_yet_valid_authority_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a standing authority whose validity window starts tomorrow
    fixture = _admit(
        tmp_path,
        monkeypatch,
        document_changes={
            "issued_at_utc": "2026-08-19T11:59:00.000000Z",
            "valid_from_utc": "2026-08-19T12:00:00.000000Z",
        },
    )
    # When a live run presents it before the window opens
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then admission fails closed
    assert code == PRECONDITION_EXIT
    assert "not currently valid" in stderr
    assert "no provider calls were attempted" in stderr


def test_authority_fails_closed_after_signing_key_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a scope issued and activated before its signing key expired
    fixture = make_standing_authority(
        tmp_path,
        document_changes={
            "valid_from_utc": "2026-08-18T11:59:15.000000Z",
        },
    )
    expired_owner = replace(
        fixture.owner_authority,
        valid_until_utc=NOW - timedelta(seconds=30),
    )
    monkeypatch.setattr(cli_module, "load_owner_authority", lambda *_args: expired_owner)
    # When a live run presents it after the signing key validity ended
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then admission fails before the zero-socket live wall
    assert code == PRECONDITION_EXIT
    assert "signing key is expired" in stderr
    assert "--run-id is required for durable recovery" not in stderr
    assert "no provider calls were attempted" in stderr


def test_verified_authority_expires_at_signing_key_boundary(tmp_path: Path) -> None:
    fixture = make_standing_authority(tmp_path)
    verified = verify_recurring_authority(
        fixture.payload,
        fixture.signature,
        fixture.owner_authority,
        now=NOW,
    )

    with pytest.raises(RecurringAuthorityError, match="signing key is expired"):
        verified.require_request(fixture.owner_authority.valid_until_utc)


def test_revoked_authority_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a valid standing authority that the owner then revokes
    fixture = _admit(tmp_path, monkeypatch)
    verified = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=NOW
    )
    write_revocation(verified, NOW)
    # When a live run presents the revoked authority
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then admission fails closed on the revocation marker
    assert code == PRECONDITION_EXIT
    assert "recurring authority was revoked" in stderr
    assert "no provider calls were attempted" in stderr


def test_malformed_revocation_marker_fails_closed(tmp_path: Path) -> None:
    # Given a verified authority and a garbage revocation marker
    fixture = make_standing_authority(tmp_path)
    verified = verify_recurring_authority(
        fixture.payload, fixture.signature, fixture.owner_authority, now=NOW
    )
    destination = revocation_path(verified)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(b"not-json")
    # When the revocation gate runs it fails closed rather than guessing
    with pytest.raises(RecurringAuthorityError, match="revocation is invalid"):
        require_not_revoked(verified, NOW)


def test_tampered_signature_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a scope signed under a different key than the trust anchor
    fixture = make_standing_authority(tmp_path)
    other = make_standing_authority(tmp_path / "other")
    monkeypatch.setattr(cli_module, "load_owner_authority", lambda *_args: other.owner_authority)
    # When a live run presents the mismatched pair
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then admission fails closed at signature verification
    assert code == PRECONDITION_EXIT
    assert "signature verification failed" in stderr
    assert "no provider calls were attempted" in stderr


def test_max_calls_above_signed_daily_budget_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a standing authority whose daily budget is below the run's --max-calls
    fixture = _admit(tmp_path, monkeypatch, document_changes={"calls_per_day": MAX_CALLS - 1})
    # When a live run requests more calls than the signed daily budget
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then admission fails closed on the spend bound
    assert code == PRECONDITION_EXIT
    assert "calls_per_day" in stderr
    assert "no provider calls were attempted" in stderr


def test_repeated_preflights_never_spend_the_daily_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_postgres: Engine
) -> None:
    fixture = _admit(tmp_path, monkeypatch, document_changes={"calls_per_day": MAX_CALLS})
    for _ in range(2):
        result = run_preflight(
            registry_path=CLEARED_REGISTRY,
            recurring_authority_path=fixture.authority_path,
            recurring_signature_path=fixture.signature_path,
            environment=_authority_environ(
                fixture, database_url=clean_postgres.url.render_as_string(hide_password=False)
            ),
            max_calls=MAX_CALLS,
            **_destinations(tmp_path),
            series_ids=DEFAULT_SERIES_IDS,
            now=NOW,
            require_live_gates=True,
        )
        assert result.authority is not None
    with clean_postgres.connect() as connection:
        assert connection.execute(select(collection_usage_records)).all() == []


def test_rate_bound_above_120_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a standing authority claiming a rate above the FRED hard cap
    fixture = _admit(tmp_path, monkeypatch, document_changes={"calls_per_minute": 121})
    # When a live run presents it
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then verification fails closed
    assert code == PRECONDITION_EXIT
    assert "calls_per_minute exceeds 120" in stderr
    assert "no provider calls were attempted" in stderr


def test_domain_outside_fred_host_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a standing authority whose allowed domains exceed the FRED host
    fixture = _admit(
        tmp_path,
        monkeypatch,
        document_changes={"allowed_domains": ["api.stlouisfed.org", "fred.example.com"]},
    )
    # When a live run presents it
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then verification fails closed on the exact-domain bound
    assert code == PRECONDITION_EXIT
    assert "allowed domains" in stderr
    assert "no provider calls were attempted" in stderr


def test_roots_outside_signed_scope_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a standing authority signed for a different raw store root
    fixture = _admit(
        tmp_path,
        monkeypatch,
        document_changes={"raw_store_root": str((tmp_path / "elsewhere-raw").resolve())},
    )
    # When a live run targets this test's roots
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(fixture),
    )
    # Then admission fails closed on the exact-roots bound
    assert code == PRECONDITION_EXIT
    assert "roots do not match the signed standing authority" in stderr
    assert "no provider calls were attempted" in stderr


def test_owner_authority_loader_accepts_independently_owned_key(tmp_path: Path) -> None:
    # Given an owner authority key file owned by a different OS principal
    fixture = make_standing_authority(tmp_path)
    # When the collector loads it
    authority = load_owner_authority(
        fixture.owner_authority_path,
        NOW,
        collector_uid=2000,
        ownership_reader=lambda _metadata: 1000,
    )
    # Then the public trust material is available
    assert authority.authority_id == fixture.owner_authority.authority_id
    assert authority.key_id == fixture.owner_authority.key_id
    assert authority.artifact_sha256 == fixture.owner_authority.artifact_sha256


def test_owner_authority_loader_rejects_collector_owned_key(tmp_path: Path) -> None:
    # Given an owner authority key file owned by the collector principal itself
    fixture = make_standing_authority(tmp_path)
    # When the collector loads it
    # Then the boundary fails closed
    with pytest.raises(OwnerAuthorityError, match="independently owned"):
        load_owner_authority(
            fixture.owner_authority_path,
            NOW,
            collector_uid=1000,
            ownership_reader=lambda _metadata: 1000,
        )


def test_owner_authority_loader_rejects_a_key_outside_its_validity_window(
    tmp_path: Path,
) -> None:
    # Given an owner authority key whose validity window has closed
    fixture = make_standing_authority(
        tmp_path, owner_changes={"valid_until_utc": "2026-08-18T11:00:00.000000Z"}
    )
    # When the collector loads it
    # Then the boundary fails closed
    with pytest.raises(OwnerAuthorityError, match="not currently valid"):
        load_owner_authority(
            fixture.owner_authority_path,
            NOW,
            collector_uid=2000,
            ownership_reader=lambda _metadata: 1000,
        )


def test_missing_credential_means_zero_calls(tmp_path: Path) -> None:
    code, stdout, stderr = _cli(tmp_path, environ={})
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "FRED_API_KEY is not set" in stderr


def test_max_calls_is_required() -> None:
    with pytest.raises(SystemExit):
        main(
            [
                "--mode",
                "probe",
                "--raw-store-root",
                "x",
                "--dataset-root",
                "y",
                "--receipt-path",
                "z",
            ],
            environ={"FRED_API_KEY": "x"},
            stdout=io.StringIO(),
            stderr=io.StringIO(),
            now=NOW,
        )


def test_invented_series_is_rejected_before_any_call(tmp_path: Path) -> None:
    code, _stdout, stderr = _cli(
        tmp_path, extra=["--series", "T10Y2Y", "--series", "GDPPOT"], dry_run=True
    )
    assert code == PRECONDITION_EXIT
    assert "must not invent extra series" in stderr


def test_duplicate_series_is_rejected_before_any_call(tmp_path: Path) -> None:
    code, _stdout, stderr = _cli(
        tmp_path, extra=["--series", "UNRATE", "--series", "UNRATE"], dry_run=True
    )
    assert code == PRECONDITION_EXIT
    assert "duplicate series_id" in stderr


def test_macro_universe_dry_run_selects_full_catalog_and_makes_zero_calls(
    tmp_path: Path,
) -> None:
    # Given an unsigned dry-run selecting the macro catalog
    code, stdout, stderr = _cli(tmp_path, extra=["--macro-universe"], dry_run=True, environ={})
    # Then the full frozen catalog prints in catalog order and nothing is called
    assert code == 0
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["provider_calls"] == 0
    assert payload["series_selection"] == "macro"
    assert payload["series_ids"] == list(MACRO_SERIES_IDS)
    assert payload["series_universe_sha256"] == MACRO_SERIES_UNIVERSE_SHA256
    assert payload["series_universe_sha256"] != payload["default_series_universe_sha256"]
    assert payload["default_series_universe_sha256"] == DEFAULT_SERIES_UNIVERSE_SHA256
    assert not (tmp_path / "raw").exists()
    assert not (tmp_path / "normalized").exists()


def test_explicit_macro_subset_hashes_by_catalog_order(tmp_path: Path) -> None:
    code, stdout, stderr = _cli(
        tmp_path,
        extra=["--series", "USREC", "--series", "CPIAUCSL", "--series", "DGS10"],
        dry_run=True,
        environ={},
    )
    assert code == 0
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["series_selection"] == "explicit"
    assert payload["series_ids"] == ["DGS10", "CPIAUCSL", "USREC"]
    assert payload["series_universe_sha256"] == series_universe_sha256(
        ("DGS10", "CPIAUCSL", "USREC")
    )
    assert payload["series_universe_sha256"] not in {
        DEFAULT_SERIES_UNIVERSE_SHA256,
        MACRO_SERIES_UNIVERSE_SHA256,
    }


def test_macro_universe_and_series_flags_are_mutually_exclusive(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exit_info:
        _cli(
            tmp_path,
            extra=["--macro-universe", "--series", "T10Y2Y"],
            dry_run=True,
            environ={},
        )
    assert exit_info.value.code == PRECONDITION_EXIT
    assert not (tmp_path / "raw").exists()


def test_macro_universe_live_run_still_needs_signed_authority(tmp_path: Path) -> None:
    # Given a macro selection without any standing authority or signature
    code, stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=["--macro-universe"],
        environ={"FRED_API_KEY": "SYNTH-FRED-KEY"},
    )
    # Then the signed gate refuses before any provider call, exactly as for the legacy four
    assert code == PRECONDITION_EXIT
    assert stdout == ""
    assert "--recurring-authority and --recurring-authority-signature are required" in stderr
    assert "no provider calls were attempted" in stderr
    assert not (tmp_path / "raw").exists()


def test_git_contained_destination_means_zero_calls(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    (repository / ".git").mkdir(parents=True)
    with pytest.raises(DestinationError, match="outside a Git repository"):
        run_preflight(
            registry_path=REPO_REGISTRY,
            recurring_authority_path=None,
            recurring_signature_path=None,
            environment={},
            max_calls=MAX_CALLS,
            raw_store_root=repository / "raw",
            dataset_root=tmp_path / "normalized",
            receipt_path=tmp_path / "receipts" / "run.receipt.json",
            series_ids=DEFAULT_SERIES_IDS,
            now=NOW,
            require_live_gates=False,
        )


def test_no_scheduler_or_live_fred_surface_exists() -> None:
    names = dir(cli_module)
    for forbidden in ("schedule", "cron", "daemon", "multpl"):
        assert not any(forbidden in name.casefold() for name in names)
    assert (Path(__file__).resolve().parents[2] / "scripts" / "collect_fred_alfred.py").is_file()


def test_injected_cli_transport_never_constructs_https_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_postgres: Engine
) -> None:
    # Given a live run fully admitted by standing authority
    fixture = _admit(tmp_path, monkeypatch)

    def boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("G-A CLI must not construct the live transport")

    monkeypatch.setattr(collector_module, "make_https_transport", boom)
    # When the run proceeds to the G-A wall
    code, _stdout, stderr = _cli(
        tmp_path,
        registry=CLEARED_REGISTRY,
        extra=list(_authority_argv(fixture)),
        environ=_authority_environ(
            fixture,
            database_url=clean_postgres.url.render_as_string(hide_password=False),
        ),
    )
    # Then it stops without ever constructing a transport
    assert code == 0
    assert stderr == ""


def test_explicit_run_recovery_reports_zero_new_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_postgres: Engine
) -> None:
    fixture = _admit(tmp_path, monkeypatch)
    extra = [*_authority_argv(fixture), "--run-id", "fred-explicit-recovery", "--series", "T10Y2Y"]
    environment = _authority_environ(
        fixture, database_url=clean_postgres.url.render_as_string(hide_password=False)
    )
    first = _cli(tmp_path, registry=CLEARED_REGISTRY, extra=extra, environ=environment)
    second = _cli(tmp_path, registry=CLEARED_REGISTRY, extra=extra, environ=environment)
    assert first[0] == second[0] == 0
    original, recovered = json.loads(first[1]), json.loads(second[1])
    assert original["provider_calls"] > 0
    assert recovered["provider_calls"] == 0
    assert recovered["run_calls_attempted"] == original["provider_calls"]
    assert recovered["recovered"] is True
    assert recovered["published_paths"] == original["published_paths"]
