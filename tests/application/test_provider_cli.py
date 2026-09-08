from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from aegis_alpha.application import provider_cli
from aegis_alpha.application.cli import main
from aegis_alpha.application.provider_cli import (
    collection_plan,
    recover_collection_catalog,
    run_collection_profile,
)
from aegis_alpha.application.provider_config import CollectionConfig, ProviderProfile
from aegis_alpha.data import fmp_catalog, sec_collector_cli


def _configured(tmp_path: Path) -> tuple[CollectionConfig, ProviderProfile]:
    credential = tmp_path / "provider.env"
    credential.write_text(
        "SEC_USER_AGENT=fixture-contact@example.invalid\nAAS_DATABASE_URL=old-secret\n"
    )
    credential.chmod(0o600)
    secret = tmp_path / "database-url"
    secret.write_text("postgresql+psycopg://fixture:new-secret@localhost/aas")
    secret.chmod(0o600)
    data = tmp_path / "data.json"
    data.write_text(
        json.dumps({"version": 1, "database_url_file": str(secret), "dataset_roots": []})
    )
    for filename in ("registry.json", "identity.json"):
        (tmp_path / filename).write_text("{}")
    profile = ProviderProfile(
        provider="sec",
        enabled=True,
        credential_file=credential,
        max_calls=7,
        mode="incremental",
        options={
            "registry": str(tmp_path / "registry.json"),
            "as_of": "2026-09-05T00:00:00+00:00",
            "identity_snapshot": str(tmp_path / "identity.json"),
            "raw_store_root": str(tmp_path / "raw"),
            "dataset_root": str(tmp_path / "normalized"),
            "receipt_path": str(tmp_path / "receipt.json"),
            "instrument_ids": ("fixture-instrument",),
        },
    )
    return CollectionConfig(data, (profile,)), profile


def test_plan_is_zero_call_unverified_and_does_not_create_outputs(tmp_path: Path) -> None:
    _, profile = _configured(tmp_path)
    result = collection_plan(profile)
    assert result["status"] == "configured_unverified"
    assert result["provider_calls"] == 0
    assert result["authority_verified"] is False
    assert not (tmp_path / "raw").exists()
    assert not (tmp_path / "normalized").exists()


def test_dispatch_uses_new_database_and_only_selected_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, profile = _configured(tmp_path)
    monkeypatch.setenv("UNRELATED_HOST_SECRET", "must-not-inherit")
    captured = {}

    def owner_main(
        argv: list[str], *, environ: dict[str, str], stdout: io.StringIO, stderr: io.StringIO
    ) -> int:
        captured.update({"arguments": argv, "environment": environ})
        stdout.write(json.dumps({"calls_attempted": 3}))
        stderr.write(environ["SEC_USER_AGENT"] + " " + environ["AAS_DATABASE_URL"])
        return 0

    monkeypatch.setattr(sec_collector_cli, "main", owner_main)
    result = run_collection_profile(config, profile)
    assert result["exit_code"] == 0
    assert result["status"] == "succeeded"
    assert result["result"] == {"calls_attempted": 3}
    assert captured["arguments"][:5] == ["--max-calls", "7", "--mode", "incremental", "--live"]
    assert captured["environment"]["AAS_DATABASE_URL"].endswith("new-secret@localhost/aas")
    assert "UNRELATED_HOST_SECRET" not in captured["environment"]
    assert result["diagnostic"] == "[redacted] [redacted]"


def test_provider_exception_never_becomes_zero_call_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, profile = _configured(tmp_path)

    def failed(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("credential-bearing-failure")

    monkeypatch.setattr(sec_collector_cli, "main", failed)
    result = run_collection_profile(config, profile)
    assert result["exit_code"] == 1
    assert result["provider_calls"] is None
    assert "credential-bearing-failure" not in json.dumps(result)


def test_blocked_provider_stops_before_database_or_dispatch(tmp_path: Path) -> None:
    profile = ProviderProfile(
        provider="fred_alfred",
        enabled=True,
        credential_file=None,
        max_calls=10,
        mode="incremental",
        options={},
    )
    result = run_collection_profile(CollectionConfig(tmp_path / "missing-db", (profile,)), profile)
    assert result["exit_code"] == 2  # noqa: PLR2004 -- CLI precondition exit contract
    assert result["execution_started"] is False
    assert result["provider_calls"] == 0
    assert isinstance(result["reasons"], list)
    assert "missing_option:recurring_authority" in result["reasons"]


def test_inventory_does_not_claim_registry_domain_coverage(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["providers"]) == 0
    result = json.loads(capsys.readouterr().out)
    providers = {item["provider"]: item for item in result["providers"]}
    assert set(providers) == {"fmp", "norgate", "fred_alfred", "sec", "finimpulse"}
    assert "transcript" not in providers["fmp"]["endpoints"]
    assert "13f_index" in providers["sec"]["endpoints"]
    assert all(item["live_verified"] is False for item in providers.values())


def test_json_escaped_secrets_in_keys_values_and_diagnostics_are_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, profile = _configured(tmp_path)
    secret = 'fixture-"quoted"\\value-한글'  # noqa: S105 -- synthetic redaction probe
    assert profile.credential_file is not None
    profile.credential_file.write_text("SEC_USER_AGENT='" + secret + "'\n")

    def owner_main(
        _argv: list[str], *, environ: dict[str, str], stdout: io.StringIO, stderr: io.StringIO
    ) -> int:
        value = environ["SEC_USER_AGENT"]
        stdout.write(json.dumps({value: [{"nested": value}]}))
        stderr.write("diagnostic: " + json.dumps(value))
        return 0

    monkeypatch.setattr(sec_collector_cli, "main", owner_main)
    result = run_collection_profile(config, profile)
    assert result["result"] == {"[redacted]": [{"nested": "[redacted]"}]}
    assert result["diagnostic"] == 'diagnostic: "[redacted]"'
    assert secret not in json.dumps(result, ensure_ascii=False)


def test_distinct_runs_have_distinct_receipts_and_explicit_identity_replays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, profile = _configured(tmp_path)
    arguments = []

    def owner_main(argv: list[str], **_kwargs: object) -> int:
        arguments.append(argv)
        return 0

    monkeypatch.setattr(sec_collector_cli, "main", owner_main)
    first = run_collection_profile(config, profile)
    second = run_collection_profile(config, profile)
    explicit = run_collection_profile(config, profile, run_id="sec-daily-2026-09-05")
    repeated = run_collection_profile(config, profile, run_id="sec-daily-2026-09-05")
    assert first["run_id"] != second["run_id"]
    assert explicit["run_id"] == repeated["run_id"]
    paths = [argv[argv.index("--receipt-path") + 1] for argv in arguments]
    assert paths[0] != paths[1]
    assert paths[-1] == paths[-2]
    assert Path(paths[-1]).name == "receipt.sec-daily-2026-09-05.json"
    assert not (tmp_path / "receipt.json").exists()


def test_local_recovery_refuses_missing_scope_before_opening_database(tmp_path: Path) -> None:
    profile = ProviderProfile(
        provider="fmp", enabled=False, credential_file=None, max_calls=1, mode="daily", options={}
    )
    config = CollectionConfig(tmp_path / "missing-data-config", (profile,))
    with pytest.raises(ValueError, match="explicit raw_store_root"):
        recover_collection_catalog(config, profile, 1)
    with pytest.raises(ValueError, match="limit"):
        recover_collection_catalog(config, profile, 0)


@pytest.mark.parametrize(("status", "code"), [("catalog_complete", 0), ("catalog_pending", 1)])
def test_local_recovery_propagates_incomplete_result_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str, code: int
) -> None:
    config, _ = _configured(tmp_path)
    profile = ProviderProfile(
        provider="fmp",
        enabled=False,
        credential_file=tmp_path / "absent-key",
        max_calls=1,
        mode="daily",
        options={
            "raw_store_root": str(tmp_path / "raw"),
            "dataset_root": str(tmp_path / "normalized"),
        },
    )

    def refuse_credentials(_profile: ProviderProfile) -> dict[str, str]:
        pytest.fail("local catalog recovery must not load provider credentials")

    def recover(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {"status": status, "provider_calls": 0}

    monkeypatch.setattr(provider_cli, "load_provider_environment", refuse_credentials)
    monkeypatch.setattr(fmp_catalog, "recover_completed_collections", recover)
    result = recover_collection_catalog(config, profile, 1)
    assert result["status"] == status
    assert result["exit_code"] == code
    assert result["provider_calls"] == 0
