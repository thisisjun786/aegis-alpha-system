from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from data.test_qveris_acquisition import FakeQveris, eod_job

from aegis_alpha.application.daily_collection import run_daily
from aegis_alpha.application.provider_config import CollectionConfig, ProviderProfile
from aegis_alpha.application.qveris_collection import run_qveris_profile
from aegis_alpha.data.serialization import canonical_json_bytes


def profile(tmp_path: Path) -> ProviderProfile:
    jobs = tmp_path / "jobs.json"
    jobs.write_bytes(canonical_json_bytes({"schema_version": 1, "jobs": [eod_job().document()]}))
    return ProviderProfile(
        provider="qveris",
        enabled=True,
        credential_file=None,
        max_calls=1,
        mode="daily",
        options={
            "jobs": str(jobs),
            "jobs_sha256": hashlib.sha256(jobs.read_bytes()).hexdigest(),
            "raw_store_root": str(tmp_path / "raw"),
            "max_credits": "3",
            "timeout_seconds": "30",
        },
    )


def test_daily_native_raw_replay_has_no_second_call(tmp_path: Path) -> None:
    selected = profile(tmp_path)
    config = CollectionConfig(tmp_path / "absent-legacy-config", (selected,))
    client = FakeQveris()

    def runner(
        _config: CollectionConfig, item: ProviderProfile, _run_id: str | None
    ) -> dict[str, object]:
        return run_qveris_profile(item, client=client)

    def clock() -> datetime:
        return datetime(2026, 9, 8, tzinfo=UTC)

    first = run_daily(config, tmp_path / "journal", runner=runner, clock=clock)
    assert first["status"] == "succeeded"
    assert client.execute_count == 1
    previous = len(client.calls)
    second = run_daily(config, tmp_path / "journal", runner=runner, clock=clock)
    assert second["status"] == "succeeded"
    assert len(client.calls) == previous
    direct = run_qveris_profile(selected, client=client)
    payload = direct["result"]
    assert isinstance(payload, dict)
    assert payload["provider_calls"] == 0
    assert payload["native_import_completed"] is False


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("max_credits", "NaN"),
        ("max_credits", "-1"),
        ("timeout_seconds", "0"),
        ("timeout_seconds", "Infinity"),
    ],
)
def test_profile_rejects_invalid_budget_before_credential_read(
    tmp_path: Path, key: str, value: str
) -> None:
    selected = profile(tmp_path)
    with pytest.raises(ValueError, match="Qveris"):
        ProviderProfile(
            provider="qveris",
            enabled=True,
            credential_file=tmp_path / "absent-key",
            max_calls=1,
            mode="daily",
            options={**selected.options, key: value},
        )


def test_job_manifest_changed_before_credentials_or_network(tmp_path: Path) -> None:
    selected = profile(tmp_path)
    Path(str(selected.options["jobs"])).write_text("{}")
    key = tmp_path / "synthetic-key"
    key.write_text("synthetic-key")
    key.chmod(0o644)
    selected = replace(selected, credential_file=key)
    report = run_qveris_profile(selected)
    assert report["error_type"] == "ValueError"
    assert report["execution_started"] is False
    payload = report["result"]
    assert isinstance(payload, dict)
    assert payload["http_requests"] == 0


def test_bad_key_returns_safe_failure_envelope(tmp_path: Path) -> None:
    selected = replace(profile(tmp_path), credential_file=tmp_path / "missing-key")
    report = run_qveris_profile(selected)
    assert report["status"] == "failed"
    assert report["error_type"] == "QverisKeyFileError"
    assert report["execution_started"] is False
