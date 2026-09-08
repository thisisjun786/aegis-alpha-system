from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from aegis_alpha.application.provider_config import (
    CollectionConfig,
    ProviderProfile,
    load_collection_config,
    load_provider_environment,
)


def _profile(tmp_path: Path) -> ProviderProfile:
    return ProviderProfile(
        provider="fmp",
        enabled=True,
        credential_file=tmp_path / "credential",
        max_calls=10,
        mode="daily",
        options={},
    )


def test_credential_assignments_are_literal_and_old_database_is_excluded(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    assert profile.credential_file is not None
    sentinel = tmp_path / "must-not-exist"
    profile.credential_file.write_text(
        f'FMP_API_KEY="$(touch {sentinel})"\nAAS_DATABASE_URL=old-connection\nUNRELATED_KEY=other\n'
    )
    profile.credential_file.chmod(0o600)
    assert load_provider_environment(profile) == {"FMP_API_KEY": f"$(touch {sentinel})"}
    assert not sentinel.exists()
    profile.credential_file.chmod(0o644)
    with pytest.raises(ValueError, match="inaccessible"):
        load_provider_environment(profile)


@pytest.mark.parametrize(
    "body",
    [
        "FMP_API_KEY=x\nFMP_API_KEY=y",
        "export FMP_API_KEY=x",
        'FMP_API_KEY="unterminated',
        "FMP_API_KEY=bad\x00value",
    ],
)
def test_invalid_private_assignments_are_rejected_without_echo(tmp_path: Path, body: str) -> None:
    profile = _profile(tmp_path)
    assert profile.credential_file is not None
    profile.credential_file.write_text(body)
    profile.credential_file.chmod(0o600)
    with pytest.raises(ValueError, match="credential") as error:
        load_provider_environment(profile)
    assert body not in str(error.value)


@pytest.mark.parametrize("maximum", [0, -1, True, 1.5, "20"])
def test_call_budget_is_an_explicit_positive_integer(tmp_path: Path, maximum: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        replace(_profile(tmp_path), max_calls=maximum)  # type: ignore[arg-type]


def test_profiles_are_frozen_and_reject_unknown_or_unsafe_options(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    original = {"registry": str(tmp_path / "registry.json")}
    frozen = replace(profile, options=original)
    original["registry"] = str(tmp_path / "changed.json")
    assert frozen.options["registry"] == str(tmp_path / "registry.json")
    for options in (
        {"execute": "rm"},
        {"registry": "relative"},
        {"registry": "/outside/../escape"},
    ):
        with pytest.raises(ValueError, match="provider"):
            replace(profile, options=options)
    with pytest.raises(ValueError, match="duplicate provider"):
        CollectionConfig(tmp_path / "db.json", (profile, profile))


def test_profile_parser_rejects_duplicate_and_unknown_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    path.write_text('{"version":1,"version":1}')
    with pytest.raises(ValueError, match="duplicate"):
        load_collection_config(path)
    path.write_text(
        json.dumps({"version": True, "data_config": str(tmp_path / "db"), "providers": []})
    )
    with pytest.raises(ValueError, match="version"):
        load_collection_config(path)
    path.write_text(
        json.dumps(
            {"version": 1, "data_config": str(tmp_path / "db"), "providers": [], "command": "echo"}
        )
    )
    with pytest.raises(ValueError, match="unknown"):
        load_collection_config(path)
