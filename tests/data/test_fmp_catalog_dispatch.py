"""Application collection succeeds only after FMP catalog commit."""

from __future__ import annotations

# ruff: noqa: F811 -- imported pytest fixtures are consumed by their parameter names
import io
import json
from pathlib import Path
from typing import TYPE_CHECKING, cast

from test_fmp_catalog import catalog_counts
from test_fmp_daily_budget import NOW, SUCCESS_CALLS, DailyHarness, daily_harness  # noqa: F401

from aegis_alpha.application.provider_cli import run_collection_profile
from aegis_alpha.application.provider_config import CollectionConfig, ProviderProfile
from aegis_alpha.data import fmp_daily_cli

if TYPE_CHECKING:
    import pytest


def test_application_success_includes_real_catalog_commit(
    daily_harness: DailyHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = daily_harness.root
    credential = root / "provider.env"
    credential.write_text("FMP_API_KEY=synthetic-budget-credential\n")
    credential.chmod(0o600)
    database = root / "database-url"
    database.write_text(daily_harness.engine.url.render_as_string(hide_password=False))
    database.chmod(0o600)
    data = root / "data.json"
    data.write_text(
        json.dumps({"version": 1, "database_url_file": str(database), "dataset_roots": []})
    )
    options = {
        name: str(root / f"{name}.json")
        for name in (
            "registry",
            "storage_notification",
            "tier",
            "recurring_authority",
            "recurring_authority_signature",
        )
    }
    for value in options.values():
        Path(value).write_text("{}")
    profile = ProviderProfile(
        provider="fmp",
        enabled=True,
        credential_file=credential,
        max_calls=SUCCESS_CALLS,
        mode="daily",
        options=options,
    )
    saved_main = fmp_daily_cli.main

    def injected_main(
        argv: list[str],
        *,
        environ: dict[str, str],
        stdout: io.StringIO,
        stderr: io.StringIO,
    ) -> int:
        return saved_main(
            argv,
            environ=environ,
            stdout=stdout,
            stderr=stderr,
            now=NOW,
            transport=daily_harness.transport,
            monotonic=lambda: 0.0,
            sleep=lambda _seconds: None,
            approval_clock=lambda: NOW,
            wall_clock=lambda: NOW,
        )

    monkeypatch.setattr(fmp_daily_cli, "main", injected_main)
    report = run_collection_profile(CollectionConfig(data, (profile,)), profile)
    assert report["status"] == "succeeded", report
    result = cast("dict[str, object]", report["result"])
    assert result["invocation_calls_attempted"] == SUCCESS_CALLS
    counts = catalog_counts(daily_harness.engine)
    assert counts[:3] == (1, 1, 4)
    assert counts[-1] == 1
