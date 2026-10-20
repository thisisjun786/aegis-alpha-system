"""Final run admission rechecks row bytes and specialized auxiliary evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.storage.paths import load_paths
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_backtest_prepare import (  # noqa: F401 -- shared fixtures
    alternate_prices,
    copied_request,
    price_rows,
    proxy_recipe,
    publication_clock,
    second_recipe,
    stored_template,
)
from tests.application.test_research_prices import _outcomes
from tests.application.test_run_boundaries import Document, RunCase
from tests.application.test_run_research import (  # noqa: F401 -- shared fixtures
    _as_sleeve_run,
    _installed,
    _write,
    composed,
    compute_environment,
)
from tests.application.test_strict_head_inputs import headed


def _assert_outcome(
    case: RunCase, surface: str, capsys: pytest.CaptureFixture[str], *, mutate: bool
) -> None:
    if surface == "api":
        if mutate:
            with pytest.raises(ValueError, match=r"hash|SHA-256|transform"):
                case.execute()
        else:
            assert case.execute()["run"]["status"] == "SUCCESS"
    else:
        assert main(case.arguments()) == int(mutate)
        captured = capsys.readouterr()
        if mutate:
            assert captured.out == ""
            error = json.loads(captured.err)
            assert any(word in error["error"] for word in ("hash", "SHA-256", "transform"))
            assert "ended FAILED" in error["notes"][0]
        else:
            assert captured.err == ""
            assert json.loads(captured.out)["run"]["status"] == "SUCCESS"
    # Reopening proves final admission released its locks even on refusal.
    with open_workspace(case.home) as workspace:
        assert [row[0] for row in workspace.state.execute("SELECT status FROM runs")] == [
            "FAILED" if mutate else "SUCCESS"
        ]
        assert workspace.market.execute("SELECT count(*) FROM result_commits").fetchall() == [
            (int(not mutate),)
        ]


@pytest.mark.parametrize("route", ["strict", "declared"])
@pytest.mark.parametrize("surface", ["api", "cli"])
@pytest.mark.parametrize("mutate", [False, True])
def test_changed_head_rows_must_not_commit(  # noqa: PLR0913, PLR0917 -- routes, surfaces and negative/control cases
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    route: str,
    surface: str,
    *,
    mutate: bool,
) -> None:
    if route == "declared":
        home, base, offense, _defense = request.getfixturevalue("composed")
        body = _outcomes(home, _as_sleeve_run(base, offense))
        generation = body["prices"]["pins"][0]["generation_id"]
    else:
        body = request.getfixturevalue("copied_request")
        home = tmp_path / "home"
        with open_workspace(home, writable=True) as workspace:
            headed(workspace, body, recorded=True)
        generation = next(
            ref["pin"]["pins"][0]["generation_id"]
            for ref in body["refs"]
            if ref["ref_kind"] == "heads" and ref["pin"]["domain"] == "prices"
        )
    case = RunCase(home, _write(tmp_path, "head-run.json", body), route)
    _installed(home)
    genuine = case.module.run_document

    def changed(raw: bytes, digest: str) -> Document:
        result = genuine(raw, digest)
        if mutate:
            with open_workspace(home, writable=True) as workspace:
                before = workspace.market.execute("SELECT * FROM market_generations").fetchall()
                count = workspace.market.execute("SELECT count(*) FROM prices").fetchall()
                workspace.market.execute(
                    "UPDATE prices SET close=close+1 WHERE generation_id=? "
                    "AND instrument_id='ASSET_B'",
                    [generation],
                )
                assert (
                    workspace.market.execute("SELECT * FROM market_generations").fetchall()
                    == before
                )
                assert workspace.market.execute("SELECT count(*) FROM prices").fetchall() == count
        return result

    monkeypatch.setattr(case.module, "run_document", changed)
    _assert_outcome(case, surface, capsys, mutate=mutate)


def _transform_run(request: pytest.FixtureRequest, root: Path, kind: str) -> tuple[RunCase, Path]:
    body = request.getfixturevalue("copied_request")
    home = root / "home"
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        if kind == "proxy":
            proxy_recipe(workspace, root, body)
            dataset = "proxy"
        else:
            second_recipe(workspace, root, body)
            # Keep the original signal generation only inside the derived definition.
            # A direct price binding must not accidentally catch this mutation for us.
            alternate_prices(workspace, root, body, "signal_prices", price_rows(signal=True))
            assert all(binding["ref_id"] != "signal" for binding in body["bindings"])
            dataset = "signal"
        digest = workspace.state.execute(
            "SELECT transform_hash FROM dataset_versions WHERE dataset_id=?", (dataset,)
        ).fetchone()[0]
    _installed(home)
    return (
        RunCase(home, _write(root, "auxiliary-run.json", body), "strict"),
        load_paths(home).raw / digest[:2] / digest,
    )


@pytest.mark.parametrize("surface", ["api", "cli"])
@pytest.mark.parametrize("mutate", [False, True])
@pytest.mark.parametrize("kind", ["proxy", "derived"])
def test_changed_proxy_transform_must_not_commit(  # noqa: PLR0913, PLR0917 -- both specialized inputs and CLI/API controls
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    surface: str,
    kind: str,
    *,
    mutate: bool,
) -> None:
    case, retained = _transform_run(request, tmp_path, kind)
    genuine = case.module.run_document

    def changed(raw: bytes, digest: str) -> Document:
        response = genuine(raw, digest)
        if mutate:
            retained.chmod(0o600)
            retained.write_bytes(b"{}")
        return response

    monkeypatch.setattr(case.module, "run_document", changed)
    _assert_outcome(case, surface, capsys, mutate=mutate)
