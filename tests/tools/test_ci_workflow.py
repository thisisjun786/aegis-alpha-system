"""Guard required-check wiring; actionlint separately validates the complete YAML."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def workflow_jobs() -> dict[str, str]:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    sections = re.split(r"(?m)^  ([a-z][a-z-]*):\n", workflow.split("jobs:\n", 1)[1])
    return dict(zip(sections[1::2], sections[2::2], strict=True))


def test_required_gate_aggregates_all_jobs() -> None:
    body = workflow_jobs()["gate"]
    assert "name: ${{ github.base_ref == 'main' && 'release-gate' || 'dev-gate' }}" in body
    assert re.findall(r"(?m)^    if: (.*)$", body) == ["always()"]
    assert re.findall(r"(?m)^    needs: (.*)$", body) == [
        "[changes, style, types, tests, database, package, container, docs, security]",
    ]
    assert "CI_NEEDS: ${{ toJSON(needs) }}" in body
    assert "scripts.ci_gate --mode" in body
    assert "setup-python" not in body
    assert "scripts/verify" not in body


def test_independent_jobs_and_fixed_candidate() -> None:
    jobs = workflow_jobs()
    for job in ("style", "types", "tests", "database", "package", "container", "docs", "security"):
        assert re.findall(r"(?m)^    needs: (.*)$", jobs[job]) == ["changes"]
        assert "ref: ${{ github.sha }}" in jobs[job]
        assert "persist-credentials: false" in jobs[job]
        assert "head.repo.full_name" not in jobs[job]
    assert "strategy:" not in jobs["container"]
    assert "matrix:" not in jobs["container"]
    assert "run: ./scripts/verify-lane-container aas" in jobs["container"]
    assert "run: ./scripts/verify-lane-database" in jobs["database"]


def test_events_and_permissions_do_not_bypass_required_checks() -> None:
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "branches:" not in text
    assert "types: [opened, reopened, synchronize, edited]" in text
    assert "self-hosted" not in text
    assert "contents: read" in text
    assert "pull_request_target" not in text
    assert "paths:" not in text
    assert "paths-ignore:" not in text
    assert "continue-on-error" not in text
    assert "secrets:" not in text
    assert "LDG/" not in text


def test_only_same_numeric_repository_dev_can_promote() -> None:
    body = workflow_jobs()["changes"]
    assert "github.base_ref == 'main'" in body
    assert "github.head_ref != 'dev'" in body
    assert (
        "github.event.pull_request.head.repo.id != github.event.pull_request.base.repo.id" in body
    )
    assert "scripts/verify-secrets" in workflow_jobs()["security"]
    assert "scripts.ci_security" in workflow_jobs()["security"]
    assert "scripts.ci_public" in workflow_jobs()["security"]


def test_dependabot_tracks_only_actions_and_root_python_lock() -> None:
    text = (ROOT / ".github/dependabot.yml").read_text()
    assert re.findall(r"(?m)^  - package-ecosystem: (.*)$", text) == ["github-actions", "uv"]
    assert re.findall(r"(?m)^    directory: (.*)$", text) == ["/", "/"]
    assert re.findall(r"(?m)^    target-branch: (.*)$", text) == ["dev", "dev"]
