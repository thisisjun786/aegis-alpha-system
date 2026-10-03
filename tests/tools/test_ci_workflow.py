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


def test_database_free_tests_run_as_complete_isolated_shards() -> None:
    body = workflow_jobs()["tests"]
    assert "fail-fast: false" in body
    shards = re.findall(r"(?m)^        shard: \[(.*)\]$", body)
    assert len(shards) == 1
    assert [int(value) for value in shards[0].split(",")] == list(
        range(1, len(shards[0].split(",")) + 1)
    )
    assert "AAS_TEST_SHARD: ${{ matrix.shard }}/${{ strategy.job-total }}" in body
    assert "run: ./scripts/verify-lane-test" in body
    assert "mount -t tmpfs" in body
    assert "strategy:" not in workflow_jobs()["gate"]


def test_shards_publish_their_measured_durations() -> None:
    body = workflow_jobs()["tests"]
    durations = "${{ runner.temp }}/durations-${{ matrix.shard }}.json"
    assert f"PYTEST_ADDOPTS: --test-durations-out={durations}" in body
    upload = body.split("- name: Publish measured test durations\n", 1)[1]
    assert re.findall(r"(?m)^        if: (.*)$", upload) == ["always()"]
    assert re.search(r"(?m)^        uses: actions/upload-artifact@[0-9a-f]{40} # v", upload)
    assert "name: test-durations-${{ matrix.shard }}" in upload
    assert f"path: {durations}" in upload
    assert re.search(r"(?m)^          retention-days: [0-9]+$", upload)


def test_body_edits_run_in_their_own_group_without_cancelling_the_push_run() -> None:
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    concurrency = text.split("\nconcurrency:\n", 1)[1].split("\n\n", 1)[0]
    assert re.findall(r"(?m)^  group: (.*)$", concurrency) == [
        (
            "aas-ci-${{ github.base_ref }}-${{ github.event.pull_request.number }}"
            "${{ github.event.action == 'edited' && !github.event.changes.base && '-edit' || '' }}"
        )
    ]
    assert "  cancel-in-progress: true" in concurrency
    # An edit run still runs every job: a skipped gate would satisfy the required check.
    for job, body in workflow_jobs().items():
        assert "github.event.action" not in body, job
        assert "github.event.changes" not in body, job


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
