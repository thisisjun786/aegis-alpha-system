from __future__ import annotations

import json
from typing import NotRequired, TypedDict

import pytest

from scripts.ci_gate import aggregate


class Need(TypedDict):
    result: str | None
    outputs: NotRequired[dict[str, str]]


def needs_fixture(*, release: bool = False) -> dict[str, Need]:
    jobs = {
        "style": release,
        "types": release,
        "tests": release,
        "database": release,
        "package": release,
        "container": release,
        "docs": True,
        "security": True,
    }
    selection = {
        "schema": 1,
        "mode": "release" if release else "dev",
        "base": "a" * 40,
        "head": "b" * 40,
        "merge_base": "c" * 40,
        "candidate": "d" * 40,
        "tree": "e" * 40,
        "jobs": jobs,
        "images": ["aas"] if release else [],
    }
    outputs = {name: str(flag).lower() for name, flag in jobs.items()}
    outputs["selection"] = json.dumps(selection)
    outputs["images"] = json.dumps(selection["images"])
    return {
        "changes": {"result": "success", "outputs": outputs},
        **{name: {"result": "success" if flag else "skipped"} for name, flag in jobs.items()},
    }


def test_docs_and_complete_release_pass() -> None:
    aggregate(needs_fixture(), mode="dev")
    aggregate(needs_fixture(release=True), mode="release")


@pytest.mark.parametrize("state", ["failure", "cancelled", "skipped", None, "neutral"])
@pytest.mark.parametrize(
    "job",
    ["changes", "style", "types", "tests", "database", "package", "container", "docs", "security"],
)
def test_required_job_must_succeed(job: str, state: str | None) -> None:
    needs = needs_fixture(release=True)
    needs[job]["result"] = state
    with pytest.raises(ValueError, match=r"classification|expected"):
        aggregate(needs, mode="release")


@pytest.mark.parametrize(
    "job",
    ["changes", "style", "types", "tests", "database", "package", "container", "docs", "security"],
)
def test_missing_result_fails(job: str) -> None:
    needs = needs_fixture()
    del needs[job]
    with pytest.raises(ValueError, match="prerequisite"):
        aggregate(needs, mode="dev")


@pytest.mark.parametrize("raw", ["", "null", "[]", "{}", "not json"])
def test_malformed_selection_fails(raw: str) -> None:
    needs = needs_fixture()
    outputs = needs["changes"]["outputs"]
    assert isinstance(outputs, dict)
    outputs["selection"] = raw
    with pytest.raises((ValueError, TypeError), match=r"JSON|selection|Expecting"):
        aggregate(needs, mode="dev")


def test_failed_classifier_empty_outputs_cannot_pass() -> None:
    needs = needs_fixture()
    needs["changes"] = {"result": "failure", "outputs": {}}
    with pytest.raises(ValueError, match="classification"):
        aggregate(needs, mode="dev")


def test_boolean_string_and_workflow_output_mismatch_fail() -> None:
    needs = needs_fixture()
    outputs = needs["changes"]["outputs"]
    assert isinstance(outputs, dict)
    outputs["tests"] = "true"
    with pytest.raises(ValueError, match="workflow selection"):
        aggregate(needs, mode="dev")
    outputs["tests"] = "false"
    value = json.loads(outputs["selection"])
    value["jobs"]["tests"] = "false"
    outputs["selection"] = json.dumps(value)
    with pytest.raises(TypeError, match="booleans"):
        aggregate(needs, mode="dev")


def test_dev_results_cannot_certify_release() -> None:
    with pytest.raises(ValueError, match="mode"):
        aggregate(needs_fixture(), mode="release")


def test_image_selection_and_event_identity_must_match() -> None:
    needs = needs_fixture(release=True)
    needs["changes"]["outputs"]["images"] = "[]"
    with pytest.raises(ValueError, match="image selection"):
        aggregate(needs, mode="release")
    with pytest.raises(ValueError, match="event base"):
        aggregate(needs_fixture(), mode="dev", expected_shas={"base": "f" * 40})


@pytest.mark.parametrize("images", [["vt"], ["aas", "vt"], ["aas", "aas"], "aas", None, {}])
def test_unsupported_or_malformed_image_selection_fails(images: object) -> None:
    needs = needs_fixture(release=True)
    outputs = needs["changes"]["outputs"]
    value = json.loads(outputs["selection"])
    value["images"] = images
    outputs["selection"] = json.dumps(value)
    outputs["images"] = json.dumps(images)
    with pytest.raises(ValueError, match="invalid image selection"):
        aggregate(needs, mode="release")


@pytest.mark.parametrize("job", ["docs", "security"])
def test_required_job_cannot_be_excluded_by_classifier(job: str) -> None:
    needs = needs_fixture()
    outputs = needs["changes"]["outputs"]
    value = json.loads(outputs["selection"])
    value["jobs"][job] = False
    outputs["selection"] = json.dumps(value)
    outputs[job] = "false"
    needs[job]["result"] = "skipped"
    with pytest.raises(ValueError, match="required job selection"):
        aggregate(needs, mode="dev")


@pytest.mark.parametrize("job", ["database", "container"])
def test_release_cannot_exclude_database_or_image(job: str) -> None:
    needs = needs_fixture(release=True)
    outputs = needs["changes"]["outputs"]
    value = json.loads(outputs["selection"])
    value["jobs"][job] = False
    if job == "container":
        value["images"] = []
        outputs["images"] = "[]"
    outputs["selection"] = json.dumps(value)
    outputs[job] = "false"
    needs[job]["result"] = "skipped"
    with pytest.raises(ValueError, match="release coverage is incomplete"):
        aggregate(needs, mode="release")


@pytest.mark.parametrize("state", ["failure", "cancelled", "neutral", None])
def test_excluded_database_job_failure_is_not_a_skip(state: str | None) -> None:
    needs = needs_fixture()
    needs["database"]["result"] = state
    with pytest.raises(ValueError, match="database: expected"):
        aggregate(needs, mode="dev")
