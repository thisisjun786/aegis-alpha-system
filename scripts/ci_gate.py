"""Validate prerequisite results without installing or rerunning the application."""

from __future__ import annotations

import argparse
import json
import os
from typing import cast

from scripts.ci_changes import JOBS, REQUIRED_JOBS, SHA


def json_object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected a JSON object")
    if any(not isinstance(key, str) for key in value):
        raise TypeError("expected string JSON keys")
    # JSON boundary: the concrete container and every key have been validated.
    return cast("dict[str, object]", value)


def validate_selection(value: object, mode: str) -> dict[str, bool]:  # noqa: C901 -- one strict JSON boundary
    payload = json_object(value)
    if set(payload) != {
        "schema",
        "mode",
        "base",
        "head",
        "merge_base",
        "candidate",
        "tree",
        "jobs",
        "images",
    }:
        raise ValueError("missing or unexpected selection fields")
    if type(payload["schema"]) is not int or payload["schema"] != 1 or payload["mode"] != mode:
        raise ValueError("incorrect selection schema or gate mode")
    for key in ("base", "head", "merge_base", "candidate", "tree"):
        sha = payload[key]
        if not isinstance(sha, str) or not SHA.fullmatch(sha):
            raise ValueError(f"invalid {key}")
    raw_jobs = json_object(payload["jobs"])
    if set(raw_jobs) != set(JOBS):
        raise ValueError("missing or unexpected selected job")
    jobs: dict[str, bool] = {}
    for name, flag in raw_jobs.items():
        if not isinstance(flag, bool):
            raise TypeError("job selections must be booleans")
        jobs[name] = flag
    images = payload["images"]
    if images not in ([], ["aas"]):
        raise ValueError("invalid image selection")
    if bool(images) != jobs["container"] or any(jobs[name] is not True for name in REQUIRED_JOBS):
        raise ValueError("inconsistent image or required job selection")
    if mode == "release" and not all(jobs.values()):
        raise ValueError("release coverage is incomplete")
    return jobs


def aggregate(  # noqa: C901 -- keep fail-closed prerequisite checks together
    needs: object, *, mode: str, expected_shas: dict[str, str] | None = None
) -> None:
    if mode not in ("dev", "release"):
        raise ValueError("unknown gate mode")
    prerequisites = json_object(needs)
    if set(prerequisites) != {"changes", *JOBS}:
        raise ValueError("missing or unexpected prerequisite")
    changes = json_object(prerequisites["changes"])
    if changes.get("result") != "success":
        raise ValueError("change classification did not succeed")
    outputs = json_object(changes.get("outputs"))
    raw = outputs.get("selection")
    if not isinstance(raw, str):
        raise TypeError("selection output is absent")
    payload = json_object(json.loads(raw))
    jobs = validate_selection(payload, mode)
    if outputs.get("images") != json.dumps(payload["images"]):
        raise ValueError("inconsistent workflow image selection")
    for key, sha in (expected_shas or {}).items():
        if payload.get(key) != sha:
            raise ValueError(f"selection does not match event {key}")
    for job, selected in jobs.items():
        if outputs.get(job) != str(selected).lower():
            raise ValueError(f"inconsistent workflow selection output: {job}")
        result = json_object(prerequisites[job])
        expected = {"success"} if selected else {"success", "skipped"}
        if result.get("result") not in expected:
            raise ValueError(f"{job}: expected {sorted(expected)}, got {result.get('result')!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("dev", "release"), required=True)
    args = parser.parse_args()
    aggregate(
        json.loads(os.environ["CI_NEEDS"]),
        mode=args.mode,
        expected_shas={
            "base": os.environ["BASE_SHA"],
            "head": os.environ["HEAD_SHA"],
            "candidate": os.environ["CANDIDATE_SHA"],
        },
    )
    print(f"{args.mode}-gate: all selected jobs succeeded")  # noqa: T201 -- CI result


if __name__ == "__main__":
    main()
