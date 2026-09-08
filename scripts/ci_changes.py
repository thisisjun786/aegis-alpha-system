"""Select repository-owned CI jobs from a complete, NUL-delimited Git diff."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from pathlib import Path, PurePosixPath

JOBS = ("style", "types", "tests", "database", "package", "container", "docs", "security")
CODE_JOBS = {"style", "types", "tests", "package"}
REQUIRED_JOBS = frozenset({"docs", "security"})
ARCHIVE_PREFIXES = (".re0/", "dev-notes/archive/", "devlog/_fin/")
PROSE_FILES = {
    "AGENTS.md",
    "CLAUDE.md",
    "POLICY.md",
    "VERSIONING.md",
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".github/RELEASE_NOTES_TEMPLATE.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
}
APP_PREFIXES = (
    "src/aegis_alpha/application/",
    "src/aegis_alpha/modules/",
    "src/aegis_alpha/engine/",
    "tests/application/",
    "tests/engine/",
)
RETAINED_PREFIXES = tuple(
    f"{root}/{area}/"
    for root in ("src/aegis_alpha", "tests")
    for area in ("data", "collection", "identity", "metadata")
)
SHA = re.compile(r"[0-9a-f]{40}\Z")
REPO_ID = re.compile(r"[1-9][0-9]*\Z")


def git(*args: str, root: Path = Path()) -> bytes:
    executable = shutil.which("git")
    if executable is None:
        raise RuntimeError("git is unavailable")
    return subprocess.run(  # noqa: S603 -- fixed git argv; no shell
        [executable, *args],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=30,
    ).stdout


def repository_id(value: str, *, name: str) -> int:
    if not REPO_ID.fullmatch(value):
        raise ValueError(f"{name} must be a numeric repository ID")
    return int(value)


def changed_paths(base: str, head: str, *, root: Path = Path()) -> list[str]:
    """Include both rename/copy sides; reject incomplete records and unsafe paths."""
    if not SHA.fullmatch(base) or not SHA.fullmatch(head):
        raise ValueError("diff endpoints must be full commit SHAs")
    raw = git("diff", "--name-status", "-z", "--find-renames", base, head, "--", root=root)
    if not raw:
        return []
    fields = raw.decode("utf-8").split("\0")
    if fields.pop() != "":
        raise ValueError("unterminated Git diff")
    paths: set[str] = set()
    while fields:
        status = fields.pop(0)
        if not re.fullmatch(r"[ADMT]|[RC]\d{1,3}", status):
            raise ValueError(f"unsupported diff status: {status!r}")
        count = 2 if status[0] in "RC" else 1
        if len(fields) < count:
            raise ValueError("incomplete Git diff record")
        for _ in range(count):
            path = fields.pop(0)
            parts = PurePosixPath(path)
            if not path or parts.is_absolute() or ".." in parts.parts:
                raise ValueError("unsafe changed path")
            paths.add(path)
    return sorted(paths)


def is_prose(path: str) -> bool:
    if path.startswith(ARCHIVE_PREFIXES):
        return False
    if path in PROSE_FILES:
        return True
    if not path.endswith(".md") or "evidence" in PurePosixPath(path).parts:
        return False
    return path.startswith(("dev-notes/", "devlog/")) or (
        path.endswith("/AGENTS.md")
        and path.startswith(("src/", "tests/", "scripts/", "migrations/"))
    )


def select_jobs(paths: list[str], *, release: bool = False) -> tuple[dict[str, bool], list[str]]:
    selected: set[str] = set(REQUIRED_JOBS)
    if not paths:
        selected.update(JOBS)
    for path in paths:
        if is_prose(path):
            continue
        if path == "README.md":
            selected.add("package")
        elif path.startswith(APP_PREFIXES):
            selected.update(CODE_JOBS)
        elif path.startswith(RETAINED_PREFIXES) or path.startswith("migrations/"):
            selected.update(CODE_JOBS | {"database"})
        elif path.startswith("examples/"):
            selected.update(CODE_JOBS | {"container"})
        else:
            # Shared harness/configuration and unknown inputs have conservative coverage.
            selected.update(JOBS)
    if release:
        selected.update(JOBS)
    images = ["aas"] if "container" in selected else []
    return {job: job in selected for job in JOBS}, images


def selection(  # noqa: PLR0913 -- CI identity and provenance are one boundary
    base: str,
    head: str,
    candidate: str,
    *,
    release: bool = False,
    head_ref: str,
    base_repo_id: str,
    head_repo_id: str,
) -> dict[str, object]:
    if any(not SHA.fullmatch(value) for value in (base, head, candidate)):
        raise ValueError("candidate inputs must be full commit SHAs")
    if not head_ref or "\n" in head_ref or "\r" in head_ref:
        raise ValueError("head ref is required")
    base_id = repository_id(base_repo_id, name="base repository ID")
    head_id = repository_id(head_repo_id, name="head repository ID")
    if release and (head_ref != "dev" or base_id != head_id):
        raise ValueError("main requires a same-repository numeric ID promotion from dev")
    parents = git("rev-list", "--parents", "-n", "1", candidate).decode().split()
    if parents != [candidate, base, head]:
        raise ValueError("candidate parents do not match the PR base and head; refresh CI")
    if git("rev-parse", "HEAD").decode().strip() != candidate:
        raise ValueError("checkout differs from tested candidate")
    merge_base = git("merge-base", base, head).decode().strip()
    paths = changed_paths(merge_base, head)
    jobs, images = select_jobs(paths, release=release)
    return {
        "schema": 1,
        "mode": "release" if release else "dev",
        "base": base,
        "head": head,
        "merge_base": merge_base,
        "candidate": candidate,
        "tree": git("rev-parse", "HEAD^{tree}").decode().strip(),
        "jobs": jobs,
        "images": images,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--head-ref", required=True)
    parser.add_argument("--base-repo-id", required=True)
    parser.add_argument("--head-repo-id", required=True)
    parser.add_argument("--release", action="store_true")
    args = parser.parse_args()
    result = selection(
        args.base,
        args.head,
        args.candidate,
        release=args.release,
        head_ref=args.head_ref,
        base_repo_id=args.base_repo_id,
        head_repo_id=args.head_repo_id,
    )
    encoded = json.dumps(result, separators=(",", ":"))
    print(encoded)  # noqa: T201 -- CI provenance
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        jobs = result["jobs"]
        assert isinstance(jobs, dict)  # noqa: S101 -- constructed above
        lines = [f"selection={encoded}", f"images={json.dumps(result['images'])}"]
        lines.extend(f"{name}={str(value).lower()}" for name, value in jobs.items())
        with Path(output).open("a", encoding="utf-8") as stream:
            stream.write("\n".join(lines) + "\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write(f"### Tested CI candidate\n\n```json\n{encoded}\n```\n")


if __name__ == "__main__":
    main()
