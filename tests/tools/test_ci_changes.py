from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import ci_changes


@pytest.mark.parametrize(
    ("paths", "expected", "images"),
    [
        (["POLICY.md", "devlog/_plan/260907_ci/010_plan.md"], {"docs"}, []),
        (["README.md"], {"docs", "package"}, []),
        (
            ["src/aegis_alpha/application/cli.py"],
            {"docs", "style", "types", "tests", "package"},
            [],
        ),
        (
            ["tests/data/test_api.py", "AGENTS.md"],
            {"docs", "style", "types", "tests", "package", "database"},
            [],
        ),
        (
            ["examples/request.json"],
            {"docs", "style", "types", "tests", "package", "container"},
            ["aas"],
        ),
        (
            ["new-subsystem/schema.sql"],
            {"docs", "style", "types", "tests", "package", "container", "database"},
            ["aas"],
        ),
        (
            ["dev-notes/archive/evidence/input.json"],
            {"docs", "style", "types", "tests", "package", "container", "database"},
            ["aas"],
        ),
        (
            ["dev-notes/archive/evidence/contract.md"],
            {"docs", "style", "types", "tests", "package", "container", "database"},
            ["aas"],
        ),
        (
            ["scripts/verify", "POLICY.md"],
            {"docs", "style", "types", "tests", "package", "container", "database"},
            ["aas"],
        ),
        (
            [],
            {"docs", "style", "types", "tests", "package", "container", "database"},
            ["aas"],
        ),
    ],
)
def test_selection(paths: list[str], expected: set[str], images: list[str]) -> None:
    jobs, selected_images = ci_changes.select_jobs(paths)
    assert {job for job, flag in jobs.items() if flag} == expected | {"security"}
    assert selected_images == images


def test_release_never_uses_docs_shortcut() -> None:
    jobs, images = ci_changes.select_jobs(["POLICY.md"], release=True)
    assert all(jobs.values())
    assert images == ["aas"]
    _, shared_images = ci_changes.select_jobs(["src/aegis_alpha/data/contracts.py"], release=True)
    assert shared_images == ["aas"]


def test_archive_edits_do_not_receive_prose_only_coverage() -> None:
    jobs, images = ci_changes.select_jobs(["dev-notes/archive/old.md"])
    assert all(jobs.values())
    assert images == ["aas"]


def commit(root: Path) -> str:
    ci_changes.git("add", ".", root=root)
    ci_changes.git(
        "-c",
        "user.name=CI",
        "-c",
        "user.email=ci@example.invalid",
        "commit",
        "-qm",
        "fixture",
        root=root,
    )
    return ci_changes.git("rev-parse", "HEAD", root=root).decode().strip()


def test_rename_delete_and_newline_paths_are_preserved(tmp_path: Path) -> None:
    ci_changes.git("init", "-q", root=tmp_path)
    (tmp_path / "schema.py").write_text("schema = 1\n")
    (tmp_path / "deleted.py").write_text("old = 1\n")
    base = commit(tmp_path)
    (tmp_path / "schema.py").rename(tmp_path / "POLICY.md")
    (tmp_path / "deleted.py").unlink()
    (tmp_path / "new\nfile.py").write_text("new = 1\n")
    head = commit(tmp_path)
    assert ci_changes.changed_paths(base, head, root=tmp_path) == [
        "POLICY.md",
        "deleted.py",
        "new\nfile.py",
        "schema.py",
    ]


@pytest.mark.parametrize(
    "raw", [b"M\0", b"R100\0one\0", b"M\0../outside\0", b"U\0path\0", b"M\0file"]
)
def test_malformed_diff_fails(monkeypatch: pytest.MonkeyPatch, raw: bytes) -> None:
    monkeypatch.setattr(ci_changes, "git", lambda *_args, **_kwargs: raw)
    with pytest.raises(ValueError, match=r"diff|path"):
        ci_changes.changed_paths("a" * 40, "b" * 40)


def test_missing_history_fails(tmp_path: Path) -> None:
    ci_changes.git("init", "-q", root=tmp_path)
    with pytest.raises(ci_changes.subprocess.CalledProcessError):
        ci_changes.changed_paths("a" * 40, "b" * 40, root=tmp_path)


def test_candidate_and_outputs_bind_both_parents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ci_changes.git("init", "-q", root=tmp_path)
    (tmp_path / "POLICY.md").write_text("# Policy\n")
    base = commit(tmp_path)
    (tmp_path / "POLICY.md").write_text("# Policy\n\nUpdated.\n")
    head = commit(tmp_path)
    ci_changes.git("checkout", "--detach", base, root=tmp_path)
    ci_changes.git(
        "-c",
        "user.name=CI",
        "-c",
        "user.email=ci@example.invalid",
        "merge",
        "--no-ff",
        "-m",
        "candidate",
        head,
        root=tmp_path,
    )
    candidate = ci_changes.git("rev-parse", "HEAD", root=tmp_path).decode().strip()
    monkeypatch.chdir(tmp_path)
    result = ci_changes.selection(
        base, head, candidate, head_ref="feature", base_repo_id="11", head_repo_id="12"
    )
    assert result["base"] == base
    assert result["head"] == head
    assert result["candidate"] == candidate
    assert result["tree"] == ci_changes.git("rev-parse", "HEAD^{tree}").decode().strip()
    assert json.loads(json.dumps(result))["jobs"]["tests"] is False
    with pytest.raises(ValueError, match="parents"):
        ci_changes.selection(
            head, base, candidate, head_ref="feature", base_repo_id="11", head_repo_id="12"
        )
    with pytest.raises(ValueError, match="parents"):
        ci_changes.selection(
            base, head, head, head_ref="feature", base_repo_id="11", head_repo_id="12"
        )


@pytest.mark.parametrize(
    ("head_ref", "base_id", "head_id"),
    [
        ("feature", "11", "11"),
        ("dev", "11", "12"),
        ("dev", "name/repo", "name/repo"),
        ("dev", "", ""),
    ],
)
def test_release_rejects_wrong_branch_or_repository_identity(
    head_ref: str, base_id: str, head_id: str
) -> None:
    with pytest.raises(ValueError, match=r"numeric|same-repository"):
        ci_changes.selection(
            "a" * 40,
            "b" * 40,
            "c" * 40,
            release=True,
            head_ref=head_ref,
            base_repo_id=base_id,
            head_repo_id=head_id,
        )


@pytest.mark.parametrize(
    "path", ["src/aegis_alpha/engine/bundle.py", "tests/engine/test_bundle.py"]
)
def test_engine_gets_app_scope(path: str) -> None:
    jobs, images = ci_changes.select_jobs([path])
    assert {name for name, enabled in jobs.items() if enabled} == {
        "docs",
        "security",
        "style",
        "types",
        "tests",
        "package",
    }
    assert images == []
