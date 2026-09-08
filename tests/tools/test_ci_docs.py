from __future__ import annotations

from pathlib import Path

from scripts.ci_changes import git
from scripts.ci_docs import check_documents, links


def commit(root: Path) -> str:
    git("add", ".", root=root)
    git(
        "-c",
        "user.name=CI",
        "-c",
        "user.email=ci@example.invalid",
        "commit",
        "-qm",
        "fixture",
        root=root,
    )
    return git("rev-parse", "HEAD", root=root).decode().strip()


def test_deleted_document_breaks_unchanged_referrer(tmp_path: Path) -> None:
    git("init", "-q", root=tmp_path)
    (tmp_path / "index.md").write_text("# Index\n\n[Target](target.md)\n")
    target = tmp_path / "target.md"
    target.write_text("# Target\n")
    base = commit(tmp_path)
    target.unlink()
    head = commit(tmp_path)
    assert check_documents(base, head, root=tmp_path) == ["index.md: missing link target.md"]


def test_changed_document_links_and_anchors(tmp_path: Path) -> None:
    git("init", "-q", root=tmp_path)
    (tmp_path / "index.md").write_text("# Index\n")
    base = commit(tmp_path)
    (tmp_path / "index.md").write_text("# Index\n\n[Good](#index) [Bad](#absent)\n")
    head = commit(tmp_path)
    assert check_documents(base, head, root=tmp_path) == ["index.md: missing anchor #absent"]


def test_code_examples_are_not_links() -> None:
    text = "# Doc\n```md\n[example](missing.md)\n```\n[real](target.md)\n"
    assert links(text) == ["target.md"]


def test_changed_heading_breaks_unchanged_incoming_link(tmp_path: Path) -> None:
    git("init", "-q", root=tmp_path)
    (tmp_path / "index.md").write_text("# Index\n\n[Target](target.md#old)\n")
    target = tmp_path / "target.md"
    target.write_text("# Old\n")
    base = commit(tmp_path)
    target.write_text("# New\n")
    head = commit(tmp_path)
    assert check_documents(base, head, root=tmp_path) == ["index.md: missing anchor target.md#old"]


def test_deleted_non_markdown_target_breaks_document_link(tmp_path: Path) -> None:
    git("init", "-q", root=tmp_path)
    (tmp_path / "index.md").write_text("# Index\n\n[Fixture](request.json)\n")
    target = tmp_path / "request.json"
    target.write_text("{}\n")
    base = commit(tmp_path)
    target.unlink()
    head = commit(tmp_path)
    assert check_documents(base, head, root=tmp_path) == ["index.md: missing link request.json"]


def test_preserved_history_and_patch_bytes_are_not_live_prose(tmp_path: Path) -> None:
    git("init", "-q", root=tmp_path)
    (tmp_path / "index.md").write_text("# Index\n")
    base = commit(tmp_path)
    archive = tmp_path / "dev-notes/archive"
    archive.mkdir(parents=True)
    (archive / "old.md").write_text("[Historical location](removed.md)  \n\n")
    (archive / "raw.txt").write_text("Captured bytes   \n")
    (tmp_path / "original.patch").write_text(" \n")
    head = commit(tmp_path)
    assert check_documents(base, head, root=tmp_path) == []
