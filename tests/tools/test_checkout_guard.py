"""Prevent the existence-only checkout guard from being copied back into src."""

from __future__ import annotations

import ast
from pathlib import Path


def test_no_git_marker_exists_checks_in_source() -> None:
    root = Path(__file__).resolve().parents[2]
    offenders = []
    for path in (root / "src").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "exists"
                and any(
                    isinstance(part, ast.Constant) and part.value == ".git"
                    for part in ast.walk(node.func.value)
                )
            ):
                offenders.append(f"{path.relative_to(root)}:{node.lineno}")  # noqa: PERF401 -- report AST locations
    assert not offenders, "Use storage.checkout: " + ", ".join(offenders)
