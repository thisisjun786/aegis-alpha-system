"""The data vertical contract names the test that holds each sentence, and the table stays true."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_DESIGN = _ROOT / "dev-notes/design/data-vertical.md"
_DECISION = _ROOT / "dev-notes/decisions/0017-data-vertical-contract.md"
_INDEX = _ROOT / "dev-notes/decisions/README.md"
_HEADING = "## 계약과 테스트 대응표"
_ROW = re.compile(
    r"^\| (?P<id>DV-\d{2}) \| (?P<sentence>[^|]+?) \| "
    r"`(?P<path>tests/[\w/]+\.py)::(?P<test>test_\w+)` \| (?P<status>\S+) \|$"
)
_STATUSES = {"구현", "예정"}


def _table(text: str) -> list[str]:
    _, found, rest = text.partition(_HEADING + "\n")
    if not found:
        raise AssertionError("contract table heading is missing")
    lines = []
    for line in rest.splitlines():
        if line.startswith("#"):
            break
        if line.startswith("| DV-"):
            lines.append(line)
    return lines


def contract_errors(text: str, root: Path) -> list[str]:
    """Return every way the contract table disagrees with the tests under ``root``."""
    errors: list[str] = []
    lines = _table(text)
    if not lines:
        return ["contract table has no rows"]
    seen: list[str] = []
    for line in lines:
        match = _ROW.match(line)
        if match is None:
            errors.append(f"malformed contract row: {line}")
            continue
        contract, path, test, status = (match[k] for k in ("id", "path", "test", "status"))
        seen.append(contract)
        if status not in _STATUSES:
            errors.append(f"{contract}: unknown status {status}")
            continue
        source = root / path
        defined = source.is_file() and re.search(
            rf"^(?:async )?def {test}\(", source.read_text(encoding="utf-8"), re.MULTILINE
        )
        if status == "구현" and not defined:
            errors.append(f"{contract}: {path}::{test} does not exist")
        if status == "예정" and defined:
            errors.append(f"{contract}: {path}::{test} exists; mark the row 구현")
    expected = [f"DV-{number:02d}" for number in range(1, len(seen) + 1)]
    if seen != expected:
        errors.append("contract ids must be unique and consecutive from DV-01")
    return errors


def test_contract_rows_match_tests() -> None:
    assert contract_errors(_DESIGN.read_text(encoding="utf-8"), _ROOT) == []


def test_decision_is_indexed_and_linked_both_ways() -> None:
    index = _INDEX.read_text(encoding="utf-8")
    assert "[0017](0017-data-vertical-contract.md) | accepted |" in index
    decision = _DECISION.read_text(encoding="utf-8")
    assert "status: accepted" in decision
    assert "../design/data-vertical.md" in decision
    assert "../decisions/0017-data-vertical-contract.md" in _DESIGN.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "owner",
    [
        "dev-notes/design/backtest-data-foundation.md",
        "dev-notes/architecture.md",
        "dev-notes/operations.md",
        "src/aegis_alpha/storage/AGENTS.md",
    ],
)
def test_owning_documents_point_at_the_contract(owner: str) -> None:
    assert "data-vertical.md" in (_ROOT / owner).read_text(encoding="utf-8")


def _document(*rows: str) -> str:
    return "\n".join(["# Doc", "", _HEADING, "", "| 계약 | 문장 | 테스트 | 상태 |", *rows, ""])


def test_checker_rejects_missing_and_stale_rows(tmp_path: Path) -> None:
    tests = tmp_path / "tests/storage"
    tests.mkdir(parents=True)
    (tests / "test_here.py").write_text("def test_present() -> None:\n    pass\n")
    text = _document(
        "| DV-01 | 있는 테스트 | `tests/storage/test_here.py::test_present` | 구현 |",
        "| DV-02 | 없는 테스트 | `tests/storage/test_here.py::test_absent` | 구현 |",
        "| DV-03 | 이미 생긴 테스트 | `tests/storage/test_here.py::test_present` | 예정 |",
        "| DV-04 | 아직 없는 파일 | `tests/storage/test_later.py::test_later` | 예정 |",
    )
    assert contract_errors(text, tmp_path) == [
        "DV-02: tests/storage/test_here.py::test_absent does not exist",
        "DV-03: tests/storage/test_here.py::test_present exists; mark the row 구현",
    ]


def test_checker_rejects_gaps_unknown_status_and_malformed_rows(tmp_path: Path) -> None:
    text = _document(
        "| DV-01 | 상태 오류 | `tests/storage/test_x.py::test_x` | 완료 |",
        "| DV-03 | 번호 건너뜀 | `tests/storage/test_y.py::test_y` | 예정 |",
        "| DV-04 | 테스트 없음 | 없음 | 예정 |",
    )
    assert contract_errors(text, tmp_path) == [
        "DV-01: unknown status 완료",
        "malformed contract row: | DV-04 | 테스트 없음 | 없음 | 예정 |",
        "contract ids must be unique and consecutive from DV-01",
    ]


def test_checker_requires_the_table() -> None:
    with pytest.raises(AssertionError, match="heading is missing"):
        contract_errors("# Doc\n", _ROOT)
