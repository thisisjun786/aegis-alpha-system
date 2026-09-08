"""Lock inventory, OSV pagination/advisories, and fail-closed responses."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from scripts import ci_security as security


def test_root_lock_is_read_and_packages_deduplicated() -> None:
    inventory = security.lock_inventory(Path(__file__).resolve().parents[2])
    assert set(inventory) == {"uv.lock"}
    pairs = security.unique_packages(inventory)
    assert len(pairs) == len(set(pairs))
    assert ("cryptography", "50.0.1") in pairs


def test_pagination_and_lowercase_ghsa_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    inventory = {"uv.lock": [("synthetic-package", "1")]}
    monkeypatch.setattr(security, "lock_inventory", lambda _: inventory)
    calls: list[tuple[str, object]] = []

    def transport(url: str, payload: dict[str, object] | None = None) -> object:
        calls.append((url, payload))
        if url == security.OSV_QUERYBATCH:
            assert payload is not None
            queries = cast("list[dict[str, object]]", payload["queries"])
            assert len(queries) == 1
            if queries[0].get("page_token") == "next":
                return {"results": [{"vulns": [{"id": "CVE-2026-12345"}]}]}
            return {
                "results": [{"vulns": [{"id": "GHSA-abcd-efgh-ijkl"}], "next_page_token": "next"}]
            }
        identity = url.rsplit("/", 1)[1]
        return {"id": identity, "aliases": ["CVE-2026-12345", "GHSA-abcd-efgh-ijkl"]}

    findings = security.audit_locks(Path(), transport=transport)
    expected = [("synthetic-package", "1", ("CVE-2026-12345", "GHSA-abcd-efgh-ijkl"))]
    assert findings == {"uv.lock": expected}
    assert [url for url, _ in calls] == [
        security.OSV_QUERYBATCH,
        security.OSV_QUERYBATCH,
        security.OSV_VULN + "GHSA-abcd-efgh-ijkl",
        security.OSV_VULN + "CVE-2026-12345",
    ]


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        {"error": "failed"},
        {"vulns": None},
        {"vulns": {}},
        {"vulns": [{}]},
        {"next_page_token": 7},
    ],
)
def test_malformed_result_fails(response: object) -> None:
    with pytest.raises((ValueError, TypeError), match=r"OSV"):
        security.parse_result(response)


def test_repeated_page_token_fails() -> None:
    def transport(_url: str, _payload: dict[str, object] | None = None) -> object:
        return {"results": [{"next_page_token": "repeat"}]}

    with pytest.raises(ValueError, match="repeated"):
        security.collect_vulnerability_ids([("a", "1")], transport=transport)


@pytest.mark.parametrize("response", [{}, {"results": []}, {"results": [None, None]}])
def test_missing_batch_results_fail(response: object) -> None:
    with pytest.raises(ValueError, match="count mismatch"):
        security.collect_vulnerability_ids([("a", "1")], transport=lambda *_: response)


def test_detail_identity_mismatch_fails() -> None:
    with pytest.raises(ValueError, match="mismatch"):
        security.alias_group("CVE-2026-12345", transport=lambda *_: {"id": "CVE-2026-99999"})


def test_transport_failure_is_not_clean_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> object:
        raise TimeoutError

    monkeypatch.setattr(security, "urlopen", fail)
    with pytest.raises(ValueError, match="transport failed"):
        security.osv_request(security.OSV_QUERYBATCH, {"queries": []})


def test_empty_result_is_clean_and_cli_advisory_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert security.parse_result({}) == ([], None)
    monkeypatch.setattr("sys.argv", ["ci_security"])
    monkeypatch.setattr(
        security, "audit_locks", lambda _: {"uv.lock": [("example", "1", ("CVE-2026-12345",))]}
    )
    with pytest.raises(SystemExit, match="example==1"):
        security.main()
    assert not capsys.readouterr().out


def test_zero_packages_missing_lock_and_malformed_lock_fail(tmp_path: Path) -> None:
    path = tmp_path / "uv.lock"
    with pytest.raises(ValueError, match="missing"):
        security.locked_packages(path)
    path.write_text("package = []\n")
    with pytest.raises(ValueError, match="no packages"):
        security.locked_packages(path)
    path.write_text(json.dumps({"bad": True}))
    with pytest.raises(ValueError, match=r"Expected|Invalid statement"):
        security.locked_packages(path)


def test_root_lock_cannot_be_replaced_by_retired_lock(tmp_path: Path) -> None:
    retired = tmp_path / "vt"
    retired.mkdir()
    (retired / "uv.lock").write_text(
        '[[package]]\nname = "synthetic-package"\nversion = "1"\n'
        'source = { registry = "https://pypi.org/simple" }\n'
    )
    with pytest.raises(ValueError, match=r"uv.lock: lockfile is missing"):
        security.audit_locks(tmp_path, transport=lambda *_: pytest.fail("must not query OSV"))


def test_root_lock_alone_is_sufficient_for_clean_audit(tmp_path: Path) -> None:
    (tmp_path / "uv.lock").write_text(
        '[[package]]\nname = "synthetic-package"\nversion = "1"\n'
        'source = { registry = "https://pypi.org/simple" }\n'
    )
    queries: list[object] = []

    def transport(url: str, payload: dict[str, object] | None = None) -> object:
        assert url == security.OSV_QUERYBATCH
        queries.append(payload)
        return {"results": [{}]}

    assert security.audit_locks(tmp_path, transport=transport) == {"uv.lock": []}
    assert queries == [
        {
            "queries": [
                {"package": {"name": "synthetic-package", "ecosystem": "PyPI"}, "version": "1"}
            ]
        }
    ]
