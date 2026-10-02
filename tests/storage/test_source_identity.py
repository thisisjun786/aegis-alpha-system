"""Content-addressed source IDs and the ``sl:`` source-snapshot link, on synthetic bytes."""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from aegis_alpha.data.descriptor_tree import DescriptorTreeError
from aegis_alpha.storage import source_identity, source_library
from aegis_alpha.storage.publication import recover_operations
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.source_identity import (
    SourceContent,
    SourceFile,
    link_source,
    source_link,
)
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace

_ROOT = Path(__file__).resolve().parents[2]
_ALPHA = "8ed3f6ad685b959ead7022518e1af76cd816f8e8ec7ccdda1ed4018e8f2223f8"
_BETA = "f44e64e75f3948e9f73f8dfa94721c4ce8cbb4f265c4790c702b2d41cfbf2753"
_FROZEN_DOCUMENT = (
    b'["aas-source-id-v1",1,[["8e/' + _ALPHA.encode() + b'",5,"' + _ALPHA.encode() + b'"],'
    b'["f4/' + _BETA.encode() + b'",4,"' + _BETA.encode() + b'"]]]'
)
_FROZEN_HEX = "6a7aacbb7efcd393288d804faf8f9068707d3ba4a807466c87c99f21d451467d"
_SCHEMA = pa.schema([("symbol", pa.string()), ("close", pa.float64())])


@pytest.fixture
def home(tmp_path: Path) -> Path:
    root = tmp_path / "aas"
    initialize(root)
    return root


def _retain(workspace: Workspace, payload: bytes) -> SourceFile:
    _, digest, size = put_raw(workspace.paths.raw, payload)
    return SourceFile(digest, size)


def _bars(*rows: tuple[str, float]) -> pa.RecordBatchReader:
    table = pa.table(
        {"symbol": [row[0] for row in rows], "close": [row[1] for row in rows]}, schema=_SCHEMA
    )
    return table.to_reader()


def _commit_count(workspace: Workspace) -> int:
    row = workspace.market.execute("SELECT count(*) FROM source_library_commits").fetchone()
    assert row is not None
    return int(row[0])


def _linked(workspace: Workspace) -> object:
    report = cast("dict[str, dict[str, object]]", verify_workspace(workspace))
    return report["source_library"].get("linked", 0)


def _manifest(workspace: Workspace, source_id: str) -> dict[str, object]:
    row = workspace.market.execute(
        "SELECT manifest_json FROM source_library_commits WHERE source_id=?", [source_id]
    ).fetchone()
    assert row is not None
    return json.loads(row[0])


def _links(workspace: Workspace) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in workspace.state.execute(
            "SELECT s.snapshot_id,s.provider,s.status,f.relative_path,f.byte_hash,f.size_bytes "
            "FROM source_snapshots s JOIN source_files f USING(snapshot_id) "
            "ORDER BY s.snapshot_id,f.relative_path"
        )
    ]


def test_source_id_format_is_frozen() -> None:
    alpha = SourceFile(_ALPHA, 5)
    beta = SourceFile(_BETA, 4)
    assert hashlib.sha256(b"alpha").hexdigest() == _ALPHA
    content = SourceContent("synthetic", "daily-bars", 1, (beta, alpha, beta))
    assert content.document() == _FROZEN_DOCUMENT
    assert content.sha256 == _FROZEN_HEX == hashlib.sha256(_FROZEN_DOCUMENT).hexdigest()
    assert content.source_id == "synthetic-daily-bars-" + _FROZEN_HEX
    # File order and repeated identical files never move the ID; the major does.
    assert SourceContent("synthetic", "daily-bars", 1, (alpha, beta)) == content
    assert SourceContent("synthetic", "daily-bars", 2, (alpha, beta)).sha256 != _FROZEN_HEX
    # Provider and shape name the source but stay out of the content hash.
    assert SourceContent("other", "quarantine", 1, (alpha, beta)).sha256 == _FROZEN_HEX
    assert SourceContent.from_record(content.record()) == content


@pytest.mark.parametrize(
    ("provider", "shape", "major", "files", "message"),
    [
        ("Upper", "bars", 1, (SourceFile(_ALPHA, 5),), "provider"),
        ("p-q", "bars", 1, (SourceFile(_ALPHA, 5),), "provider"),
        ("p", "bars-", 1, (SourceFile(_ALPHA, 5),), "shape"),
        ("p", "bars", 0, (SourceFile(_ALPHA, 5),), "major"),
        ("p", "bars", True, (SourceFile(_ALPHA, 5),), "major"),
        ("p", "bars", 1, (), "at least one"),
        ("p", "bars", 1, (SourceFile(_ALPHA, 5), SourceFile(_ALPHA, 6)), "two sizes"),
        ("p", "b" * 200, 1, (SourceFile(_ALPHA, 5),), "too long"),
    ],
)
def test_content_identity_rejects_invalid_parts(
    provider: str, shape: str, major: int, files: tuple[SourceFile, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        SourceContent(provider, shape, major, files)


def test_source_file_and_record_reject_unaddressed_entries() -> None:
    with pytest.raises(ValueError, match="SHA-256"):
        SourceFile(_ALPHA.upper(), 5)
    with pytest.raises(ValueError, match="size"):
        SourceFile(_ALPHA, -1)
    record = SourceContent("p", "bars", 1, (SourceFile(_ALPHA, 5),)).record()
    record["files"] = [["original/name.csv", 5, _ALPHA]]
    with pytest.raises(ValueError, match="raw address"):
        SourceContent.from_record(record)


def test_code_change_reuses_content_id(home: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        content = SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, b"export"),))
        first = source_library.import_content_arrow(
            workspace,
            content,
            "bars",
            _bars(("AAA", 1.5), ("BBB", 2.25)),
            lineage={"loader_sha256": "1" * 64},
        )
        second = source_library.import_content_arrow(
            workspace,
            content,
            "bars",
            _bars(("AAA", 1.5), ("BBB", 2.25)),
            lineage={"loader_sha256": "2" * 64},
        )
        assert first["source_id"] == second["source_id"] == content.source_id
        assert (first["reused"], second["reused"]) == (False, True)
        assert (first["link"], second["link"]) == ("linked", "unchanged")
        assert _commit_count(workspace) == 1
        manifest = _manifest(workspace, content.source_id)
        # The first loader's lineage stays recorded; the ID never carried it.
        assert manifest["metadata"] == {
            "source": content.record(),
            "lineage": {"loader_sha256": "1" * 64},
        }
        assert cast("list[dict[str, object]]", manifest["tables"])[0]["rows"] == 2  # noqa: PLR2004
        # The ID document is retained, so the source hash resolves to its preimage.
        stored = workspace.paths.raw / content.sha256[:2] / content.sha256
        assert stored.read_bytes() == content.document()
        # Code that changes the rows under the same major is refused, not re-loaded.
        with pytest.raises(ValueError, match="different content"):
            source_library.import_content_arrow(
                workspace, content, "bars", _bars(("AAA", 1.5)), lineage={"loader": "3"}
            )
        # A new output schema is a new major and therefore a new source.
        bumped = SourceContent("synthetic", "daily-bars", 2, content.files)
        third = source_library.import_content_arrow(
            workspace, bumped, "bars", _bars(("AAA", 1.5)), lineage={"loader": "3"}
        )
        assert third["source_id"] != content.source_id
        assert third["reused"] is False
        assert _commit_count(workspace) == 2  # noqa: PLR2004


def test_content_change_mints_new_id(home: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        original = SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, b"day-1"),))
        corrected = SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, b"day-1b"),))
        assert original.source_id != corrected.source_id
        for content in (original, corrected):
            result = source_library.import_content_arrow(
                workspace, content, "bars", _bars(("AAA", 1.0)), lineage={"loader": "same"}
            )
            assert (result["source_id"], result["reused"]) == (content.source_id, False)
        assert _commit_count(workspace) == 2  # noqa: PLR2004
        assert {row[0] for row in _links(workspace)} == {
            "sl:" + original.source_id,
            "sl:" + corrected.source_id,
        }
        # Bytes that never reached raw storage cannot name a source.
        missing = SourceContent(
            "synthetic", "daily-bars", 1, (SourceFile(hashlib.sha256(b"x").hexdigest(), 1),)
        )
        with pytest.raises(ValueError, match="not retained in raw"):
            source_library.import_content_arrow(workspace, missing, "bars", _bars(("A", 1.0)))
        assert _commit_count(workspace) == 2  # noqa: PLR2004
        assert _linked(workspace) == 2  # noqa: PLR2004


def _cli(*args: str, home: Path) -> dict[str, object]:
    return _db("source-link", *args, home=home)


def _db(*args: str, home: Path) -> dict[str, object]:
    result = subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", "db", *args],
        env={**os.environ, "AAS_HOME": str(home), "PYTHONPATH": str(_ROOT / "src")},
        cwd=home.parent,
        text=True,
        capture_output=True,
        check=False,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_source_link_is_idempotent(home: Path) -> None:
    payload = b'{"batch":"synthetic"}'
    digest = hashlib.sha256(payload).hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        # An explicit-ID commit whose pinned bytes are not in raw yet stays unlinked.
        imported = source_library.import_arrow(
            workspace, "synthetic-legacy-batch", digest, "bars", _bars(("AAA", 1.0))
        )
        assert imported["link"] == "unbacked"
        put_raw(workspace.paths.raw, payload)
        operation = workspace.state.execute(
            "SELECT created_at_us,completed_at_us FROM storage_operations WHERE target_id=?",
            ("synthetic-legacy-batch",),
        ).fetchone()
    planned = _cli("--plan", home=home)
    assert planned == {
        "mode": "plan",
        "commits": 1,
        "linked": 0,
        "unchanged": 0,
        "pending": 1,
        "unbacked": 0,
        "corrupt": 0,
        "incomplete": 0,
        "invalid": 0,
        "new_files": 1,
        "new_bytes": len(payload),
        "unbacked_sources": [],
        "corrupt_sources": [],
        "incomplete_sources": [],
        "invalid_sources": [],
    }
    with open_workspace(home) as workspace:
        assert _links(workspace) == []
    applied = _cli("--apply", home=home)
    assert (applied["linked"], applied["pending"], applied["new_files"]) == (1, 0, 1)
    again = _cli("--apply", home=home)
    assert (again["linked"], again["unchanged"], again["new_files"]) == (0, 1, 0)
    with open_workspace(home) as workspace:
        assert _links(workspace) == [
            (
                "sl:synthetic-legacy-batch",
                "source-library",
                "raw_verified",
                digest[:2] + "/" + digest,
                digest,
                len(payload),
            )
        ]
        times = workspace.state.execute(
            "SELECT requested_at_us,retrieved_at_us,publication_at_us FROM source_snapshots"
        ).fetchone()
        # The link's times are the durable intent's own, never a new wall clock.
        assert tuple(times) == (*tuple(operation), None)
        assert _linked(workspace) == 1


def test_conflicting_link_is_refused(home: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        payload = b"pinned"
        digest = hashlib.sha256(payload).hexdigest()
        source_library.import_arrow(workspace, "synthetic-x", digest, "bars", _bars(("A", 1.0)))
        put_raw(workspace.paths.raw, payload)
        workspace.state.execute(
            "INSERT INTO source_snapshots VALUES "
            "('sl:synthetic-x','source-library',0,0,NULL,'raw_verified')"
        )
        workspace.state.commit()
        with pytest.raises(ValueError, match="conflicts"):
            link_source(workspace, "synthetic-x")
        report = source_link(workspace, apply=False)
        assert report["invalid"] == 1
        invalid = cast("list[dict[str, str]]", report["invalid_sources"])
        assert invalid[0]["source_id"] == "synthetic-x"
        assert "conflicts" in invalid[0]["error"]
        # verify compares a recorded link with its commit and refuses the difference.
        with pytest.raises(ValueError, match="conflicts with its commit"):
            verify_workspace(workspace)


def test_file_order_and_duplicates_never_move_the_id() -> None:
    files = [SourceFile(hashlib.sha256(bytes([n])).hexdigest(), n + 1) for n in range(4)]
    expected = SourceContent("synthetic", "daily-bars", 1, tuple(files)).source_id
    for order in itertools.permutations(files):
        for repeat in range(len(files)):
            listed = (*order, *order[:repeat])
            assert SourceContent("synthetic", "daily-bars", 1, listed).source_id == expected


def test_regrouping_complete_units_reuses_the_same_ids(home: Path) -> None:
    """One source per complete original unit, so loader batch sizes never move the IDs."""
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        jobs = [
            (
                SourceContent(
                    "synthetic",
                    "daily-bars",
                    1,
                    (
                        _retain(workspace, f"complete-{n}".encode()),
                        _retain(workspace, f"bars-{n}".encode()),
                    ),
                ),
                (f"S{n}", float(n)),
            )
            for n in range(5)
        ]

        def load(batch: int, lineage: str) -> list[dict[str, object]]:
            results = []
            for start in range(0, len(jobs), batch):
                for content, row in jobs[start : start + batch]:
                    results.append(
                        source_library.import_content_arrow(
                            workspace, content, "bars", _bars(row), lineage={"loader": lineage}
                        )
                    )
            return results

        first = load(2, "v1")
        second = load(3, "v2")
        assert [r["source_id"] for r in first] == [r["source_id"] for r in second]
        assert {r["source_id"] for r in first} == {content.source_id for content, _ in jobs}
        assert all(r["reused"] for r in second)
        assert _commit_count(workspace) == len(jobs)
        # A batch of units is a different file group and therefore a different source.
        merged = SourceContent("synthetic", "daily-bars", 1, jobs[0][0].files + jobs[1][0].files)
        assert merged.source_id not in {r["source_id"] for r in first}


def test_reuse_with_a_different_arrow_schema_is_refused(home: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        content = SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, b"export"),))
        source_library.import_content_arrow(workspace, content, "bars", _bars(("AAA", 1.5)))
        widened = pa.table(
            {"symbol": ["AAA"], "close": [1.5]},
            schema=pa.schema([("symbol", pa.large_string()), ("close", pa.float64())]),
        )
        with pytest.raises(ValueError, match="different content"):
            source_library.import_content_arrow(workspace, content, "bars", widened.to_reader())
        assert _commit_count(workspace) == 1


def test_explicit_id_cannot_claim_a_content_identity(home: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        with pytest.raises(ValueError, match="cannot claim a content identity"):
            source_library.import_arrow(
                workspace,
                "zz-explicit",
                "1" * 64,
                "t",
                _bars(("A", 1.0)),
                metadata={"source": {"format": "aas-source-id-v1"}},
            )
        assert source_library.list_sources(workspace) == []


def _crash(*_args: object) -> None:
    raise RuntimeError("synthetic interruption")


def test_incomplete_intent_is_reported_not_linked(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"pinned"
    digest = hashlib.sha256(payload).hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        put_raw(workspace.paths.raw, payload)
        with monkeypatch.context() as patch:
            patch.setattr(source_library, "complete_operation", _crash)
            with pytest.raises(RuntimeError, match="interruption"):
                source_library.import_arrow(
                    workspace, "synthetic-x", digest, "bars", _bars(("A", 1.0))
                )
        report = source_link(workspace, apply=True)
        assert (report["incomplete"], report["incomplete_sources"]) == (1, ["synthetic-x"])
        assert _links(workspace) == []


def test_corrupt_and_invalid_commits_never_stop_the_backfill(home: Path) -> None:
    payloads = [b"first", b"second", b"third"]
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        for n, payload in enumerate(payloads):
            source_library.import_arrow(
                workspace,
                f"synthetic-{n}",
                hashlib.sha256(payload).hexdigest(),
                "bars",
                _bars(("A", float(n))),
            )
            put_raw(workspace.paths.raw, payload)
        corrupt = hashlib.sha256(payloads[0]).hexdigest()
        stored = workspace.paths.raw / corrupt[:2] / corrupt
        stored.chmod(0o600)
        stored.write_bytes(b"FIRST")
        workspace.state.execute(
            "INSERT INTO source_snapshots VALUES "
            "('sl:synthetic-1','source-library',0,0,NULL,'raw_verified')"
        )
        workspace.state.commit()
        report = source_link(workspace, apply=True)
        assert (report["corrupt"], report["corrupt_sources"]) == (1, ["synthetic-0"])
        assert report["invalid"] == 1
        assert report["linked"] == 1
        assert {row[0] for row in _links(workspace)} == {"sl:synthetic-2"}


def test_interrupted_content_link_fails_verify_until_recover(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        content = SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, b"export"),))
        # The process stops after completing the intent and before recording the link.
        with monkeypatch.context() as patch:
            patch.setattr(source_library, "link_source", _crash)
            with pytest.raises(RuntimeError, match="interruption"):
                source_library.import_content_arrow(workspace, content, "bars", _bars(("A", 1.0)))
        assert _commit_count(workspace) == 1
        with pytest.raises(ValueError, match="lacks its link"):
            verify_workspace(workspace)
    recovered = _db("recover", home=home)
    assert recovered["linked_sources"] == [content.source_id]
    assert recovered["invalid_sources"] == []
    with open_workspace(home) as workspace:
        assert _linked(workspace) == 1
    assert _db("recover", home=home)["linked_sources"] == []


def test_content_source_requires_its_id_document_in_raw(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        linked = SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, b"one"),))
        source_library.import_content_arrow(workspace, linked, "bars", _bars(("A", 1.0)))
        (workspace.paths.raw / linked.sha256[:2] / linked.sha256).unlink()
        with pytest.raises(ValueError, match="ID document"):
            verify_workspace(workspace)
        unlinked = SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, b"two"),))
        with monkeypatch.context() as patch:
            patch.setattr(source_library, "link_source", _crash)
            with pytest.raises(RuntimeError, match="interruption"):
                source_library.import_content_arrow(workspace, unlinked, "bars", _bars(("B", 2.0)))
        (workspace.paths.raw / unlinked.sha256[:2] / unlinked.sha256).unlink()
        assert link_source(workspace, unlinked.source_id) == "unbacked"


def test_recover_source_records_the_link(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"pinned"
    digest = hashlib.sha256(payload).hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        put_raw(workspace.paths.raw, payload)
        with monkeypatch.context() as patch:
            patch.setattr(source_library, "complete_operation", _crash)
            with pytest.raises(RuntimeError, match="interruption"):
                source_library.import_arrow(
                    workspace, "synthetic-x", digest, "bars", _bars(("A", 1.0))
                )
        assert _links(workspace) == []
        assert recover_operations(workspace)["pending"] == []
        assert [row[0] for row in _links(workspace)] == ["sl:synthetic-x"]
        assert _linked(workspace) == 1


def test_link_that_is_no_longer_linkable_fails_verify(home: Path) -> None:
    payload = b"pinned"
    digest = hashlib.sha256(payload).hexdigest()
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        put_raw(workspace.paths.raw, payload)
        source_library.import_arrow(workspace, "synthetic-x", digest, "bars", _bars(("A", 1.0)))
        assert _linked(workspace) == 1
        (workspace.paths.raw / digest[:2] / digest).unlink()
        with pytest.raises(ValueError, match="no longer linkable"):
            source_library.verify_sources(workspace)
        # The workspace verifier also re-hashes the linked file and fails on it first.
        with pytest.raises(DescriptorTreeError, match="cannot be opened"):
            verify_workspace(workspace)


def test_report_without_links_keeps_its_shape(home: Path) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        imported = source_library.import_arrow(
            workspace, "synthetic-x", "1" * 64, "bars", _bars(("A", 1.0))
        )
        assert imported["link"] == "unbacked"
        report = verify_workspace(workspace)
        # Backups record this report; one taken before links existed restores unchanged.
        assert report["source_library"] == {"sources": 1, "tables": 1, "rows": 1}


def test_recover_lists_an_invalid_content_commit_and_links_the_rest(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        contents = [
            SourceContent("synthetic", "daily-bars", 1, (_retain(workspace, payload),))
            for payload in (b"one", b"two")
        ]
        with monkeypatch.context() as patch:
            patch.setattr(source_library, "link_source", _crash)
            for content in contents:
                with pytest.raises(RuntimeError, match="interruption"):
                    source_library.import_content_arrow(
                        workspace, content, "bars", _bars(("A", 1.0))
                    )
        real = link_source

        def refuse_first(workspace: Workspace, source_id: str, *, apply: bool = True) -> str:
            if source_id == min(c.source_id for c in contents):
                raise ValueError("synthetic invalid record")
            return real(workspace, source_id, apply=apply)

        monkeypatch.setattr(source_identity, "link_source", refuse_first)
        result = source_identity.link_content_sources(workspace)
        first, second = sorted(c.source_id for c in contents)
        assert result == {
            "linked_sources": [second],
            "invalid_sources": [{"source_id": first, "error": "synthetic invalid record"}],
        }
