"""Content-addressed source IDs and the ``sl:`` source-snapshot link, on synthetic bytes."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from aegis_alpha.storage import source_library
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
    return report["source_library"]["linked"]


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
    result = subprocess.run(  # noqa: S603 -- fixed interpreter, temporary synthetic home
        [sys.executable, "-m", "aegis_alpha", "db", "source-link", *args],
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
        "incomplete": 0,
        "new_files": 1,
        "new_bytes": len(payload),
        "unbacked_sources": [],
        "incomplete_sources": [],
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
        with pytest.raises(ValueError, match="conflicts"):
            source_link(workspace, apply=False)
