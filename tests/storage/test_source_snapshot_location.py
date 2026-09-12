from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlsplit

from aegis_alpha.storage import source_library
from aegis_alpha.storage.workspace import initialize, open_workspace

if TYPE_CHECKING:
    import pytest


def test_sqlite_image_uses_admitted_runtime_instead_of_git_tmpdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a valid source and an unrelated Git-contained process temp directory.
    source = tmp_path / "source.sqlite"
    with closing(sqlite3.connect(source)) as connection:
        connection.execute("CREATE TABLE sample (value TEXT)")
        connection.execute("INSERT INTO sample VALUES ('hashed')")
        connection.commit()
    source.chmod(0o600)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    git_tmp = tmp_path / "git-tmp"
    git_tmp.mkdir()
    (git_tmp / ".git").mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(git_tmp))
    home = tmp_path / "home"
    initialize(home)
    images: list[Path] = []
    original_connect = sqlite3.connect

    def connect_image(database: str, *, uri: bool) -> sqlite3.Connection:
        images.append(Path(unquote(urlsplit(database).path)))
        return original_connect(database, uri=uri)

    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        monkeypatch.setattr(source_library.sqlite3, "connect", connect_image)
        # When the importer constructs and opens its real private byte image.
        source_library.import_sqlite(workspace, source, "synthetic-source", digest)
        # Then no image enters the process temp checkout, and the source stays exact.
        assert len(images) == 1
        assert images[0].is_relative_to(workspace.paths.runtime)
        assert not images[0].exists()
        assert source_library.read_table(workspace, "synthetic-source", "sample")["rows"] == [
            {"value": "hashed"}
        ]
    assert list(git_tmp.iterdir()) == [git_tmp / ".git"]
