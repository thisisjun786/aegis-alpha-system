from __future__ import annotations

import io
import json
import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data import sec_bulk
from aegis_alpha.data.sec_bulk_cli import main

if TYPE_CHECKING:
    from collections.abc import Iterator

UA = "AAS synthetic-contact@invalid.test"
CAP = 1024 * 1024


def payload() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("CIK0000000001.json", '{"cik":1,"facts":{}}')
    return buffer.getvalue()


class Reply(io.BytesIO):
    def __init__(self, body: bytes, *, status: int = 200, length: int | None = None) -> None:
        super().__init__(body)
        self.status = status
        self.headers = {"Content-Length": str(len(body) if length is None else length)}
        self.url = sec_bulk.ARCHIVES["companyfacts"]

    def geturl(self) -> str:
        return self.url


class Transport:
    def __init__(self, reply: Reply) -> None:
        self.reply = reply
        self.calls = 0

    @contextmanager
    def __call__(self, _url: str, _agent: str) -> Iterator[Reply]:
        self.calls += 1
        yield self.reply


def test_capture_and_reuse_verify_without_network(tmp_path: Path) -> None:
    transport = Transport(Reply(payload()))
    root = tmp_path / "bulk"
    result = sec_bulk.acquire_archive(
        "companyfacts", root, user_agent=UA, max_bytes=CAP, workers=2, transport=transport
    )
    assert result["members"] == 1
    assert result["catalog_registered"] is False
    assert transport.calls == 1
    reused = sec_bulk.acquire_archive(
        "companyfacts", root, user_agent=UA, max_bytes=CAP, transport=transport
    )
    assert reused["http_calls"] == 0
    assert transport.calls == 1
    receipt = (root / "companyfacts.receipt.json").read_text()
    assert UA not in receipt
    assert "synthetic-contact@invalid.test" not in receipt
    (root / "companyfacts.zip").write_bytes(payload() + b"tampered")
    with pytest.raises(sec_bulk.BulkError, match="differs"):
        sec_bulk.acquire_archive(
            "companyfacts", root, user_agent=UA, max_bytes=CAP, transport=transport
        )
    assert transport.calls == 1


@pytest.mark.parametrize("status", [403, 429, 500])
def test_http_failure_does_not_publish_or_retry(tmp_path: Path, status: int) -> None:
    root = tmp_path / "bulk"
    transport = Transport(Reply(b"rejected", status=status))
    with pytest.raises(sec_bulk.BulkError, match="status"):
        sec_bulk.acquire_archive(
            "companyfacts", root, user_agent=UA, max_bytes=CAP, transport=transport
        )
    assert transport.calls == 1
    assert not (root / "companyfacts.zip").exists()
    assert not (root / "companyfacts.receipt.json").exists()


@pytest.mark.parametrize("case", ["truncated", "invalid_zip", "limit", "redirect", "leak"])
def test_invalid_response_preserves_no_success(tmp_path: Path, case: str) -> None:
    root = tmp_path / "bulk"
    reply = Reply(payload())
    cap = CAP
    if case == "truncated":
        reply.headers["Content-Length"] = str(len(payload()) + 1)
    elif case == "invalid_zip":
        reply = Reply(b"not zip")
    elif case == "limit":
        cap = 1
    elif case == "redirect":
        reply.url = "https://invalid.test/leak"
    else:
        reply.headers["ETag"] = UA
    with pytest.raises((sec_bulk.BulkError, RuntimeError)):
        sec_bulk.acquire_archive(
            "companyfacts", root, user_agent=UA, max_bytes=cap, transport=Transport(reply)
        )
    assert not (root / "companyfacts.zip").exists()
    assert not (root / "companyfacts.receipt.json").exists()


def test_disk_rejection_precedes_http(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        os, "fstatvfs", lambda _fd: os.statvfs_result((4096, 4096, 0, 0, 0, 0, 0, 0, 0, 255))
    )
    transport = Transport(Reply(payload()))
    with pytest.raises(sec_bulk.BulkError, match="disk"):
        sec_bulk.acquire_archive(
            "companyfacts", tmp_path / "bulk", user_agent=UA, max_bytes=CAP, transport=transport
        )
    assert transport.calls == 0


def test_plan_needs_no_credentials_and_creates_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "absent"
    assert (
        main(["--archive", "companyfacts", "--output-root", str(root), "--max-bytes", str(CAP)])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["raw_only"] is True
    assert not root.exists()


@pytest.mark.parametrize("name", ["../bad", "/absolute.json", "bad.txt"])
def test_zip_member_paths_refused(name: str) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, "{}")
    buffer.seek(0)
    with pytest.raises(sec_bulk.BulkError, match="member"):
        sec_bulk.validate_zip(buffer, 1)


def test_symlink_output_refused(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    transport = Transport(Reply(payload()))
    with pytest.raises(ValueError, match=r"aliases|directory"):
        sec_bulk.acquire_archive(
            "companyfacts", alias, user_agent=UA, max_bytes=CAP, transport=transport
        )
    assert transport.calls == 0


def test_interrupted_body_keeps_partial_only(tmp_path: Path) -> None:
    class Interrupted(Reply):
        def read(self, amount: int | None = -1, /) -> bytes:
            if self.tell():
                raise TimeoutError("synthetic interrupted stream")
            return super().read(16 if amount is None or amount < 0 else min(amount, 16))

    root = tmp_path / "bulk"
    transport = Transport(Interrupted(payload()))
    with pytest.raises(TimeoutError):
        sec_bulk.acquire_archive(
            "companyfacts", root, user_agent=UA, max_bytes=CAP, transport=transport
        )
    assert transport.calls == 1
    assert [x.stat().st_size for x in (root / "partial").glob("*.zip")] == [16]
    assert not (root / "companyfacts.receipt.json").exists()


def test_foreign_receipt_refused_without_http(tmp_path: Path) -> None:
    root = tmp_path / "bulk"
    root.mkdir()
    (root / "companyfacts.receipt.json").write_text('{"contract":"foreign"}')
    transport = Transport(Reply(payload()))
    with pytest.raises(sec_bulk.BulkError, match="identity"):
        sec_bulk.acquire_archive(
            "companyfacts", root, user_agent=UA, max_bytes=CAP, transport=transport
        )
    assert transport.calls == 0


def test_redirect_handler_refuses_before_second_request() -> None:
    with pytest.raises(sec_bulk.BulkError, match="redirect"):
        sec_bulk._NoRedirect().redirect_request()  # noqa: SLF001 -- exercise actual opener handler


@pytest.mark.parametrize(("content", "valid"), [(b"Placeholder file", True), (b"changed", False)])
def test_exact_official_placeholder_is_metadata(content: bytes, *, valid: bool) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CIK0000000001.json", "{}")
        archive.writestr("placeholder.txt", content)
    buffer.seek(0)
    if valid:
        assert sec_bulk.validate_zip(buffer, 2)[0] == 2  # noqa: PLR2004 -- one JSON and one placeholder
    else:
        with pytest.raises(sec_bulk.BulkError, match="placeholder"):
            sec_bulk.validate_zip(buffer, 2)
