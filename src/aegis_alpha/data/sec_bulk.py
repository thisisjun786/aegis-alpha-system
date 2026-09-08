"""Owner-directed acquisition of the two official SEC bulk archives.

This raw-only path does not run or authorize the retained SEC verifier.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import stat
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path, PurePosixPath
from typing import IO, Protocol, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.sec_evidence import open_directory, publish_bytes, read_bytes
from aegis_alpha.data.sec_transport import assert_user_agent_absent, validate_user_agent
from aegis_alpha.data.serialization import canonical_json_bytes

ARCHIVES = {
    "companyfacts": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
    "submissions": "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
}
CHUNK_BYTES = 1024 * 1024
DISK_RESERVE = 1024 * 1024 * 1024
MAX_WORKERS = 64
MAX_MEMBERS = 1_000_000
MAX_EXPANDED_BYTES = 512 * 1024 * 1024 * 1024


class BulkError(ValueError):
    """An archive cannot be admitted as complete evidence."""


class Response(Protocol):
    status: int
    headers: Mapping[str, str]

    def geturl(self) -> str: ...
    def read(self, amount: int) -> bytes: ...
    def close(self) -> None: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        raise BulkError("SEC bulk redirects are refused")


@contextmanager
def open_archive(url: str, user_agent: str) -> Iterator[Response]:
    if url not in ARCHIVES.values():
        raise BulkError("SEC bulk URL is not an authorized archive")
    request = urllib.request.Request(  # noqa: S310 -- exact HTTPS allowlist above
        url, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"}
    )
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        response = cast("Response", opener.open(request, timeout=60))
    except urllib.error.HTTPError as error:
        raise BulkError(f"SEC bulk HTTP {error.code}; no automatic retry") from None
    except (urllib.error.URLError, TimeoutError):
        raise BulkError("SEC bulk connection failed; no automatic retry") from None
    try:
        yield response
    finally:
        response.close()


def archive_plan(name: str, output_root: Path, max_bytes: int) -> dict[str, object]:
    if name not in ARCHIVES:
        raise BulkError("unknown SEC bulk archive")
    if not output_root.is_absolute() or ".." in output_root.parts:
        raise BulkError("SEC bulk output root must be absolute without traversal")
    if any((parent / ".git").exists() for parent in (output_root, *output_root.parents)):
        raise BulkError("SEC bulk output must be outside Git")
    if type(max_bytes) is not int or max_bytes < 1:
        raise BulkError("SEC bulk max bytes must be positive")
    return {
        "archive": name,
        "url": ARCHIVES[name],
        "output_root": str(output_root),
        "max_bytes": max_bytes,
        "raw_only": True,
        "scheduled": False,
    }


def _hash(handle: IO[bytes]) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: handle.read(CHUNK_BYTES), b""):
        size += len(chunk)
        digest.update(chunk)
    return digest.hexdigest(), size


def _validate_members(archive: zipfile.ZipFile, members: list[zipfile.ZipInfo]) -> None:
    for item in members:
        path = PurePosixPath(item.filename)
        mode = item.external_attr >> 16
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in item.filename
            or stat.S_ISLNK(mode)
            or item.flag_bits & 1
            or (not item.filename.endswith(".json") and item.filename != "placeholder.txt")
        ):
            raise BulkError("SEC ZIP contains an unsupported member")
        if item.filename == "placeholder.txt" and (
            item.file_size != len(b"Placeholder file") or archive.read(item) != b"Placeholder file"
        ):
            raise BulkError("SEC ZIP placeholder metadata differs from official format")


def validate_zip(handle: IO[bytes], workers: int) -> tuple[int, int]:
    try:
        with zipfile.ZipFile(handle) as archive:
            members = archive.infolist()
            if not members or len(members) > MAX_MEMBERS:
                raise BulkError("SEC ZIP member count is invalid")
            names = [item.filename for item in members]
            if len(names) != len(set(names)):
                raise BulkError("SEC ZIP contains duplicate members")
            expanded = sum(item.file_size for item in members)
            if expanded > MAX_EXPANDED_BYTES:
                raise BulkError("SEC ZIP expanded bytes exceed limit")
            _validate_members(archive, members)

            # Full decompression/CRC pass, bounded memory; never extract filenames.
            def check_group(group: list[zipfile.ZipInfo]) -> None:
                for item in group:
                    with archive.open(item) as entry:
                        for _chunk in iter(lambda: entry.read(CHUNK_BYTES), b""):
                            pass

            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(check_group, [members[index::workers] for index in range(workers)]))
            return len(members), expanded
    except (zipfile.BadZipFile, NotImplementedError, RuntimeError, EOFError) as error:
        raise BulkError("SEC ZIP integrity validation failed") from error


def _existing(tree: DescriptorTree, name: str, max_bytes: int, workers: int) -> dict[str, object]:
    receipt = json.loads(read_bytes(tree.logical_root / f"{name}.receipt.json"))
    if (
        receipt.get("contract") != "aas-sec-bulk/v1"
        or receipt.get("url") != ARCHIVES[name]
        or receipt.get("final_url") != ARCHIVES[name]
        or receipt.get("status") != HTTPStatus.OK
        or receipt.get("archive") != f"{name}.zip"
    ):
        raise BulkError("SEC bulk existing receipt identity mismatch")
    with tree.binary_reader(f"{name}.zip") as handle:
        digest, size = _hash(handle)
        handle.seek(0)
        members, expanded = validate_zip(handle, workers)
    if (
        size > max_bytes
        or receipt.get("sha256") != digest
        or receipt.get("bytes") != size
        or receipt.get("members") != members
        or receipt.get("expanded_bytes") != expanded
    ):
        raise BulkError("SEC bulk existing archive differs from receipt")
    return {**receipt, "reused": True, "http_calls": 0}


def _receive(
    tree: DescriptorTree, temporary: str, response: Response, max_bytes: int
) -> tuple[str, int]:
    length = response.headers.get("Content-Length")
    expected = int(length) if length is not None else None
    if expected is not None and not 0 < expected <= max_bytes:
        raise BulkError("SEC bulk declared length exceeds bounds")
    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
        raise BulkError("SEC bulk requires identity content encoding")
    digest = hashlib.sha256()
    size = 0
    with tree.binary_writer(temporary, exclusive=True) as handle:
        try:
            for chunk in iter(lambda: response.read(CHUNK_BYTES), b""):
                size += len(chunk)
                if size > max_bytes:
                    raise BulkError("SEC bulk received bytes exceed limit")
                handle.write(chunk)
                digest.update(chunk)
        finally:
            handle.flush()
            os.fsync(handle.fileno())
    if expected is not None and size != expected:
        raise BulkError("SEC bulk response length mismatch")
    return digest.hexdigest(), size


def acquire_archive(  # noqa: PLR0913 -- bounded, explicit acquisition inputs
    name: str,
    output_root: Path,
    *,
    user_agent: str,
    max_bytes: int,
    workers: int = 1,
    transport: Callable[..., object] = open_archive,
) -> dict[str, object]:
    archive_plan(name, output_root, max_bytes)
    validate_user_agent(user_agent)
    if "\r" in user_agent or "\n" in user_agent:
        raise BulkError("SEC User-Agent contains a line break")
    if type(workers) is not int or not 1 <= workers <= MAX_WORKERS:
        raise BulkError("SEC bulk worker count must be between 1 and 64")
    started = datetime.now(UTC).isoformat()
    with open_directory(output_root, create=True) as tree:
        try:
            fcntl.flock(tree.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise BulkError("SEC bulk output is already in use") from None
        if tree.exists(f"{name}.receipt.json"):
            return _existing(tree, name, max_bytes, workers)
        if tree.exists(f"{name}.zip"):
            raise BulkError("SEC bulk archive lacks receipt; preserve for local recovery")
        space = os.fstatvfs(tree.descriptor)
        if space.f_bavail * space.f_frsize < max_bytes + DISK_RESERVE:
            raise BulkError("SEC bulk free disk is below max bytes plus reserve")
        tree.mkdir("partial", exist_ok=True)
        run_id = f"{name}-{secrets.token_hex(12)}"
        temporary = f"partial/{run_id}.zip"
        from contextlib import AbstractContextManager  # noqa: PLC0415 -- typing transport adapter

        context = cast("AbstractContextManager[Response]", transport(ARCHIVES[name], user_agent))
        with context as response:
            if response.status != HTTPStatus.OK or response.geturl() != ARCHIVES[name]:
                raise BulkError("SEC bulk response status or final URL is invalid")
            metadata = {
                key: response.headers[key]
                for key in ("ETag", "Last-Modified", "Content-Length")
                if key in response.headers
            }
            assert_user_agent_absent(user_agent, canonical_json_bytes(metadata))
            context_record = {
                "contract": "aas-sec-bulk-response/v1",
                "run_id": run_id,
                "url": ARCHIVES[name],
                "final_url": response.geturl(),
                "status": response.status,
                "headers": metadata,
                "started_at_utc": started,
            }
            publish_bytes(
                output_root / "partial" / f"{run_id}.response.json",
                canonical_json_bytes(context_record),
            )
            digest, size = _receive(tree, temporary, response, max_bytes)
        with tree.binary_reader(temporary) as handle:
            members, expanded = validate_zip(handle, workers)
        receipt = {
            "contract": "aas-sec-bulk/v1",
            "run_id": run_id,
            "url": ARCHIVES[name],
            "final_url": ARCHIVES[name],
            "status": 200,
            "archive": f"{name}.zip",
            "bytes": size,
            "sha256": digest,
            "members": members,
            "expanded_bytes": expanded,
            "headers": metadata,
            "started_at_utc": started,
            "finished_at_utc": datetime.now(UTC).isoformat(),
            "tool_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "raw_only": True,
            "catalog_registered": False,
        }
        encoded = canonical_json_bytes(receipt)
        assert_user_agent_absent(user_agent, encoded)
        with tree.open_directory("partial") as partial:
            os.link(
                f"{run_id}.zip",
                f"{name}.zip",
                src_dir_fd=partial,
                dst_dir_fd=tree.descriptor,
                follow_symlinks=False,
            )
        tree.fsync_directory()
        publish_bytes(output_root / f"{name}.receipt.json", encoded)
        tree.unlink(temporary)
        tree.fsync_directory("partial")
        return {**receipt, "reused": False, "http_calls": 1}
