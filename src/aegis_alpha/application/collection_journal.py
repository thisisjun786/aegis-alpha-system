from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

from aegis_alpha.application.data_config import _pairs
from aegis_alpha.data.descriptor_tree import DescriptorTree

_MAX_RECORD_BYTES = 8 * 1024 * 1024
_MAX_RECORD_FILES = 100_000
_HASH = re.compile(r"[0-9a-f]{64}")
_START_NAME = re.compile(r"([0-9]{4}-[0-9]{2}-[0-9]{2})\.([a-z][a-z0-9_]{0,31})\.start\.json")
_START_KEYS = {
    "version",
    "service_day",
    "provider",
    "invocation_id",
    "profile_sha256",
    "reserved_calls",
    "started_at_utc",
}
_RESULT_KEYS = {
    "version",
    "service_day",
    "provider",
    "start_sha256",
    "finished_at_utc",
    "status",
    "calls_attempted",
    "result",
}


class CollectionBusyError(RuntimeError):
    """Another automatic collection holds the shared journal lock."""


def encoded(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def validate_root(root: Path) -> None:
    if not root.is_absolute() or ".." in root.parts or root.is_symlink():
        raise ValueError("collection state needs an absolute directory without aliases")
    if any((path / ".git").exists() for path in (root, *root.parents)):
        raise ValueError("collection state must be outside Git")


def _private(tree: DescriptorTree) -> None:
    info = os.fstat(tree.descriptor)
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("collection state directory must be private and owned by this user")


def _create_root(root: Path) -> None:
    validate_root(root)
    with DescriptorTree.open_path(Path(root.anchor)) as tree:
        for index in range(1, len(root.parts)):
            relative = Path(*root.parts[1 : index + 1])
            tree.mkdir(relative, mode=0o700, exist_ok=True)
            tree.fsync_directory(relative.parent)


def publish(tree: DescriptorTree, name: str, record: Mapping[str, object]) -> None:
    payload = encoded(record)
    if len(payload) > _MAX_RECORD_BYTES:
        raise ValueError("collection receipt exceeds the supported size")
    temporary = ".journal-" + uuid4().hex + ".tmp"
    try:
        with tree.binary_writer(temporary, exclusive=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(
            temporary,
            name,
            src_dir_fd=tree.descriptor,
            dst_dir_fd=tree.descriptor,
            follow_symlinks=False,
        )
        tree.fsync_directory()
    finally:
        tree.unlink(temporary, missing_ok=True)


def _read(tree: DescriptorTree, name: str, keys: set[str]) -> dict[str, object]:
    value = json.loads(tree.read_bytes(name, max_bytes=_MAX_RECORD_BYTES), object_pairs_hook=_pairs)
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise ValueError("invalid collection journal record")
    return value


class JournalEntry(TypedDict):
    start: dict[str, object]
    finish: dict[str, object] | None


def records(tree: DescriptorTree) -> list[JournalEntry]:
    result: list[JournalEntry] = []
    names = set(os.listdir(tree.descriptor))  # noqa: PTH208 -- list the pinned directory descriptor
    if len(names) > _MAX_RECORD_FILES:
        raise ValueError("collection journal has too many record files")
    for name in names:
        if (
            name.endswith(".result.json")
            and name.removesuffix("result.json") + "start.json" not in names
        ):
            raise ValueError("collection result has no admission record")
    for name in sorted(names):
        match = _START_NAME.fullmatch(name)
        if match is None:
            continue
        start = _read(tree, name, _START_KEYS)
        _validate_start(start)
        if (
            (start["service_day"], start["provider"]) != match.groups()
            or type(start["reserved_calls"]) is not int
            or start["reserved_calls"] < 1
        ):
            raise ValueError("collection journal identity or reserved budget is invalid")
        entry: JournalEntry = {"start": start, "finish": None}
        finish_name = name.removesuffix("start.json") + "result.json"
        if tree.exists(finish_name):
            finish = _read(tree, finish_name, _RESULT_KEYS)
            _validate_finish(finish)
            if (finish["service_day"], finish["provider"]) != match.groups() or finish[
                "start_sha256"
            ] != hashlib.sha256(encoded(start)).hexdigest():
                raise ValueError("collection result is not bound to its original admission")
            calls = finish["calls_attempted"]
            if calls is not None and (
                type(calls) is not int or not 0 <= calls <= start["reserved_calls"]
            ):
                raise ValueError("collection result has an invalid call count")
            entry["finish"] = finish
        result.append(entry)
    return result


def _aware(value: object) -> None:
    if not isinstance(value, str):
        raise TypeError("journal timestamp must be a string")
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError("journal timestamp is invalid") from None
    if moment.utcoffset() is None:
        raise ValueError("journal timestamp must be timezone-aware")


def _validate_start(start: dict[str, object]) -> None:
    digest = start["profile_sha256"]
    identifier = start["invocation_id"]
    if not isinstance(digest, str) or _HASH.fullmatch(digest) is None:
        raise ValueError("journal profile hash is invalid")
    if (
        not isinstance(identifier, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", identifier) is None
    ):
        raise ValueError("journal invocation identity is invalid")
    date.fromisoformat(str(start["service_day"]))
    _aware(start["started_at_utc"])


def _validate_finish(finish: dict[str, object]) -> None:
    status = finish["status"]
    if not isinstance(status, str) or status not in {
        "succeeded",
        "failed",
        "blocked",
        "unknown",
        "partial",
    }:
        raise ValueError("journal completion status is invalid")
    if not isinstance(finish["result"], dict):
        raise TypeError("journal completion result must be an object")
    _aware(finish["finished_at_utc"])


@contextmanager
def collection_lock(root: Path) -> Iterator[DescriptorTree]:
    _create_root(root)
    with DescriptorTree.open_path(root) as tree:
        _private(tree)
        with tree.binary_writer(".daily.lock", append=True, truncate=False) as handle:
            info = os.fstat(handle.fileno())
            if (
                info.st_uid != os.geteuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) & 0o077
            ):
                raise ValueError("collection lock must be private, owned and singly linked")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CollectionBusyError("daily collection is already running") from None
            try:
                revalidate_root(tree, root)
                yield tree
                revalidate_root(tree, root)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def revalidate_root(tree: DescriptorTree, root: Path) -> None:
    with DescriptorTree.open_path(root) as current:
        if current.identity != tree.identity:
            raise ValueError("collection state directory changed during execution")


def journal_status(root: Path) -> dict[str, object]:
    validate_root(root)
    if not root.exists():
        return {"active": False, "records": []}
    with DescriptorTree.open_path(root) as tree:
        _private(tree)
        active = False
        if tree.exists(".daily.lock"):
            with tree.binary_reader(".daily.lock", require_single_link=True) as handle:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    active = True
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        result = records(tree)
        revalidate_root(tree, root)
        return {"active": active, "records": result}
