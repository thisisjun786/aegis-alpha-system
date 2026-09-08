"""Descriptor-relative, immutable SEC evidence with bounded verified reads."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.serialization import canonical_json_bytes

MAX_DOCUMENT_BYTES = 16 * 1024 * 1024


def open_directory(path: Path, *, create: bool = False) -> DescriptorTree:
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("SEC evidence requires an absolute path without parent traversal")
    if create:
        with DescriptorTree.open_path(Path(path.anchor)) as root:
            relative = path.relative_to(path.anchor).as_posix()
            root.mkdir(relative, parents=True, exist_ok=True)
    return DescriptorTree.open_path(path)


def read_bytes(path: Path, *, maximum: int = MAX_DOCUMENT_BYTES) -> bytes:
    with open_directory(path.parent) as tree:
        before = tree.stat(path.name)
        content = tree.read_bytes(path.name, max_bytes=maximum)
        if _state(before) != _state(tree.stat(path.name)):
            raise ValueError("SEC evidence changed while reading")
        _require_visible_tree(tree)
        return content


def publish_bytes(path: Path, content: bytes) -> bool:
    """Publish no-clobber bytes; retain partial bundles for forensic recovery."""
    with open_directory(path.parent, create=True) as tree:
        if tree.exists(path.name):
            if read_bytes(path, maximum=len(content)) != content:
                raise FileExistsError("SEC immutable evidence conflicts")
            return False
        temporary = f".sec-{secrets.token_hex(12)}.tmp"
        created = False
        try:
            with tree.binary_writer(temporary, exclusive=True) as handle:
                created = True
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(
                    temporary,
                    path.name,
                    src_dir_fd=tree.descriptor,
                    dst_dir_fd=tree.descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                if tree.read_bytes(path.name, max_bytes=len(content)) != content:
                    raise FileExistsError("SEC immutable evidence conflicts") from None
            tree.fsync_directory()
            with open_directory(path.parent) as visible:
                if visible.identity != tree.identity:
                    raise ValueError("SEC evidence directory changed during publication")
        finally:
            if created:
                tree.unlink(temporary, missing_ok=True)
    return True


def _require_visible_tree(tree: DescriptorTree) -> None:
    with open_directory(tree.logical_root) as visible:
        if visible.identity != tree.identity:
            raise ValueError("SEC evidence directory changed during read")


def _state(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def file_pin(root: Path, path: Path) -> dict[str, object]:
    relative = path.relative_to(root).as_posix()
    with open_directory(root) as tree, tree.binary_reader(relative) as handle:
        before = os.fstat(handle.fileno())
        digest = hashlib.sha256()
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
        if _state(before) != _state(os.fstat(handle.fileno())) or _state(before) != _state(
            tree.stat(relative)
        ):
            raise ValueError("SEC evidence changed while hashing")
        _require_visible_tree(tree)
    return {
        "relative_path": relative,
        "size_bytes": before.st_size,
        "content_sha256": digest.hexdigest(),
    }


def verify_pin(root: Path, pin: Mapping[str, object]) -> None:
    if set(pin) != {"relative_path", "size_bytes", "content_sha256"}:
        raise ValueError("invalid SEC evidence pin")
    relative = pin["relative_path"]
    if (
        not isinstance(relative, str)
        or Path(relative).is_absolute()
        or ".." in Path(relative).parts
    ):
        raise ValueError("invalid SEC evidence path")
    if type(pin["size_bytes"]) is not int or pin["size_bytes"] < 0:
        raise ValueError("invalid SEC evidence size")
    if file_pin(root, root / relative) != dict(pin):
        raise ValueError("SEC evidence hash or size mismatch")


def _pairs(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate SEC evidence field")
        result[key] = value
    return result


def read_document(path: Path) -> dict[str, object]:
    payload = read_bytes(path)
    document = json.loads(payload, object_pairs_hook=_pairs)
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise ValueError("SEC evidence must be a canonical JSON object")
    return cast("dict[str, object]", document)
