"""No-clobber content-addressed source bytes below an admitted private root."""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.locks import private_directory, private_file

_SHA256_LENGTH = 64


def put_raw_file(root: Path, source_path: Path) -> tuple[str, str, int]:
    """Stream a private snapshot into raw storage without retaining its bytes in memory."""
    private_directory(root)
    private_file(source_path)
    temporary = "." + uuid.uuid4().hex + ".tmp"
    with DescriptorTree.open_path(root) as target:
        try:
            with (
                DescriptorTree.open_path(source_path.parent) as origin,
                origin.binary_reader(source_path.name, require_single_link=True) as source,
                target.binary_writer(temporary, exclusive=True) as destination,
            ):
                before = os.fstat(source.fileno())
                digest = hashlib.sha256()
                size = 0
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    destination.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                after = os.fstat(source.fileno())
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    size,
                    after.st_mtime_ns,
                    after.st_ctime_ns,
                ):
                    raise ValueError("raw snapshot changed while copying")
                destination.flush()
                os.fsync(destination.fileno())
            value = digest.hexdigest()
            relative = value[:2] + "/" + value
            target.mkdir(value[:2], exist_ok=True)
            try:
                os.link(
                    temporary,
                    relative,
                    src_dir_fd=target.descriptor,
                    dst_dir_fd=target.descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                verify_raw(root, relative, value, size)
            target.fsync_directory(value[:2])
        finally:
            if target.exists(temporary):
                target.unlink(temporary)
                target.fsync_directory()
    return relative, value, size


def put_raw(root: Path, payload: bytes) -> tuple[str, str, int]:
    private_directory(root)
    digest = hashlib.sha256(payload).hexdigest()
    relative = digest[:2] + "/" + digest
    with DescriptorTree.open_path(root) as tree:
        tree.mkdir(digest[:2], exist_ok=True)
        if tree.exists(relative):
            existing = tree.read_bytes(relative, max_bytes=len(payload))
            if existing != payload:
                raise ValueError("raw hash path contains different bytes")
            return relative, digest, len(payload)
        temporary = digest[:2] + "/." + uuid.uuid4().hex + ".tmp"
        with tree.binary_writer(temporary, exclusive=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # link is atomic and fails if the final name already exists.
            try:
                os.link(
                    temporary,
                    relative,
                    src_dir_fd=tree.descriptor,
                    dst_dir_fd=tree.descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                if tree.read_bytes(relative, max_bytes=len(payload)) != payload:
                    raise ValueError("raw hash path contains different bytes") from None
            tree.fsync_directory(digest[:2])
        finally:
            tree.unlink(temporary)
            tree.fsync_directory(digest[:2])
    return relative, digest, len(payload)


def verify_raw(root: Path, relative: str, digest: str, size: int) -> None:
    if relative != digest[:2] + "/" + digest or len(digest) != _SHA256_LENGTH:
        raise ValueError("raw reference must match its content hash")
    with (
        DescriptorTree.open_path(root) as tree,
        tree.binary_reader(relative, require_single_link=True) as source,
    ):
        hasher = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
        observed = hasher.hexdigest()
        if observed != digest or os.fstat(source.fileno()).st_size != size:
            raise ValueError("raw source checksum or size mismatch")
