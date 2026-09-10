"""One cooperating writer and immutable descriptor-relative Qveris evidence."""

from __future__ import annotations

import fcntl
import hashlib
import os
import secrets
from contextlib import AbstractContextManager, nullcontext
from decimal import Decimal
from pathlib import Path
from typing import Self

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.data.qveris_contracts import (
    MAX_RESPONSE_BYTES,
    credit_value,
    load_json,
    object_value,
)
from aegis_alpha.data.sec_evidence import open_directory
from aegis_alpha.data.serialization import canonical_json_bytes


class QverisDestinationError(RuntimeError):
    """Evidence root admission failed before provider work."""


def validate_destination(label: str, destination: Path) -> Path:
    """Admit roots using the retained sec_collector rule without its runtime imports."""
    if not destination.is_absolute() or ".." in destination.parts:
        raise QverisDestinationError(f"{label} must be an absolute path without parent traversal")
    if any((parent / ".git").exists() for parent in (destination, *destination.parents)):
        raise QverisDestinationError(f"{label} must be outside a Git repository")
    if destination.is_symlink():
        raise QverisDestinationError(f"{label} must not be a symlink")
    ancestor = destination if destination.is_dir() else destination.parent
    while not ancestor.exists():
        ancestor = ancestor.parent
    try:
        with open_directory(ancestor):
            pass
    except (ValueError, OSError) as error:
        raise QverisDestinationError(f"{label} must not traverse a symlink") from error
    return destination


class QverisStore(AbstractContextManager["QverisStore"]):
    def __init__(self, root: Path, account_key: str) -> None:
        self.root = validate_destination("Qveris evidence root", root)
        self.account_key = account_key
        self.tree: DescriptorTree | None = None

    def __enter__(self) -> Self:
        tree = open_directory(self.root, create=True)
        try:
            fcntl.flock(tree.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.tree = tree
            self.publish(
                "credential-binding.json",
                canonical_json_bytes({"credential_sha256": self.account_key}),
            )
            self.assert_owned()
        except BaseException:
            self.tree = None
            tree.close()
            raise
        return self

    def __exit__(self, *_args: object) -> None:
        tree = self.tree
        self.tree = None
        if tree is not None:
            tree.close()

    def _tree(self) -> DescriptorTree:
        if self.tree is None:
            raise RuntimeError("Qveris evidence lock is not held")
        return self.tree

    def assert_owned(self) -> None:
        tree = self._tree()
        with DescriptorTree.open_path(self.root) as visible:
            if visible.identity != tree.identity:
                raise RuntimeError("Qveris evidence root changed during execution")

    def exists(self, relative: str) -> bool:
        self.assert_owned()
        return self._tree().exists(relative)

    def read(self, relative: str, maximum: int = MAX_RESPONSE_BYTES) -> bytes:
        self.assert_owned()
        tree = self._tree()
        before = tree.stat(relative)
        payload = tree.read_bytes(relative, max_bytes=maximum)
        after = tree.stat(relative)
        if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ValueError("Qveris evidence changed during read")
        self.assert_owned()
        return payload

    def document(self, relative: str) -> dict[str, object]:
        return object_value(load_json(self.read(relative)))

    def publish_document(self, relative: str, document: object) -> None:
        self.publish(relative, canonical_json_bytes(document))

    def publish(self, relative: str, payload: bytes) -> None:
        self.assert_owned()
        tree = self._tree()
        parent = Path(relative).parent.as_posix()
        if parent != ".":
            tree.mkdir(parent, parents=True, exist_ok=True)
        if tree.exists(relative):
            if self.read(relative, len(payload)) != payload:
                raise ValueError("immutable Qveris evidence conflicts")
            return
        with nullcontext(tree) if parent == "." else tree.subtree(parent) as directory:
            temp = f".qveris-{secrets.token_hex(16)}.tmp"
            with directory.binary_writer(temp, exclusive=True) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(
                    temp,
                    Path(relative).name,
                    src_dir_fd=directory.descriptor,
                    dst_dir_fd=directory.descriptor,
                    follow_symlinks=False,
                )
                directory.fsync_directory()
            except FileExistsError:
                if self.read(relative, len(payload)) != payload:
                    raise ValueError("immutable Qveris evidence conflicts") from None
            finally:
                directory.unlink(temp, missing_ok=True)
        self.assert_owned()

    def pin(self, relative: str) -> dict[str, object]:
        payload = self.read(relative)
        return {
            "path": relative,
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    def verify(self, pin: object) -> None:
        expected = object_value(pin)
        path = expected.get("path")
        if not isinstance(path, str) or self.pin(path) != expected:
            raise ValueError("Qveris evidence pin differs")

    def pending_pages(self, *, include_quarantined: bool = False) -> tuple[str, ...]:
        tree = self._tree()
        if not tree.exists("jobs"):
            return ()
        result = []
        quarantined_batches = set(self.pending_batches(include_quarantined=True)) - set(
            self.pending_batches()
        )
        for job in tree.listdir("jobs"):
            base = f"jobs/{job}"
            for name in tree.listdir(base):
                if name.endswith(".intent.json"):
                    page = f"{base}/{name.removesuffix('.intent.json')}"
                    if (
                        not include_quarantined
                        and self.document(f"{page}.intent.json").get("batch_id")
                        in quarantined_batches
                    ):
                        continue
                    if (
                        not include_quarantined
                        and tree.exists(f"{page}.quarantine.json")
                        and not tree.exists(f"{page}.billing.json")
                    ):
                        continue
                    if (
                        not tree.exists(f"{page}.billing.json")
                        or self.document(f"{page}.billing.json").get("over_quote") is not False
                    ):
                        result.append(page)
        return tuple(result)

    def reserved_credits(self) -> Decimal:
        reserved = sum(
            (
                credit_value(self.document(f"{base}/manifest.json")["reserved_credits"])
                for base in self.pending_batches(include_quarantined=True)
            ),
            Decimal(0),
        )
        for page in self.pending_pages(include_quarantined=True):
            quarantine = f"{page}.quarantine.json"
            if self.exists(quarantine) and not self.exists(f"{page}.billing.json"):
                document = self.document(quarantine)
                self.verify(document["intent"])
                amount = credit_value(self.document(f"{page}.intent.json")["quoted_credits"])
                if credit_value(document.get("reserved_credits")) != amount:
                    raise ValueError("quarantine reservation differs from intent")
                reserved += amount
        return reserved

    def pending_batches(self, *, include_quarantined: bool = False) -> tuple[str, ...]:
        if not self._tree().exists("parallel-batches"):
            return ()
        pending = tuple(
            f"parallel-batches/{name}"
            for name in self._tree().listdir("parallel-batches")
            if self.exists(f"parallel-batches/{name}/manifest.json")
            and not self.exists(f"parallel-batches/{name}/complete.json")
        )
        results = []
        for base in pending:
            if self.exists(f"{base}/quarantine.json"):
                record = self.document(f"{base}/quarantine.json")
                self.verify(record["manifest"])
                if not include_quarantined:
                    continue
            results.append(base)
        return tuple(results)

    def quarantined_batch_fingerprints(self) -> set[str]:
        results = set()
        for base in self.pending_batches(include_quarantined=True):
            if self.exists(f"{base}/quarantine.json"):
                intents = self.document(f"{base}/manifest.json")["intents"]
                if not isinstance(intents, list):
                    raise ValueError("invalid batch intents")
                results.update(str(object_value(item)["fingerprint"]) for item in intents)
        return results

    def require_no_pending_batches(self) -> None:
        if self.pending_batches():
            raise RuntimeError("PENDING_BATCH: use parallel recovery before serial collection")
