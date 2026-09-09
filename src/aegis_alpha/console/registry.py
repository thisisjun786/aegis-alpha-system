"""Private resource pointers and notes, independent of market-store admission."""

from __future__ import annotations

import json
import os
import stat
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from aegis_alpha.data.descriptor_tree import DescriptorTree
from aegis_alpha.storage.locks import (
    file_lock,
    private_directory,
    private_file,
    require_outside_checkout,
)

MAX_BYTES = 1024 * 1024
MAX_RESOURCES = 100
_ID_LENGTH = 32
KINDS = frozenset({"file", "directory", "database"})


class ConflictError(ValueError):
    """The editor holds an older resource revision."""


def now() -> str:
    return datetime.now(UTC).isoformat()


def text_field(value: object, maximum: int, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or "\x00" in value:
        raise ValueError("입력 형식이나 길이를 확인해주세요.")
    if not empty and not value.strip():
        raise ValueError("필수 항목을 입력해주세요.")
    return value.strip()


def pointer_info(path: Path) -> dict[str, object]:
    require_outside_checkout(path)
    with DescriptorTree.open_path(path.parent) as tree:
        info = tree.stat(path.name)
    if info.st_uid != os.getuid() or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise ValueError("소유한 일반 파일이나 폴더만 등록할 수 있습니다.")
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
        raise ValueError("연결된 파일은 등록할 수 없습니다.")
    return {
        "status": "available",
        "bytes": info.st_size if stat.S_ISREG(info.st_mode) else None,
        "is_directory": stat.S_ISDIR(info.st_mode),
        "modified_at": datetime.fromtimestamp(info.st_mtime, UTC).isoformat(),
    }


class Registry:
    def __init__(self, root: Path) -> None:
        self.root = root
        private_directory(root, create=True)

    def _read(self) -> list[dict[str, object]]:
        with DescriptorTree.open_path(self.root) as tree:
            if not tree.exists("resources.json"):
                return []
            private_file(self.root / "resources.json")
            doc = json.loads(tree.read_bytes("resources.json", max_bytes=MAX_BYTES))
        if (
            not isinstance(doc, dict)
            or set(doc) != {"version", "resources"}
            or type(doc["version"]) is not int
            or doc["version"] != 1
        ):
            raise ValueError("지원하지 않는 리소스 목록입니다.")
        rows = doc["resources"]
        if not isinstance(rows, list) or len(rows) > MAX_RESOURCES:
            raise ValueError("리소스 목록 형식이 잘못됐습니다.")
        ids = set()
        for row in rows:
            if not isinstance(row, dict) or set(row) != {
                "id",
                "name",
                "path",
                "kind",
                "note",
                "revision",
                "created_at",
                "updated_at",
            }:
                raise ValueError("리소스 항목 형식이 잘못됐습니다.")
            rid = text_field(row["id"], 32)
            if (
                len(rid) != _ID_LENGTH
                or any(c not in "0123456789abcdef" for c in rid)
                or rid in ids
            ):
                raise ValueError("리소스 ID가 잘못됐습니다.")
            ids.add(rid)
            text_field(row["name"], 120)
            text_field(row["note"], 4000, empty=True)
            path = Path(text_field(row["path"], 4096))
            if not path.is_absolute() or ".." in path.parts or row["kind"] not in KINDS:
                raise ValueError("리소스 경로와 종류를 확인해주세요.")
            if type(row["revision"]) is not int or row["revision"] < 1:
                raise ValueError("리소스 버전이 잘못됐습니다.")
            for key in ("created_at", "updated_at"):
                datetime.fromisoformat(text_field(row[key], 64))
        return cast("list[dict[str, object]]", rows)

    def _write(self, rows: list[dict[str, object]]) -> None:
        payload = json.dumps(
            {"version": 1, "resources": rows}, ensure_ascii=False, allow_nan=False
        ).encode()
        if len(payload) > MAX_BYTES:
            raise ValueError("리소스 목록 저장 한도를 초과했습니다.")
        with DescriptorTree.open_path(self.root) as tree:
            tree.atomic_write_bytes("resources.json", payload)

    def resources(self) -> list[dict[str, object]]:
        private_directory(self.root)
        with file_lock(self.root / ".registry.lock"):
            rows = self._read()
        result = []
        for row in rows:
            try:
                info = pointer_info(Path(str(row["path"])))
            except (OSError, ValueError):
                info = {"status": "unavailable", "bytes": None}
            result.append({**row, **info})
        return result

    def create(self, body: dict[str, object]) -> dict[str, object]:
        if set(body) != {"name", "path", "kind", "note"}:
            raise ValueError("이름, 경로, 종류, 메모를 확인해주세요.")
        name = text_field(body["name"], 120)
        note = text_field(body["note"], 4000, empty=True)
        path = Path(text_field(body["path"], 4096))
        kind = text_field(body["kind"], 16)
        if not path.is_absolute() or ".." in path.parts or kind not in KINDS:
            raise ValueError("절대 경로와 올바른 종류를 입력해주세요.")
        info = pointer_info(path)
        if (kind == "directory") != info["is_directory"]:
            raise ValueError("선택한 종류가 실제 파일 또는 폴더와 다릅니다.")
        stamp = now()
        row: dict[str, object] = {
            "id": uuid.uuid4().hex,
            "name": name,
            "path": str(path),
            "kind": kind,
            "note": note,
            "revision": 1,
            "created_at": stamp,
            "updated_at": stamp,
        }
        with file_lock(self.root / ".registry.lock"):
            rows = self._read()
            if len(rows) >= MAX_RESOURCES:
                raise ValueError("리소스는 최대 100개까지 등록할 수 있습니다.")
            if any(r["path"] == str(path) for r in rows):
                raise ConflictError("이미 등록한 경로입니다. 기존 항목의 메모를 수정해주세요.")
            self._write([*rows, row])
        return row

    def update(self, rid: str, body: dict[str, object]) -> dict[str, object]:
        if set(body) != {"name", "note", "revision"} or type(body["revision"]) is not int:
            raise ValueError("이름, 메모, 버전을 확인해주세요.")
        name = text_field(body["name"], 120)
        note = text_field(body["note"], 4000, empty=True)
        with file_lock(self.root / ".registry.lock"):
            rows = self._read()
            row = next((r for r in rows if r["id"] == rid), None)
            if row is None:
                raise LookupError("리소스를 찾을 수 없습니다.")
            if row["revision"] != body["revision"]:
                raise ConflictError("다른 창에서 수정한 내용이 있습니다. 목록을 새로고침해주세요.")
            row.update(
                name=name, note=note, revision=cast("int", row["revision"]) + 1, updated_at=now()
            )
            self._write(rows)
            return row
