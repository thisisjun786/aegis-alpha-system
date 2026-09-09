"""Short-lived read-only inventory through the existing storage owner."""

from __future__ import annotations

# ruff: noqa: TRY301 -- inventory preserves file status on admission failure.
import shutil
import threading
import time
from pathlib import Path
from typing import cast

from aegis_alpha.console.registry import now, pointer_info
from aegis_alpha.storage.inspection import (
    LIMIT,
    list_datasets,
    list_runs,
    preview_source,
    source_catalog,
)
from aegis_alpha.storage.locks import private_directory
from aegis_alpha.storage.paths import load_paths, resolve_home
from aegis_alpha.storage.research_coverage import coverage, read_deadline
from aegis_alpha.storage.research_inspection import strategies
from aegis_alpha.storage.strategies import list_strategies
from aegis_alpha.storage.workspace import open_workspace

_CACHE_SECONDS = 60


class Catalog:
    def __init__(self, home: Path) -> None:
        self.home = resolve_home(home)
        self.lock = threading.Lock()
        self._strategy_snapshot: dict[str, object] | None = None
        self._strategy_captured = 0.0
        self._coverage_snapshot: dict[str, object] | None = None
        self._coverage_captured = 0.0

    def _inspect(self, kind: str) -> dict[str, object]:
        snapshot_name = f"_{kind}_snapshot"
        captured_name = f"_{kind}_captured"
        snapshot = cast("dict[str, object] | None", getattr(self, snapshot_name))
        captured = cast("float", getattr(self, captured_name))
        if snapshot is not None and time.monotonic() - captured <= _CACHE_SECONDS:
            return snapshot
        if not self.lock.acquire(blocking=False):
            raise RuntimeError("installation_busy")
        try:
            with open_workspace(self.home) as workspace, read_deadline(workspace):
                snapshot = (strategies if kind == "strategy" else coverage)(workspace)
                snapshot["observed_at"] = now()
                setattr(self, snapshot_name, snapshot)
                setattr(self, captured_name, time.monotonic())
                return snapshot
        finally:
            self.lock.release()

    def strategies(
        self, *, collection: str | None, query: str, country: str, offset: int
    ) -> dict[str, object]:
        snapshot = self._inspect("strategy")
        collections = cast("list[dict[str, object]]", snapshot["collections"])
        selected = collection or (str(collections[0]["id"]) if collections else None)
        records: list[dict[str, object]] = []
        if selected is not None:
            item = next((value for value in collections if value["id"] == selected), None)
            if item is None:
                raise LookupError("collection absent")
            records = cast("list[dict[str, object]]", item["records"])
        needle = query.casefold()
        filtered = [
            record
            for record in records
            if (
                not needle
                or needle in str(record["name"]).casefold()
                or needle in " ".join(cast("list[str]", record["assets"] or [])).casefold()
                or needle
                in " ".join(cast("list[str]", record["defensive_assets"] or [])).casefold()
            )
            and (not country or record["country"] == country)
        ]
        return {
            "collections": [
                {key: value for key, value in item.items() if key != "records"}
                | {"total": len(cast("list[object]", item["records"]))}
                for item in collections
            ],
            "collection_id": selected,
            "records": filtered[offset : offset + 100],
            "total": len(filtered),
            "offset": offset,
            "limit": 100,
            "observed_at": snapshot["observed_at"],
            "status": "partial" if snapshot["truncated"] else "ready",
            "message": "표시 한도에 도달해 일부 자료만 확인했습니다."
            if snapshot["truncated"]
            else "",
            "truncated": snapshot["truncated"],
        }

    def coverage(self, *, query: str, category: str, offset: int) -> dict[str, object]:
        snapshot = self._inspect("coverage")
        all_items = cast("list[dict[str, object]]", snapshot["items"])
        groups = []
        for name in ("us_equity", "kr_equity", "us_etf", "kr_etf", "other", "unknown"):
            matched = [item for item in all_items if item["category"] == name]
            groups.append(
                {
                    "category": name,
                    "label": name,
                    "instruments": len(matched),
                    "first_date": min((str(item["first_date"]) for item in matched), default=None),
                    "last_date": max((str(item["last_date"]) for item in matched), default=None),
                    "oldest_last_date": min(
                        (str(item["last_date"]) for item in matched), default=None
                    ),
                    "rows": sum(int(cast("int", item["rows"])) for item in matched),
                }
            )
        needle = query.casefold()
        filtered = [
            item
            for item in all_items
            if (not category or item["category"] == category)
            and (
                not needle
                or needle in str(item["symbol"]).casefold()
                or needle in str(item["name"] or "").casefold()
            )
        ]
        return {
            "groups": groups,
            "items": filtered[offset : offset + 100],
            "total": len(filtered),
            "offset": offset,
            "limit": 100,
            "observed_at": snapshot["observed_at"],
            "status": "partial" if snapshot["truncated"] else "ready",
            "message": "표시 한도에 도달해 일부 자료만 확인했습니다."
            if snapshot["truncated"]
            else "",
            "truncated": bool(snapshot["truncated"]),
            "unclassified_tables": snapshot["unclassified_tables"],
            "invalid_dates": snapshot["invalid_dates"],
            "invalid_values": snapshot["invalid_values"],
            "native_status": snapshot["native_status"],
        }

    def overview(self) -> dict[str, object]:
        result: dict[str, object] = {
            "home": str(self.home),
            "observed_at": now(),
            "stores": [],
            "sources": [],
            "strategies": [],
            "datasets": [],
            "runs": [],
            "disk": None,
            "catalog_status": "missing",
            "message": "",
            "limit": LIMIT,
            "truncated": False,
        }
        try:
            private_directory(self.home)
            paths = load_paths(self.home)
            stores = []
            for kind in ("state", "strategies", "market", "raw", "runs", "backups"):
                path = getattr(paths, kind)
                try:
                    info = pointer_info(path)
                except (OSError, ValueError):
                    info = {"status": "unavailable", "bytes": None}
                stores.append({"kind": kind, "path": str(path), "modified_at": None, **info})
            result["stores"] = stores
            disk = shutil.disk_usage(self.home)
            result["disk"] = {"total": disk.total, "free": disk.free}
            if not self.lock.acquire(blocking=False):
                raise RuntimeError("installation_busy")
            try:
                with open_workspace(self.home, require_strategies=False) as workspace:
                    result["datasets"] = list_datasets(workspace.state)
                    result["runs"] = list_runs(workspace.state)
                    if workspace.strategies is None:
                        result.update(
                            catalog_status="partial",
                            message="전략 저장소가 없어 일부 목록만 표시합니다.",
                        )
                    else:
                        sources, source_total = source_catalog(workspace)
                        result["sources"] = sources
                        result["source_total"] = source_total
                        result["truncated"] = source_total > len(sources) or any(
                            s["metadata_limited"] for s in sources
                        )
                        strategies = list_strategies(workspace.strategies)
                        result["strategies"] = strategies[:LIMIT]
                        result["truncated"] = bool(result["truncated"]) or len(strategies) >= LIMIT
                        result.update(catalog_status="ready", message="")
            finally:
                self.lock.release()
        except RuntimeError:
            result.update(
                catalog_status="busy",
                message="다른 작업이 저장소를 사용 중입니다. 잠시 후 새로고침해주세요.",
            )
        except FileNotFoundError:
            result.update(
                catalog_status="missing",
                message="설치된 저장소를 찾지 못했습니다. 실행 시 지정한 경로를 확인해주세요.",
            )
        except (OSError, ValueError, TypeError, KeyError):
            result.update(
                catalog_status="invalid",
                message="저장소 경로나 설치 상태를 확인할 수 없습니다. aas doctor로 확인해주세요.",
            )
        return result

    def sample(self, source: str, table: str) -> dict[str, object]:
        if not self.lock.acquire(blocking=False):
            raise RuntimeError("installation_busy")
        try:
            with open_workspace(self.home) as workspace:
                return preview_source(workspace, source, table)
        finally:
            self.lock.release()
