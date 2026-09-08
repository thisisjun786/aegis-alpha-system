from __future__ import annotations

# ruff: noqa: PLC0415 -- descriptor helpers load only for explicit data configuration reads.
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

_MAX_CONFIG_BYTES = 1024 * 1024
_MAX_SECRET_BYTES = 16 * 1024


@dataclass(frozen=True, slots=True)
class DatasetLocation:
    dataset_id: str
    dataset_version: str
    root: Path

    def __post_init__(self) -> None:
        if any(
            not isinstance(v, str) or not v or v != v.strip()
            for v in (self.dataset_id, self.dataset_version)
        ):
            raise ValueError("dataset location needs an exact ID and version")
        if not isinstance(self.root, Path) or not self.root.is_absolute():
            raise ValueError("dataset root must be absolute")


@dataclass(frozen=True, slots=True)
class DataConfig:
    database_url_file: Path
    dataset_roots: tuple[DatasetLocation, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.database_url_file, Path) or not self.database_url_file.is_absolute():
            raise ValueError("database secret path must be absolute")
        if not isinstance(self.dataset_roots, tuple):
            raise TypeError("dataset locations must be immutable")
        keys = [(d.dataset_id, d.dataset_version) for d in self.dataset_roots]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate dataset location")

    def dataset_root(self, dataset_id: str, version: str) -> Path:
        for location in self.dataset_roots:
            if (location.dataset_id, location.dataset_version) == (dataset_id, version):
                return location.root
        raise ValueError("dataset/version has no configured publication root")


def default_config_path() -> Path:
    value = os.environ.get("AAS_DATA_CONFIG")
    return (
        Path(value) if value is not None else Path.home() / ".local/share/aegis-alpha/runtime.json"
    )


def _read(path: Path, maximum: int, *, private: bool) -> bytes:
    if not path.is_absolute():
        raise ValueError("configuration paths must be absolute")
    from aegis_alpha.data.descriptor_tree import DescriptorTree

    with (
        DescriptorTree.open_path(path.parent) as tree,
        tree.binary_reader(path.name, require_single_link=private) as handle,
    ):
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("configuration input must be a regular file")
        if private and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077):
            raise ValueError("secret must be owned by this user and inaccessible to group/others")
        data = handle.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError("configuration input is too large")
        return data


def read_secret(path: Path) -> str:
    value = _read(path, _MAX_SECRET_BYTES, private=True).decode("utf-8").strip()
    if not value or any(c in value for c in "\n\r\x00"):
        raise ValueError("secret file must contain one nonempty line")
    return value


def _pairs(values: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in values:
        if key in result:
            raise ValueError("duplicate configuration field")
        result[key] = value
    return result


def load_data_config(path: Path) -> DataConfig:
    value = json.loads(_read(path, _MAX_CONFIG_BYTES, private=False), object_pairs_hook=_pairs)
    if not isinstance(value, dict) or set(value) != {
        "version",
        "database_url_file",
        "dataset_roots",
    }:
        raise ValueError("data config has missing or unknown fields")
    if type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("unsupported data config version")
    if not isinstance(value["database_url_file"], str) or not isinstance(
        value["dataset_roots"], list
    ):
        raise TypeError("invalid data config field types")
    roots = []
    for row in value["dataset_roots"]:
        if not isinstance(row, dict) or set(row) != {"dataset_id", "dataset_version", "root"}:
            raise ValueError("invalid dataset location fields")
        if any(not isinstance(v, str) for v in row.values()):
            raise ValueError("dataset location values must be strings")
        roots.append(DatasetLocation(row["dataset_id"], row["dataset_version"], Path(row["root"])))
    return DataConfig(Path(value["database_url_file"]), tuple(roots))
