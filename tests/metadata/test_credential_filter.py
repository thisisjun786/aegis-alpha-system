from __future__ import annotations

from collections.abc import Iterator, Mapping

import pytest

from aegis_alpha.metadata.records import (
    reject_credential_metadata,
    validated_json_copy,
)


@pytest.mark.parametrize(
    "key",
    [
        "private_key",
        "privateKey",
        "PRIVATE-KEY",
        "private\uff3fkey",
        "access_key",
        "access.key",
        "accessKey",
        "ACCESSKey",
        "\uff21\uff23\uff23\uff25\uff33\uff33\uff2b\uff45\uff59",
        "api_key",
        "client_secret",
        "bearer_token",
    ],
)
def test_credential_key_forms_are_rejected(key: str) -> None:
    with pytest.raises(ValueError, match="credential"):
        reject_credential_metadata({key: "value"})


@pytest.mark.parametrize(
    "value",
    [
        "Bearer abcdef1234567890",
        "Bearer abc",
        "see //user:pw@host.invalid/x",
        "url=https://user@host.invalid/x",
        "url=https://user:pw@host.invalid/x",
        "https://user%3Apw%40host.invalid/x",
        "https://host.invalid/x?api_key=abc",
        "https://host.invalid/x?access_token=abc",
        "https://host.invalid/x?access.key=abc",
        "https://host.invalid/x#access.key=abc",
        "//host.invalid/x?access.key=abc",
    ],
)
def test_credential_value_forms_are_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="credential"):
        reject_credential_metadata({"note": value})


@pytest.mark.parametrize(
    "value",
    [
        "bearer of notes",
        "https://host.invalid/docs",
        "https://host.invalid/x?page=2",
        "https://host.invalid/x#section-2",
        "//host.invalid/x?accessory.key=public",
        "file:///external/norgate/2026-07-29-us-platinum",
        "provider returned HTTP 500 after 3 retries",
        "public_key",
        "monkey",
    ],
)
def test_non_credential_forms_still_pass(value: str) -> None:
    reject_credential_metadata({"note": value})


class _ShiftingMapping(Mapping[str, object]):
    def __init__(self, snapshots: tuple[dict[str, object], ...]) -> None:
        self._snapshots = snapshots
        self._scan = -1

    def __getitem__(self, key: str) -> object:
        return self._current()[key]

    def __iter__(self) -> Iterator[str]:
        self._scan += 1
        return iter(self._current())

    def __len__(self) -> int:
        return len(self._current())

    def _current(self) -> dict[str, object]:
        return self._snapshots[min(self._scan, len(self._snapshots) - 1)]


def test_validated_json_copy_validates_the_same_snapshot_it_thaws() -> None:
    clean_first = _ShiftingMapping(({"note": "clean"}, {"note": "Bearer abcdef123456"}))

    assert validated_json_copy(clean_first) == {"note": "clean"}

    dirty_first = _ShiftingMapping(({"note": "Bearer abcdef123456"}, {"note": "clean"}))
    with pytest.raises(ValueError, match="credential"):
        validated_json_copy(dirty_first)
