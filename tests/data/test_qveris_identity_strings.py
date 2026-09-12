from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from aegis_alpha.data.qveris_native import normalize_korean_price_history, read_completed_job
from tests.data.test_qveris_native import IDENTITY, complete

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("value", [1, True, None, "", " ", " padded", "padded ", "a\nb", "a\x00b"])
def test_invalid_stable_identity_is_rejected(tmp_path: Path, value: object) -> None:
    history = read_completed_job(*complete(tmp_path))
    identities = {"123456.KO": {**IDENTITY["123456.KO"], "instrument_id": cast("str", value)}}
    with pytest.raises(ValueError, match="identity"):
        normalize_korean_price_history(history, identities)


def test_exact_stable_identity_is_preserved(tmp_path: Path) -> None:
    history = read_completed_job(*complete(tmp_path))
    result = normalize_korean_price_history(history, IDENTITY)
    assert len(result.rows) == 1
    assert result.rows[0]["instrument_id"] == IDENTITY["123456.KO"]["instrument_id"]
    assert not result.quarantine
