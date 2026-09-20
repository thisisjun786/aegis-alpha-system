"""What a retained market chain's text costs, and what the chain estimate charges for it.

The estimate exists to refuse work that will not fit before any of it is materialized.
That is only sound if it stays above what CPython actually allocates, and it is only
useful if it does not sit so far above the truth that history we already stored gets
refused. A flat per-character charge fails both ways at once: almost all of a short
value's cost is its object header, and almost none of a long value's is. Retained market
history is mostly identifiers and digests, so the long end is the one that decides
whether a real panel can be backed up.
"""

from __future__ import annotations

import sys

from aegis_alpha.storage.market_inputs import _retained_bytes
from aegis_alpha.storage.market_schema import (
    COMMON,
    DOMAINS,
    TEXT_OVERHEAD_BYTES,
    text_bytes,
    text_columns,
)

# What the estimate charged before: a flat rate per character, with nothing per value.
FLAT_PER_CHARACTER = 32


def held(values: list[str]) -> int:
    """What CPython actually allocates for these values."""
    return sum(sys.getsizeof(value) for value in values)


def charged(values: list[str]) -> int:
    return text_bytes(len(values), sum(len(value) for value in values))


def test_the_estimate_stays_above_every_representation_cpython_picks() -> None:
    """Compact ASCII, UCS-2 and UCS-4, at both ends of the length range."""
    for value in (
        "",
        "a",
        "x" * 64,
        "0123456789" * 512,
        "e\u0301",
        "e\u0301" * 512,
        "🙂",
        "🙂" * 512,
    ):
        assert charged([value]) >= sys.getsizeof(value)


def test_four_byte_text_is_the_tightest_case_and_the_header_covers_it() -> None:
    """Where the per-character term is exactly consumed, the per-value term is the margin."""
    value = "🙂" * 4096
    assert charged([value]) >= sys.getsizeof(value)
    assert charged([value]) - sys.getsizeof(value) < TEXT_OVERHEAD_BYTES


def test_a_flat_per_character_charge_undercharged_short_values() -> None:
    """The direction that was a soundness bug, not merely wasted headroom."""
    values = ["a"] * 4096
    assert FLAT_PER_CHARACTER * sum(len(value) for value in values) < held(values)
    assert charged(values) >= held(values)


def test_a_digest_is_no_longer_charged_thirty_two_times_its_length() -> None:
    """The direction that refused a backup of history we had already stored."""
    digests = ["a" * 64] * 4096
    assert charged(digests) >= held(digests)
    assert charged(digests) * 5 < FLAT_PER_CHARACTER * sum(len(value) for value in digests)


def test_text_columns_counts_every_column_that_can_hold_a_value() -> None:
    """A nullable VARCHAR still holds text when it is not null, so it counts."""
    schema = COMMON + DOMAINS["feature_values"]
    assert text_columns(schema) == sum(1 for _name, kind in schema if kind.rstrip("?") == "VARCHAR")
    mixed = (("a", "VARCHAR?"), ("b", "BIGINT"), ("c", "VARCHAR"))
    assert text_columns(mixed) == len(mixed) - 1


def test_a_retained_observation_chain_is_bounded_without_being_digest_dominated() -> None:
    """A history shaped like the real panel: one generation, identifiers and digests.

    The estimate has to stay above what the rows' text holds while landing far below the
    flat charge that refused this shape. The per-row term is untouched and still leads,
    which is the point: what changed is the term that was wrong, not the safety factor.
    """
    rows = 2048
    history = tuple(
        {
            "generation_id": "obs-b4df6751-open-v2",
            "record_id": f"{index:064x}",
            "revision_id": str(index),
            "supersedes_revision_id": None,
            "op": "ASSERT",
            "available_at_us": None,
            "revision_known_at_us": None,
            "ingested_at_us": 1789896517613469,
            "source_snapshot_id": "a" * 71,
            "source_row_hash": f"{index:064x}",
            "contract_id": "aas8-snowball-b4df6751/open",
            "contract_version": "v2",
            "contract_hash": "b" * 64,
            "input_bundle_hash": "c" * 64,
            "instrument_id": "aas-obs-280324",
            "feature_at_us": 1294876800000000,
            "value": 1.0,
            "value_state": "present",
        }
        for index in range(rows)
    )
    text = [value for row in history for value in row.values() if isinstance(value, str)]
    estimate = _retained_bytes(history)
    assert estimate >= held(text)
    row_term = rows * (1024 + 256 * len(COMMON + DOMAINS["feature_values"]))
    flat = FLAT_PER_CHARACTER * sum(len(value) for value in text)
    assert estimate - row_term < flat // 2
