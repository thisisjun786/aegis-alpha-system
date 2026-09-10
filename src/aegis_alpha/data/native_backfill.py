"""Missing-field windows on an explicitly supplied exchange calendar."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class DataGap:
    instrument_id: str
    field: str
    start: date
    end: date
    sessions: tuple[date, ...]


def plan_gaps(  # noqa: C901 -- strict input admission then per-field consecutive gaps
    instrument_id: str,
    inception: date,
    sessions: tuple[date, ...],
    required_fields: tuple[str, ...],
    coverage: Mapping[date, frozenset[str]],
) -> tuple[DataGap, ...]:
    """Do not infer exchange sessions, inception, or price validity from row presence."""
    if (
        not isinstance(instrument_id, str)
        or not instrument_id.strip()
        or instrument_id != instrument_id.strip()
    ):
        raise ValueError("instrument identity must be explicit")
    if (
        type(inception) is not date
        or not isinstance(sessions, tuple)
        or any(type(day) is not date for day in sessions)
    ):
        raise ValueError("inception and sessions must be date values")
    if tuple(sorted(set(sessions))) != sessions:
        raise ValueError("exchange sessions must be unique and ordered")
    if (
        not isinstance(required_fields, tuple)
        or not required_fields
        or any(
            not isinstance(field, str) or not field.strip() or field != field.strip()
            for field in required_fields
        )
        or len(set(required_fields)) != len(required_fields)
    ):
        raise ValueError("required fields must be explicit and unique")
    if not isinstance(coverage, Mapping) or any(
        type(day) is not date
        or not isinstance(fields, frozenset)
        or any(not isinstance(field, str) for field in fields)
        for day, fields in coverage.items()
    ):
        raise ValueError("coverage must map dates to verified field sets")
    calendar = tuple(day for day in sessions if day >= inception)
    gaps = []
    for field in required_fields:
        missing: list[date] = []
        for day in calendar:
            if field not in coverage.get(day, frozenset()):
                missing.append(day)
            elif missing:
                gaps.append(DataGap(instrument_id, field, missing[0], missing[-1], tuple(missing)))
                missing.clear()
        if missing:
            gaps.append(DataGap(instrument_id, field, missing[0], missing[-1], tuple(missing)))
    return tuple(gaps)
