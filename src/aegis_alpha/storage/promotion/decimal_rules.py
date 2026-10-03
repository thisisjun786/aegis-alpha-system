"""Named, versioned conversions of source numbers into exact ``DECIMAL(38,12)`` values.

A spec names one rule per numeric domain column. Each rule has a SQL form that the
promotion runs in DuckDB and a Python form that is its reference; the parity tests in
``tests/storage/test_decimal_rules.py`` hold the two together. A rule that changes the
source value leaves a quality flag; the original stays in the source library and in the
row's ``source_row_hash``. Every rule refuses NaN, infinities and values outside
``DECIMAL(38,12)`` (26 integer digits) instead of clamping them.

- ``exact@1`` (binary64, binary32 or integer input): the exact value, refused unless it
  fits 12 decimals. No flag.
- ``krw_tick@1`` (binary64 KRW price): the exact binary expansion rounded to whole won,
  half to even. ``provider_float_reconstructed`` when the value was not whole and
  ``decimal_rounding_tie`` when it was exactly half.
- ``float_shortest@1`` (binary32 or binary64): the fewest correctly rounded significant
  digits that convert back to the stored value at its width, then 12 decimals half to
  even. ``provider_float_storage`` when the result differs from the stored binary value
  and ``decimal_rounding_tie`` at an exact half.
- ``decimal_text@1`` (decimal text): the exact value of the text, refused beyond 12
  decimals. ``volume_precision_limited`` for exponent notation with at most 7 mantissa
  digits.

DuckDB's ``round`` rounds half away from zero, so every rounding here is integer and
text arithmetic on the exact digits instead.
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from decimal import ROUND_FLOOR, ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from fractions import Fraction
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    import duckdb

_SCALE: Final = Decimal("0.000000000001")
_INTEGER_DIGITS: Final = 26
_TEXT: Final = re.compile(r"([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?")
_TEXT_SQL: Final = r"^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$"
_LIMITED_DIGITS: Final = 7
_FLOAT_KINDS: Final = frozenset({"FLOAT", "DOUBLE"})
_INTEGER_KINDS: Final = frozenset({"TINYINT", "SMALLINT", "INTEGER", "BIGINT"})
_PRICE_COLUMNS: Final = frozenset({"open", "high", "low", "close"})
_WIDTH_DIGITS: Final = {"FLOAT": 9, "DOUBLE": 17}
_LARGEST_FLOAT32: Final = struct.unpack(">f", b"\x7f\x7f\xff\xff")[0]
UNITS_DECIMAL_MACRO: Final = (
    "CREATE OR REPLACE TEMP MACRO _aas_units_decimal(units) AS "
    "CAST((CASE WHEN units < 0 THEN '-' ELSE '' END) "
    "|| CAST(abs(units) // 1000000000000 AS VARCHAR) || '.' "
    "|| lpad(CAST(abs(units) % 1000000000000 AS VARCHAR), 12, '0') AS DECIMAL(38,12))"
)

type Layer = list[tuple[str, str]]


@dataclass(frozen=True, slots=True)
class Conversion:
    """One column's rule in SQL: the value, when it is refused, and its flag conditions."""

    value: str
    refused: str
    flags: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class Converted:
    """The Python reference result for one source value."""

    value: Decimal | None
    flags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DecimalRule:
    rule_id: str
    version: str
    kinds: frozenset[str]

    @property
    def name(self) -> str:
        return f"{self.rule_id}@{self.version}"


EXACT = DecimalRule("exact", "1", _FLOAT_KINDS | _INTEGER_KINDS)
KRW_TICK = DecimalRule("krw_tick", "1", frozenset({"DOUBLE"}))
FLOAT_SHORTEST = DecimalRule("float_shortest", "1", _FLOAT_KINDS)
DECIMAL_TEXT = DecimalRule("decimal_text", "1", frozenset({"VARCHAR"}))
RULES: Final = {found.name: found for found in (EXACT, KRW_TICK, FLOAT_SHORTEST, DECIMAL_TEXT)}


def rule(name: str) -> DecimalRule:
    if name not in RULES:
        raise ValueError(f"unknown decimal rule {name}")
    return RULES[name]


def check_column(name: str, column: str, kind: str, *, domain: str) -> DecimalRule:
    """The named rule after checking it applies to this domain column and input type."""
    found = rule(name)
    if kind not in found.kinds:
        raise ValueError(f"decimal rule {name} does not convert {kind} input ({column})")
    if found is KRW_TICK and (domain != "prices" or column not in _PRICE_COLUMNS):
        raise ValueError("krw_tick@1 converts KRW price columns only (open, high, low, close)")
    return found


def install(connection: duckdb.DuckDBPyConnection) -> None:
    """Define the one helper macro the conversions use, in the connection's temp catalog."""
    connection.execute(UNITS_DECIMAL_MACRO)


def _units(prefix: str, negative: str, digits: str, power: str) -> list[Layer]:
    """Layers turning digits times a power of ten into DECIMAL(38,12) units, half to even.

    The units column ``{prefix}u`` is NULL when the value needs more than 26 integer
    digits or more than 38 significant ones. ``{prefix}tie`` and ``{prefix}inexact`` say
    whether rounding to 12 decimals met an exact half or dropped a nonzero digit.
    """
    p = prefix
    stripped = f"rtrim(ltrim({digits}, '0'), '0')"
    return [
        [
            (f"{p}n", f"coalesce({negative}, false)"),
            (f"{p}d", stripped),
            (f"{p}w", f"{power} + length(ltrim({digits}, '0')) - length({stripped})"),
        ],
        [(f"{p}k", f"{p}w + 12"), (f"{p}l", f"length({p}d)")],
        [
            (f"{p}over", f"{p}d <> '' AND ({p}l > 38 OR {p}l + {p}w > 26)"),
            (
                f"{p}rest",
                (
                    f"CASE WHEN {p}d = '' OR {p}k >= 0 THEN NULL "
                    f"WHEN {p}l > -{p}k THEN right({p}d, CAST(-{p}k AS INTEGER)) "
                    f"ELSE lpad({p}d, CAST(-{p}k AS INTEGER), '0') END"
                ),
            ),
        ],
        [
            (
                f"{p}kept",
                (
                    f"CASE WHEN {p}d IS NULL OR {p}over THEN NULL WHEN {p}d = '' THEN 0::HUGEINT "
                    f"WHEN {p}k >= 0 THEN "
                    f"CAST({p}d || repeat('0', CAST({p}k AS INTEGER)) AS HUGEINT) "
                    f"WHEN {p}l > -{p}k THEN "
                    f"CAST(left({p}d, CAST({p}l + {p}k AS INTEGER)) AS HUGEINT) "
                    "ELSE 0::HUGEINT END"
                ),
            ),
            (
                f"{p}half",
                (
                    f"CASE WHEN {p}rest IS NULL THEN NULL "
                    f"ELSE '5' || repeat('0', CAST(length({p}rest) - 1 AS INTEGER)) END"
                ),
            ),
        ],
        [
            (
                f"{p}u",
                (
                    f"(CASE WHEN {p}n THEN -1 ELSE 1 END) * ({p}kept + CASE "
                    f"WHEN {p}rest IS NOT NULL AND ({p}rest > {p}half "
                    f"OR ({p}rest = {p}half AND {p}kept % 2 = 1)) "
                    "THEN 1 ELSE 0 END)"
                ),
            ),
            (f"{p}tie", f"coalesce({p}rest = {p}half, false)"),
            (f"{p}inexact", f"{p}rest IS NOT NULL"),
        ],
    ]


def _parsed(prefix: str, text: str) -> list[Layer]:
    """Layers parsing decimal text into ``{prefix}neg``, ``{prefix}digits``, ``{prefix}pow``.

    Digits are NULL when the text is not a decimal number, and the power is NULL when its
    exponent does not fit a 32-bit integer.
    """
    p = prefix
    match = f"{p}m"
    return [
        [
            (
                match,
                (
                    f"CASE WHEN regexp_full_match({text}, '{_TEXT_SQL}') "
                    f"THEN regexp_extract({text}, '{_TEXT_SQL}', ['s', 'i', 'f', 'e']) END"
                ),
            )
        ],
        [
            (f"{p}neg", f"{match}.s = '-'"),
            (f"{p}digits", f"nullif({match}.i || coalesce({match}.f, ''), '')"),
            (
                f"{p}pow",
                (
                    f"TRY_CAST(coalesce(nullif({match}.e, ''), '0') AS INTEGER) "
                    f"- length(coalesce({match}.f, ''))"
                ),
            ),
            (f"{p}exp", f"coalesce({match}.e, '') <> ''"),
            (f"{p}mant", f"length(ltrim({match}.i || coalesce({match}.f, ''), '0'))"),
        ],
    ]


def _text_units(prefix: str, text: str) -> list[Layer]:
    return [
        [(f"{prefix}t", text)],
        *_parsed(prefix, f"{prefix}t"),
        *_units(prefix, f"{prefix}neg", f"{prefix}digits", f"{prefix}pow"),
    ]


def _zip(*groups: list[Layer]) -> list[Layer]:
    """Run independent layer lists side by side, layer by layer."""
    width = max(len(group) for group in groups)
    return [
        [column for group in groups if index < len(group) for column in group[index]]
        for index in range(width)
    ]


def _shortest_text(value: str, kind: str) -> str:
    cases = " ".join(
        f"WHEN CAST(format('{{:.{digits - 1}e}}', CAST({value} AS DOUBLE)) AS {kind}) = {value}"
        f" THEN format('{{:.{digits - 1}e}}', CAST({value} AS DOUBLE))"
        for digits in range(1, _WIDTH_DIGITS[kind] + 1)
    )
    return f"(CASE {cases} END)"


def conversion(
    found: DecimalRule, column: str, kind: str, prefix: str
) -> tuple[list[Layer], Conversion]:
    """The SQL of ``found`` over the source value ``column`` of type ``kind``.

    Returns projection layers (each may read the columns of the layers before it; every
    name starts with ``prefix``) and the final expressions over those columns.
    """
    present = f"{column} IS NOT NULL"
    if found is DECIMAL_TEXT:
        layers = _text_units(prefix, column)
        units = f"{prefix}u"
        return layers, Conversion(
            value=f"_aas_units_decimal({units})",
            refused=(
                f"{present} AND ({prefix}digits IS NULL OR {prefix}pow IS NULL "
                f"OR {units} IS NULL OR {prefix}inexact)"
            ),
            flags=(
                (
                    "volume_precision_limited",
                    f"{present} AND {prefix}exp AND {prefix}mant <= {_LIMITED_DIGITS}",
                ),
            ),
        )
    if found is EXACT and kind in _INTEGER_KINDS:
        return [], Conversion(f"CAST({column} AS DECIMAL(38,12))", "false", ())
    double = f"CAST({column} AS DOUBLE)"
    finite = f"(isfinite({double}) AND abs({double}) < 1e26)"
    if found is KRW_TICK:
        whole = f"floor({column})"
        rest = f"({column} - {whole})"
        rounded = (
            f"CASE WHEN {rest} > 0.5 THEN {whole} + 1 WHEN {rest} < 0.5 THEN {whole} "
            f"WHEN abs({whole} % 2) = 1 THEN {whole} + 1 ELSE {whole} END"
        )
        return [], Conversion(
            value=f"CAST(CAST({rounded} AS HUGEINT) AS DECIMAL(38,12))",
            refused=f"{present} AND NOT {finite}",
            flags=(
                ("provider_float_reconstructed", f"{present} AND {rest} <> 0"),
                ("decimal_rounding_tie", f"{present} AND {rest} = 0.5"),
            ),
        )
    # A finite binary value below 1e26 that fits 12 decimals has at most 38 significant
    # digits, so its 38-digit correctly rounded text is exact; anything else is inexact.
    exact = _text_units(prefix + "x", f"CASE WHEN {finite} THEN format('{{:.37e}}', {double}) END")
    exact_units = f"{prefix}xu"
    if found is EXACT:
        return exact, Conversion(
            value=f"_aas_units_decimal({exact_units})",
            refused=f"{present} AND ({exact_units} IS NULL OR {prefix}xinexact)",
            flags=(),
        )
    shortest = _text_units(
        prefix + "s", f"CASE WHEN {finite} THEN {_shortest_text(column, kind)} END"
    )
    shortest_units = f"{prefix}su"
    return _zip(exact, shortest), Conversion(
        value=f"_aas_units_decimal({shortest_units})",
        refused=f"{present} AND {shortest_units} IS NULL",
        flags=(
            (
                "provider_float_storage",
                (
                    f"{present} AND ({prefix}xinexact OR {exact_units} IS DISTINCT FROM "
                    f"{shortest_units})"
                ),
            ),
            ("decimal_rounding_tie", f"{present} AND {prefix}stie"),
        ),
    )


# --- Python reference --------------------------------------------------------------------


def _bounded(value: Decimal) -> Decimal:
    if not value.is_finite() or (not value.is_zero() and value.adjusted() >= _INTEGER_DIGITS):
        raise ValueError("number exceeds DECIMAL(38,12)")
    return value


def _quantize(value: Decimal) -> tuple[Decimal, bool]:
    """Round to 12 decimals half to even, and whether the dropped part was exactly half."""
    with localcontext() as context:
        context.prec = 2000
        result = value.quantize(_SCALE, rounding=ROUND_HALF_EVEN)
        scaled = value.scaleb(12)
        tie = abs(scaled - scaled.to_integral_value(rounding=ROUND_FLOOR)) == Decimal("0.5")
    return _bounded(result), tie


def _bits32(value: float) -> int:
    return struct.unpack(">I", struct.pack(">f", value))[0]


def _from_bits32(bits: int) -> float:
    return struct.unpack(">f", (bits & 0xFFFFFFFF).to_bytes(4, "big"))[0]


def _nearest32(text: str) -> float:
    """The binary32 value nearest the exact decimal ``text``, ties to the even mantissa."""
    exact = Fraction(Decimal(text))
    if abs(exact) > Fraction(_LARGEST_FLOAT32):
        return float("inf")
    near = float(exact)
    guess = _from_bits32(_bits32(max(min(near, _LARGEST_FLOAT32), -_LARGEST_FLOAT32)))
    best: tuple[tuple[Fraction, int], float] | None = None
    for bits in {_bits32(guess) + step for step in (-1, 0, 1)}:
        candidate = _from_bits32(bits)
        if candidate != candidate or abs(candidate) == float("inf"):  # noqa: PLR0124 -- NaN
            continue
        key = (abs(Fraction(candidate) - exact), bits & 1)
        if best is None or key < best[0]:
            best = (key, candidate)
    if best is None:
        raise ValueError("no binary32 neighbour")
    return best[1]


def shortest_text(value: float, kind: str) -> str:
    """Fewest correctly rounded significant digits that convert back at the stored width."""
    for digits in range(1, _WIDTH_DIGITS[kind] + 1):
        text = f"{value:.{digits - 1}e}"
        back = _nearest32(text) if kind == "FLOAT" else float(text)
        if back == value:
            return text
    raise ValueError("no round-trip decimal text")


def _text_reference(value: object) -> Converted:
    if not isinstance(value, str) or (match := _TEXT.fullmatch(value)) is None:
        raise ValueError("decimal text is not a decimal number")
    if not (match[2] or match[3]):
        raise ValueError("decimal text is not a decimal number")
    try:
        number = _bounded(Decimal(value))
    except InvalidOperation:
        raise ValueError("decimal text is not a decimal number") from None
    result, _ = _quantize(number)
    if result != number:
        raise ValueError("decimal text has more than 12 decimals")
    mantissa = len(((match[2] or "") + (match[3] or "")).lstrip("0"))
    limited = bool(match[4]) and mantissa <= _LIMITED_DIGITS
    return Converted(result, ("volume_precision_limited",) if limited else ())


def _float_reference(found: DecimalRule, value: float, kind: str) -> Converted:
    exact = _bounded(Decimal(value))
    if found is EXACT:
        result, _ = _quantize(exact)
        if result != exact:
            raise ValueError("value has more than 12 decimals")
        return Converted(result, ())
    flags = []
    if found is KRW_TICK:
        whole = exact.to_integral_value(rounding=ROUND_HALF_EVEN)
        with localcontext() as context:
            context.prec = 2000
            rest = exact - exact.to_integral_value(rounding=ROUND_FLOOR)
        if rest != 0:
            flags.append("provider_float_reconstructed")
        if rest == Decimal("0.5"):
            flags.append("decimal_rounding_tie")
        result, _ = _quantize(whole)
        return Converted(result, tuple(flags))
    result, tie = _quantize(Decimal(shortest_text(value, kind)))
    if result != exact:
        flags.append("provider_float_storage")
    if tie:
        flags.append("decimal_rounding_tie")
    return Converted(result, tuple(flags))


def convert(found: DecimalRule, value: object, kind: str) -> Converted:
    """The Python reference of ``found`` for one source value of DuckDB type ``kind``."""
    if value is None:
        return Converted(None, ())
    if found is DECIMAL_TEXT:
        return _text_reference(value)
    if isinstance(value, bool):
        raise TypeError("booleans are not numbers")
    if isinstance(value, int):
        if found is not EXACT:
            raise TypeError(f"{found.name} converts floating point values")
        result, _ = _quantize(_bounded(Decimal(value)))
        return Converted(result, ())
    if not isinstance(value, float):
        raise TypeError("expected a floating point source value")
    return _float_reference(found, value, kind)
