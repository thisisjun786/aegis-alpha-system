"""DART financial statement receipts as ``fundamentals`` and ``filings``.

Input is the source library's OpenDART receipts table: one row per provider request with
its ``endpoint``, ``outcome``, ``request_json`` (whose ``parameters_json`` names
``corp_code``, ``bsns_year``, ``reprt_code`` and ``fs_div``), the response bytes as
``raw_base64`` with their ``raw_sha256``, and the collection instant
``retrieved_at_utc``. Other columns (fingerprints, validation notes) stay in the source
row and its hash.

A row reads as ``completed`` only when it is a ``financials`` request that completed, its
response bytes decode to UTF-8 text whose SHA-256 is the recorded one, and that text is a
JSON document with provider status ``000`` and a nonempty ``list`` of statement lines.
Every other row is coverage, never a fact: ``no_data`` (the provider had no statement for
the request), ``failed``, ``other_endpoint`` (the corp code list shares the table),
``unreadable`` (a completed financials response that fails any check above) and
``unknown_outcome``. An ``unreadable`` or ``unknown_outcome`` row maps to one row with no
issuer, which the promotion refuses as a missing required column, so a damaged response
is never skipped silently.

Shared rules of both mappers:

- The issuer is ``mint_issuer('dart_corp_code', corp_code)`` of the request's eight-digit
  corp code, the DART issuer anchor. A statement line whose corp code, business year or
  report code differs from its request has no issuer and is refused.
- The filing is the line's receipt number ``rcept_no`` (14 digits), whose first eight
  digits are the receipt date in Korea; that date is ``filed_date`` and the one time input.
  OpenDART answers with the statements of the latest filing for a report, so the receipt
  number names the filing (an amendment included) whose values the row holds.
- ``form`` is the OpenDART report code: ``11011`` annual, ``11012`` half-year, ``11013``
  first quarter, ``11014`` third quarter.

``dart.fnltt@1`` writes one fundamentals row per statement line and amount field the
line carries: ``thstrm_amount`` (this term) always and ``thstrm_add_amount`` (this term
cumulative) when present. Prior-period comparatives (``frmtrm_*``, ``bfefrmtrm_*``)
stay in the source.

- ``concept`` is ``account_id`` as DART spells it (``-표준계정코드 미사용-`` for a line
  without a standard account), ``unit`` is the line's currency, and ``value`` is the
  amount text for ``decimal_text@1``: ``present`` for a decimal number, ``missing`` for
  an empty field and ``invalid`` (no value) for any other text.
- DART states no period dates. ``fiscal_period`` is the report slot (``FY``, ``H1``,
  ``Q1``, ``Q3``) for this-term amounts and the slot with ``-cumulative`` for cumulative
  ones. ``period_end`` is the slot's nominal end in ``bsns_year`` (``03-31``, ``06-30``,
  ``09-30``, ``12-31``), which is the period end for an issuer with a December year end
  and a slot label otherwise; ``period_start`` is NULL.
- ``dimensions_hash`` is ``aas-dimensions-v1`` of ``fs_div``, ``sj_div``,
  ``account_nm``, ``account_detail``, ``ord`` and ``rcept_no``. DART repeats an account
  and its name inside one statement, so the line order is part of the line, and every
  filing's lines are their own records: an amendment adds records under its own receipt
  number rather than guessing which earlier line it restates.

``dart.fnltt_filings@1`` writes one filings row per completed response: the filing
``rcept_no`` with its ``form`` and ``filed_date``. ``accepted_at_us`` and ``period_end``
are NULL because the response states neither. Two responses naming one filing (its
consolidated and separate statements) repeat a natural key and are promoted in separate
generations, where the second is unchanged.

A spec partition selects requests by business year: the partition date of a row is
January 1 of its request's ``bsns_year``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.identity import ISSUER_FORMAT
from aegis_alpha.storage.promotion.formats import dimensions_hash_sql
from aegis_alpha.storage.promotion.time_rules import InputKind

_TEXT: Final = frozenset({"VARCHAR"})
_COLUMNS: Final = (
    "endpoint",
    "outcome",
    "request_json",
    "raw_base64",
    "raw_sha256",
    "retrieved_at_utc",
)
_INSTANT: Final = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z"
_DECIMAL: Final = r"-?[0-9]+(\.[0-9]+)?"
SLOTS: Final = {"11011": ("FY", 12), "11012": ("H1", 6), "11013": ("Q1", 3), "11014": ("Q3", 9)}
AMOUNTS: Final = ("thstrm_amount", "thstrm_add_amount")
DIMENSIONS: Final = ("account_detail", "account_nm", "fs_div", "ord", "rcept_no", "sj_div")
REFUSED_OUTCOMES: Final = frozenset({"unreadable", "unknown_outcome"})


def _param(name: str) -> str:
    return f"json_extract_string(_d_params, '$.{name}')"


def _item(name: str) -> str:
    return f"json_extract_string(_d_j, '$.{name}')"


_PARAMS: Final = (
    "CASE WHEN json_valid(request_json) THEN "
    "json_extract_string(request_json, '$.parameters_json') END"
)
_BODY: Final = (
    "CASE WHEN endpoint = 'financials' AND outcome = 'COMPLETED' "
    "THEN try(decode(try(from_base64(raw_base64)))) END"
)


def _outcome(body: str) -> str:
    """A receipt row's outcome, given the SQL of its decoded financials response text."""
    readable = (
        f"{body} IS NOT NULL AND sha256({body}) = raw_sha256 AND json_valid({body}) "
        f"AND json_extract_string({body}, '$.status') = '000' "
        f"AND json_type({body}, '$.list') = 'ARRAY' AND json_array_length({body}, '$.list') > 0"
    )
    return (
        "CASE WHEN endpoint IS DISTINCT FROM 'financials' THEN 'other_endpoint' "
        "WHEN outcome = 'NO_DATA' THEN 'no_data' WHEN outcome = 'FAILED' THEN 'failed' "
        f"WHEN outcome = 'COMPLETED' THEN CASE WHEN {readable} THEN 'completed' "
        "ELSE 'unreadable' END ELSE 'unknown_outcome' END"
    )


def _receipts(source: str) -> str:
    """Each source row with its request parameters, response text and outcome."""
    decoded = (
        f"SELECT *, CASE WHEN json_valid({_PARAMS}) THEN {_PARAMS} END AS _d_params, "  # noqa: S608 -- engine-named relation
        f"{_BODY} AS _d_body FROM {source}"
    )
    return f"SELECT *, {_outcome('_d_body')} AS _d_outcome FROM ({decoded})"  # noqa: S608 -- engine-named relation


def _lines(source: str) -> str:
    """One row per statement line of a completed response, one NULL line for a refused row."""
    refused = ", ".join(f"'{name}'" for name in sorted(REFUSED_OUTCOMES))
    return (
        "SELECT * EXCLUDE (_d_body, _d_lines), unnest(_d_lines) AS _d_j, "  # noqa: S608 -- engine-named relation
        "generate_subscripts(_d_lines, 1) - 1 AS _d_line FROM ("
        "SELECT *, CASE WHEN _d_outcome = 'completed' THEN json_extract(_d_body, '$.list[*]') "
        "ELSE [CAST(NULL AS JSON)] END AS _d_lines "
        f"FROM ({_receipts(source)}) WHERE _d_outcome = 'completed' OR _d_outcome IN ({refused}))"
    )


def _matching(value: str, pattern: str) -> str:
    """``value`` when the whole text matches ``pattern``, else NULL."""
    return f"CASE WHEN regexp_full_match({value}, '{pattern}') THEN {value} END"


def _ingested() -> str:
    return (
        f"CASE WHEN regexp_full_match(retrieved_at_utc, '{_INSTANT}') "
        "THEN epoch_us(TRY_CAST(retrieved_at_utc AS TIMESTAMPTZ)) END"
    )


def _issuer() -> str:
    """The minted DART issuer of a line that agrees with its request, else NULL."""
    corp = _param("corp_code")
    agrees = (
        f"_d_outcome = 'completed' AND regexp_full_match({corp}, '[0-9]{{8}}') "
        f"AND {_item('corp_code')} = {corp} "
        f"AND {_item('bsns_year')} = {_param('bsns_year')} "
        f"AND {_item('reprt_code')} = {_param('reprt_code')}"
    )
    anchor = f'["{ISSUER_FORMAT}","dart_corp_code","'
    return f"CASE WHEN {agrees} THEN 'iss-' || sha256('{anchor}' || {corp} || '\"]') END"


def _filing() -> tuple[str, str]:
    """(receipt number, filed date) of a line, NULL unless the number is 14 digits."""
    number = _item("rcept_no")
    valid = f"regexp_full_match({number}, '[0-9]{{14}}')"
    filed = f"CAST(try_strptime(substr({number}, 1, 8), '%Y%m%d') AS DATE)"
    return f"CASE WHEN {valid} THEN {number} END", f"CASE WHEN {valid} THEN {filed} END"


def _form() -> str:
    codes = ", ".join(f"'{code}'" for code in sorted(SLOTS))
    code = _item("reprt_code")
    return f"CASE WHEN {code} IN ({codes}) THEN {code} END"


def _partition() -> str:
    params = f"CASE WHEN json_valid({_PARAMS}) THEN {_PARAMS} END"
    year = f"json_extract_string({params}, '$.bsns_year')"
    return (
        f"CASE WHEN regexp_full_match({year}, '[0-9]{{4}}') "
        f"THEN make_date(CAST({year} AS INTEGER), 1, 1) END"
    )


class _Receipts:
    """What both DART receipt mappers share: source shape, partition, time and outcome."""

    provider: Final = "dart"
    time_inputs: Final[Mapping[str, InputKind]] = {"filed_date": "date"}

    @property
    def partition_date(self) -> str:
        return _partition()

    def check_args(self, args: Mapping[str, object]) -> None:
        if args:
            raise ValueError("DART receipt mappers take no arguments")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return dict.fromkeys(_COLUMNS, _TEXT)

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def outcome(self, args: Mapping[str, object]) -> str:
        del args
        return _outcome(_BODY)


class DartFnltt(_Receipts):
    name: Final = "dart.fnltt"
    major: Final = 1
    domain: Final = "fundamentals"
    date_column: Final = "period_end"
    expands: Final = True

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"value": "VARCHAR"}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        del args
        number, filed = _filing()
        slot = _item("reprt_code")
        names = " ".join(f"WHEN '{code}' THEN '{name}'" for code, (name, _) in SLOTS.items())
        months = " ".join(f"WHEN '{code}' THEN {month}" for code, (_, month) in SLOTS.items())
        period = f"(CASE {slot} {names} END)"
        year = f"TRY_CAST({_item('bsns_year')} AS INTEGER)"
        end = (
            f"CASE WHEN regexp_full_match({_item('bsns_year')}, '[0-9]{{4}}') THEN "
            f"last_day(make_date({year}, CASE {slot} {months} END, 1)) END"
        )
        amount = f"CASE WHEN _d_field = 0 THEN {_item(AMOUNTS[0])} ELSE {_item(AMOUNTS[1])} END"
        dimensions = dimensions_hash_sql(
            [
                ("account_detail", _item("account_detail")),
                ("account_nm", _item("account_nm")),
                ("fs_div", _matching(_param("fs_div"), "CFS|OFS")),
                ("ord", _matching(_item("ord"), "[0-9]+")),
                ("rcept_no", number),
                ("sj_div", _matching(_item("sj_div"), "[A-Z]+")),
            ]
        )
        state = (
            "CASE WHEN _d_amount = '' THEN 'missing' "
            f"WHEN regexp_full_match(_d_amount, '{_DECIMAL}') THEN 'present' "
            "WHEN _d_amount IS NOT NULL THEN 'invalid' END"
        )
        fields = (
            "SELECT *, "  # noqa: S608 -- engine-named relation
            f"{amount} AS _d_amount FROM (SELECT *, unnest([0, 1]) AS _d_field FROM ("
            f"{_lines(source)})) WHERE _d_field = 0 OR json_exists(_d_j, '$.{AMOUNTS[1]}')"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "2 * _d_line + _d_field AS _aas_item, "
            f"{_ingested()} AS _aas_ingested_at_us, "
            f"{_issuer()} AS issuer_id, {_item('account_id')} AS concept, "
            "CAST(NULL AS DATE) AS period_start, "
            f"{end} AS period_end, "
            f"CASE WHEN _d_field = 0 THEN {period} ELSE {period} || '-cumulative' END "
            "AS fiscal_period, "
            f"CASE WHEN regexp_full_match({_item('currency')}, '[A-Z]{{3}}') "
            f"THEN {_item('currency')} END AS unit, "
            f"{dimensions} AS dimensions_hash, {_form()} AS form, {number} AS accession, "
            "CAST(NULL AS BIGINT) AS accepted_at_us, "
            f"CASE WHEN {state} = 'present' THEN _d_amount END AS value, "
            f"{state} AS value_state, {filed} AS _aas_t_filed_date FROM ({fields})"
        )


class DartFnlttFilings(_Receipts):
    name: Final = "dart.fnltt_filings"
    major: Final = 1
    domain: Final = "filings"
    date_column: Final = "filed_date"
    expands: Final = False

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def select(self, source: str, args: Mapping[str, object]) -> str:
        del args
        number, filed = _filing()
        # A response holds one filing: every line names the same receipt number, so the
        # first line speaks for the response and a disagreeing line voids the filing.
        same = (
            "count(DISTINCT coalesce(json_extract_string(_d_j, '$.rcept_no'), '')) "
            "OVER (PARTITION BY _aas_pin, _aas_ordinal) = 1"
        )
        lines = (
            "SELECT *, "  # noqa: S608 -- engine-named relation
            f"{same} AS _d_one FROM ({_lines(source)})"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            f"{_ingested()} AS _aas_ingested_at_us, "
            f"CASE WHEN _d_one THEN {_issuer()} END AS issuer_id, "
            f"{number} AS filing_id, {_form()} AS form, {filed} AS filed_date, "
            "CAST(NULL AS BIGINT) AS accepted_at_us, CAST(NULL AS DATE) AS period_end, "
            f"{filed} AS _aas_t_filed_date FROM ({lines}) WHERE _d_line = 0"
        )
