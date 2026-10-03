"""DART financial statement receipts as ``fundamentals`` and ``filings``.

Input is the source library's OpenDART receipts table: one row per provider request with
its ``endpoint``, ``outcome``, ``request_json`` (whose ``parameters_json`` names
``corp_code``, ``bsns_year``, ``reprt_code`` and ``fs_div``), the response bytes as
``raw_base64`` with their ``raw_sha256``, and the collection instant
``retrieved_at_utc``. Other columns (fingerprints, validation notes) stay in the source
row and its hash.

Each ``financials`` row has one outcome, the first that applies:

- ``unreadable``: the request does not name an eight-digit corp code, a four-digit
  business year, a known report code and ``CFS`` or ``OFS``; or the request completed
  but its response bytes are not UTF-8 text whose SHA-256 is the recorded one, or that
  text is not a JSON document with provider status ``000`` and a nonempty ``list`` of
  statement lines. Every line has an ``sj_div`` of ``BS``, ``IS``, ``CIS``, ``CF`` or
  ``SCE``, a 14-digit ``rcept_no`` that starts with a calendar date, a numeric ``ord``, a
  three-letter ``currency`` and ``account_id``, ``account_nm`` and ``account_detail``, so a
  completed response maps whole.
- ``no_data`` (the provider had no statement for the request) and ``failed``.
- ``mismatched``: a completed response in which a line names another corp code, business
  year or report code than the request, or the lines name more than one receipt number.
- ``completed``: every other completed response. Only these give rows.
- ``unknown_outcome``: an outcome other than ``COMPLETED``, ``NO_DATA`` and ``FAILED``.

Rows of other endpoints (the corp code list shares the table) are ``other_endpoint``.
An ``unreadable``, ``mismatched`` or ``unknown_outcome`` row maps to one row with no
issuer, which the promotion refuses as a missing required column. The mapper argument
``accept`` (a sorted list of those outcome names) grants a promotion that leaves such
rows out instead; they stay counted in the promotion's ``source_outcomes``.

Shared rules of both mappers:

- The issuer is ``mint_issuer('dart_corp_code', corp_code)`` of the request's eight-digit
  corp code, the DART issuer anchor.
- The filing is the response's receipt number ``rcept_no`` (14 digits), whose first eight
  digits are the receipt date in Korea; that date is ``filed_date`` and the one time
  input. OpenDART answers with the statements of the latest filing for a report, so the
  receipt number names the filing (an amendment included) whose values the rows hold.
- ``form`` is the OpenDART report code: ``11011`` annual, ``11012`` half-year, ``11013``
  first quarter, ``11014`` third quarter.
- Responses that repeat a filing in one promotion are read once, the earliest retrieval
  speaking for them and the others staying in the source: for statements, the same
  response bytes collected twice; for filings, any responses of one filing (its
  consolidated and separate statements included). Two different statement responses
  of one filing both map, and the promotion refuses the natural keys they repeat.

``dart.fnltt@1`` writes one fundamentals row per statement line and the period it
measures. DART states no period dates; the fiscal year is taken to run January to
December of ``bsns_year``, which is exact for an issuer with a December year end and a
period label otherwise. The report covers the year to date through its end month (3, 6,
9 or 12) and its quarter (the last three months of that). By OpenDART's definitions:

- ``thstrm_amount`` of an income statement (``IS``, ``CIS``) measures the quarter:
  ``Q1``, ``Q2``, ``Q3`` or ``FY`` for the annual report;
- ``thstrm_add_amount`` of an income statement in a half-year or third-quarter report
  measures the year to date, ``H1`` or ``9M``. In the other reports it measures the same
  period as ``thstrm_amount`` (or is empty) and stays in the source;
- ``thstrm_amount`` of a cash flow or equity statement (``CF``, ``SCE``) measures the year
  to date: ``Q1``, ``H1``, ``9M`` or ``FY``;
- ``thstrm_amount`` of a balance sheet (``BS``) is the instant at the report's end; it
  takes the year-to-date label and no ``period_start``.

``period_start`` is the first day of the measured period and ``period_end`` the last day
of the report's end month. Prior-period comparatives (``frmtrm_*``, ``bfefrmtrm_*``)
stay in the source.

- ``concept`` is ``account_id`` as DART spells it (``-표준계정코드 미사용-`` for a line
  without a standard account), ``unit`` is the line's currency, and ``value`` is the
  amount text for ``decimal_text@1``: ``present`` for a decimal number, ``missing`` for
  an empty field and ``invalid`` (no value) for any other text.
- ``dimensions_hash`` is ``aas-dimensions-v1`` of ``fs_div``, ``sj_div``,
  ``account_nm``, ``account_detail``, ``ord`` and ``rcept_no``. DART repeats an account
  and its name inside one statement, so the line order is part of the line, and every
  filing's lines are their own records: an amendment adds records under its own receipt
  number rather than guessing which earlier line it restates.

``dart.fnltt_filings@1`` writes one filings row per completed response: the filing
``rcept_no`` with its ``form`` and ``filed_date``. ``accepted_at_us`` and ``period_end``
are NULL because the response states neither.

A spec partition selects requests by business year: the partition date of a row is
January 1 of its request's ``bsns_year``.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
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
# Report code: (year-to-date label, quarter label, end month).
REPORTS: Final = {
    "11011": ("FY", "FY", 12),
    "11012": ("H1", "Q2", 6),
    "11013": ("Q1", "Q1", 3),
    "11014": ("9M", "Q3", 9),
}
STATEMENTS: Final = ("BS", "CF", "CIS", "IS", "SCE")
REFUSED_OUTCOMES: Final = frozenset({"mismatched", "unknown_outcome", "unreadable"})


def _quoted(names: Iterable[str]) -> str:
    return ", ".join(f"'{name}'" for name in sorted(names))


def _params() -> str:
    """The request's ``parameters_json`` document, NULL unless both levels are JSON."""
    inner = (
        "CASE WHEN json_valid(request_json) THEN "
        "json_extract_string(request_json, '$.parameters_json') END"
    )
    return f"CASE WHEN json_valid({inner}) THEN {inner} END"


def _param(params: str, name: str) -> str:
    return f"json_extract_string({params}, '$.{name}')"


def _item(name: str) -> str:
    return f"_d_j.{name}"


_BODY: Final = (
    "CASE WHEN endpoint = 'financials' AND outcome = 'COMPLETED' "
    "THEN try(decode(try(from_base64(raw_base64)))) END"
)
# The response fields the mappers read, parsed once per response. ``json_transform``
# reads a value as ``json_extract_string`` would: text as is, other JSON as its text,
# and an absent or null field as NULL.
LINE_FIELDS: Final = (
    "account_detail",
    "account_id",
    "account_nm",
    "bsns_year",
    "corp_code",
    "currency",
    "ord",
    "rcept_no",
    "reprt_code",
    "sj_div",
    "thstrm_add_amount",
    "thstrm_amount",
)
_SHAPE: Final = json.dumps(
    {"status": "VARCHAR", "list": [dict.fromkeys(LINE_FIELDS, "VARCHAR")]},
    separators=(",", ":"),
)


def _document(text: str) -> str:
    """The response fields of JSON ``text`` as a struct, NULL when it is not JSON."""
    return f"json_transform(CASE WHEN json_valid({text}) THEN {text} END, '{_SHAPE}')"


def _outcome(body: str, document: str, params: str) -> str:
    """A receipt row's outcome over its response text, parsed response and parameters.

    ``body`` and ``document`` must be cheap to repeat (columns or lambda parameters).
    """
    request = (
        f"regexp_full_match({_param(params, 'corp_code')}, '[0-9]{{8}}') "
        f"AND regexp_full_match({_param(params, 'bsns_year')}, '[0-9]{{4}}') "
        f"AND {_param(params, 'reprt_code')} IN ({_quoted(REPORTS)}) "
        f"AND {_param(params, 'fs_div')} IN ('CFS', 'OFS')"
    )
    lines = f"{document}.list"
    line = (
        f"j.sj_div IN ({_quoted(STATEMENTS)}) "
        "AND regexp_full_match(j.rcept_no, '[0-9]{14}') "
        "AND try_strptime(substr(j.rcept_no, 1, 8), '%Y%m%d') IS NOT NULL "
        "AND regexp_full_match(j.ord, '[0-9]+') "
        "AND regexp_full_match(j.currency, '[A-Z]{3}') "
        "AND j.account_id IS NOT NULL AND j.account_nm IS NOT NULL "
        "AND j.account_detail IS NOT NULL"
    )
    readable = (
        f"{body} IS NOT NULL AND sha256({body}) = raw_sha256 "
        f"AND {document}.status = '000' AND len({lines}) > 0 "
        f"AND len(list_filter({lines}, lambda j: NOT coalesce({line}, false))) = 0"
    )
    differs = " OR ".join(
        f"j.{name} IS DISTINCT FROM {_param(params, name)}"
        for name in ("corp_code", "bsns_year", "reprt_code")
    )
    numbers = f"list_distinct(list_transform({lines}, lambda j: j.rcept_no))"
    agrees = f"len(list_filter({lines}, lambda j: {differs})) = 0 AND len({numbers}) = 1"
    return (
        "CASE WHEN endpoint IS DISTINCT FROM 'financials' THEN 'other_endpoint' "
        f"WHEN NOT coalesce({request}, false) THEN 'unreadable' "
        "WHEN outcome = 'NO_DATA' THEN 'no_data' WHEN outcome = 'FAILED' THEN 'failed' "
        f"WHEN outcome = 'COMPLETED' THEN CASE WHEN NOT coalesce({readable}, false) "
        f"THEN 'unreadable' WHEN {agrees} THEN 'completed' ELSE 'mismatched' END "
        "ELSE 'unknown_outcome' END"
    )


def _ingested() -> str:
    return (
        f"CASE WHEN regexp_full_match(retrieved_at_utc, '{_INSTANT}') "
        "THEN epoch_us(TRY_CAST(retrieved_at_utc AS TIMESTAMPTZ)) END"
    )


def _accepted(args: Mapping[str, object]) -> frozenset[str]:
    accepted = args.get("accept", [])
    assert isinstance(accepted, list)  # noqa: S101 -- check_args admitted the spec
    return frozenset(str(name) for name in accepted)


def _lines(source: str, args: Mapping[str, object], *, filing_only: bool) -> str:
    """One row per statement line of a response read once, one NULL line for a refused row.

    Completed responses that repeat a filing keep the earliest retrieval. For filings
    (``filing_only``) a filing is its corp code, report and receipt number, which fix
    every filings column. For statements it is also the business year, ``fs_div`` and
    the exact response bytes, so two different responses for one filing both stay and
    the promotion refuses the natural keys they repeat.
    """
    receipts = f"SELECT *, {_params()} AS _d_params, {_BODY} AS _d_body FROM {source}"  # noqa: S608 -- engine-named relation
    parsed = f"SELECT *, {_document('_d_body')} AS _d_doc FROM ({receipts})"  # noqa: S608 -- engine-named relation
    outcomes = (
        f"SELECT *, {_outcome('_d_body', '_d_doc', '_d_params')} AS _d_outcome, "  # noqa: S608 -- engine-named relation
        f"{_ingested()} AS _d_ingested FROM ({parsed})"
    )
    keys = (
        ("corp_code", "reprt_code")
        if filing_only
        else ("corp_code", "bsns_year", "reprt_code", "fs_div")
    )
    filing = ", ".join(_param("_d_params", name) for name in keys)
    if not filing_only:
        filing += ", raw_sha256"
    first = (
        f"row_number() OVER (PARTITION BY _d_outcome, {filing}, "
        "_d_doc.list[1].rcept_no "
        "ORDER BY _d_ingested NULLS LAST, _aas_pin, _aas_ordinal) = 1"
    )
    refused = REFUSED_OUTCOMES - _accepted(args)
    kept = f"_d_outcome = 'completed' AND {first}"
    if refused:
        kept += f" OR _d_outcome IN ({_quoted(refused)})"
    return (
        "SELECT * EXCLUDE (_d_body, _d_doc, _d_lines), unnest(_d_lines) AS _d_j, "  # noqa: S608 -- engine-named relation
        "generate_subscripts(_d_lines, 1) - 1 AS _d_line FROM ("
        "SELECT *, CASE WHEN _d_outcome = 'completed' THEN _d_doc.list "
        "ELSE [NULL] END AS _d_lines "
        f"FROM ({outcomes}) QUALIFY {kept})"
    )


def _issuer() -> str:
    """The minted DART issuer of a completed response's request, else NULL."""
    corp = _param("_d_params", "corp_code")
    anchor = f'["{ISSUER_FORMAT}","dart_corp_code","'
    return (
        f"CASE WHEN _d_outcome = 'completed' "
        f"THEN 'iss-' || sha256('{anchor}' || {corp} || '\"]') END"
    )


def _filing() -> tuple[str, str]:
    """(receipt number, filed date) of a line of a completed response."""
    number = _item("rcept_no")
    return number, f"CAST(try_strptime(substr({number}, 1, 8), '%Y%m%d') AS DATE)"


def _form() -> str:
    code = _item("reprt_code")
    return f"CASE WHEN {code} IN ({_quoted(REPORTS)}) THEN {code} END"


def _partition() -> str:
    year = _param(_params(), "bsns_year")
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
        if not set(args) <= {"accept"}:
            raise ValueError("DART receipt mappers take only an accept argument")
        if "accept" in args:
            accepted = args["accept"]
            if (
                not isinstance(accepted, list)
                or not accepted
                or accepted != sorted(set(accepted))
                or not set(accepted) <= REFUSED_OUTCOMES
            ):
                raise ValueError(
                    "accept is a sorted nonempty list of distinct outcomes among "
                    + ", ".join(sorted(REFUSED_OUTCOMES))
                )

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return dict.fromkeys(_COLUMNS, _TEXT)

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def outcome(self, args: Mapping[str, object]) -> str:
        del args
        # Lambda parameters bind the decoded and parsed response once per row.
        inner = _outcome("b", "d", _params())
        return (
            f"list_transform([{_BODY}], lambda b: "
            f"list_transform([{_document('b')}], lambda d: {inner})[1])[1]"
        )


def _period(field: str) -> tuple[str, str]:
    """(fiscal_period, period_start) SQL of a line's amount ``field`` (0 this term, 1 add)."""
    slot, sheet = _item("reprt_code"), _item("sj_div")
    year = f"CAST({_item('bsns_year')} AS INTEGER)"
    ytd = " ".join(f"WHEN '{code}' THEN '{label}'" for code, (label, _, _) in REPORTS.items())
    quarter = " ".join(f"WHEN '{code}' THEN '{label}'" for code, (_, label, _) in REPORTS.items())
    first = " ".join(
        f"WHEN '{code}' THEN {1 if label == 'FY' else month - 2}"
        for code, (_, label, month) in REPORTS.items()
    )
    measures_quarter = f"{field} = 0 AND {sheet} IN ('CIS', 'IS')"
    label = (
        f"CASE WHEN {measures_quarter} THEN CASE {slot} {quarter} END "
        f"ELSE CASE {slot} {ytd} END END"
    )
    start = (
        f"CASE WHEN {sheet} = 'BS' THEN CAST(NULL AS DATE) "
        f"WHEN {measures_quarter} THEN make_date({year}, CASE {slot} {first} END, 1) "
        f"ELSE make_date({year}, 1, 1) END"
    )
    return label, start


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
        number, filed = _filing()
        slot = _item("reprt_code")
        months = " ".join(f"WHEN '{code}' THEN {month}" for code, (_, _, month) in REPORTS.items())
        end = (
            f"last_day(make_date(CAST({_item('bsns_year')} AS INTEGER), "
            f"CASE {slot} {months} END, 1))"
        )
        label, start = _period("_d_field")
        amount = (
            f"CASE WHEN _d_field = 0 THEN {_item('thstrm_amount')} "
            f"ELSE {_item('thstrm_add_amount')} END"
        )
        dimensions = dimensions_hash_sql(
            [
                ("account_detail", _item("account_detail")),
                ("account_nm", _item("account_nm")),
                ("fs_div", _param("_d_params", "fs_div")),
                ("ord", _item("ord")),
                ("rcept_no", number),
                ("sj_div", _item("sj_div")),
            ]
        )
        state = (
            "CASE WHEN _d_amount = '' THEN 'missing' "
            f"WHEN regexp_full_match(_d_amount, '{_DECIMAL}') THEN 'present' "
            "WHEN _d_amount IS NOT NULL THEN 'invalid' END"
        )
        # The year to date of an income statement in a half-year or third-quarter report.
        cumulative = (
            f"{_item('sj_div')} IN ('CIS', 'IS') AND {slot} IN ('11012', '11014') "
            f"AND {_item('thstrm_add_amount')} IS NOT NULL"
        )
        fields = (
            "SELECT *, "  # noqa: S608 -- engine-named relation
            f"{amount} AS _d_amount FROM (SELECT *, unnest([0, 1]) AS _d_field FROM ("
            f"{_lines(source, args, filing_only=False)})) "
            f"WHERE _d_field = 0 OR (_d_outcome = 'completed' AND {cumulative})"
        )
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "2 * _d_line + _d_field AS _aas_item, "
            "_d_ingested AS _aas_ingested_at_us, "
            f"{_issuer()} AS issuer_id, {_item('account_id')} AS concept, "
            f"{start} AS period_start, {end} AS period_end, {label} AS fiscal_period, "
            f"{_item('currency')} AS unit, "
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
        number, filed = _filing()
        # Every line of a completed response names the same receipt number, so the first
        # line speaks for the response.
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "_d_ingested AS _aas_ingested_at_us, "
            f"{_issuer()} AS issuer_id, "
            f"{number} AS filing_id, {_form()} AS form, {filed} AS filed_date, "
            "CAST(NULL AS BIGINT) AS accepted_at_us, CAST(NULL AS DATE) AS period_end, "
            f"{filed} AS _aas_t_filed_date FROM ({_lines(source, args, filing_only=True)}) "
            "WHERE _d_line = 0"
        )
