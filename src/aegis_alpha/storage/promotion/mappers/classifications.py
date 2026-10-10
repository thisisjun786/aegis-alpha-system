"""Snapshot classifications: Norgate security types and exchanges, SEC SIC, KIND industries.

Each source is a snapshot that states a subject's current classification and nothing about
when it began. A classification row therefore starts at the snapshot: ``effective_from``
is the snapshot's date and ``effective_to`` is NULL, and the row is never extended into the
past. A later snapshot is promoted as rows of its own date, so a reader takes, among the
rows known at its cutoff, the latest ``effective_from`` on or before the day it asks about.

- The subject is an instrument or an issuer. A source that carries the subject's permanent
  anchor (a Norgate asset ID, an SEC CIK) mints the ID from it, exactly as the identity
  registry does; a source that carries only a code (a KRX short code) resolves it through
  the pinned identity snapshot. A ticker, name or date never makes an ID.
- ``scheme`` names the provider's classification; ``code`` is its value and ``label`` its
  provider text. Nothing is translated between schemes.
- A row whose required value is absent or malformed keeps no subject or code and is
  refused, never repaired. A source row with no classification at all (an SEC filer
  without a SIC code or without its description, such as the unassigned ``0000``, a KIND
  row without an industry) maps to no row.
- The time inputs are the snapshot's date ``as_of`` and, where the source records it, the
  collection instant ``observed_at``. A date-only snapshot is known at
  ``local_day_end@1(as_of)``, which a strict reader uses only under a grant; a snapshot
  with its collection instant is known at ``source_column@1(observed_at)``.

``norgate.classification@1`` reads the Norgate security master (``assetid``, the type
levels ``subtype1``..``subtype3``, ``exchange``, ``exchange_full`` and the text dates
``first_date``, ``last_date``) with the arguments ``scheme`` and ``as_of``, the export date
the spec declares from the export's evidence. ``norgate.security_type`` codes the type
path ``subtype1 > subtype2 > subtype3`` (levels present, none skipped) and labels it with
the most specific level; ``norgate.exchange`` codes the listing exchange and labels it
with its full name. A row dated after ``as_of`` contradicts the declared export date and
is refused.

``sec.sic@1`` reads the company table ``aas import sec-companies`` commits from an SEC
submissions archive (``member``, the stated ``cik``, ``sic``, ``sic_description``,
``latest_filing_date``) with the argument ``as_of``, the archive's download date. The
issuer is minted from the member's ten-digit CIK when the document states the same CIK;
the code is the four-digit SIC and the label its description; a company that states no
SIC or no description has no classification. A company whose latest filing is after
``as_of`` is refused.

``kind.industry@1`` reads the KIND listed-company table ``aas identity kr-import`` commits
(``short_code``, ``industry``, ``retrieved_at_utc``). The instrument is the snapshot's
resolution of (``kind``, ``krx_short_code``) at the collection instant, and ``as_of`` is
that instant's date in Asia/Seoul. KIND names an industry without a code, so the code and
the label are both its text. Every source row with an industry maps to a row.

``kind.industry@2`` reads the same columns and maps them the same way, except that source
rows equal in (``short_code``, ``industry``, ``retrieved_at_utc``) map once. KIND repeats
a company on rows that differ only in a column no classification reads (the 지역 region
of an administrative merger, for one), and ``aas identity kr-import`` keeps every raw row.
Of such rows the one with the smallest source row hash maps, then the lowest
(``_aas_pin``, ``_aas_ordinal``), and its provenance is the row's; the hash depends only on
the row's content, so the classification rows and their hashes do not depend on the order
in which the rows were collected. The other rows stay in the source, unselected. Two rows
of one short code at one instant with different industries both map, and the promotion
refuses the repeated natural key rather than choosing an industry.
"""

from __future__ import annotations

# ruff: noqa: S608 -- the relation is engine-named and every literal is code-owned or quoted.
import re
from collections.abc import Mapping
from datetime import date
from typing import Final

from aegis_alpha.storage.identity import INSTRUMENT_FORMAT, ISSUER_FORMAT
from aegis_alpha.storage.promotion.formats import sql_literal
from aegis_alpha.storage.promotion.mappers import IdentityKey
from aegis_alpha.storage.promotion.mappers.common import iso_day
from aegis_alpha.storage.promotion.time_rules import InputKind

_TEXT: Final = frozenset({"VARCHAR"})
_DAY: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_INSTANT_SQL: Final = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?Z"
_COMMON: Final = "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "
# A Norgate asset ID is canonical as a positive decimal of at most 18 digits.
_ASSETID_LIMIT: Final = 10**18
SECURITY_TYPE: Final = "norgate.security_type"
EXCHANGE: Final = "norgate.exchange"
SIC: Final = "sec.sic"
INDUSTRY: Final = "kind.industry"
KIND_ZONE: Final = "Asia/Seoul"
TYPE_SEPARATOR: Final = " > "
COMPANIES_PREFIX: Final = "sec-submissions-companies-"
# kind.industry@2 maps source rows repeated on (short_code, industry, retrieved_at_utc) once.
_KIND_COLLAPSED: Final = 2
_KIND_MAJORS: Final = frozenset({1, _KIND_COLLAPSED})


def _observed_us(text: str) -> str:
    """UTC microseconds of an ISO ``...Z`` instant text; NULL for any other spelling."""
    return (
        f"CASE WHEN regexp_full_match({text}, '{_INSTANT_SQL}') "
        f"THEN epoch_us(try_cast(rtrim({text}, 'Z') AS TIMESTAMP)) END"
    )


def _kind_day(instant: str) -> str:
    """The Asia/Seoul date of a UTC microsecond instant."""
    zone = sql_literal(KIND_ZONE)
    return f"CAST(timezone({zone}, timezone('UTC', make_timestamp({instant}))) AS DATE)"


def minted_sql(prefix: str, fmt: str, namespace: str, token: str) -> str:
    """SQL of ``mint_instrument``/``mint_issuer`` over a canonical token expression.

    The canonical JSON of ``[format, namespace, token]`` needs no escaping for the digit
    tokens these mappers mint from, so its bytes are the plain concatenation.
    """
    head = sql_literal(f'["{fmt}","{namespace}","')
    return f"('{prefix}' || sha256({head} || {token} || '\"]'))"


def _nonempty(text: str) -> str:
    return f"({text} IS NOT NULL AND {text} <> '')"


def _as_of(args: Mapping[str, object], name: str) -> date:
    value = args.get("as_of")
    if not isinstance(value, str) or _DAY.fullmatch(value) is None:
        raise ValueError(f"{name} as_of must be a YYYY-MM-DD date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{name} as_of must be a YYYY-MM-DD date") from None


def _day_literal(args: Mapping[str, object]) -> str:
    return f"DATE '{date.fromisoformat(str(args['as_of'])).isoformat()}'"


def _rows(subject: str, kind: str, scheme: str, value: tuple[str, str], start: str) -> str:
    """The classification columns from SQL for the subject, (code, label) and start date."""
    code, label = value
    return (
        f"{subject} AS subject_id, '{kind}' AS subject_kind, {sql_literal(scheme)} AS scheme, "
        f"{code} AS code, {label} AS label, {start} AS effective_from, "
        "CAST(NULL AS DATE) AS effective_to"
    )


class NorgateClassification:
    name: Final = "norgate.classification"
    major: Final = 1
    provider: Final = "norgate"
    domain: Final = "classifications"
    source_prefixes: Final = ()
    partition_sql: Final = iso_day("first_date")
    date_column: Final = "effective_from"
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False
    time_inputs: Final[Mapping[str, InputKind]] = {"as_of": "date"}
    schemes: Final = (SECURITY_TYPE, EXCHANGE)

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {"scheme", "as_of"}:
            raise ValueError("norgate.classification@1 takes exactly scheme and as_of arguments")
        if args["scheme"] not in self.schemes:
            raise ValueError(f"norgate.classification@1 scheme is one of {list(self.schemes)}")
        _as_of(args, "norgate.classification@1")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "assetid": frozenset({"BIGINT"}),
            "subtype1": _TEXT,
            "subtype2": _TEXT,
            "subtype3": _TEXT,
            "exchange": _TEXT,
            "exchange_full": _TEXT,
            "first_date": _TEXT,
            "last_date": _TEXT,
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        day = _day_literal(args)
        later = " OR ".join(
            f"coalesce({iso_day(column)} > {day}, false)" for column in ("first_date", "last_date")
        )
        canonical = f"assetid > 0 AND assetid < {_ASSETID_LIMIT} AND NOT ({later})"
        assetid = "CAST(assetid AS VARCHAR)"
        subject = (
            f"CASE WHEN {canonical} THEN "
            f"{minted_sql('ins-', INSTRUMENT_FORMAT, 'norgate_assetid', assetid)} END"
        )
        if args["scheme"] == SECURITY_TYPE:
            levels = ("subtype1", "subtype2", "subtype3")
            # Present levels must be nonempty and contiguous from the first.
            shaped = (
                f"{_nonempty('subtype1')} AND (subtype2 IS NULL OR subtype2 <> '') "
                "AND (subtype3 IS NULL OR (subtype3 <> '' AND subtype2 IS NOT NULL))"
            )
            path = f"concat_ws({sql_literal(TYPE_SEPARATOR)}, {', '.join(levels)})"
            code = f"CASE WHEN {shaped} THEN {path} END"
            label = f"CASE WHEN {shaped} THEN coalesce(subtype3, subtype2, subtype1) END"
        else:
            shaped = f"{_nonempty('exchange')} AND {_nonempty('exchange_full')}"
            code = f"CASE WHEN {shaped} THEN exchange END"
            label = f"CASE WHEN {shaped} THEN exchange_full END"
        scheme = str(args["scheme"])
        return (
            _COMMON
            + "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            + _rows(subject, "instrument", scheme, (code, label), day)
            + f", {day} AS _aas_t_as_of FROM {source}"
        )


class SecSic:
    name: Final = "sec.sic"
    major: Final = 1
    provider: Final = "sec"
    domain: Final = "classifications"
    source_prefixes: Final = (COMPANIES_PREFIX,)
    partition_sql: Final = iso_day("latest_filing_date")
    date_column: Final = "effective_from"
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False
    time_inputs: Final[Mapping[str, InputKind]] = {"as_of": "date"}

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {"as_of"}:
            raise ValueError("sec.sic@1 takes exactly an as_of argument")
        _as_of(args, "sec.sic@1")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            "member": _TEXT,
            "cik": _TEXT,
            "sic": _TEXT,
            "sic_description": _TEXT,
            "latest_filing_date": _TEXT,
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        day = _day_literal(args)
        cik = "substr(member, 4, 10)"
        stated = (
            r"regexp_full_match(member, 'CIK[0-9]{10}[.]json') "
            "AND regexp_full_match(cik, '[0-9]{1,10}') "
            f"AND lpad(cik, 10, '0') = {cik}"
        )
        later = f"coalesce({iso_day('latest_filing_date')} > {day}, false)"
        subject = (
            f"CASE WHEN {stated} AND NOT {later} THEN "
            f"{minted_sql('iss-', ISSUER_FORMAT, 'sec_cik', cik)} END"
        )
        code = "CASE WHEN regexp_full_match(sic, '[0-9]{4}') THEN sic END"
        return (
            _COMMON
            + "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            + _rows(subject, "issuer", SIC, (code, "sic_description"), day)
            + f", {day} AS _aas_t_as_of FROM {source} "
            + f"WHERE {_nonempty('sic')} AND {_nonempty('sic_description')}"
        )


class KindIndustry:
    """``kind.industry@1`` maps every source row; ``@2`` maps each repeated row once."""

    name: Final = "kind.industry"
    provider: Final = "kind"
    domain: Final = "classifications"
    source_prefixes: Final = ()
    partition_sql: Final = _kind_day(_observed_us("retrieved_at_utc"))
    date_column: Final = "effective_from"
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    expands: Final = False
    time_inputs: Final[Mapping[str, InputKind]] = {"observed_at": "utc_us", "as_of": "date"}

    def __init__(self, major: int = 1) -> None:
        if major not in _KIND_MAJORS:
            raise ValueError(f"kind.industry has no major {major}")
        self.major = major

    def check_args(self, args: Mapping[str, object]) -> None:
        if args:
            raise ValueError(f"kind.industry@{self.major} takes no arguments")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {"short_code": _TEXT, "industry": _TEXT, "retrieved_at_utc": _TEXT}

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> IdentityKey:
        del args
        return IdentityKey("kind", "krx_short_code")

    def outcome(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        del args
        observed = _observed_us("retrieved_at_utc")
        local = _kind_day("_k_observed")
        once = ""
        if self.major == _KIND_COLLAPSED:
            # Content first, so the kept row does not depend on the collection order.
            once = (
                " QUALIFY row_number() OVER (PARTITION BY short_code, industry, "
                "retrieved_at_utc ORDER BY _aas_row_hash NULLS LAST, _aas_pin, _aas_ordinal) = 1"
            )
        return (
            _COMMON
            + "_k_observed AS _aas_ingested_at_us, "
            + "short_code AS _aas_id_token, _k_observed AS _aas_id_at_us, "
            + "subject_kind, scheme, code, label, effective_from, effective_to, "
            + "_k_observed AS _aas_t_observed_at, effective_from AS _aas_t_as_of FROM ("
            + "SELECT *, 'instrument' AS subject_kind, "
            + f"{sql_literal(INDUSTRY)} AS scheme, industry AS code, industry AS label, "
            + f"{local} AS effective_from, CAST(NULL AS DATE) AS effective_to FROM ("
            + f"SELECT *, {observed} AS _k_observed FROM {source} "
            + f"WHERE {_nonempty('industry')}{once}))"
        )
