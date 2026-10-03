"""SEC submissions as ``filings`` and SEC company facts as ``fundamentals``.

Both mappers are issuer level: the issuer is ``mint_issuer('sec_cik', cik)`` of the
ten-digit CIK, the SEC issuer anchor, and no instrument is resolved (a fundamentals row
names none). A CIK of another
spelling leaves the row without an issuer, which the promotion refuses as a missing
required column.

``sec.submissions@1`` reads the filings table of an SEC submissions archive
(``sec.submissions_filings@1``): one row per filing a filer's submissions document
lists, its fields as SEC wrote them. It writes one ``filings`` row per row:

- ``filing_id`` is ``accessionNumber`` in its ``##########-##-######`` spelling,
  ``form`` is ``form`` as written, ``filed_date`` is ``filingDate`` and ``period_end``
  is ``reportDate`` (NULL when empty). Any other spelling of a required field leaves it
  NULL and the promotion refuses the row; nothing is repaired.
- ``accepted_at_us`` is ``acceptanceDateTime`` (``YYYY-MM-DDTHH:MM:SS[.fff]Z``, UTC) in
  microseconds. EDGAR states no acceptance instant for filings it holds only by date: it
  writes their local midnight in New York. Such an instant, and any other spelling, is
  NULL, so the row's time is unknown rather than earlier than the filing.
- SEC lists some filings twice (in a filer's recent filings and an older page, or in two
  pages). Rows that state the same CIK, accession, dates, acceptance and form are one
  listing, read once from its first row in source order; rows of one accession that
  differ in any of those fields all map, and the promotion refuses the natural key they
  repeat.
- The time inputs are ``accepted_at`` (the acceptance instant, for
  ``source_column@1``) and ``filed_date``. A spec partition selects rows by
  ``filingDate``.

``sec.companyfacts@1`` reads the company facts table (one row per fact SEC's
companyfacts document reports in one filing: ``cik``, ``taxonomy``, ``tag``, ``unit``,
``period_start``, ``period_end``, ``accession_number``, ``form``, ``filed``, ``value``
as decimal text and the collection instant ``retrieved_at``). Other columns (SEC's
``fy``, ``fp`` and ``frame`` among them) stay in the source row and its hash. It writes
one ``fundamentals`` row per fact:

- ``concept`` is ``taxonomy:tag``, ``unit`` is SEC's unit, and ``period_start`` (NULL for
  an instant) and ``period_end`` are the fact's own dates.
- ``fiscal_period`` names the measured span from those dates alone: ``instant``, or
  ``P<n>D`` for a duration of ``n`` days counting both ends. SEC's ``fp`` describes the
  filing that reports a fact, not the fact (a 10-K's prior-year comparatives carry the
  10-K's ``FY``), and many filings state none, so it stays in the source.
- ``dimensions_hash`` is ``aas-dimensions-v1`` of the ``accession``: every filing's
  facts are their own records. A later filing that reports the same concept and period
  (a comparative, a restatement, an amendment) adds records under its own accession
  rather than guessing which earlier value it restates, and the earlier filing's value
  stays its fact. A changed value of one accession in a later collection is a SUPERSEDE.
- ``form`` and ``accession`` are the reporting filing's. ``accepted_at_us`` is that
  accession's acceptance instant in the pinned ``filings`` generation (mapper argument
  ``filings``): NULL when the generation does not hold the accession or its rows
  disagree on the instant.
- ``value`` is the decimal text for ``decimal_text@1``: ``present`` for a decimal number
  (an exponent included), ``missing`` for empty text and ``invalid`` (no value) for any
  other text.
- The time inputs are ``accepted_at`` (the joined acceptance instant, for
  ``source_column@1``) and ``filed``. A spec partition selects facts by ``filed``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from aegis_alpha.storage.identity import ISSUER_FORMAT
from aegis_alpha.storage.promotion.formats import dimensions_hash_sql
from aegis_alpha.storage.promotion.mappers import Reference, reference_pin, reference_table
from aegis_alpha.storage.promotion.time_rules import InputKind

_TEXT: Final = frozenset({"VARCHAR"})
_ACCESSION: Final = "[0-9]{10}-[0-9]{2}-[0-9]{6}"
_DAY: Final = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
_INSTANT: Final = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?Z"
_DECIMAL: Final = "[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([eE][+-]?[0-9]+)?"
FILINGS_REFERENCE: Final = "filings"
# The fields a submissions row states about its filing; a repeated listing repeats them all.
_LISTING: Final = 'cik, "accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", form'


def _issuer(cik: str) -> str:
    """The minted SEC issuer of a ten-digit CIK, else NULL."""
    anchor = f'["{ISSUER_FORMAT}","sec_cik","'
    minted = f"'iss-' || sha256('{anchor}' || {cik} || '\"]')"
    return f"CASE WHEN regexp_full_match({cik}, '[0-9]{{10}}') THEN {minted} END"


def _day(text: str) -> str:
    return f"CASE WHEN regexp_full_match({text}, '{_DAY}') THEN TRY_CAST({text} AS DATE) END"


class SecSubmissions:
    name: Final = "sec.submissions"
    major: Final = 1
    provider: Final = "sec"
    domain: Final = "filings"
    date_column: Final = "filed_date"
    time_inputs: Final[Mapping[str, InputKind]] = {"accepted_at": "utc_us", "filed_date": "date"}

    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None

    # The filings table of an SEC submissions archive (``sec.submissions_filings@1``).
    source_prefixes: Final = ("sec-submissions-filings-",)

    @property
    def partition_sql(self) -> str:
        return _day('"filingDate"')

    def check_args(self, args: Mapping[str, object]) -> None:
        if args:
            raise ValueError("sec.submissions@1 takes no arguments")

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return dict.fromkeys(
            ("cik", "accessionNumber", "filingDate", "reportDate", "acceptanceDateTime", "form"),
            _TEXT,
        )

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        del args
        text = '"acceptanceDateTime"'
        instant = (
            f"CASE WHEN regexp_full_match({text}, '{_INSTANT}') "
            f"THEN epoch_us(TRY_CAST({text} AS TIMESTAMPTZ)) END"
        )
        # EDGAR writes local midnight in New York for a filing it holds only by date.
        midnight = "epoch_us(timezone('America/New_York', CAST(_s_filed AS TIMESTAMP)))"
        accepted = f"CASE WHEN _s_instant IS DISTINCT FROM {midnight} THEN _s_instant END"
        number = '"accessionNumber"'
        report = _day('"reportDate"')
        filed = _day('"filingDate"')
        return (
            "SELECT _aas_pin, _aas_ordinal, _aas_row_hash, "  # noqa: S608 -- engine-named relation
            "CAST(NULL AS BIGINT) AS _aas_ingested_at_us, "
            f"{_issuer('cik')} AS issuer_id, "
            f"CASE WHEN regexp_full_match({number}, '{_ACCESSION}') THEN {number} END "
            "AS filing_id, "
            "form, _s_filed AS filed_date, "
            f"{accepted} AS accepted_at_us, "
            f"{report} AS period_end, "
            f"{accepted} AS _aas_t_accepted_at, _s_filed AS _aas_t_filed_date "
            f"FROM (SELECT *, {filed} AS _s_filed, "
            f"{instant} AS _s_instant FROM {source} QUALIFY row_number() OVER ("
            f"PARTITION BY {_LISTING} ORDER BY _aas_pin, _aas_ordinal) = 1)"
        )


class SecCompanyfacts:
    name: Final = "sec.companyfacts"
    major: Final = 1
    provider: Final = "sec"
    domain: Final = "fundamentals"
    date_column: Final = "period_end"
    partition_sql: Final = "filed"
    # Company facts tables come from several normalizations; their columns are checked.
    source_prefixes: Final = ()
    row_flags: Final[Mapping[str, str]] = {}
    manifest_items: Final = None
    time_inputs: Final[Mapping[str, InputKind]] = {"accepted_at": "utc_us", "filed": "date"}

    def check_args(self, args: Mapping[str, object]) -> None:
        if set(args) != {FILINGS_REFERENCE}:
            raise ValueError("sec.companyfacts@1 takes exactly a filings generation pin")
        pin = reference_pin(args[FILINGS_REFERENCE], "sec.companyfacts@1 filings")
        if not pin.dataset_id.startswith("filings."):
            raise ValueError("sec.companyfacts@1 filings must pin a filings dataset")

    def references(self, args: Mapping[str, object]) -> Mapping[str, Reference]:
        pin = reference_pin(args[FILINGS_REFERENCE], "sec.companyfacts@1 filings")
        return {FILINGS_REFERENCE: Reference("filings", pin)}

    def source_columns(self) -> Mapping[str, frozenset[str]]:
        return {
            **dict.fromkeys(
                ("cik", "taxonomy", "tag", "unit", "form", "accession_number", "value"), _TEXT
            ),
            "period_start": frozenset({"DATE"}),
            "period_end": frozenset({"DATE"}),
            "filed": frozenset({"DATE"}),
            "retrieved_at": frozenset({"TIMESTAMP WITH TIME ZONE"}),
        }

    def numeric_columns(self, args: Mapping[str, object]) -> Mapping[str, str]:
        del args
        return {"value": "VARCHAR"}

    def identity(self, args: Mapping[str, object]) -> None:
        del args

    def select(self, source: str, args: Mapping[str, object]) -> str:
        del args
        filings = reference_table(FILINGS_REFERENCE)
        # One instant per accession: co-registrants share a filing, and rows that
        # disagree on its acceptance give none.
        accepted = (
            "SELECT filing_id, CASE WHEN count(DISTINCT accepted_at_us) = 1 "  # noqa: S608 -- engine-named relation
            "AND count(accepted_at_us) = count(*) THEN min(accepted_at_us) END AS accepted_at_us "
            f"FROM {filings} GROUP BY filing_id"
        )
        span = (
            "CASE WHEN f.period_start IS NULL THEN 'instant' "
            "ELSE 'P' || CAST(date_diff('day', f.period_start, f.period_end) + 1 AS VARCHAR) "
            "|| 'D' END"
        )
        state = (
            "CASE WHEN f.value = '' THEN 'missing' "
            f"WHEN regexp_full_match(f.value, '{_DECIMAL}') THEN 'present' "
            "WHEN f.value IS NOT NULL THEN 'invalid' END"
        )
        accession = (
            f"CASE WHEN regexp_full_match(f.accession_number, '{_ACCESSION}') "
            "THEN f.accession_number END"
        )
        dimensions = dimensions_hash_sql([("accession", accession)])
        return (
            "SELECT f._aas_pin, f._aas_ordinal, f._aas_row_hash, "  # noqa: S608 -- engine-named relation
            "epoch_us(f.retrieved_at) AS _aas_ingested_at_us, "
            f"{_issuer('f.cik')} AS issuer_id, CAST(NULL AS VARCHAR) AS instrument_id, "
            "f.taxonomy || ':' || f.tag AS concept, f.period_start, f.period_end, "
            f"{span} AS fiscal_period, f.unit, {dimensions} AS dimensions_hash, "
            f"f.form, {accession} AS accession, a.accepted_at_us, "
            f"CASE WHEN {state} = 'present' THEN f.value END AS value, {state} AS value_state, "
            "a.accepted_at_us AS _aas_t_accepted_at, f.filed AS _aas_t_filed "
            f"FROM {source} f LEFT JOIN ({accepted}) a ON a.filing_id = f.accession_number"
        )
