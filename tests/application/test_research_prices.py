"""A declared research run whose open and close panels come from canonical price pins.

The observation route reads a retained reference panel. This route reads the canonical
unadjusted bars of a price binding through ``read_heads`` in research mode, so a KRW run
reads the promoted ``prices.kr.eodhd`` chain from its first session rather than from where
a retained snapshot happened to start. The fixture's canonical execution prices carry the
same numbers as its observation panels, which is what the equivalence test holds the two
routes to; the KRW chain is promoted here from a synthetic source table.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application import backtest_prepare
from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.backtest_prepare import PreparedResearchRun, prepare_research_run
from aegis_alpha.application.research_run import ResearchRunError, parse_research_run_request
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.storage import publication
from aegis_alpha.storage.backtest_requests import request_schema
from aegis_alpha.storage.identity import (
    mint_instrument,
    parse_registry,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.promotion.engine import promote
from aegis_alpha.storage.read_heads import HeadRead, HeadRow
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_backtest_prepare import BUDGET, DAYS
from tests.application.test_prepare_cli import sha
from tests.application.test_research_composition import (  # noqa: F401 -- shared fixture
    _as_sleeve_run,
    _composition,
    composed,
)
from tests.application.test_research_execution import CLOCK_NS, KNOWLEDGE_US, copy_installation
from tests.application.test_run_research import (
    _execute,
    _installed,
    _rerun,
    _run_id,
    _show,
    _write,
)
from tests.storage.promotion_support import (
    UNBOUNDED,
    add_source,
    at,
    publish_calendar,
    spec,
    us,
)

if TYPE_CHECKING:
    from pathlib import Path

    from aegis_alpha.storage.workspace import Workspace

type Document = dict[str, Any]

# The fixture's numbers per instrument and session, as test_backtest_prepare.price_rows
# writes them. Whole numbers, so a KRW chain can carry them at 1000 won per unit.
VALUES = {
    "ASSET_A": (10, 12, 15, 15, 12, 12, 12, 12),
    "ASSET_B": (10, 10, 11, 11, 20, 20, 20, 20),
    "REF_X": (10, 10, 10, 10, 10, 10, 10, 10),
}
KR_SYMBOLS = {"ASSET_A": "900001.KO", "ASSET_B": "900002.KO", "REF_X": "900003.KQ"}
KR_ANCHORS = {"ASSET_A": "900001", "ASSET_B": "900002", "REF_X": "900003"}
KR_IDS = {
    logical: mint_instrument("norgate_assetid", token) for logical, token in KR_ANCHORS.items()
}
# Collected before the fixture's knowledge time (DAYS[-1] 16:00 UTC).
COLLECTED = at("2026-04-01T09:00:00")
LATE = at("2026-05-01T00:00:00")
# REF_X's last close, before and after a correction collected at LATE.
ORIGINAL_WON = 10000.0
CORRECTED_WON = 99000.0


@pytest.fixture(autouse=True)
def compute_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))


@pytest.fixture
def installation(
    tmp_path_factory: pytest.TempPathFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Document, Document]:
    monkeypatch.setattr(time, "time_ns", lambda: CLOCK_NS)
    return copy_installation(tmp_path_factory, tmp_path)


def _pin(record: Document, start: str | None = None, end: str | None = None) -> Document:
    return {
        **{
            key: record[key]
            for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
        },
        "from": start,
        "to": end,
    }


def _priced(
    declaration: Document,
    pins: list[Document],
    mapping: dict[str, str],
    *,
    currency: str = "USD",
    excluded: list[str] | None = None,
) -> Document:
    """The observation declaration with its panel source swapped for a price binding."""
    priced = {key: value for key, value in declaration.items() if key != "observations"}
    priced["prices"] = {"pins": pins, "excluded_flags": excluded or []}
    priced["instrument_map"] = mapping
    priced["conventions"] = {**declaration["conventions"], "currency": currency}
    return priced


def _prepared(home: Path, declaration: Document) -> PreparedResearchRun:
    with open_workspace(home) as workspace:
        return prepare_research_run(
            workspace,
            parse_research_run_request(canonical_json_bytes(declaration)),
            budget=BUDGET,
        )


def _outcomes(home: Path, declaration: Document) -> Document:
    """The canonical execution prices the fixture registered, as one open pin."""
    with open_workspace(home) as workspace:
        record = publication.read_dataset(workspace, "outcomes", "1")
    return _priced(declaration, [_pin(record)], {name: name for name in VALUES})


def _register_kr(workspace: Workspace, link: str) -> dict[str, str]:
    """Register the three synthetic KR ETFs and snapshot their EODHD symbols."""
    document = {
        "schema": "aas-identity-registry-v1",
        "issuers": [],
        "instruments": [
            {
                "anchor_namespace": "norgate_assetid",
                "anchor_token": token,
                "issuer": None,
                "asset_type": "etf",
                "venue": "XKRX",
            }
            for token in sorted(KR_ANCHORS.values())
        ],
        "assertions": [
            {
                "instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": token},
                "provider": "eodhd",
                "namespace": "eodhd_symbol",
                "token": KR_SYMBOLS[logical],
                "valid_from_us": UNBOUNDED,
                "valid_to_us": None,
                "known_from_us": 1,
                "supersedes_assertion_id": None,
                "source_snapshot_id": "sl:" + link,
                "source_hash": content_sha256(KR_SYMBOLS[logical]),
            }
            for logical, token in sorted(KR_ANCHORS.items())
        ],
    }
    register_identities(workspace.state, parse_registry(document), apply=True)
    report = snapshot_identities(workspace.state, "kr", created_at_us=5, apply=True)
    return {"snapshot_id": str(report["snapshot_id"]), "content_hash": str(report["content_hash"])}


def _bars(retrieved: datetime, *, residue: float = 0.0) -> list[tuple[object, ...]]:
    """KRW bars at 1000 won per fixture unit; ``residue`` is provider float noise."""
    rows: list[tuple[object, ...]] = []
    for logical, values in VALUES.items():
        for day, value in zip(DAYS, values, strict=True):
            won = value * 1000 + residue
            rows.append(
                (KR_SYMBOLS[logical], day, won, won, won, won, won, 1000.0, "KRW", retrieved)
            )
    return rows


def _promote_kr(
    home: Path, rows: list[tuple[object, ...]], *, parent: str | None = None
) -> Document:
    """Promote ``rows`` into ``prices.kr.eodhd``; return the committed generation pin."""
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        source = add_source(workspace, rows, tag="kr-" + str(parent), linked=COLLECTED)
        if parent is None:
            identity = _register_kr(workspace, source["source_id"])
            publish_calendar(
                workspace,
                {
                    day: (
                        us(at(day.isoformat() + "T00:00:00")),
                        us(at(day.isoformat() + "T06:30:00")),
                    )
                    for day in DAYS
                },
            )
        else:
            snapshot_id, content_hash = workspace.state.execute(
                "SELECT snapshot_id, content_hash FROM identity_snapshots WHERE snapshot_id='kr'"
            ).fetchone()
            identity = {"snapshot_id": snapshot_id, "content_hash": content_hash}
        document = spec([source], identity, parent=parent)
        report = promote(workspace, document[0], document[1], apply=True)
        generation = str(cast("Document", report)["generation_id"])
        record = workspace.state.execute(
            "SELECT dataset_id, version, generation_id, chain_hash, manifest_hash "
            "FROM dataset_versions WHERE generation_id=?",
            (generation,),
        ).fetchone()
        workspace.state.commit()
        _ = workspace.market.execute("CHECKPOINT")
    return dict(
        zip(
            ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash"),
            record,
            strict=True,
        )
    )


def _kr(declaration: Document, record: Document) -> Document:
    return _priced(
        declaration,
        [_pin(record)],
        {KR_IDS[logical]: logical for logical in VALUES},
        currency="KRW",
    )


def test_the_price_route_matches_the_observation_route(
    installation: tuple[Path, Document, Document],
) -> None:
    """Same numbers through canonical pins and through observations: the same run."""
    home, _body, declaration = installation
    observed = _prepared(home, declaration)
    priced = _prepared(home, _outcomes(home, declaration))
    assert priced.inputs.dates == observed.inputs.dates
    assert priced.inputs.opens == observed.inputs.opens
    assert priced.inputs.closes == observed.inputs.closes
    assert priced.inputs.targets == observed.inputs.targets
    # The instruments are the store's classified ETFs, not observation series.
    assert dict(priced.inputs.instrument_types) == dict.fromkeys(VALUES, "ETF")
    result = run_document(priced.envelope.canonical_bytes, priced.envelope.envelope_sha256)
    assert cast("Document", result["result"])["nav"]


def test_the_sealed_preparation_carries_the_head_read_receipt(
    installation: tuple[Path, Document, Document],
) -> None:
    """The run's sealed document is the record of what the panels were read from."""
    home, _body, declaration = installation
    priced_declaration = _outcomes(home, declaration)
    prepared = _prepared(home, priced_declaration)
    sealed = json.loads(prepared.provenance)
    assert "observations" not in sealed
    block = cast("Document", sealed["prices"])
    receipt = cast("Document", block["head_read"])
    assert receipt["schema"] == "aas-head-read-v1"
    assert receipt["mode"] == "observed_snapshot_research"
    assert receipt["binding_hash"] == block["binding_hash"]
    assert block["head_read_sha256"] == content_sha256(receipt)
    query = cast("Document", receipt["query"])
    assert query["known_ceiling_us"] == sealed["conventions"]["knowledge_time_us"]
    assert query["ingestion_cutoff_us"] is None
    assert query["subjects"] == sorted(VALUES)
    assert query["price_roles"] == ["canonical"]
    # One bar per instrument and session from the history start to the period end
    # (DAYS[6]); the read is pushed down to the dates the run can use.
    assert (query["from"], query["to"]) == (DAYS[0].isoformat(), "2026-03-31")
    assert receipt["heads"] == len(VALUES) * (len(DAYS) - 1)
    assert receipt["time_rules"] == [[0, "outcomes", "source_column@1", "source_column@1"]]
    assert (
        sealed["resolved_calendar"]["observed_calendar_ref"]
        == canonical_json_bytes(
            {"schema": "aas-head-binding-v1", "binding_hash": block["binding_hash"]}
        ).decode()
    )
    # The request store files a price-pinned declaration under the same research schema.
    assert request_schema(priced_declaration) == "aas-research-run-v2"


def test_a_krw_run_over_a_promoted_chain_decides_like_the_usd_run(
    installation: tuple[Path, Document, Document],
) -> None:
    """KRW bars promoted with krw_tick@1 drive the same decisions at 1000x the prices."""
    home, _body, declaration = installation
    usd = _prepared(home, _outcomes(home, declaration))
    record = _promote_kr(home, _bars(COLLECTED, residue=0.4))
    krw = _prepared(home, _kr(declaration, record))
    assert krw.inputs.dates == usd.inputs.dates
    # krw_tick@1 rounded the provider residue back to whole won.
    assert [
        {name: value / 1000 for name, value in row.items()} for row in krw.inputs.closes
    ] == list(usd.inputs.closes)
    assert krw.inputs.targets == usd.inputs.targets
    sealed = json.loads(krw.provenance)
    receipt = cast("Document", sealed["prices"]["head_read"])
    assert receipt["time_rules"] == [
        [0, record["generation_id"], "local_day_end@1", "local_day_end@1"]
    ]
    assert sealed["conventions"]["currency"] == "KRW"


def test_a_revision_received_after_the_knowledge_time_is_not_read(
    installation: tuple[Path, Document, Document],
) -> None:
    """The declared knowledge time is the knowledge ceiling of the read."""
    home, _body, declaration = installation
    first = _promote_kr(home, _bars(COLLECTED))
    rows = _bars(LATE)
    corrected = [
        (*row[:2], *(CORRECTED_WON,) * 4, *row[6:])
        # DAYS[6] is the declared period's last session.
        if row[0] == KR_SYMBOLS["REF_X"] and row[1] == DAYS[6]
        else row
        for row in rows
    ]
    second = _promote_kr(home, corrected, parent=str(first["generation_id"]))
    before = _prepared(home, _kr(declaration, second))
    assert before.inputs.closes[-1]["REF_X"] == ORIGINAL_WON
    after_time = datetime(2026, 5, 2, tzinfo=UTC).isoformat()
    later = _kr(declaration, second)
    later["conventions"] = {**later["conventions"], "knowledge_time": after_time}
    after = _prepared(home, later)
    assert after.inputs.closes[-1]["REF_X"] == CORRECTED_WON


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"currency": "USD"}, "price currency is not the declared account currency"),
        ({"drop": "ASSET_B"}, "instrument_map names instruments no pinned price carries"),
    ],
)
def test_a_price_run_refuses_what_the_pins_do_not_carry(
    installation: tuple[Path, Document, Document], change: Document, message: str
) -> None:
    home, _body, declaration = installation
    record = _promote_kr(home, _bars(COLLECTED))
    priced = _kr(declaration, record)
    if "currency" in change:
        priced["conventions"] = {**priced["conventions"], "currency": change["currency"]}
    else:
        mapping = {KR_IDS[name]: name for name in VALUES} | {"ins-missing": change["drop"]}
        del mapping[KR_IDS[change["drop"]]]
        priced["instrument_map"] = mapping
    with pytest.raises(ValueError, match=message):
        _ = _prepared(home, priced)


def test_a_declaration_names_exactly_one_panel_source(
    installation: tuple[Path, Document, Document],
) -> None:
    home, _body, declaration = installation
    priced = _outcomes(home, declaration)
    both = priced | {"observations": declaration["observations"]}
    with pytest.raises(ResearchRunError, match="both observations and prices"):
        _ = parse_research_run_request(canonical_json_bytes(both))
    observation_keys = priced | {"instrument_map": declaration["instrument_map"]}
    with pytest.raises(ResearchRunError, match="must name an instrument id"):
        _ = parse_research_run_request(canonical_json_bytes(observation_keys))
    gap = cast("Document", priced["prices"])
    pin = cast("list[Document]", gap["pins"])[0]
    split = priced | {
        "prices": {
            "pins": [pin | {"to": "2026-01-01"}, pin | {"from": "2026-02-01"}],
            "excluded_flags": [],
        }
    }
    with pytest.raises(ResearchRunError, match="ordered and contiguous"):
        _ = parse_research_run_request(canonical_json_bytes(split))


def _rewritten(
    monkeypatch: pytest.MonkeyPatch, rewrite: Callable[[list[HeadRow]], list[HeadRow]]
) -> None:
    """Hand the price panel a read whose rows ``rewrite`` changed after the store read."""
    original = backtest_prepare.load_pinned_heads

    def load(*args: Any, **kwargs: Any) -> HeadRead:  # noqa: ANN401 -- passthrough
        read = original(*args, **kwargs)
        return replace(read, rows=tuple(rewrite(list(read.rows))))

    monkeypatch.setattr(backtest_prepare, "load_pinned_heads", load)


def _changed(rows: list[HeadRow], subject: str, day: date, **values: object) -> list[HeadRow]:
    return [
        replace(row, values={**row.values, **values})
        if (row.values["instrument_id"], row.values["session_date"]) == (subject, day)
        else row
        for row in rows
    ]


@pytest.mark.parametrize(
    "values",
    [{"value_state": "invalid"}, {"available_at_us": KNOWLEDGE_US + 1}],
    ids=["not-present", "available-after-the-ceiling"],
)
def test_a_skipped_bar_leaves_its_session_empty_and_is_not_filled(
    installation: tuple[Path, Document, Document],
    monkeypatch: pytest.MonkeyPatch,
    values: Document,
) -> None:
    """A bar the panel cannot admit is left out; its instrument does not take another value."""
    home, _body, declaration = installation
    priced = _outcomes(home, declaration)
    full = _prepared(home, priced)
    # The period's last session, where the full run reads every instrument.
    _rewritten(monkeypatch, lambda rows: _changed(rows, "REF_X", DAYS[6], **values))
    skipped = _prepared(home, priced)
    assert "REF_X" not in skipped.inputs.closes[-1]
    assert full.inputs.closes[-1]["REF_X"] == VALUES["REF_X"][6]


@pytest.mark.parametrize(
    ("rewrite", "message"),
    [
        (
            lambda rows: _changed(rows, "REF_X", DAYS[6], price_role="reference"),
            "reads only canonical unadjusted bars",
        ),
        (
            lambda rows: _changed(rows, "REF_X", DAYS[6], basis="total_return"),
            "reads only canonical unadjusted bars",
        ),
        (lambda rows: [*rows, rows[-1]], "price panel repeats one session"),
    ],
    ids=["reference-role", "adjusted-basis", "repeated-session"],
)
def test_a_price_panel_refuses_rows_it_cannot_read_as_one_bar_per_session(
    installation: tuple[Path, Document, Document],
    monkeypatch: pytest.MonkeyPatch,
    rewrite: Callable[[list[HeadRow]], list[HeadRow]],
    message: str,
) -> None:
    home, _body, declaration = installation
    priced = _outcomes(home, declaration)
    _rewritten(monkeypatch, rewrite)
    with pytest.raises(ValueError, match=message):
        _ = _prepared(home, priced)


def test_a_declaration_with_neither_panel_source_is_refused(
    installation: tuple[Path, Document, Document],
) -> None:
    _home, _body, declaration = installation
    neither = {key: value for key, value in declaration.items() if key != "observations"}
    with pytest.raises(ResearchRunError, match="observations"):
        _ = parse_research_run_request(canonical_json_bytes(neither))


@pytest.mark.parametrize("scope", ["sleeve", "composition"])
def test_a_priced_run_is_recorded_and_reproduces_from_its_declaration(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    scope: str,
) -> None:
    """`aas run research` records a price-pinned declaration and `run rerun` reproduces it."""
    # Requested by name so the imported fixture is reused and no parameter shadows it.
    composition = request.getfixturevalue("composed")
    home, base, offense, defense = cast("tuple[Path, Document, Document, Document]", composition)
    declaration = (
        _as_sleeve_run(base, offense) if scope == "sleeve" else _composition(base, offense, defense)
    )
    priced = _outcomes(home, declaration)
    _installed(home)
    path = _write(tmp_path, scope + ".json", priced)
    receipt = _execute(home, path, capsys)
    assert cast("Document", receipt["run"])["status"] == "SUCCESS"
    run_id = _run_id(receipt)
    shown = cast("Document", _show(home, run_id, capsys)["run"])
    assert shown["request_hash"] == receipt["request_hash"]

    rerun = _rerun(
        home, run_id, capsys, "--declaration", str(path), "--sha256", sha(path.read_bytes())
    )
    assert rerun["checked"] == ["preparation", "result"]
    assert rerun["reproduced"] is True
    preparation = cast("Document", cast("Document", rerun["checks"])["preparation"])
    assert preparation["matches"] == {"run_id": True, "envelope": True, "preparation": True}
