"""A declared research run's macro and FX grants: what they admit and what the run records.

A sleeve reading a macro series is fed the series its declaration grants, read at each
decision's cutoff under the granted binding. Prices in a currency other than the declared
account currency are converted with the fixings of a granted binding, and a grant may bring
its own market's price chain, so one run reads a KRW chain and a USD chain together. The
sealed preparation records every grant, its binding and every read made under it.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application import backtest_prepare
from aegis_alpha.application.backtest_prepare import PreparedResearchRun, prepare_research_run
from aegis_alpha.application.research_run import ResearchRunError, parse_research_run_request
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.bundle import EngineBundle
from aegis_alpha.engine.ensemble import EnsembleMembership
from aegis_alpha.engine.membership import MembershipRow, membership_hash
from aegis_alpha.engine.models import MacroSignalSpec
from aegis_alpha.engine.requirements import derive_execution_definition
from aegis_alpha.storage.backtest_requests import request_schema
from aegis_alpha.storage.market_inputs import GenerationPin
from aegis_alpha.storage.read_heads import HeadBinding, HeadPin
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import open_workspace
from tests.application.test_backtest_prepare import BUDGET, DAYS, micros
from tests.application.test_prepare_cli import sha
from tests.application.test_research_composition import (  # noqa: F401 -- shared fixture
    _as_sleeve_run,
    composed,
)
from tests.application.test_research_execution import CLOCK_NS, copy_installation
from tests.application.test_research_prices import (
    COLLECTED,
    KR_IDS,
    VALUES,
    _bars,
    _outcomes,
    _pin,
    _promote_kr,
)
from tests.application.test_run_research import _execute, _installed, _rerun, _run_id, _write
from tests.application.test_strict_head_inputs import (
    USD_KRW,
    _common,
    _identity,
    _krw_bars,
    price_rows,
    sealed,
)
from tests.engine.engine_support import bundle, contract, raw_bundle

if TYPE_CHECKING:
    from pathlib import Path

    from aegis_alpha.storage.workspace import Workspace

type Document = dict[str, Any]

# KRW per USD at each fixture session. The USD chain's ASSET_B converted at these rates is
# 1000 won per fixture unit only where the rate is 1000, so the constant fixing reproduces
# the all-KRW run and the varying one shows the conversion is applied session by session.
CONSTANT = (1000,) * len(DAYS)


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


def _prepared(home: Path, declaration: Document) -> PreparedResearchRun:
    with open_workspace(home) as workspace:
        return prepare_research_run(
            workspace,
            parse_research_run_request(canonical_json_bytes(declaration)),
            budget=BUDGET,
        )


def _fixings(
    workspace: Workspace,
    name: str,
    rates: dict[date, str],
    *,
    base: str = "USD",
    available: dict[date, int] | None = None,
) -> Document:
    """Publish ``BASE/KRW`` fixings, fixed and known at 16:00 UTC; return the binding block.

    A fixing ``available`` names a later instant for is recorded as known when it was
    fixed but published only at that instant.
    """
    late = available or {}
    rows = [
        _identity(
            {
                **_common(name, index, micros(day), ingested=late.get(day, micros(day))),
                "available_at_us": late.get(day, micros(day)),
                "base_currency": base,
                "quote_currency": "KRW",
                "fixing_at_us": micros(day),
                "rate": rate,
                "value_state": "present",
            },
            "fx_rates",
        )
        for index, (day, rate) in enumerate(sorted(rates.items()))
    ]
    pin = sealed(workspace, name, "fx_rates", rows)
    return {"pins": [{**pin, "from": None, "to": None}], "excluded_flags": []}


def _macro_strategy(workspace: Workspace, root: Path, declaration: Document) -> None:
    """The fixture strategy with one macro signal on the USD/KRW fixing: cash below 1."""
    value = replace(
        contract(), macro_signals=(MacroSignalSpec("USD/KRW", (0,), "EXACT", "LT", (1.0,)),)
    )
    document = json.loads(raw_bundle(value))
    document["bundle_version"] = "3"
    raw = canonical_json_bytes(document)
    path = root / "macro-strategy.json"
    path.write_bytes(raw)
    registered = register_strategy(
        workspace, path, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "3"
    )
    declaration["strategy"] = {
        **declaration["strategy"],
        "version": "3",
        "raw_sha256": registered["raw_sha256"],
        "contract_sha256": registered["contract_sha256"],
    }


def _macro_declaration(home: Path, root: Path, declaration: Document) -> Document:
    """The USD price-pinned declaration over a sleeve reading USD/KRW, with its grant."""
    priced = _outcomes(home, declaration)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        _macro_strategy(workspace, root, priced)
        binding = _fixings(
            workspace, "fx.usdkrw.macro", {date(2025, 12, 31): "0.5", date(2026, 1, 31): "2"}
        )
        assert workspace.strategies is not None
        workspace.strategies.commit()
        _ = workspace.market.execute("CHECKPOINT")
    priced["macro"] = [
        {"series_id": "USD/KRW", "unit": "KRW", "binding": {"domain": "fx_rates", **binding}}
    ]
    return priced


def test_a_granted_macro_series_feeds_its_sleeve_at_each_decision(
    installation: tuple[Path, Document, Document], tmp_path: Path
) -> None:
    """The macro grant replaces the refusal: the sleeve decides on the series it reads."""
    home, _body, declaration = installation
    granted = _macro_declaration(home, tmp_path, declaration)
    prepared = _prepared(home, granted)
    # 0.5 below 1 at the first decision sends it to cash; 2 at the second does not.
    assert [dict(item.signals["synthetic-choice"]) for item in prepared.decisions] == [
        {"USD/KRW": True},
        {"USD/KRW": False},
    ]
    assert dict(prepared.inputs.targets) == {DAYS[2]: {}, DAYS[4]: {"ASSET_B": 1.0}}
    sealed_document = json.loads(prepared.provenance)
    block = cast("Document", sealed_document["macro"])
    binding = HeadBinding(
        "fx_rates",
        tuple(
            HeadPin(GenerationPin(*(pin[key] for key in list(pin)[:5])), pin["from"], pin["to"])
            for pin in granted["macro"][0]["binding"]["pins"]
        ),
    )
    assert block["inputs"] == [
        {
            "series_id": "USD/KRW",
            "unit": "KRW",
            "binding_hash": binding.binding_hash,
            "binding": binding.document(),
        }
    ]
    reads = cast("list[Document]", block["head_reads"])
    assert [(item["purpose"], item["decision_date"]) for item in reads] == [
        ("admission", None),
        ("decision", DAYS[2].isoformat()),
        ("decision", DAYS[4].isoformat()),
    ]
    knowledge = sealed_document["conventions"]["knowledge_time_us"]
    assert reads[0]["receipt"]["query"]["known_ceiling_us"] == knowledge
    for item, slot in zip(reads[1:], prepared.slots, strict=True):
        # Each decision reads the series as known at its own cutoff.
        assert item["receipt"]["query"]["known_ceiling_us"] == min(slot.cutoff_us, knowledge)
        assert item["receipt"]["mode"] == "observed_snapshot_research"
        assert item["receipt_sha256"] == content_sha256(item["receipt"])
    assert request_schema(granted) == "aas-research-run-v2"


def test_a_macro_grant_names_exactly_the_series_the_sleeves_read(
    installation: tuple[Path, Document, Document], tmp_path: Path
) -> None:
    home, _body, declaration = installation
    granted = _macro_declaration(home, tmp_path, declaration)
    ungranted = {key: value for key, value in granted.items() if key != "macro"}
    with pytest.raises(ValueError, match=r"read macro series \[USD/KRW\]; .* grants \[\]"):
        _ = _prepared(home, ungranted)
    unread = _outcomes(home, declaration) | {"macro": granted["macro"]}
    with pytest.raises(ValueError, match=r"read macro series \[\]; .* grants \[USD/KRW\]"):
        _ = _prepared(home, unread)
    wrong_unit = json.loads(json.dumps(granted))
    wrong_unit["macro"][0]["unit"] = "USD"
    with pytest.raises(ValueError, match="quote currency"):
        _ = _prepared(home, wrong_unit)
    for value, message in (
        ([], "omitted, not empty"),
        ([granted["macro"][0], granted["macro"][0]], "unique and ordered"),
        ([{**granted["macro"][0], "series_id": "USDKRW"}], "BASE/QUOTE"),
        (
            [
                {
                    **granted["macro"][0],
                    "binding": {**granted["macro"][0]["binding"], "domain": "prices"},
                }
            ],
            "macro_observations or fx_rates",
        ),
    ):
        with pytest.raises(ResearchRunError, match=message):
            _ = parse_research_run_request(canonical_json_bytes(granted | {"macro": value}))


def _mixed(  # noqa: PLR0913 -- the two chains and the terms of their conversion
    home: Path,
    declaration: Document,
    rates: tuple[int, ...],
    *,
    available: dict[date, int] | None = None,
    signal_basis: str = "account_currency",
    max_age: int = 0,
) -> Document:
    """ASSET_A and REF_X from the KRW chain, ASSET_B from the USD chain, in a KRW account."""
    record = _promote_kr(home, _bars(COLLECTED))
    usd = cast("Document", _outcomes(home, declaration)["prices"])
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        fixings = _fixings(
            workspace,
            "fx.usdkrw.syn",
            {day: str(rate) for day, rate in zip(DAYS, rates, strict=True)},
            available=available,
        )
        _ = workspace.market.execute("CHECKPOINT")
    priced = _outcomes(home, declaration)
    priced["prices"] = {"pins": [_pin(record)], "excluded_flags": []}
    priced["instrument_map"] = {
        KR_IDS["ASSET_A"]: "ASSET_A",
        KR_IDS["REF_X"]: "REF_X",
        "ASSET_B": "ASSET_B",
    }
    priced["conventions"] = {**priced["conventions"], "currency": "KRW"}
    priced["fx_conversions"] = [
        {
            "currency": "USD",
            "series_id": "USD/KRW",
            "max_fixing_age_days": max_age,
            "signal_basis": signal_basis,
            "binding": fixings,
            "prices": usd,
        }
    ]
    return priced


def test_a_mixed_currency_run_converts_the_granted_chain_into_its_account(
    installation: tuple[Path, Document, Document],
) -> None:
    """A USD chain beside a KRW chain decides and marks like the all-KRW chain."""
    home, _body, declaration = installation
    mixed = _mixed(home, declaration, CONSTANT)
    prepared = _prepared(home, mixed)
    krw_only = {key: value for key, value in mixed.items() if key != "fx_conversions"}
    krw_only["instrument_map"] = {KR_IDS[name]: name for name in VALUES}
    baseline = _prepared(home, krw_only)
    assert prepared.inputs.dates == baseline.inputs.dates
    assert prepared.inputs.closes == baseline.inputs.closes
    assert prepared.inputs.opens == baseline.inputs.opens
    assert prepared.inputs.targets == baseline.inputs.targets
    sealed_document = json.loads(prepared.provenance)
    block = cast("Document", sealed_document["fx_conversions"])
    (conversion,) = block["conversions"]
    assert conversion["conversion"]["direction"] == "multiply"
    assert conversion["conversion"]["account_currency"] == "KRW"
    assert conversion["binding"] == {"role": "fx_conversion", "ordinal": 0}
    assert conversion["unconverted"] == []
    assert [row[0] for row in conversion["fixings"]] == [
        day.isoformat() for day in DAYS[: len(DAYS) - 1]
    ]
    reads = block["head_reads"]
    assert [item["purpose"] for item in reads] == ["prices", "panels"] + ["decision"] * len(
        prepared.slots
    )
    assert reads[0]["receipt"]["query"]["subjects"] == sorted(mixed["instrument_map"])
    # Each decision converts its signals with the fixings known at its own cutoff.
    for item, slot in zip(reads[2:], prepared.slots, strict=True):
        assert item["decision_date"] == slot.decision_date.isoformat()
        assert item["receipt"]["query"]["known_ceiling_us"] == slot.cutoff_us
        assert item["unconverted"] == []


def test_each_session_takes_its_own_fixing(
    installation: tuple[Path, Document, Document],
) -> None:
    home, _body, declaration = installation
    rates = (1000, 1100, 1200, 1300, 1400, 1300, 1200, 1100)
    prepared = _prepared(home, _mixed(home, declaration, rates))
    for day, close in zip(prepared.inputs.dates, prepared.inputs.closes, strict=True):
        index = DAYS.index(day)
        assert close["ASSET_B"] == VALUES["ASSET_B"][index] * rates[index]
        assert close["ASSET_A"] == VALUES["ASSET_A"][index] * 1000


def _signal_spy(monkeypatch: pytest.MonkeyPatch) -> dict[date, dict[date, float]]:
    """Record the ASSET_B signal closes each decision's sleeve is handed."""
    seen: dict[date, dict[date, float]] = {}
    original = backtest_prepare._replay_sleeve  # noqa: SLF001 -- the decision's own inputs

    def spy(sleeve: Any, points: Any, slot: Any, *args: Any) -> Any:  # noqa: ANN401
        seen[slot.decision_date] = {point.as_of: point.close for point in points["ASSET_B"]}
        return original(sleeve, points, slot, *args)

    monkeypatch.setattr(backtest_prepare, "_replay_sleeve", spy)
    return seen


RATES = (1000, 1100, 1200, 1300, 1400, 1300, 1200, 1100)


def test_a_decision_converts_its_signals_with_the_fixings_its_cutoff_knows(
    installation: tuple[Path, Document, Document], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fixing published only after a decision converts the marks, never that decision.

    The first decision's own session fixing is published days after it. That decision falls
    back to the fixing before it inside the granted age, the later decision reads the late
    fixing once it is known, and the marking panels convert with it throughout.
    """
    home, _body, declaration = installation
    seen = _signal_spy(monkeypatch)
    late = DAYS[2]
    mixed = _mixed(
        home,
        declaration,
        RATES,
        available={late: micros(DAYS[3])},
        max_age=40,
    )
    prepared = _prepared(home, mixed)
    first, second = (slot.decision_date for slot in prepared.slots)
    assert first == late
    index = DAYS.index(late)
    assert prepared.slots[0].cutoff_us < micros(DAYS[3]) <= prepared.slots[1].cutoff_us
    assert seen[first][late] == VALUES["ASSET_B"][index] * RATES[index - 1]
    assert seen[second][late] == VALUES["ASSET_B"][index] * RATES[index]
    for day, close in zip(prepared.inputs.dates, prepared.inputs.closes, strict=True):
        position = DAYS.index(day)
        assert close["ASSET_B"] == VALUES["ASSET_B"][position] * RATES[position]
    block = cast("Document", json.loads(prepared.provenance)["fx_conversions"])
    (conversion,) = block["conversions"]
    assert [late.isoformat(), late.isoformat(), float(RATES[index])] in conversion["fixings"]
    # With no age to fall back on, the first decision has no converted point for that
    # session, so its sleeve is short of the month the session would have supplied.
    exact = json.loads(json.dumps(mixed))
    exact["fx_conversions"][0]["max_fixing_age_days"] = 0
    with pytest.raises(ValueError, match="insufficient eligible buckets for ASSET_B"):
        _ = _prepared(home, exact)


def test_a_research_conversion_states_which_currency_its_signals_read(
    installation: tuple[Path, Document, Document], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Signals read converted closes, or the USD closes when the grant keeps them there."""
    home, _body, declaration = installation
    seen = _signal_spy(monkeypatch)
    mixed = _mixed(home, declaration, RATES, signal_basis="price_currency")
    native = _prepared(home, mixed)
    in_usd = {decision: dict(points) for decision, points in seen.items()}
    mixed["fx_conversions"][0]["signal_basis"] = "account_currency"
    account = _prepared(home, mixed)
    for decision, points in in_usd.items():
        assert points
        for day, close in points.items():
            index = DAYS.index(day)
            assert close == VALUES["ASSET_B"][index]
            assert seen[decision][day] == VALUES["ASSET_B"][index] * RATES[index]
    # Signals in USD read no fixing at a decision; the marks are converted either way.
    reads = json.loads(native.provenance)["fx_conversions"]["head_reads"]
    assert [item["purpose"] for item in reads] == ["prices", "panels"]
    assert native.inputs.opens == account.inputs.opens
    assert native.inputs.closes == account.inputs.closes


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda mixed: mixed.update(
                prices=mixed.pop("fx_conversions")[0]["prices"],
                instrument_map={name: name for name in VALUES},
            ),
            "no FX conversion grants it: USD",
        ),
        (
            lambda mixed: mixed["fx_conversions"][0].update(prices=mixed["prices"]),
            "carries a bar not in USD",
        ),
    ],
    ids=["ungranted-currency", "grant-chain-in-another-currency"],
)
def test_a_price_currency_is_read_only_under_its_grant(
    installation: tuple[Path, Document, Document],
    change: Any,  # noqa: ANN401 -- a mutating lambda
    message: str,
) -> None:
    home, _body, declaration = installation
    mixed = _mixed(home, declaration, CONSTANT)
    change(mixed)
    with pytest.raises(ValueError, match=message):
        _ = _prepared(home, mixed)


def test_fx_conversions_convert_canonical_price_pins_only(
    installation: tuple[Path, Document, Document],
) -> None:
    home, _body, declaration = installation
    mixed = _mixed(home, declaration, CONSTANT)
    observed = declaration | {"fx_conversions": mixed["fx_conversions"]}
    with pytest.raises(ResearchRunError, match="convert canonical price pins"):
        _ = parse_research_run_request(canonical_json_bytes(observed))
    for change, message in (
        ({"currency": "KRW", "series_id": "KRW/KRW"}, "other than the account"),
        ({"series_id": "EUR/KRW"}, "USD/KRW or KRW/USD"),
        ({"signal_basis": "native"}, "signal_basis"),
    ):
        changed = json.loads(json.dumps(mixed))
        changed["fx_conversions"][0].update(change)
        with pytest.raises(ResearchRunError, match=message):
            _ = parse_research_run_request(canonical_json_bytes(changed))
    with pytest.raises(ResearchRunError, match="omitted, not empty"):
        _ = parse_research_run_request(canonical_json_bytes(mixed | {"fx_conversions": []}))


def _sealed_mixed(home: Path, declaration: Document) -> Document:
    """ASSET_A and REF_X from a USD chain, ASSET_B from a KRW chain, in a USD account.

    Both chains are sealed imports, which a backup and ``aas db verify`` read in full, so the
    run can be recorded. ASSET_B's KRW bars are its USD numbers at the day's fixing.
    """
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        usd = sealed(
            workspace,
            "prices.syn.usd",
            "prices",
            [
                row
                for row in price_rows(split=False, name="usd-bar")
                if row["instrument_id"] != "ASSET_B"
            ],
        )
        krw = sealed(workspace, "prices.syn.krw", "prices", _krw_bars())
        fixings = _fixings(
            workspace,
            "fx.usdkrw.recorded",
            {day: str(rate) for day, rate in zip(DAYS, USD_KRW, strict=True)},
        )
        _ = workspace.market.execute("CHECKPOINT")
    priced = _outcomes(home, declaration)
    priced["prices"] = {"pins": [{**usd, "from": None, "to": None}], "excluded_flags": []}
    priced["fx_conversions"] = [
        {
            "currency": "KRW",
            "series_id": "USD/KRW",
            "max_fixing_age_days": 0,
            "signal_basis": "account_currency",
            "binding": fixings,
            "prices": {"pins": [{**krw, "from": None, "to": None}], "excluded_flags": []},
        }
    ]
    return priced


def test_a_granted_run_is_recorded_and_reproduces_from_its_declaration(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`aas run research` stores a declaration carrying grants and `run rerun` reproduces it."""
    # Requested by name so the imported fixture is reused and no parameter shadows it.
    composition = request.getfixturevalue("composed")
    home, base, offense, _defense = cast("tuple[Path, Document, Document, Document]", composition)
    declaration = _as_sleeve_run(base, offense)
    mixed = _sealed_mixed(home, declaration)
    prepared = _prepared(home, mixed)
    usd = _prepared(home, _outcomes(home, declaration))
    # The KRW chain converted into the USD account is the USD fixture's own numbers.
    assert prepared.inputs.closes == usd.inputs.closes
    assert prepared.inputs.opens == usd.inputs.opens
    assert prepared.inputs.targets == usd.inputs.targets
    _installed(home)
    path = _write(tmp_path, "mixed.json", mixed)
    receipt = _execute(home, path, capsys)
    assert cast("Document", receipt["run"])["status"] == "SUCCESS"
    rerun = _rerun(
        home,
        _run_id(receipt),
        capsys,
        "--declaration",
        str(path),
        "--sha256",
        sha(path.read_bytes()),
    )
    assert rerun["checked"] == ["preparation", "result"]
    assert rerun["reproduced"] is True


def test_a_composition_offense_switches_on_its_canary_not_a_macro_signal() -> None:
    """A granted macro series feeds a sleeve; it cannot become the composition's switch.

    A macro signal folds into the offense's master switch, so routing on it would send a
    decision to the defensive sleeve for a condition the record does not name.
    """
    plain = bundle(contract())
    macro = bundle(
        replace(
            contract(), macro_signals=(MacroSignalSpec("USD/KRW", (0,), "EXACT", "LT", (1.0,)),)
        )
    )
    rows = (MembershipRow("synthetic-choice", Decimal(1)),)
    membership = EnsembleMembership(rows, membership_hash(rows))

    def sleeve(role: str, value: EngineBundle) -> backtest_prepare._Sleeve:
        return backtest_prepare._Sleeve(  # noqa: SLF001 -- the composition rule itself
            role, value, derive_execution_definition(value), membership
        )

    backtest_prepare._require_composable(  # noqa: SLF001
        sleeve("offense", plain), sleeve("defense", macro)
    )
    with pytest.raises(ValueError, match="declares macro signals"):
        backtest_prepare._require_composable(  # noqa: SLF001
            sleeve("offense", macro), sleeve("defense", plain)
        )
