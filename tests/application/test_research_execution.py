"""A declared uncertified research run, from pinned observations to a stored run.

The strict path refuses this panel for reasons that stay refusals here: the transform
is not a price transform, the pin is not a price domain, and the derived reader turns
an observation contract away. Those cases are exercised at the bottom of this module
so a future change that loosens any of them fails here rather than quietly succeeding.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.backtest_prepare import (
    PreparedResearchRun,
    _require_fillable,
    prepare_research_run,
    research_source_identity,
)
from aegis_alpha.application.research_run import (
    PREPARED_SCHEMA,
    ResearchRunError,
    parse_research_run_request,
)
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.backtest_request import EnvelopeInputs, parse_prepare_request
from aegis_alpha.storage import publication
from aegis_alpha.storage.backup import backup, restore
from aegis_alpha.storage.market_inputs import (
    GenerationPin,
    admit_native_input,
    load_pinned_observations,
)
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.application.test_backtest_prepare import (
    BUDGET,
    DAYS,
    micros,
    price_rows,
    stored_request,
)
from tests.storage.test_research_inputs import (
    _hash_json,
    _register_domain,
    _source_row,
    _spec,
)

if TYPE_CHECKING:
    from pathlib import Path

type Document = dict[str, Any]

# The panel names its own series; only the declaration says which instrument each is.
SERIES = {"aas-obs-a": "ASSET_A", "aas-obs-b": "ASSET_B", "aas-obs-x": "REF_X"}
KNOWLEDGE_US = micros(DAYS[-1])
KNOWLEDGE_TIME = datetime.fromtimestamp(KNOWLEDGE_US / 1_000_000, UTC).isoformat()
NATURAL = (
    "contract_id",
    "contract_version",
    "input_bundle_hash",
    "instrument_id",
    "feature_at_us",
)
COMMON = frozenset(
    {
        "generation_id",
        "record_id",
        "revision_id",
        "supersedes_revision_id",
        "op",
        "available_at_us",
        "revision_known_at_us",
        "ingested_at_us",
        "source_snapshot_id",
        "source_row_hash",
    }
)


def _definition(source: Document, role: str) -> Document:
    return {
        "series_id": "SYNTHETIC-PANEL",
        "version": "1",
        "observation_role": role,
        "basis": "adjusted",
        "currency": "USD",
        "price_role": "reference",
        "adjustment": "total_return",
        "value_domain": "positive",
        "certified": False,
        "normalization": {"input_number": "ieee_float", "output": "ieee754_binary64"},
        "observed_source": source,
        "calendar_ref": {"id": "synthetic-calendar", "version": "1", "sha256": "e" * 64},
    }


def _panel(
    workspace: Workspace,
    root: Path,
    name: str,
    role: str,
    options: Document | None = None,
) -> Document:
    """Register one observation generation carrying the fixture's own numbers."""
    settings: Document = {"changes": {}, "known": None, **(options or {})}
    retained = _spec(
        workspace,
        root / (name + "-panel.sqlite3"),
        [{**_source_row(), "record_id": "panel-0", "revision_id": "pr-0"}],
    )
    definition = _definition(json.loads(retained.read_bytes())["source"], role)
    definition.update(cast("Document", settings["changes"]))
    observed = {
        (str(row["instrument_id"]), str(row["session_date"])): float(str(row[role]))
        for row in price_rows(signal=True)
    }
    rows = []
    for series, instrument in sorted(SERIES.items()):
        for day in DAYS:
            row = {key: item for key, item in _source_row().items() if key in COMMON}
            row.update(
                contract_id=definition["series_id"] + "/" + role,
                contract_version=definition["version"],
                contract_hash=_hash_json(definition),
                input_bundle_hash=_hash_json(
                    [definition["observed_source"], definition["calendar_ref"]]
                ),
                instrument_id=series,
                feature_at_us=micros(day),
                value=observed[instrument, day.isoformat()],
                value_state="present",
                # The retained panel carries no knowledge times, so none is invented.
                available_at_us=settings["known"],
                revision_known_at_us=settings["known"],
                ingested_at_us=KNOWLEDGE_US,
            )
            row["record_id"] = _hash_json(
                ["aas-record-v1", "feature_values", [[key, row[key]] for key in NATURAL]]
            )
            row["revision_id"] = name + "-" + str(row["record_id"])
            rows.append(row)
    spec = _spec(workspace, root / (name + ".sqlite3"), rows)
    document = json.loads(spec.read_bytes())
    for key in ("price", "calendar", "decimal_conversion"):
        del document[key]
    document.update(
        schema_version="aas-observation-transform-v1",
        observation=definition,
        instruments=[
            {"instrument_id": series, "asset_type": "research_observation", "venue": "SYN"}
            for series in sorted(SERIES)
        ],
        dataset={
            "dataset_id": name,
            "version": "1",
            "generation_id": name,
            "operation_id": "op-" + name,
            "parent_id": None,
        },
    )
    _ = spec.write_text(json.dumps(document, indent=2))
    _ = _register_domain(workspace, spec, "observation")
    record = publication.read_dataset(workspace, name, "1")
    return {
        key: record[key]
        for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
    }


def _pin(reference: Document) -> GenerationPin:
    return GenerationPin(
        *(
            str(reference[key])
            for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
        )
    )


def _reference(body: Document, role: str) -> Document:
    """The fixture already registered these; read the pin back rather than rebuild it."""
    binding = next(item for item in body["bindings"] if item["role"] == role)
    return cast(
        "Document", next(ref for ref in body["refs"] if ref["ref_id"] == binding["ref_id"])["pin"]
    )


def _declaration(body: Document, close: Document, opening: Document) -> Document:
    strategy = body["strategy"]
    return {
        "schema_version": "aas-research-run-v2",
        "execution_mode": "research-uncertified",
        "strategy": {
            key: strategy[key]
            for key in (
                "strategy_store_id",
                "strategy_id",
                "version",
                "raw_sha256",
                "contract_sha256",
            )
        },
        "observations": [
            {
                **{
                    key: pin[key]
                    for key in (
                        "dataset_id",
                        "version",
                        "generation_id",
                        "chain_hash",
                        "manifest_hash",
                    )
                },
                "observation_role": role,
            }
            for pin, role in ((opening, "open"), (close, "close"))
        ],
        "calendar": {
            "calendar_id": "synthetic-research-calendar",
            "basis": "observed-sessions-date-only",
        },
        "membership": _reference(body, "membership"),
        "period": body["period"],
        "history": body["history"],
        "execution": {"cost": 0.0003, "initial_cash": 10000.0},
        "instrument_map": dict(SERIES),
        "conventions": {
            "knowledge_time": KNOWLEDGE_TIME,
            "calendar": "the registered synthetic session generation",
            "cost": "proportional on both sides at the declared rate",
            "capital": "the declared initial cash, with no cashflow",
            "currency": "USD",
        },
        "semantics": {
            "data_basis": "M",
            "abs_compare": "default-sign",
            "defensive_rule": "synthetic fixture rule; no source system is described",
            "expand": "extended-history-not-used",
            "expand_source": None,
            "rebalance_timing": "previous-month result applied at the following month start",
            "fill_price": "fill-next-session-open-v1",
            "tie_rule": "momentum-tie-canonical-id-asc-v1",
        },
        "unsettled": [],
        "uncertainty": [
            (
                "synthetic panel over a synthetic calendar; no parity with any source "
                "system is claimed and none could be"
            )
        ],
    }


@pytest.fixture(autouse=True)
def compute_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))


@pytest.fixture
def installation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Document, Document]:
    """One installation carrying both the executable fixture and the observation panels."""
    monkeypatch.setattr(time, "time_ns", lambda: micros(date(2026, 6, 1)) * 1000)
    home = tmp_path / "home"
    _ = initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        close = _panel(workspace, tmp_path, "obs-close", "close")
        opening = _panel(workspace, tmp_path, "obs-open", "open")
        workspace.state.commit()
        assert workspace.strategies is not None
        workspace.strategies.commit()
        _ = workspace.market.execute("CHECKPOINT")
    return home, body, _declaration(body, close, opening)


def _prepared(home: Path, declaration: Document) -> PreparedResearchRun:
    with open_workspace(home) as workspace:
        return prepare_research_run(
            workspace,
            parse_research_run_request(canonical_json_bytes(declaration)),
            budget=BUDGET,
        )


def _refused(home: Path, declaration: Document, message: str) -> None:
    with pytest.raises((ValueError, ResearchRunError), match=message):
        _ = _prepared(home, declaration)


def test_a_declared_run_evaluates_the_strategy_over_the_observation_panel(
    installation: tuple[Path, Document, Document],
) -> None:
    """The panel the strict path refuses still produces decisions under a declaration."""
    home, _body, declaration = installation
    prepared = _prepared(home, declaration)
    assert prepared.certified is False
    assert prepared.decisions
    # Every decision names instruments, never the aas-obs- series they were read from.
    assert all(set(weights) <= set(SERIES.values()) for weights in prepared.inputs.targets.values())
    assert not set(prepared.inputs.targets) & set(SERIES)
    sealed = json.loads(prepared.provenance)
    assert sealed["schema"] == PREPARED_SCHEMA
    assert sealed["execution_mode"] == "research-uncertified"
    assert sealed["certified"] is False
    assert sealed["executable_prices"] is False
    assert sealed["point_in_time_certified"] is False
    assert sealed["semantics"]["source_parity"] == "unknown"
    assert sealed["unsettled"] == []
    assert sealed["conventions"]["knowledge_time_us"] == KNOWLEDGE_US
    assert sealed["envelope_sha256"] == prepared.envelope.envelope_sha256


def test_the_accounting_runs_and_two_preparations_agree_exactly(
    installation: tuple[Path, Document, Document],
) -> None:
    """Same declaration, same envelope bytes, same result: the calculation is repeatable."""
    home, _body, declaration = installation
    first, second = _prepared(home, declaration), _prepared(home, declaration)
    assert first.envelope.canonical_bytes == second.envelope.canonical_bytes
    assert first.provenance == second.provenance
    result = run_document(first.envelope.canonical_bytes, first.envelope.envelope_sha256)
    again = run_document(second.envelope.canonical_bytes, second.envelope.envelope_sha256)
    assert canonical_json_bytes(result) == canonical_json_bytes(again)
    assert result["observed_prices_verified"] is False
    assert result["point_in_time_verified"] is False
    assert result["live_orders"] is False
    assert cast("Document", result["result"])["nav"]


def test_an_unmapped_observation_series_is_refused(
    installation: tuple[Path, Document, Document],
) -> None:
    """A series with no mapping is a refusal, never an asset silently left out."""
    home, _body, declaration = installation
    mapping = dict(SERIES)
    del mapping["aas-obs-b"]
    _refused(home, declaration | {"instrument_map": mapping}, "no declared instrument mapping")


def test_a_declaration_missing_one_panel_role_is_refused(
    installation: tuple[Path, Document, Document],
) -> None:
    """Signals read closes and fills read opens; one panel cannot stand in for both."""
    home, _body, declaration = installation
    single = [
        pin
        for pin in cast("list[Document]", declaration["observations"])
        if pin["observation_role"] == "close"
    ]
    _refused(home, declaration | {"observations": single}, "one open and one close panel")


def test_a_declared_role_that_contradicts_the_contract_is_refused(
    installation: tuple[Path, Document, Document],
) -> None:
    """The stored observation contract owns the role; the declaration cannot rename it."""
    home, _body, declaration = installation
    swapped = [
        {**pin, "observation_role": "close" if pin["observation_role"] == "open" else "open"}
        for pin in cast("list[Document]", declaration["observations"])
    ]
    _refused(home, declaration | {"observations": swapped}, "role disagrees with the declared pin")


def test_a_currency_the_panel_does_not_carry_is_refused(
    installation: tuple[Path, Document, Document],
) -> None:
    home, _body, declaration = installation
    conventions = cast("Document", declaration["conventions"]) | {"currency": "KRW"}
    _refused(home, declaration | {"conventions": conventions}, "not the declared account currency")


def test_the_strict_price_route_still_refuses_the_observation_generation(
    installation: tuple[Path, Document, Document],
) -> None:
    """The refusal this path was built beside, not through. It stays a refusal.

    The price route turns the pin away on domain before it ever reaches the transform
    schema check, so a published observation cannot be read as a price by either gate.
    """
    home, _body, declaration = installation
    pin = cast("list[Document]", declaration["observations"])[0]
    with (
        open_workspace(home) as workspace,
        pytest.raises(ValueError, match="pin has incompatible dataset/domain"),
    ):
        _ = admit_native_input(
            workspace, _pin(pin), expected_schema="aas-price-transform-v1", budget=BUDGET
        )


def test_strict_point_in_time_still_selects_nothing_from_the_panel(
    installation: tuple[Path, Document, Document],
) -> None:
    """The declared ceiling changes nothing about what strict PIT will admit: nothing."""
    home, _body, declaration = installation
    pin = cast("list[Document]", declaration["observations"])[0]
    with open_workspace(home) as workspace:
        series = load_pinned_observations(workspace, _pin(pin), budget=BUDGET)
        assert series.project_as_of(KNOWLEDGE_US, mode="strict_pit").rows == ()
        assert series.project_as_of(KNOWLEDGE_US, mode="observed_snapshot_research").rows


def test_the_executable_request_refuses_a_reference_series_as_an_execution_input(
    installation: tuple[Path, Document, Document],
) -> None:
    """Why a declared run needs its own document rather than an executable request.

    An aas-backtest-request-v1 holds execution prices to canonical unadjusted data, so
    a reference observation can never be named as one. This is the boundary the declared
    path works beside; if it ever stops refusing, this test is where that shows up.
    """
    home, body, _declared = installation
    _ = home
    request = cast("Document", json.loads(json.dumps(body)))
    selection = next(
        item
        for item in cast("list[Document]", request["price_inputs"])
        if cast("Document", item["binding"])["role"] == "execution_prices"
    )
    selection.update(price_role="reference", basis="total_return")
    with pytest.raises(ValueError, match="execution requires canonical unadjusted prices"):
        _ = parse_prepare_request(canonical_json_bytes(request))


def test_the_run_identity_is_the_declaration_and_what_it_produced(
    installation: tuple[Path, Document, Document],
) -> None:
    """Two runs of one declaration are one run; a changed convention is a different one."""
    home, _body, declaration = installation
    first, second = _prepared(home, declaration), _prepared(home, declaration)
    assert first.run_id == second.run_id
    assert first.run_id.startswith("research-")
    dearer = cast("Document", declaration["execution"]) | {"cost": 0.002}
    assert _prepared(home, declaration | {"execution": dearer}).run_id != first.run_id


def test_a_restored_installation_reproduces_the_same_run(
    installation: tuple[Path, Document, Document], tmp_path: Path
) -> None:
    """Backup and restore carry everything the run is: it comes back byte for byte.

    There is no stored run record to read back, because the run store admits only
    executable runs (see 010-run-store-blocker in the evidence root). What a restored
    installation does carry is every pinned input the declaration names, so the same
    declaration reproduces the same identity, the same envelope and the same accounting.
    """
    home, _body, declaration = installation
    original = _prepared(home, declaration)
    archive = tmp_path / "backup"
    _ = backup(home, archive, budget=BUDGET)
    restored = tmp_path / "restored"
    _ = restore(archive, restored, budget=BUDGET)
    again = _prepared(restored, declaration)
    assert again.run_id == original.run_id
    assert again.envelope.canonical_bytes == original.envelope.canonical_bytes
    assert again.provenance == original.provenance
    assert canonical_json_bytes(
        run_document(again.envelope.canonical_bytes, again.envelope.envelope_sha256)
    ) == canonical_json_bytes(
        run_document(original.envelope.canonical_bytes, original.envelope.envelope_sha256)
    )


def test_a_daily_basis_declaration_is_refused_rather_than_run_monthly(
    installation: tuple[Path, Document, Document],
) -> None:
    """The engine evaluates at month end, so a daily declaration cannot be honoured.

    Accepting it would seal a document describing a daily calculation over a monthly
    schedule, which is worse than refusing: the record would be wrong rather than absent.
    """
    home, _body, declaration = installation
    semantics = cast("Document", declaration["semantics"]) | {
        "data_basis": "D",
        "fill_price": "decision-close",
    }
    _refused(
        home,
        declaration | {"semantics": semantics, "unsettled": ["fill_price"]},
        "a D basis cannot be honoured",
    )


def test_the_sealed_document_names_the_code_that_decided_the_run(
    installation: tuple[Path, Document, Document],
) -> None:
    """The engine identity covers engine/ only, so the preparation names its own source."""
    home, _body, declaration = installation
    sealed = json.loads(_prepared(home, declaration).provenance)
    assert sealed["preparation_source_sha256"] == research_source_identity()


def test_the_sealed_document_records_the_calendar_the_run_actually_used(
    installation: tuple[Path, Document, Document],
) -> None:
    """The declared calendar is prose, so the identity that ran is recorded beside it."""
    home, _body, declaration = installation
    sealed = json.loads(_prepared(home, declaration).provenance)
    resolved = cast("Document", sealed["resolved_calendar"])
    assert resolved["calendar_id"] == "synthetic-research-calendar"
    assert resolved["basis"] == "observed-sessions-date-only"
    # The session count is the panel's own, not a calendar anyone published.
    assert resolved["sessions"] == len(cast("Any", _prepared(home, declaration)).inputs.dates)
    assert (
        sealed["conventions"]["calendar"]
        == cast("Document", declaration["conventions"])["calendar"]
    )


def test_panels_that_disagree_on_adjustment_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Signals from one basis and fills from another mark one account in a mixture."""
    monkeypatch.setattr(time, "time_ns", lambda: micros(date(2026, 6, 1)) * 1000)
    home = tmp_path / "home"
    _ = initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        close = _panel(workspace, tmp_path, "obs-close", "close")
        opening = _panel(
            workspace, tmp_path, "obs-open", "open", {"changes": {"adjustment": "capital"}}
        )
        workspace.state.commit()
        assert workspace.strategies is not None
        workspace.strategies.commit()
        _ = workspace.market.execute("CHECKPOINT")
    _refused(home, _declaration(body, close, opening), "disagree on basis, adjustment or calendar")


def test_a_row_the_panel_calls_unavailable_at_the_ceiling_is_not_admitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The declaration claims every row precedes its instant; a later row is not one."""
    monkeypatch.setattr(time, "time_ns", lambda: micros(date(2026, 6, 1)) * 1000)
    home = tmp_path / "home"
    _ = initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, tmp_path)
        close = _panel(workspace, tmp_path, "obs-close", "close", {"known": KNOWLEDGE_US})
        opening = _panel(workspace, tmp_path, "obs-open", "open", {"known": KNOWLEDGE_US})
        workspace.state.commit()
        assert workspace.strategies is not None
        workspace.strategies.commit()
        _ = workspace.market.execute("CHECKPOINT")
    # The declaration names an instant one microsecond before every row became known, so
    # the panel it claims to have had is empty and the schedule has nothing to build on.
    declaration = _declaration(body, close, opening)
    conventions = cast("Document", declaration["conventions"]) | {
        "knowledge_time": datetime.fromtimestamp((KNOWLEDGE_US - 1) / 1_000_000, UTC).isoformat()
    }
    _refused(home, declaration | {"conventions": conventions}, "fewer than two observed sessions")


def test_the_envelope_names_the_declared_mode_and_observation_instruments(
    installation: tuple[Path, Document, Document],
) -> None:
    """The panel is reference data by the store's own classification, and says so.

    Nothing is relabelled as an ETF to make the accounting accept it: the type travels
    with the series the values were read from, and the mode is the one that admits it.
    """
    home, _body, declaration = installation
    envelope = json.loads(_prepared(home, declaration).envelope.canonical_bytes)
    assert envelope["research_mode"] == "declared_uncertified_research"
    assert set(cast("Document", envelope["instrument_types"]).values()) == {"OBSERVATION"}
    weighted = {
        symbol
        for weights in cast("Document", envelope["targets"]).values()
        for symbol, weight in cast("Document", weights).items()
        if weight > 0
    }
    assert weighted
    result = run_document(
        _prepared(home, declaration).envelope.canonical_bytes,
        _prepared(home, declaration).envelope.envelope_sha256,
    )
    assert result["research_mode"] == "declared_uncertified_research"
    assert result["observed_prices_verified"] is False
    assert result["point_in_time_verified"] is False
    assert result["live_orders"] is False


@pytest.mark.parametrize("mode", ["observed_etf_research", "synthetic"])
def test_an_observation_weight_is_still_refused_in_the_established_modes(
    installation: tuple[Path, Document, Document], mode: str
) -> None:
    """The guard the declared mode relaxes stays exactly where it was for the others.

    Two layers hold here and this pins the outer one: an established mode does not admit
    the OBSERVATION type at all, so the same envelope under a different label is refused
    before its weights are read.
    """
    home, _body, declaration = installation
    envelope = json.loads(_prepared(home, declaration).envelope.canonical_bytes)
    envelope["research_mode"] = mode
    raw = canonical_json_bytes(envelope)
    with pytest.raises(ValueError, match="unsupported instrument type"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


@pytest.mark.parametrize("mode", ["observed_etf_research", "synthetic"])
def test_a_nontradeable_type_still_cannot_carry_weight_in_the_established_modes(
    installation: tuple[Path, Document, Document], mode: str
) -> None:
    """The inner guard too: an admitted but untradeable type still refuses a weight.

    INDEX passes the type gate in both established modes, so this is the check that
    would have been weakened if the declared mode had been added carelessly.
    """
    home, _body, declaration = installation
    envelope = json.loads(_prepared(home, declaration).envelope.canonical_bytes)
    envelope["research_mode"] = mode
    envelope["instrument_types"] = dict.fromkeys(
        cast("Document", envelope["instrument_types"]), "INDEX"
    )
    raw = canonical_json_bytes(envelope)
    with pytest.raises(ValueError, match="require an explicit ETF instrument type"):
        run_document(raw, hashlib.sha256(raw).hexdigest())


def _inputs(opens: list[Document], closes: list[Document], targets: Document) -> EnvelopeInputs:
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)]
    return EnvelopeInputs(
        tuple(days),
        tuple(opens),
        tuple(closes),
        {date.fromisoformat(day): weights for day, weights in targets.items()},
        {"AAA": "OBSERVATION"},
        (),
    )


def test_a_held_position_without_a_marking_close_is_refused_by_name() -> None:
    """A position stays on the book after the session that bought it.

    The replay marks it at every following close, so a panel that stops observing it
    fails on arithmetic deep inside the accounting unless admission walks the same
    holdings. This is that walk.
    """
    inputs = _inputs(
        [{"AAA": 10.0}, {"AAA": 10.0}, {"AAA": 10.0}],
        [{"AAA": 10.0}, {"AAA": 10.0}, {}],
        {"2026-01-02": {"AAA": 1.0}},
    )
    with pytest.raises(ValueError, match="close panel has no usable observation on 2026-01-06"):
        _require_fillable(inputs)


def test_a_liquidation_without_an_open_is_refused_by_name() -> None:
    """Selling out of a position needs the open it is sold at, not only the buy leg."""
    inputs = _inputs(
        [{"AAA": 10.0}, {"AAA": 10.0}, {}],
        [{"AAA": 10.0}, {"AAA": 10.0}, {"AAA": 10.0}],
        {"2026-01-02": {"AAA": 1.0}, "2026-01-05": {"AAA": 0.0}},
    )
    with pytest.raises(ValueError, match="open panel has no usable observation on 2026-01-06"):
        _require_fillable(inputs)


@pytest.mark.parametrize("price", [0.0, -1.0, float("nan")])
def test_a_price_the_accounting_cannot_use_is_refused_like_an_absent_one(price: float) -> None:
    """Present is not usable: the replay requires positive finite observed prices."""
    inputs = _inputs(
        [{"AAA": 10.0}, {"AAA": 10.0}, {"AAA": 10.0}],
        [{"AAA": 10.0}, {"AAA": price}, {"AAA": 10.0}],
        {"2026-01-02": {"AAA": 1.0}},
    )
    with pytest.raises(ValueError, match="close panel has no usable observation on 2026-01-05"):
        _require_fillable(inputs)
