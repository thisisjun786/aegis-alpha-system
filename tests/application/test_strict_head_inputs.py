"""Strict preparation over head bindings: promoted prices, actions, calendars, macro and FX.

The fixture's installation already holds the native generations ``test_backtest_prepare``
prepares from. This module publishes the same synthetic numbers as rule-timed generations
(each cataloged with a retained ``aas-promotion-v1`` time-rule declaration, which is the
provenance ``read_heads`` reads) and binds them through ``heads`` references. ASSET_A carries
a 2:1 split on DAYS[3], so its unadjusted bars before that session are twice the fixture's
signal numbers and the split-adjusted series the preparation derives is the fixture's own.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final, cast

import pytest

from aegis_alpha.application import backtest_prepare as preparation
from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.application.run_backtest import RunBacktestRequest, run_backtest
from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.engine.backtest_request import parse_prepare_request
from aegis_alpha.engine.models import MacroSignalSpec
from aegis_alpha.storage import market, publication
from aegis_alpha.storage.import_document import parse_import
from aegis_alpha.storage.input_pins import read_head_binding
from aegis_alpha.storage.market_inputs import (
    GenerationPin,
    load_pinned_revisions,
    verify_head_binding,
)
from aegis_alpha.storage.market_schema import NATURAL_KEYS
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.read_heads import HeadBinding, HeadPin, HeadQuery, head_binding
from aegis_alpha.storage.runs import read_run
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.application.test_backtest_prepare import (
    BUDGET,
    DAYS,
    Document,
    copy_request,
    micros,
    prepare,
    second_recipe,
    session_rows,
    stored_request,
    target_membership_interval,
)
from tests.application.test_storage_cli import run_cli
from tests.engine.engine_support import contract, raw_bundle

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

LAG: Final = "session_close_plus_lag@1"
EXDATE: Final = "exdate_open@1"
DECLARED: Final = "declared_session_end@1"
DAY_END: Final = "local_day_end@1"
ABSENT: Final = "absent_actions_as_none@1"
VALUES: Final = {
    "ASSET_A": (10, 12, 15, 15, 12, 12, 12, 12),
    "ASSET_B": (10, 10, 11, 11, 20, 20, 20, 20),
    "REF_X": (10, 10, 10, 10, 10, 10, 10, 10),
}
SPLIT: Final = DAYS[3]
EXPECTED: Final = {DAYS[2]: {"ASSET_A": 1.0}, DAYS[4]: {"ASSET_B": 1.0}}


@pytest.fixture(autouse=True)
def publication_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    # The native fixture's ingestion instant, never elapsed runtime.
    monkeypatch.setattr(time, "time_ns", lambda: micros(date(2026, 6, 1)) * 1000)


@pytest.fixture(scope="module")
def stored_template(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, bytes]:
    """The native preparation fixture of ``test_backtest_prepare``, built once per module."""
    root = tmp_path_factory.mktemp("strict-heads-template")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(time, "time_ns", lambda: micros(date(2026, 6, 1)) * 1000)
        _ = initialize(root / "home")
        with open_workspace(root / "home", writable=True, strategy_write=True) as workspace:
            body = stored_request(workspace, root)
            workspace.state.commit()
            assert workspace.strategies is not None
            workspace.strategies.commit()
            _ = workspace.market.execute("CHECKPOINT")
    return root, canonical_json_bytes(body)


@pytest.fixture(autouse=True)
def compute_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("AAS_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))


def _identity(row: Document, domain: str) -> Document:
    row["record_id"] = market.record_identity(domain, [row[key] for key in NATURAL_KEYS[domain]])
    return row


def _common(name: str, index: int, known: int | None, *, ingested: int) -> Document:
    return {
        "revision_id": f"{name}-{index}",
        "supersedes_revision_id": None,
        "op": "ASSERT",
        "available_at_us": known,
        "revision_known_at_us": known,
        "ingested_at_us": ingested,
        "source_snapshot_id": "sl:synthetic-" + name,
        "source_row_hash": hashlib.sha256(f"{name}-{index}".encode()).hexdigest(),
    }


def publish(  # noqa: PLR0913 -- one rule-timed generation and how it is named
    workspace: Workspace,
    dataset: str,
    domain: str,
    rows: list[Document],
    rules: tuple[str, str],
    *,
    sequence: int = 1,
) -> Document:
    """Publish ``rows`` and catalog them with a retained time-rule declaration; return the pin.

    The catalog's transform is an ``aas-promotion-v1`` document naming the rules of the two
    time columns, which is all a head read takes from a promotion's spec.
    """
    spec = canonical_json_bytes(
        {
            "schema_version": "aas-promotion-v1",
            "time_rules": {
                column: {"rule": rule, "basis": "record", "input": None, "args": {}}
                for column, rule in zip(
                    ("available_at_us", "revision_known_at_us"), rules, strict=True
                )
            },
        }
    )
    _, transform, _ = put_raw(workspace.paths.raw, spec)
    generation = f"{dataset}-g{sequence}"
    parent = None if sequence == 1 else f"{dataset}-g{sequence - 1}"
    request = hashlib.sha256(generation.encode()).hexdigest()
    marker = market.publish_generation(
        workspace.market,
        dataset_id=dataset,
        version=str(sequence),
        generation_id=generation,
        operation_id="op-" + generation,
        request_hash=request,
        parent_id=parent,
        domain=domain,
        rows=rows,
    )
    workspace.state.execute(
        "INSERT OR IGNORE INTO datasets VALUES (?, ?, 'aas-market-rowset-v1', 'promotion')",
        (dataset, domain),
    )
    workspace.state.execute(
        "INSERT INTO dataset_versions(dataset_id, version, generation_id, parent_generation_id, "
        "sequence, chain_hash, manifest_hash, record_schema, normalizer_version, transform_hash, "
        "identity_snapshot_hash, authority_policy_hash, row_count, coverage, status) "
        "VALUES (?,?,?,?,?,?,?,'aas-market-rowset-v1','synthetic@1',?,NULL,NULL,?,'all',"
        "'committed')",
        (
            dataset,
            str(sequence),
            generation,
            parent,
            sequence,
            marker["chain_hash"],
            request,
            transform,
            marker["row_count"],
        ),
    )
    workspace.state.commit()
    return {
        "dataset_id": dataset,
        "version": str(sequence),
        "generation_id": generation,
        "chain_hash": str(marker["chain_hash"]),
        "manifest_hash": request,
    }


def sealed(workspace: Workspace, dataset: str, domain: str, rows: list[Document]) -> Document:
    """Publish ``rows`` through a sealed ``aas-market-import-v1`` document; return the pin.

    Such a generation carries recorded times (``source_column@1``), passes ``aas db verify``
    and needs no grant.
    """
    kept = {"revision_id", "supersedes_revision_id", "op", "available_at_us"}
    kept |= {"revision_known_at_us", "ingested_at_us"}
    kept |= {name for name, _ in market.DOMAINS[domain]}
    body = [
        {
            key: (
                str(value)
                if isinstance(value, Decimal)
                else value.isoformat()
                if isinstance(value, date)
                else value
            )
            for key, value in row.items()
            if key in kept
        }
        for row in rows
    ]
    raw = canonical_json_bytes(
        {
            "schema_version": "aas-market-import-v1",
            "dataset_id": dataset,
            "version": "1",
            "generation_id": dataset + "-g1",
            "operation_id": "op-" + dataset,
            "parent_id": None,
            "domain": domain,
            "provider": "synthetic",
            "publication_at_us": None,
            "normalizer_version": "synthetic-v1",
            "transform_sha256": hashlib.sha256(dataset.encode()).hexdigest(),
            "instruments": [],
            "rows": body,
        }
    )
    publication.publish_document(workspace, parse_import(raw))
    record = publication.read_dataset(workspace, dataset, "1")
    return {
        key: record[key]
        for key in ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")
    }


def price_rows(*, split: bool = True, scale: int = 1, name: str = "bar") -> list[Document]:
    """Canonical unadjusted bars, known at their own close; ASSET_A doubled before the split."""
    rows = []
    for symbol, values in VALUES.items():
        for index, (day, value) in enumerate(zip(DAYS, values, strict=True)):
            unadjusted = scale * value * (2 if split and symbol == "ASSET_A" and day < SPLIT else 1)
            row = {
                **_common(f"{name}-{symbol}", index, micros(day), ingested=micros(DAYS[-1])),
                "instrument_id": symbol,
                "session_date": day,
                "interval": "1d",
                "bar_end_us": micros(day),
                "basis": "unadjusted",
                "currency": "USD",
                **dict.fromkeys(("open", "high", "low", "close"), Decimal(unadjusted)),
                "volume": Decimal(1000),
                "price_role": "canonical",
                "value_state": "present",
            }
            rows.append(_identity(row, "prices"))
    return rows


def action_rows(known: int | None = None) -> list[Document]:
    """ASSET_A's 2:1 split, known at the open of its ex-date session unless told otherwise."""
    row = {
        **_common(
            "split", 0, micros(SPLIT, 9) if known is None else known, ingested=micros(DAYS[-1])
        ),
        "instrument_id": "ASSET_A",
        "action_id": "split:" + SPLIT.isoformat(),
        "action_type": "split",
        "ex_date": SPLIT,
        "record_date": None,
        "pay_date": None,
        "effective_date": SPLIT,
        "amount": None,
        "ratio": Decimal(2),
        "currency": None,
        "value_state": "present",
    }
    return [_identity(row, "corporate_actions")]


def calendar_rows(known: Callable[[Document], int] | None = None) -> list[Document]:
    """The fixture calendar, known from the epoch unless ``known`` times each session."""
    rows = []
    for index, row in enumerate(session_rows()):
        values = {
            key: row[key]
            for key in (
                "calendar_id",
                "venue",
                "open_at_us",
                "close_at_us",
                "status",
                "timezone_version",
            )
        }
        values["session_date"] = date.fromisoformat(row["session_date"])
        at = 1 if known is None else known(values)
        rows.append(
            _identity({**_common("session", index, at, ingested=at), **values}, "calendar_sessions")
        )
    return rows


def heads_ref(pins: list[Document], grants: list[str], domain: str, **cut: str | None) -> Document:
    pin = {
        "domain": domain,
        "pins": [{**item, "from": cut.get("from"), "to": cut.get("to")} for item in pins],
        "granted_rules": sorted(grants),
        "excluded_flags": [],
    }
    digest = content_sha256({"schema": "aas-head-binding-v1", **pin})
    return {
        "ref_kind": "heads",
        "ref_id": digest,
        "ref_version": "aas-head-binding-v1",
        "hash": digest,
        "schema": "aas-head-binding-v1",
        "hash_format": "aas-canonical-json-sha256-v1",
        "pin": pin,
    }


def bind(body: Document, role: str, ref: Document, ordinal: int = 0) -> None:
    """Point ``role`` at ``ref``, dropping a reference nothing else binds any more."""
    body["bindings"] = [
        item for item in body["bindings"] if (item["role"], item["ordinal"]) != (role, ordinal)
    ]
    body["bindings"].append(
        {key: value for key, value in ref.items() if key not in ("pin", "schema")}
        | {"role": role, "ordinal": ordinal, "ref_schema": ref["schema"]}
    )
    used = {(item["ref_kind"], item["ref_id"], item["ref_version"]) for item in body["bindings"]}
    body["refs"] = [
        item
        for item in body["refs"]
        if (item["ref_kind"], item["ref_id"], item["ref_version"]) in used
        and item["ref_id"] != ref["ref_id"]
    ]
    body["refs"].append(ref)


def headed(  # noqa: PLR0913 -- what each head binding grants and how the actions are timed
    workspace: Workspace,
    body: Document,
    *,
    price_grants: tuple[str, ...] = (LAG,),
    action_grants: tuple[str, ...] = (ABSENT, EXDATE),
    session_grants: tuple[str, ...] = (DECLARED,),
    action_known: int | None = None,
    mode: str = "strict_pit",
    recorded: bool = False,
    sessions_known: Callable[[Document], int] | None = None,
) -> Document:
    """Rebind signal, execution prices and sessions to head bindings.

    The generations are rule-timed unless ``recorded``, which publishes them as sealed
    imports with recorded times; those need no time-rule grant. The actions hold only
    ASSET_A's split, so the actions binding grants reading the other instruments' absent
    actions as none unless told otherwise.
    """
    if recorded:
        prices = sealed(workspace, "prices.syn.canonical", "prices", price_rows())
        actions = sealed(workspace, "actions.syn", "corporate_actions", action_rows(action_known))
        sessions = sealed(workspace, "sessions.syn", "calendar_sessions", calendar_rows())
        price_grants = session_grants = ()
        action_grants = tuple(grant for grant in action_grants if grant == ABSENT)
    else:
        prices = publish(workspace, "prices.syn.canonical", "prices", price_rows(), (LAG, LAG))
        actions = publish(
            workspace,
            "actions.syn",
            "corporate_actions",
            action_rows(action_known),
            (EXDATE, EXDATE),
        )
        sessions = publish(
            workspace,
            "sessions.syn",
            "calendar_sessions",
            calendar_rows(sessions_known),
            (DECLARED, DECLARED),
        )
    priced = heads_ref([prices], list(price_grants), "prices")
    bind(body, "signal_prices", priced)
    bind(body, "execution_prices", priced)
    bind(body, "sessions", heads_ref([sessions], list(session_grants), "calendar_sessions"))
    bind(body, "actions", heads_ref([actions], list(action_grants), "corporate_actions"))
    for selection in body["price_inputs"]:
        if selection["binding"]["role"] == "signal_prices":
            selection.update(basis="split_adjusted", price_role="canonical")
    body["cutoff"]["mode"] = mode
    return body


@pytest.fixture
def heads_home(stored_template: tuple[Path, bytes], tmp_path: Path) -> tuple[Path, Document]:
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        native = json.loads(json.dumps(body))
        headed(workspace, body)
        _ = workspace.market.execute("CHECKPOINT")
    return tmp_path / "home", {"heads": body, "native": native}


def _reads(prepared: object) -> list[Document]:
    sealed = json.loads(cast("Any", prepared).provenance)
    return cast("list[Document]", sealed["head_reads"])


def test_strict_preparation_records_head_read_receipt(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """Every head read a strict preparation made is in its sealed document, and in its run."""
    body = copy_request(stored_template, tmp_path)
    home = tmp_path / "home"
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        headed(workspace, body, recorded=True)
        _ = workspace.market.execute("CHECKPOINT")
    with open_workspace(home) as workspace:
        prepared = prepare(workspace, body)
    assert dict(prepared.targets) == EXPECTED
    reads = _reads(prepared)
    assert [(item["role"], item["purpose"], item["decision_date"]) for item in reads] == [
        ("sessions", "calendar", None),
        ("signal_prices", "decision", DAYS[2].isoformat()),
        ("signal_prices", "decision", DAYS[4].isoformat()),
        ("execution_prices", "outcomes", None),
    ]
    for item in reads:
        assert item["receipt_sha256"] == content_sha256(item["receipt"])
    calendar, *decisions, outcomes = (item["receipt"] for item in reads)
    assert calendar["schema"] == "aas-head-revisions-v1"
    assert calendar["mode"] == "strict_pit"
    for receipt, slot in zip(decisions, prepared.slots, strict=True):
        # A derived series: the adjusted receipt carries both reads it was made from.
        assert receipt["schema"] == "aas-adjusted-read-v1"
        assert receipt["basis"] == "split_adjusted"
        assert receipt["prices"]["query"]["cutoff_us"] == slot.cutoff_us
        assert receipt["actions"]["query"]["cutoff_us"] == slot.cutoff_us
        # The open sessions known at the cutoff are the derivation's grid, so a dividend
        # whose prior session has no bar is never reinvested at an older close.
        assert receipt["prices"]["query"]["grid"] == [
            day.isoformat() for day in DAYS if day <= slot.decision_date
        ]
        assert receipt["prices_hash"] == content_sha256(receipt["prices"])
        assert receipt["actions_hash"] == content_sha256(receipt["actions"])
    # The split is first known at its ex-date open, after the first decision's cutoff.
    assert [receipt["actions"]["heads"] for receipt in decisions] == [0, 1]
    assert outcomes["schema"] == "aas-head-read-v1"
    assert outcomes["query"]["cutoff_us"] == body["cutoff"]["knowledge_cutoff_us"]
    assert outcomes["query"]["price_roles"] == ["canonical"]
    # A recorded run keeps the same sealed bytes, and its bundle names each binding by a
    # hash whose document stays readable for every later verification.
    request = tmp_path / "request.json"
    request.write_bytes(canonical_json_bytes(body))
    installed = run_cli("db", "run-install", home=home)
    assert installed.returncode == 0, installed.stderr
    receipt = run_backtest(
        RunBacktestRequest(
            request=request,
            request_sha256=hashlib.sha256(request.read_bytes()).hexdigest(),
            home=home,
        )
    )
    assert receipt["preparation"] == {"sha256": hashlib.sha256(prepared.provenance).hexdigest()}
    with open_workspace(home) as workspace:
        stored = read_run(workspace, str(cast("Document", receipt["run"])["run_id"]), budget=BUDGET)
        assert stored["status"] == "SUCCESS"
        for ref in (ref for ref in body["refs"] if ref["ref_kind"] == "heads"):
            assert read_head_binding(workspace, ref["hash"]).binding_hash == ref["hash"]
    verified = run_cli("db", "verify", home=home)
    assert verified.returncode == 0, verified.stderr
    # The bundle is verified again from the retained document, so changing it is refused.
    digest = next(ref["hash"] for ref in body["refs"] if ref["ref_kind"] == "heads")
    stored = home / "raw" / digest[:2] / digest
    stored.chmod(0o600)
    stored.write_bytes(stored.read_bytes() + b"\n")
    assert run_cli("db", "verify", home=home).returncode != 0


def test_head_reads_record_the_rules_their_grants_apply(
    heads_home: tuple[Path, Document],
) -> None:
    """A rule-timed binding's reads name the rules they relied on and withheld none."""
    home, bodies = heads_home
    with open_workspace(home) as workspace:
        prepared = prepare(workspace, bodies["heads"])
    assert dict(prepared.targets) == EXPECTED
    calendar, *decisions, outcomes = (item["receipt"] for item in _reads(prepared))
    assert calendar["applied_rules"] == [DECLARED]
    assert calendar["withheld_rules"] == []
    assert calendar["time_rules"] == [[0, "sessions.syn-g1", DECLARED, DECLARED]]
    for receipt in decisions:
        assert receipt["prices"]["applied_rules"] == [LAG]
        assert receipt["actions"]["applied_rules"] == [EXDATE]
        assert receipt["withheld_rules"] == []
        assert receipt["actions"]["held"] == []
    assert outcomes["applied_rules"] == [LAG]


def test_derived_canonical_signal_decides_like_the_native_reference_series(
    heads_home: tuple[Path, Document],
) -> None:
    """Unadjusted bars and a split decide what the provider-adjusted reference decided."""
    home, bodies = heads_home
    with open_workspace(home) as workspace:
        native = prepare(workspace, bodies["native"])
        heads = prepare(workspace, bodies["heads"])
    assert dict(native.targets) == dict(heads.targets) == EXPECTED
    assert [slot.decision_date for slot in heads.slots] == [DAYS[2], DAYS[4]]
    for day in (DAYS[2], DAYS[4]):
        assert {
            key: value.returns[2] for key, value in heads.features[day].items()
        } == pytest.approx({key: value.returns[2] for key, value in native.features[day].items()})
    # Execution reads the unadjusted bars: ASSET_A opens at twice the fixture before the split.
    envelope = json.loads(heads.envelope.canonical_bytes)
    assert envelope["dates"] == [day.isoformat() for day in DAYS[2:7]]
    assert envelope["opens"][0]["ASSET_A"] == 2 * VALUES["ASSET_A"][2]
    assert envelope["opens"][1]["ASSET_A"] == VALUES["ASSET_A"][3]
    assert heads.inputs.source_pins == ()
    result = cast(
        "Document", run_document(heads.envelope.canonical_bytes, heads.envelope.envelope_sha256)
    )
    assert result["result"]["nav"]


def test_an_ungranted_price_rule_withholds_strict_bars_but_not_research(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body, price_grants=())
        headed_research = cast("Document", json.loads(json.dumps(body)))
        headed_research["cutoff"]["mode"] = "observed_snapshot_research"
        with pytest.raises(ValueError, match="insufficient eligible buckets"):
            prepare(workspace, body)
        # A research read ignores grants: the same binding decides as granted.
        prepared = prepare(workspace, headed_research)
        assert dict(prepared.targets) == EXPECTED
        assert all(
            item["receipt"]["prices"]["mode"] == "observed_snapshot_research"
            for item in _reads(prepared)
            if item["purpose"] == "decision"
        )


def test_a_known_action_withheld_by_its_grant_is_never_dropped(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """Without its grant the split is held, so ASSET_A's earlier bars cannot be adjusted."""
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body, action_grants=())
        with pytest.raises(ValueError, match="ASSET_A"):
            prepare(workspace, body)


def test_an_action_known_after_a_decision_does_not_adjust_it(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """A split first known after the second decision leaves that decision unadjusted."""
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body, action_known=micros(DAYS[5]))
        prepared = prepare(workspace, body)
    returns = {key: value.returns[2] for key, value in prepared.features[DAYS[4]].items()}
    # Two buckets back is the unadjusted December close, 24, so ASSET_A fell by half; the
    # split known in time leaves it flat (12 against an adjusted 12).
    assert returns["ASSET_A"] == pytest.approx(12 / 24 - 1)
    decision = next(item for item in _reads(prepared) if item["decision_date"] == "2026-02-26")
    assert decision["receipt"]["actions"]["heads"] == 0


def test_an_ungranted_calendar_knows_no_session(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body, session_grants=())
        with pytest.raises(ValueError, match="open-session"):
            prepare(workspace, body)


def test_a_native_price_generation_needs_a_sessions_generation(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        sessions = publish(
            workspace, "sessions.syn", "calendar_sessions", calendar_rows(), (DECLARED, DECLARED)
        )
        bind(body, "sessions", heads_ref([sessions], [DECLARED], "calendar_sessions"))
        with pytest.raises(ValueError, match="sessions generation"):
            prepare(workspace, body)


def test_a_head_bound_macro_series_decides_like_its_native_generation(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        second_recipe(workspace, tmp_path, body)
        native = prepare(workspace, body)
        rows = [
            _identity(
                {
                    **_common(
                        "macro", index, micros(date.fromisoformat(day)), ingested=micros(DAYS[-1])
                    ),
                    "series_id": "MACRO",
                    "observation_period": date.fromisoformat(day),
                    "unit": "ratio",
                    "source_vintage_start": None,
                    "source_vintage_end": None,
                    "value": Decimal(value),
                    "value_state": "present",
                },
                "macro_observations",
            )
            for index, (day, value) in enumerate((("2025-12-31", "0"), ("2026-01-31", "2")))
        ]
        pin = publish(workspace, "macro.syn", "macro_observations", rows, (DAY_END, DAY_END))
        bind(body, "macro", heads_ref([pin], [DAY_END], "macro_observations"))
        heads = prepare(workspace, body)
        assert dict(heads.targets) == dict(native.targets)
        assert [dict(item.signals["synthetic-choice"]) for item in heads.decisions] == [
            dict(item.signals["synthetic-choice"]) for item in native.decisions
        ]
        macro = [item for item in _reads(heads) if item["role"] == "macro"]
        assert [item["purpose"] for item in macro] == ["admission", "decision", "decision"]
        wrong = json.loads(json.dumps(body))
        wrong["macro_inputs"][0]["unit"] = "percent"
        with pytest.raises(ValueError, match="macro unit mismatch"):
            prepare(workspace, wrong)
        ungranted = json.loads(json.dumps(body))
        ungranted["cutoff"]["mode"] = "strict_pit"
        bind(ungranted, "macro", heads_ref([pin], [], "macro_observations"))
        with pytest.raises(ValueError, match="holds no head of MACRO"):
            prepare(workspace, ungranted)


def _fx_strategy(workspace: Workspace, root: Path, body: Document) -> None:
    """The fixture strategy with one macro signal on the USD/KRW fixing: cash below 1."""
    value = replace(
        contract(), macro_signals=(MacroSignalSpec("USD/KRW", (0,), "EXACT", "LT", (1.0,)),)
    )
    document = json.loads(raw_bundle(value))
    document["bundle_version"] = "3"
    raw = canonical_json_bytes(document)
    path = root / "fx-strategy.json"
    path.write_bytes(raw)
    registered = register_strategy(
        workspace, path, hashlib.sha256(raw).hexdigest(), "synthetic-probe", "3"
    )
    body["strategy"].update(
        version="3",
        raw_sha256=registered["raw_sha256"],
        contract_sha256=registered["contract_sha256"],
    )


def test_a_head_bound_fx_fixing_is_a_macro_series(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """An FX fixing reads as series BASE/QUOTE, dated by its UTC day, in its quote currency."""
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        _fx_strategy(workspace, tmp_path, body)
        rows = []
        for index, (day, rate) in enumerate((("2025-12-31", "0.5"), ("2026-01-31", "2"))):
            fixing = micros(date.fromisoformat(day))
            rows.append(
                _identity(
                    {
                        **_common("fx", index, fixing, ingested=micros(DAYS[-1])),
                        "base_currency": "USD",
                        "quote_currency": "KRW",
                        "fixing_at_us": fixing,
                        "rate": Decimal(rate),
                        "value_state": "present",
                    },
                    "fx_rates",
                )
            )
        pin = publish(workspace, "fx.usdkrw.syn", "fx_rates", rows, (DAY_END, DAY_END))
        ref = heads_ref([pin], [DAY_END], "fx_rates")
        body["refs"].append(ref)
        body["bindings"].append(
            {key: value for key, value in ref.items() if key not in ("pin", "schema")}
            | {"role": "macro", "ordinal": 0, "ref_schema": ref["schema"]}
        )
        body["macro_inputs"] = [
            {"binding": {"role": "macro", "ordinal": 0}, "series_id": "USD/KRW", "unit": "KRW"}
        ]
        prepared = prepare(workspace, body)
        # 0.5 below 1 at the first decision sends it to cash; 2 at the second does not.
        assert [dict(item.signals["synthetic-choice"]) for item in prepared.decisions] == [
            {"USD/KRW": True},
            {"USD/KRW": False},
        ]
        assert dict(prepared.targets) == {DAYS[2]: {}, DAYS[4]: {"ASSET_B": 1.0}}
        wrong = json.loads(json.dumps(body))
        wrong["macro_inputs"][0]["unit"] = "USD"
        with pytest.raises(ValueError, match="quote currency"):
            prepare(workspace, wrong)


def test_head_binding_hash_is_one_identity_across_request_and_reader() -> None:
    """The request admits a head reference by the same hash the reader gives its binding."""
    pin = GenerationPin("prices.syn", "2", "prices.syn-g2", "a" * 64, "b" * 64)
    other = GenerationPin("prices.alt", "1", "prices.alt-g1", "c" * 64, "d" * 64)
    binding = HeadBinding(
        "prices",
        (HeadPin(pin, None, date(2026, 1, 1)), HeadPin(other, date(2026, 1, 1), None)),
        granted_rules=(LAG, DAY_END),
        excluded_flags=("provider_reported_partial",),
    )
    document = binding.document()
    ref = heads_ref([], [], "prices")
    ref["pin"] = {
        key: value
        for key, value in json.loads(canonical_json_bytes(document)).items()
        if key != "schema"
    }
    assert head_binding(json.loads(canonical_json_bytes(document))) == binding
    digest = content_sha256({"schema": "aas-head-binding-v1", **ref["pin"]})
    assert digest == binding.binding_hash
    unsorted = json.loads(canonical_json_bytes(document))
    unsorted["granted_rules"].reverse()
    with pytest.raises(ValueError, match="canonical spelling"):
        head_binding(unsorted)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("hash", "disagrees with complete pin"),
        ("unsorted", "sorted and unique"),
        ("gap", "contiguous"),
        ("domain", "does not serve its role"),
        ("unused", "exactly one actions binding"),
        ("missing", "exactly one actions binding"),
        ("extra", "exactly one actions binding"),
    ],
)
def test_the_request_admits_head_references_only_as_their_roles_allow(
    stored_template: tuple[Path, bytes], tmp_path: Path, change: str, message: str
) -> None:
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body)
    parse_prepare_request(canonical_json_bytes(body))
    actions = next(ref for ref in body["refs"] if ref["pin"].get("domain") == "corporate_actions")
    prices = next(ref for ref in body["refs"] if ref["pin"].get("domain") == "prices")
    if change == "hash":
        actions["pin"]["granted_rules"] = [DAY_END]
    elif change == "unsorted":
        prices["pin"]["granted_rules"] = [LAG, DAY_END]
    elif change == "gap":
        first = prices["pin"]["pins"][0]
        prices["pin"]["pins"] = [{**first, "to": "2026-01-01"}, {**first, "from": "2026-01-02"}]
    elif change == "domain":
        bind(body, "sessions", prices)
    elif change == "unused":
        # A reference signal is read as stored, so nothing derives from the actions.
        for selection in body["price_inputs"]:
            if selection["binding"]["role"] == "signal_prices":
                selection["price_role"] = "reference"
    elif change == "extra":
        # A second actions binding that no derived selection takes.
        extra = heads_ref(actions["pin"]["pins"], [EXDATE], "corporate_actions")
        bind(body, "actions", extra, ordinal=1)
    else:
        body["bindings"] = [item for item in body["bindings"] if item["role"] != "actions"]
        body["refs"] = [ref for ref in body["refs"] if ref is not actions]
    with pytest.raises(ValueError, match=message):
        parse_prepare_request(canonical_json_bytes(body))


def test_a_cutover_reads_each_interval_from_its_own_pin(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """Execution bars after a cutover come from the second pin, never from the first."""
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body)
        # The second provider states every bar at three times the first one's number.
        second = publish(
            workspace,
            "prices.syn.second",
            "prices",
            price_rows(split=False, scale=3, name="second"),
            (LAG, LAG),
        )
        first = next(ref for ref in body["refs"] if ref["pin"].get("domain") == "prices")
        pins = [{**item, "from": None, "to": None} for item in first["pin"]["pins"]]
        cutover = heads_ref([], [LAG], "prices")
        cutover["pin"]["pins"] = [
            {**pins[0], "to": "2026-03-01"},
            {**second, "from": "2026-03-01", "to": None},
        ]
        cutover["ref_id"] = cutover["hash"] = content_sha256(
            {"schema": "aas-head-binding-v1", **cutover["pin"]}
        )
        bind(body, "execution_prices", cutover)
        prepared = prepare(workspace, body)
    envelope = json.loads(prepared.envelope.canonical_bytes)
    assert [row.get("ASSET_A") for row in envelope["opens"]] == [30, 15, 12, 36, 36]
    outcomes = next(item for item in _reads(prepared) if item["purpose"] == "outcomes")
    assert [item["generation_id"] for item in outcomes["receipt"]["binding"]["pins"]] == [
        "prices.syn.canonical-g1",
        "prices.syn.second-g1",
    ]


def _signal_reads(prepared: object) -> list[Document]:
    return [item for item in _reads(prepared) if item["role"] == "signal_prices"]


def test_an_absent_action_source_is_recorded_and_strict_needs_its_grant(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """No action for an instrument is no evidence of none: strict reads it only as granted."""
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body, action_grants=(EXDATE,))
        # The split is not yet known at the first decision, so nothing is known of ASSET_A.
        with pytest.raises(
            ValueError, match=r"no corporate action evidence for ASSET_A, ASSET_B, REF_X"
        ):
            prepare(workspace, body)
        research = cast("Document", json.loads(json.dumps(body)))
        research["cutoff"]["mode"] = "observed_snapshot_research"
        prepared = prepare(workspace, research)
        assert dict(prepared.targets) == EXPECTED
        assert [item["actions"] for item in _signal_reads(prepared)] == [
            {"role": "actions", "ordinal": 0}
        ] * 2
        assert [item["no_action_source"] for item in _signal_reads(prepared)] == [
            ["ASSET_A", "ASSET_B", "REF_X"],
            ["ASSET_B", "REF_X"],
        ]
    granted = copy_request(stored_template, tmp_path / "granted")
    with open_workspace(
        tmp_path / "granted" / "home", writable=True, strategy_write=True
    ) as workspace:
        headed(workspace, granted)
        prepared = prepare(workspace, granted)
    assert dict(prepared.targets) == EXPECTED
    assert [item["no_action_source"] for item in _signal_reads(prepared)] == [
        ["ASSET_A", "ASSET_B", "REF_X"],
        ["ASSET_B", "REF_X"],
    ]


def _dividends(workspace: Workspace) -> Document:
    """A dividend each for ASSET_B and REF_X: evidence about them a split read ignores."""
    rows = [
        _identity(
            {
                **_common("dividend", index, micros(DAYS[1], 9), ingested=micros(DAYS[-1])),
                "instrument_id": symbol,
                "action_id": "dividend:" + DAYS[1].isoformat(),
                "action_type": "dividend",
                "ex_date": DAYS[1],
                "record_date": None,
                "pay_date": None,
                "effective_date": DAYS[1],
                "amount": Decimal("0.1"),
                "ratio": None,
                "currency": "USD",
                "value_state": "present",
            },
            "corporate_actions",
        )
        for index, symbol in enumerate(("ASSET_B", "REF_X"))
    ]
    return publish(workspace, "actions.syn.b", "corporate_actions", rows, (EXDATE, EXDATE))


@pytest.mark.parametrize("swapped", [False, True])
def test_each_derived_selection_takes_its_own_actions_binding(
    stored_template: tuple[Path, bytes], tmp_path: Path, *, swapped: bool
) -> None:
    """Two derived selections bind two action sources, taken by ordinal in binding order."""
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body)
        prices = next(ref for ref in body["refs"] if ref["pin"].get("domain") == "prices")
        split = next(ref for ref in body["refs"] if ref["pin"].get("domain") == "corporate_actions")
        pins = [
            {key: value for key, value in item.items() if key not in ("from", "to")}
            for item in prices["pin"]["pins"]
        ]
        # The same bars under a second binding: one selection per binding.
        bind(body, "signal_prices", heads_ref(pins, [DAY_END, LAG], "prices"), ordinal=1)
        first = next(
            row for row in body["price_inputs"] if row["binding"]["role"] == "signal_prices"
        )
        others = [name for name in first["instrument_ids"] if name != "ASSET_A"]
        body["price_inputs"].append(
            {**first, "binding": {"role": "signal_prices", "ordinal": 1}, "instrument_ids": others}
        )
        first["instrument_ids"] = ["ASSET_A"]
        dividends = heads_ref([_dividends(workspace)], [ABSENT, EXDATE], "corporate_actions")
        order = (dividends, split) if swapped else (split, dividends)
        for ordinal, ref in enumerate(order):
            bind(body, "actions", ref, ordinal=ordinal)
        prepared = prepare(workspace, body)
    reads = sorted(
        (
            item["ordinal"],
            item["decision_date"],
            item["actions"]["ordinal"],
            item["no_action_source"],
        )
        for item in _signal_reads(prepared)
    )
    first, second = DAYS[2].isoformat(), DAYS[4].isoformat()
    if swapped:
        # ASSET_A reads the dividends, which say nothing of it, so its split never applies.
        assert reads == [
            (0, first, 0, ["ASSET_A"]),
            (0, second, 0, ["ASSET_A"]),
            (1, first, 1, ["ASSET_B", "REF_X"]),
            (1, second, 1, ["ASSET_B", "REF_X"]),
        ]
        return
    assert dict(prepared.targets) == EXPECTED
    # The split is first known after the first decision.
    assert reads == [
        (0, first, 0, ["ASSET_A"]),
        (0, second, 0, []),
        (1, first, 1, []),
        (1, second, 1, []),
    ]


def test_a_strict_schedule_needs_the_next_session_known_at_its_cutoff(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """A calendar whose sessions are known only from their own end schedules no decision.

    That is how ``declared_session_end@1`` times a declaration's earlier dates: no
    decision before the declaration knows its next session.
    """
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(
            workspace,
            body,
            sessions_known=lambda row: (
                cast("int", row["close_at_us"])
                if row["close_at_us"] is not None
                else micros(cast("date", row["session_date"]), 23)
            ),
        )
        with pytest.raises(ValueError, match="incomplete decision-local calendar"):
            prepare(workspace, body)


def test_each_decision_projects_the_head_calendar_its_cutoff_knows(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """A session reopened between two decisions is closed at the first and open at the second."""
    inserted = date(2026, 1, 30)
    copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        rows = calendar_rows()
        publish(workspace, "sessions.rev", "calendar_sessions", rows, (DECLARED, DECLARED))
        original = next(row for row in rows if row["session_date"] == inserted)
        assert original["status"] == "closed"
        reopened = {
            **original,
            **_common("reopen", 0, micros(DAYS[3]), ingested=micros(DAYS[3])),
            "supersedes_revision_id": original["revision_id"],
            "op": "SUPERSEDE",
            "status": "open",
            "open_at_us": micros(inserted, 9),
            "close_at_us": micros(inserted),
        }
        pin = publish(
            workspace,
            "sessions.rev",
            "calendar_sessions",
            [reopened],
            (DECLARED, DECLARED),
            sequence=2,
        )
        binding = HeadBinding(
            "calendar_sessions", (HeadPin(GenerationPin(**pin)),), granted_rules=(DECLARED,)
        )
        read = load_pinned_revisions(
            workspace,
            binding,
            HeadQuery(
                subjects=("synthetic-calendar",),
                from_date=DAYS[0],
                to_date=DAYS[-1] + timedelta(days=1),
            ),
            strict=True,
            budget=BUDGET,
        )
    calendar = preparation._Calendar(  # noqa: SLF001
        tuple(item.values for item in read.revisions), read
    )
    visibility = preparation._Visibility(  # noqa: SLF001
        "strict_pit", micros(DAYS[-1]), None, DAYS[0], DAYS[4]
    )

    def status(cutoff: int) -> str:
        sessions = preparation._sessions(calendar, visibility, cutoff)  # noqa: SLF001
        return next(item.status for item in sessions if item.session_date == inserted)

    assert status(micros(DAYS[2])) == "closed"
    assert status(micros(DAYS[3]) - 1) == "closed"
    assert status(micros(DAYS[3])) == "open"
    assert status(micros(DAYS[4])) == "open"


@pytest.mark.parametrize("role", ["identity", "universe"])
@pytest.mark.parametrize("change", ["known_later", "valid_to_open"])
def test_head_signals_and_targets_are_held_by_the_membership_pins(
    stored_template: tuple[Path, bytes], tmp_path: Path, role: str, change: str
) -> None:
    """A head-bound instrument the pins do not hold is neither a signal nor a target.

    Known only after the decision's cutoff, ASSET_A's bars leave the signal; valid only
    until the execution open, it cannot be the decision's target.
    """
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        headed(workspace, body)
        body["explicit_decision_dates"] = [DAYS[2].isoformat()]
        assert dict(prepare(workspace, body).targets) == {DAYS[2]: {"ASSET_A": 1.0}}
        if change == "known_later":
            target_membership_interval(
                workspace, body, role, {"known_from_us": micros(DAYS[2]) + 1}
            )
            message = "insufficient eligible buckets"
        else:
            target_membership_interval(workspace, body, role, {"valid_to_us": micros(DAYS[3], 9)})
            message = (
                "selected target identity unavailable"
                if role == "identity"
                else "selected target outside pinned universe"
            )
        with pytest.raises(ValueError, match=message):
            prepare(workspace, body)


def test_a_head_binding_is_verified_again_only_from_its_retained_document(
    heads_home: tuple[Path, Document], tmp_path: Path
) -> None:
    """``binding-import`` keeps a canonical binding; a missing, changed or stale one is refused."""
    home, bodies = heads_home
    ref = next(ref for ref in bodies["heads"]["refs"] if ref["pin"].get("domain") == "prices")
    raw = canonical_json_bytes({"schema": "aas-head-binding-v1", **ref["pin"]})
    with open_workspace(home) as workspace:
        with pytest.raises(ValueError, match="not retained"):
            read_head_binding(workspace, ref["hash"])
        stored = workspace.paths.raw / ref["hash"][:2] / ref["hash"]
    loose = tmp_path / "loose.json"
    loose.write_bytes(json.dumps(json.loads(raw), indent=1).encode())
    refused = run_cli(
        "data",
        "binding-import",
        "--spec",
        str(loose),
        "--sha256",
        hashlib.sha256(loose.read_bytes()).hexdigest(),
        home=home,
    )
    assert refused.returncode != 0
    assert "not canonical" in refused.stderr
    spec = tmp_path / "binding.json"
    spec.write_bytes(raw)
    imported = run_cli(
        "data", "binding-import", "--spec", str(spec), "--sha256", ref["hash"], home=home
    )
    assert imported.returncode == 0, imported.stderr
    assert json.loads(imported.stdout)["pin"] == {"binding_hash": ref["hash"]}
    with open_workspace(home) as workspace:
        binding = read_head_binding(workspace, ref["hash"])
        assert binding.binding_hash == ref["hash"]
        verify_head_binding(workspace, binding, budget=BUDGET)
        pin = binding.pins[0]
        stale = replace(pin, pin=replace(pin.pin, manifest_hash="0" * 64))
        for changed in (
            replace(binding, pins=(stale,)),
            replace(binding, domain="calendar_sessions"),
        ):
            with pytest.raises(ValueError, match="does not match its marker"):
                verify_head_binding(workspace, changed, budget=BUDGET)
    stored.chmod(0o600)
    stored.write_bytes(raw + b"\n")
    with open_workspace(home) as workspace, pytest.raises(ValueError, match="hash mismatch"):
        read_head_binding(workspace, ref["hash"])


# KRW per USD at each fixture session: the KRW bars below are ASSET_B's USD numbers at
# these rates, so converting them back into the USD account gives the fixture's numbers.
USD_KRW: Final = (1000, 1100, 1200, 1300, 1400, 1300, 1200, 1100)


def _krw_bars() -> list[Document]:
    """ASSET_B's canonical unadjusted bars in KRW at the day's USD/KRW fixing."""
    rows = []
    for index, (day, value, rate) in enumerate(zip(DAYS, VALUES["ASSET_B"], USD_KRW, strict=True)):
        row = {
            **_common("krw-bar", index, micros(day), ingested=micros(DAYS[-1])),
            "instrument_id": "ASSET_B",
            "session_date": day,
            "interval": "1d",
            "bar_end_us": micros(day),
            "basis": "unadjusted",
            "currency": "KRW",
            **dict.fromkeys(("open", "high", "low", "close"), Decimal(value * rate)),
            "volume": Decimal(1000),
            "price_role": "canonical",
            "value_state": "present",
        }
        rows.append(_identity(row, "prices"))
    return rows


def _fixings(days: tuple[date, ...] = DAYS) -> list[Document]:
    """The USD/KRW fixing of each of ``days``, fixed and known at the session's close."""
    rows = []
    for index, (day, rate) in enumerate(zip(DAYS, USD_KRW, strict=True)):
        if day not in days:
            continue
        rows.append(
            _identity(
                {
                    **_common("usdkrw", index, micros(day), ingested=micros(DAYS[-1])),
                    "base_currency": "USD",
                    "quote_currency": "KRW",
                    "fixing_at_us": micros(day),
                    "rate": Decimal(rate),
                    "value_state": "present",
                },
                "fx_rates",
            )
        )
    return rows


def mixed_currency(  # noqa: PLR0913 -- the market split and the terms of its conversion
    workspace: Workspace,
    body: Document,
    *,
    signal_basis: str = "account_currency",
    max_age: int = 0,
    fixed: tuple[date, ...] = DAYS,
    grant: bool = True,
) -> Document:
    """Move ASSET_B to a KRW chain beside the USD one and grant converting it into USD.

    The USD bindings keep ASSET_A and REF_X. ASSET_B's signal and execution selections bind
    the KRW chain, its derived signal takes a second actions binding, and one
    ``fx_conversion`` binding over the USD/KRW fixings converts KRW into the USD account.
    """
    headed(workspace, body, recorded=True)
    krw = heads_ref([sealed(workspace, "prices.syn.krw", "prices", _krw_bars())], [], "prices")
    actions = sealed(
        workspace,
        "actions.syn.krw",
        "corporate_actions",
        [{**row, "revision_id": "krw-" + row["revision_id"]} for row in action_rows()],
    )
    fx = heads_ref(
        [sealed(workspace, "fx.usdkrw.syn", "fx_rates", _fixings(fixed))], [], "fx_rates"
    )
    for selection in body["price_inputs"]:
        selection["instrument_ids"] = [
            name for name in selection["instrument_ids"] if name != "ASSET_B"
        ]
    for role in ("signal_prices", "execution_prices"):
        bind(body, role, krw, 1)
        usd = next(row for row in body["price_inputs"] if row["binding"]["role"] == role)
        body["price_inputs"].append(
            {**usd, "binding": {"role": role, "ordinal": 1}, "instrument_ids": ["ASSET_B"]}
            | {"currency": "KRW"}
        )
    bind(body, "actions", heads_ref([actions], [ABSENT], "corporate_actions"), 1)
    bind(body, "fx_conversion", fx)
    if grant:
        body["fx_conversions"] = [
            {
                "binding": {"role": "fx_conversion", "ordinal": 0},
                "currency": "KRW",
                "series_id": "USD/KRW",
                "max_fixing_age_days": max_age,
                "signal_basis": signal_basis,
            }
        ]
    _ = workspace.market.execute("CHECKPOINT")
    return body


def test_a_mixed_currency_strategy_runs_on_its_granted_fx_pin(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """KRW bars converted into the USD account decide and fill as the USD fixture does.

    The run records the grant: the conversion, its binding, the fixing applied to each
    execution session, and each fixing read's receipt beside the reads it converted.
    """
    body = copy_request(stored_template, tmp_path)
    home = tmp_path / "home"
    with open_workspace(home, writable=True, strategy_write=True) as workspace:
        mixed_currency(workspace, body)
    with open_workspace(home) as workspace:
        prepared = prepare(workspace, body)
    assert dict(prepared.targets) == EXPECTED
    envelope = json.loads(prepared.envelope.canonical_bytes)
    period = DAYS[2:7]
    assert envelope["dates"] == [day.isoformat() for day in period]
    assert [row["ASSET_B"] for row in envelope["opens"]] == [
        VALUES["ASSET_B"][DAYS.index(day)] for day in period
    ]
    sealed_document = json.loads(prepared.provenance)
    (record,) = sealed_document["fx_conversions"]
    assert record["conversion"] == {
        "schema": "aas-fx-conversion-v1",
        "rule": "fx_latest_fixing_on_or_before@1",
        "currency": "KRW",
        "account_currency": "USD",
        "series_id": "USD/KRW",
        "direction": "divide",
        "max_fixing_age_days": 0,
        "signal_basis": "account_currency",
    }
    fx_ref = next(ref for ref in body["refs"] if ref["pin"].get("domain") == "fx_rates")
    assert record["binding"] == {"role": "fx_conversion", "ordinal": 0}
    assert record["binding_hash"] == fx_ref["hash"]
    assert record["fixings"] == [
        [day.isoformat(), day.isoformat(), float(USD_KRW[DAYS.index(day)])] for day in period
    ]
    assert record["unconverted"] == []
    reads = [item for item in _reads(prepared) if item["role"] == "fx_conversion"]
    assert [(item["purpose"], item["decision_date"]) for item in reads] == [
        ("decision", DAYS[2].isoformat()),
        ("decision", DAYS[4].isoformat()),
        ("outcomes", None),
    ]
    for item, cutoff in zip(
        reads,
        (*(slot.cutoff_us for slot in prepared.slots), body["cutoff"]["knowledge_cutoff_us"]),
        strict=True,
    ):
        assert item["receipt"]["query"]["cutoff_us"] == cutoff
        assert item["receipt"]["query"]["subjects"] == ["USD/KRW"]
        assert item["fx_conversion"] == record["conversion"]
        assert item["unconverted"] == []
    # Each decision converts the KRW signal bars it reads; the outcomes convert both
    # prices of each period session.
    assert [item["converted"] for item in reads] == [3, 5, 2 * len(period)]
    # The recorded run seals the same preparation, so the grant travels with the run.
    request = tmp_path / "request.json"
    request.write_bytes(canonical_json_bytes(body))
    installed = run_cli("db", "run-install", home=home)
    assert installed.returncode == 0, installed.stderr
    receipt = run_backtest(
        RunBacktestRequest(
            request=request,
            request_sha256=hashlib.sha256(request.read_bytes()).hexdigest(),
            home=home,
        )
    )
    assert receipt["preparation"] == {"sha256": hashlib.sha256(prepared.provenance).hexdigest()}
    with open_workspace(home) as workspace:
        stored = read_run(workspace, str(cast("Document", receipt["run"])["run_id"]), budget=BUDGET)
        assert stored["status"] == "SUCCESS"
        assert read_head_binding(workspace, fx_ref["hash"]).domain == "fx_rates"
    verified = run_cli("db", "verify", home=home)
    assert verified.returncode == 0, verified.stderr


def test_a_conversion_states_which_currency_its_signals_read(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """Signals read converted closes, or the KRW closes when the grant keeps them there."""
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        mixed_currency(workspace, body, signal_basis="price_currency")
        native = prepare(workspace, body)
        converted = json.loads(json.dumps(body))
        converted["fx_conversions"][0]["signal_basis"] = "account_currency"
        account = prepare(workspace, converted)
    decision = DAYS[4]
    rate = USD_KRW[DAYS.index(decision)]
    assert account.features[decision]["ASSET_B"].latest_price == VALUES["ASSET_B"][4]
    assert native.features[decision]["ASSET_B"].latest_price == VALUES["ASSET_B"][4] * rate
    # Signals in KRW read no fixing; the fills are still converted into the USD account.
    assert [item["purpose"] for item in _reads(native) if item["role"] == "fx_conversion"] == [
        "outcomes"
    ]
    assert (
        json.loads(native.envelope.canonical_bytes)["opens"]
        == json.loads(account.envelope.canonical_bytes)["opens"]
    )


def test_a_fixing_converts_only_within_its_granted_age(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    """A session without its own fixing takes an earlier one only inside the granted age."""
    missing = DAYS[5]
    fixed = tuple(day for day in DAYS if day != missing)
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        mixed_currency(workspace, body, fixed=fixed, max_age=7)
        prepared = prepare(workspace, body)
        (record,) = json.loads(prepared.provenance)["fx_conversions"]
        earlier = DAYS[4]
        assert [missing.isoformat(), earlier.isoformat(), float(USD_KRW[4])] in record["fixings"]
        envelope = json.loads(prepared.envelope.canonical_bytes)
        index = envelope["dates"].index(missing.isoformat())
        assert envelope["opens"][index]["ASSET_B"] == pytest.approx(
            VALUES["ASSET_B"][5] * USD_KRW[5] / USD_KRW[4]
        )
        # With no age at all, the decision's fill has no USD open, and the export refuses.
        strict = json.loads(json.dumps(body))
        strict["fx_conversions"][0]["max_fixing_age_days"] = 0
        with pytest.raises(ValueError, match="next-session open"):
            prepare(workspace, strict)


def test_a_foreign_currency_without_a_grant_is_refused_by_name(
    stored_template: tuple[Path, bytes], tmp_path: Path
) -> None:
    body = copy_request(stored_template, tmp_path)
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as workspace:
        mixed_currency(workspace, body, grant=False)
        with pytest.raises(ValueError, match="fx_conversion binding requires exactly one"):
            prepare(workspace, body)
        body["bindings"] = [item for item in body["bindings"] if item["role"] != "fx_conversion"]
        body["refs"] = [ref for ref in body["refs"] if ref["pin"].get("domain") != "fx_rates"]
        with pytest.raises(ValueError, match="KRW need an fx_conversions grant"):
            prepare(workspace, body)
