"""Invariants of ``eodhd.us_symbol_list@1`` over seeded random scenarios.

Each scenario is synthetic: 3-5 US symbols, 2-3 Norgate instruments with registered FMP
ISINs, registered master claims through Tuesday 2026-09-08, then 3-8 daily US symbol lists
on random calendar days (weekends included) with renames, reuses, missing symbols, and rows
whose ISIN is missing, invalid, unregistered, of another currency or repeated. A seeded
``random.Random`` stands in for a property-based library (none is a dependency), so every
failure names its seed and reproduces exactly.

On every scenario:

- (a) batch invariance: one build of every list equals the claims the real maintenance
  identity stage registers when the lists arrive one run at a time in the order of the
  days they cover (a subset of scenarios, each in its own disposable installation);
- (b) permutation invariance: the order the lists are given in does not matter;
- (c) safety: no claim covers a day on which an observation of its symbol names another
  instrument, or one on which its instrument is observed under another symbol, and no
  claim overlaps a registered claim of another instrument;
- (d) every claim continues a registered or accepted claim of the same symbol and
  instrument (never a rejected observation), across Saturdays and Sundays at most.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from aegis_alpha.storage import maintain_identity
from aegis_alpha.storage.identity import mint_instrument, parse_registry, register_identities
from aegis_alpha.storage.kr_identity import UNBOUNDED, SourceRows, eodhd_unit, import_unit
from aegis_alpha.storage.us_identity import session_start_us
from aegis_alpha.storage.us_symbols import Held, Registry, build_symbol_claims, covered_day
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.kr_identity_support import bad_check_digit, eodhd_job, isin, symbol
from tests.storage.promotion_support import add_source, at, bar

THROUGH = date(2026, 9, 8)
SYMBOLS = ("AAA", "BBB", "CCC", "DDD", "EEE")
ASSETIDS = ("900001", "900002", "900003")
ISINS = {assetid: isin(f"0000000{index}1", "US") for index, assetid in enumerate(ASSETIDS)}
UNREGISTERED = isin("000000099", "US")
SCENARIOS = 300
RUN_BY_RUN = 8
SATURDAY = 5

type Claim = tuple[str, str, int, int, int, str]
"""(token, Norgate asset ID, valid from, valid to, known from, source row hash)."""


@dataclass(slots=True)
class Scenario:
    seed: int
    symbols: tuple[str, ...]
    assetids: tuple[str, ...]
    base: dict[str, str]
    """Symbol -> asset ID registered through ``THROUGH``."""
    lists: list[tuple[str, list[dict[str, object]]]] = field(default_factory=list)
    """(retrieval instant, rows) in retrieval order."""


def _scenario(seed: int) -> Scenario:  # noqa: C901 -- one branch per kind of row
    rng = random.Random(seed)  # noqa: S311 -- reproducible synthetic scenarios
    symbols = SYMBOLS[: rng.randint(3, 5)]
    assetids = ASSETIDS[: rng.randint(2, 3)]
    pool = list(assetids)
    rng.shuffle(pool)
    base = {name: pool.pop() for name in symbols if pool and rng.random() < 0.8}  # noqa: PLR2004
    holders = dict(base)
    scenario = Scenario(seed, symbols, assetids, base)
    for offset in sorted(rng.sample(range(1, 17), rng.randint(3, 8))):
        day = THROUGH + timedelta(days=offset)
        if rng.random() < 0.3:  # noqa: PLR2004 -- a rename or a reuse
            moved, target = rng.choice(symbols), rng.choice(assetids)
            holders = {name: held for name, held in holders.items() if held != target}
            holders[moved] = target
        rows: list[dict[str, object]] = []
        for name in symbols:
            roll = rng.random()
            truth = holders.get(name)
            other = ISINS[rng.choice(assetids)]
            if roll < 0.12:  # noqa: PLR2004
                continue
            if roll < 0.6 and truth is not None:  # noqa: PLR2004
                value: str | None = ISINS[truth]
            elif roll < 0.7:  # noqa: PLR2004
                value = other
            elif roll < 0.76:  # noqa: PLR2004
                value = None
            elif roll < 0.82:  # noqa: PLR2004
                value = bad_check_digit(other)
            elif roll < 0.88:  # noqa: PLR2004
                value = UNREGISTERED
            elif roll < 0.94:  # noqa: PLR2004
                rows.append(symbol(name, other, exchange="NYSE", currency="EUR"))
                continue
            else:
                rows.append(symbol(name, other, exchange="NYSE", currency="USD"))
                value = other
            rows.append(symbol(name, value, exchange="NYSE", currency="USD"))
        hour = 12 if rng.random() < 0.15 else 22  # noqa: PLR2004 -- before the open: the day before
        scenario.lists.append((f"{day.isoformat()}T{hour:02d}:00:00Z", rows))
    return scenario


def _instrument(assetid: str) -> str:
    return mint_instrument("norgate_assetid", assetid)


def _registry(scenario: Scenario) -> Registry:
    end = session_start_us(THROUGH + timedelta(days=1))
    held = tuple(Held(f"{name}.US", _instrument(assetid), UNBOUNDED, end, listed=False)
                 for name, assetid in sorted(scenario.base.items()))  # fmt: skip
    owners = {ISINS[assetid]: frozenset({_instrument(assetid)}) for assetid in scenario.assetids}
    anchors = {_instrument(assetid): assetid for assetid in scenario.assetids}
    return Registry(held, owners, anchors)


def _sources(scenario: Scenario) -> list[SourceRows]:
    return [eodhd_unit(*eodhd_job(rows, exchange="US", retrieved=retrieved)).source_rows()
            for retrieved, rows in scenario.lists]  # fmt: skip


def _batch(scenario: Scenario, order: list[int] | None = None) -> set[Claim]:
    sources = _sources(scenario)
    if order is not None:
        sources = [sources[index] for index in order]
    document = build_symbol_claims(sources, _registry(scenario)).document()
    return {
        (str(row["token"]),
         str(cast("dict[str, str]", row["instrument"])["anchor_token"]),
         cast("int", row["valid_from_us"]), cast("int", row["valid_to_us"]),
         cast("int", row["known_from_us"]), str(row["source_hash"]))
        for row in cast("list[dict[str, object]]", document["assertions"])
    }  # fmt: skip


def _days(claim: Claim) -> list[date]:
    first = covered_day(claim[2] + 10 * 3_600_000_000)  # 10:00 New York of its first day
    count = round((claim[3] - claim[2]) / 86_400_000_000)
    return [first + timedelta(days=n) for n in range(count)]


def _named(scenario: Scenario) -> list[tuple[str, date, str]]:
    """(symbol, covered day, asset ID) of every row that names a registered instrument."""
    owners = {ISINS[assetid]: assetid for assetid in scenario.assetids}
    found = []
    for retrieved, rows in scenario.lists:
        moment = datetime.fromisoformat(retrieved)
        day = covered_day((moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1))
        for row in rows:
            owner = owners.get(cast("str", row["Isin"]))
            if owner is not None and row["Currency"] == "USD":
                found.append((f"{row['Code']}.US", day, owner))
    return found


def _violations(scenario: Scenario, claims: set[Claim]) -> list[str]:
    errors: list[str] = []
    named = _named(scenario)
    edge = session_start_us(THROUGH + timedelta(days=1))
    registered = {(f"{name}.US", assetid) for name, assetid in scenario.base.items()}
    for claim in sorted(claims):
        token, assetid, start, _ = claim[:4]
        days = set(_days(claim))
        for seen_symbol, seen_day, owner in named:
            if seen_day not in days:
                continue
            if seen_symbol == token and owner != assetid:
                errors.append(f"(c) {claim} covers {seen_day}: {token} named {owner}")
            if seen_symbol != token and owner == assetid:
                errors.append(f"(c) {claim} covers {seen_day}: {assetid} named as {seen_symbol}")
        for name, held in scenario.base.items():
            if f"{name}.US" == token and held != assetid and start < edge:
                errors.append(f"(c) {claim} overlaps {token}'s registered claim of {held}")
        anchors = [other for other in claims if other[:2] == (token, assetid) and other[3] <= start]
        last = max([other[3] for other in anchors], default=None)
        if (token, assetid) in registered:
            last = max(last or edge, edge)
        gap = [] if last is None else [
            day for day in (covered_day(last + 10 * 3_600_000_000) + timedelta(days=n)
                            for n in range(round((start - last) / 86_400_000_000)))
            if day.weekday() < SATURDAY]  # fmt: skip
        if last is None or gap:
            errors.append(f"(d) {claim} continues no registered or accepted claim")
    return errors


@pytest.mark.parametrize("chunk", range(SCENARIOS // 50))
def test_batch_builds_are_order_free_and_safe(chunk: int) -> None:
    failures: list[str] = []
    for seed in range(chunk * 50, chunk * 50 + 50):
        scenario = _scenario(seed)
        claims = _batch(scenario)
        order = list(range(len(scenario.lists)))
        random.Random(seed + 7919).shuffle(order)  # noqa: S311 -- a reproducible permutation
        if _batch(scenario, order) != claims:
            failures.append(f"seed {seed}: (b) the list order changes the claims")
        failures += [f"seed {seed}: {error}" for error in _violations(scenario, claims)]
    assert failures == []


# --- (a) run by run through the maintenance identity stage ---------------------------------


def _install(root: Path, scenario: Scenario) -> None:
    initialize(root)
    with open_workspace(root, writable=True, strategy_write=True) as ws:
        link = add_source(ws, [bar("AAA.US", THROUGH, 1.0, retrieved=at("2026-09-09T10:00:00"),
                                   currency="USD")], tag="base")["source_id"]  # fmt: skip
        end = session_start_us(THROUGH + timedelta(days=1))
        assertions = []
        for assetid in scenario.assetids:
            for provider, namespace, token, valid_to in (
                ("norgate", "norgate_assetid", assetid, None),
                ("fmp", "isin", ISINS[assetid], None),
                *(("eodhd", "eodhd_symbol", f"{name}.US", end)
                  for name, held in scenario.base.items() if held == assetid),
            ):  # fmt: skip
                assertions.append({
                    "instrument": {"anchor_namespace": "norgate_assetid", "anchor_token": assetid},
                    "provider": provider, "namespace": namespace, "token": token,
                    "valid_from_us": UNBOUNDED, "valid_to_us": valid_to, "known_from_us": 1,
                    "supersedes_assertion_id": None, "source_snapshot_id": "sl:" + link,
                    "source_hash": hashlib.sha256(token.encode()).hexdigest()})  # fmt: skip
        document = {
            "schema": "aas-identity-registry-v1", "issuers": [],
            "instruments": [{"anchor_namespace": "norgate_assetid", "anchor_token": assetid,
                             "issuer": None, "asset_type": "unclassified", "venue": "XNYS"}
                            for assetid in scenario.assetids],
            "assertions": assertions,
        }  # fmt: skip
        register_identities(ws.state, parse_registry(document), apply=True)


def _run_by_run(root: Path, scenario: Scenario) -> set[Claim]:
    for retrieved, rows in scenario.lists:
        with open_workspace(root, writable=True, strategy_write=True) as ws:
            import_unit(ws, eodhd_unit(*eodhd_job(rows, exchange="US", retrieved=retrieved)))
            now = (datetime.now(tz=UTC) - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(
                microseconds=1
            )
            maintain_identity.advance(ws, apply=True, now_us=now)
    with open_workspace(root) as ws:
        rows_found = ws.state.execute(
            "SELECT a.token, n.token, a.valid_from_us, a.valid_to_us, a.known_from_us, "
            "a.source_hash FROM identity_assertions a JOIN identity_assertions n ON "
            "n.instrument_id = a.instrument_id AND n.provider = 'norgate' AND "
            "n.namespace = 'norgate_assetid' WHERE a.provider = 'eodhd' AND "
            "substr(a.source_snapshot_id, 1, 33) = 'sl:qveris-eodhd-exchange-symbols-'"
        ).fetchall()
    return {(str(a), str(b), int(c), int(d), int(e), str(f)) for a, b, c, d, e, f in rows_found}


def _chronological(scenario: Scenario) -> Scenario:
    """The scenario's lists that each cover a later day than every list before them.

    A registered claim is never withdrawn, so a list arriving in a later run with other
    evidence about a day an earlier run already judged cannot change that judgement (the
    stage reports it as ``list_registered_conflict``). Run-by-run invariance is promised
    for lists that arrive in the order of the days they cover.
    """
    kept: list[tuple[str, list[dict[str, object]]]] = []
    last: date | None = None
    for retrieved, rows in scenario.lists:
        moment = datetime.fromisoformat(retrieved)
        day = covered_day((moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1))
        if last is None or day > last:
            kept.append((retrieved, rows))
            last = day
    return Scenario(scenario.seed, scenario.symbols, scenario.assetids, scenario.base, kept)


@pytest.mark.parametrize("seed", range(RUN_BY_RUN))
def test_run_by_run_maintenance_registers_what_one_build_finds(tmp_path: Path, seed: int) -> None:
    scenario = _chronological(_scenario(seed * 13 + 5))
    root = tmp_path / "aas"
    _install(root, scenario)
    registered = _run_by_run(root, scenario)
    assert registered == _batch(scenario), f"seed {scenario.seed}"
    assert _violations(scenario, registered) == []
