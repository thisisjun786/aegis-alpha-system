"""The documented dataset catalog is the code catalog, the registry agrees, and the CLI says so."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from aegis_alpha.data import fred_collect, sec_collect
from aegis_alpha.storage import calendar_refresh, kr_collection, kr_prices, strategy_registry
from aegis_alpha.storage.calendar_declaration import parse_declaration
from aegis_alpha.storage.dataset_catalog import (
    CATALOG,
    CLAMP_FLAG,
    DECIMAL_FLAGS,
    QUALITY_FLAGS,
    catalog_entry,
)
from aegis_alpha.storage.market_schema import DOMAINS
from aegis_alpha.storage.promotion import decimal_rules
from aegis_alpha.storage.promotion.mappers import REGISTRY
from aegis_alpha.storage.promotion.spec import CROSS_PROVIDER
from aegis_alpha.storage.promotion.time_rules import CLAMP_FLAG as RULE_CLAMP_FLAG
from aegis_alpha.storage.promotion.time_rules import RULES as TIME_RULES
from aegis_alpha.storage.source_reader import SourcePin

_ROOT = Path(__file__).resolve().parents[2]
_DESIGN = _ROOT / "dev-notes/design/data-vertical.md"
_DECLARATIONS = _ROOT / "src/aegis_alpha/storage/calendar_declarations"
_HEADING = "## dataset 카탈로그"
_COLUMNS = [
    "dataset",
    "도메인",
    "역할",
    "매퍼",
    "시간 규칙",
    "숫자 규칙",
    "품질 규칙",
    "flag",
    "동결",
    "원천",
]
_CODE = re.compile(r"`([^`]+)`")
_FROZEN = {"예": True, "아니오": False}


def _names(cell: str) -> list[str]:
    if cell == "없음":
        return []
    names = _CODE.findall(cell)
    if ", ".join(f"`{name}`" for name in names) != cell:
        raise AssertionError(f"a list cell is backticked names joined by ', ': {cell}")
    return names


def _one(cell: str) -> str:
    (name,) = _names(cell)
    return name


def documented_catalog(text: str) -> list[dict[str, object]]:
    """The catalog table of ``text`` as ``aas data datasets`` spells each entry."""
    _, found, rest = text.partition(_HEADING + "\n")
    if not found:
        raise AssertionError("dataset catalog heading is missing")
    table = []
    for line in rest.splitlines():
        if line.startswith("#"):
            break
        if line.startswith("| "):
            table.append([cell.strip() for cell in line.strip().strip("|").split(" | ")])
    if not table or table[0] != _COLUMNS or set(table[1]) != {"---"}:
        raise AssertionError("dataset catalog table header changed")
    entries: list[dict[str, object]] = []
    for cells in table[2:]:
        if len(cells) != len(_COLUMNS):
            raise AssertionError(f"malformed catalog row: {cells}")
        entries.append(
            {
                "dataset_id": _one(cells[0]),
                "domain": _one(cells[1]),
                "role": cells[2],
                "mappers": _names(cells[3]),
                "time_rules": _names(cells[4]),
                "decimal_rules": _names(cells[5]),
                "quality_rules": _names(cells[6]),
                "flags": _names(cells[7]),
                "frozen": _FROZEN[cells[8]],
            }
        )
    return entries


def _code_catalog() -> list[dict[str, object]]:
    return [entry.describe() for entry in CATALOG]


def test_the_documented_table_is_the_code_catalog() -> None:
    assert documented_catalog(_DESIGN.read_text(encoding="utf-8")) == _code_catalog()


def test_datasets_command_reports_the_documented_catalog(tmp_path: Path) -> None:
    home = tmp_path / "aas"

    def aas(*arguments: str) -> dict[str, object]:
        result = subprocess.run(  # noqa: S603 -- fixed interpreter, synthetic home
            [sys.executable, "-m", "aegis_alpha", "--home", str(home), *arguments],
            cwd=_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    aas("init")
    listed = aas("data", "datasets")
    assert listed["datasets"] == []
    assert listed["uncataloged"] == []
    catalog = listed["catalog"]
    assert isinstance(catalog, list)
    assert [item.pop("published") for item in catalog] == [[] for _ in catalog]
    assert catalog == documented_catalog(_DESIGN.read_text(encoding="utf-8"))


def test_entries_are_unique_named_and_in_market_domains() -> None:
    ids = [entry.dataset_id for entry in CATALOG]
    assert len(ids) == len(set(ids))
    for entry in CATALOG:
        segments = entry.dataset_id.split(".")
        # <domain>.<market>.<provider>[.ref]; a calendar is sessions.<venue>.
        allowed = {2} if entry.domain == "calendar_sessions" else {3, 4}
        assert len(segments) in allowed, entry.dataset_id
        assert entry.domain in DOMAINS, entry
        assert entry.role == ("reference" if "ref" in segments else "canonical"), entry
        assert catalog_entry(entry.dataset_id) is entry
        assert catalog_entry(entry.dataset_id + ".r2") is entry
        assert catalog_entry(entry.dataset_id + ".r12") is entry
    # The first time-rule generation has no suffix, so .r0 and .r1 are other names.
    assert catalog_entry("prices.kr.eodhd.r1") is None
    assert catalog_entry("prices.kr.eodhd.r0") is None
    assert catalog_entry("prices.kr") is None


def test_every_registered_mapper_builds_a_dataset_of_its_domain() -> None:
    used: set[str] = set()
    for entry in CATALOG:
        for name in entry.mappers:
            assert name in REGISTRY, name
            assert REGISTRY[name].domain == entry.domain, (entry.dataset_id, name)
            used.add(name)
    assert used == set(REGISTRY)


def test_rules_are_registered_and_fit_the_mappers() -> None:
    for entry in CATALOG:
        mappers = [REGISTRY[name] for name in entry.mappers]
        for name in entry.time_rules:
            kind = TIME_RULES[name].input_kind
            if kind is not None and mappers:
                inputs = {found for mapper in mappers for found in mapper.time_inputs.values()}
                assert kind in inputs, (entry.dataset_id, name)
        assert len(entry.time_rules) <= 1 or not mappers, entry.dataset_id
        if mappers:
            assert len(entry.time_rules) == 1, entry.dataset_id
        kinds = {kind for mapper in mappers for kind in mapper.numeric_columns({}).values()}
        rules = [decimal_rules.rule(name) for name in entry.decimal_rules]
        # Every numeric column has a rule that reads it, and every rule reads some column.
        for kind in kinds:
            assert any(kind in found.kinds for found in rules), (entry.dataset_id, kind)
        for found in rules:
            assert found.kinds & kinds, (entry.dataset_id, found.name)
        assert set(entry.quality_rules) <= set(QUALITY_FLAGS)
        if entry.quality_rules:
            assert entry.domain == "prices"
    assert set(QUALITY_FLAGS) == {CROSS_PROVIDER}
    assert set(DECIMAL_FLAGS) == set(decimal_rules.RULES)
    assert CLAMP_FLAG == RULE_CLAMP_FLAG


@pytest.mark.parametrize(
    ("name", "value", "kind", "currency", "expected"),
    [
        ("exact@1", 0.5, "DOUBLE", None, ()),
        ("krw_tick@1", 2_649_999.947, "DOUBLE", "KRW", ("provider_float_reconstructed",)),
        (
            "krw_tick@1",
            2.5,
            "DOUBLE",
            "KRW",
            ("provider_float_reconstructed", "decimal_rounding_tie"),
        ),
        ("float_shortest@1", 0.1, "DOUBLE", None, ("provider_float_storage",)),
        (
            "float_shortest@1",
            2.0**-13,
            "DOUBLE",
            None,
            ("provider_float_storage", "decimal_rounding_tie"),
        ),
        ("decimal_text@1", "1.6357e+06", "VARCHAR", None, ("volume_precision_limited",)),
    ],
)
def test_declared_rule_flags_are_the_flags_rules_attach(
    name: str, value: object, kind: str, currency: str | None, expected: tuple[str, ...]
) -> None:
    found = decimal_rules.convert(decimal_rules.rule(name), value, kind, currency=currency)
    assert set(found.flags) == set(expected)
    assert set(found.flags) <= set(DECIMAL_FLAGS[name])


def test_decimal_flags_are_the_flags_the_sql_conversion_sets() -> None:
    for name, found in decimal_rules.RULES.items():
        attached = {
            flag
            for kind in found.kinds
            for flag, _ in decimal_rules.conversion(found, "value", kind, "p_", currency="'KRW'")[
                1
            ].flags
        }
        assert attached == set(DECIMAL_FLAGS[name]), name


def _spec_rules(raw: bytes) -> tuple[str, list[str], list[str], list[str], list[str]]:
    spec = json.loads(raw)
    times = {rule["rule"] for rule in spec["time_rules"].values()}
    target = spec["target"]["dataset_id"]
    return (
        target,
        [spec["mapper"]["name"]],
        sorted(times),
        sorted(set(spec["decimal_rule"].values())),
        sorted(spec["quality_rules"]),
    )


def _entry_rules(dataset_id: str) -> tuple[list[str], list[str], list[str], list[str]]:
    entry = catalog_entry(dataset_id)
    assert entry is not None, dataset_id
    return (
        list(entry.mappers),
        sorted(entry.time_rules),
        sorted(entry.decimal_rules),
        sorted(entry.quality_rules),
    )


_KR_STEP_KINDS = {
    "eodhd.bars@1": "history",
    "eodhd.bars_adjusted@1": "history",
    "eodhd.bars_quarantine@1": "held",
    "eodhd.bulk_quarantine@1": "bulk",
    "eodhd.bulk_quarantine_adjusted@1": "bulk",
}


def _kr_spec(dataset: str, mapper: str) -> bytes:
    pin = {"source_id": "s", "source_sha256": "0" * 64, "table": "t", "digest": "1" * 64}
    step = kr_prices.Step(
        _KR_STEP_KINDS[mapper], date(2026, 1, 1), date(2027, 1, 1), mapper, (pin,)
    )
    return kr_prices.step_spec(
        step,
        dataset=dataset,
        parent=None,
        identity={"snapshot_id": "i", "content_hash": "2" * 64},
        calendar={"snapshot_id": "c", "content_hash": "3" * 64},
        lag_us=0,
    )


@pytest.mark.parametrize("dataset", [kr_prices.DATASET, kr_prices.REFERENCE_DATASET])
def test_kr_price_specs_use_the_catalog_rules(dataset: str) -> None:
    mappers, times, decimals, quality = _entry_rules(dataset)
    used: set[str] = set()
    for mapper in mappers:
        target, named, spec_times, spec_decimals, spec_quality = _spec_rules(
            _kr_spec(dataset, mapper)
        )
        assert (target, named) == (dataset, [mapper])
        assert (spec_times, spec_quality) == (times, quality), mapper
        used.update(spec_decimals)
    # Each spec picks per column from the entry's numeric rules; together they use all.
    assert sorted(used) == decimals


@pytest.mark.parametrize("path", sorted(_DECLARATIONS.glob("*.json")), ids=lambda path: path.stem)
def test_calendar_specs_use_the_catalog_rules(path: Path) -> None:
    raw = path.read_bytes()
    declaration = parse_declaration(raw, hashlib.sha256(raw).hexdigest())
    pin = SourcePin("s", "0" * 64, "t", "1" * 64)
    spec = calendar_refresh.promotion_spec(declaration, pin, None, "v")
    target, named, times, decimals, quality = _spec_rules(spec)
    assert catalog_entry(target) is not None, target
    assert (named, times, decimals, quality) == _entry_rules(target)


def test_flags_follow_the_rules_and_mappers() -> None:
    entry = catalog_entry("prices.kr.eodhd")
    assert entry is not None
    assert "provider_reported_partial" in entry.flags  # a mapper's row flag
    assert "provider_float_reconstructed" in entry.flags  # krw_tick@1
    us = catalog_entry("prices.us.eodhd")
    assert us is not None
    assert "cross_provider_mismatch" in us.flags
    macro = catalog_entry("macro.us.alfred")
    assert macro is not None
    assert "time_precision_day" in macro.flags  # local_day_end@1
    for entry in CATALOG:
        assert CLAMP_FLAG not in entry.flags


def test_frozen_providers_are_frozen() -> None:
    for entry in CATALOG:
        if "fmp" in entry.dataset_id.split("."):
            assert entry.frozen, entry.dataset_id
            assert entry.role == "reference", entry.dataset_id
    assert {entry.dataset_id for entry in CATALOG if entry.frozen} == {
        "prices.us.fmp.ref",
        "prices.ref.norgate",
        "actions.us.norgate",
        "actions.us.fmp.ref",
        "fx.usdkrw.norgate",
    }


def test_dataset_names_the_code_writes_are_cataloged() -> None:
    named = {
        kr_prices.DATASET,
        kr_prices.REFERENCE_DATASET,
        kr_prices.CALENDAR[0],
        fred_collect.ALFRED_DATASET,
        *fred_collect.DEFAULT_CSV_SERIES.values(),
        *sec_collect.DATASETS.values(),
        *(name for name in kr_collection.DATASETS.values() if not name.startswith("identity.")),
        strategy_registry._KR_PRICES,  # noqa: SLF001 -- the registry's fixed map
        strategy_registry._US_PRICES,  # noqa: SLF001
        strategy_registry._US_MACRO,  # noqa: SLF001
        strategy_registry._USDKRW_FX,  # noqa: SLF001
    }
    for path in sorted(_DECLARATIONS.glob("*.json")):
        named.add("sessions." + json.loads(path.read_text(encoding="utf-8"))["calendar_id"].lower())
    assert {name for name in named if catalog_entry(name) is None} == set()


def test_checker_rejects_a_changed_table() -> None:
    text = _DESIGN.read_text(encoding="utf-8")
    row = next(line for line in text.splitlines() if line.startswith("| `prices.kr.eodhd` |"))
    changed = text.replace(row, row.replace("| canonical |", "| reference |", 1))
    assert documented_catalog(changed) != _code_catalog()
    joined = text.replace(row, row.replace("`, `", "` `", 1))
    with pytest.raises(AssertionError, match="list cell"):
        documented_catalog(joined)
    with pytest.raises(AssertionError, match="heading is missing"):
        documented_catalog("# Doc\n")
