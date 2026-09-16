"""Real source-byte changes must distinguish successful stored preparations."""

from __future__ import annotations

import ast
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import aegis_alpha
from aegis_alpha.application.backtest_prepare import CALCULATION_MODULES
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.application.test_backtest_prepare import (
    DAYS,
    Document,
    generation_pin,
    micros,
    native,
    price_rows,
    replace_pin,
    stored_request,
)

# Imports occur only after selecting the disposable package, without bytecode caches.
PROBE = """
import hashlib, json, sys
from pathlib import Path
from fractions import Fraction
sys.path.insert(0, sys.argv[1])
import aegis_alpha
from aegis_alpha.application.backtest_prepare import (
    CALCULATION_MODULES, PrepareRequest, parse_prepare_request, prepare_backtest)
from aegis_alpha.application.backtest_cli import run_document
from aegis_alpha.compute_resources import ComputeBudget, compute_lease
from aegis_alpha.data.serialization import canonical_json_bytes
from aegis_alpha.engine.backtest_request import parse_backtest_request
from aegis_alpha.storage.workspace import open_workspace
package = Path(aegis_alpha.__file__).parent
assert package == Path(sys.argv[1]) / 'aegis_alpha'
payload = json.loads(sys.stdin.buffer.read())
root = Path(sys.argv[2])
with compute_lease(root / 'compute.lock'), open_workspace(root / 'home') as workspace:
    prepared = prepare_backtest(workspace,
        PrepareRequest(parse_prepare_request(canonical_json_bytes(payload['request']))),
        budget=ComputeBudget(Fraction(1), 512*1024*1024))
provenance = json.loads(prepared.provenance)
old = payload.get('old_projection')
if old is not None:
    _, reread = parse_backtest_request(old.encode(), definition=prepared.definition,
        convention_documents=tuple(canonical_json_bytes(doc)
            for doc in provenance['conventions']))
    assert reread.canonical_bytes == old.encode()
    assert reread.request_hash == hashlib.sha256(old.encode()).hexdigest()
print(json.dumps({
    'package': str(package),
    'modules': list(CALCULATION_MODULES),
    'sources': {str(path.relative_to(package)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(package.rglob('*.py'))},
    'projection': prepared.projection.canonical_bytes.decode(),
    'request_hash': prepared.request_hash,
    'envelope': prepared.envelope.canonical_bytes.decode(),
    'envelope_sha256': prepared.envelope.envelope_sha256,
    'provenance_sha256': hashlib.sha256(prepared.provenance).hexdigest(),
    'result': run_document(prepared.envelope.canonical_bytes, prepared.envelope.envelope_sha256),
    'old_projection_read': old is not None,
}))
"""


def identity_fixture(root: Path) -> Document:
    """Publish a real known revision: A's November close changes from 10 to 100."""
    initialize(root / "home")
    with open_workspace(root / "home", writable=True, strategy_write=True) as workspace:
        body = stored_request(workspace, root)
        original = next(
            row
            for row in price_rows(signal=True)
            if row["instrument_id"] == "ASSET_A" and row["session_date"] == DAYS[0].isoformat()
        )
        row = {
            **original,
            "revision_id": "known-correction",
            "op": "SUPERSEDE",
            "supersedes_revision_id": "signal:" + original["revision_id"],
            "revision_known_at_us": micros(DAYS[1]),
            "available_at_us": micros(DAYS[1]),
            **dict.fromkeys(("open", "high", "low", "close"), "100"),
        }
        native(
            workspace,
            root,
            "known-revision",
            [row],
            destination={
                "dataset_id": "signal",
                "version": "2",
                "generation_id": "known-revision",
                "operation_id": "op-known-revision",
                "parent_id": "signal",
            },
        )
        replace_pin(body, "signal_prices", generation_pin(workspace, "signal", "2"))
    return body


def source_copy(root: Path, name: str) -> Path:
    package = root / name / "aegis_alpha"
    shutil.copytree(
        Path(aegis_alpha.__file__).parent,
        package,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return package


def probe(root: Path, package: Path, name: str, payload: Document) -> Document:
    result = subprocess.run(  # noqa: S603 -- fixed offline child; run bounds and reaps it
        [sys.executable, "-B", "-c", PROBE, str(package.parent), str(root)],
        input=canonical_json_bytes(payload),
        cwd=root,
        capture_output=True,
        timeout=60,
        check=True,
    )
    (root / (name + ".json")).write_bytes(result.stdout)
    report = json.loads(result.stdout)
    assert hashlib.sha256(report["projection"].encode()).hexdigest() == report["request_hash"]
    assert hashlib.sha256(report["envelope"].encode()).hexdigest() == report["envelope_sha256"]
    inventory = [
        {
            "module": module,
            "sha256": hashlib.sha256(
                (
                    package / (module.removeprefix("aegis_alpha.").replace(".", "/") + ".py")
                ).read_bytes()
            ).hexdigest(),
        }
        for module in sorted(report["modules"])
    ]
    # Independent stdlib encoding of the specified preimage, not the product hash helper.
    preimage = json.dumps(
        {
            "schema": "aas-calculation-sources-v1",
            "hash_format": "aas-canonical-json-sha256-v1",
            "files": inventory,
        },
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert json.loads(report["projection"])["engine"]["calculation_source_hash"] == (
        hashlib.sha256(preimage).hexdigest()
    )
    return report


def test_source_bytes_distinguish_preparation_and_preserve_old_content(tmp_path: Path) -> None:
    body = identity_fixture(tmp_path)
    payload = {"request": body}
    baseline_package = source_copy(tmp_path, "baseline")
    baseline = probe(tmp_path, baseline_package, "baseline", payload)
    assert probe(tmp_path, baseline_package, "baseline-repeat", payload) == baseline
    assert json.loads(baseline["envelope"])["targets"] == {
        "2026-01-29": {"ASSET_B": 1.0},
        "2026-02-26": {"ASSET_B": 1.0},
    }
    assert baseline["result"]["result"]["nav"][-1]["equity"] == pytest.approx(2000 / 11)
    variants = {
        "application-target": (
            "application/backtest_prepare.py",
            "result[instrument] = result.get(instrument, 0.0) + weight",
            "result[instrument] = result.get(instrument, 0.0) + weight / 2",
        ),
        "storage-revision": (
            "storage/market.py",
            "heads[record_id] = row",
            "heads.setdefault(record_id, row)",
        ),
        "included-control": ("engine/schedule.py", "", "\n# source-byte control\n"),
        "application-control": (
            "application/backtest_prepare.py",
            "",
            "\n# source-byte control\n",
        ),
    }
    reports = {}
    for name, (filename, before, after) in variants.items():
        package = source_copy(tmp_path, name)
        path = package / filename
        source = path.read_text()
        if before:
            assert source.count(before) == 1
            path.write_text(source.replace(before, after))
        else:
            path.write_text(source + after)
        report = probe(tmp_path, package, name, payload)
        reports[name] = report
        assert {
            key for key in baseline["sources"] if baseline["sources"][key] != report["sources"][key]
        } == {filename}
        assert report["modules"] == baseline["modules"]
        assert (
            json.loads(report["envelope"])["source_pins"]
            == (json.loads(baseline["envelope"])["source_pins"])
        )
    assert json.loads(reports["application-target"]["envelope"])["targets"] == {
        "2026-01-29": {"ASSET_B": 0.5},
        "2026-02-26": {"ASSET_B": 0.5},
    }
    assert reports["application-target"]["result"]["result"]["nav"][-1]["equity"] == (
        pytest.approx(50 + 1000 / 11)
    )
    assert json.loads(reports["storage-revision"]["envelope"])["targets"] == {
        "2026-01-29": {"ASSET_A": 1.0},
        "2026-02-26": {"ASSET_B": 1.0},
    }
    assert reports["storage-revision"]["result"]["result"]["nav"][-1]["equity"] == pytest.approx(80)
    for name in ("included-control", "application-control"):
        assert reports[name]["envelope"] == baseline["envelope"]
        assert reports[name]["result"] == baseline["result"]
    changed = {
        name: (
            report["request_hash"] != baseline["request_hash"]
            and json.loads(report["projection"])["engine"]["calculation_source_hash"]
            != json.loads(baseline["projection"])["engine"]["calculation_source_hash"]
        )
        for name, report in reports.items()
    }
    assert changed == dict.fromkeys(variants, True)

    reordered = json.loads(json.dumps(body))
    reordered["metadata"]["created_at_us"] = 2
    for field in ("bindings", "refs", "price_inputs"):
        reordered[field].reverse()
    reordered = dict(reversed(list(reordered.items())))
    assert probe(tmp_path, baseline_package, "reordered", {"request": reordered}) == baseline

    # Recreate the historical 27-file identity using only this test's synthetic inputs.
    old_package = source_copy(tmp_path, "historical")
    path = old_package / "application/backtest_prepare.py"
    source = path.read_text()
    assignment = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "CALCULATION_MODULES"
            for target in node.targets
        )
    )
    old_modules = tuple(
        name
        for name in CALCULATION_MODULES
        if name.startswith(("aegis_alpha.engine.", "aegis_alpha.data."))
    )
    lines = source.splitlines(keepends=True)
    lines[assignment.lineno - 1 : assignment.end_lineno] = [
        f"CALCULATION_MODULES = {old_modules!r}\n"
    ]
    path.write_text("".join(lines))
    old = probe(tmp_path, old_package, "historical", payload)
    assert old["envelope"] == baseline["envelope"]
    assert old["result"] == baseline["result"]
    assert old["request_hash"] != baseline["request_hash"]
    current = probe(
        tmp_path,
        baseline_package,
        "old-content-read",
        {"request": body, "old_projection": old["projection"]},
    )
    assert current.pop("old_projection_read") is True
    assert current == {
        key: value for key, value in baseline.items() if key != "old_projection_read"
    }
