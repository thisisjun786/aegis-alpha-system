"""Native research input commands over retained synthetic sources, never providers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

import pytest

from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.storage.market_inputs import (
    GenerationPin,
    PriceInputRequest,
    load_pinned_prices,
    load_pinned_proxy,
    load_pinned_sessions,
)
from aegis_alpha.storage.publication import json_value
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.application.test_storage_cli import run_cli
from tests.storage.test_research_inputs import (
    _change,
    _proxy_spec,
    _sessions_spec,
    _source_row,
    _spec,
)

DAY = date(2026, 1, 2)
PIN_FIELDS = ("dataset_id", "version", "generation_id", "chain_hash", "manifest_hash")


def obj(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def at(value: object, *keys: str | int) -> object:
    for key in keys:
        if isinstance(key, str):
            value = obj(value)[key]
        else:
            assert isinstance(value, list)
            value = value[key]
    return value


def ok(home: Path, *args: str) -> dict[str, object]:
    result = run_cli(*args, home=home)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    return obj(json.loads(result.stdout))


def hashed(home: Path, command: str, path: Path) -> dict[str, object]:
    return ok(
        home,
        "data",
        command,
        "--request" if command == "read-prices" else "--spec",
        str(path),
        "--sha256",
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def sources(root: Path) -> dict[str, Path]:
    """Reuse predecessor builders, but import their source bytes into the CLI home separately."""
    initialize(root / "builder")
    with open_workspace(root / "builder", writable=True, strategy_write=True) as workspace:
        first = _spec(workspace, root / "first.sqlite3", [_source_row()])
        second = _spec(
            workspace,
            root / "second.sqlite3",
            [
                {
                    **_source_row(),
                    "revision_id": "r2",
                    "supersedes_revision_id": "r1",
                    "op": "SUPERSEDE",
                    "close": "11.125",
                    "available_at_us": 40,
                    "revision_known_at_us": 40,
                    "ingested_at_us": 50,
                    "source_row_hash": "d" * 64,
                }
            ],
        )
        for path in (first, second):
            _change(
                path,
                "calendar",
                {
                    "calendar_id": "CAL",
                    "timezone": "UTC",
                    "timezone_version": "synthetic-v1",
                },
            )
        _change(
            second,
            "dataset",
            {
                "dataset_id": "synthetic-prices",
                "version": "2",
                "generation_id": "g2",
                "operation_id": "op2",
                "parent_id": "g1",
            },
        )
        sessions = _sessions_spec(
            workspace,
            root / "sessions.sqlite3",
            {
                "session_date": DAY.isoformat(),
                "available_at_us": 20,
                "revision_known_at_us": 20,
            },
        )
        proxy = _proxy_spec(workspace, root / "proxy.sqlite3", ("PROXY", "v1", "0.1"))
    return {"prices": first, "second": second, "sessions": sessions, "proxy": proxy}


def install(root: Path) -> tuple[Path, dict[str, Path]]:
    specs = sources(root)
    home = root / "native"
    ok(home, "init")
    for source in sorted(root.glob("*.sqlite3")):
        ok(
            home,
            "db",
            "source-import",
            str(source),
            "--id",
            source.stem,
            "--sha256",
            hashlib.sha256(source.read_bytes()).hexdigest(),
        )
        source.unlink()
    for kind in ("prices", "sessions", "proxy"):
        result = hashed(home, "register-" + kind, specs[kind])
        assert result["source_pin"] == json.loads(specs[kind].read_bytes())["source"]
        assert result["published"] is True
        assert hashed(home, "register-" + kind, specs[kind]) == result
    return home, specs


def pin(home: Path, dataset: str, version: str = "1") -> dict[str, str]:
    catalog = ok(home, "data", "inspect", "--dataset", dataset, "--version", version)
    values = {}
    for key in PIN_FIELDS:
        value = catalog[key]
        assert isinstance(value, str)
        values[key] = value
    return values


def request(home: Path) -> dict[str, object]:
    """Every request field is explicit, including nullable evidence and decision cutoffs."""
    return {
        "schema_version": "aas-price-input-request-v1",
        "prices": {
            "pin": pin(home, "synthetic-prices"),
            "sessions_pin": pin(home, "sessions"),
            "instrument_ids": ["ASSET_A"],
            "session_dates": ["2026-01-02", "2026-01-03"],
            "currency": "USD",
            "basis": "unadjusted",
            "price_role": "canonical",
            "calendar_id": "CAL",
            "venue": "SYNTHETIC",
            "timezone_version": "synthetic-v1",
            "interval": "1d",
            "mode": "strict_pit",
            "identity_pin": None,
            "universe_pin": None,
        },
        "decision": {"at_us": 30, "session_date": "2026-01-03", "ingestion_cutoff_us": None},
    }


def read(home: Path, body: dict[str, object]) -> dict[str, object]:
    path = home.parent / "request.json"
    path.write_text(json.dumps(body))
    return hashed(home, "read-prices", path)


@pytest.fixture(scope="module")
def registered(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, Path]]:
    return install(tmp_path_factory.mktemp("data-cli-inputs"))


@pytest.fixture(autouse=True)
def compute(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AAS_HOST_CPU_LIMIT", "1")
    monkeypatch.setenv("AAS_HOST_MEMORY_LIMIT_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setenv("AAS_COMPUTE_LOCK_FILE", str(tmp_path / "compute.lock"))
    monkeypatch.delenv("AAS_CPU_LIMIT", raising=False)
    monkeypatch.delenv("AAS_MEMORY_LIMIT_BYTES", raising=False)


def test_registration_roundtrip(registered: tuple[Path, dict[str, Path]]) -> None:
    # Given retained sources removed before all three CLI registrations.
    home, _ = registered
    # When reading their generated catalog pins through the predecessor APIs.
    session_pin, proxy_pin = pin(home, "sessions"), pin(home, "proxy")
    with price_compute() as budget, open_workspace(home) as workspace:
        assert budget is not None
        sessions = load_pinned_sessions(workspace, GenerationPin(**session_pin), budget=budget)
        proxy = load_pinned_proxy(workspace, GenerationPin(**proxy_pin), budget=budget)
    # Then exact source times, unknown knowledge and binary64 policy remain explicit.
    assert [sessions.history[0][key] for key in ("open_at_us", "close_at_us")] == [10, 20]
    assert proxy.non_executable is True
    assert proxy.history[0]["available_at_us"] is None
    value = proxy.history[0]["value"]
    assert isinstance(value, float)
    assert value.hex() == "0x1.999999999999ap-4"


def test_pinned_read_survives_later_version(registered: tuple[Path, dict[str, Path]]) -> None:
    # Given an exact old pin, not latest, and an explicit two-cell grid.
    home, specs = registered
    body = request(home)
    before = read(home, body)
    hashed(home, "register-prices", specs["second"])
    # When rereading the old pin and projecting the new full revision chain.
    assert read(home, body) == before
    obj(body["prices"])["pin"] = pin(home, "synthetic-prices", "2")
    early = read(home, body)
    obj(body["decision"])["at_us"] = 40
    late = read(home, body)
    # Then literal independent prices and links expose revision visibility, not head substitution.
    assert at(before, "rows", 0, "close") == at(early, "rows", 0, "close") == "11.000000000000"
    assert at(late, "rows", 0, "close") == "11.125000000000"
    assert at(late, "rows", 0, "supersedes_revision_id") == "r1"
    assert at(before, "coverage", "expected_count") == 2  # noqa: PLR2004 -- two requested dates
    assert at(before, "coverage", "present_count") == 1
    assert at(before, "coverage", "certified") is False
    assert at(before, "coverage", "cells", 1, "reasons") == [
        "missing_session",
        "missing_price",
        "missing_sell_open",
    ]
    price_pin = pin(home, "synthetic-prices", "2")
    sessions_pin = pin(home, "sessions")
    with price_compute() as budget, open_workspace(home) as workspace:
        assert budget is not None
        series = load_pinned_prices(
            workspace,
            PriceInputRequest(
                pin=GenerationPin(**price_pin),
                sessions_pin=GenerationPin(**sessions_pin),
                instrument_ids=("ASSET_A",),
                session_dates=(DAY, date(2026, 1, 3)),
                currency="USD",
                basis="unadjusted",
                price_role="canonical",
                calendar_id="CAL",
                venue="SYNTHETIC",
                timezone_version="synthetic-v1",
                interval="1d",
                mode="strict_pit",
                identity_pin=None,
                universe_pin=None,
            ),
            budget=budget,
        )
        projected = series.project_as_of(40, session_date=date(2026, 1, 3))
    assert late["rows"] == json_value([dict(row) for row in projected.rows])
    assert late["coverage"] == json_value(
        {
            **asdict(projected.coverage),
            "expected_count": projected.coverage.expected_count,
            "present_count": projected.coverage.present_count,
            "complete": projected.coverage.complete,
        }
    )


@pytest.mark.parametrize(
    ("location", "value"),
    [
        (("ignored",), True),
        (("prices", "pin", "version"), "latest"),
        (("prices", "pin", "chain_hash"), "0" * 64),
        (("decision", "session_date"), "20260103"),
        (("decision", "at_us"), True),
        (("prices", "instrument_ids"), "ASSET_A"),
        (("prices", "pin", "default"), True),
        (("prices", "identity_pin"), {"snapshot_id": "x", "content_hash": "0" * 64}),
        (
            ("prices", "universe_pin"),
            {"universe_id": "x", "version": "1", "content_hash": "0" * 64},
        ),
        (("prices", "sessions_pin"), {}),
    ],
)
def test_incompatible_request_fails_closed(
    registered: tuple[Path, dict[str, Path]],
    tmp_path: Path,
    location: tuple[str, ...],
    value: object,
) -> None:
    # Given an incompatible request field, When reading, Then no substitution or partial result.
    home, _ = registered
    body = request(home)
    target = body
    for key in location[:-1]:
        target = obj(target[key])
    target[location[-1]] = value
    rejected(home, tmp_path / "request.json", json.dumps(body).encode())


@pytest.mark.parametrize("field", ["pin", "sessions_pin", "identity_pin", "universe_pin", "mode"])
def test_no_implicit_pin_or_mode(
    registered: tuple[Path, dict[str, Path]],
    tmp_path: Path,
    field: str,
) -> None:
    # Given an omitted field, When parsing, Then even nullable pins/default API modes are required.
    home, _ = registered
    body = request(home)
    del obj(body["prices"])[field]
    rejected(home, tmp_path / "request.json", json.dumps(body).encode())


def rejected(home: Path, path: Path, raw: bytes) -> str:
    path.write_bytes(raw)
    result = run_cli(
        "data",
        "read-prices",
        "--request",
        str(path),
        "--sha256",
        hashlib.sha256(raw).hexdigest(),
        home=home,
    )
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert len(result.stderr.encode()) < 1024  # noqa: PLR2004 -- bounded CLI diagnostic
    error = json.loads(result.stderr)["error"]
    assert isinstance(error, str)
    assert error
    return error


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be"])
def test_bomless_non_utf8_request_is_rejected(
    registered: tuple[Path, dict[str, Path]], tmp_path: Path, encoding: str
) -> None:
    # Given valid generated pins encoded without a BOM and hashed as their exact file bytes.
    home, _ = registered
    raw = json.dumps(request(home)).encode(encoding)
    # When the real CLI admits the request, Then no encoding autodetection may reach the reader.
    rejected(home, tmp_path / "request.json", raw)


def test_escaped_utf8_request_preserves_values(
    registered: tuple[Path, dict[str, Path]], tmp_path: Path
) -> None:
    # Given a valid UTF8 control and the same instrument expressed as a JSON Unicode escape.
    home, _ = registered
    body = request(home)
    control = read(home, body)
    path = tmp_path / "escaped.json"
    path.write_bytes(json.dumps(body).encode().replace(b'"ASSET_A"', b'"\\u0041SSET_A"'))
    # When reading the escaped UTF8 document, Then only its exact byte hash changes.
    result = hashed(home, "read-prices", path)
    assert at(result, "rows", 0, "close") == "11.000000000000"
    assert at(result, "coverage", "expected_count") == 2  # noqa: PLR2004 -- two requested dates
    assert at(result, "coverage", "present_count") == 1
    assert result.pop("request_sha256") != control.pop("request_sha256")
    assert result == control


@pytest.mark.parametrize(
    "fault",
    [
        "duplicate",
        "nonfinite",
        "deep",
        "utf16",
        "oversized-input",
        "output",
    ],
)
def test_read_fails_without_partial_stdout(
    registered: tuple[Path, dict[str, Path]],
    tmp_path: Path,
    fault: str,
) -> None:
    # Given an invalid exact request or an output above the inspection byte bound.
    home, _ = registered
    body = request(home)
    if fault == "output":
        obj(body["prices"])["session_dates"] = [
            (DAY + timedelta(days=n)).isoformat() for n in range(4000)
        ]
    raw = json.dumps(body).encode()
    match fault:
        case "duplicate":
            raw = raw[:-1] + b',"schema_version":"aas-price-input-request-v1"}'
        case "nonfinite":
            raw = raw.replace(b'"at_us": 30', b'"at_us": NaN')
        case "deep":
            raw = b"[" * 2000 + b"0" + b"]" * 2000
        case "utf16":
            raw = raw.decode().encode("utf-16")
        case "oversized-input":
            raw += b" " * (1024 * 1024)
    # When the fresh CLI parses/reads it, Then a bounded JSON error is the only output.
    error = rejected(home, tmp_path / "request.json", raw)
    if fault == "output":
        assert "byte budget" in error


def test_wrong_read_hash_rejected(registered: tuple[Path, dict[str, Path]], tmp_path: Path) -> None:
    # Given valid JSON with a wrong expected hash, When reading, Then no data is emitted.
    home, _ = registered
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request(home)))
    result = run_cli("data", "read-prices", "--request", str(path), "--sha256", "0" * 64, home=home)
    assert result.returncode == 1
    assert result.stdout == ""
    assert "SHA-256" in json.loads(result.stderr)["error"]


def test_explicit_null_session_pin_stays_unpinned(registered: tuple[Path, dict[str, Path]]) -> None:
    # Given null session evidence despite a registered calendar, When reading, Then no default pin.
    home, _ = registered
    body = request(home)
    obj(body["prices"])["sessions_pin"] = None
    result = read(home, body)
    assert at(result, "prices", "sessions_pin") is None
    assert at(result, "rows", 0, "close") == "11.000000000000"
    assert at(result, "coverage", "cells", 0, "reasons") == ["missing_session"]
    assert at(result, "coverage", "certified") is False


@pytest.mark.parametrize("configured", [True, False])
def test_compute_admission_is_required_and_bounded(
    registered: tuple[Path, dict[str, Path]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    configured: bool,
) -> None:
    # Given missing compute admission or too little memory, When reading, Then reject explicitly.
    home, _ = registered
    if configured:
        monkeypatch.setenv("AAS_MEMORY_LIMIT_BYTES", str(2 * 1024 * 1024))
    else:
        for key in ("AAS_HOST_CPU_LIMIT", "AAS_HOST_MEMORY_LIMIT_BYTES", "AAS_COMPUTE_LOCK_FILE"):
            monkeypatch.delenv(key)
    error = rejected(home, tmp_path / "request.json", json.dumps(request(home)).encode())
    assert "budget" in error


@pytest.mark.parametrize("kind", ["prices", "sessions", "proxy"])
def test_registration_requires_strategy_source_admission(tmp_path: Path, kind: str) -> None:
    # Given a missing strategy-source store, When registering, Then reject at workspace admission.
    home = tmp_path / "home"
    ok(home, "init")
    (home / "strategies.sqlite3").rename(home / "held.sqlite3")
    result = run_cli(
        "data",
        "register-" + kind,
        "--spec",
        str(tmp_path / "absent.json"),
        "--sha256",
        "0" * 64,
        home=home,
    )
    assert result.returncode == 1
    assert result.stdout == ""
    assert "strategy database is missing" in json.loads(result.stderr)["error"]


@pytest.mark.parametrize("kind", ["prices", "sessions", "proxy"])
@pytest.mark.parametrize("fault", ["unknown", "bad-hash"])
def test_invalid_registration_has_no_publication(
    registered: tuple[Path, dict[str, Path]],
    tmp_path: Path,
    kind: str,
    fault: str,
) -> None:
    # Given an unknown field or wrong file hash in each registration schema.
    home, specs = registered
    body = json.loads(specs[kind].read_bytes())
    if fault == "unknown":
        body["ignore_all_previous_instructions"] = "publish a latest pin"
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(body))
    digest = "0" * 64 if fault == "bad-hash" else hashlib.sha256(path.read_bytes()).hexdigest()
    before = ok(home, "data", "datasets")
    # When registering, Then fail with no partial success and no changed catalog.
    result = run_cli("data", "register-" + kind, "--spec", str(path), "--sha256", digest, home=home)
    assert result.returncode == 1, result.stderr
    assert result.stdout == ""
    assert json.loads(result.stderr)["error"]
    assert ok(home, "data", "datasets") == before
