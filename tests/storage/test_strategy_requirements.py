"""Registered reader boundary using real stores and independently authored values."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from aegis_alpha.engine.errors import BundleIdentityError
from aegis_alpha.engine.models import DerivedInputBinding, EngineContract
from aegis_alpha.engine.requirements import InputRequirement
from aegis_alpha.storage.input_pins import ConventionPin, read_convention, register_convention
from aegis_alpha.storage.strategies import LineageSpec, load_strategy
from aegis_alpha.storage.strategy_import import register_strategy
from aegis_alpha.storage.strategy_requirements import read_execution_definition
from aegis_alpha.storage.workspace import Workspace, initialize, open_workspace
from tests.engine.engine_support import contract, raw_bundle
from tests.engine.test_requirements import rich_contract

A = (
    b'{"hash_format":"aas-canonical-json-sha256-v1","id":"synthetic-capital","kind":"basis",'
    b'"payload":{"price_basis":"capital","schema":"aas-basis-v1"},'
    b'"schema":"aas-convention-v1","version":"1"}'
)
B = (
    b'{"hash_format":"aas-canonical-json-sha256-v1","id":"synthetic-total-return","kind":"basis",'
    b'"payload":{"price_basis":"total_return","schema":"aas-basis-v1"},'
    b'"schema":"aas-convention-v1","version":"1"}'
)
A_PIN = ConventionPin(
    "basis",
    "synthetic-capital",
    "1",
    "df91bada6689f62f25a63826bd87597ba8846eec804855f8380d0909077a68b6",
)
B_PIN = ConventionPin(
    "basis",
    "synthetic-total-return",
    "1",
    "1a4b88d3126bbad00ce087aee34a5467abc01b23e04320d909f9e75d8dc23d5f",
)
ROLES = ("calendar", "basis", "cost", "execution")
SELECT_ROWS = (
    "SELECT strategy_id,version,role,ordinal,required_schema,required_field,domain,"
    "warmup,basis,cadence FROM strategy_requirements ORDER BY strategy_id,version,role,ordinal"
)
EXPECTED_ROWS = [
    (
        "synthetic-probe",
        "1",
        "prices",
        1,
        "engine-price-v1",
        "close",
        "prices",
        3,
        "explicit-input",
        "calendar_month_end",
    ),
    (
        "synthetic-rich",
        "1",
        "macro",
        1,
        "engine-macro-v1",
        "MACRO_Y",
        "macro_observations",
        2,
        "not_applicable",
        "calendar_month_end",
    ),
    (
        "synthetic-rich",
        "1",
        "macro",
        2,
        "engine-macro-v1",
        "YIELD_Y",
        "macro_observations",
        1,
        "not_applicable",
        "calendar_month_end",
    ),
    (
        "synthetic-rich",
        "1",
        "prices",
        1,
        "engine-price-v1",
        "close",
        "prices",
        8,
        "explicit-input",
        "calendar_month_end",
    ),
]


def register_fixture(  # noqa: PLR0913 -- explicit fixture identity and optional lineage
    workspace: Workspace,
    source: Path,
    value: EngineContract,
    identity: str,
    version: str = "1",
    *,
    lineage: LineageSpec | None = None,
) -> str:
    document = json.loads(raw_bundle(value))
    document.update(bundle_id=identity, bundle_version=version)
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    source.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    register_strategy(workspace, source, digest, identity, version, lineage=lineage)
    source.unlink()
    return digest


@pytest.fixture
def stored(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    home = tmp_path / "aas"
    initialize(home)
    with open_workspace(home, writable=True, strategy_write=True) as ws:
        digests = {
            identity: register_fixture(ws, tmp_path / "input.json", value, identity)
            for identity, value in (
                ("synthetic-probe", contract()),
                ("synthetic-rich", rich_contract()),
            )
        }
        for raw, pin in ((A, A_PIN), (B, B_PIN)):
            assert (
                register_convention(
                    ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
                )
                == pin
            )
    return home, digests


@contextmanager
def select_only(*connections: sqlite3.Connection) -> Iterator[list[str]]:
    trace: list[str] = []
    changes = [connection.total_changes for connection in connections]

    def authorize(action: int, *_args: str | None) -> int:
        return (
            sqlite3.SQLITE_OK
            if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ)
            else sqlite3.SQLITE_DENY
        )

    for connection in connections:
        connection.set_authorizer(authorize)
        connection.set_trace_callback(trace.append)
    try:
        yield trace
    finally:
        for connection in connections:
            connection.set_authorizer(None)
            connection.set_trace_callback(None)
        assert [connection.total_changes for connection in connections] == changes
        assert all(statement.lstrip().upper().startswith("SELECT") for statement in trace)


@pytest.mark.parametrize("bindings", [None, ()])
def test_two_stored_definitions_preserve_independent_requirements(
    stored: tuple[Path, dict[str, str]],
    bindings: tuple[ConventionPin, ...] | None,
) -> None:
    home, digests = stored
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with select_only(ws.strategies, ws.state) as trace:
            base = read_execution_definition(
                ws.strategies,
                "synthetic-probe",
                "1",
                digests["synthetic-probe"],
                bindings,
                state_connection=ws.state,
            )
            rich = read_execution_definition(
                ws.strategies, "synthetic-rich", "1", digests["synthetic-rich"], bindings
            )
            assert [tuple(row) for row in ws.strategies.execute(SELECT_ROWS)] == EXPECTED_ROWS
        assert not any("FROM conventions" in statement for statement in trace)
    assert base.input_requirements == (
        InputRequirement("prices", ("ASSET_A", "ASSET_B", "REF_X"), 3),
        InputRequirement(
            "membership",
            ("ensemble:8334c78ba32edf7e826bf3e111fcdabf5375e1a3855b41d3611f0c022adcb57b",),
        ),
    )
    assert rich.input_requirements == (
        InputRequirement("prices", ("ASSET_Z", "CANARY_ON", "REF_Y"), 8),
        InputRequirement("macro", ("MACRO_Y",), lag_months=(2, 0), lag_combination="OR"),
        InputRequirement(
            "derived",
            ("YIELD_Y",),
            lag_months=(1,),
            trailing_months=4,
            input_bindings=(
                DerivedInputBinding("synthetic-prices", "2", "PRICE_Y", "price"),
                DerivedInputBinding("synthetic-flows", "3", "FLOW_Y", "addend_a"),
            ),
            basis="capital",
        ),
        InputRequirement("membership", ("ensemble:" + "b" * 64,)),
    )
    for definition, value in ((base, contract()), (rich, rich_contract())):
        assert definition.bundle_version == "1"
        assert definition.source_sha256 == digests[definition.bundle_id]
        expected_contract = json.loads(raw_bundle(value))["contract"]
        assert (
            definition.contract_sha256
            == hashlib.sha256(
                json.dumps(expected_contract, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        assert (
            definition.required_convention_roles == definition.unresolved_convention_roles == ROLES
        )
        assert definition.executable is False
    assert (base.cash_asset_ids, rich.cash_asset_ids) == (("CASH_X",), ("CASH_Y",))
    assert rich.asset_ids == ("ASSET_Z", "CANARY_ON", "DEF_Y", "REF_Y")


@pytest.mark.parametrize(
    ("identity", "pin", "basis"),
    [
        ("synthetic-probe", A_PIN, "capital"),
        ("synthetic-probe", B_PIN, "total_return"),
        ("synthetic-rich", A_PIN, "capital"),
    ],
)
def test_compatible_basis_changes_only_returned_requirements(
    stored: tuple[Path, dict[str, str]],
    identity: str,
    pin: ConventionPin,
    basis: str,
) -> None:
    home, digests = stored
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with select_only(ws.strategies, ws.state):
            original = read_execution_definition(ws.strategies, identity, "1", digests[identity])
            result = read_execution_definition(
                ws.strategies, identity, "1", digests[identity], (pin,), state_connection=ws.state
            )
            assert result == replace(
                original,
                input_requirements=(
                    replace(original.input_requirements[0], basis=basis),
                    *original.input_requirements[1:],
                ),
                unresolved_convention_roles=("calendar", "cost", "execution"),
            )
            assert [tuple(row) for row in ws.strategies.execute(SELECT_ROWS)] == EXPECTED_ROWS
            assert (
                load_strategy(ws.strategies, identity, "1", digests[identity]).source_sha256
                == digests[identity]
            )


def test_valid_total_return_pin_is_incompatible_only_with_capital_consumer(
    stored: tuple[Path, dict[str, str]],
) -> None:
    home, digests = stored
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with select_only(ws.strategies, ws.state):
            assert read_convention(ws.state, B_PIN) == B
            assert (
                load_strategy(
                    ws.strategies, "synthetic-rich", "1", digests["synthetic-rich"]
                ).contract
                == rich_contract()
            )
            with pytest.raises(ValueError, check=lambda error: type(error) is ValueError) as caught:
                read_execution_definition(
                    ws.strategies,
                    "synthetic-rich",
                    "1",
                    digests["synthetic-rich"],
                    (B_PIN,),
                    state_connection=ws.state,
                )
            assert "YIELD_Y" in str(caught.value)
            assert [tuple(row) for row in ws.strategies.execute(SELECT_ROWS)] == EXPECTED_ROWS


@pytest.mark.parametrize(
    ("bindings", "error"),
    [([A_PIN], TypeError), (("basis",), TypeError), ((A_PIN, A_PIN), ValueError)],
)
def test_malformed_or_duplicate_bindings_fail(
    stored: tuple[Path, dict[str, str]], bindings: tuple[ConventionPin, ...], error: type[Exception]
) -> None:
    home, digests = stored
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with select_only(ws.strategies, ws.state), pytest.raises(error):
            read_execution_definition(
                ws.strategies,
                "synthetic-probe",
                "1",
                digests["synthetic-probe"],
                bindings,
                state_connection=ws.state,
            )


@pytest.mark.parametrize("kind", ["calendar", "cost", "execution", "fx", "benchmark", "risk_free"])
def test_opaque_roles_are_verified_not_semantically_resolved(
    stored: tuple[Path, dict[str, str]], kind: str
) -> None:
    home, digests = stored
    raw = json.dumps(
        {
            "schema": "aas-convention-v1",
            "hash_format": "aas-canonical-json-sha256-v1",
            "kind": kind,
            "id": "opaque",
            "version": "1",
            "payload": {"schema": "synthetic-opaque-v1"},
        }
    ).encode()
    with open_workspace(home, writable=True) as ws:
        pin = register_convention(
            ws.state, raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()
        )
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with select_only(ws.strategies, ws.state):
            if kind in ("fx", "benchmark", "risk_free"):
                with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
                    read_execution_definition(
                        ws.strategies,
                        "synthetic-probe",
                        "1",
                        digests["synthetic-probe"],
                        (pin,),
                        state_connection=ws.state,
                    )
            else:
                result = read_execution_definition(
                    ws.strategies,
                    "synthetic-probe",
                    "1",
                    digests["synthetic-probe"],
                    (pin, A_PIN),
                    state_connection=ws.state,
                )
                assert result.unresolved_convention_roles == ("calendar", "cost", "execution")
                assert result.executable is False
                with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
                    read_execution_definition(
                        ws.strategies,
                        "synthetic-probe",
                        "1",
                        digests["synthetic-probe"],
                        (replace(pin, hash="0" * 64),),
                        state_connection=ws.state,
                    )


@pytest.mark.parametrize("state_mode", ["absent", "strategies"])
def test_nonempty_binding_requires_explicit_state_store(
    stored: tuple[Path, dict[str, str]], state_mode: str
) -> None:
    home, digests = stored
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        state = None if state_mode == "absent" else ws.strategies
        error = ValueError if state is None else sqlite3.OperationalError
        with select_only(ws.strategies, ws.state), pytest.raises(error):
            read_execution_definition(
                ws.strategies,
                "synthetic-probe",
                "1",
                digests["synthetic-probe"],
                (A_PIN,),
                state_connection=state,
            )


@pytest.mark.parametrize(
    ("identity", "version", "digest"),
    [("missing", "1", None), ("synthetic-rich", "2", None), ("synthetic-rich", "1", "0" * 64)],
)
def test_wrong_strategy_pin_is_rejected(
    stored: tuple[Path, dict[str, str]], identity: str, version: str, digest: str | None
) -> None:
    home, digests = stored
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with (
            select_only(ws.strategies, ws.state),
            pytest.raises(ValueError, check=lambda error: type(error) is ValueError),
        ):
            read_execution_definition(
                ws.strategies, identity, version, digest or digests["synthetic-rich"]
            )


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE strategy_versions SET raw_bundle=x'7b7d'",
        "UPDATE strategy_versions SET raw_sha256='" + "0" * 64 + "'",
        "UPDATE strategy_versions SET contract_json='{}'",
        "UPDATE strategy_versions SET contract_sha256='" + "0" * 64 + "'",
        (
            "UPDATE strategy_requirements SET strategy_id='synthetic-probe'"
            " WHERE role='macro' AND ordinal=1"
        ),
        "UPDATE strategy_requirements SET version='2' WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET role='other' WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET ordinal=3 WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET required_schema='other' WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET required_field='OTHER' WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET domain='other' WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET warmup=9 WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET basis='other' WHERE role='macro' AND ordinal=1",
        "UPDATE strategy_requirements SET cadence='other' WHERE role='macro' AND ordinal=1",
        "DELETE FROM strategy_requirements WHERE role='macro' AND ordinal=1",
        (
            "INSERT INTO strategy_requirements SELECT strategy_id,version,'extra',ordinal,"
            "required_schema,required_field,domain,warmup,basis,cadence "
            "FROM strategy_requirements WHERE role='prices'"
        ),
    ],
)
def test_copied_store_tampering_is_rejected(
    stored: tuple[Path, dict[str, str]], tmp_path: Path, mutation: str
) -> None:
    home, digests = stored
    copied = tmp_path / "tampered"
    shutil.copytree(home, copied)
    with closing(sqlite3.connect(copied / "strategies.sqlite3")) as connection:
        triggers = connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND "
            "(tbl_name='strategy_versions' OR tbl_name='strategy_requirements')"
        ).fetchall()
        for name, _sql in triggers:
            connection.execute('DROP TRIGGER "' + name + '"')
        connection.execute(mutation)
        for _name, sql in triggers:
            connection.execute(sql)
        connection.commit()
    error = (
        BundleIdentityError
        if mutation == "UPDATE strategy_versions SET raw_bundle=x'7b7d'"
        else ValueError
    )
    with open_workspace(copied) as ws:
        assert ws.strategies is not None
        with select_only(ws.strategies, ws.state), pytest.raises(error):
            read_execution_definition(
                ws.strategies, "synthetic-rich", "1", digests["synthetic-rich"]
            )
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        assert [tuple(row) for row in ws.strategies.execute(SELECT_ROWS)] == EXPECTED_ROWS


def test_late_parent_cannot_resolve_immutable_lineage(
    stored: tuple[Path, dict[str, str]], tmp_path: Path
) -> None:
    home, _digests = stored
    lineage = LineageSpec("synthetic-parent", "7", "derived", "synthetic reason")
    with open_workspace(home, writable=True, strategy_write=True) as ws:
        child1 = register_fixture(
            ws, tmp_path / "child.json", contract(), "synthetic-child", lineage=lineage
        )
        register_fixture(ws, tmp_path / "parent.json", contract(), "synthetic-parent", "7")
        child2 = register_fixture(
            ws, tmp_path / "child.json", contract(), "synthetic-child", "2", lineage=lineage
        )
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with select_only(ws.strategies, ws.state):
            with pytest.raises(ValueError, check=lambda error: type(error) is ValueError):
                read_execution_definition(
                    ws.strategies,
                    "synthetic-child",
                    "1",
                    child1,
                    (A_PIN,),
                    state_connection=ws.state,
                )
            result = read_execution_definition(ws.strategies, "synthetic-child", "2", child2)
            assert (
                result.bundle_id,
                result.bundle_version,
                result.source_sha256,
                result.executable,
            ) == ("synthetic-child", "2", child2, False)
            assert [
                tuple(row)
                for row in ws.strategies.execute(
                    "SELECT version,parent_version,parent_status "
                    "FROM strategy_lineage ORDER BY version"
                )
            ] == [("1", "7", "unresolved"), ("2", "7", "resolved")]


def test_fresh_process_reads_two_definitions_after_source_deletion(
    stored: tuple[Path, dict[str, str]],
) -> None:
    home, digests = stored
    child = subprocess.run(  # noqa: S603 -- fixed interpreter/code and owned synthetic home
        [
            sys.executable,
            "-c",
            """
import json, sys
from pathlib import Path
from aegis_alpha.storage.workspace import open_workspace
from aegis_alpha.storage.strategy_requirements import read_execution_definition
from tests.storage.test_strategy_requirements import select_only
with open_workspace(Path(sys.argv[1])) as ws:
    assert ws.strategies is not None
    with select_only(ws.strategies, ws.state):
        pins = zip(("synthetic-probe", "synthetic-rich"), sys.argv[2:], strict=True)
        result = [read_execution_definition(ws.strategies, identity, "1", digest)
                  for identity, digest in pins]
print(json.dumps([(r.bundle_id, r.price_asset_ids, r.cash_asset_ids,
                   r.unresolved_convention_roles, r.executable) for r in result]))
""",
            str(home),
            digests["synthetic-probe"],
            digests["synthetic-rich"],
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert json.loads(child.stdout) == [
        ["synthetic-probe", ["ASSET_A", "ASSET_B", "REF_X"], ["CASH_X"], list(ROLES), False],
        ["synthetic-rich", ["ASSET_Z", "CANARY_ON", "REF_Y"], ["CASH_Y"], list(ROLES), False],
    ]


@pytest.mark.parametrize(
    "pin",
    [replace(A_PIN, hash="0" * 64), replace(A_PIN, version="2"), replace(A_PIN, id="missing")],
)
def test_unverified_basis_reference_cannot_resolve_role(
    stored: tuple[Path, dict[str, str]], pin: ConventionPin
) -> None:
    home, digests = stored
    with open_workspace(home) as ws:
        assert ws.strategies is not None
        with (
            select_only(ws.strategies, ws.state),
            pytest.raises(ValueError, check=lambda error: type(error) is ValueError),
        ):
            read_execution_definition(
                ws.strategies,
                "synthetic-probe",
                "1",
                digests["synthetic-probe"],
                (pin,),
                state_connection=ws.state,
            )
