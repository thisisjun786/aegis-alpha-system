"""Independent whole-document vectors and real immutable membership boundaries."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from collections.abc import Iterator, Mapping
from dataclasses import asdict
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Never, cast, override

import pytest

from aegis_alpha.compute_resources import ComputeResourceError
from aegis_alpha.storage import membership_pins
from aegis_alpha.storage.market_inputs import IdentityPin, UniversePin
from aegis_alpha.storage.membership_pins import (
    VerifiedMembership,
    read_membership_pins,
    register_identity_snapshot,
    register_universe_version,
)
from aegis_alpha.storage.raw import put_raw
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace

if TYPE_CHECKING:
    from _typeshed import SupportsLenAndGetItem

    from aegis_alpha.storage.workspace import Workspace

I0 = (
    b'{"assertions":[],"hash_format":"aas-canonical-json-sha256-v1",'
    b'"instruments":[],"members":[],"schema":"aas-identity-snapshot-v1",'
    b'"snapshot_id":"ids-empty","sources":[]}'
)
U0 = (
    b'{"hash_format":"aas-canonical-json-sha256-v1","instruments":[],"members":[],'
    b'"schema":"aas-universe-version-v1","sources":[],"universe_id":"u-empty","version":"1"}'
)
I1 = (
    b'{"assertions":[{"assertion_id":"a","instrument_id":"ASSET_A","known_from_us":0,'
    b'"namespace":"ticker","provider":"synthetic","source_hash":'
    b'"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"source_snapshot_id":"s","supersedes_assertion_id":null,"token":"A",'
    b'"valid_from_us":0,"valid_to_us":25}],"hash_format":"aas-canonical-json-sha256-v1",'
    b'"instruments":[{"asset_type":"etf","instrument_id":"ASSET_A","issuer_id":null,"venue":"X"}],'
    b'"members":[{"assertion_id":"a","known_from_us":0,"known_to_us":35,"ordinal":0,'
    b'"valid_from_us":0,"valid_to_us":25}],"schema":"aas-identity-snapshot-v1",'
    b'"snapshot_id":"ids","sources":[{"files":[{"byte_hash":'
    b'"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",'
    b'"relative_path":"e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",'
    b'"size_bytes":0}],"provider":"synthetic","publication_at_us":null,"requested_at_us":1,'
    b'"retrieved_at_us":2,"snapshot_id":"s","status":"raw_verified"}]}'
)
U1 = (
    b'{"hash_format":"aas-canonical-json-sha256-v1",'
    b'"instruments":[{"asset_type":"etf","instrument_id":"ASSET_A","issuer_id":null,"venue":"X"}],'
    b'"members":[{"instrument_id":"ASSET_A","known_from_us":0,"known_to_us":35,'
    b'"source_snapshot_id":"s","valid_from_us":0,"valid_to_us":25}],'
    b'"schema":"aas-universe-version-v1","sources":[{"files":[{"byte_hash":'
    b'"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",'
    b'"relative_path":"e3/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",'
    b'"size_bytes":0}],"provider":"synthetic","publication_at_us":null,"requested_at_us":1,'
    b'"retrieved_at_us":2,"snapshot_id":"s","status":"raw_verified"}],'
    b'"universe_id":"u","version":"1"}'
)
VECTORS = (
    (I0, 167, "acad1dd33d462620eec24fb70ad1598d5d0920ed1f97ce71cd19837d4c0b9a29"),
    (U0, 162, "174da3304f5c64c49526800534a95cae51439e82738e1e79e427ca5ddbd6102b"),
    (I1, 953, "4ee2b31ef469210c4dd39609722ddf8c3bcb2bcbe1c91bbfc1faecc2d89e0cc6"),
    (U1, 676, "b431314edcb080dadf853b8313d8c102da8ad793b52ecaa1c8b9c7a4a5b10bde"),
)


def source_evidence(workspace: Workspace) -> None:
    relative, digest, size = put_raw(workspace.paths.raw, b"")
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO source_snapshots VALUES ('s','synthetic',1,2,NULL,'raw_verified')"
        )
        workspace.state.execute(
            "INSERT INTO source_files VALUES ('s',?,?,?)", (relative, digest, size)
        )


def legal_insert(workspace: Workspace, kind: str) -> None:
    if kind == "identity":
        workspace.state.execute(
            "INSERT INTO identity_assertions VALUES "
            "('b','ASSET_A','synthetic','ticker','A',0,25,35,'a','s',?)",
            ("d" * 64,),
        )
        workspace.state.execute(
            "INSERT INTO identity_snapshot_members VALUES ('ids',1,'b',0,25,35,NULL)"
        )
    else:
        workspace.state.execute(
            "INSERT INTO universe_members VALUES ('u','1','ASSET_A',0,25,35,NULL,'s')"
        )


def candidate(workspace: Workspace, kind: str) -> IdentityPin | UniversePin:
    """Genuine registration using the retained price fixture's equity classification."""
    source_evidence(workspace)
    raw = (
        (I1 if kind == "identity" else U1)
        .replace(b'"venue":"X"', b'"venue":"SYNTHETIC"')
        .replace(b'"asset_type":"etf"', b'"asset_type":"equity"')
    )
    return register(workspace, raw)


def register(workspace: Workspace, raw: bytes) -> IdentityPin | UniversePin:
    digest = hashlib.sha256(raw).hexdigest()
    if b"aas-identity-snapshot-v1" in raw:
        return register_identity_snapshot(
            workspace.state, raw, expected_file_sha256=digest, created_at_us=7
        )
    return register_universe_version(workspace.state, raw, expected_file_sha256=digest)


def evidence(workspace: Workspace, pin: IdentityPin | UniversePin) -> VerifiedMembership:
    result = read_membership_pins(
        workspace.state,
        pin if isinstance(pin, IdentityPin) else None,
        pin if isinstance(pin, UniversePin) else None,
        max_materialization_bytes=64 * 1024 * 1024,
    )
    found = result.identity if isinstance(pin, IdentityPin) else result.universe
    assert found is not None
    return found


@pytest.fixture
def workspace(tmp_path: Path) -> Iterator[Workspace]:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as current:
        source_evidence(current)
        yield current


def state_image(workspace: Workspace) -> tuple[str, ...]:
    return tuple(workspace.state.iterdump())


@pytest.mark.parametrize(("raw", "length", "digest"), VECTORS)
def test_literal_vectors(raw: bytes, length: int, digest: str) -> None:
    assert len(raw) == length
    assert hashlib.sha256(raw).hexdigest() == digest
    assert (
        json.dumps(
            json.loads(raw), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        == raw
    )


@pytest.mark.parametrize(("raw", "length", "digest"), VECTORS)
def test_registered_literal_roundtrip(
    workspace: Workspace, raw: bytes, length: int, digest: str
) -> None:
    pin = register(workspace, raw)
    before = state_image(workspace)
    assert register(workspace, json.dumps(json.loads(raw), indent=2).encode()) == pin
    if isinstance(pin, IdentityPin):
        assert (
            register_identity_snapshot(
                workspace.state, raw, expected_file_sha256=digest, created_at_us=99
            )
            == pin
        )
        assert (
            workspace.state.execute("SELECT created_at_us FROM identity_snapshots").fetchone()[0]
            == 7  # noqa: PLR2004 -- explicit original registration timestamp
        )
    actual = evidence(workspace, pin)
    assert actual.pin.content_hash == digest
    assert actual.canonical_bytes == raw
    assert len(actual.canonical_bytes) == length
    assert state_image(workspace) == before
    if actual.members:
        assert dict(actual.members[0]) == {
            "instrument_id": "ASSET_A",
            "valid_from_us": 0,
            "valid_to_us": 25,
            "known_from_us": 0,
            "known_to_us": 35,
        }
        with pytest.raises(TypeError):
            cast("dict[str, object]", actual.members[0])["known_to_us"] = 99
    else:
        assert actual.members == ()
    assert verify_workspace(workspace)["verified"] is True


@pytest.mark.parametrize("raw", [I1, U1])
def test_wrong_requested_and_stored_hashes_reject(workspace: Workspace, raw: bytes) -> None:
    pin = register(workspace, raw)
    wrong = (
        IdentityPin(pin.snapshot_id, "0" * 64)
        if isinstance(pin, IdentityPin)
        else UniversePin(pin.universe_id, pin.version, "0" * 64)
    )
    before = state_image(workspace)
    with pytest.raises(ValueError, match="pin mismatch"):
        evidence(workspace, wrong)
    assert state_image(workspace) == before
    if isinstance(pin, IdentityPin):
        workspace.state.execute("DROP TRIGGER immutable_identity_snapshots_update")
        workspace.state.execute("UPDATE identity_snapshots SET content_hash=?", ("b" * 64,))
    else:
        workspace.state.execute("DROP TRIGGER immutable_universe_versions_update")
        workspace.state.execute("UPDATE universe_versions SET content_hash=?", ("b" * 64,))
    before = state_image(workspace)
    with pytest.raises(ValueError, match="pin mismatch"):
        evidence(workspace, pin)
    with pytest.raises(ValueError, match="content mismatch"):
        verify_workspace(workspace)
    assert state_image(workspace) == before


def test_exact_lowercase_universe_latest_is_rejected(workspace: Workspace) -> None:
    before = state_image(workspace)
    raw = U0.replace(b'"version":"1"', b'"version":"latest"')
    with pytest.raises(ValueError, match="universe version"):
        register(workspace, raw)
    with pytest.raises(ValueError, match="universe version"):
        UniversePin("u-empty", "latest", "a" * 64)
    assert state_image(workspace) == before


@pytest.mark.parametrize("created", [True, -1, 2**63, 1.5])
def test_creation_metadata_is_validated_before_sql(workspace: Workspace, created: object) -> None:
    before = state_image(workspace)
    with pytest.raises(ValueError, match="integer"):
        register_identity_snapshot(
            workspace.state,
            I0,
            expected_file_sha256=VECTORS[0][2],
            created_at_us=cast("int", created),
        )
    assert state_image(workspace) == before


def test_unpinned_read_performs_no_lookup(workspace: Workspace) -> None:
    statements = []
    workspace.state.set_trace_callback(statements.append)
    try:
        result = read_membership_pins(workspace.state, None, None, max_materialization_bytes=1)
    finally:
        workspace.state.set_trace_callback(None)
    assert result.identity is None
    assert result.universe is None
    assert statements == []


def test_writer_logical_admission_precedes_writes(workspace: Workspace) -> None:
    raw = I1.replace(b'"token":"A"', b'"token":"' + b"x" * 600000 + b'"')
    before = state_image(workspace)
    with pytest.raises(ComputeResourceError):
        register(workspace, raw)
    assert state_image(workspace) == before


def test_raw_hash_and_provider_meanings_are_not_invented(workspace: Workspace) -> None:
    raw = I1.replace(
        b'"namespace":"ticker","provider":"synthetic"', b'"namespace":"ticker","provider":"other"'
    )
    pin = register(workspace, raw)
    assert evidence(workspace, pin).canonical_bytes == raw
    assert verify_workspace(workspace)["verified"] is True


def test_external_predecessor_and_existing_issuer_are_reused(workspace: Workspace) -> None:
    register(workspace, I1)
    with atomic(workspace.state):
        workspace.state.execute("INSERT INTO issuers VALUES ('issuer','display only')")
    body = json.loads(I1)
    body["snapshot_id"] = "new"
    body["instruments"][0].update(instrument_id="OTHER", issuer_id="issuer")
    body["assertions"][0].update(
        assertion_id="b", instrument_id="OTHER", supersedes_assertion_id="a"
    )
    body["members"][0]["assertion_id"] = "b"
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    pin = register(workspace, raw)
    assert evidence(workspace, pin).canonical_bytes == raw
    assert evidence(workspace, IdentityPin("ids", VECTORS[2][2])).canonical_bytes == I1


def test_reconstruction_validates_overlap_even_with_matching_hash(workspace: Workspace) -> None:
    pin = register(workspace, multi_document())
    workspace.state.execute("DROP TRIGGER immutable_identity_snapshot_members_update")
    workspace.state.execute(
        "UPDATE identity_snapshot_members SET known_from_us=34 WHERE assertion_id='z'"
    )
    workspace.state.execute("DROP TRIGGER immutable_identity_snapshots_update")
    digest = hashlib.sha256(multi_document(overlap=True)).hexdigest()
    workspace.state.execute("UPDATE identity_snapshots SET content_hash=?", (digest,))
    before = state_image(workspace)
    with pytest.raises(ValueError, match="overlap"):
        evidence(workspace, IdentityPin("ids", digest))
    with pytest.raises(ValueError, match="overlap"):
        verify_workspace(workspace)
    assert state_image(workspace) == before
    assert pin.content_hash != digest


def test_oversized_header_integrity_enumeration_is_bounded(workspace: Workspace) -> None:
    workspace.state.execute(
        "INSERT INTO identity_snapshots VALUES (?, ?, 0)", ("x" * (1024 * 1024 + 1), "a" * 64)
    )
    before = state_image(workspace)
    with pytest.raises(ValueError, match="document byte limit"):
        verify_workspace(workspace)
    assert state_image(workspace) == before


@pytest.mark.parametrize(
    ("kind", "label"),
    [
        ("identity", "latest"),
        ("identity", "LATEST"),
        ("universe", "Latest"),
        ("universe", "LATEST"),
    ],
)
def test_exact_case_sensitive_labels(workspace: Workspace, kind: str, label: str) -> None:
    raw = (
        I0.replace(b"ids-empty", label.encode())
        if kind == "identity"
        else U0.replace(b'"version":"1"', b'"version":"' + label.encode() + b'"')
    )
    pin = register(workspace, raw)
    assert evidence(workspace, pin).canonical_bytes == raw
    assert pin.content_hash == hashlib.sha256(raw).hexdigest()
    other = (
        IdentityPin(label.swapcase(), pin.content_hash)
        if kind == "identity"
        else UniversePin("u-empty", "Latest" if label == "LATEST" else "LATEST", pin.content_hash)
    )
    with pytest.raises(ValueError, match="pin mismatch"):
        evidence(workspace, other)


@pytest.mark.parametrize(
    "raw",
    [
        I1 + b" ",
        I1.replace(b'"schema":', b'"schema":"bad","schema":'),
        I1.replace(b'"known_from_us":0', b'"known_from_us":true'),
        I1.replace(b'"known_from_us":0', b'"known_from_us":0.0'),
        I1.replace(b'"known_from_us":0', b'"known_from_us":9223372036854775808'),
        I1.replace(b'"known_from_us":0', b'"known_from_us":-1'),
        I1.replace(b'"valid_to_us":25', b'"valid_to_us":0'),
        I1.replace(b'"known_to_us":35', b'"known_to_us":0'),
        I1.replace(b'"ordinal":0', b'"ordinal":1'),
        I1.replace(b'"ordinal":0', b'"ordinal":-1'),
        I1.replace(b'"ordinal":0', b'"ordinal":null'),
        I1.replace(b'"ordinal":0', b'"ordinal":false'),
        I1.replace(b'"token":"A"', b'"token":null'),
        I1.replace(b'"token":"A"', b'"token":"\\ud800"'),
        I1.replace(b'"token":"A"', b'"token":"A\\u0000B"'),
        I1.replace(b'"token":"A"', b'"token":" A"'),
        I1.replace(b'"token":"A"', b'"token":"A","unused":1'),
        I1.replace(b'"supersedes_assertion_id":null', b'"supersedes_assertion_id":"a"'),
        I1.replace(b'"supersedes_assertion_id":null', b'"supersedes_assertion_id":"absent"'),
        I1.replace(b'"issuer_id":null', b'"issuer_id":"absent"'),
        I1.replace(b'"source_snapshot_id":"s"', b'"source_snapshot_id":"absent"'),
        I1.replace(b'"status":"raw_verified"', b'"status":"eligible"'),
        I1.replace(b'"relative_path":"e3/', b'"relative_path":"../'),
        I1.replace(b'"relative_path":"e3/', b'"relative_path":"/e3/'),
        I1.replace(b'"size_bytes":0', b'"size_bytes":false'),
        I1.replace(b'"retrieved_at_us":2', b'"retrieved_at_us":0'),
        I1.replace(b'"publication_at_us":null', b'"publication_at_us":-1'),
        I1.replace(b"aas-canonical-json-sha256-v1", b"aas-rowset-v1"),
        I1.replace(b"aas-identity-snapshot-v1", b"aas-identity-snapshot-v2"),
        U1.replace(b'"version":"1"', b'"version":"latest"'),
        b"\xef\xbb\xbf" + I1,
        I1.decode().encode("utf-16"),
        b"[" * 2000 + b"0" + b"]" * 2000,
        I1.replace(b'"known_to_us":35', b'"known_to_us":NaN'),
        I1 + b" " * (1024 * 1024),
    ],
)
def test_invalid_transport_or_document_is_atomic(workspace: Workspace, raw: bytes) -> None:
    before = state_image(workspace)
    with pytest.raises(
        ValueError,
        match=r"membership|document|integer|non-finite|duplicate|predecessor|universe|printable",
    ):
        register_identity_snapshot(
            workspace.state,
            raw,
            expected_file_sha256=hashlib.sha256(I1 if raw == I1 + b" " else raw).hexdigest(),
            created_at_us=0,
        )
    assert state_image(workspace) == before


@pytest.mark.parametrize("array", ["instruments", "assertions", "members", "sources", "files"])
def test_duplicate_declarations_rejected(workspace: Workspace, array: str) -> None:
    body = json.loads(I1)
    rows = body["sources"][0]["files"] if array == "files" else body[array]
    rows.append(rows[0].copy())
    before = state_image(workspace)
    with pytest.raises(ValueError, match="duplicate"):
        register(workspace, json.dumps(body).encode())
    assert state_image(workspace) == before


@pytest.mark.parametrize("array", ["instruments", "assertions", "members", "sources", "files"])
def test_partial_inventory_rejected(workspace: Workspace, array: str) -> None:
    body = json.loads(I1)
    if array == "files":
        body["sources"][0]["files"] = []
    else:
        body[array] = []
    before = state_image(workspace)
    with pytest.raises(ValueError, match=r"references|inventory"):
        register(workspace, json.dumps(body).encode())
    assert state_image(workspace) == before


def multi_document(*, overlap: bool = False) -> bytes:
    body = json.loads(I1)
    # Canonical assertion a depends on z: insertion must not follow array order.
    body["assertions"][0]["supersedes_assertion_id"] = "z"
    body["assertions"].append(
        {**body["assertions"][0], "assertion_id": "z", "supersedes_assertion_id": None}
    )
    body["members"].append(
        {
            **body["members"][0],
            "assertion_id": "z",
            "ordinal": 1,
            "known_from_us": 34 if overlap else 35,
            "known_to_us": None,
        }
    )
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def test_permutations_dependency_order_and_boundary_adjacency(workspace: Workspace) -> None:
    raw = multi_document()
    body = json.loads(raw)
    for key in ("assertions", "members", "instruments", "sources"):
        body[key].reverse()
    pin = register(workspace, json.dumps(body, indent=2).encode())
    assert evidence(workspace, pin).canonical_bytes == raw
    assert pin.content_hash == hashlib.sha256(raw).hexdigest()
    before = state_image(workspace)
    assert register(workspace, raw) == pin
    assert state_image(workspace) == before


def test_projected_intervals_are_not_inferred_from_raw(workspace: Workspace) -> None:
    raw = I1.replace(
        b'"known_to_us":35,"ordinal":0,"valid_from_us":0,"valid_to_us":25',
        b'"known_to_us":40,"ordinal":0,"valid_from_us":-10,"valid_to_us":50',
    )
    pin = register(workspace, raw)
    assert evidence(workspace, pin).canonical_bytes == raw


def test_overlap_and_cycle_are_rejected_without_writes(workspace: Workspace) -> None:
    before = state_image(workspace)
    with pytest.raises(ValueError, match="overlap"):
        register(workspace, multi_document(overlap=True))
    body = json.loads(multi_document())
    body["assertions"][1]["supersedes_assertion_id"] = "a"
    with pytest.raises(ValueError, match="cycle"):
        register(workspace, json.dumps(body).encode())
    assert state_image(workspace) == before


def test_universe_episodes_keep_any_active_semantics(workspace: Workspace) -> None:
    body = json.loads(U1)
    body["members"].append({**body["members"][0], "known_from_us": 10, "known_to_us": None})
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    pin = register(workspace, raw)
    assert evidence(workspace, pin).canonical_bytes == raw
    body["members"].append(body["members"][0].copy())
    with pytest.raises(ValueError, match="duplicate"):
        register(workspace, json.dumps(body).encode())


@pytest.mark.parametrize("nested", [False, True])
def test_failure_after_insert_rolls_back_every_declaration(
    workspace: Workspace, monkeypatch: pytest.MonkeyPatch, *, nested: bool
) -> None:
    if nested:
        workspace.state.execute("BEGIN")
        workspace.state.execute("INSERT INTO issuers VALUES ('prior','caller work')")
    before = state_image(workspace)
    original = membership_pins.read_membership_pins

    def fail_after_verification(
        connection: sqlite3.Connection,
        identity_pin: IdentityPin | None,
        universe_pin: UniversePin | None,
        *,
        max_materialization_bytes: int,
    ) -> Never:
        result = original(
            connection,
            identity_pin,
            universe_pin,
            max_materialization_bytes=max_materialization_bytes,
        )
        assert result.identity is not None
        assert result.identity.canonical_bytes == I1
        raise ValueError("forced final verification failure")

    monkeypatch.setattr(membership_pins, "read_membership_pins", fail_after_verification)
    with pytest.raises(ValueError, match="forced"):
        register(workspace, I1)
    assert state_image(workspace) == before
    assert workspace.state.in_transaction is nested
    if nested:
        workspace.state.rollback()
        assert (
            workspace.state.execute(
                "SELECT count(*) FROM issuers WHERE issuer_id='prior'"
            ).fetchone()[0]
            == 0
        )


def test_successful_nested_registration_does_not_commit_caller(workspace: Workspace) -> None:
    before = state_image(workspace)
    workspace.state.execute("BEGIN")
    register(workspace, I1)
    assert workspace.state.in_transaction
    workspace.state.rollback()
    assert state_image(workspace) == before


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b'"venue":"X"', b'"venue":"Y"'),
        (b'"token":"A"', b'"token":"B"'),
        (b'"known_to_us":35', b'"known_to_us":36'),
    ],
)
def test_conflicts_never_overwrite_shared_or_header_content(
    workspace: Workspace, old: bytes, new: bytes
) -> None:
    pin = register(workspace, I1)
    before = state_image(workspace)
    with pytest.raises(ValueError, match="mismatch"):
        register(workspace, I1.replace(old, new))
    if old != b'"known_to_us":35':
        with pytest.raises(ValueError, match="declaration mismatch"):
            register(
                workspace,
                I1.replace(old, new).replace(b'"snapshot_id":"ids"', b'"snapshot_id":"new"'),
            )
    assert state_image(workspace) == before
    assert evidence(workspace, pin).canonical_bytes == I1


def test_new_snapshot_and_version_leave_old_content_intact(workspace: Workspace) -> None:
    for raw in (I1, U1):
        old = register(workspace, raw)
        changed = (
            raw.replace(b'"known_to_us":35', b'"known_to_us":36')
            .replace(b'"snapshot_id":"ids"', b'"snapshot_id":"new"')
            .replace(b'"version":"1"', b'"version":"2"')
        )
        new = register(workspace, changed)
        assert new != old
        assert evidence(workspace, old).canonical_bytes == raw
        assert evidence(workspace, new).canonical_bytes == changed


@pytest.mark.parametrize("kind", ["identity", "universe"])
@pytest.mark.parametrize("empty", [False, True])
def test_legal_insert_invalidates_even_empty_membership(
    workspace: Workspace, kind: str, *, empty: bool
) -> None:
    raw = I1 if kind == "identity" else U1
    if empty:
        body = json.loads(raw)
        for field in ("members", "instruments", "sources", "assertions"):
            if field in body:
                body[field] = []
        raw = json.dumps(body).encode()
    pin = register(workspace, raw)
    detached = evidence(workspace, pin)
    if empty:
        workspace.state.execute("INSERT INTO instruments VALUES ('ASSET_A',NULL,'etf','X')")
        if kind == "identity":
            workspace.state.execute(
                "INSERT INTO identity_assertions VALUES "
                "('a','ASSET_A','synthetic','ticker','A',0,25,0,NULL,'s',?)",
                ("a" * 64,),
            )
            workspace.state.execute(
                "INSERT INTO identity_snapshot_members VALUES ('ids',0,'a',0,25,0,35)"
            )
        else:
            workspace.state.execute(
                "INSERT INTO universe_members VALUES ('u','1','ASSET_A',0,25,0,35,'s')"
            )
    else:
        legal_insert(workspace, kind)
    before = state_image(workspace)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        evidence(workspace, pin)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        verify_workspace(workspace)
    assert state_image(workspace) == before
    assert detached.pin == pin
    assert len(detached.members) == (0 if empty else 1)


CORRUPTIONS = (
    ("identity_assertions", "UPDATE identity_assertions SET instrument_id='OTHER'"),
    ("identity_assertions", "UPDATE identity_assertions SET provider='changed'"),
    ("identity_assertions", "UPDATE identity_assertions SET token='changed'"),
    (
        "identity_assertions",
        (
            "UPDATE identity_assertions SET source_hash='"
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
        ),
    ),
    ("identity_snapshot_members", "UPDATE identity_snapshot_members SET valid_to_us=26"),
    ("identity_snapshot_members", "UPDATE identity_snapshot_members SET known_to_us=36"),
    ("instruments", "UPDATE instruments SET venue='changed' WHERE instrument_id='ASSET_A'"),
    ("source_snapshots", "UPDATE source_snapshots SET publication_at_us=1 WHERE snapshot_id='s'"),
    ("source_files", "UPDATE source_files SET size_bytes=1 WHERE snapshot_id='s'"),
)


@pytest.mark.parametrize(("table", "sql"), CORRUPTIONS)
def test_changed_logical_evidence_rejects_shared_read_and_integrity(
    workspace: Workspace, table: str, sql: str
) -> None:
    pin = register(workspace, I1)
    workspace.state.execute("INSERT INTO instruments VALUES ('OTHER',NULL,'etf','X')")
    workspace.state.execute(f"DROP TRIGGER immutable_{table}_update")
    workspace.state.execute(sql)
    before = state_image(workspace)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        evidence(workspace, pin)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        verify_workspace(workspace)
    assert state_image(workspace) == before


@pytest.mark.parametrize("target", ["identity_assertions", "instruments", "source_snapshots"])
def test_dangling_joins_do_not_disappear(workspace: Workspace, target: str) -> None:
    pin = register(workspace, I1)
    workspace.state.execute("PRAGMA foreign_keys=OFF")
    workspace.state.execute(f"DROP TRIGGER immutable_{target}_delete")
    workspace.state.execute(f"DELETE FROM {target}")  # noqa: S608 -- fixed parameterized test table inventory
    before = state_image(workspace)
    with pytest.raises(ValueError, match="unknown reference"):
        evidence(workspace, pin)
    with pytest.raises(ValueError, match="foreign key"):
        verify_workspace(workspace)
    assert state_image(workspace) == before


def test_added_source_inventory_is_not_hidden(workspace: Workspace) -> None:
    pin = register(workspace, I1)
    relative, digest, size = put_raw(workspace.paths.raw, b"new retained evidence")
    workspace.state.execute("INSERT INTO source_files VALUES ('s',?,?,?)", (relative, digest, size))
    before = state_image(workspace)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        evidence(workspace, pin)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        verify_workspace(workspace)
    assert state_image(workspace) == before


def test_unrelated_registry_additions_leave_pin_readable(workspace: Workspace) -> None:
    pin = register(workspace, I1)
    workspace.state.execute("INSERT INTO instruments VALUES ('OTHER',NULL,'proxy','Y')")
    workspace.state.execute(
        "INSERT INTO source_snapshots VALUES ('other','other',0,0,NULL,'quarantined')"
    )
    workspace.state.execute(
        "INSERT INTO identity_assertions VALUES "
        "('other','OTHER','other','ticker','A',0,NULL,0,NULL,'other',?)",
        ("b" * 64,),
    )
    assert evidence(workspace, pin).canonical_bytes == I1
    assert verify_workspace(workspace)["verified"] is True


def test_unverified_old_header_is_preserved_without_adoption(workspace: Workspace) -> None:
    workspace.state.execute("INSERT INTO identity_snapshots VALUES ('ids-empty',?,0)", ("b" * 64,))
    before = state_image(workspace)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        evidence(workspace, IdentityPin("ids-empty", "b" * 64))
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        register(workspace, I0)
    with pytest.raises(ValueError, match=r"membership.*mismatch"):
        verify_workspace(workspace)
    assert state_image(workspace) == before


class SelectBoundary(sqlite3.Connection):
    """Observe the actual workspace connection only during membership admission."""

    observing: bool = False
    scalars: int = 0
    materialized: int = 0

    @override
    def execute(
        self,
        sql: str,
        parameters: SupportsLenAndGetItem[object] | Mapping[str, object] = (),
        /,
    ) -> sqlite3.Cursor:
        cursor = super().execute(sql, parameters)
        if not self.observing:
            return cursor
        names = [item[0] for item in cursor.description or ()]
        if any(
            name in {"instrument_id", "assertion_id", "provider", "relative_path", "ordinal"}
            for name in names
        ):
            self.materialized += 1
            raise AssertionError("materialized before aggregate admission")
        self.scalars += 1
        return cursor


@pytest.fixture
def boundary_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Workspace]:
    monkeypatch.setattr(sqlite3, "connect", partial(sqlite3.connect, factory=SelectBoundary))
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as current:
        source_evidence(current)
        yield current


@pytest.mark.parametrize("fault", ["combined", "token", "path", "nul"])
def test_prefetch_admission_counts_both_pins_and_provenance(
    boundary_workspace: Workspace, fault: str
) -> None:
    workspace = boundary_workspace
    identity = register(workspace, I1)
    universe = register(workspace, U1)
    assert isinstance(identity, IdentityPin)
    assert isinstance(universe, UniversePin)

    # Count the literal input's byte inventory without a product admission helper.
    def charge(raw: bytes) -> int:
        body = json.loads(raw)
        rows = body["members"] + body.get("assertions", []) + body["instruments"] + body["sources"]
        rows += [file for source in body["sources"] for file in source["files"]]
        count = sum(
            len(value.encode()) for row in rows for value in row.values() if isinstance(value, str)
        )
        count += sum(
            len(body[key].encode())
            for key in ("snapshot_id", "universe_id", "version")
            if key in body
        )
        return 65536 + 16384 * len(rows) + 128 * count

    allowance = charge(I1) + charge(U1) - 1
    if fault in ("token", "nul"):
        workspace.state.execute("DROP TRIGGER immutable_identity_assertions_update")
        workspace.state.execute(
            "UPDATE identity_assertions SET token=?",
            ("A" + ("\x00" if fault == "nul" else "") + "x" * 600000,),
        )
    elif fault == "path":
        workspace.state.execute("DROP TRIGGER immutable_source_files_update")
        workspace.state.execute("UPDATE source_files SET relative_path=?", ("x" * 600000,))
    boundary = workspace.state
    assert isinstance(boundary, SelectBoundary)
    before = state_image(workspace)
    boundary.observing = True
    try:
        with pytest.raises(ComputeResourceError):
            read_membership_pins(
                boundary,
                identity,
                universe,
                max_materialization_bytes=allowance,
            )
    finally:
        boundary.observing = False
    assert boundary.scalars > 0
    assert boundary.materialized == 0
    assert state_image(workspace) == before
    if fault == "combined":
        assert (
            read_membership_pins(
                workspace.state, identity, universe, max_materialization_bytes=allowance + 1
            ).identity
            is not None
        )


def test_read_only_fresh_process_after_document_removal(
    workspace: Workspace, tmp_path: Path
) -> None:
    # Close workspace locks before launching; use an independent owned home.
    home = tmp_path / "fresh"
    initialize(home)
    path = tmp_path / "document.json"
    pins = []
    with open_workspace(home, writable=True) as writer:
        source_evidence(writer)
        for raw, _, _ in VECTORS:
            path.write_bytes(raw)
            pins.append(register(writer, path.read_bytes()))
        before = state_image(writer)
    path.unlink()
    code = """
import json, sqlite3, sys
from pathlib import Path
from aegis_alpha.storage.workspace import open_workspace
from aegis_alpha.storage.membership_pins import IdentityPin, UniversePin, read_membership_pins
with open_workspace(Path(sys.argv[1])) as w:
    assert w.state.execute('PRAGMA query_only').fetchone()[0] == 1
    before = tuple(w.state.iterdump())
    operations = []
    def authorize(action, arg1, arg2, db, trigger):
        operations.append(action)
        allowed = (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION)
        return sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY
    w.state.set_authorizer(authorize)
    output = []
    for value in json.loads(sys.argv[2]):
        identity = IdentityPin(**value) if 'snapshot_id' in value else None
        universe = UniversePin(**value) if 'universe_id' in value else None
        result = read_membership_pins(w.state, identity, universe,
                                     max_materialization_bytes=64*1024*1024)
        found = result.identity if identity is not None else result.universe
        output.append(found.canonical_bytes.hex())
    assert operations
    w.state.set_authorizer(None)
    assert tuple(w.state.iterdump()) == before
    print(json.dumps(output))
"""
    result = subprocess.run(  # noqa: S603 -- fixed offline child, bounded and reaped
        [sys.executable, "-c", code, str(home), json.dumps([asdict(pin) for pin in pins])],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert [bytes.fromhex(value) for value in json.loads(result.stdout)] == [
        raw for raw, _, _ in VECTORS
    ]
    with open_workspace(home) as reader:
        assert state_image(reader) == before
    assert verify_workspace(workspace)["verified"] is True
