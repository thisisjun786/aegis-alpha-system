"""Chunked identity and universe documents: deterministic parts under one manifest pin."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from aegis_alpha.data.serialization import canonical_json_bytes, content_sha256
from aegis_alpha.storage import membership_pins
from aegis_alpha.storage.identity import (
    identity_document,
    issuer_link_token,
    mint_instrument,
    mint_issuer,
    parse_registry,
    register_identities,
    show_snapshot,
    snapshot_identities,
)
from aegis_alpha.storage.membership_pins import (
    IdentityPin,
    UniversePin,
    membership_parts,
    plan_membership_manifest,
    read_membership_pins,
    register_identity_manifest,
    register_identity_snapshot,
    register_universe_manifest,
)
from aegis_alpha.storage.state import atomic
from aegis_alpha.storage.verification import verify_workspace
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.identity_support import (
    SOURCE,
    assertion,
    document,
    instrument,
    link_source,
    master,
)

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace

NORGATE_MASTER_ROWS = 35_603
_ALLOWANCE = 2**40


@pytest.fixture
def workspace(tmp_path: Path) -> Iterator[Workspace]:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as current:
        link_source(current)
        yield current


def registered(workspace: Workspace, body: dict[str, object]) -> None:
    report = register_identities(workspace.state, parse_registry(body), apply=True)
    assert report["conflict_count"] == 0


def read(
    workspace: Workspace, pin: IdentityPin | UniversePin
) -> membership_pins.VerifiedMembership:
    result = read_membership_pins(
        workspace.state,
        pin if isinstance(pin, IdentityPin) else None,
        pin if isinstance(pin, UniversePin) else None,
        max_materialization_bytes=_ALLOWANCE,
    )
    found = result.identity if isinstance(pin, IdentityPin) else result.universe
    assert found is not None
    return found


# The recorded manifest of the synthetic Norgate-sized master registered below. A
# change to chunking, ordering, part naming, ID minting or any document format moves it.
FROZEN_MASTER_MANIFEST = "8f3b4e275a6aa11cc2004ef82f31b655232267f294d23296f6252b2a70e5ec94"


def test_chunked_snapshot_hashes_reproduce(tmp_path: Path, workspace: Workspace) -> None:
    registered(workspace, master(NORGATE_MASTER_ROWS))
    report = snapshot_identities(workspace.state, "norgate-master", created_at_us=5, apply=True)
    pin = IdentityPin("norgate-master", cast("str", report["content_hash"]))
    parts = cast("list[dict[str, str]]", report["parts"])
    assert report["members"] == NORGATE_MASTER_ROWS
    assert len(parts) > 1
    stored = cast("tuple[IdentityPin, ...]", membership_parts(workspace.state, pin))
    assert [(part.snapshot_id, part.content_hash) for part in stored] == [
        (part["snapshot_id"], part["content_hash"]) for part in parts
    ]
    manifest = {
        "schema": "aas-identity-manifest-v1",
        "hash_format": "aas-canonical-json-sha256-v1",
        "snapshot_id": "norgate-master",
        "parts": parts,
    }
    assert pin.content_hash == content_sha256(manifest) == FROZEN_MASTER_MANIFEST
    for part in stored:
        size = len(read(workspace, part).canonical_bytes)
        assert size <= 1024 * 1024
    evidence = read(workspace, pin)
    assert evidence.canonical_bytes == canonical_json_bytes(manifest)
    assert len(evidence.members) == NORGATE_MASTER_ROWS
    assert verify_workspace(workspace)["verified"] is True
    # A second installation that registers the same rows derives the same parts.
    initialize(tmp_path / "again")
    with open_workspace(tmp_path / "again", writable=True) as other:
        link_source(other)
        registered(other, master(NORGATE_MASTER_ROWS))
        again = snapshot_identities(other.state, "norgate-master", created_at_us=99, apply=False)
    assert (again["content_hash"], again["parts"]) == (pin.content_hash, parts)
    # Registering again is idempotent and keeps the original creation time.
    assert (
        snapshot_identities(workspace.state, "norgate-master", created_at_us=7, apply=True)[
            "content_hash"
        ]
        == pin.content_hash
    )
    assert show_snapshot(workspace.state, "norgate-master")["created_at_us"] == 5  # noqa: PLR2004 -- first registration time


def test_snapshot_projects_correction_knowledge(workspace: Workspace) -> None:
    registered(workspace, document(instruments=[instrument("1")], assertions=[assertion("1")]))
    (original,) = (
        row[0] for row in workspace.state.execute("SELECT assertion_id FROM identity_assertions")
    )
    registered(
        workspace, document(assertions=[assertion("1", value="B", known=9, supersedes=original)])
    )
    body = identity_document(workspace.state, "ids")
    members = {
        cast("str", row["assertion_id"]): row
        for row in cast("list[dict[str, object]]", body["members"])
    }
    assert members[original]["known_to_us"] == 9  # noqa: PLR2004 -- the correction's known_from
    assert [row["known_to_us"] for key, row in members.items() if key != original] == [None]
    pin = register_identity_manifest(workspace.state, body, created_at_us=1)
    assert sorted((m["known_from_us"], m["known_to_us"]) for m in read(workspace, pin).members) == [
        (2, 9),
        (9, None),
    ]


def test_part_names_are_reserved_for_manifests(workspace: Workspace) -> None:
    raw = canonical_json_bytes(
        {
            "schema": "aas-identity-snapshot-v1",
            "hash_format": "aas-canonical-json-sha256-v1",
            "snapshot_id": "ids#00000",
            "instruments": [],
            "assertions": [],
            "members": [],
            "sources": [],
        }
    )
    with pytest.raises(ValueError, match="manifest part"):
        register_identity_snapshot(
            workspace.state,
            raw,
            expected_file_sha256=hashlib.sha256(raw).hexdigest(),
            created_at_us=1,
        )
    with pytest.raises(ValueError, match="manifest part"):
        register_identity_manifest(
            workspace.state, identity_document(workspace.state, "ids#00001"), created_at_us=1
        )
    # A v1 document and a manifest can never share a root.
    plain = canonical_json_bytes(
        {**cast("dict[str, object]", __import__("json").loads(raw)), "snapshot_id": "ids"}
    )
    register_identity_snapshot(
        workspace.state,
        plain,
        expected_file_sha256=hashlib.sha256(plain).hexdigest(),
        created_at_us=1,
    )
    with pytest.raises(ValueError, match="pin mismatch"):
        register_identity_manifest(
            workspace.state, identity_document(workspace.state, "ids"), created_at_us=1
        )


def _universe(count: int) -> dict[str, object]:
    return {
        "schema": "aas-universe-version-v1",
        "hash_format": "aas-canonical-json-sha256-v1",
        "universe_id": "listed",
        "version": "2026-10-03",
        "instruments": [
            {
                "instrument_id": f"I{index:03d}",
                "issuer_id": None,
                "asset_type": "equity",
                "venue": "X",
            }
            for index in range(count)
        ],
        "members": [
            {
                "instrument_id": f"I{index:03d}",
                "valid_from_us": episode * 10,
                "valid_to_us": episode * 10 + 5,
                "known_from_us": 0,
                "known_to_us": None,
                "source_snapshot_id": SOURCE,
            }
            for index in range(count)
            for episode in range(2)
        ],
        "sources": identity_sources(),
    }


def identity_sources() -> list[dict[str, object]]:
    return [
        {
            "snapshot_id": SOURCE,
            "provider": "source-library",
            "requested_at_us": 1,
            "retrieved_at_us": 2,
            "publication_at_us": None,
            "status": "raw_verified",
            "files": [
                {
                    "relative_path": "62/" + hashlib.sha256(b"m").hexdigest(),
                    "byte_hash": hashlib.sha256(b"m").hexdigest(),
                    "size_bytes": 1,
                }
            ],
        }
    ]


def test_universe_manifest_roundtrip(workspace: Workspace, monkeypatch: pytest.MonkeyPatch) -> None:
    """Members split across small parts read back as one document in canonical order."""
    monkeypatch.setattr(membership_pins, "_MAX_BYTES", 4096)
    body = _universe(40)
    with atomic(workspace.state):
        for row in cast("list[dict[str, object]]", body["instruments"]):
            workspace.state.execute(
                "INSERT INTO instruments VALUES (?,?,?,?)",
                (row["instrument_id"], None, "equity", "X"),
            )
    pin = register_universe_manifest(workspace.state, body)
    parts = membership_parts(workspace.state, pin)
    assert len(parts) > 2  # noqa: PLR2004 -- the byte limit forces several parts
    assert [cast("UniversePin", part).version for part in parts][:2] == [
        "2026-10-03#00000",
        "2026-10-03#00001",
    ]
    members = read(workspace, pin).members
    assert [(m["instrument_id"], m["valid_from_us"]) for m in members] == [
        (f"I{index:03d}", episode * 10) for index in range(40) for episode in range(2)
    ]
    assert register_universe_manifest(workspace.state, body) == pin
    assert verify_workspace(workspace)["verified"] is True
    changed = {**body, "members": cast("list[object]", body["members"])[:-1]}
    with pytest.raises(ValueError, match="pin mismatch"):
        register_universe_manifest(workspace.state, changed)


def test_manifest_detects_tampered_or_extra_parts(
    workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(membership_pins, "_MAX_BYTES", 4096)
    registered(workspace, master(30))
    pin = cast(
        "IdentityPin",
        plan_membership_manifest(identity_document(workspace.state, "m"), identity=True).pin,
    )
    assert (
        register_identity_manifest(
            workspace.state, identity_document(workspace.state, "m"), created_at_us=1
        )
        == pin
    )
    first = cast("IdentityPin", membership_parts(workspace.state, pin)[0])
    extra = workspace.state.execute(
        "SELECT assertion_id FROM identity_assertions ORDER BY assertion_id DESC LIMIT 1"
    ).fetchone()[0]
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO identity_snapshot_members VALUES (?,?,?,?,?,?,?)",
            (first.snapshot_id, 999, extra, 50, None, 99, None),
        )
    with pytest.raises(ValueError, match=r"mismatch|canonical"):
        read(workspace, pin)
    with pytest.raises(ValueError, match=r"mismatch|canonical"):
        verify_workspace(workspace)


def test_manifest_rejects_an_extra_contiguous_part(
    workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(membership_pins, "_MAX_BYTES", 4096)
    registered(workspace, master(30))
    pin = register_identity_manifest(
        workspace.state, identity_document(workspace.state, "m"), created_at_us=1
    )
    count = len(membership_parts(workspace.state, pin))
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO identity_snapshots VALUES (?,?,1)",
            (membership_pins.part_name("m", count), "0" * 64),
        )
    with pytest.raises(ValueError, match="manifest content mismatch"):
        read(workspace, pin)
    with pytest.raises(ValueError, match="manifest content mismatch"):
        verify_workspace(workspace)


def test_manifest_root_members_are_rejected(
    workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A manifest root holds no members; a row stored under it fails read and verify."""
    monkeypatch.setattr(membership_pins, "_MAX_BYTES", 4096)
    registered(workspace, master(30))
    pin = register_identity_manifest(
        workspace.state, identity_document(workspace.state, "m"), created_at_us=1
    )
    assert len(read(workspace, pin).members) == 30  # noqa: PLR2004 -- every registered row
    stray = workspace.state.execute("SELECT assertion_id FROM identity_assertions").fetchone()[0]
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO identity_snapshot_members VALUES ('m',0,?,0,NULL,2,NULL)", (stray,)
        )
    with pytest.raises(ValueError, match="content mismatch"):
        read(workspace, pin)
    with pytest.raises(ValueError, match="content mismatch"):
        verify_workspace(workspace)


def test_v1_documents_named_like_parts_stay_readable(workspace: Workspace) -> None:
    """A store may hold independent v1 documents ``x`` and ``x#00000``; both still read."""
    registered(workspace, master(2))
    whole = identity_document(workspace.state, "x")
    rows = {
        name: cast("list[dict[str, object]]", whole[name])
        for name in ("members", "assertions", "instruments")
    }
    pins = []
    for name, members in (("x", 1), ("x#00000", 2), ("y", 0), ("y#00003", 1)):
        chosen = rows["members"][:members]
        kept = {row["assertion_id"] for row in chosen}
        assertions = [row for row in rows["assertions"] if row["assertion_id"] in kept]
        used = {row["instrument_id"] for row in assertions}
        body = {
            **whole,
            "snapshot_id": name,
            "members": chosen,
            "assertions": assertions,
            "instruments": [row for row in rows["instruments"] if row["instrument_id"] in used],
            "sources": whole["sources"] if members else [],
        }
        # Stored before the suffix was reserved, so written as such a store holds them.
        canonical = membership_pins._canonical(body, identity=True)  # noqa: SLF001 -- legacy store
        pin = IdentityPin(name, content_sha256(canonical))
        membership_pins._registered(  # noqa: SLF001 -- legacy store
            workspace.state, canonical, canonical_json_bytes(canonical), pin, 1
        )
        pins.append((pin, members))
    for pin, members in pins:
        assert len(read(workspace, pin).members) == members
        assert membership_parts(workspace.state, pin) == ()
    assert verify_workspace(workspace)["verified"] is True


def test_source_inventory_bounds_a_part(workspace: Workspace) -> None:
    """A part carries every referenced source's whole inventory, which caps its file count."""
    registered(workspace, master(1))
    with atomic(workspace.state):
        workspace.state.executemany(
            "INSERT INTO source_files VALUES (?,?,?,1)",
            (
                (SOURCE, f"00/{index:062d}", hashlib.sha256(str(index).encode()).hexdigest())
                for index in range(5000)
            ),
        )
    with pytest.raises(ValueError, match=f"source {SOURCE} lists 5001 files"):
        snapshot_identities(workspace.state, "wide", created_at_us=1, apply=False)


def test_manifest_rejects_a_gap_in_its_parts(
    workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(membership_pins, "_MAX_BYTES", 4096)
    registered(workspace, master(30))
    pin = register_identity_manifest(
        workspace.state, identity_document(workspace.state, "m"), created_at_us=1
    )
    count = len(membership_parts(workspace.state, pin))
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO identity_snapshots VALUES (?,?,1)",
            (membership_pins.part_name("m", count + 1), "0" * 64),
        )
    with pytest.raises(ValueError, match="not contiguous"):
        read(workspace, pin)


def _issuer_link(issuer: str, *, known: int = 2) -> dict[str, object]:
    token = issuer_link_token(
        mint_issuer("sec_cik", issuer), mint_instrument("norgate_assetid", "1")
    )
    return assertion("1", provider="sec", namespace="issuer", value=token, known=known)


_CIK_A = {"anchor_namespace": "sec_cik", "anchor_token": "0000000001", "name": "A"}


@pytest.mark.parametrize(
    ("first", "second", "issuers"),
    [
        # One ticker on two instruments at once.
        (assertion("1", value="X"), assertion("2", value="X", known=3), []),
        # One instrument linked to two issuers at once by one provider.
        (_issuer_link("0000000001"), _issuer_link("0000000002", known=3), [_CIK_A]),
    ],
    ids=["ticker", "issuer-link"],
)
def test_cross_part_overlap_is_rejected(
    workspace: Workspace,
    first: dict[str, object],
    second: dict[str, object],
    issuers: list[dict[str, object]],
) -> None:
    """Parts that are each valid still fail when one key overlaps across two of them."""
    registered(
        workspace,
        document(
            issuers=issuers,
            instruments=[instrument("1"), instrument("2")],
            assertions=[first],
        ),
    )
    # The registry refuses the ambiguous second row, so it arrives as another route would.
    (row,) = parse_registry(document(assertions=[second])).assertions
    columns, marks = ",".join(row), ",".join("?" * len(row))
    with atomic(workspace.state):
        workspace.state.execute(
            f"INSERT INTO identity_assertions ({columns}) VALUES ({marks})",  # noqa: S608 -- fixed test columns
            tuple(row.values()),
        )
    whole = identity_document(workspace.state, "x")
    with pytest.raises(ValueError, match="overlap"):
        plan_membership_manifest(whole, identity=True)
    # Register each member as its own part directly, as a hand-made store could.
    pins = []
    for index, member in enumerate(cast("list[dict[str, object]]", whole["members"])):
        chosen = next(
            row
            for row in cast("list[dict[str, object]]", whole["assertions"])
            if row["assertion_id"] == member["assertion_id"]
        )
        body = {
            **whole,
            "snapshot_id": membership_pins.part_name("x", index),
            "assertions": [chosen],
            "instruments": [
                row
                for row in cast("list[dict[str, object]]", whole["instruments"])
                if row["instrument_id"] == chosen["instrument_id"]
            ],
            "members": [{**member, "ordinal": 0}],
        }
        canonical = membership_pins._canonical(body, identity=True)  # noqa: SLF001 -- crafted store
        part_pin = IdentityPin(cast("str", canonical["snapshot_id"]), content_sha256(canonical))
        membership_pins._registered(  # noqa: SLF001 -- crafted store
            workspace.state, canonical, canonical_json_bytes(canonical), part_pin, 1
        )
        pins.append(part_pin)
    manifest = membership_pins._manifest_bytes(IdentityPin("x", "0" * 64), pins)  # noqa: SLF001 -- crafted store
    root = IdentityPin("x", hashlib.sha256(manifest).hexdigest())
    with atomic(workspace.state):
        workspace.state.execute(
            "INSERT INTO identity_snapshots VALUES ('x',?,1)", (root.content_hash,)
        )
    with pytest.raises(ValueError, match="overlap"):
        read(workspace, root)
    with pytest.raises(ValueError, match="overlap"):
        verify_workspace(workspace)
