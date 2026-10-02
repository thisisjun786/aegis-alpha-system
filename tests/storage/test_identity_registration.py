"""The registry appends issuers, instruments and assertions; it never rewrites one."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from aegis_alpha.application.cli import main
from aegis_alpha.storage.identity import (
    decode_registry,
    issuer_link_token,
    mint_instrument,
    mint_issuer,
    parse_registry,
    register_identities,
    snapshot_identities,
)
from aegis_alpha.storage.workspace import initialize, open_workspace
from tests.storage.identity_support import (
    assertion,
    document,
    instrument,
    link_source,
    master,
)

if TYPE_CHECKING:
    from aegis_alpha.storage.workspace import Workspace


@pytest.fixture
def workspace(tmp_path: Path) -> Iterator[Workspace]:
    initialize(tmp_path / "home")
    with open_workspace(tmp_path / "home", writable=True, strategy_write=True) as current:
        link_source(current)
        yield current


def image(workspace: Workspace) -> tuple[str, ...]:
    return tuple(workspace.state.iterdump())


def apply(workspace: Workspace, body: dict[str, object]) -> dict[str, Any]:
    """Register and return the report as the CLI prints it, which also proves it is JSON."""
    report = register_identities(workspace.state, parse_registry(body), apply=True)
    return json.loads(json.dumps(report))


def plan(workspace: Workspace, body: dict[str, object]) -> dict[str, Any]:
    report = register_identities(workspace.state, parse_registry(body), apply=False)
    return json.loads(json.dumps(report))


def assertion_ids(workspace: Workspace) -> list[str]:
    return [
        row[0]
        for row in workspace.state.execute(
            "SELECT assertion_id FROM identity_assertions ORDER BY known_from_us"
        )
    ]


def test_plan_classifies_without_writing_and_apply_is_idempotent(workspace: Workspace) -> None:
    body = master(3)
    before = image(workspace)
    report = plan(workspace, body)
    assert image(workspace) == before
    assert report["new"] == {"issuers": 0, "instruments": 3, "assertions": 3}
    assert report["conflict_count"] == 0
    assert apply(workspace, body)["new"] == report["new"]
    after = image(workspace)
    repeated = apply(workspace, body)
    assert repeated["new"] == {"issuers": 0, "instruments": 0, "assertions": 0}
    assert repeated["existing"] == {"issuers": 0, "instruments": 3, "assertions": 3}
    assert image(workspace) == after
    stored = workspace.state.execute("SELECT instrument_id FROM instruments").fetchall()
    assert sorted(row[0] for row in stored) == sorted(
        mint_instrument("norgate_assetid", token) for token in ("100000", "100001", "100002")
    )


def test_correction_is_a_new_assertion_without_update(workspace: Workspace) -> None:
    apply(
        workspace,
        document(instruments=[instrument("1")], assertions=[assertion("1", value="AAA")]),
    )
    (original,) = assertion_ids(workspace)
    stored = workspace.state.execute(
        "SELECT * FROM identity_assertions WHERE assertion_id=?", (original,)
    ).fetchone()
    statements: list[str] = []
    workspace.state.set_trace_callback(statements.append)
    try:
        report = apply(
            workspace,
            document(assertions=[assertion("1", value="AAB", known=9, supersedes=original)]),
        )
    finally:
        workspace.state.set_trace_callback(None)
    assert report["new"] == {"issuers": 0, "instruments": 0, "assertions": 1}
    writes = [sql for sql in statements if sql.lstrip().upper().startswith(("UPDATE", "DELETE"))]
    assert writes == []
    assert any(sql.lstrip().upper().startswith("INSERT") for sql in statements)
    assert (
        workspace.state.execute(
            "SELECT * FROM identity_assertions WHERE assertion_id=?", (original,)
        ).fetchone()
        == stored
    )
    correction = assertion_ids(workspace)[1]
    row = workspace.state.execute(
        "SELECT supersedes_assertion_id,token FROM identity_assertions WHERE assertion_id=?",
        (correction,),
    ).fetchone()
    assert tuple(row) == (original, "AAB")


def test_correction_must_become_known_after_what_it_corrects(workspace: Workspace) -> None:
    apply(workspace, document(instruments=[instrument("1")], assertions=[assertion("1")]))
    (original,) = assertion_ids(workspace)
    early = document(assertions=[assertion("1", value="B", known=2, supersedes=original)])
    report = plan(workspace, early)
    assert [conflict["kind"] for conflict in report["conflicts"]] == ["correction_not_later"]
    before = image(workspace)
    with pytest.raises(ValueError, match="1 conflicts"):
        apply(workspace, early)
    assert image(workspace) == before


def test_overlapping_unrelated_assertions_conflict(workspace: Workspace) -> None:
    """One ticker on two instruments at once is ambiguous unless one corrects the other."""
    apply(
        workspace,
        document(
            instruments=[instrument("1"), instrument("2")],
            assertions=[assertion("1", value="X", valid=(0, 100))],
        ),
    )
    before = image(workspace)
    reused = document(assertions=[assertion("2", value="X", valid=(50, None), known=5)])
    report = plan(workspace, reused)
    assert report["conflict_count"] == 1
    conflict = report["conflicts"][0]
    assert (conflict["kind"], conflict["token"], conflict["same_instrument"]) == (
        "assertion_overlap",
        "X",
        False,
    )
    with pytest.raises(ValueError, match="identity registration refused"):
        apply(workspace, reused)
    assert image(workspace) == before
    # The same ticker after the first listing ended is a new assertion, not a conflict.
    later = document(assertions=[assertion("2", value="X", valid=(100, None), known=5)])
    assert apply(workspace, later)["conflict_count"] == 0
    # A correction that supersedes the first assertion is a chain, not a conflict.
    (first, _) = assertion_ids(workspace)
    moved = document(
        assertions=[assertion("2", value="X", valid=(0, 100), known=7, supersedes=first)]
    )
    assert plan(workspace, moved)["conflict_count"] == 0


def test_correction_is_never_known_before_its_source(workspace: Workspace) -> None:
    """A correction takes no knowledge time earlier than the collection that carries it."""
    link_source(workspace, "sl:late", b"late", retrieved=10**15)
    apply(workspace, document(instruments=[instrument("1")], assertions=[assertion("1")]))
    (original,) = assertion_ids(workspace)
    early = document(
        assertions=[assertion("1", value="B", known=3, supersedes=original, source="sl:late")]
    )
    report = plan(workspace, early)
    assert [conflict["kind"] for conflict in report["conflicts"]] == ["correction_before_source"]
    assert report["conflicts"][0]["source_retrieved_at_us"] == 10**15
    before = image(workspace)
    with pytest.raises(ValueError, match="1 conflicts"):
        apply(workspace, early)
    assert image(workspace) == before
    known = document(
        assertions=[assertion("1", value="B", known=10**15, supersedes=original, source="sl:late")]
    )
    assert apply(workspace, known)["conflict_count"] == 0
    # An initial assertion keeps its declared historical knowledge time.
    history = document(
        instruments=[instrument("2")], assertions=[assertion("2", known=3, source="sl:late")]
    )
    assert plan(workspace, history)["conflict_count"] == 0


def test_instrument_row_is_first_registration_context(workspace: Workspace) -> None:
    """A later venue or issuer is reported and kept as evidence; asset_type still conflicts."""
    apply(workspace, document(instruments=[instrument("1")]))
    moved = apply(workspace, document(instruments=[instrument("1", venue="Nasdaq")]))
    assert (moved["conflict_count"], moved["instrument_difference_count"]) == (0, 1)
    assert workspace.state.execute("SELECT venue FROM instruments").fetchone()[0] == "NYSE"
    fund = {**instrument("1"), "asset_type": "fund"}
    report = plan(workspace, document(instruments=[fund]))
    assert [conflict["kind"] for conflict in report["conflicts"]] == ["instrument_attributes"]


def test_issuer_link_after_a_null_issuer(workspace: Workspace) -> None:
    """An instrument registered without an issuer is linked to one later by assertion."""
    cik: dict[str, object] = {"anchor_namespace": "sec_cik", "anchor_token": "0000320193"}
    other: dict[str, object] = {"anchor_namespace": "sec_cik", "anchor_token": "0000789019"}
    apply(workspace, document(instruments=[instrument("1"), instrument("2")]))
    issuer, rival = mint_issuer("sec_cik", "0000320193"), mint_issuer("sec_cik", "0000789019")
    first, second = mint_instrument("norgate_assetid", "1"), mint_instrument("norgate_assetid", "2")

    def link(
        token: str,
        issuer_id: str,
        instrument_id: str,
        *,
        known: int = 2,
        supersedes: str | None = None,
    ) -> dict[str, object]:
        value = issuer_link_token(issuer_id, instrument_id)
        return assertion(
            token,
            provider="sec",
            namespace="issuer",
            value=value,
            known=known,
            supersedes=supersedes,
        )

    linked = document(
        issuers=[{**cik, "name": "Apple Inc"}],
        instruments=[instrument("1", issuer=cik), instrument("2", issuer=cik)],
        assertions=[link("1", issuer, first), link("2", issuer, second)],
    )
    report = apply(workspace, linked)
    # Two share classes of one issuer are not an overlap; the stored rows keep NULL.
    assert (report["conflict_count"], report["instrument_difference_count"]) == (0, 2)
    assert report["new"] == {"issuers": 1, "instruments": 0, "assertions": 2}
    stored = workspace.state.execute("SELECT issuer_id FROM instruments").fetchall()
    assert [row[0] for row in stored] == [None, None]
    # One provider never links one instrument to two issuers at once ...
    rival_link = document(
        issuers=[{**other, "name": "Microsoft"}], assertions=[link("1", rival, first, known=5)]
    )
    conflicts = plan(workspace, rival_link)["conflicts"]
    assert [(c["kind"], c["same_instrument"]) for c in conflicts] == [("assertion_overlap", True)]
    # ... unless the new link corrects the earlier one.
    (earlier,) = (
        row[0]
        for row in workspace.state.execute(
            "SELECT assertion_id FROM identity_assertions WHERE instrument_id=?", (first,)
        )
    )
    corrected = document(
        issuers=[{**other, "name": "Microsoft"}],
        assertions=[link("1", rival, first, known=5, supersedes=earlier)],
    )
    assert apply(workspace, corrected)["conflict_count"] == 0
    snapshot = snapshot_identities(workspace.state, "links", created_at_us=1, apply=True)
    assert snapshot["members"] == 3  # noqa: PLR2004 -- two links and one correction
    with pytest.raises(ValueError, match="issuer link token"):
        parse_registry(document(assertions=[link("1", issuer, second)]))
    missing = document(assertions=[link("1", mint_issuer("sec_cik", "0000000001"), first)])
    assert plan(workspace, missing)["missing_count"]["issuers"] == 1


def test_missing_references_are_reported_and_refused(workspace: Workspace) -> None:
    cik: dict[str, object] = {"anchor_namespace": "sec_cik", "anchor_token": "0000320193"}
    body = document(
        instruments=[instrument("1", issuer=cik)],
        assertions=[
            assertion("1", source="sl:absent"),
            assertion("2", value="Y"),
            assertion("1", value="Z", known=4, supersedes="asr-" + "0" * 64),
        ],
    )
    report = plan(workspace, body)
    assert report["missing_count"] == {
        "sources": 1,
        "issuers": 1,
        "instruments": 1,
        "predecessors": 1,
    }
    assert report["missing"]["sources"] == ["sl:absent"]
    assert report["missing"]["issuers"] == [mint_issuer("sec_cik", "0000320193")]
    with pytest.raises(ValueError, match="identity registration refused"):
        apply(workspace, body)


def test_issuer_is_reused_and_a_differing_name_is_reported(workspace: Workspace) -> None:
    cik: dict[str, object] = {"anchor_namespace": "sec_cik", "anchor_token": "0000320193"}
    apply(
        workspace,
        document(
            issuers=[{**cik, "name": "Apple Computer Inc"}],
            instruments=[instrument("1", issuer=cik)],
        ),
    )
    renamed = document(issuers=[{**cik, "name": "Apple Inc"}])
    report = apply(workspace, renamed)
    assert report["new"]["issuers"] == 0
    assert report["existing"]["issuers"] == 1
    assert report["issuer_name_difference_count"] == 1
    assert workspace.state.execute("SELECT name FROM issuers").fetchone()[0] == (
        "Apple Computer Inc"
    )
    stored = workspace.state.execute("SELECT issuer_id FROM instruments").fetchone()[0]
    assert stored == mint_issuer("sec_cik", "0000320193")


def test_document_shape_is_exact() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        parse_registry({**document(), "schema": "aas-identity-registry-v2"})
    with pytest.raises(ValueError, match="missing or unknown"):
        parse_registry({**document(), "extra": []})
    with pytest.raises(ValueError, match="repeats an instrument"):
        parse_registry(document(instruments=[instrument("1"), instrument("1")]))
    with pytest.raises(ValueError, match="repeats an assertion"):
        parse_registry(document(assertions=[assertion("1"), assertion("1")]))
    with pytest.raises(ValueError, match="end must exceed"):
        parse_registry(document(assertions=[assertion("1", valid=(5, 5))]))
    raw = json.dumps(document()).encode()
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        decode_registry(raw, expected_file_sha256="0" * 64)
    assert decode_registry(raw, expected_file_sha256=hashlib.sha256(raw).hexdigest()).issuers == ()


def test_cli_register_snapshot_and_show(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    home = tmp_path / "home"
    initialize(home)
    with open_workspace(home, writable=True) as current:
        link_source(current)
    path = tmp_path / "registry.json"
    path.write_bytes(json.dumps(master(2)).encode())
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    def run(*argv: str) -> dict[str, object]:
        assert main(["--home", str(home), "identity", *argv]) == 0
        return cast("dict[str, object]", json.loads(capsys.readouterr().out))

    planned = run("register", "--file", str(path), "--sha256", digest, "--plan")
    assert (planned["mode"], planned["new"]) == (
        "plan",
        {"issuers": 0, "instruments": 2, "assertions": 2},
    )
    assert run("register", "--file", str(path), "--sha256", digest)["mode"] == "apply"
    snapshot = run("snapshot", "--id", "master", "--provider", "norgate")
    assert snapshot["members"] == 2  # noqa: PLR2004 -- two registered assertions
    shown = run("show", "--snapshot", "master")
    assert (shown["kind"], shown["content_hash"]) == ("manifest", snapshot["content_hash"])
    by_anchor = run("show", "--anchor", "norgate_assetid", "100000")
    instrument_id = mint_instrument("norgate_assetid", "100000")
    assert cast("dict[str, object]", by_anchor["instrument"])["instrument_id"] == instrument_id
    by_key = run("show", "--key", "norgate", "norgate_assetid", "100001")
    assert by_key["instrument_ids"] == [mint_instrument("norgate_assetid", "100001")]
    assert main(["--home", str(home), "identity", "show", "--anchor", "ticker", "AAPL"]) == 1
    assert "not a permanent anchor" in capsys.readouterr().err
