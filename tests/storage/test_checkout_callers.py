"""Every persistent-path guard agrees on synthetic and real repository markers."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

import pytest

from aegis_alpha.application import collection_journal, container_install
from aegis_alpha.compute_resources import ComputeResourceError, compute_lock_path
from aegis_alpha.data import (
    finimpulse_collector_cli,
    finimpulse_owner_authority,
    fmp_collector,
    fmp_owner_authority,
    fmp_usage_authority,
    fred_alfred_collector,
    fred_alfred_owner_authority,
    fred_raw_archive,
    qveris_client,
    qveris_store,
    sec_bulk,
    sec_collector,
)
from aegis_alpha.storage.locks import private_directory
from tests.checkout_support import init_repository

# Exercise the actual admission boundaries, including authority file reads before parsing.
CASES = [
    (
        lambda p: private_directory(p / "new", create=True),
        ValueError,
        "persistent storage cannot be inside a Git checkout",
    ),
    (
        lambda p: compute_lock_path({"AAS_COMPUTE_LOCK_FILE": str(p / "lock")}),
        ComputeResourceError,
        "compute lock must be outside Git",
    ),
    (collection_journal.validate_root, ValueError, "collection state must be outside Git"),
    (
        container_install._private_directory,  # noqa: SLF001 -- admission boundary
        ValueError,
        "installation state must be outside Git",
    ),
    (
        lambda p: qveris_client._read_key_file(p / "artifact"),  # noqa: SLF001
        qveris_client.QverisKeyFileError,
        "Qveris key file must be outside a Git repository",
    ),
    (
        lambda p: qveris_store.validate_destination("output", p),
        qveris_store.QverisDestinationError,
        "output must be outside a Git repository",
    ),
    (
        lambda p: sec_bulk.archive_plan("companyfacts", p, 1024),
        sec_bulk.BulkError,
        "SEC bulk output must be outside Git",
    ),
    (
        lambda p: fred_raw_archive.plan_archive(p / "fred-manual", ("CPIAUCSL",), 3),
        ValueError,
        "manual FRED archive must be outside Git",
    ),
    (
        lambda p: fmp_owner_authority._read_external(p / "artifact", -1, lambda m: m.st_uid),  # noqa: SLF001
        ValueError,
        "owner approval authority must be outside Git",
    ),
    (
        lambda p: finimpulse_owner_authority._read_external(p / "artifact", -1, lambda m: m.st_uid),  # noqa: SLF001
        finimpulse_owner_authority.OwnerAuthorityError,
        "owner authority must be outside Git",
    ),
    (
        lambda p: fred_alfred_owner_authority._read_external(  # noqa: SLF001
            p / "artifact", -1, lambda m: m.st_uid
        ),
        fred_alfred_owner_authority.OwnerAuthorityError,
        "owner authority must be outside Git",
    ),
    (
        lambda p: fmp_usage_authority._read_external_artifact(p / "artifact"),  # noqa: SLF001
        ValueError,
        "authority artifact must be outside a Git repository",
    ),
    (
        lambda p: fmp_collector.validate_destination("output", p),
        fmp_collector.DestinationError,
        "output must be outside a Git repository (resolved repository: {root})",
    ),
    (
        lambda p: sec_collector.validate_destination("output", p),
        sec_collector.DestinationError,
        "output must be outside a Git repository",
    ),
    (
        lambda p: fred_alfred_collector.validate_destination("output", p),
        fred_alfred_collector.DestinationError,
        "output must be outside a Git repository (resolved repository: {root})",
    ),
    (
        lambda p: finimpulse_collector_cli.validate_live_destinations(output=p),
        finimpulse_collector_cli.CollectorError,
        "live output must be outside a Git repository (resolved repository: {root})",
    ),
]
IDS = [
    "storage",
    "compute",
    "journal",
    "container",
    "qveris-key",
    "qveris-store",
    "sec-bulk",
    "fred-raw",
    "fmp-owner",
    "finimpulse-owner",
    "fred-owner",
    "fmp-usage",
    "fmp",
    "sec",
    "fred",
    "finimpulse",
]


@pytest.mark.parametrize(("admit", "error", "message"), CASES, ids=IDS)
@pytest.mark.parametrize("marker", ["empty", "synthetic-file", "repository", "gitdir"])
def test_checkout_admission(
    tmp_path: Path,
    admit: Callable[[Path], object],
    error: type[Exception],
    message: str,
    marker: str,
) -> None:
    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    artifact = root / "artifact"
    artifact.write_text("synthetic-credential\n")
    artifact.chmod(0o600)
    git = root / ".git"
    if marker == "gitdir":
        git.write_text("gitdir: ../external-metadata\n")
    elif marker == "synthetic-file":
        git.write_text("synthetic mount target\n")
    else:
        git.mkdir()
        if marker == "repository":
            init_repository(root)
    if marker in {"repository", "gitdir"}:
        with pytest.raises(error, match="^" + re.escape(message.format(root=root)) + "$"):
            admit(root)
    else:
        admit(root)
    assert artifact.read_text() == "synthetic-credential\n"


def test_compute_checks_only_parents(tmp_path: Path) -> None:
    (tmp_path / ".git").write_text("gitdir: external\n")
    assert compute_lock_path({"AAS_COMPUTE_LOCK_FILE": str(tmp_path)}) == tmp_path
