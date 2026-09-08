"""Distribution boundaries use actual archived members and candidate bytes."""

from __future__ import annotations

import io
import stat
import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts import ci_public


@pytest.mark.parametrize(
    "name",
    [
        "private/record.json",
        "devlog/_plan/private-session.md",
        ".git/config",
        ".venv/lib/site-packages/direct_url.json",
        ".secrets/provider.env",
        "aegis_alpha/strategies/recipe.py",
        "strategy-db-v1.json",
        "data.db",
        ".aas/runtime.json",
        "state.sqlite3-wal",
        "state.sqlite3-shm",
        "market.duckdb.wal",
        "data.DB",
        "src/aegis_alpha/engine/private/recipe.json",
        ".env",
        "config/.env.production",
        "provider.key",
        "certificate.pem",
        "identity.p12",
        "identity.pfx",
        "state.sqlite-wal",
        "state.db-shm",
        "state.db-journal",
        "data/.aas/runtime.json",
        "data/.git/config",
        "data/.secrets/values.json",
        "data/.venv/config.json",
        "data/.codexclaw/session.json",
    ],
)
def test_private_payload_paths_are_rejected(name: str) -> None:
    assert ci_public.check_content(name, b"{}")


def test_personal_values_are_not_echoed() -> None:
    # Assemble prohibited examples without committing literal private locations.
    private_path = "/".join(["", "home", "private-person", "documents", "file"])  # noqa: FLY002
    address = ".".join(["192", "168", "42", "9"])  # noqa: FLY002
    errors = ci_public.check_content("notes.md", f"{private_path}\n{address}".encode())
    assert errors == [
        "notes.md:1: personal home path",
        "notes.md:2: internal network address",
    ]
    assert private_path not in str(errors)
    assert address not in str(errors)


def test_synthetic_text_and_frozen_hashes_are_allowed() -> None:
    assert ci_public.check_content("test.py", b"asset = 'SYNTHETIC_A'\nsha = 'a' * 64\n") == []
    assert ci_public.check_content("Dockerfile", b"USER aas\nWORKDIR /home/aas/app\n") == []
    assert ci_public.check_content("config/.env.example", b"AAS_HOME=~/.aas\n") == []
    assert ci_public.check_content("src/secrets.py", b'"""Credential handling."""') == []


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ("C:" + "\\".join(["", "Users", "private-person", "data"]), "personal home path"),  # noqa: FLY002
        ("/".join(["", "Volumes", "PrivateDisk", "data"]), "operator volume path"),  # noqa: FLY002
        ("ssh-ed25519 " + "A" * 68, "embedded SSH public key"),
        ("SHA256:" + "A" * 43, "embedded SSH fingerprint"),
        ("codex:" + "//threads/00000000", "private task link"),
        ("owner" + "@personal-mail.net", "non-example contact address"),
    ],
)
def test_operating_identity_is_rejected_without_echo(payload: str, reason: str) -> None:
    assert ci_public.check_content("profile.txt", payload.encode()) == [f"profile.txt:1: {reason}"]


def test_json_escaped_windows_path_is_rejected() -> None:
    payload = "C:" + "\\\\".join(["", "Users", "private-person", "data"])  # noqa: FLY002
    assert ci_public.check_content("profile.json", payload.encode()) == [
        "profile.json:1: personal home path"
    ]


def test_wheel_member_is_inspected(tmp_path: Path) -> None:
    wheel = tmp_path / "example.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("example.dist-info/licenses/LICENSE", "license")
        archive.writestr("example.dist-info/licenses/THIRD_PARTY_NOTICES.md", "notices")
        archive.writestr("aegis_alpha/engine/__init__.py", '"""Engine."""')
        archive.writestr("aegis_alpha/strategies/recipe.py", "private = True")
    assert ci_public.check_distribution(wheel) == [
        "aegis_alpha/strategies/recipe.py: private payload path"
    ]


def test_sdist_member_is_inspected(tmp_path: Path) -> None:
    distribution = tmp_path / "example.tar.gz"
    payload = b"{}"
    with tarfile.open(distribution, "w:gz") as archive:
        for name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
            legal = tarfile.TarInfo(f"example-1/{name}")
            legal.size = len(payload)
            archive.addfile(legal, io.BytesIO(payload))
        member = tarfile.TarInfo("example-1/private/strategy.json")
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))
    assert ci_public.check_distribution(distribution) == [
        "private/strategy.json: private payload path"
    ]


def test_distribution_requires_legal_notices(tmp_path: Path) -> None:
    wheel = tmp_path / "example.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("aegis_alpha/__init__.py", "")
    assert ci_public.check_distribution(wheel) == [
        "example.whl: missing LICENSE",
        "example.whl: missing THIRD_PARTY_NOTICES.md",
    ]


def test_sdist_cannot_drop_a_root_level_private_member(tmp_path: Path) -> None:
    distribution = tmp_path / "foreign.tar.gz"
    with tarfile.open(distribution, "w:gz") as archive:
        member = tarfile.TarInfo("strategy-db-v1.json")
        member.size = 2
        archive.addfile(member, io.BytesIO(b"{}"))
    assert "strategy-db-v1.json: missing sdist package prefix" in ci_public.check_distribution(
        distribution
    )


@pytest.mark.parametrize("alias", ["./", "src/../", "src//"])
def test_sdist_rejects_noncanonical_member_paths(tmp_path: Path, alias: str) -> None:
    distribution = tmp_path / "aliased.tar.gz"
    with tarfile.open(distribution, "w:gz") as archive:
        for name in (
            "LICENSE",
            "THIRD_PARTY_NOTICES.md",
            alias + "aegis_alpha/strategies/recipe.py",
        ):
            member = tarfile.TarInfo("example-1/" + name)
            member.size = 2
            archive.addfile(member, io.BytesIO(b"{}"))
    assert ci_public.check_distribution(distribution)


def test_deleted_candidate_file_is_not_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ci_public, "candidate_paths", lambda _root: ["private/deleted.json"])
    assert ci_public.check_candidate(tmp_path) == []


def test_candidate_symlink_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "target"
    target.write_text("content")
    (tmp_path / "link").symlink_to(target)
    monkeypatch.setattr(ci_public, "candidate_paths", lambda _root: ["link"])
    assert ci_public.check_candidate(tmp_path) == ["link: candidate entry must be a regular file"]


@pytest.mark.parametrize(
    "name", ["config/.env.production", "state.sqlite-wal", "data/.aas/runtime.json"]
)
def test_private_paths_are_blocked_in_candidate_and_archives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    payload = b"operator_settings=true\n"
    target = tmp_path / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    monkeypatch.setattr(ci_public, "candidate_paths", lambda _root: [name])
    expected = [f"{name}: private payload path"]
    assert ci_public.check_candidate(tmp_path) == expected
    wheel = tmp_path / "sample.whl"
    sdist = tmp_path / "sample.tar.gz"
    with zipfile.ZipFile(wheel, "w") as archive:
        for member in ("LICENSE", "THIRD_PARTY_NOTICES.md", name):
            archive.writestr(member, payload)
    with tarfile.open(sdist, "w:gz") as archive:
        for member in ("LICENSE", "THIRD_PARTY_NOTICES.md", name):
            entry = tarfile.TarInfo("sample-1/" + member)
            entry.size = len(payload)
            archive.addfile(entry, io.BytesIO(payload))
    assert ci_public.check_distribution(wheel) == expected
    assert ci_public.check_distribution(sdist) == expected


@pytest.mark.parametrize("name", ["aegis_alpha/link", "aegis_alpha/link/"])
def test_wheel_symlink_is_rejected(tmp_path: Path, name: str) -> None:
    wheel = tmp_path / "sample.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        for legal_name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
            archive.writestr(legal_name, "license")
        entry = zipfile.ZipInfo(name)
        entry.create_system = 3
        entry.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(entry, "target")
    assert ci_public.check_distribution(wheel) == [
        f"{name}: distribution entry must be a regular file"
    ]


@pytest.mark.parametrize(
    "name",
    [
        "./example-1/aegis_alpha/strategies/recipe.py",
        "example-1/vendor/aegis_alpha/strategies/recipe.py",
        "../aegis_alpha/module.py",
        "/example-1/aegis_alpha/module.py",
        "other-1/aegis_alpha/module.py",
    ],
)
def test_sdist_checks_full_member_paths_and_one_package_root(tmp_path: Path, name: str) -> None:
    sdist = tmp_path / "sample.tar.gz"
    with tarfile.open(sdist, "w:gz") as archive:
        for member in ("example-1/LICENSE", "example-1/THIRD_PARTY_NOTICES.md", name):
            entry = tarfile.TarInfo(member)
            entry.size = 2
            archive.addfile(entry, io.BytesIO(b"{}"))
    assert ci_public.check_distribution(sdist)
