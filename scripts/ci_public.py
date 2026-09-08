"""Check candidate/distribution contents for known private material, without printing it."""

from __future__ import annotations

import argparse
import re
import stat
import subprocess
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

PRIVATE_PREFIXES = (
    ".git/",
    ".aas/",
    ".venv/",
    ".secrets/",
    ".codexclaw/",
    ".re0/",
    "private/",
    "strategy-db/",
    "strategy-data/",
    "dev-notes/archive/",
    "devlog/",
    "artifacts/",
    "config/postgres/targets/",
    "src/aegis_alpha/strategies/",
    "tests/strategies/",
    "tests/fixtures/provider_neutral/daa_contract/",
    "aegis_alpha/strategies/",
)
PRIVATE_SUFFIXES = (
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".duckdb",
    ".duckdb.wal",
    ".sqlite3-wal",
    ".sqlite3-shm",
    ".sqlite3-journal",
    ".parquet",
    ".dump",
    ".zip",
)
PRIVATE_COMPONENTS = frozenset(
    {
        ".git",
        ".aas",
        ".venv",
        ".secrets",
        ".codexclaw",
        ".re0",
        "private",
        "strategy-db",
        "strategy-data",
        "strategies",
    }
)
SQLITE_SIDECAR = re.compile(r"\.(?:db|sqlite3?)-(?:wal|shm|journal)$")
HOME_PATH = re.compile(r"/(?:home|Users)/([^/\s\"'<>`]+)/")
WINDOWS_HOME = re.compile(r"[A-Za-z]:[\\/]+Users[\\/]+([^\\/\s\"'<>`]+)[\\/]+")
EXAMPLE_HOMES = frozenset({"aas", "user", "example", "runner", "test", "owner"})
VOLUME_PATH = re.compile(r"/Volumes" + r"/[^/\s\"'<>`]+/")
SSH_KEY = re.compile(r"\b(?:ssh-(?:ed25519|rsa)|ecdsa-sha2-nistp\d+)\s+[A-Za-z0-9+/]{40,}={0,3}")
SSH_FINGERPRINT = re.compile(r"\bSHA256:[A-Za-z0-9+/]{43}(?:=)?")
EMAIL = re.compile(r"[\w.+-]+@([\w.-]+\.[A-Za-z]{2,})")
# Provider-owned contact strings can occur in synthetic error-response fixtures.
PUBLIC_EMAIL_DOMAINS = frozenset({"sec.gov", "financialmodelingprep.com"})
PRIVATE_IP = re.compile(
    r"(?<![\d.])(?:10\.(?:\d{1,3}\.){2}\d{1,3}|"
    r"192\.168\.\d{1,3}\.\d{1,3}|"
    r"172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|"
    r"100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3})(?![\d.])"
)
OPERATING_MARKERS = (
    ("operator volume path", VOLUME_PATH),
    ("embedded SSH public key", SSH_KEY),
    ("embedded SSH fingerprint", SSH_FINGERPRINT),
    ("private task link", re.compile("codex:" + "//threads/")),
    ("internal network address", PRIVATE_IP),
)
MAX_TEXT_BYTES = 8 * 1024 * 1024
ZIP_UNIX_SYSTEM = 3


def check_content(name: str, payload: bytes) -> list[str]:
    """Return locations/reasons only; never echo matched private values."""
    path = PurePosixPath(name)
    if not name or path.is_absolute() or ".." in path.parts or str(path) != name or "\\" in name:
        return [f"{name}: unsafe content path"]
    normalized = name.lower()
    basename = path.name.lower()
    if (
        normalized.startswith(PRIVATE_PREFIXES)
        or normalized.endswith((*PRIVATE_SUFFIXES, ".tar", ".tar.gz", ".tar.zst"))
        or SQLITE_SIDECAR.search(normalized)
        or {part.lower() for part in path.parts} & PRIVATE_COMPONENTS
        or (basename != ".env.example" and (basename == ".env" or basename.startswith(".env.")))
        or path.name.lower().startswith(("strategy-db", "strategy-flat", "backtest-strategy-list"))
    ):
        return [f"{name}: private payload path"]
    if len(payload) > MAX_TEXT_BYTES or b"\0" in payload:
        return [f"{name}: binary or oversized payload requires explicit distribution review"]
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return [f"{name}: non-text payload requires explicit distribution review"]
    errors: list[str] = []
    for number, line in enumerate(text.splitlines(), 1):
        if any(
            match.group(1) not in EXAMPLE_HOMES
            for pattern in (HOME_PATH, WINDOWS_HOME)
            for match in pattern.finditer(line)
        ):
            errors.append(f"{name}:{number}: personal home path")
        for reason, pattern in OPERATING_MARKERS:
            if pattern.search(line):
                errors.append(f"{name}:{number}: {reason}")
        if any(not example_email_domain(match.group(1)) for match in EMAIL.finditer(line)):
            errors.append(f"{name}:{number}: non-example contact address")
    return errors


def example_email_domain(domain: str) -> bool:
    domain = domain.lower()
    return (
        domain in PUBLIC_EMAIL_DOMAINS
        or domain.endswith((".test", ".invalid"))
        or any(
            domain == example or domain.endswith("." + example)
            for example in ("example.com", "example.org", "example.net")
        )
    )


def candidate_paths(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],  # noqa: S607
        cwd=root,
        capture_output=True,
        check=True,
        timeout=30,
    )
    return sorted(set(result.stdout.decode("utf-8").split("\0")) - {""})


def check_candidate(root: Path) -> list[str]:
    errors: list[str] = []
    for name in candidate_paths(root):
        path = root / name
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            continue  # A tracked deletion is absent from the candidate being reviewed.
        if not stat.S_ISREG(mode):
            errors.append(f"{name}: candidate entry must be a regular file")
            continue
        with path.open("rb") as source:
            errors.extend(check_content(name, source.read(MAX_TEXT_BYTES + 1)))
    return errors


def _wheel_contents(path: Path) -> tuple[list[str], list[str]]:
    names: list[str] = []
    errors: list[str] = []
    with zipfile.ZipFile(path) as archive:
        for member in archive.infolist():
            kind = stat.S_IFMT(member.external_attr >> 16)
            if member.is_dir() and (
                member.create_system != ZIP_UNIX_SYSTEM or kind in (0, stat.S_IFDIR)
            ):
                continue
            names.append(member.filename)
            if member.create_system == ZIP_UNIX_SYSTEM and kind not in (0, stat.S_IFREG):
                errors.append(f"{member.filename}: distribution entry must be a regular file")
            elif member.file_size > MAX_TEXT_BYTES:
                errors.append(f"{member.filename}: oversized distribution entry")
            else:
                errors.extend(check_content(member.filename, archive.read(member)))
    return names, errors


def _sdist_contents(path: Path) -> tuple[list[str], list[str]]:
    names: list[str] = []
    errors: list[str] = []
    package_root: str | None = None
    with tarfile.open(path) as archive:
        for member in archive:
            full_name = member.name.rstrip("/") if member.isdir() else member.name
            full_path = PurePosixPath(full_name)
            if (
                not full_name
                or full_path.is_absolute()
                or ".." in full_path.parts
                or str(full_path) != full_name
                or "\\" in full_name
            ):
                errors.append(f"{member.name}: unsafe sdist path")
                continue
            if not member.isdir() and len(full_path.parts) == 1:
                errors.append(f"{member.name}: missing sdist package prefix")
                continue
            if package_root is None:
                package_root = full_path.parts[0]
            if full_path.parts[0] != package_root:
                errors.append(f"{member.name}: inconsistent sdist package prefix")
                continue
            if member.isdir():
                continue
            if not member.isfile():
                errors.append(f"{member.name}: distribution entry must be a regular file")
                continue
            name = member.name.partition("/")[2]
            names.append(name)
            source = archive.extractfile(member)
            if source is None:
                errors.append(f"{member.name}: unreadable distribution entry")
            else:
                with source:
                    errors.extend(check_content(name, source.read(MAX_TEXT_BYTES + 1)))
    return names, errors


def check_distribution(path: Path) -> list[str]:
    names, errors = _wheel_contents(path) if path.suffix == ".whl" else _sdist_contents(path)
    errors.extend(
        f"{path.name}: missing {required}"
        for required in ("LICENSE", "THIRD_PARTY_NOTICES.md")
        if not any(PurePosixPath(name).name == required for name in names)
    )
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--artifact", type=Path, action="append", default=[])
    args = parser.parse_args()
    errors = check_candidate(args.root)
    for artifact in args.artifact:
        errors.extend(check_distribution(artifact))
    if errors:
        raise SystemExit("\n".join(errors))
    print("candidate/distribution private-content checks: pass")  # noqa: T201


if __name__ == "__main__":
    main()
