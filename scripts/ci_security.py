"""Audit locked PyPI packages through the public OSV querybatch API."""

from __future__ import annotations

import argparse
import json
import re
import tomllib
from pathlib import Path
from typing import Protocol, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

OSV_QUERYBATCH = "https://api.osv.dev/v1/querybatch"
OSV_VULN = "https://api.osv.dev/v1/vulns/"
OSV_HOST_PREFIX = "https://api.osv.dev/"
LOCKS = ("uv.lock",)
BATCH_SIZE = 100
TIMEOUT_SECONDS = 30
HTTP_OK = 200
VULN_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{1,127}$")


class OsvTransport(Protocol):
    def __call__(self, url: str, payload: dict[str, object] | None = None, /) -> object: ...


def json_object(value: object, *, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"invalid {name}")
    return cast("dict[str, object]", value)


def locked_packages(path: Path) -> list[tuple[str, str]]:
    if not path.is_file():
        raise ValueError(f"{path}: lockfile is missing")
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    packages = raw.get("package")
    if not isinstance(packages, list) or not packages:
        raise ValueError(f"{path}: lockfile has no packages")
    locked: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in packages:
        package = json_object(entry, name=f"{path} package")
        source = json_object(package.get("source"), name=f"{path} source")
        if "registry" not in source:
            continue
        name = package.get("name")
        version = package.get("version")
        if not isinstance(name, str) or not isinstance(version, str) or not name or not version:
            raise ValueError(f"{path}: registry package is missing a name or version")
        item = (name, version)
        if item in seen:
            raise ValueError(f"{path}: duplicate locked package {name}=={version}")
        seen.add(item)
        locked.append(item)
    if not locked:
        raise ValueError(f"{path}: no registry packages to audit")
    return locked


def lock_inventory(root: Path) -> dict[str, list[tuple[str, str]]]:
    return {relative: locked_packages(root / relative) for relative in LOCKS}


def unique_packages(inventory: dict[str, list[tuple[str, str]]]) -> list[tuple[str, str]]:
    packages = {item for items in inventory.values() for item in items}
    return sorted(packages)


def osv_request(url: str, payload: dict[str, object] | None = None) -> object:
    if not url.startswith(OSV_HOST_PREFIX):
        raise ValueError("unexpected OSV URL")
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(  # noqa: S310 -- host-prefixed public OSV HTTPS endpoint
        url,
        data=data,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 -- host-prefixed public OSV HTTPS endpoint
            status = getattr(response, "status", None)
            if status != HTTP_OK:
                raise ValueError(f"OSV HTTP {status}")
            raw = response.read()
    except HTTPError as exc:
        raise ValueError(f"OSV HTTP {exc.code}") from exc
    except (OSError, URLError, TimeoutError) as exc:
        raise ValueError(f"OSV transport failed: {exc}") from exc
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("OSV response is not JSON") from exc


def vulnerability_id(entry: object) -> str:
    payload = json_object(entry, name="OSV vulnerability")
    identity = payload.get("id")
    if not isinstance(identity, str) or not VULN_ID.fullmatch(identity):
        raise ValueError("OSV vulnerability is missing an identifier")
    return identity


def parse_result(value: object) -> tuple[list[str], str | None]:
    payload = json_object(value, name="OSV query result")
    if set(payload) - {"vulns", "next_page_token"}:
        raise ValueError("unexpected OSV query result fields")
    if "vulns" in payload and payload["vulns"] is None:
        raise ValueError("OSV vulns must not be null")
    found = payload.get("vulns", [])
    if not isinstance(found, list):
        raise TypeError("OSV vulns must be a list")
    ids = [vulnerability_id(item) for item in found]
    next_token = payload.get("next_page_token")
    if next_token in (None, ""):
        return ids, None
    if not isinstance(next_token, str):
        raise TypeError("OSV next_page_token must be a string")
    return ids, next_token


def query_package(query: dict[str, object]) -> dict[str, object]:
    package = json_object(query.get("package"), name="OSV package")
    name = package.get("name")
    ecosystem = package.get("ecosystem")
    version = query.get("version")
    if ecosystem != "PyPI" or not isinstance(name, str) or not isinstance(version, str):
        raise ValueError("OSV query is not a PyPI package version")
    payload: dict[str, object] = {
        "package": {"name": name, "ecosystem": "PyPI"},
        "version": version,
    }
    token = query.get("page_token")
    if isinstance(token, str) and token:
        payload["page_token"] = token
    return payload


def query_batch(
    queries: list[dict[str, object]],
    *,
    transport: OsvTransport,
) -> list[object]:
    if not queries or len(queries) > BATCH_SIZE:
        raise ValueError("OSV querybatch size is invalid")
    payload = json_object(
        transport(OSV_QUERYBATCH, {"queries": queries}),
        name="OSV querybatch response",
    )
    results = payload.get("results")
    if not isinstance(results, list) or len(results) != len(queries):
        raise ValueError("OSV querybatch result count mismatch")
    # The result array is intentionally parsed element by element below.
    return cast("list[object]", results)


def collect_vulnerability_ids(
    packages: list[tuple[str, str]],
    *,
    transport: OsvTransport,
) -> dict[tuple[str, str], list[str]]:
    pending: list[tuple[tuple[str, str], str | None]] = [(item, None) for item in packages]
    seen_tokens: dict[tuple[str, str], set[str]] = {item: set() for item in packages}
    found: dict[tuple[str, str], list[str]] = {item: [] for item in packages}
    while pending:
        batch = pending[:BATCH_SIZE]
        pending = pending[BATCH_SIZE:]
        queries = [
            query_package(
                {
                    "package": {"name": name, "ecosystem": "PyPI"},
                    "version": version,
                    **({} if token is None else {"page_token": token}),
                }
            )
            for (name, version), token in batch
        ]
        for ((name, version), _token), result in zip(
            batch, query_batch(queries, transport=transport), strict=True
        ):
            ids, next_token = parse_result(result)
            found[name, version].extend(ids)
            if next_token is None:
                continue
            if next_token in seen_tokens[name, version]:
                raise ValueError("OSV pagination repeated a page token")
            seen_tokens[name, version].add(next_token)
            pending.append(((name, version), next_token))
    return found


def vuln_url(identity: str) -> str:
    if not VULN_ID.fullmatch(identity):
        raise ValueError("OSV vulnerability is missing an identifier")
    return f"{OSV_VULN}{identity}"


def alias_group(identity: str, *, transport: OsvTransport) -> set[str]:
    payload = json_object(transport(vuln_url(identity), None), name="OSV vulnerability detail")
    observed = vulnerability_id(payload)
    if observed != identity:
        raise ValueError("OSV detail identifier mismatch")
    ids = {observed}
    aliases = payload.get("aliases", [])
    if aliases is None:
        raise ValueError("OSV aliases must not be null")
    if not isinstance(aliases, list):
        raise TypeError("OSV aliases must be a list")
    for alias in aliases:
        if not isinstance(alias, str) or not VULN_ID.fullmatch(alias):
            raise ValueError("OSV aliases must be identifiers")
        ids.add(alias)
    return ids


def merge_groups(groups: list[set[str]]) -> list[tuple[str, ...]]:
    merged: list[set[str]] = []
    for ids in groups:
        overlap = [group for group in merged if group & ids]
        combined = set(ids)
        for group in overlap:
            combined |= group
            merged.remove(group)
        merged.append(combined)
    return [
        tuple(sorted(group))
        for group in sorted(merged, key=lambda group: next(iter(sorted(group))))
    ]


def audit_locks(
    root: Path,
    *,
    transport: OsvTransport = osv_request,
) -> dict[str, list[tuple[str, str, tuple[str, ...]]]]:
    inventory = lock_inventory(root)
    packages = unique_packages(inventory)
    raw_ids = collect_vulnerability_ids(packages, transport=transport)
    details: dict[str, set[str]] = {}
    findings: dict[str, list[tuple[str, str, tuple[str, ...]]]] = {lock: [] for lock in LOCKS}
    for package, identities in raw_ids.items():
        if not identities:
            continue
        groups = []
        for identity in identities:
            if identity not in details:
                details[identity] = alias_group(identity, transport=transport)
            groups.append(details[identity])
        identifiers = merge_groups(groups)
        for lock, locked in inventory.items():
            if package in locked:
                for group in identifiers:
                    findings[lock].append((package[0], package[1], group))
    return findings


def render_findings(findings: dict[str, list[tuple[str, str, tuple[str, ...]]]]) -> str:
    lines = [
        f"{lock}: {name}=={version}: {', '.join(identifiers)}"
        for lock, items in findings.items()
        for name, version, identifiers in items
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    findings = audit_locks(args.root)
    rendered = render_findings(findings)
    if rendered:
        raise SystemExit(rendered)
    print("locked-package OSV audit: pass")  # noqa: T201 -- CI result


if __name__ == "__main__":
    main()
