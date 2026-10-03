"""One isolated home per test process, and the guard that refuses live state.

``tests/conftest.py`` calls :func:`isolate` once per process before any ``aegis_alpha``
module binds a path. Under pytest-xdist every worker is its own process, so every
worker gets its own tree. The tree holds the process's ``HOME``, ``XDG_*_HOME``,
``AAS_HOME`` and ``AAS_DATA_ROOT``; nothing a test writes through the defaults reaches
the operator's ``~/.aas`` or ``~/.local/share/aegis-alpha``.

``HOME`` and ``XDG_*_HOME`` always point into the tree: a login session sets ``HOME``
for every process, so a caller value there says nothing about intent. ``AAS_HOME``
and ``AAS_DATA_ROOT`` keep a caller value, the way mounted real-input acceptance
names its roots; :func:`live_state_refusal` then refuses the run when such a value,
or anything the defaults resolve to, lies inside live state.
"""

from __future__ import annotations

import os
import pwd
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, MutableMapping

XDG_HOMES = ("XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME")
# Caller-supplied roots that product code reads; the guard checks each one that is set.
AAS_PATHS = (
    "AAS_HOME",
    "AAS_DATA_ROOT",
    "AAS_DATA_CONFIG",
    "AAS_INSTALL_CONFIG",
    "AAS_COLLECTION_STATE",
    "AAS_COLLECTION_CONFIG",
)
# The container installation's state mount.
CONTAINER_STATE = Path("/state/aas")
# State a native installation keeps under a user's home.
_HOME_STATE = (".aas", ".local/share/aegis-alpha")


def isolate(root: Path, environ: MutableMapping[str, str]) -> None:
    """Point the process's home, XDG and AAS roots into ``root`` (an existing directory)."""
    homes = {
        "HOME": root / "home",
        "XDG_CONFIG_HOME": root / "xdg-config",
        "XDG_DATA_HOME": root / "xdg-data",
        "XDG_STATE_HOME": root / "xdg-state",
        "XDG_CACHE_HOME": root / "xdg-cache",
    }
    defaults = {"AAS_HOME": root / "aas-home", "AAS_DATA_ROOT": root / "data"}
    for path in (*homes.values(), *defaults.values()):
        path.mkdir()
    for name, path in homes.items():
        environ[name] = os.fspath(path)
    for name, path in defaults.items():
        environ.setdefault(name, os.fspath(path))


def live_roots(*operator_homes: str | None) -> frozenset[Path]:
    """The live state of the account's real home, each named operator home, and the container."""
    homes = {pwd.getpwuid(os.getuid()).pw_dir, *(home for home in operator_homes if home)}
    roots = {Path(home) / name for home in homes for name in _HOME_STATE}
    return frozenset(_resolved(path) for path in (*roots, CONTAINER_STATE))


def live_state_refusal(environ: Mapping[str, str], roots: Iterable[Path]) -> str | None:
    """Why the environment would let a test reach live state, or None when it cannot."""
    home = Path(environ.get("HOME", "/"))
    candidates = {f"HOME/{name}": home / name for name in _HOME_STATE}
    for name in (*XDG_HOMES, *AAS_PATHS):
        value = environ.get(name)
        if value:
            candidates[name] = Path(value).expanduser()
    roots = tuple(roots)
    for name, path in sorted(candidates.items()):
        resolved = _resolved(path)
        for root in roots:
            if resolved == root or root in resolved.parents:
                return (
                    f"tests refuse to run against live state: {name}={path} resolves inside {root}"
                )
    return None


def _resolved(path: Path) -> Path:
    return Path(os.path.realpath(path.expanduser()))
