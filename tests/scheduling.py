"""Whole-file xdist scheduling that keeps declared serial groups on one worker.

``--dist loadgroup`` sends every test marked ``@pytest.mark.xdist_group(name)`` to one
worker as one unit; xdist's own scheduler then spreads the unmarked tests one by one,
which would rebuild a file's module fixtures on several workers. Here the unmarked tests
are scheduled by file instead, exactly as ``--dist loadfile`` does, so a group spans the
files that share a host-wide resource and every other file still runs whole.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from xdist.scheduler import LoadGroupScheduling


def scope_of(nodeid: str) -> str:
    """The group a marked test belongs to, else the test's file."""
    # xdist appends ``@<group>``; a ``]`` after the last ``@`` means a parameter holds the @.
    if nodeid.rfind("@") > nodeid.rfind("]"):
        return nodeid.rsplit("@", 1)[1]
    return nodeid.split("::", 1)[0]


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_make_scheduler(config: pytest.Config, log: Any) -> LoadGroupScheduling | None:  # noqa: ANN401 -- xdist's Producer
    if config.getvalue("dist") != "loadgroup":
        return None
    from xdist.scheduler import LoadGroupScheduling  # noqa: PLC0415 -- only under xdist

    class FileOrGroupScheduling(LoadGroupScheduling):
        def _split_scope(self, nodeid: str) -> str:
            return scope_of(nodeid)

    return FileOrGroupScheduling(config, log)
