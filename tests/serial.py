"""Serial groups: resources one test run shares across its worker processes."""

from __future__ import annotations

# The Qveris store binds a host-wide abstract socket named by the account, and the
# synthetic clients reuse an account within a process. Keep the explicit group and guard
# while tests/isolation.py separates accounts across independent pytest processes.
QVERIS_LEASE_GROUP = "qveris-account-lease"
