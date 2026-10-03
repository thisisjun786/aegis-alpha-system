"""Serial groups: resources one test run shares across its worker processes."""

from __future__ import annotations

# The Qveris store binds a host-wide abstract socket named by the account, and the
# synthetic accounts are fixed, so every test that takes it shares this serial group;
# tests/conftest.py fails a test that takes the lease outside it.
QVERIS_LEASE_GROUP = "qveris-account-lease"
