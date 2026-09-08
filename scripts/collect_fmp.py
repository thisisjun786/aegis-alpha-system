"""Operator entry point for the bounded AAS-DATA-004C FMP collector.

Live execution remains fail-closed unless every configured contract,
notification, destination, credential, and trusted historical-usage preflight
passes. Probe, incremental, and universe-build invocations retain their
per-run owner approval. Backfill instead requires a detached-signed standing
authority, rejects per-run approval and ``--max-calls``, and enforces its
signed daily budget against trusted usage. Verification uses the public-only
authority at ``AAS_FMP_OWNER_APPROVAL_AUTHORITY_PATH``; that file and its
immediate directory must be administered by a UID other than the non-root
collector UID. Neither authority grants scheduling, canonical promotion, or
downstream use.

This boundary assumes immutable collector code and credential release. A caller
that can rewrite code, inject imports, or obtain the FMP credential directly can
bypass application checks; deployment must prevent those capabilities.
"""

from __future__ import annotations

import sys

from aegis_alpha.data.fmp_collector_cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
