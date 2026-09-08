"""Operator entry point for the bounded AAS-DATA-012 G-A FRED/ALFRED collector.

G-A performs zero provider calls. Live FRED access remains a separately gated
G-B run and is not authorized by this script.
"""

from __future__ import annotations

import sys

from aegis_alpha.data.fred_alfred_collector_cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
