from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from aegis_alpha.application import (
    backtest_cli,
    compute_cli,
    data_cli,
    etf_cli,
    provider_cli,
    proxy_cli,
    research_cli,
    storage_cli,
)
from aegis_alpha.application.contracts import parse_request
from aegis_alpha.application.portfolio import compose_portfolio
from aegis_alpha.modules.catalog import module_catalog


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aas", description="Aegis Alpha standalone research CLI")
    parser.add_argument("--home", type=Path, help="AAS user data directory (default: ~/.aas)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("modules", help="Show the three module contracts")
    commands.add_parser("status", help="Show implemented capabilities and pending work")
    commands.add_parser("resources", help="Show configured CPU/memory limits and compute lease")
    preview = commands.add_parser(
        "preview", help="Validate and combine explicit module allocations"
    )
    preview.add_argument("--input", required=True, type=Path, help="Portfolio request JSON file")
    storage_cli.add_commands(commands)
    data_cli.add_commands(commands)
    provider_cli.add_commands(commands)
    backtest_cli.add_commands(commands)
    proxy_cli.add_commands(commands)
    research_cli.add_commands(commands)
    etf_cli.add_commands(commands)
    return parser


def _status() -> dict[str, object]:
    return {
        "application": "aegis-alpha-system",
        "mode": "standalone-cli",
        "capabilities": {
            "allocation_preview": True,
            "strategy_execution": False,
            "aegis_etf_target_replay": True,
            "research_proxy_returns": True,
            "research_candidate_generation": True,
            "database_adapter": True,
            "provider_collection": True,
            "daily_collection": True,
            "compute_budget": True,
            "live_orders": False,
            "etf_candidate_comparison": True,
        },
        "modules": module_catalog(),
        "runtime_dependencies": {
            "vibe_trading": False,
            "database_for_preview": False,
            "database_server": False,
            "docker": False,
        },
        "storage": {"state": "sqlite", "strategies": "sqlite", "market": "duckdb"},
        "provider_storage": "legacy_adapter_pending_migration",
    }


def main(argv: list[str] | None = None) -> int:  # noqa: C901, PLR0912 -- explicit command dispatch
    args = _parser().parse_args(argv)
    try:
        match args.command:
            case "modules":
                result = {"modules": module_catalog()}
            case "status":
                result = _status()
            case "resources":
                result = compute_cli.compute_status()
            case "backtest":
                result = backtest_cli.execute(args)
            case "proxy":
                result = proxy_cli.execute(args)
            case "research":
                result = research_cli.execute(args)
            case "etfs":
                result = etf_cli.execute(args)
            case "providers" | "collect":
                result = provider_cli.execute(args)
            case "legacy-db" | "legacy-data":
                args.command = args.command.removeprefix("legacy-")
                result = data_cli.execute(args)
            case "init" | "doctor" | "db" | "data" | "strategy":
                result = storage_cli.execute(args)
            case "preview":
                request = parse_request(args.input.read_text(encoding="utf-8"))
                result = compose_portfolio(request)
            case _:
                raise ValueError("unknown command")  # noqa: TRY301 -- unreachable after argparse choices
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False))  # noqa: T201 -- CLI output
    except (ValueError, OSError, RuntimeError, TypeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)  # noqa: T201 -- CLI diagnostic
        return 1
    code = result.get("exit_code", 0) if args.command == "collect" else 0
    return code if type(code) is int else 1
