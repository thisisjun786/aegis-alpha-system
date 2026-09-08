from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy DB imports keep modules/status/help independent of installed drivers.
import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.application.data_config import default_config_path, load_data_config, read_secret

if TYPE_CHECKING:
    from sqlalchemy import Engine


def add_commands(commands: argparse._SubParsersAction) -> None:
    db = commands.add_parser("legacy-db", help="Transitional PostgreSQL installation utilities")
    db_commands = db.add_subparsers(dest="db_command", required=True)
    status = db_commands.add_parser("status", help="Read database schema and adoption status")
    status.add_argument("--config", type=Path, default=default_config_path())
    install = db_commands.add_parser(
        "install", help="Create an empty app DB through owned schema template"
    )
    install.add_argument("--admin-url-file", type=Path, required=True)
    install.add_argument("--database", required=True)
    install.add_argument("--runtime-role", required=True)
    install.add_argument("--runtime-password-file", type=Path, required=True)
    install.add_argument(
        "--project-root",
        type=Path,
        default=Path(os.environ.get("AAS_PROJECT_ROOT", str(Path(__file__).resolve().parents[3]))),
    )
    adopt = db_commands.add_parser("adopt", help="Atomically import a verified legacy snapshot")
    adopt.add_argument("--admin-url-file", type=Path, required=True)
    adopt.add_argument("--database", required=True)
    adopt.add_argument("--snapshot", type=Path, required=True)
    adopt.add_argument("--sha256", required=True)
    data = commands.add_parser("legacy-data", help="Transitional Parquet publication inspection")
    data_commands = data.add_subparsers(dest="data_command", required=True)
    for name in ("datasets", "inspect", "prices"):
        parser = data_commands.add_parser(name)
        parser.add_argument("--config", type=Path, default=default_config_path())
        if name != "datasets":
            parser.add_argument("--dataset", required=True)
            parser.add_argument("--version", required=True)
        if name == "prices":
            _price_options(parser)


def _price_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--instrument", action="append", required=True)
    parser.add_argument("--from", dest="start_date", type=date.fromisoformat, required=True)
    parser.add_argument("--to", dest="end_date", type=date.fromisoformat, required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--basis", required=True, help="Explicit canonical adjustment basis")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--inspect", action="store_true", help="Inspect bytes without claiming backtest eligibility"
    )


def _status(engine: Engine) -> dict[str, object]:
    from sqlalchemy import text

    with engine.connect() as connection:
        info = (
            connection.execute(
                text(
                    "SELECT current_database() AS database, current_user AS role, "
                    "(SELECT version_num FROM public.alembic_version) AS schema_head"
                )
            )
            .mappings()
            .one()
        )
        count = connection.scalar(text("SELECT count(*) FROM engine.data_adoptions"))
        return {
            **dict(info),
            "connected": True,
            "read_only": connection.scalar(text("SHOW transaction_read_only")) == "on",
            "adoptions": count,
            "backtest_execution": False,
            "provider_live_verification": False,
        }


def _db_mutation(args: argparse.Namespace) -> dict[str, object]:
    from aegis_alpha.metadata.runtime_install import (
        InstallRequest,
        install_runtime,
        parse_runtime_url,
        runtime_engine,
    )
    from aegis_alpha.metadata.snapshot_import import adopt_snapshot

    admin = read_secret(args.admin_url_file)
    if args.db_command == "install":
        return install_runtime(
            admin,
            InstallRequest(
                args.database, args.runtime_role, read_secret(args.runtime_password_file)
            ),
            args.project_root,
        )
    url = parse_runtime_url(admin).set(database=args.database)
    engine = runtime_engine(url.render_as_string(hide_password=False))
    try:
        return adopt_snapshot(engine, args.snapshot, args.sha256)
    finally:
        engine.dispose()


def execute(args: argparse.Namespace) -> dict[str, object]:
    """Lazily load database dependencies only for explicit DB/data commands."""
    from sqlalchemy.exc import SQLAlchemyError

    try:
        if args.command == "db" and args.db_command != "status":
            return _db_mutation(args)
        return _read_command(args)
    except SQLAlchemyError:
        raise ValueError(
            "database operation failed; check connection and schema readiness"
        ) from None


def _read_command(args: argparse.Namespace) -> dict[str, object]:
    from aegis_alpha.data.catalog_access import list_datasets, load_dataset
    from aegis_alpha.data.contracts import AdjustmentBasis
    from aegis_alpha.data.pinned_prices import PriceQuery, read_prices
    from aegis_alpha.metadata.runtime_install import runtime_engine

    config = load_data_config(args.config)
    engine = runtime_engine(read_secret(config.database_url_file), read_only=True)
    try:
        if args.command == "db":
            return _status(engine)
        if args.data_command == "datasets":
            return {"datasets": list_datasets(engine)}
        view = load_dataset(engine, args.dataset, args.version)
        if args.data_command == "inspect":
            return view.to_dict()
        query = PriceQuery(
            args.start_date,
            args.end_date,
            args.cutoff,
            tuple(args.instrument),
            AdjustmentBasis(args.basis),
            args.limit,
            "inspection" if args.inspect else "backtest",
        )
        with price_compute() as budget:
            result = read_prices(
                view, config.dataset_root(args.dataset, args.version), query, budget=budget
            )
            if budget is not None:
                result["compute"] = budget.to_dict()
            return result
    finally:
        engine.dispose()
