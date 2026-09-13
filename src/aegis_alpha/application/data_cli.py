from __future__ import annotations

# ruff: noqa: PLC0415 -- lazy DB imports keep modules/status/help independent of installed drivers.
import argparse
import hashlib
import json
import os
from dataclasses import asdict, fields
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from aegis_alpha.application.compute_cli import price_compute
from aegis_alpha.application.data_config import default_config_path, load_data_config, read_secret

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from aegis_alpha.storage.market_inputs import PriceInputRequest
    from aegis_alpha.storage.workspace import Workspace


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


def _input_object(value: object, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("research input object has missing or unknown fields")
    return value


def _input_text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("research input requires text")
    return value


def _input_day(value: object) -> date:
    text = _input_text(value)
    day = date.fromisoformat(text)
    if day.isoformat() != text:
        raise ValueError("research input date must be ISO YYYY-MM-DD")
    return day


def _input_array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("research input requires a JSON array")
    return value


def _input_choice[T: str](value: object, choices: tuple[T, ...]) -> T:
    for choice in choices:
        if value == choice:
            return choice
    raise ValueError("unsupported research input convention")


def _price_request(value: object) -> PriceInputRequest:
    from aegis_alpha.storage.market_inputs import (
        GenerationPin,
        IdentityPin,
        PriceInputRequest,
        UniversePin,
    )

    body = _input_object(value, {field.name for field in fields(PriceInputRequest)})

    def pin[T: (GenerationPin, IdentityPin, UniversePin)](value: object, kind: type[T]) -> T:
        values = _input_object(value, {field.name for field in fields(kind)})
        return kind(**{key: _input_text(item) for key, item in values.items()})

    return PriceInputRequest(
        pin=pin(body["pin"], GenerationPin),
        sessions_pin=None
        if body["sessions_pin"] is None
        else pin(body["sessions_pin"], GenerationPin),
        identity_pin=None
        if body["identity_pin"] is None
        else pin(body["identity_pin"], IdentityPin),
        universe_pin=None
        if body["universe_pin"] is None
        else pin(body["universe_pin"], UniversePin),
        instrument_ids=tuple(_input_text(item) for item in _input_array(body["instrument_ids"])),
        session_dates=tuple(_input_day(item) for item in _input_array(body["session_dates"])),
        currency=_input_text(body["currency"]),
        basis=_input_text(body["basis"]),
        price_role=_input_choice(body["price_role"], ("canonical", "reference")),
        calendar_id=_input_text(body["calendar_id"]),
        venue=_input_text(body["venue"]),
        timezone_version=_input_text(body["timezone_version"]),
        interval=_input_choice(body["interval"], ("1d",)),
        mode=_input_choice(body["mode"], ("strict_pit", "observed_snapshot_research")),
    )


def _read_price_input(workspace: Workspace, path: Path, sha256: str) -> dict[str, object]:
    """Read aas-price-input-request-v1, with no implicit pins, modes or decision cutoffs.

    Root requires schema_version, prices and decision. prices requires EVERY
    PriceInputRequest field, even defaults: pin, sessions_pin, instrument_ids,
    session_dates, currency, basis, price_role, calendar_id, venue,
    timezone_version, interval, mode, identity_pin, universe_pin. Nullable pins
    must be explicit null; generation/identity/universe objects require every
    field of their storage dataclass. Dates are ISO YYYY-MM-DD arrays/scalars.
    decision requires at_us, session_date and nullable ingestion_cutoff_us.
    Microseconds are nonnegative JSON integers (not booleans). read-prices uses
    the existing explicit compute environment/lease; unconfigured reads reject.
    Input and complete indented UTF-8 output each fit MAX_INSPECTION_BYTES.
    """
    from aegis_alpha.data.descriptor_tree import DescriptorTree
    from aegis_alpha.engine.codec import decode_json
    from aegis_alpha.storage.market_inputs import load_pinned_prices
    from aegis_alpha.storage.publication import json_value
    from aegis_alpha.storage.source_reader import MAX_INSPECTION_BYTES

    path = path.absolute()
    with DescriptorTree.open_path(path.parent) as tree:
        raw = tree.read_bytes(path.name, max_bytes=MAX_INSPECTION_BYTES)
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ValueError("research request bytes do not match the expected SHA-256")
    raw.decode("utf-8")  # json.loads(bytes) also accepts UTF-16/32; this contract does not.
    # BOM-less UTF-16/32 ASCII can decode as UTF-8 with NULs, then be autodetected as bytes.
    if b"\x00" in raw:
        raise ValueError("research request requires UTF-8 JSON without literal NUL bytes")
    body = _input_object(decode_json(raw), {"schema_version", "prices", "decision"})
    if body["schema_version"] != "aas-price-input-request-v1":
        raise ValueError("unsupported research request schema")
    request = _price_request(body["prices"])
    decision = _input_object(body["decision"], {"at_us", "session_date", "ingestion_cutoff_us"})
    at_us, cutoff = decision["at_us"], decision["ingestion_cutoff_us"]
    if (
        type(at_us) is not int
        or at_us < 0
        or (cutoff is not None and (type(cutoff) is not int or cutoff < 0))
    ):
        raise ValueError(
            "decision cutoffs require nonnegative integer microseconds or null ingestion"
        )
    day = _input_day(decision["session_date"])
    with price_compute() as budget:
        if budget is None:
            raise ValueError("read-prices requires the explicit AAS compute budget environment")
        projected = load_pinned_prices(workspace, request, budget=budget).project_as_of(
            at_us, session_date=day, ingestion_cutoff_us=cutoff
        )
        result = {
            "request_sha256": sha256,
            "prices": json_value(asdict(request)),
            "decision": decision,
            "rows": json_value([dict(row) for row in projected.rows]),
            "coverage": json_value(
                {
                    **asdict(projected.coverage),
                    "expected_count": projected.coverage.expected_count,
                    "present_count": projected.coverage.present_count,
                    "complete": projected.coverage.complete,
                }
            ),
            "backtest_eligible": False,
        }
        # Match cli.main's encoding, including its newline, before any stdout.
        size = 1
        encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        for chunk in encoder.iterencode(result):
            size += len(chunk.encode("utf-8"))
            if size > MAX_INSPECTION_BYTES:
                raise ValueError("research input output exceeds inspection byte budget")
        return result


def execute_native_data(workspace: Workspace, args: argparse.Namespace) -> dict[str, object]:
    """Delegate exact aas-{price,sessions,proxy}-transform-v1 specs to their sole owner.

    All transforms require schema_version, source, dataset, columns, instruments,
    publication_at_us (nullable), provider and normalizer_version. source requires
    source_id/source_sha256/table/table_digest; dataset requires
    dataset_id/version/generation_id/operation_id/parent_id (nullable).
    Each instrument requires instrument_id/asset_type/venue; sessions require [].

    columns maps every COMMON field: generation_id, record_id, revision_id,
    supersedes_revision_id, op, available_at_us, revision_known_at_us,
    ingested_at_us, source_snapshot_id, source_row_hash; plus every domain field:
    prices: instrument_id/session_date/interval/bar_end_us/basis/currency/
    open/high/low/close/volume/price_role/value_state;
    sessions: calendar_id/venue/session_date/open_at_us/close_at_us/status/timezone_version;
    proxy: contract_id/contract_version/contract_hash/input_bundle_hash/instrument_id/
    feature_at_us/value/value_state. Mappings use distinct actual source columns.

    Prices additionally require price {basis,currency,price_role}, calendar
    {calendar_id,timezone,timezone_version}, decimal_conversion {open,high,low,
    close,volume}, each decimal_string or ieee_float. Sessions instead require
    calendar {calendar_id,venue,timezone,timezone_version}. Proxy instead requires
    proxy {proxy_id,version,normalization,transition}; normalization requires
    input_number (decimal_string/ieee_float), output (ieee754_binary64).
    transition requires donor_id/target_id/logical_exposure_id/switch_decision_date/
    mode (signal_only/observed_instrument_switch), donor_source/target_source
    (SourcePins), basis_ref/calendar_ref/cost_ref (each {id,version,sha256}).
    storage.research_inputs owns validation, numeric policies and feature hashes;
    its bounded read and strict schema are reused without a parallel publication
    path. Other native data commands retain publication.execute_data unchanged.
    """
    from aegis_alpha.storage.research_inputs import (
        register_price_input,
        register_proxy_input,
        register_sessions_input,
    )

    match args.data_command:
        case "register-prices":
            return register_price_input(workspace, args.spec, args.sha256)
        case "register-sessions":
            return register_sessions_input(workspace, args.spec, args.sha256)
        case "register-proxy":
            return register_proxy_input(workspace, args.spec, args.sha256)
        case "read-prices":
            return _read_price_input(workspace, args.request, args.sha256)
        case _:
            from aegis_alpha.storage.publication import execute_data

            return execute_data(workspace, args)


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
