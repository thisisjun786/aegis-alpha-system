"""Standing-authority admission for historical FMP backfill collection."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from aegis_alpha.collection.records import CollectionMode
from aegis_alpha.data.fmp_cli_artifacts import PreconditionError
from aegis_alpha.data.fmp_dataset_selection import DatasetSelection
from aegis_alpha.data.fmp_live_authorization import (
    AuthorizationContext,
    LiveAuthorization,
)
from aegis_alpha.data.fmp_recurring_authorization import RecurringOperation
from aegis_alpha.data.fmp_recurring_runtime import authorize_recurring_operation


def authorize_backfill_command(
    arguments: argparse.Namespace,
    context: AuthorizationContext,
) -> LiveAuthorization:
    """Authenticate backfill inputs without a per-run approval artifact."""

    if arguments.command != "collect" or arguments.mode != CollectionMode.BACKFILL.value:
        raise PreconditionError("standing authority only covers FMP backfill collection")
    if (
        arguments.owner_approval is not None
        or arguments.owner_approval_signature is not None
        or arguments.max_calls is not None
    ):
        raise PreconditionError("FMP backfill does not accept per-run approval inputs")
    if arguments.recurring_authority is None or arguments.recurring_authority_signature is None:
        raise PreconditionError("FMP backfill requires standing authority artifacts")
    if arguments.operator_from is None:
        raise PreconditionError("FMP backfill requires --from")
    try:
        operator_from = date.fromisoformat(arguments.operator_from)
        selection = DatasetSelection(arguments.dataset_selection)
    except ValueError as error:
        raise PreconditionError("FMP backfill scope is invalid") from error

    authorization = authorize_recurring_operation(
        operation=RecurringOperation(
            command="collect",
            service_day=operator_from,
            output_path=Path(arguments.receipt_path),
            manifest_path=Path(arguments.universe_manifest),
            mode=CollectionMode.BACKFILL,
            selection=selection,
            operator_from=operator_from,
        ),
        recurring_authority_path=Path(arguments.recurring_authority),
        recurring_signature_path=Path(arguments.recurring_authority_signature),
        registry_path=Path(arguments.registry),
        notification_path=Path(arguments.storage_notification),
        tier_path=Path(arguments.tier),
        environment=context.environment,
        moment=context.moment,
        approval_clock=context.approval_clock,
        allow_historical_service_day=True,
    )
    if Path(arguments.raw_store_root).resolve(strict=False) != authorization.raw_store_root:
        raise PreconditionError("raw store root does not match standing authority")
    if Path(arguments.dataset_root).resolve(strict=False) != authorization.dataset_root:
        raise PreconditionError("dataset root does not match standing authority")
    return authorization
