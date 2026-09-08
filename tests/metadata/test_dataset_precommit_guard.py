"""AAS-DATA-006C: dataset registration guards must roll back real transactions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import func, select
from test_registry import (
    _dataset_registration,
    _source_registration,
)

from aegis_alpha.metadata.registry import MetadataRegistry
from aegis_alpha.metadata.schema import (
    dataset_artifacts,
    dataset_versions,
    quality_results,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine


class _GuardRejectedError(RuntimeError):
    """Raised by a precommit guard to abort the writing transaction."""


def _reject() -> None:
    raise _GuardRejectedError("publication changed after verification")


def _counts(engine: Engine) -> tuple[int, int, int]:
    with engine.connect() as connection:
        return (
            int(connection.scalar(select(func.count()).select_from(dataset_versions)) or 0),
            int(connection.scalar(select(func.count()).select_from(dataset_artifacts)) or 0),
            int(connection.scalar(select(func.count()).select_from(quality_results)) or 0),
        )


def test_dataset_precommit_guard_rolls_back_the_whole_registration(
    clean_postgres: Engine,
) -> None:
    """A rejected guard must leave no dataset, artifact, or quality row behind."""

    registry = MetadataRegistry(clean_postgres)
    registry.register_source_snapshot(_source_registration())

    with pytest.raises(_GuardRejectedError):
        registry.register_dataset(_dataset_registration(), precommit_guard=_reject)

    assert _counts(clean_postgres) == (0, 0, 0)


def test_dataset_precommit_guard_commits_when_it_passes(clean_postgres: Engine) -> None:
    registry = MetadataRegistry(clean_postgres)
    registry.register_source_snapshot(_source_registration())

    registry.register_dataset(_dataset_registration(), precommit_guard=lambda: None)

    versions, artifacts, quality = _counts(clean_postgres)
    assert versions == 1
    assert artifacts > 0
    assert quality > 0


def test_idempotent_dataset_replay_still_honors_the_precommit_guard(
    clean_postgres: Engine,
) -> None:
    """The already-registered path must not bypass the invariant."""

    registry = MetadataRegistry(clean_postgres)
    registry.register_source_snapshot(_source_registration())
    registration = _dataset_registration()
    registry.register_dataset(registration)
    before = _counts(clean_postgres)

    with pytest.raises(_GuardRejectedError):
        registry.register_dataset(registration, precommit_guard=_reject)

    assert _counts(clean_postgres) == before
