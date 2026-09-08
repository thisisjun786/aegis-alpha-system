from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from aegis_alpha.collection.registry import CollectionRegistry

if TYPE_CHECKING:
    from sqlalchemy import Engine


@pytest.fixture
def collection_registry(clean_postgres: Engine) -> CollectionRegistry:
    return CollectionRegistry(clean_postgres)
