"""A synthetic core schema step after the newest real one, for the step runner's tests.

Its texts change nothing, so a store at that version holds exactly the objects of the
version before and only its receipts and checksums are new. Every module that binds the
version tables is patched together, the way a real new version would change them all.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Final

from aegis_alpha.storage import market, migration, state, workspace
from aegis_alpha.storage.sqlite import schema_checksums

if TYPE_CHECKING:
    import pytest

MARKET_STEP: Final = "-- synthetic core step: the market store changes nothing\n"
STATE_STEP: Final = "-- synthetic core step: the state store changes nothing\n"


def add_synthetic_step(patch: pytest.MonkeyPatch) -> int:
    """Make this code know one more core version; return that version."""
    market_texts = (*market.MIGRATIONS, MARKET_STEP)
    state_texts = (*state.MIGRATIONS, STATE_STEP)
    market_checksums = tuple(hashlib.sha256(text.encode()).hexdigest() for text in market_texts)
    state_checksums = schema_checksums(state_texts)
    version = len(market_texts)
    patch.setattr(market, "MIGRATIONS", market_texts)
    patch.setattr(market, "MARKET_CHECKSUMS", market_checksums)
    patch.setattr(state, "MIGRATIONS", state_texts)
    patch.setattr(migration, "MARKET_CHECKSUMS", market_checksums)
    patch.setattr(migration, "STATE_CHECKSUMS", state_checksums)
    patch.setattr(migration, "CORE_VERSION", version)
    patch.setattr(workspace, "CORE_VERSIONS", tuple(range(1, version + 1)))
    return version
