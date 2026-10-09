"""A DuckDB connection whose COMMIT fails the way an exhausted one does.

DuckDB ends a transaction whose COMMIT failed, so a later ROLLBACK finds none. The
adapter reproduces exactly that on a real connection: COMMIT rolls back and raises.
"""

from __future__ import annotations

from typing import Final, cast
from unittest.mock import Mock

import duckdb

# What a promotion COMMIT raises when its index blocks no longer fit DuckDB's memory limit.
PIN_BLOCK: Final = (
    "TransactionContext Error: Failed to commit: failed to pin block of size 256.0 KiB "
    "(48.0 MiB/48.0 MiB used)"
)


class FailingCommit:
    """Forward SQL to the real connection, except that COMMIT ends the transaction and fails."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, message: str = PIN_BLOCK) -> None:
        self.connection: duckdb.DuckDBPyConnection = connection
        self.message: str = message
        self.commits: int = 0
        self.settings: list[tuple[int, str]] = []
        # A spec-typed forwarding adapter; every other statement reaches the real connection.
        self.borrowed: duckdb.DuckDBPyConnection = cast(
            "duckdb.DuckDBPyConnection", Mock(spec=duckdb.DuckDBPyConnection, wraps=self)
        )

    def execute(
        self, query: str, parameters: list[object] | None = None
    ) -> duckdb.DuckDBPyConnection:
        if query == "COMMIT":
            self.commits += 1
            self.settings.append(
                cast(
                    "tuple[int, str]",
                    self.connection.execute(
                        "SELECT current_setting('threads'), current_setting('memory_limit')"
                    ).fetchone(),
                )
            )
            self.connection.execute("ROLLBACK")
            raise duckdb.TransactionException(self.message)
        return self.connection.execute(query, parameters)

    def executemany(self, query: str, parameters: list[list[object]]) -> duckdb.DuckDBPyConnection:
        return self.connection.executemany(query, parameters)
