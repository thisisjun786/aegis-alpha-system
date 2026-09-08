from __future__ import annotations

import pytest

from aegis_alpha.metadata.database import create_metadata_engine, load_database_url


def test_runtime_database_url_is_required_without_secret_echo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AAS_DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="AAS_DATABASE_URL is required"):
        load_database_url()


def test_runtime_database_url_rejects_non_postgresql_without_echo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = "DO_NOT_LEAK_SENTINEL"
    value = f"sqlite:///{sentinel}"
    monkeypatch.setenv("AAS_DATABASE_URL", value)

    with pytest.raises(RuntimeError) as error:
        load_database_url()
    assert sentinel not in str(error.value)


def test_metadata_engine_uses_psycopg_and_bounded_connect_timeout() -> None:
    engine = create_metadata_engine("postgresql+psycopg://user:password@localhost/database")
    try:
        assert engine.dialect.name == "postgresql"
        assert engine.url.drivername == "postgresql+psycopg"
    finally:
        engine.dispose()
