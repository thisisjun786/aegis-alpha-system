from __future__ import annotations

import os

from sqlalchemy import Engine, create_engine, make_url
from sqlalchemy.exc import ArgumentError


def load_database_url() -> str:
    value = os.environ.get("AAS_DATABASE_URL")
    if not value:
        raise RuntimeError("AAS_DATABASE_URL is required")
    try:
        url = make_url(value)
    except ArgumentError:
        raise RuntimeError("AAS_DATABASE_URL is invalid") from None
    if url.drivername != "postgresql+psycopg":
        raise RuntimeError("AAS_DATABASE_URL must use postgresql+psycopg")
    return value


def create_metadata_engine(database_url: str | None = None) -> Engine:
    value = database_url if database_url is not None else load_database_url()
    try:
        url = make_url(value)
    except ArgumentError:
        raise RuntimeError("metadata database URL is invalid") from None
    if url.drivername != "postgresql+psycopg":
        raise RuntimeError("metadata database URL must use postgresql+psycopg")
    return create_engine(value, pool_pre_ping=True, connect_args={"connect_timeout": 5})
