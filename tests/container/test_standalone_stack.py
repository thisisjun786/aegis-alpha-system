from __future__ import annotations

import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_cli_is_independent_and_restricted() -> None:
    config = json.loads((_ROOT / "docker-compose.yml").read_text())
    app = config["services"]["aas"]
    assert "depends_on" not in app
    assert "secrets" not in app
    assert app["volumes"][0]["target"] == "/state/aas"
    assert app["volumes"][0]["bind"] == {"create_host_path": False}
    assert "ports" not in app
    assert app["read_only"] is True
    assert app["cap_drop"] == ["ALL"]
    assert app["security_opt"] == ["no-new-privileges:true"]
    assert set(config["services"]) == {"aas"}
    assert "volumes" not in config
    assert app["environment"]["AAS_HOME"] == "/state/aas"
    dockerfile = (_ROOT / "Dockerfile").read_text()
    assert "COPY vt/" not in dockerfile
    assert "COPY brokers/" not in dockerfile
    assert 'ENTRYPOINT ["aas"]' in dockerfile
    assert "USER aas" in dockerfile
    for line in dockerfile.splitlines():
        if line.startswith("FROM "):
            assert "@sha256:" in line


def test_single_app_uses_owned_user_root_without_database_service() -> None:
    config = json.loads((_ROOT / "docker-compose.yml").read_text())
    assert set(config["services"]) == {"aas"}
    app = config["services"]["aas"]
    assert app["user"] == "${AAS_UID:-1000}:${AAS_GID:-1000}"
    assert app["volumes"][0]["source"] == "${AAS_HOME:-${HOME}/.aas}"
    assert "secrets" not in config
    dockerfile = (_ROOT / "Dockerfile").read_text()
    assert "COPY migrations" not in dockerfile
    assert "COPY alembic.ini" not in dockerfile
