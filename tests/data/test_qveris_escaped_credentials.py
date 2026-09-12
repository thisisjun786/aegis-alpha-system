from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from aegis_alpha.data.qveris_client import (
    QverisClient,
    QverisCredentialEchoError,
    QverisRequestError,
)
from tests.data.test_qveris_client import FakeOpener, FakeResponse

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(params=['synthetic"credential', r"synthetic\credential", r"synthetic\"credential"])
def escaped_key(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[Path, str]:
    value = request.param
    assert isinstance(value, str)
    path = tmp_path / "key"
    path.write_text(value, encoding="ascii")
    path.chmod(0o600)
    return path, value


def test_json_escaped_outgoing_credential_is_refused_before_transport(
    escaped_key: tuple[Path, str],
) -> None:
    path, key = escaped_key
    opener = FakeOpener([FakeResponse()])
    client = QverisClient(path, opener=opener)
    with pytest.raises(QverisRequestError) as error:
        client.request("/search", body={"query": "prefix:" + key + ":suffix"})
    assert not opener.calls
    assert key not in str(error.value)


def test_json_escaped_response_credential_is_discarded(
    escaped_key: tuple[Path, str],
) -> None:
    path, key = escaped_key
    response = FakeResponse(status=401, body=json.dumps({"error": "invalid:" + key}).encode())
    opener = FakeOpener([response])
    client = QverisClient(path, opener=opener)
    with pytest.raises(QverisCredentialEchoError) as error:
        client.request("/auth/credits")
    assert response.closed
    assert len(opener.calls) == 1
    assert key not in str(error.value)


def test_json_escaped_header_credential_is_discarded(
    escaped_key: tuple[Path, str],
) -> None:
    path, key = escaped_key
    response = FakeResponse(header_pairs=[("x-qveris-api-version", json.dumps(key)[1:-1])])
    client = QverisClient(path, opener=FakeOpener([response]))
    with pytest.raises(QverisCredentialEchoError):
        client.request("/auth/credits")
    assert response.closed


def test_escaped_key_remains_usable_without_echo(escaped_key: tuple[Path, str]) -> None:
    path, _key = escaped_key
    response = FakeResponse(body=b'{"ok": true}')
    client = QverisClient(path, opener=FakeOpener([response]))
    assert client.request("/auth/credits").document() == {"ok": True}
