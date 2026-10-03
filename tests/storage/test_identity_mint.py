"""Opaque IDs come from permanent anchors only, and their formats are frozen."""

from __future__ import annotations

import hashlib
from collections.abc import Callable

import pytest

from aegis_alpha.storage.identity import (
    IdentityAnchorError,
    assertion_id,
    mint_instrument,
    mint_issuer,
    parse_registry,
)
from tests.storage.identity_support import anchor, document, instrument


@pytest.mark.parametrize(
    ("namespace", "token"),
    [
        ("ticker", "AAPL"),
        ("eodhd_symbol", "AAPL.US"),
        ("krx_short_code", "005930"),
        ("path", "raw/norgate/AAPL.csv"),
        ("date", "2020-08-28"),
    ],
)
def test_ticker_anchor_is_refused(namespace: str, token: str) -> None:
    with pytest.raises(IdentityAnchorError, match="not a permanent anchor"):
        mint_instrument(namespace, token)
    with pytest.raises(IdentityAnchorError, match="not a permanent anchor"):
        mint_issuer(namespace, token)
    with pytest.raises(IdentityAnchorError, match="not a permanent anchor"):
        parse_registry(
            document(instruments=[{**instrument("1"), **anchor(token, namespace=namespace)}])
        )


@pytest.mark.parametrize(
    ("mint", "namespace", "token"),
    [
        (mint_instrument, "norgate_assetid", "AAPL"),
        (mint_instrument, "norgate_assetid", "2020-08-28"),
        (mint_instrument, "norgate_assetid", "0131684"),
        (mint_instrument, "krx_isin", "kr7005930003"),
        (mint_instrument, "krx_isin", "KR7005930004"),
        (mint_issuer, "sec_cik", "320193"),
        (mint_issuer, "sec_cik", "CIK0000320193"),
        (mint_issuer, "dart_corp_code", "126380"),
    ],
)
def test_anchor_tokens_must_be_canonical(
    mint: Callable[[str, str], str], namespace: str, token: str
) -> None:
    """A wrong or non-canonical spelling is refused rather than repaired into another ID."""
    with pytest.raises(IdentityAnchorError):
        mint(namespace, token)


def _independent(prefix: str, fmt: str, namespace: str, token: str) -> str:
    preimage = f'["{fmt}","{namespace}","{token}"]'.encode()
    return prefix + hashlib.sha256(preimage).hexdigest()


def test_identity_id_formats_are_frozen() -> None:
    cases = [
        (
            mint_instrument("norgate_assetid", "131684"),
            ("ins-", "aas-instrument-v1", "norgate_assetid", "131684"),
            "ins-c7bf6953fbafb8e4409851737cf21d71fa198fc7765041df82c435aad25e6584",
        ),
        (
            mint_instrument("krx_isin", "KR7005930003"),
            ("ins-", "aas-instrument-v1", "krx_isin", "KR7005930003"),
            "ins-e6cab5338c6cf4375b5bf7c1a296d8a1bb34b0c09f67ea9a8238d3fbcabff02c",
        ),
        (
            mint_issuer("sec_cik", "0000320193"),
            ("iss-", "aas-issuer-v1", "sec_cik", "0000320193"),
            "iss-d22177b6a2836681c661adec3176c5a5f7ac3132851fd6fda5f66a61abeac49e",
        ),
        (
            mint_issuer("dart_corp_code", "00126380"),
            ("iss-", "aas-issuer-v1", "dart_corp_code", "00126380"),
            "iss-7d111d94e533d760f53cdd4c4a709331e7e7735400a1d8fa42c1e6b5621e3ed1",
        ),
    ]
    for minted, parts, frozen in cases:
        assert minted == _independent(*parts) == frozen
    row = {
        "instrument_id": cases[0][2],
        "provider": "norgate",
        "namespace": "ticker",
        "token": "A",
        "valid_from_us": 0,
        "valid_to_us": None,
        "known_from_us": 1,
        "supersedes_assertion_id": None,
        "source_snapshot_id": "s",
        "source_hash": "a" * 64,
    }
    preimage = (
        f'["aas-assertion-v1","{cases[0][2]}","norgate","ticker","A",'
        f'0,null,1,null,"s","{"a" * 64}"]'
    ).encode()
    assert assertion_id(row) == "asr-" + hashlib.sha256(preimage).hexdigest()
    assert assertion_id(row) == (
        "asr-0511b8a5588e6960b6aee5ea9c7b8e7edf9d6ba338e74007020cc36796bf346f"
    )


def test_mint_is_scoped_by_kind_and_namespace() -> None:
    """The same digits under another namespace or entity kind never collide."""
    assert mint_instrument("norgate_assetid", "12345678") != mint_issuer(
        "dart_corp_code", "12345678"
    )
    assert mint_instrument("norgate_assetid", "131684") == mint_instrument(
        "norgate_assetid", "131684"
    )
