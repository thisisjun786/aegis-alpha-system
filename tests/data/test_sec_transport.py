"""Host allowlist, User-Agent gate, and synthetic socket denial. Zero network."""

from __future__ import annotations

import email
import socket
import urllib.request
from datetime import UTC, datetime
from typing import Self

import pytest
from sec_collector_support import PADDED_CIK, fixture_bodies, synthetic_user_agent

from aegis_alpha.data.sec_transport import (
    ALLOWED_HOST,
    CollectorRequest,
    DatasetKind,
    HostForbiddenError,
    RefuseRedirectHandler,
    SocketAccessError,
    TransportError,
    UserAgentError,
    assert_user_agent_absent,
    companyfacts_url,
    make_fixture_transport,
    make_https_opener,
    make_https_transport,
    no_socket_access,
    require_allowed_url,
    require_final_url,
    submissions_url,
    validate_user_agent,
)

HTTP_OK = 200
HTTPS_PORT = 443
HTTP_FOUND = 302
ARCHIVES_REDIRECT = "https://www.sec.gov/Archives/edgar/data/990001/file.htm"


def test_first_wave_urls_are_data_sec_gov_only() -> None:
    assert submissions_url(PADDED_CIK).startswith(f"https://{ALLOWED_HOST}/submissions/")
    assert companyfacts_url(PADDED_CIK).startswith(f"https://{ALLOWED_HOST}/api/xbrl/companyfacts/")


def test_archives_hosts_and_paths_are_forbidden() -> None:
    with pytest.raises(HostForbiddenError):
        require_allowed_url("https://www.sec.gov/Archives/edgar/data/990001/file.htm")
    with pytest.raises(HostForbiddenError):
        require_allowed_url("https://efts.sec.gov/LATEST/search-index")
    with pytest.raises(HostForbiddenError):
        require_allowed_url(f"https://{ALLOWED_HOST}/Archives/edgar/data/990001/file.htm")


def test_missing_or_addressless_user_agent_makes_zero_calls() -> None:
    with pytest.raises(UserAgentError, match="missing"):
        validate_user_agent(None)
    with pytest.raises(UserAgentError, match="contact address"):
        validate_user_agent("AegisAlpha")
    validated = validate_user_agent(synthetic_user_agent())
    assert "@" in validated


def test_user_agent_errors_never_echo_the_value() -> None:
    secret = synthetic_user_agent()
    with pytest.raises(UserAgentError) as error:
        validate_user_agent("AegisAlpha-no-address")
    assert secret not in str(error.value)
    assert "AegisAlpha-no-address" not in str(error.value)


def _fixed_clock() -> datetime:
    return datetime(2026, 8, 18, tzinfo=UTC)


def test_fixture_transport_cannot_open_a_socket() -> None:
    transport = make_fixture_transport(fixture_bodies(), clock=_fixed_clock)
    response = transport(CollectorRequest(DatasetKind.SUBMISSIONS, PADDED_CIK))
    assert response.status_code == HTTP_OK
    assert b"SYNTHETIC ISSUER" in response.body


def test_socket_escape_is_denied_inside_synthetic_scope() -> None:
    with no_socket_access(), pytest.raises(SocketAccessError):
        socket.create_connection(("data.sec.gov", HTTPS_PORT), timeout=0.1)


def test_redirect_to_archives_is_refused_without_a_socket() -> None:
    handler = RefuseRedirectHandler()
    request = urllib.request.Request(submissions_url(PADDED_CIK))  # noqa: S310
    headers = email.message_from_string(f"Location: {ARCHIVES_REDIRECT}\n")
    with pytest.raises(HostForbiddenError, match="refuses HTTP redirects"):
        handler.redirect_request(
            request,
            fp=None,
            code=HTTP_FOUND,
            msg="Found",
            headers=headers,
            newurl=ARCHIVES_REDIRECT,
        )
    with pytest.raises(HostForbiddenError):
        require_final_url(ARCHIVES_REDIRECT)
    assert isinstance(RefuseRedirectHandler(), urllib.request.HTTPRedirectHandler)
    assert make_https_opener() is not None


class _ArchivesResponse:
    def __init__(self) -> None:
        self.status = HTTP_OK
        self._headers: dict[str, str] = {}

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    def read(self) -> bytes:
        return b"{}"

    def geturl(self) -> str:
        return ARCHIVES_REDIRECT

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


class _ArchivesOpener:
    def open(
        self,
        request: urllib.request.Request,
        timeout: float | None = None,
    ) -> _ArchivesResponse:
        del request, timeout
        return _ArchivesResponse()


def test_followed_archives_url_fails_closed_without_a_socket() -> None:
    transport = make_https_transport(
        user_agent=synthetic_user_agent(),
        clock=_fixed_clock,
        opener=_ArchivesOpener(),
    )
    with pytest.raises(HostForbiddenError):
        transport(CollectorRequest(DatasetKind.SUBMISSIONS, PADDED_CIK))


def test_user_agent_scan_matches_configured_contact_only() -> None:
    user_agent = synthetic_user_agent()
    contact = user_agent.split()[-1]
    sec_forbidden_html = b"<html>Please contact webmaster@sec.gov for assistance.</html>"

    assert_user_agent_absent(user_agent, sec_forbidden_html)

    with pytest.raises(TransportError, match="User-Agent material") as leaked_ua:
        assert_user_agent_absent(user_agent, user_agent.encode())
    with pytest.raises(TransportError, match="User-Agent material") as leaked_contact:
        assert_user_agent_absent(user_agent, f"reflected {contact}".encode())
    assert user_agent not in str(leaked_ua.value)
    assert user_agent not in str(leaked_contact.value)
    assert contact not in str(leaked_ua.value)
    assert contact not in str(leaked_contact.value)
