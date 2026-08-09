import json

import pytest

from marketleak.ingestion.connectors.http import (
    ConnectorTransportError,
    EvidenceHttpClient,
    HttpResponse,
    RequestsTransport,
    safe_url,
    sanitize_error_text,
)
from marketleak.ingestion.raw_store import RawArtifactStore


class _BoundedRaw:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.read_sizes: list[int] = []

    def read(self, size: int, *, decode_content: bool) -> bytes:
        assert decode_content is True
        self.read_sizes.append(size)
        return self.body[:size]


class _StreamingResponse:
    def __init__(self, body: bytes) -> None:
        self.status_code = 200
        self.raw = _BoundedRaw(body)
        self.headers = {"Content-Type": "application/json"}
        self.url = "https://example.test/chunked"


class _StreamingSession:
    def __init__(self, response: _StreamingResponse) -> None:
        self.headers: dict[str, str] = {}
        self.response = response
        self.kwargs = None

    def request(self, method, url, **kwargs):
        self.kwargs = kwargs
        return self.response


def test_requests_transport_reads_only_limit_plus_one_and_discards_oversized_body():
    upstream = _StreamingResponse(b"x" * 4096)
    session = _StreamingSession(upstream)

    response = RequestsTransport(session).request(
        "GET",
        "https://example.test/chunked",
        params=None,
        timeout=3,
        max_response_bytes=1024,
    )

    assert upstream.raw.read_sizes == [1025]
    assert session.kwargs["stream"] is True
    assert session.kwargs["allow_redirects"] is False
    assert response.oversized is True
    assert response.body == b""


def test_safe_url_and_auth_errors_do_not_retain_path_query_or_multi_token_credentials():
    path_token = "private-path-token"
    bearer = "bearer-part-one bearer-part-two"
    url = f"https://example.test/download/{path_token}?signature=query-secret"

    assert safe_url(url) == "https://example.test"
    rendered = sanitize_error_text(
        f"Authorization: Bearer {bearer}; failed at {url}"
    )
    assert path_token not in rendered
    assert "query-secret" not in rendered
    assert "bearer-part-one" not in rendered
    assert "bearer-part-two" not in rendered
    assert rendered == "Authorization=[REDACTED]; failed at https://example.test"

    digest = sanitize_error_text(
        'Authorization: Digest username="private-user", realm="private-realm", nonce="private-nonce"'
    )
    assert digest == "Authorization=[REDACTED]"
    assert "private-user" not in digest
    assert "private-realm" not in digest
    assert "private-nonce" not in digest


def test_error_sanitizer_redacts_cookie_custom_secret_headers_and_assignments_outside_urls():
    rendered = sanitize_error_text(
        "Cookie: sid=cookie-secret; theme=dark\n"
        "Set-Cookie: access_token=set-cookie-secret; Secure\n"
        "X-Session-Token: custom-header-secret\n"
        "X-Api-Key: custom-api-secret\n"
        "request failed api_key=form-api access_token=form-access signature=form-signature "
        "session=form-session token=form-token password=form-password secret=form-secret safe=value"
    )
    for secret in (
        "cookie-secret", "set-cookie-secret", "custom-header-secret", "custom-api-secret",
        "form-api", "form-access", "form-signature", "form-session", "form-token",
        "form-password", "form-secret",
    ):
        assert secret not in rendered
    assert "Cookie=[REDACTED]" in rendered
    assert "Set-Cookie=[REDACTED]" in rendered
    assert "X-Session-Token=[REDACTED]" in rendered
    assert "X-Api-Key=[REDACTED]" in rendered
    assert "safe=value" in rendered


@pytest.mark.parametrize(
    "payload",
    [
        "cookie=cookie-value safe=value",
        "refresh_token=refresh-value safe=value",
        "ACCESS-TOKEN=Bearer access-value; safe=value",
        "client_secret = 'client-value' safe=value",
        '"credential": "credential-value", "safe":"value"',
        "credentials=credential-plural-value safe=value",
        "session: session-value safe=value",
        "session_id=session-id-value safe=value",
        "SESSION-ID=Basic session-basic-value; safe=value",
        "api_key=api-underscore-value safe=value",
        "apikey:api-compact-value safe=value",
        '"signature":"signature-value", "safe":"value"',
        "password=password-value safe=value",
        "passwd: passwd-value safe=value",
        "secret=secret-value safe=value",
        "token=Bearer token-value safe=value",
    ],
)
def test_sensitive_assignment_variants_are_fully_redacted_without_losing_clear_safe_context(payload):
    rendered = sanitize_error_text(payload)
    assert "-value" not in rendered
    assert "basic-value" not in rendered.casefold()
    assert "[REDACTED]" in rendered
    assert "safe" in rendered.casefold()
    assert "value" in rendered.casefold()


def test_sensitive_header_substrings_and_two_part_values_are_fail_closed():
    rendered = sanitize_error_text(
        "X-Refresh-Token: Bearer header-secret second-part\n"
        "X-Client-Secret: Basic client-secret-value\n"
        "X-Credentials-Version: credential-header-value\n"
        "safe context follows"
    )
    assert "header-secret" not in rendered
    assert "second-part" not in rendered
    assert "client-secret-value" not in rendered
    assert "credential-header-value" not in rendered
    assert "safe context follows" in rendered


def test_sanitizer_bounds_input_before_regex_and_never_calls_arbitrary_str():
    huge = "safe-prefix " + ("x" * 3_000_000) + " api_key=late-secret"
    rendered = sanitize_error_text(huge)
    assert len(rendered) <= 8192
    assert rendered.endswith("...[TRUNCATED]")
    assert "late-secret" not in rendered

    class HostileString:
        def __str__(self):
            raise AssertionError("arbitrary __str__ must not run")

    assert sanitize_error_text(HostileString()) == "<HostileString>"


class _BoundAwareTransport:
    def __init__(self, body: bytes, *, oversized: bool, peer: str | None = "approved") -> None:
        self.body = body
        self.oversized = oversized
        self.peer = peer
        self.calls = []

    def request(
        self, method, url, *, params, timeout, max_response_bytes, approved_addresses
    ):
        self.calls.append((max_response_bytes, approved_addresses))
        peer = approved_addresses[0] if self.peer == "approved" else (self.peer or "")
        return HttpResponse(200, self.body, {}, url, self.oversized, peer)


def test_generic_http_client_passes_transport_bound_and_never_captures_oversized_body(tmp_path):
    transport = _BoundAwareTransport(b"", oversized=True)
    store = RawArtifactStore(tmp_path / "raw")
    client = EvidenceHttpClient(
        store,
        transport=transport,
        max_response_bytes=1024,
        resolver=lambda _host, _port: ("93.184.216.34",),
    )

    with pytest.raises(Exception, match="exceeded max_response_bytes=1024"):
        client.get_json(
            platform="polymarket",
            source="polymarket:public-data-api",
            url="https://data-api.polymarket.com/trades",
        )

    assert transport.calls == [(1024, ("93.184.216.34",))]
    assert not any(store.receipts.rglob("*.json"))


def test_public_parameter_receipt_retention_is_explicit_bounded_and_secret_free(tmp_path):
    store = RawArtifactStore(tmp_path / "raw")
    client = EvidenceHttpClient(
        store,
        transport=_BoundAwareTransport(b"{}", oversized=False),
        resolver=lambda _host, _port: ("93.184.216.34",),
    )
    client.get_json(
        platform="polymarket",
        source="data-api/trades",
        url="https://data-api.polymarket.com/trades",
        params={
            "market": "0x" + "1" * 64,
            "offset": 0,
            "takerOnly": False,
            "api_key": "must-never-persist",
            "cookie": "must-never-persist-either",
        },
        public_parameter_allowlist=frozenset({"market", "offset", "takerOnly"}),
    )
    receipt = json.loads(next(store.receipts.rglob("*.json")).read_bytes())
    assert receipt["request"]["public_parameters"] == {
        "market": "0x" + "1" * 64,
        "offset": 0,
        "takerOnly": False,
    }
    serialized = json.dumps(receipt)
    assert "must-never-persist" not in serialized
    assert "api_key" not in receipt["request"]["public_parameters"]


@pytest.mark.parametrize("name", ["api_key", "token", "cookie", "signature", "client_secret"])
def test_sensitive_names_cannot_be_declared_public(tmp_path, name):
    client = EvidenceHttpClient(
        RawArtifactStore(tmp_path / "raw"),
        transport=_BoundAwareTransport(b"{}", oversized=False),
        resolver=lambda _host, _port: ("93.184.216.34",),
    )
    with pytest.raises(ValueError, match="unsafe name"):
        client.get_json(
            platform="example",
            source="example:fixture",
            url="https://example.test/data",
            params={name: "secret"},
            public_parameter_allowlist=frozenset({name}),
        )


@pytest.mark.parametrize("value", [["nested"], {"nested": True}, "x" * 513])
def test_nested_or_oversized_public_values_fail_before_transport(tmp_path, value):
    transport = _BoundAwareTransport(b"{}", oversized=False)
    client = EvidenceHttpClient(
        RawArtifactStore(tmp_path / "raw"),
        transport=transport,
        resolver=lambda _host, _port: ("93.184.216.34",),
    )
    with pytest.raises(ValueError, match="scalar|exceeds"):
        client.get_json(
            platform="example",
            source="example:fixture",
            url="https://example.test/data",
            params={"market": value},
            public_parameter_allowlist=frozenset({"market"}),
        )
    assert transport.calls == []


@pytest.mark.parametrize(
    ("response_url", "peer"),
    [
        ("https://attacker.test/data", "approved"),
        ("https://example.test/data", None),
    ],
)
def test_generic_http_client_rejects_cross_origin_or_missing_peer_before_capture(
    tmp_path, response_url, peer
):
    class BoundaryTransport(_BoundAwareTransport):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            return HttpResponse(
                response.status_code, b"{}", response.headers, response_url,
                peer_address=response.peer_address,
            )

    store = RawArtifactStore(tmp_path / "raw")
    client = EvidenceHttpClient(
        store,
        transport=BoundaryTransport(b"{}", oversized=False, peer=peer),
        resolver=lambda _host, _port: ("93.184.216.34",),
    )
    with pytest.raises(Exception):
        client.get_json(platform="example", source="example:fixture", url="https://example.test/data")
    assert not any(store.receipts.rglob("*.json"))


def test_generic_http_client_sanitizes_arbitrary_transport_exception_and_configured_secrets(tmp_path):
    configured_secret = "configured-exact-secret"

    class ExplodingTransport:
        def request(self, method, url, **kwargs):
            raise RuntimeError(
                f"socket failed {configured_secret} api_key=exception-api-value "
                "X-Session-Token: exception-header-value"
            )

    client = EvidenceHttpClient(
        RawArtifactStore(tmp_path / "raw"),
        transport=ExplodingTransport(),
        resolver=lambda _host, _port: ("93.184.216.34",),
        secrets=(configured_secret,),
    )
    with pytest.raises(ConnectorTransportError) as caught:
        client.get_json(platform="example", source="example:fixture", url="https://example.test/data")
    rendered = str(caught.value)
    assert configured_secret not in rendered
    assert "exception-api-value" not in rendered
    assert "exception-header-value" not in rendered
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert configured_secret not in repr(caught.value)


def test_transport_error_does_not_call_hostile_exception_string_and_has_no_hidden_context(tmp_path):
    class HostileError(RuntimeError):
        def __str__(self):
            raise AssertionError("exception __str__ must not run")

    class ExplodingTransport:
        def request(self, method, url, **kwargs):
            raise HostileError()

    client = EvidenceHttpClient(
        RawArtifactStore(tmp_path / "raw"),
        transport=ExplodingTransport(),
        resolver=lambda _host, _port: ("93.184.216.34",),
    )
    with pytest.raises(ConnectorTransportError) as caught:
        client.get_json(platform="example", source="example:fixture", url="https://example.test/data")
    assert "HostileError" in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


class _PinnedUpstream:
    status = 200

    def read(self, size):
        assert size == 1025
        return b"{}"

    def getheaders(self):
        return [("Content-Type", "application/json")]


class _FakePinnedConnection:
    def __init__(self, hostname, address, *, timeout, record):
        record.append((hostname, address, timeout))

    def request(self, method, target, *, headers):
        assert method == "GET"
        assert target == "/data"
        assert headers["Host"] == "example.test"

    def getresponse(self):
        return _PinnedUpstream()

    def close(self):
        return None


def test_default_transport_connects_to_the_single_approved_resolution_without_re_resolving(tmp_path):
    connections = []
    answers = iter((("93.184.216.34",), ("127.0.0.1",)))

    def factory(hostname, address, *, timeout):
        return _FakePinnedConnection(hostname, address, timeout=timeout, record=connections)

    client = EvidenceHttpClient(
        RawArtifactStore(tmp_path / "raw"),
        transport=RequestsTransport(connection_factory=factory),
        resolver=lambda _host, _port: next(answers),
        max_response_bytes=1024,
    )
    client.get_json(
        platform="example",
        source="example:fixture",
        url="https://example.test/data",
    )

    assert connections == [("example.test", "93.184.216.34", 20.0)]
    assert next(answers) == ("127.0.0.1",)


def test_pinned_transport_fails_over_deterministically_without_new_dns(tmp_path):
    connections = []

    class FailFirst(_FakePinnedConnection):
        def __init__(self, hostname, address, *, timeout, record):
            super().__init__(hostname, address, timeout=timeout, record=record)
            self.address = address

        def request(self, method, target, *, headers):
            if self.address == "93.184.216.34":
                raise OSError("failure text api_key=must-not-escape")
            super().request(method, target, headers=headers)

    def factory(hostname, address, *, timeout):
        return FailFirst(hostname, address, timeout=timeout, record=connections)

    client = EvidenceHttpClient(
        RawArtifactStore(tmp_path / "raw"),
        transport=RequestsTransport(connection_factory=factory),
        resolver=lambda _host, _port: ("93.184.216.35", "93.184.216.34"),
        max_response_bytes=1024,
    )
    client.get_json(platform="example", source="example:fixture", url="https://example.test/data")
    assert [entry[1] for entry in connections] == ["93.184.216.34", "93.184.216.35"]


def test_pinned_transport_all_failures_are_sanitized_and_fail_closed(tmp_path):
    class AlwaysFail(_FakePinnedConnection):
        def request(self, method, target, *, headers):
            raise OSError("token=transport-secret")

    transport = RequestsTransport(
        connection_factory=lambda hostname, address, *, timeout: AlwaysFail(
            hostname, address, timeout=timeout, record=[]
        )
    )
    with pytest.raises(ConnectionError) as caught:
        transport.request(
            "GET", "https://example.test/data", params=None, timeout=1,
            max_response_bytes=1024,
            approved_addresses=("93.184.216.34", "93.184.216.35"),
        )
    assert "transport-secret" not in str(caught.value)
    assert str(caught.value).count("OSError") == 2


def test_primary_failure_and_secret_close_failure_do_not_stop_approved_ip_fallback(tmp_path):
    attempts = []

    class FailRequestAndClose(_FakePinnedConnection):
        def __init__(self, hostname, address, *, timeout, record):
            super().__init__(hostname, address, timeout=timeout, record=record)
            self.address = address

        def request(self, method, target, *, headers):
            attempts.append(self.address)
            if self.address == "93.184.216.34":
                raise OSError("api_key=primary-secret")

        def close(self):
            if self.address == "93.184.216.34":
                raise RuntimeError("token=close-secret")

    transport = RequestsTransport(
        connection_factory=lambda hostname, address, *, timeout: FailRequestAndClose(
            hostname, address, timeout=timeout, record=[]
        )
    )
    response = transport.request(
        "GET", "https://example.test/data", params=None, timeout=1,
        max_response_bytes=1024,
        approved_addresses=("93.184.216.34", "93.184.216.35"),
    )
    assert attempts == ["93.184.216.34", "93.184.216.35"]
    assert response.peer_address == "93.184.216.35"


def test_successful_requests_with_close_failures_try_all_addresses_then_fail_safely():
    attempts = []

    class CloseFails(_FakePinnedConnection):
        def __init__(self, hostname, address, *, timeout, record):
            super().__init__(hostname, address, timeout=timeout, record=record)
            self.address = address

        def request(self, method, target, *, headers):
            attempts.append(self.address)

        def close(self):
            raise RuntimeError("client_secret=close-leak")

    transport = RequestsTransport(
        connection_factory=lambda hostname, address, *, timeout: CloseFails(
            hostname, address, timeout=timeout, record=[]
        )
    )
    with pytest.raises(ConnectionError) as caught:
        transport.request(
            "GET", "https://example.test/data", params=None, timeout=1,
            max_response_bytes=1024,
            approved_addresses=("93.184.216.34", "93.184.216.35"),
        )
    assert attempts == ["93.184.216.34", "93.184.216.35"]
    assert "close-leak" not in str(caught.value)
    assert str(caught.value).count("RuntimeError") == 2


def test_ipv6_origin_and_host_are_bracketed_for_pinned_transport():
    assert safe_url("https://[2001:4860:4860::8888]/private/token") == "https://[2001:4860:4860::8888]"
    observed = {}

    class IPv6Connection(_FakePinnedConnection):
        def request(self, method, target, *, headers):
            observed.update(headers)

    transport = RequestsTransport(
        connection_factory=lambda hostname, address, *, timeout: IPv6Connection(
            hostname, address, timeout=timeout, record=[]
        )
    )
    response = transport.request(
        "GET", "https://[2001:4860:4860::8888]/data", params=None, timeout=1,
        max_response_bytes=1024,
        approved_addresses=("2001:4860:4860::8888",),
    )
    assert observed["Host"] == "[2001:4860:4860::8888]"
    assert response.peer_address == "2001:4860:4860::8888"
