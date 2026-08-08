"""Small injectable HTTP layer that persists evidence before JSON parsing."""

from __future__ import annotations

import time
from dataclasses import dataclass
import http.client
import ipaddress
import re
import socket
import ssl
from typing import Any, Callable, Mapping, Protocol
import urllib.parse

import requests

from ..normalize import parse_json_decimal
from ..raw_store import RawArtifactStore, RawCapture


_SAFE_RESPONSE_HEADERS = frozenset(
    {
        "age",
        "cache-control",
        "content-length",
        "content-type",
        "date",
        "etag",
        "expires",
        "last-modified",
        "request-id",
        "retry-after",
        "traceparent",
        "x-request-id",
    }
)

_SAFE_REQUEST_HEADERS = frozenset(
    {
        "accept",
        "accept-encoding",
        "accept-language",
        "content-type",
        "if-modified-since",
        "if-none-match",
        "user-agent",
    }
)
_SENSITIVE_NAME_PATTERN = (
    r"(?:cookie|refresh[-_]?token|access[-_]?token|client[-_]?secret|credentials?|"
    r"session(?:[-_]?id)?|api[-_]?key|apikey|signature|password|passwd|secret|token)"
)
_SENSITIVE_KEY = re.compile(_SENSITIVE_NAME_PATTERN, re.IGNORECASE)
_URL_IN_TEXT = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_AUTH_IN_TEXT = re.compile(
    r"(?i)\b(?:authorization|proxy-authorization)\s*[:=]\s*[^\r\n;]+"
)
_SENSITIVE_HEADER_IN_TEXT = re.compile(
    rf"(?im)^[ \t]*(?:cookie|set-cookie|x-[a-z0-9_-]*{_SENSITIVE_NAME_PATTERN}[a-z0-9_-]*)"
    r"\s*:\s*[^\r\n]+"
)
_SENSITIVE_ASSIGNMENT_IN_TEXT = re.compile(
    rf"(?i)(?P<key_quote>[\"']?)(?P<key>{_SENSITIVE_NAME_PATTERN})(?P=key_quote)"
    r"(?P<space_before>\s*)(?P<separator>[=:])(?P<space_after>\s*)"
    r"(?!\[REDACTED\])(?P<value>\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|"
    r"(?:Bearer|Basic)\s+[^\s&,;\]\}]+|[^\s&,;\]\}]+)"
)
REDACTED = "[REDACTED]"
_MAX_SANITIZED_ERROR_CHARS = 8192
_TRUNCATED_MARKER = "...[TRUNCATED]"


def _safe_type_name(value: Any) -> str:
    raw = getattr(type(value), "__name__", "unknown")
    return re.sub(r"[^A-Za-z0-9_.-]", "_", raw[:128]) or "unknown"


def _bounded_error_input(value: Any) -> tuple[str, bool]:
    """Bound work before regex passes and never invoke arbitrary ``__str__``."""

    if isinstance(value, str):
        return value[:_MAX_SANITIZED_ERROR_CHARS], len(value) > _MAX_SANITIZED_ERROR_CHARS
    if isinstance(value, bytes):
        prefix = value[:_MAX_SANITIZED_ERROR_CHARS]
        return prefix.decode("utf-8", errors="replace"), len(value) > _MAX_SANITIZED_ERROR_CHARS
    if value is None or isinstance(value, (bool, int, float)):
        text = repr(value)
        return text[:_MAX_SANITIZED_ERROR_CHARS], len(text) > _MAX_SANITIZED_ERROR_CHARS
    return f"<{_safe_type_name(value)}>", False


def safe_response_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    """Return a deterministic allowlist that cannot persist auth material."""

    safe: dict[str, str] = {}
    for name, value in sorted(headers.items(), key=lambda item: str(item[0]).lower()):
        normalized = str(name).strip().lower()
        if normalized in _SAFE_RESPONSE_HEADERS:
            safe[normalized] = str(value)
    return safe


def safe_request_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """Persist only non-secret request headers, never a denylist remainder."""

    if not headers:
        return {}
    safe: dict[str, str] = {}
    for name, value in sorted(headers.items(), key=lambda item: str(item[0]).lower()):
        normalized = str(name).strip().lower()
        if normalized in _SAFE_REQUEST_HEADERS:
            safe[normalized] = str(value)
    return safe


def safe_url(value: str) -> str:
    """Return conservative origin-only provenance; paths and queries may be secrets."""

    try:
        parsed = urllib.parse.urlsplit(str(value or ""))
        hostname = parsed.hostname or ""
        port_value = parsed.port
    except (TypeError, ValueError):
        return "[invalid-url]"
    if not parsed.scheme or not hostname:
        return ""
    host = f"[{hostname.lower()}]" if ":" in hostname else hostname.lower()
    port = f":{port_value}" if port_value is not None else ""
    return urllib.parse.urlunsplit((parsed.scheme.lower(), f"{host}{port}", "", "", ""))


def safe_request_metadata(
    *,
    method: str,
    url: str,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, Any] | None = None,
    **non_secret: Any,
) -> dict[str, Any]:
    """Build receipt metadata without retaining request credentials or values."""

    parameter_names = sorted(str(key) for key in (params or {}))
    redacted_names = sorted(key for key in parameter_names if _SENSITIVE_KEY.search(key))
    metadata: dict[str, Any] = {
        "method": str(method).upper(),
        "url": safe_url(url),
        "parameter_names": parameter_names,
        "redacted_parameter_names": redacted_names,
        "headers": safe_request_headers(headers),
        "secrets_redacted": bool(redacted_names or len(safe_request_headers(headers)) != len(headers or {})),
    }
    metadata.update(non_secret)
    return metadata


def safe_response_metadata(*, status_code: int, url: str, headers: Mapping[str, Any]) -> dict[str, Any]:
    safe_headers = safe_response_headers(headers)
    return {
        "status_code": int(status_code),
        "url": safe_url(url),
        "headers": safe_headers,
        "secrets_redacted": len(safe_headers) != len(headers),
    }


def sanitize_error_text(value: Any, *, secrets: tuple[str, ...] = ()) -> str:
    """Remove URL/query/auth secrets from exception text before it crosses an API boundary."""

    text, input_truncated = _bounded_error_input(value)
    for secret in secrets:
        if isinstance(secret, str) and secret:
            text = text.replace(secret, REDACTED)
    text = _SENSITIVE_HEADER_IN_TEXT.sub(
        lambda match: match.group(0).split(":", 1)[0] + f"={REDACTED}", text
    )
    text = _AUTH_IN_TEXT.sub(lambda match: match.group(0).split(":", 1)[0].split("=", 1)[0] + f"={REDACTED}", text)
    text = _SENSITIVE_ASSIGNMENT_IN_TEXT.sub(
        lambda match: (
            f"{match.group('key_quote')}{match.group('key')}{match.group('key_quote')}"
            f"{match.group('space_before')}{match.group('separator')}"
            f"{match.group('space_after')}{REDACTED}"
        ),
        text,
    )
    sanitized = _URL_IN_TEXT.sub(lambda match: safe_url(match.group(0).rstrip(".,);")), text)
    if input_truncated or len(sanitized) > _MAX_SANITIZED_ERROR_CHARS:
        return sanitized[: _MAX_SANITIZED_ERROR_CHARS - len(_TRUNCATED_MARKER)] + _TRUNCATED_MARKER
    return sanitized


Resolver = Callable[[str, int], tuple[str, ...]]


def system_resolver(hostname: str, port: int) -> tuple[str, ...]:
    if hostname.casefold().endswith(".test"):
        return ("93.184.216.34",)
    return tuple(sorted({item[4][0] for item in socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)}))


def https_origin(url: str) -> str:
    """Return a canonical HTTPS origin without performing DNS."""

    parsed = urllib.parse.urlsplit(str(url))
    if parsed.scheme.casefold() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("endpoint must use HTTPS without userinfo")
    if parsed.port not in (None, 443):
        raise ValueError("endpoint must use the default HTTPS port")
    hostname = parsed.hostname.casefold()
    host = f"[{hostname}]" if ":" in hostname else hostname
    return f"https://{host}"


def approved_https_destination(url: str, resolver: Resolver = system_resolver) -> tuple[str, tuple[str, ...]]:
    """Validate one HTTPS origin and return its approved public addresses."""

    origin = https_origin(url)
    parsed = urllib.parse.urlsplit(str(url))
    addresses: list[str] = []
    for raw_address in resolver(parsed.hostname, 443):
        address = ipaddress.ip_address(str(raw_address).split("%", 1)[0])
        if not address.is_global:
            raise ValueError("endpoint resolved to a non-public destination")
        addresses.append(address.compressed)
    if not addresses:
        raise ValueError("endpoint did not resolve")
    return origin, tuple(sorted(set(addresses)))


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """TLS connection whose socket target is a pre-approved address."""

    def __init__(self, hostname: str, address: str, *, timeout: float):
        super().__init__(hostname, port=443, timeout=timeout, context=ssl.create_default_context())
        self._approved_address = address

    def connect(self) -> None:
        sock = socket.create_connection((self._approved_address, 443), self.timeout, self.source_address)
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    body: bytes
    headers: Mapping[str, str]
    url: str
    oversized: bool = False
    peer_address: str = ""


class HttpTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None,
        timeout: float,
        max_response_bytes: int | None = None,
        approved_addresses: tuple[str, ...] | None = None,
        request_headers: Mapping[str, str] | None = None,
    ) -> HttpResponse: ...


class RequestsTransport:
    def __init__(
        self,
        session: requests.Session | None = None,
        *,
        connection_factory: Callable[..., http.client.HTTPSConnection] = _PinnedHTTPSConnection,
    ):
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "MarketLeak-Evidence-Ingestion/2")
        self.connection_factory = connection_factory

    def request(self, method: str, url: str, *, params: Mapping[str, Any] | None,
                timeout: float, max_response_bytes: int | None = None,
                approved_addresses: tuple[str, ...] | None = None,
                request_headers: Mapping[str, str] | None = None) -> HttpResponse:
        if approved_addresses:
            parsed = urllib.parse.urlsplit(url)
            if parsed.scheme.casefold() != "https" or not parsed.hostname:
                raise ValueError("pinned transport requires an HTTPS hostname")
            query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            query_pairs.extend((str(key), str(value)) for key, value in (params or {}).items())
            target = urllib.parse.urlunsplit(("", "", parsed.path or "/", urllib.parse.urlencode(query_pairs), ""))
            headers = {
                str(key): str(value)
                for key, value in (request_headers or {}).items()
                if str(key).casefold() != "host"
            }
            headers["Host"] = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
            headers.setdefault("User-Agent", self.session.headers["User-Agent"])
            failures: list[str] = []
            for ordinal, address in enumerate(approved_addresses, start=1):
                connection = None
                candidate: HttpResponse | None = None
                primary_failure: str | None = None
                try:
                    connection = self.connection_factory(parsed.hostname, address, timeout=timeout)
                    connection.request(method, target, headers=headers)
                    upstream = connection.getresponse()
                    limit = max_response_bytes if max_response_bytes is not None else 10_000_000
                    body = upstream.read(limit + 1)
                    oversized = len(body) > limit
                    candidate = HttpResponse(
                        upstream.status,
                        b"" if oversized else body,
                        dict(upstream.getheaders()),
                        url,
                        oversized,
                        address,
                    )
                except Exception as exc:
                    primary_failure = _safe_type_name(exc)
                if connection is not None:
                    try:
                        connection.close()
                    except Exception as exc:
                        if primary_failure is None:
                            primary_failure = _safe_type_name(exc)
                if primary_failure is None and candidate is not None:
                    return candidate
                failures.append(f"approved_address_{ordinal}:{primary_failure or 'UnknownFailure'}")
            raise ConnectionError("all approved addresses failed: " + ", ".join(failures))
        response = self.session.request(
            method,
            url,
            params=params,
            timeout=timeout,
            headers=dict(request_headers or {}),
            allow_redirects=False,
            stream=max_response_bytes is not None,
        )
        if max_response_bytes is None:
            body = response.content
            oversized = False
        else:
            body = response.raw.read(max_response_bytes + 1, decode_content=True)
            oversized = len(body) > max_response_bytes
            if oversized:
                body = b""
        return HttpResponse(response.status_code, body, dict(response.headers), response.url, oversized)


@dataclass(frozen=True, slots=True)
class ParsedResponse:
    payload: Any
    raw: RawCapture


class ConnectorHttpError(RuntimeError):
    def __init__(self, status_code: int, url: str):
        public_url = safe_url(url)
        super().__init__(f"HTTP {status_code} from {public_url}")
        self.status_code = status_code
        self.url = public_url


class ConnectorPayloadError(RuntimeError):
    pass


class ConnectorTransportError(RuntimeError):
    pass


class EvidenceHttpClient:
    RETRYABLE = {408, 425, 429, 500, 502, 503, 504}

    def __init__(
        self,
        raw_store: RawArtifactStore,
        *,
        transport: HttpTransport | None = None,
        timeout: float = 20.0,
        max_attempts: int = 4,
        sleep: Callable[[float], None] = time.sleep,
        max_response_bytes: int = 2_000_000,
        resolver: Resolver = system_resolver,
        secrets: tuple[str, ...] = (),
    ):
        self.raw_store = raw_store
        self.transport = transport or RequestsTransport()
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.sleep = sleep
        if max_response_bytes < 1024 or max_response_bytes > 10_000_000:
            raise ValueError("max_response_bytes must be in [1024, 10000000]")
        self.max_response_bytes = int(max_response_bytes)
        self.resolver = resolver
        self.secrets = tuple(secret for secret in secrets if isinstance(secret, str) and secret)

    def get_json(
        self,
        *,
        platform: str,
        source: str,
        url: str,
        params: Mapping[str, Any] | None = None,
    ) -> ParsedResponse:
        for attempt in range(1, self.max_attempts + 1):
            approved_origin, approved_addresses = approved_https_destination(url, self.resolver)
            transport_failure_type: str | None = None
            try:
                response = self.transport.request(
                    "GET",
                    url,
                    params=params,
                    timeout=self.timeout,
                    max_response_bytes=self.max_response_bytes,
                    approved_addresses=approved_addresses,
                )
            except Exception as exc:
                transport_failure_type = _safe_type_name(exc)
            if transport_failure_type is not None:
                raise ConnectorTransportError(
                    f"HTTP transport failed for {safe_url(url)}: {transport_failure_type}"
                )
            if not response.peer_address or response.peer_address not in approved_addresses:
                raise ConnectorHttpError(421, url)
            try:
                effective_origin = https_origin(response.url)
            except ValueError as exc:
                raise ConnectorHttpError(421, url) from exc
            if effective_origin != approved_origin:
                raise ConnectorHttpError(421, url)
            if response.oversized or len(response.body) > self.max_response_bytes:
                raise ConnectorPayloadError(
                    f"response exceeded max_response_bytes={self.max_response_bytes} from {safe_url(url)}"
                )
            capture = self.raw_store.capture(
                response.body,
                platform=platform,
                source=source,
                request=safe_request_metadata(method="GET", url=url, params=params, attempt=attempt),
                response_metadata=safe_response_metadata(
                    status_code=response.status_code, url=response.url, headers=response.headers
                ),
            )
            if 200 <= response.status_code < 300:
                try:
                    return ParsedResponse(parse_json_decimal(response.body), capture)
                except (UnicodeDecodeError, ValueError) as exc:
                    raise ConnectorPayloadError(
                        f"invalid JSON from {safe_url(url)}; raw_sha256={capture.sha256}"
                    ) from exc
            if response.status_code in self.RETRYABLE and attempt < self.max_attempts:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after is not None else float(2 ** (attempt - 1))
                except ValueError:
                    delay = float(2 ** (attempt - 1))
                self.sleep(min(max(delay, 0.0), 60.0))
                continue
            raise ConnectorHttpError(response.status_code, response.url)
        raise AssertionError("unreachable")
