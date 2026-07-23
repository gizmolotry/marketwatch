"""Small injectable HTTP layer that persists evidence before JSON parsing."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol

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


def _safe_response_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    """Return a deterministic allowlist that cannot persist auth material."""

    safe: dict[str, str] = {}
    for name, value in sorted(headers.items(), key=lambda item: str(item[0]).lower()):
        normalized = str(name).strip().lower()
        if normalized in _SAFE_RESPONSE_HEADERS:
            safe[normalized] = str(value)
    return safe


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    body: bytes
    headers: Mapping[str, str]
    url: str


class HttpTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None,
        timeout: float,
    ) -> HttpResponse: ...


class RequestsTransport:
    def __init__(self, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "MarketLeak-Evidence-Ingestion/2")

    def request(self, method: str, url: str, *, params: Mapping[str, Any] | None,
                timeout: float) -> HttpResponse:
        response = self.session.request(method, url, params=params, timeout=timeout)
        return HttpResponse(response.status_code, response.content, dict(response.headers), response.url)


@dataclass(frozen=True, slots=True)
class ParsedResponse:
    payload: Any
    raw: RawCapture


class ConnectorHttpError(RuntimeError):
    def __init__(self, status_code: int, url: str):
        super().__init__(f"HTTP {status_code} from {url}")
        self.status_code = status_code
        self.url = url


class ConnectorPayloadError(RuntimeError):
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
    ):
        self.raw_store = raw_store
        self.transport = transport or RequestsTransport()
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.sleep = sleep

    def get_json(
        self,
        *,
        platform: str,
        source: str,
        url: str,
        params: Mapping[str, Any] | None = None,
    ) -> ParsedResponse:
        for attempt in range(1, self.max_attempts + 1):
            response = self.transport.request("GET", url, params=params, timeout=self.timeout)
            capture = self.raw_store.capture(
                response.body,
                platform=platform,
                source=source,
                request={"method": "GET", "url": url, "params": dict(params or {}), "attempt": attempt},
                response_metadata={
                    "status_code": response.status_code,
                    "url": response.url,
                    "headers": _safe_response_headers(response.headers),
                },
            )
            if 200 <= response.status_code < 300:
                try:
                    return ParsedResponse(parse_json_decimal(response.body), capture)
                except (UnicodeDecodeError, ValueError) as exc:
                    raise ConnectorPayloadError(
                        f"invalid JSON from {url}; raw_sha256={capture.sha256}"
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
