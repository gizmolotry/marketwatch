"""Configurable point-in-time collectors for explicit public sources.

The collector captures every HTTP response body before status handling or
parsing.  Source publication timestamps are descriptive metadata; local
receipt time is the only prospective first-seen clock.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator

from marketleak.domain import CoverageStatus
from marketleak.evidence.archive import EvidenceArchive, EvidenceConflictError
from marketleak.evidence.coverage import CoverageLedger, SourceCoverageInterval
from marketleak.evidence.normalize import NormalizedEvidence, normalize_evidence, utc_datetime
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture
from marketleak.ingestion.connectors.http import (
    RequestsTransport as PinnedRequestsTransport,
    approved_https_destination,
    safe_request_metadata,
    safe_response_metadata,
    sanitize_error_text,
    system_resolver,
)


_TAG_RE = re.compile(r"<[^>]+>")


class UnsafeSourceUrlError(ValueError):
    """The configured or discovered URL is outside the declared public boundary."""


def _unsafe_source_detail(exc: Exception) -> str:
    if type(exc) is UnsafeSourceUrlError and exc.args and isinstance(exc.args[0], str):
        return sanitize_error_text(exc.args[0])
    return sanitize_error_text(exc)


def _origin(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme.casefold() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise UnsafeSourceUrlError("source URL must use HTTPS without userinfo")
    if parsed.port not in (None, 443):
        raise UnsafeSourceUrlError("source URL must use the default HTTPS port")
    hostname = parsed.hostname.casefold()
    host = f"[{hostname}]" if ":" in hostname else hostname
    return f"https://{host}"


def canonical_url(value: str, *, base_url: str | None = None) -> str:
    absolute = urllib.parse.urljoin(base_url or "", str(value or "").strip())
    parsed = urllib.parse.urlsplit(absolute)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return ""
    hostname = parsed.hostname.casefold()
    if ":" in hostname:
        hostname = f"[{hostname}]"
    port = parsed.port
    if port is not None and not (
        (parsed.scheme.casefold() == "http" and port == 80)
        or (parsed.scheme.casefold() == "https" and port == 443)
    ):
        hostname = f"{hostname}:{port}"
    query = urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)))
    path = parsed.path or "/"
    return urllib.parse.urlunsplit((parsed.scheme.casefold(), hostname, path, query, ""))


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = _TAG_RE.sub(" ", text)
    return " ".join(text.split())


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].casefold()


def _child_text(node: ET.Element, *names: str) -> str:
    wanted = {name.casefold() for name in names}
    for child in node:
        if _local_name(child.tag) in wanted:
            return "".join(child.itertext()).strip()
    return ""


def _entry_link(node: ET.Element) -> str:
    for child in node:
        if _local_name(child.tag) != "link":
            continue
        relation = str(child.attrib.get("rel", "alternate")).casefold()
        if relation not in {"", "alternate"}:
            continue
        return str(child.attrib.get("href") or child.text or "").strip()
    return ""


def _next_link(root: ET.Element, base_url: str) -> str:
    for node in root.iter():
        if _local_name(node.tag) == "link" and str(node.attrib.get("rel", "")).casefold() == "next":
            return canonical_url(str(node.attrib.get("href") or node.text or ""), base_url=base_url)
    return ""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status_code: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    url: str = ""
    oversized: bool = False
    peer_address: str = ""


class HttpTransport(Protocol):
    def get(
        self, url: str, *, timeout: float, headers: Mapping[str, str], max_response_bytes: int,
        approved_addresses: tuple[str, ...],
    ) -> HttpResponse: ...


class UrllibTransport:
    """Compatibility transport using a TLS connection pinned to approved IPs."""

    def __init__(self) -> None:
        self._transport = PinnedRequestsTransport()

    def get(
        self, url: str, *, timeout: float, headers: Mapping[str, str], max_response_bytes: int,
        approved_addresses: tuple[str, ...],
    ) -> HttpResponse:
        response = self._transport.request(
            "GET",
            url,
            params=None,
            timeout=timeout,
            max_response_bytes=max_response_bytes,
            approved_addresses=approved_addresses,
            request_headers=headers,
        )
        return HttpResponse(
            response.status_code, response.body, response.headers, response.url,
            response.oversized, response.peer_address
        )


class PublicSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    source_id: str = Field(min_length=1)
    url: str = Field(min_length=1)
    format: str = Field(pattern=r"^(rss|atom|auto|document)$")
    publisher: str = Field(min_length=1)
    enabled: bool = True
    backfill: bool = False
    timeout_seconds: float = Field(default=10.0, gt=0.0, le=60.0)
    max_pages: int = Field(default=1, ge=1, le=10)
    max_entries: int = Field(default=100, ge=1, le=1000)
    max_response_bytes: int = Field(default=2_000_000, ge=1024, le=10_000_000)
    headers: dict[str, str] = Field(default_factory=lambda: {"User-Agent": "MarketLeak-Evidence/1.0"})
    pagination_allowed_origins: tuple[str, ...] = Field(default_factory=tuple, max_length=10)
    document_title: str | None = None

    @field_validator("url")
    @classmethod
    def _absolute_http_url(cls, value: str) -> str:
        _origin(value)
        normalized = canonical_url(value)
        if not normalized:
            raise ValueError("url must be an absolute HTTPS URL")
        _origin(normalized)
        return normalized

    @field_validator("pagination_allowed_origins")
    @classmethod
    def _valid_pagination_origins(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({_origin(str(value).rstrip("/")) for value in values}))

    @field_validator("format")
    @classmethod
    def _lower_format(cls, value: str) -> str:
        return value.casefold()


@dataclass(frozen=True, slots=True)
class CollectionBatch:
    documents: tuple[NormalizedEvidence, ...]
    coverage: tuple[SourceCoverageInterval, ...]
    raw_captures: tuple[RawCapture, ...]
    errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ParsedEntry:
    source_document_id: str
    url: str
    publisher: str
    title: str
    body: str
    claimed_published_at: str | None
    modified_at: str | None
    source_revision: str | None
    metadata: dict[str, Any]


def load_source_config(path: str | Path) -> tuple[PublicSourceConfig, ...]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("sources"), list):
        raise ValueError("evidence source config must contain an explicit 'sources' list")
    return tuple(PublicSourceConfig.model_validate(item) for item in payload["sources"])


class PublicEvidenceCollector:
    def __init__(
        self,
        *,
        sources: Sequence[PublicSourceConfig | Mapping[str, Any]],
        raw_store: RawArtifactStore,
        archive: EvidenceArchive,
        coverage_ledger: CoverageLedger | None = None,
        transport: HttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        resolver: Callable[[str, int], Iterable[str]] | None = None,
        connector_version: str = "public-evidence-collector-v1",
    ):
        if sources is None:
            raise ValueError("sources must be supplied explicitly")
        self.sources = tuple(
            source if isinstance(source, PublicSourceConfig) else PublicSourceConfig.model_validate(source)
            for source in sources
        )
        self.raw_store = raw_store
        self.archive = archive
        self.coverage_ledger = coverage_ledger
        self.transport = transport or UrllibTransport()
        self.clock = clock or (lambda: datetime.now(UTC))
        self.connector_version = connector_version
        self.resolver = resolver

    def _validate_origin(self, url: str, source: PublicSourceConfig) -> str:
        _origin(url)
        normalized = canonical_url(url)
        origin = _origin(normalized)
        approved = {_origin(source.url), *source.pagination_allowed_origins}
        if origin not in approved:
            raise UnsafeSourceUrlError("source URL origin is not approved")
        return normalized

    def _approve_destination(self, url: str, source: PublicSourceConfig) -> tuple[str, tuple[str, ...]]:
        normalized = self._validate_origin(url, source)
        try:
            _, addresses = approved_https_destination(
                normalized,
                self.resolver or system_resolver,
            )
        except ValueError as exc:
            raise UnsafeSourceUrlError(str(exc)) from exc
        return normalized, addresses

    def collect(self) -> CollectionBatch:
        documents: list[NormalizedEvidence] = []
        coverage: list[SourceCoverageInterval] = []
        captures: list[RawCapture] = []
        errors: list[str] = []
        for source in self.sources:
            if not source.enabled:
                continue
            source_result = self._collect_source(source)
            documents.extend(source_result.documents)
            coverage.extend(source_result.coverage)
            captures.extend(source_result.raw_captures)
            errors.extend(source_result.errors)
        return CollectionBatch(
            documents=tuple(documents),
            coverage=tuple(coverage),
            raw_captures=tuple(captures),
            errors=tuple(errors),
        )

    def _collect_source(self, source: PublicSourceConfig) -> CollectionBatch:
        started_at = utc_datetime(self.clock(), field_name="collection_started_at")
        current_url = source.url
        visited: set[str] = set()
        documents: list[NormalizedEvidence] = []
        captures: list[RawCapture] = []
        errors: list[str] = []
        partial = False
        last_received = started_at

        for page_index in range(source.max_pages):
            if not current_url or current_url in visited:
                break
            try:
                current_url, approved_addresses = self._approve_destination(current_url, source)
            except Exception as exc:
                errors.append(f"{source.source_id} rejected unsafe source URL: {_unsafe_source_detail(exc)}")
                break
            visited.add(current_url)
            request_metadata = safe_request_metadata(
                method="GET",
                url=current_url,
                headers=source.headers,
                timeout_seconds=source.timeout_seconds,
                page_index=page_index,
            )
            try:
                response = self.transport.get(
                    current_url,
                    timeout=source.timeout_seconds,
                    headers=source.headers,
                    max_response_bytes=source.max_response_bytes,
                    approved_addresses=approved_addresses,
                )
                received_at = utc_datetime(self.clock(), field_name="received_at")
                last_received = max(last_received, received_at)
            except Exception as exc:
                last_received = max(last_received, utc_datetime(self.clock(), field_name="failure_received_at"))
                errors.append(
                    f"{source.source_id} request failed for {sanitize_error_text(current_url)}: "
                    f"{sanitize_error_text(exc)}"
                )
                break

            if response.oversized or len(response.body) > source.max_response_bytes:
                errors.append(f"{source.source_id} response exceeded max_response_bytes={source.max_response_bytes}")
                break
            try:
                final_url = self._validate_origin(response.url or current_url, source)
                if not response.peer_address or response.peer_address not in approved_addresses:
                    raise UnsafeSourceUrlError("connection peer was not an approved destination")
            except Exception as exc:
                errors.append(f"{source.source_id} rejected unsafe redirect: {_unsafe_source_detail(exc)}")
                break

            # This capture is deliberately before status, size, content-type,
            # or parser checks. Failed and malformed responses remain auditable.
            capture = self.raw_store.capture(
                response.body,
                platform="public",
                source=source.source_id,
                request=request_metadata,
                received_at=received_at,
                response_metadata=safe_response_metadata(
                    status_code=response.status_code,
                    url=final_url,
                    headers=response.headers,
                ),
            )
            captures.append(capture)

            if response.status_code < 200 or response.status_code >= 300:
                errors.append(
                    f"{source.source_id} returned HTTP {response.status_code} for "
                    f"{sanitize_error_text(current_url)}"
                )
                break
            try:
                entries, next_url = self._parse_response(
                    source,
                    response.body,
                    final_url,
                    response.headers,
                )
            except Exception as exc:
                errors.append(
                    f"{source.source_id} parse failed for {sanitize_error_text(current_url)}: "
                    f"{sanitize_error_text(exc)}"
                )
                break

            remaining = source.max_entries - len(documents)
            if len(entries) > remaining:
                entries = entries[:remaining]
                partial = True
                errors.append(f"{source.source_id} entry bound reached at {source.max_entries}")
            for entry in entries:
                try:
                    observation = self._normalize_entry(source, entry, capture, received_at)
                    documents.append(self.archive.ingest(observation))
                except EvidenceConflictError as exc:
                    errors.append(
                        f"{source.source_id} normalized evidence conflict was quarantined: {exc}"
                    )
            if len(documents) >= source.max_entries:
                if next_url:
                    partial = True
                break
            if next_url:
                try:
                    next_url = self._validate_origin(next_url, source)
                except Exception as exc:
                    partial = True
                    errors.append(f"{source.source_id} rejected unsafe pagination URL: {_unsafe_source_detail(exc)}")
                    break
            current_url = next_url
            if next_url and page_index + 1 >= source.max_pages:
                partial = True
                errors.append(f"{source.source_id} page bound reached at {source.max_pages}")

        ended_at = max(last_received, started_at + timedelta(microseconds=1))
        if errors and not documents:
            status = CoverageStatus.UNAVAILABLE
        elif errors:
            status = CoverageStatus.PARTIAL
        elif partial:
            status = CoverageStatus.PARTIAL
        else:
            status = CoverageStatus.COMPLETE
        detail = "; ".join(errors) if errors else f"Collected {len(documents)} public documents."
        interval = SourceCoverageInterval(
            source=source.source_id,
            started_at=started_at,
            ended_at=ended_at,
            status=status,
            connector_version=self.connector_version,
            scope=sanitize_error_text(source.url),
            details=detail,
        )
        if self.coverage_ledger is not None:
            self.coverage_ledger.add(interval)
        return CollectionBatch(
            documents=tuple(documents),
            coverage=(interval,),
            raw_captures=tuple(captures),
            errors=tuple(errors),
        )

    def _parse_response(
        self,
        source: PublicSourceConfig,
        body: bytes,
        response_url: str,
        response_headers: Mapping[str, str],
    ) -> tuple[list[_ParsedEntry], str]:
        if source.format == "document":
            decoded = body.decode("utf-8", errors="replace")
            public_response_url = sanitize_error_text(response_url)
            title = source.document_title or public_response_url
            modified = next(
                (value for key, value in response_headers.items() if key.casefold() == "last-modified"),
                None,
            )
            revision = next(
                (value for key, value in response_headers.items() if key.casefold() == "etag"),
                None,
            ) or modified
            entry = _ParsedEntry(
                source_document_id=public_response_url,
                url=public_response_url,
                publisher=source.publisher,
                title=_clean_text(title),
                body=_clean_text(decoded),
                claimed_published_at=None,
                modified_at=modified,
                source_revision=revision,
                metadata={"document_format": "public_document"},
            )
            return [entry], ""

        root = ET.fromstring(body)
        root_name = _local_name(root.tag)
        if source.format == "rss" and root_name not in {"rss", "rdf"}:
            raise ValueError("configured RSS source did not return RSS")
        if source.format == "atom" and root_name != "feed":
            raise ValueError("configured Atom source did not return Atom")
        nodes = [node for node in root.iter() if _local_name(node.tag) in {"item", "entry"}]
        feed_publisher = source.publisher
        if not feed_publisher:
            feed_publisher = _child_text(root, "title")
        entries: list[_ParsedEntry] = []
        for ordinal, node in enumerate(nodes):
            link = canonical_url(_entry_link(node), base_url=response_url)
            title = _clean_text(_child_text(node, "title"))
            body_text = _clean_text(_child_text(node, "content", "description", "summary"))
            entry_id = _child_text(node, "guid", "id") or link
            if not entry_id:
                seed = f"{title}|{body_text}|{ordinal}"
                entry_id = f"generated:{hashlib.sha256(seed.encode('utf-8')).hexdigest()}"
            claimed_raw = _child_text(node, "pubdate", "published", "date") or None
            modified_raw = _child_text(node, "updated", "modified") or None
            entries.append(
                _ParsedEntry(
                    source_document_id=entry_id,
                    url=link,
                    publisher=feed_publisher,
                    title=title,
                    body=body_text,
                    claimed_published_at=claimed_raw,
                    modified_at=modified_raw,
                    source_revision=modified_raw,
                    metadata={
                        "entry_ordinal": ordinal,
                        "feed_url": sanitize_error_text(response_url),
                        "claimed_published_at_raw": claimed_raw,
                        "modified_at_raw": modified_raw,
                    },
                )
            )
        return entries, _next_link(root, response_url)

    def _normalize_entry(
        self,
        source: PublicSourceConfig,
        entry: _ParsedEntry,
        capture: RawCapture,
        received_at: datetime,
    ) -> NormalizedEvidence:
        modified = None
        if entry.modified_at:
            try:
                modified = utc_datetime(entry.modified_at, field_name="modified_at", allow_none=True)
            except ValueError:
                modified = None
        metadata = {
            **entry.metadata,
            "publisher": entry.publisher,
            "canonical_url": entry.url,
            "raw_sha256": capture.sha256,
            "receipt_path": str(capture.receipt_path),
            "source_revision_raw": entry.source_revision,
        }
        normalize_kwargs = dict(
            source=source.source_id,
            source_document_id=entry.source_document_id,
            retrieved_at=received_at,
            first_seen_at=received_at,
            claimed_published_at=entry.claimed_published_at,
            modified_at=modified,
            backfill=source.backfill,
            url=entry.url,
            title=entry.title,
            body=entry.body,
            raw_artifact_uid=f"public:raw/{capture.sha256}",
            parser_version=self.connector_version,
            metadata=metadata,
        )
        content_probe = normalize_evidence(**normalize_kwargs, source_revision=None)
        revision_seed = json.dumps(
            {
                "claimed_published_at": entry.claimed_published_at,
                "content_hash": content_probe.content_hash,
                "modified_at": entry.modified_at,
                "publisher": entry.publisher,
                "upstream_revision": entry.source_revision,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        canonical_revision = hashlib.sha256(revision_seed.encode("utf-8")).hexdigest()
        observation = normalize_evidence(
            **normalize_kwargs,
            source_revision=canonical_revision,
        )
        prior = [
            item
            for item in self.archive.revisions(observation.document_uid)
            if item.content_hash == observation.content_hash
            and item.revision_uid == observation.revision_uid
        ]
        if prior:
            earliest = min(item.first_seen_at for item in prior)
            if earliest < observation.first_seen_at:
                observation = observation.model_copy(update={"first_seen_at": earliest})
        return observation


__all__ = [
    "CollectionBatch",
    "HttpResponse",
    "HttpTransport",
    "PublicEvidenceCollector",
    "PublicSourceConfig",
    "UrllibTransport",
    "UnsafeSourceUrlError",
    "canonical_url",
    "load_source_config",
]
