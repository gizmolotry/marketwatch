"""Append-only, content-addressed storage for unmodified source payloads."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .normalize import canonical_json_bytes, jsonable, require_text, utc_datetime


@dataclass(frozen=True, slots=True)
class RawCapture:
    sha256: str
    byte_length: int
    object_path: Path
    receipt_path: Path
    received_at: datetime
    platform: str
    source: str


class RawArtifactIntegrityError(IOError):
    """Raised when a content-addressed object no longer matches its name."""


def _atomic_create(path: Path, payload: bytes) -> bool:
    """Create a file atomically and never replace an existing artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
            return True
        except FileExistsError:
            return False
        except OSError:
            # Windows may not permit hard-link creation in some environments.
            try:
                with path.open("xb") as target:
                    target.write(payload)
                    target.flush()
                    os.fsync(target.fileno())
                return True
            except FileExistsError:
                return False
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class RawArtifactStore:
    """Store response bodies by SHA-256 and each retrieval as a receipt.

    A repeated response reuses the immutable object but still receives a new
    receipt, retaining request and retrieval provenance without duplicating the
    potentially large response body.
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.objects = self.root / "objects" / "sha256"
        self.receipts = self.root / "receipts"

    def capture(
        self,
        payload: bytes | str,
        *,
        platform: str,
        source: str,
        request: Mapping[str, Any],
        received_at: datetime | None = None,
        response_metadata: Mapping[str, Any] | None = None,
    ) -> RawCapture:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        digest = hashlib.sha256(raw).hexdigest()
        object_path = self.objects / digest[:2] / digest[2:4] / f"{digest}.raw"
        _atomic_create(object_path, raw)

        # `_atomic_create` deliberately never replaces an existing object.  An
        # existing digest-named file therefore has to be authenticated before
        # this retrieval may receive a new authoritative receipt.  Otherwise a
        # damaged object could be made to look freshly captured and then flow
        # into normalization under the expected digest.
        try:
            stored_length = object_path.stat().st_size
            stored_digest = hashlib.sha256(object_path.read_bytes()).hexdigest()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                f"raw artifact could not be verified before receipt creation: {digest}"
            ) from exc
        if stored_length != len(raw) or stored_digest != digest:
            raise RawArtifactIntegrityError(
                "raw artifact integrity mismatch before receipt creation: "
                f"expected sha256={digest} bytes={len(raw)}, "
                f"observed sha256={stored_digest} bytes={stored_length}"
            )

        timestamp = utc_datetime(received_at or datetime.now(UTC), "received_at")
        receipt_id = f"{timestamp.strftime('%Y%m%dT%H%M%S.%fZ')}-{uuid.uuid4().hex}"
        receipt_path = self.receipts / timestamp.strftime("%Y-%m-%d") / f"{receipt_id}.json"
        receipt = {
            "schema_version": 1,
            "receipt_id": receipt_id,
            "sha256": digest,
            "byte_length": len(raw),
            "platform": require_text(platform, "platform").lower(),
            "source": require_text(source, "source"),
            "received_at": timestamp,
            "request": dict(request),
            "response_metadata": dict(response_metadata or {}),
        }
        if not _atomic_create(receipt_path, canonical_json_bytes(receipt)):
            raise RuntimeError(f"receipt collision: {receipt_path}")
        return RawCapture(
            sha256=digest,
            byte_length=len(raw),
            object_path=object_path,
            receipt_path=receipt_path,
            received_at=timestamp,
            platform=receipt["platform"],
            source=receipt["source"],
        )

    def read(self, sha256: str, *, verify: bool = True) -> bytes:
        digest = require_text(sha256, "sha256").lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        path = self.objects / digest[:2] / digest[2:4] / f"{digest}.raw"
        payload = path.read_bytes()
        if verify and hashlib.sha256(payload).hexdigest() != digest:
            raise IOError(f"raw artifact hash mismatch: {digest}")
        return payload

    def verify(self, capture_or_hash: RawCapture | str) -> bool:
        digest = capture_or_hash.sha256 if isinstance(capture_or_hash, RawCapture) else capture_or_hash
        try:
            self.read(digest, verify=True)
            return True
        except (FileNotFoundError, IOError, ValueError):
            return False

    def read_receipt(self, capture: RawCapture | Path | str) -> dict[str, Any]:
        path = capture.receipt_path if isinstance(capture, RawCapture) else Path(capture)
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def to_domain(capture: RawCapture, *, source_uid: str, parser_version: str):
        """Create the canonical lineage record for a captured payload."""

        from marketleak.domain import RawArtifact

        artifact_uid = f"{capture.platform}:raw/{capture.sha256}"
        return RawArtifact(
            event_time=capture.received_at,
            ingested_at=capture.received_at,
            source_uid=source_uid,
            raw_artifact_uid=artifact_uid,
            parser_version=parser_version,
            content_hash=f"sha256:{capture.sha256}",
            media_type="application/json",
            storage_uri=capture.object_path.resolve().as_uri(),
            byte_length=capture.byte_length,
            retrieved_at=capture.received_at,
        )
