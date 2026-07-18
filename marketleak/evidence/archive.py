"""Append-only point-in-time evidence archive with optional persistence."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

from marketleak.evidence.normalize import NormalizedEvidence, utc_datetime


class EvidenceArchiveIntegrityError(RuntimeError):
    pass


class EvidenceConflictError(RuntimeError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    if isinstance(value, NormalizedEvidence):
        value = value.model_dump(mode="json")
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _atomic_create(path: Path, payload: bytes) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            with path.open("xb") as target:
                target.write(payload)
                target.flush()
                os.fsync(target.fileno())
            return True
        except FileExistsError:
            return False
    finally:
        temporary.unlink(missing_ok=True)


def _revision_storage_key(observation: NormalizedEvidence) -> str:
    seed = "|".join(
        (observation.document_uid, observation.revision_uid, observation.content_hash)
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _expected_content_hash(observation: NormalizedEvidence) -> str:
    payload = {
        "body": observation.body,
        "source_document_id": observation.source_document_id,
        "title": observation.title,
        "url": observation.url,
    }
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


class EvidenceArchive:
    """Revision-idempotent archive with restart-safe point-in-time state.

    Without ``root`` it preserves the previous in-memory API. With ``root``,
    every unique content revision is atomically created as canonical JSON and
    verified when the process restarts.
    """

    def __init__(
        self,
        observations: Iterable[NormalizedEvidence] = (),
        *,
        root: str | Path | None = None,
    ):
        self._lock = RLock()
        self.root = Path(root) if root is not None else None
        self.records_dir = self.root / "revisions" if self.root is not None else None
        self.quarantine_dir = self.root / "quarantine" if self.root is not None else None
        self._by_uid: dict[str, NormalizedEvidence] = {}
        self._by_document: dict[str, list[str]] = defaultdict(list)
        self._by_revision_key: dict[tuple[str, str, str], str] = {}
        self._revision_content: dict[tuple[str, str], str] = {}
        self._quarantined: list[dict[str, Any]] = []
        if self.root is not None:
            self.records_dir.mkdir(parents=True, exist_ok=True)
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)
            self._load()
        for observation in observations:
            self.ingest(observation)

    @property
    def quarantine_count(self) -> int:
        return len(self._quarantined) + (
            len(tuple(self.quarantine_dir.glob("*.json")))
            if self.quarantine_dir is not None
            else 0
        )

    def _record_path(self, observation: NormalizedEvidence) -> Path:
        assert self.records_dir is not None
        key = _revision_storage_key(observation)
        return self.records_dir / key[:2] / f"{key}.json"

    def _quarantine(
        self,
        *,
        reason: str,
        incoming: NormalizedEvidence,
        existing: NormalizedEvidence | None,
    ) -> None:
        payload = {
            "reason": reason,
            "quarantined_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "incoming": incoming.model_dump(mode="json"),
            "existing": existing.model_dump(mode="json") if existing is not None else None,
        }
        if self.quarantine_dir is None:
            self._quarantined.append(payload)
            return
        digest = hashlib.sha256(_canonical_bytes(payload)).hexdigest()
        _atomic_create(self.quarantine_dir / f"{digest}.json", _canonical_bytes(payload))

    def _register(self, observation: NormalizedEvidence, *, loading: bool = False) -> NormalizedEvidence:
        existing_uid = self._by_uid.get(observation.evidence_uid)
        if existing_uid is not None and existing_uid != observation:
            if loading:
                raise EvidenceArchiveIntegrityError(
                    f"evidence_uid collision on disk: {observation.evidence_uid}"
                )
            self._quarantine(reason="evidence_uid_collision", incoming=observation, existing=existing_uid)
            raise EvidenceConflictError(f"evidence_uid collision: {observation.evidence_uid}")

        revision_identity = (observation.document_uid, observation.revision_uid)
        known_content = self._revision_content.get(revision_identity)
        if known_content is not None and known_content != observation.content_hash:
            existing = next(
                (
                    item
                    for item in self._by_uid.values()
                    if item.document_uid == observation.document_uid
                    and item.revision_uid == observation.revision_uid
                ),
                None,
            )
            if loading:
                raise EvidenceArchiveIntegrityError(
                    f"revision collision on disk: {observation.revision_uid}"
                )
            self._quarantine(reason="revision_content_conflict", incoming=observation, existing=existing)
            raise EvidenceConflictError(f"revision content conflict: {observation.revision_uid}")

        revision_key = (
            observation.document_uid,
            observation.revision_uid,
            observation.content_hash,
        )
        canonical_uid = self._by_revision_key.get(revision_key)
        if canonical_uid is not None:
            existing = self._by_uid[canonical_uid]
            # Normal collection is chronological. An imported older receipt is
            # retained as a separate immutable observation so the earliest
            # point-in-time fact remains recoverable.
            if observation.first_seen_at >= existing.first_seen_at:
                return existing

        self._by_uid[observation.evidence_uid] = observation
        self._by_document[observation.document_uid].append(observation.evidence_uid)
        self._revision_content[revision_identity] = observation.content_hash
        if canonical_uid is None or observation.first_seen_at < self._by_uid[canonical_uid].first_seen_at:
            self._by_revision_key[revision_key] = observation.evidence_uid
        return observation

    def ingest(self, observation: NormalizedEvidence) -> NormalizedEvidence:
        if not isinstance(observation, NormalizedEvidence):
            observation = NormalizedEvidence.model_validate(observation)
        if _expected_content_hash(observation) != observation.content_hash:
            self._quarantine(reason="normalized_content_hash_mismatch", incoming=observation, existing=None)
            raise EvidenceConflictError("normalized evidence content hash mismatch")
        with self._lock:
            existing_uid_record = self._by_uid.get(observation.evidence_uid)
            if existing_uid_record is not None and existing_uid_record != observation:
                self._quarantine(
                    reason="evidence_uid_collision",
                    incoming=observation,
                    existing=existing_uid_record,
                )
                raise EvidenceConflictError(
                    f"evidence_uid collision: {observation.evidence_uid}"
                )
            revision_identity = (observation.document_uid, observation.revision_uid)
            known_content = self._revision_content.get(revision_identity)
            if known_content is not None and known_content != observation.content_hash:
                existing_revision = next(
                    (
                        item
                        for item in self._by_uid.values()
                        if item.document_uid == observation.document_uid
                        and item.revision_uid == observation.revision_uid
                    ),
                    None,
                )
                self._quarantine(
                    reason="revision_content_conflict",
                    incoming=observation,
                    existing=existing_revision,
                )
                raise EvidenceConflictError(
                    f"revision content conflict: {observation.revision_uid}"
                )
            revision_key = (
                observation.document_uid,
                observation.revision_uid,
                observation.content_hash,
            )
            existing_uid = self._by_revision_key.get(revision_key)
            if existing_uid is not None and observation.first_seen_at >= self._by_uid[existing_uid].first_seen_at:
                return self._by_uid[existing_uid]
            if self.root is not None:
                path = self._record_path(observation)
                payload = _canonical_bytes(observation)
                created = _atomic_create(path, payload)
                if not created and path.read_bytes() != payload:
                    existing = None
                    try:
                        existing = NormalizedEvidence.model_validate_json(path.read_bytes())
                    except Exception:
                        pass
                    self._quarantine(
                        reason="revision_path_collision",
                        incoming=observation,
                        existing=existing,
                    )
                    raise EvidenceConflictError(f"persistent revision collision: {path.name}")
            return self._register(observation)

    def _load(self) -> None:
        assert self.records_dir is not None
        for path in sorted(self.records_dir.glob("*/*.json")):
            raw = path.read_bytes()
            try:
                observation = NormalizedEvidence.model_validate_json(raw)
            except Exception as exc:
                raise EvidenceArchiveIntegrityError(f"invalid evidence record: {path}") from exc
            if raw != _canonical_bytes(observation):
                raise EvidenceArchiveIntegrityError(f"non-canonical evidence record: {path}")
            if path != self._record_path(observation):
                raise EvidenceArchiveIntegrityError(f"evidence record path/hash mismatch: {path}")
            if _expected_content_hash(observation) != observation.content_hash:
                raise EvidenceArchiveIntegrityError(f"evidence content hash mismatch: {path}")
            self._register(observation, loading=True)

    def verify(self) -> tuple[NormalizedEvidence, ...]:
        if self.root is None:
            return self.all()
        check = EvidenceArchive(root=self.root)
        if check.all() != self.all():
            raise EvidenceArchiveIntegrityError("in-memory and persisted evidence indexes differ")
        return check.all()

    def all(self) -> tuple[NormalizedEvidence, ...]:
        with self._lock:
            values = tuple(self._by_uid.values())
        return tuple(sorted(values, key=lambda item: (item.first_seen_at, item.source, item.evidence_uid)))

    def as_of(self, cutoff: datetime, *, sources: Iterable[str] | None = None) -> tuple[NormalizedEvidence, ...]:
        cutoff_utc = utc_datetime(cutoff, field_name="cutoff")
        source_set = {str(source) for source in sources} if sources is not None else None
        return tuple(
            item
            for item in self.all()
            if item.first_seen_at <= cutoff_utc
            and item.retrieved_at <= cutoff_utc
            and (source_set is None or item.source in source_set)
        )

    def revisions(self, document_uid: str, *, as_of: datetime | None = None) -> tuple[NormalizedEvidence, ...]:
        with self._lock:
            values = [self._by_uid[uid] for uid in self._by_document.get(document_uid, ())]
        if as_of is not None:
            cutoff = utc_datetime(as_of, field_name="as_of")
            values = [item for item in values if item.first_seen_at <= cutoff and item.retrieved_at <= cutoff]
        return tuple(sorted(values, key=lambda item: (item.first_seen_at, item.retrieved_at, item.evidence_uid)))


class PersistentEvidenceArchive(EvidenceArchive):
    def __init__(self, root: str | Path):
        super().__init__(root=root)


__all__ = [
    "EvidenceArchive",
    "EvidenceArchiveIntegrityError",
    "EvidenceConflictError",
    "PersistentEvidenceArchive",
]
