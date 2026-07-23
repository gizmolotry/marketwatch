"""Rebuildable, exact-fact index for causal public-wallet cohort analysis.

The SQLite database is a derived acceleration artifact.  Canonical JSON files
remain the evidence of record and are verified both while indexing and when a
row is materialized.  Economic values are stored as decimal strings; SQLite
floating-point arithmetic is deliberately not used.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from hashlib import sha256
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from marketleak.actors.features import ActorFeatures, ActorPosition
from marketleak.domain import ActorVisibility, TradeFill, TradeSide
from marketleak.ingestion.normalize import canonical_json_bytes


SCHEMA_VERSION = 1
_FILL_COLUMNS = (
    "fill_uid", "actor_uid", "actor_visibility", "market_uid", "outcome_uid",
    "platform", "source_uid", "event_time", "ingested_at", "side",
    "price_decimal", "size_decimal", "notional_decimal", "price_key", "size_key", "transaction_uid",
    "raw_artifact_uid", "canonical_path", "canonical_sha256",
)
_REQUIRED_INDEXES = frozenset(
    {"sqlite_autoindex_fills_1", "fills_actor_clocks", "fills_market_clocks", "fills_outcome_clocks", "fills_source_clocks"}
)
_REQUIRED_TABLES = frozenset({"fills", "conflicts", "source_inventory", "manifest"})
_REQUIRED_TRIGGERS = frozenset(
    {
        "immutable_manifest_update", "immutable_manifest_delete",
        "sealed_fills_insert", "sealed_fills_update", "sealed_fills_delete",
        "sealed_conflicts_insert", "sealed_conflicts_update", "sealed_conflicts_delete",
        "sealed_inventory_insert", "sealed_inventory_update", "sealed_inventory_delete",
    }
)


class CohortStoreError(RuntimeError):
    """Base error for a cohort index that cannot be trusted."""


class CanonicalFileError(CohortStoreError):
    """A source file is not the exact canonical record it claims to be."""


class FillConflictError(CohortStoreError):
    """The same fill UID was observed with different canonical bytes."""

    def __init__(self, fill_uid: str, existing_sha256: str, incoming_sha256: str) -> None:
        self.fill_uid = fill_uid
        self.existing_sha256 = existing_sha256
        self.incoming_sha256 = incoming_sha256
        super().__init__(f"conflicting canonical bytes for {fill_uid}")


class IndexedFileCorruptionError(CohortStoreError):
    """An indexed canonical file changed or disappeared after indexing."""


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _time(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _resolve_source_path(value: str | Path, *, description: str) -> Path:
    try:
        return Path(value).resolve(strict=True)
    except OSError as exc:
        raise CanonicalFileError(f"cannot resolve {description}: {value}") from exc


def _decimal_key(value: Decimal) -> str:
    """Canonical equality key without applying the ambient decimal context."""

    sign, digits, exponent = value.as_tuple()
    mutable = list(digits)
    while mutable and mutable[-1] == 0:
        mutable.pop()
        exponent += 1
    if not mutable:
        return "0"
    coefficient = "".join(str(digit) for digit in mutable)
    return f"{'-' if sign else ''}{coefficient}e{exponent}"


def _mul_exact(left: Decimal, right: Decimal) -> Decimal:
    precision = max(1, len(left.as_tuple().digits) + len(right.as_tuple().digits))
    with localcontext() as context:
        context.prec = precision
        return left * right


def _add_exact(left: Decimal, right: Decimal) -> Decimal:
    left_places = max(0, -left.as_tuple().exponent)
    right_places = max(0, -right.as_tuple().exponent)
    integer_digits = max(left.adjusted() + 1, right.adjusted() + 1, 1)
    with localcontext() as context:
        context.prec = integer_digits + max(left_places, right_places) + 2
        return left + right


@dataclass(frozen=True, slots=True)
class CohortCursor:
    event_time: str
    ingested_at: str
    fill_uid: str

    def encode(self) -> str:
        return canonical_json_bytes(
            {"event_time": self.event_time, "fill_uid": self.fill_uid, "ingested_at": self.ingested_at}
        ).hex()

    @classmethod
    def decode(cls, value: str) -> "CohortCursor":
        try:
            payload = json.loads(bytes.fromhex(value))
            return cls(
                event_time=str(payload["event_time"]),
                ingested_at=str(payload["ingested_at"]),
                fill_uid=str(payload["fill_uid"]),
            )
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            raise ValueError("invalid cohort cursor") from exc


@dataclass(frozen=True, slots=True)
class CohortFill:
    fill_uid: str
    actor_uid: str
    actor_visibility: ActorVisibility
    market_uid: str
    outcome_uid: str
    platform: str
    source_uid: str
    event_time: datetime
    ingested_at: datetime
    side: TradeSide
    price: Decimal
    size: Decimal
    notional: Decimal
    transaction_uid: str | None
    raw_artifact_uid: str
    canonical_sha256: str
    canonical_path: Path


@dataclass(frozen=True, slots=True)
class CohortPage:
    items: tuple[CohortFill, ...]
    next_cursor: str | None
    has_more: bool


@dataclass(frozen=True, slots=True)
class CohortManifest:
    schema_version: int
    snapshot_sha256: str
    source_file_count: int
    indexed_fill_count: int
    duplicate_file_count: int
    source_counts: tuple[tuple[str, int], ...]


class WalletCohortStore:
    """SQLite index over immutable canonical ``TradeFill`` JSON files."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not self.database_path.exists() or self.database_path.stat().st_size == 0
        if is_new:
            self._initialize()
        else:
            self._validate_existing()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(str(self.database_path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS fills (
                    fill_uid TEXT PRIMARY KEY,
                    actor_uid TEXT NOT NULL,
                    actor_visibility TEXT NOT NULL,
                    market_uid TEXT NOT NULL,
                    outcome_uid TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    source_uid TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price_decimal TEXT NOT NULL,
                    size_decimal TEXT NOT NULL,
                    notional_decimal TEXT NOT NULL,
                    price_key TEXT NOT NULL,
                    size_key TEXT NOT NULL,
                    transaction_uid TEXT,
                    raw_artifact_uid TEXT NOT NULL,
                    canonical_path TEXT NOT NULL,
                    canonical_sha256 TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS fills_actor_clocks
                    ON fills(actor_uid, event_time, ingested_at, fill_uid);
                CREATE INDEX IF NOT EXISTS fills_market_clocks
                    ON fills(market_uid, event_time, ingested_at, fill_uid);
                CREATE INDEX IF NOT EXISTS fills_outcome_clocks
                    ON fills(outcome_uid, event_time, ingested_at, fill_uid);
                CREATE INDEX IF NOT EXISTS fills_source_clocks
                    ON fills(source_uid, event_time, ingested_at, fill_uid);
                CREATE TABLE IF NOT EXISTS conflicts (
                    fill_uid TEXT NOT NULL,
                    existing_sha256 TEXT NOT NULL,
                    incoming_sha256 TEXT NOT NULL,
                    incoming_path TEXT NOT NULL,
                    PRIMARY KEY(fill_uid, incoming_sha256)
                );
                CREATE TABLE IF NOT EXISTS source_inventory (
                    canonical_path TEXT NOT NULL,
                    canonical_sha256 TEXT NOT NULL,
                    fill_uid TEXT NOT NULL,
                    source_root TEXT NOT NULL,
                    PRIMARY KEY(source_root, canonical_path)
                );
                CREATE TABLE IF NOT EXISTS manifest (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version INTEGER NOT NULL,
                    snapshot_sha256 TEXT NOT NULL,
                    source_file_count INTEGER NOT NULL,
                    indexed_fill_count INTEGER NOT NULL,
                    duplicate_file_count INTEGER NOT NULL,
                    source_counts_json TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS immutable_manifest_update
                    BEFORE UPDATE ON manifest BEGIN SELECT RAISE(ABORT, 'manifest is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS immutable_manifest_delete
                    BEFORE DELETE ON manifest BEGIN SELECT RAISE(ABORT, 'manifest is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_fills_insert BEFORE INSERT ON fills
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed cohort fills are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_fills_update BEFORE UPDATE ON fills
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed cohort fills are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_fills_delete BEFORE DELETE ON fills
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed cohort fills are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_conflicts_insert BEFORE INSERT ON conflicts
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed cohort conflicts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_conflicts_update BEFORE UPDATE ON conflicts
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed cohort conflicts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_conflicts_delete BEFORE DELETE ON conflicts
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed cohort conflicts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_inventory_insert BEFORE INSERT ON source_inventory
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed source inventory is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_inventory_update BEFORE UPDATE ON source_inventory
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed source inventory is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS sealed_inventory_delete BEFORE DELETE ON source_inventory
                    WHEN EXISTS(SELECT 1 FROM manifest)
                    BEGIN SELECT RAISE(ABORT, 'sealed source inventory is immutable'); END;
                """
            )

    def _validate_existing(self) -> None:
        """Inspect an existing database without repairing or changing it."""

        try:
            with self._connect() as connection:
                objects = {
                    (str(row["type"]), str(row["name"]))
                    for row in connection.execute(
                        "SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                    )
                }
                missing_tables = _REQUIRED_TABLES - {name for kind, name in objects if kind == "table"}
                missing_indexes = _REQUIRED_INDEXES - {
                    str(row["name"]) for row in connection.execute("PRAGMA index_list('fills')")
                }
                missing_triggers = _REQUIRED_TRIGGERS - {name for kind, name in objects if kind == "trigger"}
                if missing_tables or missing_indexes or missing_triggers:
                    raise IndexedFileCorruptionError("existing cohort database schema is incomplete")
                sealed = connection.execute("SELECT 1 FROM manifest WHERE singleton=1").fetchone() is not None
        except sqlite3.DatabaseError as exc:
            raise IndexedFileCorruptionError("existing cohort database is unreadable") from exc
        if sealed:
            self.verify_integrity()

    @staticmethod
    def _validate_file(path: Path) -> tuple[TradeFill, bytes, str]:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise CanonicalFileError(f"cannot read canonical file: {path}") from exc
        try:
            fill = TradeFill.model_validate_json(raw)
        except Exception as exc:
            raise CanonicalFileError(f"invalid TradeFill model: {path}") from exc
        expected_name = f"{sha256(fill.fill_uid.encode('utf-8')).hexdigest()}.json"
        if path.name != expected_name:
            raise CanonicalFileError(f"filename does not match fill UID: {path}")
        if raw != canonical_json_bytes(fill):
            raise CanonicalFileError(f"bytes are not canonical JSON: {path}")
        if fill.event_time > fill.ingested_at:
            raise CanonicalFileError(f"event_time follows ingested_at: {path}")
        if fill.actor_uid is None or fill.actor_visibility not in {
            ActorVisibility.PUBLIC_WALLET,
            ActorVisibility.OWN_ACCOUNT_ONLY,
        }:
            raise CanonicalFileError(f"fill has no observable actor: {path}")
        return fill, raw, sha256(raw).hexdigest()

    @staticmethod
    def _row(fill: TradeFill, path: Path, digest: str) -> tuple[object, ...]:
        return (
            fill.fill_uid,
            fill.actor_uid,
            fill.actor_visibility.value,
            fill.market_uid,
            fill.outcome_uid,
            fill.platform,
            fill.source_uid,
            _time(fill.event_time),
            _time(fill.ingested_at),
            fill.side.value,
            str(fill.price),
            str(fill.size),
            str(_mul_exact(fill.price, fill.size)),
            _decimal_key(fill.price),
            _decimal_key(fill.size),
            fill.transaction_uid,
            fill.raw_artifact_uid,
            str(path.resolve()),
            digest,
        )

    def add_files(
        self,
        paths: Iterable[str | Path],
        *,
        batch_size: int = 1_000,
        source_root: str | Path | None = None,
    ) -> tuple[int, int]:
        """Insert verified files in bounded bulk transactions.

        Returns ``(inserted, exact_duplicates)``. A conflicting UID is recorded
        in ``conflicts`` and then raised; trusted rows are never overwritten.
        """

        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        with self._connect() as connection:
            if connection.execute("SELECT 1 FROM manifest").fetchone() is not None:
                raise CohortStoreError("sealed cohort index is immutable; rebuild a new index")
        inserted = duplicates = 0
        root_text = None if source_root is None else str(
            _resolve_source_path(source_root, description="canonical source root")
        )
        batch: list[tuple[TradeFill, Path, str, str]] = []

        def flush(items: Sequence[tuple[TradeFill, Path, str, str]]) -> tuple[int, int]:
            local_inserted = local_duplicates = 0
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                for fill, path, digest, declared_root in items:
                    inventory = connection.execute(
                        "SELECT canonical_sha256,fill_uid,source_root FROM source_inventory "
                        "WHERE source_root=? AND canonical_path=?",
                        (declared_root, str(path)),
                    ).fetchone()
                    if inventory is not None:
                        if (inventory["canonical_sha256"], inventory["fill_uid"], inventory["source_root"]) != (
                            digest, fill.fill_uid, declared_root
                        ):
                            raise CanonicalFileError(f"source inventory path changed: {path}")
                    else:
                        connection.execute(
                            "INSERT INTO source_inventory VALUES(?,?,?,?)",
                            (str(path), digest, fill.fill_uid, declared_root),
                        )
                    existing = connection.execute(
                        "SELECT canonical_sha256 FROM fills WHERE fill_uid=?", (fill.fill_uid,)
                    ).fetchone()
                    if existing is not None:
                        if existing["canonical_sha256"] == digest:
                            local_duplicates += 1
                            continue
                        connection.execute(
                            "INSERT OR IGNORE INTO conflicts VALUES(?,?,?,?)",
                            (fill.fill_uid, existing["canonical_sha256"], digest, str(path.resolve())),
                        )
                        connection.commit()
                        raise FillConflictError(fill.fill_uid, existing["canonical_sha256"], digest)
                    connection.execute(
                        f"INSERT INTO fills VALUES({','.join('?' for _ in _FILL_COLUMNS)})",
                        self._row(fill, path, digest),
                    )
                    local_inserted += 1
                connection.commit()
            return local_inserted, local_duplicates

        iterator = iter(paths)
        while True:
            try:
                source = next(iterator)
            except StopIteration:
                break
            except OSError as exc:
                raise CanonicalFileError("cannot enumerate canonical source files") from exc
            path = _resolve_source_path(source, description="canonical source file")
            fill, _raw, digest = self._validate_file(path)
            batch.append((fill, path, digest, root_text or str(path.parent)))
            if len(batch) >= batch_size:
                added, replayed = flush(batch)
                inserted += added
                duplicates += replayed
                batch.clear()
        if batch:
            added, replayed = flush(batch)
            inserted += added
            duplicates += replayed
        return inserted, duplicates

    @classmethod
    def rebuild(
        cls,
        database_path: str | Path,
        roots: Iterable[str | Path],
        *,
        batch_size: int = 1_000,
    ) -> "WalletCohortStore":
        """Atomically rebuild an index from one or more canonical roots."""

        destination = Path(database_path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            normalized_roots = tuple(
                sorted(
                    (_resolve_source_path(root, description="canonical source root") for root in roots),
                    key=str,
                )
            )
        except OSError as exc:
            raise CanonicalFileError("cannot enumerate canonical source roots") from exc
        if any(not root.is_dir() for root in normalized_roots):
            raise CanonicalFileError("canonical source root must be a directory")
        descriptor, temporary_name = tempfile.mkstemp(prefix=".cohort-rebuild-", suffix=".sqlite3", dir=destination.parent)
        os.close(descriptor)
        temporary = Path(temporary_name)
        temporary.unlink()
        try:
            store = cls(temporary)
            source_counts: list[tuple[str, int]] = []
            inserted = duplicates = source_file_count = 0
            for root in normalized_roots:
                count = 0
                def counted_paths() -> Iterator[Path]:
                    nonlocal count
                    for path in root.rglob("*.json"):
                        count += 1
                        yield path
                root_inserted, root_duplicates = store.add_files(
                    counted_paths(), batch_size=batch_size, source_root=root
                )
                inserted += root_inserted
                duplicates += root_duplicates
                source_file_count += count
                source_counts.append((str(root), count))
            store._seal_manifest(
                source_file_count=source_file_count,
                indexed_fill_count=inserted,
                duplicate_file_count=duplicates,
                source_counts=tuple(source_counts),
            )
            store.verify_integrity()
            os.replace(temporary, destination)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return cls(destination)

    def _seal_manifest(
        self,
        *,
        source_file_count: int,
        indexed_fill_count: int,
        duplicate_file_count: int,
        source_counts: tuple[tuple[str, int], ...],
    ) -> None:
        with self._connect() as connection:
            if connection.execute("SELECT 1 FROM manifest").fetchone() is not None:
                raise CohortStoreError("manifest is already sealed")
            snapshot = self._database_snapshot(connection)
            connection.execute(
                "INSERT INTO manifest VALUES(1,?,?,?,?,?,?)",
                (
                    SCHEMA_VERSION,
                    snapshot,
                    source_file_count,
                    indexed_fill_count,
                    duplicate_file_count,
                    canonical_json_bytes(list(source_counts)).decode("utf-8"),
                ),
            )

    @staticmethod
    def _database_snapshot(connection: sqlite3.Connection) -> str:
        digest = sha256()
        sections = (
            (
                "schema",
                connection.execute(
                    "SELECT type,name,tbl_name,COALESCE(sql,'') AS sql FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
                ),
                ("type", "name", "tbl_name", "sql"),
            ),
            (
                "fills",
                connection.execute(f"SELECT {','.join(_FILL_COLUMNS)} FROM fills ORDER BY fill_uid"),
                _FILL_COLUMNS,
            ),
            (
                "conflicts",
                connection.execute(
                    "SELECT fill_uid,existing_sha256,incoming_sha256,incoming_path "
                    "FROM conflicts ORDER BY fill_uid,incoming_sha256"
                ),
                ("fill_uid", "existing_sha256", "incoming_sha256", "incoming_path"),
            ),
            (
                "source_inventory",
                connection.execute(
                    "SELECT canonical_path,canonical_sha256,fill_uid,source_root "
                    "FROM source_inventory ORDER BY source_root,canonical_path"
                ),
                ("canonical_path", "canonical_sha256", "fill_uid", "source_root"),
            ),
        )
        for name, rows, columns in sections:
            digest.update(canonical_json_bytes({"section": name}))
            digest.update(b"\n")
            for row in rows:
                digest.update(canonical_json_bytes([row[column] for column in columns]))
                digest.update(b"\n")
        return digest.hexdigest()

    def manifest(self) -> CohortManifest:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM manifest WHERE singleton=1").fetchone()
        if row is None:
            raise CohortStoreError("index manifest is not sealed")
        return CohortManifest(
            schema_version=int(row["schema_version"]),
            snapshot_sha256=str(row["snapshot_sha256"]),
            source_file_count=int(row["source_file_count"]),
            indexed_fill_count=int(row["indexed_fill_count"]),
            duplicate_file_count=int(row["duplicate_file_count"]),
            source_counts=tuple((str(key), int(value)) for key, value in json.loads(row["source_counts_json"])),
        )

    def verify_integrity(self) -> CohortManifest:
        """Verify SQLite structure and every manifest-bound indexed fact.

        This is the fast integrity gate used before cohort computation. It does
        not claim the source files still exist; ``verify_source_files`` performs
        that slower evidence audit explicitly.
        """

        manifest = self.manifest()
        if manifest.schema_version != SCHEMA_VERSION:
            raise IndexedFileCorruptionError("cohort schema version disagrees with runtime")
        if manifest.duplicate_file_count != manifest.source_file_count - manifest.indexed_fill_count:
            raise IndexedFileCorruptionError("duplicate file count disagrees with sealed manifest totals")
        with self._connect() as connection:
            check = connection.execute("PRAGMA integrity_check").fetchone()
            if check is None or check[0] != "ok":
                raise IndexedFileCorruptionError("SQLite integrity_check failed")
            indexes = {
                str(row["name"])
                for row in connection.execute("PRAGMA index_list('fills')")
            }
            if not _REQUIRED_INDEXES.issubset(indexes):
                raise IndexedFileCorruptionError("required cohort index was removed or renamed")
            count = int(connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0])
            if count != manifest.indexed_fill_count:
                raise IndexedFileCorruptionError("indexed fill count disagrees with sealed manifest")
            inventory_count = int(connection.execute("SELECT COUNT(*) FROM source_inventory").fetchone()[0])
            if inventory_count != manifest.source_file_count:
                raise IndexedFileCorruptionError("source inventory count disagrees with sealed manifest")
            actual_source_counts = tuple(
                (str(row["source_root"]), int(row["count"]))
                for row in connection.execute(
                    "SELECT source_root,COUNT(*) AS count FROM source_inventory GROUP BY source_root ORDER BY source_root"
                )
            )
            if actual_source_counts != manifest.source_counts:
                raise IndexedFileCorruptionError("source inventory roots disagree with sealed manifest")
            if self._database_snapshot(connection) != manifest.snapshot_sha256:
                raise IndexedFileCorruptionError("indexed facts disagree with sealed snapshot")
        return manifest

    def verify_source_files(self) -> int:
        """Reopen and verify every canonical source file referenced by the index."""

        self.verify_integrity()
        checked = 0
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT canonical_path,canonical_sha256,fill_uid FROM source_inventory ORDER BY canonical_path"
            )
            for row in rows:
                path = Path(row["canonical_path"])
                try:
                    fill, _raw, digest = self._validate_file(path)
                except CanonicalFileError as exc:
                    raise IndexedFileCorruptionError(f"source inventory file failed audit: {path}") from exc
                if digest != row["canonical_sha256"] or fill.fill_uid != row["fill_uid"]:
                    raise IndexedFileCorruptionError(f"source inventory mismatch: {path}")
                checked += 1
        if checked != self.manifest().source_file_count:
            raise IndexedFileCorruptionError("source audit did not cover the sealed inventory")
        return checked

    @staticmethod
    def _materialize(row: sqlite3.Row) -> CohortFill:
        path = Path(row["canonical_path"])
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise IndexedFileCorruptionError(f"indexed file unavailable: {path}") from exc
        digest = sha256(raw).hexdigest()
        if digest != row["canonical_sha256"]:
            raise IndexedFileCorruptionError(f"indexed file hash changed: {path}")
        try:
            fill = TradeFill.model_validate_json(raw)
        except Exception as exc:
            raise IndexedFileCorruptionError(f"indexed file no longer validates: {path}") from exc
        if raw != canonical_json_bytes(fill) or fill.fill_uid != row["fill_uid"]:
            raise IndexedFileCorruptionError(f"indexed file identity/canonical bytes changed: {path}")
        price = Decimal(row["price_decimal"])
        size = Decimal(row["size_decimal"])
        notional = Decimal(row["notional_decimal"])
        if price != fill.price or size != fill.size or notional != _mul_exact(fill.price, fill.size):
            raise IndexedFileCorruptionError(f"indexed economic fields disagree with canonical file: {path}")
        return CohortFill(
            fill_uid=fill.fill_uid,
            actor_uid=fill.actor_uid or "",
            actor_visibility=fill.actor_visibility,
            market_uid=fill.market_uid,
            outcome_uid=fill.outcome_uid,
            platform=fill.platform,
            source_uid=fill.source_uid,
            event_time=fill.event_time,
            ingested_at=fill.ingested_at,
            side=fill.side,
            price=price,
            size=size,
            notional=notional,
            transaction_uid=fill.transaction_uid,
            raw_artifact_uid=fill.raw_artifact_uid,
            canonical_sha256=digest,
            canonical_path=path,
        )

    def page(
        self,
        *,
        as_of: datetime,
        start: datetime | None = None,
        end: datetime | None = None,
        actor_uid: str | None = None,
        market_uid: str | None = None,
        source_uid: str | None = None,
        cursor: str | None = None,
        page_size: int = 1_000,
    ) -> CohortPage:
        """Return a causal page, explicitly indicating whether more rows exist."""

        if not 1 <= page_size <= 10_000:
            raise ValueError("page_size must be between 1 and 10000")
        cutoff = _utc(as_of, "as_of")
        lower = None if start is None else _utc(start, "start")
        upper = cutoff if end is None else min(_utc(end, "end"), cutoff)
        if lower is not None and upper < lower:
            return CohortPage((), None, False)
        conditions = ["event_time <= ?", "ingested_at <= ?"]
        parameters: list[object] = [_time(upper), _time(cutoff)]
        if lower is not None:
            conditions.append("event_time >= ?")
            parameters.append(_time(lower))
        for column, value in (("actor_uid", actor_uid), ("market_uid", market_uid), ("source_uid", source_uid)):
            if value is not None:
                conditions.append(f"{column} = ?")
                parameters.append(value)
        decoded = CohortCursor.decode(cursor) if cursor is not None else None
        if decoded is not None:
            conditions.append("(event_time, ingested_at, fill_uid) > (?, ?, ?)")
            parameters.extend((decoded.event_time, decoded.ingested_at, decoded.fill_uid))
        parameters.append(page_size + 1)
        sql = (
            "SELECT * FROM fills WHERE " + " AND ".join(conditions)
            + " ORDER BY event_time, ingested_at, fill_uid LIMIT ?"
        )
        with self._connect() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        has_more = len(rows) > page_size
        visible = rows[:page_size]
        items = tuple(self._materialize(row) for row in visible)
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = CohortCursor(last["event_time"], last["ingested_at"], last["fill_uid"]).encode()
        return CohortPage(items, next_cursor, has_more)

    def iter_fills(self, **kwargs: object) -> Iterator[CohortFill]:
        """Traverse every matching row without a hidden result cap."""

        cursor = kwargs.pop("cursor", None)
        while True:
            page = self.page(cursor=cursor, **kwargs)  # type: ignore[arg-type]
            yield from page.items
            if not page.has_more:
                return
            cursor = page.next_cursor

    def actor_features(
        self,
        *,
        cutoff: datetime,
        available_as_of: datetime | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        market_uid: str | None = None,
        source_uid: str | None = None,
    ) -> tuple[ActorFeatures, ...]:
        """Aggregate exact formulas used by ``build_actor_features``.

        SQL performs semantic de-duplication and causal clock filtering. Rows
        stream actor-by-actor, so memory is bounded by the largest actor rather
        than the complete corpus. Outcome resolutions are intentionally absent
        from this fill-only index, hence realized performance is unavailable.
        """

        self.verify_integrity()
        cutoff = _utc(cutoff, "cutoff")
        availability = cutoff if available_as_of is None else _utc(available_as_of, "available_as_of")
        lower = None if start is None else _utc(start, "start")
        upper = cutoff if end is None else min(cutoff, _utc(end, "end"))
        if lower is not None and upper < lower:
            return ()
        conditions = ["event_time <= ?", "ingested_at <= ?"]
        parameters: list[object] = [_time(upper), _time(availability)]
        if lower is not None:
            conditions.append("event_time >= ?")
            parameters.append(_time(lower))
        if market_uid is not None:
            conditions.append("market_uid = ?")
            parameters.append(market_uid)
        if source_uid is not None:
            conditions.append("source_uid = ?")
            parameters.append(source_uid)
        sql = """
            WITH eligible AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY actor_uid, market_uid, outcome_uid, event_time,
                                 price_key, size_key, side, COALESCE(transaction_uid, '')
                    ORDER BY fill_uid
                ) AS semantic_rank
                FROM fills
                WHERE %s
            )
            SELECT * FROM eligible WHERE semantic_rank=1
            ORDER BY actor_uid, event_time, fill_uid
        """ % " AND ".join(conditions)
        result: list[ActorFeatures] = []
        with self._connect() as connection:
            rows = connection.execute(sql, parameters)
            current_actor: str | None = None
            current_visibility: ActorVisibility | None = None
            fill_count = 0
            markets: set[str] = set()
            directional: dict[str, Decimal] = {}
            positions: dict[tuple[ActorVisibility, str], list[Decimal | int]] = {}

            def finish() -> None:
                nonlocal fill_count
                if current_actor is None or current_visibility is None:
                    return
                actor_positions = tuple(
                    ActorPosition(
                        actor_uid=current_actor,
                        actor_visibility=visibility,
                        outcome_uid=outcome,
                        net_size=_add_exact(values[0], -values[1]),
                        gross_buy_size=values[0],
                        gross_sell_size=values[1],
                        gross_notional=values[2],
                        net_cashflow=values[3],
                        fill_count=int(values[4]),
                    )
                    for (visibility, outcome), values in sorted(positions.items(), key=lambda item: item[0][1])
                )
                gross = Decimal("0")
                for position in actor_positions:
                    gross = _add_exact(gross, position.gross_notional)
                concentration = (
                    max((abs(value) / gross for value in directional.values()), default=Decimal("0"))
                    if gross > 0 else Decimal("0")
                )
                result.append(
                    ActorFeatures(
                        actor_uid=current_actor,
                        actor_visibility=current_visibility,
                        cutoff=cutoff,
                        fill_count=fill_count,
                        market_count=len(markets),
                        outcome_count=len({outcome for _visibility, outcome in positions}),
                        gross_notional=gross,
                        directional_concentration=concentration,
                        positions=actor_positions,
                        resolved_fill_count=0,
                        realized_performance=None,
                    )
                )

            for row in rows:
                actor = str(row["actor_uid"])
                if current_actor is not None and actor != current_actor:
                    finish()
                    fill_count = 0
                    markets = set()
                    directional = {}
                    positions = {}
                if current_actor != actor:
                    current_actor = actor
                    current_visibility = ActorVisibility(row["actor_visibility"])
                price, size = Decimal(row["price_decimal"]), Decimal(row["size_decimal"])
                notional = _mul_exact(price, size)
                sign = Decimal("1") if row["side"] == TradeSide.BUY.value else Decimal("-1")
                outcome = str(row["outcome_uid"])
                visibility = ActorVisibility(row["actor_visibility"])
                values = positions.setdefault((visibility, outcome), [Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), 0])
                if sign > 0:
                    values[0] = _add_exact(values[0], size)
                    values[3] = _add_exact(values[3], -notional)
                else:
                    values[1] = _add_exact(values[1], size)
                    values[3] = _add_exact(values[3], notional)
                values[2] = _add_exact(values[2], notional)
                values[4] += 1
                directional[outcome] = _add_exact(
                    directional.get(outcome, Decimal("0")), notional if sign > 0 else -notional
                )
                markets.add(str(row["market_uid"]))
                fill_count += 1
            finish()
        return tuple(result)
