"""Reconstruct one frozen, event-anchored public-wallet peer cohort.

This module is deliberately narrow.  It replays the three captured public
Polymarket deliveries for the documented Van Dyke/Burdensome-Mix case and
compares the declared wallet with the other public wallets in that same market
at an externally chosen event cutoff.  It does not train a model or infer an
identity, intent, access, or legal outcome.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

from marketleak.actors import (
    CohortQueryFilter,
    CohortMarketScopeBinding,
    CohortRankingReport,
    NuisanceVector,
    PopulationCoverage,
    PopulationCoverageSlice,
    WalletCohortPolicy,
    rank_wallet_cohort,
)
from marketleak.domain import CoverageStatus, TradeFill, TradeSide
from marketleak.ingestion.connectors.polymarket import PolymarketConnector
from marketleak.ingestion.normalize import canonical_json_bytes
from marketleak.ingestion.raw_store import RawCapture


CONDITION_ID = "0x580adc1327de9bf7c179ef5aaffa3377bb5cb252b7d6390b027172d43fd6f993"
MARKET_UID = f"polymarket:market/{CONDITION_ID}"
YES_ASSET = "24918067747661759048720135607687934209172945708698786072564741153847301974566"
YES_OUTCOME_UID = f"polymarket:outcome/{YES_ASSET}"
CANDIDATE_WALLET = "0x31a56e9e690c621ed21de08cb559e9524cdb8ed9"
CANDIDATE_ACTOR_UID = f"polymarket:wallet/{CANDIDATE_WALLET}"
EVENT_CUTOFF = datetime(2026, 1, 3, 9, 20, 59, tzinfo=UTC)

_HEAD_DIGESTS = (
    "3ecb05d539eb07c903098b2c8d0554710004df62e70dd2b5866e0af47d6ca3ed",
    "d820a38bef0e7c96ee39a3b7de2796dea259d97de134e49c1760eea8bab313c7",
)
_TAIL_DIGEST = "2844c24c01a676e7672b01268477161d8a9bc455fef960135206bb92e7b9f75a"


class WalletCaseCohortReplayError(ValueError):
    """Captured cohort inputs or their deterministic reconstruction are invalid."""


def _time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_time(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise WalletCaseCohortReplayError(f"{field} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WalletCaseCohortReplayError(f"{field} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WalletCaseCohortReplayError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class _ExpectedDelivery:
    digest: str
    interval_start: int
    interval_end: int
    offset: int
    received_at: datetime
    expected_row_count: int


_EXPECTED = {
    _HEAD_DIGESTS[0]: _ExpectedDelivery(
        _HEAD_DIGESTS[0], 1, 1767409105, 0, datetime(2026, 7, 19, 21, 24, 16, 367906, tzinfo=UTC), 10000
    ),
    _HEAD_DIGESTS[1]: _ExpectedDelivery(
        _HEAD_DIGESTS[1], 1, 1767409105, 10000, datetime(2026, 7, 19, 21, 24, 18, 215378, tzinfo=UTC), 2679
    ),
    _TAIL_DIGEST: _ExpectedDelivery(
        _TAIL_DIGEST, 1767409106, 1767432059, 0, datetime(2026, 7, 19, 21, 26, 52, 94187, tzinfo=UTC), 9477
    ),
}


@dataclass(frozen=True, slots=True)
class _Delivery:
    expected: _ExpectedDelivery
    capture: RawCapture
    fills: tuple[TradeFill, ...]

    def source_delivery_payload(self) -> dict[str, Any]:
        """Compact, safe binding to one immutable raw object and receipt."""

        receipt_bytes = self.capture.receipt_path.read_bytes()
        return {
            "raw_sha256": self.capture.sha256,
            "raw_byte_length": self.capture.byte_length,
            "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "received_at": _time(self.capture.received_at),
            "request": {
                "method": "GET",
                "url": "https://data-api.polymarket.com/trades",
                "params": {
                    "end": self.expected.interval_end,
                    "limit": 10000,
                    "market": CONDITION_ID,
                    "offset": self.expected.offset,
                    "start": self.expected.interval_start,
                    "takerOnly": False,
                },
            },
            "raw_record_count": len(self.fills),
        }


def _raw_path(root: Path, digest: str) -> Path:
    return root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"


def _one_delivery(root: Path, digest: str) -> _Delivery:
    expected = _EXPECTED[digest]
    raw_path = _raw_path(root, digest)
    if not raw_path.is_file():
        raise WalletCaseCohortReplayError(f"missing frozen raw object {digest}")
    raw = raw_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise WalletCaseCohortReplayError(f"raw SHA-256 mismatch for {digest}")
    receipts = list((root / "raw" / "receipts").rglob("*.json"))
    matching: list[tuple[Path, Mapping[str, Any]]] = []
    for receipt_path in receipts:
        try:
            payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WalletCaseCohortReplayError(f"invalid receipt {receipt_path}") from exc
        if isinstance(payload, Mapping) and payload.get("sha256") == digest:
            matching.append((receipt_path, payload))
    if len(matching) != 1:
        raise WalletCaseCohortReplayError(f"expected exactly one receipt for {digest}")
    receipt_path, receipt = matching[0]
    request = receipt.get("request")
    response = receipt.get("response_metadata")
    if not isinstance(request, Mapping) or not isinstance(response, Mapping):
        raise WalletCaseCohortReplayError(f"receipt metadata is malformed for {digest}")
    params = request.get("params")
    exact_params = {
        "end": expected.interval_end,
        "limit": 10000,
        "market": CONDITION_ID,
        "offset": expected.offset,
        "start": expected.interval_start,
        "takerOnly": False,
    }
    if (
        receipt.get("platform") != "polymarket"
        or receipt.get("source") != "data-api/trades"
        or receipt.get("byte_length") != len(raw)
        or request.get("method") != "GET"
        or request.get("url") != "https://data-api.polymarket.com/trades"
        or params != exact_params
        or response.get("status_code") != 200
        or _parse_time(receipt.get("received_at"), "receipt received_at") != expected.received_at
    ):
        raise WalletCaseCohortReplayError(f"receipt contract mismatch for {digest}")
    capture = RawCapture(
        sha256=digest,
        byte_length=len(raw),
        object_path=raw_path,
        receipt_path=receipt_path,
        received_at=expected.received_at,
        platform="polymarket",
        source="data-api/trades",
    )
    try:
        records = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WalletCaseCohortReplayError(f"raw delivery is not JSON for {digest}") from exc
    if not isinstance(records, list):
        raise WalletCaseCohortReplayError(f"raw delivery must be a JSON list for {digest}")
    if len(records) != expected.expected_row_count:
        raise WalletCaseCohortReplayError(f"page shape is not the exhausted frozen delivery for {digest}")
    try:
        fills = tuple(PolymarketConnector.normalize_trade(item, capture) for item in records if isinstance(item, Mapping))
    except (TypeError, ValueError) as exc:
        raise WalletCaseCohortReplayError(f"cannot normalize frozen delivery {digest}") from exc
    if len(fills) != len(records):
        raise WalletCaseCohortReplayError(f"raw delivery has a non-object record for {digest}")
    lower = datetime.fromtimestamp(expected.interval_start, tz=UTC)
    upper = datetime.fromtimestamp(expected.interval_end, tz=UTC)
    if any(
        fill.market_uid != MARKET_UID or not lower <= fill.event_time <= upper
        for fill in fills
    ):
        raise WalletCaseCohortReplayError(f"delivery scope is outside the declared cohort for {digest}")
    return _Delivery(expected=expected, capture=capture, fills=fills)


def _deduplicate(fills: Iterable[TradeFill]) -> tuple[tuple[TradeFill, ...], int, int]:
    unique: dict[str, TradeFill] = {}
    duplicates = 0
    conflicts = 0
    for fill in fills:
        previous = unique.get(fill.fill_uid)
        if previous is None:
            unique[fill.fill_uid] = fill
        elif canonical_json_bytes(previous) == canonical_json_bytes(fill):
            duplicates += 1
        else:
            conflicts += 1
    return tuple(sorted(unique.values(), key=lambda item: item.fill_uid)), duplicates, conflicts


def _slice(deliveries: tuple[_Delivery, ...], *, start: int, end: int) -> PopulationCoverageSlice:
    fills = tuple(fill for delivery in deliveries for fill in delivery.fills)
    unique, duplicates, conflicts = _deduplicate(fills)
    latest = max(item.capture.received_at for item in deliveries)
    return PopulationCoverageSlice(
        source_uid=PolymarketConnector.TRADE_SOURCE_UID,
        interval_start=datetime.fromtimestamp(start, tz=UTC),
        interval_end=datetime.fromtimestamp(end, tz=UTC),
        retrieved_at=latest,
        complete_through=datetime.fromtimestamp(end, tz=UTC),
        status=CoverageStatus.COMPLETE,
        record_count=len(unique),
        raw_record_count=len(fills),
        duplicate_record_count=duplicates,
        conflict_record_count=conflicts,
        canonical_record_count=len(unique),
        raw_sha256=tuple(item.capture.sha256 for item in deliveries),
        query_filters=(
            CohortQueryFilter.from_value("market", CONDITION_ID),
            CohortQueryFilter.from_value("takerOnly", False),
            CohortQueryFilter.from_value("start", start),
            CohortQueryFilter.from_value("end", end),
        ),
    )


def _descriptive_rank(values: Mapping[str, Decimal], candidate: str) -> dict[str, int]:
    value = values[candidate]
    return {
        "rank": 1 + sum(item > value for item in values.values()),
        "population_size": len(values),
        "strictly_larger_count": sum(item > value for item in values.values()),
        "tied_count": sum(item == value for item in values.values()),
    }


@dataclass(frozen=True, slots=True)
class WalletCaseCohortReplay:
    report: CohortRankingReport
    raw_input_count: int
    canonical_fill_count: int
    exact_duplicate_count: int
    conflict_count: int
    wallet_count: int
    focus_outcome_buy_notional_rank: Mapping[str, int]
    gross_market_notional_rank: Mapping[str, int]
    source_deliveries: tuple[Mapping[str, Any], ...]
    event_cutoff: datetime = EVENT_CUTOFF

    @property
    def report_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self._unsigned_payload())).hexdigest()

    def _unsigned_payload(self) -> dict[str, Any]:
        candidate = next(
            item for item in self.report.rows if item.actor_uid == CANDIDATE_ACTOR_UID
        )
        return {
            "schema_version": "wallet-case-cohort-replay-v1",
            "signal_assessment": {
                "classification": candidate.signal_classification,
                "review_priority": candidate.signal_classification,
                "confidence": "high",
                "coverage": "complete_same_market_population",
                "composite_population_rank": candidate.population_rank,
                "composite_population_size": candidate.population_size,
                "focus_yes_buy_notional_rank": dict(self.focus_outcome_buy_notional_rank),
                "gross_market_notional_rank": dict(self.gross_market_notional_rank),
                "summary": "High-priority wallet activity signal in the frozen same-market cohort.",
                "decision_owner": "human_reviewer",
            },
            "case_uid": "wallet-case:cftc-doj-van-dyke-burdensome-mix-2026",
            "replay_mode": "hindsight_reconstructed",
            "training_eligible": False,
            "event_cutoff": _time(self.event_cutoff),
            "candidate_actor_uid": CANDIDATE_ACTOR_UID,
            "market_uid": MARKET_UID,
            "focus_outcome_uid": YES_OUTCOME_UID,
            "focus_clock_basis": {
                "public_existence_no_later_than": _time(self.report.policy.focus_event_time),
                "event_time_basis": "earliest_same_market_trade_event",
                "published_at": None,
                "publication_status": "unmapped_source_deliveries_contain_no_market_publication_timestamp",
                "publication_time_confidence": "unavailable",
                "existence_bound_confidence": "observed_same_market_trade",
                "available_at": "earliest_raw_delivery_receipt",
            },
            "population": {
                "raw_input_count": self.raw_input_count,
                "canonical_fill_count": self.canonical_fill_count,
                "exact_duplicate_count": self.exact_duplicate_count,
                "conflict_count": self.conflict_count,
                "wallet_count": self.wallet_count,
                "coverage": self.report.coverage.to_payload(),
            },
            "source_deliveries": [dict(item) for item in self.source_deliveries],
            "descriptive_ranks": {
                "focus_outcome_buy_notional": dict(self.focus_outcome_buy_notional_rank),
                "gross_market_notional": dict(self.gross_market_notional_rank),
            },
            "cohort_ranking": self.report.to_payload(),
        }

    def to_payload(self) -> dict[str, Any]:
        return {**self._unsigned_payload(), "report_sha256": self.report_sha256}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_payload())


def build_wallet_case_cohort_replay(head_root: str | Path, tail_root: str | Path) -> WalletCaseCohortReplay:
    """Rebuild the frozen same-market peer comparison from exact raw deliveries."""

    head = Path(head_root)
    tail = Path(tail_root)
    head_deliveries = tuple(_one_delivery(head, digest) for digest in _HEAD_DIGESTS)
    tail_deliveries = (_one_delivery(tail, _TAIL_DIGEST),)
    all_fills = tuple(fill for delivery in (*head_deliveries, *tail_deliveries) for fill in delivery.fills)
    unique, duplicates, conflicts = _deduplicate(all_fills)
    if (len(all_fills), len(unique), duplicates, conflicts) != (22156, 21785, 371, 0):
        raise WalletCaseCohortReplayError("frozen delivery counts do not match the documented cohort")
    actors = {fill.actor_uid for fill in unique}
    if len(actors) != 3449 or CANDIDATE_ACTOR_UID not in actors:
        raise WalletCaseCohortReplayError("frozen cohort actor population is unexpected")

    first_slice = _slice(head_deliveries, start=1, end=1767409105)
    second_slice = _slice(tail_deliveries, start=1767409106, end=1767432059)
    # The cohort contract intentionally contains the two contiguous source
    # slices.  This is not represented as one invented API request.
    coverage = PopulationCoverage(
        platform="polymarket",
        dataset="public_market_trades",
        source_uid=PolymarketConnector.TRADE_SOURCE_UID,
        scope_kind="market",
        scope_market_uids=(MARKET_UID,),
        scope_market_bindings=(
            CohortMarketScopeBinding(
                canonical_market_uid=MARKET_UID,
                source_query_market_id=CONDITION_ID,
            ),
        ),
        interval_start=datetime.fromtimestamp(1, tz=UTC),
        interval_end=EVENT_CUTOFF,
        retrieved_at=second_slice.retrieved_at,
        as_of=EVENT_CUTOFF,
        complete_through=EVENT_CUTOFF,
        status=CoverageStatus.COMPLETE,
        record_count=len(unique),
        raw_record_count=len(all_fills),
        duplicate_record_count=duplicates,
        conflict_record_count=conflicts,
        canonical_record_count=len(unique),
        raw_sha256=tuple(sorted((*_HEAD_DIGESTS, _TAIL_DIGEST))),
        query_filters=(
            CohortQueryFilter.from_value("market", CONDITION_ID),
            CohortQueryFilter.from_value("takerOnly", False),
            CohortQueryFilter.from_value("start", 1),
            CohortQueryFilter.from_value("end", int(EVENT_CUTOFF.timestamp())),
        ),
        slices=(first_slice, second_slice),
    )
    policy = WalletCohortPolicy(
        as_of=EVENT_CUTOFF,
        # This policy and focus mapping were frozen from the captures in July,
        # not retroactively claimed to have existed at the January cutoff.
        frozen_at=second_slice.retrieved_at,
        focus_market_uid=MARKET_UID,
        focus_outcome_uid=YES_OUTCOME_UID,
        focus_event_time=min(fill.event_time for fill in unique),
        # The endpoint does not expose market-publication metadata.  Preserve
        # that clock as unmapped; the outer report separately records only an
        # existence-no-later-than bound from the earliest observed trade.
        focus_published_at=None,
        # The focus mapping was captured later and is explicitly carried as a
        # hindsight availability clock, never as a January live input.
        focus_available_at=head_deliveries[0].capture.received_at,
        analysis_mode="hindsight_reconstructed",
        availability_cutoff=second_slice.retrieved_at,
        nuisance_scales=(("same_market_scope", Decimal("1")),),
    )
    by_actor: dict[str, list[TradeFill]] = {}
    for fill in unique:
        if fill.actor_uid is not None:
            by_actor.setdefault(fill.actor_uid, []).append(fill)
    for fills in by_actor.values():
        fills.sort(key=lambda fill: (fill.event_time, fill.fill_uid))
    nuisance = tuple(
        NuisanceVector(
            actor_uid=actor,
            values=(("same_market_scope", Decimal("0")),),
            event_time=by_actor[actor][0].event_time,
            available_at=by_actor[actor][0].ingested_at,
            source_uid=by_actor[actor][0].source_uid,
            raw_artifact_uid=by_actor[actor][0].raw_artifact_uid,
        )
        for actor in sorted(by_actor)
    )
    report = rank_wallet_cohort(
        coverage=coverage,
        policy=policy,
        fills=unique,
        candidate_actor_uids=(CANDIDATE_ACTOR_UID,),
        nuisance_vectors=nuisance,
    )
    focus_buy = {
        actor: sum(
            (fill.price * fill.size for fill in fills if fill.outcome_uid == YES_OUTCOME_UID and fill.side == TradeSide.BUY),
            Decimal("0"),
        )
        for actor, fills in by_actor.items()
    }
    focus_buy = {actor: value for actor, value in focus_buy.items() if value > 0}
    gross = {actor: sum((fill.price * fill.size for fill in fills), Decimal("0")) for actor, fills in by_actor.items()}
    return WalletCaseCohortReplay(
        report=report,
        raw_input_count=len(all_fills),
        canonical_fill_count=len(unique),
        exact_duplicate_count=duplicates,
        conflict_count=conflicts,
        wallet_count=len(actors),
        focus_outcome_buy_notional_rank=_descriptive_rank(focus_buy, CANDIDATE_ACTOR_UID),
        gross_market_notional_rank=_descriptive_rank(gross, CANDIDATE_ACTOR_UID),
        source_deliveries=tuple(
            item.source_delivery_payload()
            for item in sorted(
                (*head_deliveries, *tail_deliveries),
                key=lambda item: (item.expected.interval_start, item.expected.offset),
            )
        ),
    )


def regenerate_wallet_case_cohort_replay(
    *, head_root: str | Path, tail_root: str | Path, output: str | Path
) -> WalletCaseCohortReplay:
    replay = build_wallet_case_cohort_replay(head_root, tail_root)
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(replay.canonical_bytes())
    return replay


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate the frozen Van Dyke same-market cohort replay.")
    parser.add_argument("--head-root", required=True)
    parser.add_argument("--tail-root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    regenerate_wallet_case_cohort_replay(head_root=args.head_root, tail_root=args.tail_root, output=args.output)
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(_main())
