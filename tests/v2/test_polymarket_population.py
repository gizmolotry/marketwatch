"""Mechanics fixtures for bounded, evidence-bound Polymarket acquisition only."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest

from marketleak.ingestion.connectors.http import EvidenceHttpClient, HttpResponse
from marketleak.ingestion.connectors.models import IngestionBatch
from marketleak.ingestion.connectors.polymarket import PolymarketConnector
from marketleak.ingestion.normalize import canonical_json_bytes
from marketleak.ingestion.polymarket_population import (
    PolymarketPopulationBackfill,
    PolymarketPopulationError,
    PolymarketPopulationEvidenceBound,
    PolymarketPopulationRequest,
)
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.ingestion.storage import NormalizedStore


CONDITION_ID = "0x" + "1a" * 32
START = datetime(2026, 1, 1, tzinfo=UTC)
OBSERVED = datetime(2026, 2, 1, tzinfo=UTC)
CONTRACT_URI = "https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets"


def _safe_request(origin: str, params: dict[str, object]) -> dict[str, object]:
    return {
        "method": "GET",
        "url": origin,
        "parameter_names": sorted(params),
        "redacted_parameter_names": [],
        "headers": {},
        "secrets_redacted": False,
        "attempt": 1,
        "public_parameters": dict(sorted(params.items())),
    }


def _bound(tmp_path: Path, *, contract_at=OBSERVED, metadata_at=OBSERVED + timedelta(seconds=1)):
    store = RawArtifactStore(tmp_path / "bound-raw")
    contract_body = b"operator-reviewed Polymarket trades API contract snapshot"
    contract = store.capture(
        contract_body,
        platform="polymarket",
        source="official-docs/data-api-trades",
        request=_safe_request("https://docs.polymarket.com", {}),
        received_at=contract_at,
        response_metadata={
            "status_code": 200,
            "url": "https://docs.polymarket.com",
            "headers": {},
            "secrets_redacted": False,
        },
    )
    metadata_body = canonical_json_bytes(
        {
            "id": "123",
            "conditionId": CONDITION_ID,
            "acceptingOrdersTimestamp": START,
        }
    )
    metadata_url = "https://gamma-api.polymarket.com/markets/123"
    metadata = store.capture(
        metadata_body,
        platform="polymarket",
        source="polymarket:source/gamma-market-clock",
        request={"method": "GET", "url": metadata_url, "params": {}},
        received_at=metadata_at,
        response_metadata={
            "status_code": 200,
            "url": metadata_url,
            "effective_url_matches_request": True,
            "headers": {},
        },
    )
    bound = PolymarketPopulationEvidenceBound.from_capture(
        condition_id=CONDITION_ID,
        gamma_market_id="123",
        official_contract_version="observed-2026-02-01",
        contract_capture=contract,
        market_metadata_capture=metadata,
    )
    return bound, contract, metadata


def _request(tmp_path: Path, start=0, end=0, **changes) -> PolymarketPopulationRequest:
    bound, _, _ = _bound(tmp_path)
    values = {
        "condition_id": CONDITION_ID,
        "interval_start": START + timedelta(seconds=start),
        "interval_end": START + timedelta(seconds=end),
        "source_bound": bound,
        "page_size": 1_000,
    }
    values.update(changes)
    return PolymarketPopulationRequest(**values)


class ScriptedConnector:
    MAX_TRADE_OFFSET = 10_000

    def __init__(self, root: Path, script, *, supports_side=True):
        self.store = RawArtifactStore(root / "trade-raw")
        self.script = script
        self.calls = []
        self.supports_side = supports_side

    def iter_trade_pages(self, *, side=None):  # capability signature only
        raise AssertionError("not called")

    def fetch_trades(self, **kwargs):
        if not self.supports_side and kwargs.get("side") is not None:
            raise TypeError("side unsupported")
        self.calls.append(dict(kwargs))
        start, end, side = kwargs["start"], kwargs["end"], kwargs.get("side")
        state, names = self.script((int(start.timestamp()), int(end.timestamp()), side))
        offset = 0
        if kwargs.get("continuation"):
            offset = json.loads(kwargs["continuation"])["offset"]
        params = {
            "limit": kwargs["page_size"],
            "offset": offset,
            "market": kwargs["market"],
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
            "takerOnly": kwargs["taker_only"],
        }
        if side is not None:
            params["side"] = side
        # Minimal mechanics rows follow the documented real /trades shape and
        # are normalized by the production parser.  They are fixtures only,
        # never effectiveness evidence.
        rows = [
            {
                "proxyWallet": "0x1111111111111111111111111111111111111111",
                "side": side or "BUY",
                "asset": "yes",
                "conditionId": CONDITION_ID,
                "size": "2",
                "price": "0.4",
                "timestamp": int(start.timestamp()),
                "outcomeIndex": 0,
                "transactionHash": f"0x{name}",
            }
            for name in names
        ]
        body = canonical_json_bytes(rows)
        capture = self.store.capture(
            body,
            platform="polymarket",
            source="data-api/trades",
            request=_safe_request("https://data-api.polymarket.com", params),
            received_at=OBSERVED + timedelta(minutes=1, seconds=len(self.calls)),
            response_metadata={
                "status_code": 200,
                "url": "https://data-api.polymarket.com",
                "headers": {},
                "secrets_redacted": False,
            },
        )
        artifact = RawArtifactStore.to_domain(
            capture,
            source_uid="polymarket:source/data-api-trades",
            parser_version=PolymarketConnector.PARSER_VERSION,
        )
        fills = [PolymarketConnector.normalize_trade(row, capture) for row in rows]
        continuation = None
        complete = state == "complete"
        if not complete:
            continuation = json.dumps(
                {
                    "filters": {},
                    "offset": offset + len(names),
                    "state": state,
                    "version": 1,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        return IngestionBatch(
            fills=fills,
            raw_artifacts=[artifact],
            raw_captures=[capture],
            continuation=continuation,
            complete=complete,
        )


def _collector(tmp_path, connector, request):
    return PolymarketPopulationBackfill(
        connector,
        NormalizedStore(tmp_path / "normalized"),
        approved_contract_sha256=frozenset(
            {request.source_bound.official_contract_sha256}
        ),
    )


def test_recursive_boundaries_receipts_and_parent_reconciliation(tmp_path):
    root = int(START.timestamp())

    def script(key):
        start, end, _ = key
        if (start, end) == (root, root + 4):
            return "split_required", ("shared",)
        if start == root:
            return "complete", ("shared",)
        return "complete", ("right",)

    request = _request(tmp_path, 0, 4)
    connector = ScriptedConnector(tmp_path, script)
    result = _collector(tmp_path, connector, request).collect(request)

    assert [(int(c["start"].timestamp()), int(c["end"].timestamp())) for c in connector.calls] == [
        (root, root + 4), (root, root + 2), (root + 3, root + 4)
    ]
    assert all(c["taker_only"] is False for c in connector.calls)
    assert result.manifest.query_exhausted is True
    assert result.manifest.complete is False
    assert result.manifest.coverage_status == "query_exhausted_coverage_limited"
    assert result.manifest.source_consistent is True
    assert result.manifest.logical_request_count == 3
    assert result.manifest.http_attempt_count == 3
    assert len(result.manifest.receipt_ids) == 3
    assert result.manifest.raw_record_count == 3
    assert result.manifest.duplicate_record_count == 1


def test_one_second_buy_sell_partition_and_source_inconsistency(tmp_path):
    def consistent(key):
        side = key[2]
        if side is None:
            return "split_required", ("buy",)
        return "complete", (("buy",) if side == "BUY" else ("sell",))

    request = _request(tmp_path / "ok")
    result = _collector(
        tmp_path / "ok", ScriptedConnector(tmp_path / "ok", consistent), request
    ).collect(request)
    assert [leaf.side for leaf in result.leaves] == [None, "BUY", "SELL"]
    assert result.manifest.source_consistent is True

    def inconsistent(key):
        return ("split_required", ("missing",)) if key[2] is None else ("complete", ())

    bad_request = _request(tmp_path / "bad")
    bad = _collector(
        tmp_path / "bad", ScriptedConnector(tmp_path / "bad", inconsistent), bad_request
    ).collect(bad_request)
    assert bad.manifest.source_consistent is False
    assert bad.manifest.coverage_status == "partial"
    assert "source_inconsistent" in bad.manifest.limitation_reasons


def test_irreducible_side_saturation_is_partial(tmp_path):
    def script(key):
        side = key[2]
        if side is None:
            return "split_required", ("buy",)
        return ("split_required", ("buy",)) if side == "BUY" else ("complete", ())

    request = _request(tmp_path)
    result = _collector(tmp_path, ScriptedConnector(tmp_path, script), request).collect(request)
    assert result.manifest.coverage_status == "partial"
    assert "one_second_side_partition_exhausted_at_offset_ceiling" in result.manifest.limitation_reasons


def test_no_side_capability_records_irreducibly_partial(tmp_path):
    class NoSideConnector:
        MAX_TRADE_OFFSET = 10_000
        def __init__(self, inner): self.inner = inner; self.calls = inner.calls
        def fetch_trades(
            self, *, market, start, end, taker_only, page_size, max_pages, continuation
        ):
            return self.inner.fetch_trades(
                market=market, start=start, end=end, taker_only=taker_only,
                page_size=page_size, max_pages=max_pages, continuation=continuation,
            )

    request = _request(tmp_path)
    inner = ScriptedConnector(tmp_path, lambda _key: ("split_required", ()))
    result = _collector(tmp_path, NoSideConnector(inner), request).collect(request)
    assert len(inner.calls) == 1
    assert result.leaves[0].status == "irreducibly_partial"
    assert "one_second_side_partition_unavailable" in result.manifest.limitation_reasons


def test_request_budget_preserves_first_partial_page(tmp_path):
    request = _request(tmp_path, max_requests=1)
    connector = ScriptedConnector(tmp_path, lambda _key: ("page", ("first",)))
    result = _collector(tmp_path, connector, request).collect(request)
    leaf = result.leaves[0]
    assert len(connector.calls) == 1
    assert leaf.status == "budget_exhausted"
    assert leaf.raw_record_count == 1 and len(leaf.receipts) == 1
    assert result.manifest.budget_exhausted is True
    assert "budget_exhausted" in result.manifest.limitation_reasons


def test_leaf_budget_stops_recursive_growth(tmp_path):
    request = _request(tmp_path, 0, 8, max_leaves=1)
    connector = ScriptedConnector(tmp_path, lambda _key: ("split_required", ()))
    result = _collector(tmp_path, connector, request).collect(request)
    assert len(connector.calls) == 1
    assert any(item.status == "budget_exhausted" for item in result.leaves)


def test_market_activity_lower_bound_is_derived_from_gamma_and_distinct_clocks(tmp_path):
    bound, _, _ = _bound(
        tmp_path,
        contract_at=OBSERVED,
        metadata_at=OBSERVED + timedelta(hours=2),
    )
    assert bound.market_activity_lower_bound == START
    assert bound.observed_at == OBSERVED + timedelta(hours=2)
    assert bound.market_activity_lower_bound_field == "acceptingOrdersTimestamp"


def test_request_rejects_epoch_zero_pre_floor_and_post_observation(tmp_path):
    bound, _, _ = _bound(tmp_path)
    with pytest.raises(ValueError, match="market activity lower bound"):
        PolymarketPopulationRequest(
            CONDITION_ID, datetime.fromtimestamp(0, tz=UTC), START, bound
        )
    with pytest.raises(ValueError, match="market activity lower bound"):
        PolymarketPopulationRequest(
            CONDITION_ID, START - timedelta(seconds=1), START, bound
        )
    with pytest.raises(ValueError, match="source-contract observation"):
        PolymarketPopulationRequest(
            CONDITION_ID, START, bound.observed_at + timedelta(seconds=1), bound
        )


@pytest.mark.parametrize("field", ["conditionId", "acceptingOrdersTimestamp", "id"])
def test_tampered_gamma_body_or_identity_fails(tmp_path, field):
    bound, _, metadata = _bound(tmp_path)
    payload = json.loads(metadata.object_path.read_bytes())
    payload[field] = {
        "conditionId": "0x" + "2" * 64,
        "acceptingOrdersTimestamp": "not-a-time",
        "id": "999",
    }[field]
    metadata.object_path.write_bytes(canonical_json_bytes(payload))
    request = PolymarketPopulationRequest(CONDITION_ID, START, START, bound)
    connector = ScriptedConnector(tmp_path, lambda _key: ("complete", ()))
    with pytest.raises(PolymarketPopulationError):
        _collector(tmp_path, connector, request).collect(request)


def test_contract_policy_tamper_non_2xx_and_unapproved_hash_fail(tmp_path):
    bound, contract, _ = _bound(tmp_path)
    request = PolymarketPopulationRequest(CONDITION_ID, START, START, bound)
    connector = ScriptedConnector(tmp_path, lambda _key: ("complete", ()))
    with pytest.raises(PolymarketPopulationError, match="operator-approved"):
        PolymarketPopulationBackfill(
            connector,
            NormalizedStore(tmp_path / "unapproved"),
            approved_contract_sha256=frozenset({"f" * 64}),
        ).collect(request)
    receipt = json.loads(contract.receipt_path.read_bytes())
    receipt["response_metadata"]["status_code"] = 500
    contract.receipt_path.write_bytes(canonical_json_bytes(receipt))
    with pytest.raises(PolymarketPopulationError):
        _collector(tmp_path, connector, request).collect(request)


def test_receipt_or_raw_object_tamper_and_exact_query_mismatch_fail(tmp_path):
    request = _request(tmp_path)

    class TamperConnector(ScriptedConnector):
        def fetch_trades(self, **kwargs):
            batch = super().fetch_trades(**kwargs)
            receipt = json.loads(batch.raw_captures[0].receipt_path.read_bytes())
            receipt["request"]["public_parameters"]["takerOnly"] = True
            batch.raw_captures[0].receipt_path.write_bytes(canonical_json_bytes(receipt))
            return batch

    with pytest.raises(PolymarketPopulationError, match="exact public request"):
        _collector(
            tmp_path, TamperConnector(tmp_path, lambda _key: ("complete", ())), request
        ).collect(request)


@pytest.mark.parametrize(
    "substitution",
    [
        {"parser_version": "substituted-parser"},
        {"source_uid": "polymarket:source/substituted"},
        {"ingested_at": OBSERVED + timedelta(days=1)},
        {"actor_uid": "polymarket:wallet/0x2222222222222222222222222222222222222222"},
    ],
)
def test_duplicate_semantics_and_connector_substitution_fail_closed(
    tmp_path, substitution
):
    request = _request(tmp_path)
    duplicate = ScriptedConnector(tmp_path, lambda _key: ("complete", ("same", "same")))
    result = _collector(tmp_path, duplicate, request).collect(request)
    assert (result.manifest.raw_record_count, result.manifest.canonical_record_count) == (2, 1)

    class Substitution(ScriptedConnector):
        def fetch_trades(self, **kwargs):
            batch = super().fetch_trades(**kwargs)
            batch.fills[1] = batch.fills[1].model_copy(update=substitution)
            return batch

    bad_request = _request(tmp_path / "conflict")
    with pytest.raises(PolymarketPopulationError, match="exactly equal"):
        _collector(
            tmp_path / "conflict",
            Substitution(
                tmp_path / "conflict", lambda _key: ("complete", ("same", "same"))
            ),
            bad_request,
        ).collect(bad_request)


def test_real_connector_kwargs_side_fallback_and_receipts(tmp_path):
    class CeilingZero(PolymarketConnector):
        MAX_TRADE_OFFSET = 0

    class Transport:
        def __init__(self): self.calls = []
        def request(self, method, url, *, params, timeout, max_response_bytes=None, approved_addresses=None):
            query = dict(params or {}); self.calls.append(query)
            payload = [] if query.get("side") else [{
                "proxyWallet": "0x1111111111111111111111111111111111111111",
                "side": "BUY", "asset": "yes", "conditionId": CONDITION_ID,
                "size": 2, "price": 0.4, "timestamp": int(START.timestamp()),
                "outcomeIndex": 0, "transactionHash": "0xabc",
            }]
            return HttpResponse(200, canonical_json_bytes(payload), {}, url, peer_address=approved_addresses[0])

    transport = Transport()
    connector = CeilingZero(EvidenceHttpClient(
        RawArtifactStore(tmp_path / "real-raw"), transport=transport,
        resolver=lambda _host, _port: ("93.184.216.34",),
    ))
    request = _request(tmp_path, page_size=1)
    result = _collector(tmp_path, connector, request).collect(request)
    assert [call.get("side") for call in transport.calls] == [None, "BUY", "SELL"]
    assert result.manifest.logical_request_count == 3
    assert result.manifest.http_attempt_count == 3


def test_retry_attempts_are_all_bound_and_total_http_budget_is_hard(tmp_path):
    trade = {
        "proxyWallet": "0x1111111111111111111111111111111111111111",
        "side": "BUY",
        "asset": "yes",
        "conditionId": CONDITION_ID,
        "size": "2",
        "price": "0.4",
        "timestamp": int(START.timestamp()),
        "outcomeIndex": 0,
        "transactionHash": "0xretry",
    }

    class RetryingTransport:
        def __init__(self, responses):
            self.responses = list(responses)
            self.calls = 0

        def request(
            self, method, url, *, params, timeout, max_response_bytes=None,
            approved_addresses=None,
        ):
            self.calls += 1
            status, body = self.responses.pop(0)
            return HttpResponse(
                status,
                body,
                {"Retry-After": "0"},
                url,
                peer_address=approved_addresses[0],
            )

    same_body = canonical_json_bytes([trade])
    transport = RetryingTransport([(429, same_body), (200, same_body)])
    connector = PolymarketConnector(
        EvidenceHttpClient(
            RawArtifactStore(tmp_path / "retry-raw"),
            transport=transport,
            max_attempts=4,
            sleep=lambda _delay: None,
            resolver=lambda _host, _port: ("93.184.216.34",),
        )
    )
    request = _request(tmp_path / "success", max_http_attempts=2)
    result = _collector(tmp_path / "success", connector, request).collect(request)
    assert transport.calls == 2
    assert result.manifest.logical_request_count == 1
    assert result.manifest.http_attempt_count == 2
    assert len(result.raw_captures) == 2
    assert len(result.manifest.raw_sha256) == 1
    assert len(result.manifest.receipt_ids) == 2
    assert [binding.status_code for binding in result.leaves[0].receipts] == [429, 200]
    assert [binding.attempt for binding in result.leaves[0].receipts] == [1, 2]

    capped_transport = RetryingTransport(
        [(429, b"first"), (429, b"second"), (200, canonical_json_bytes([trade]))]
    )
    capped_connector = PolymarketConnector(
        EvidenceHttpClient(
            RawArtifactStore(tmp_path / "capped-raw"),
            transport=capped_transport,
            max_attempts=4,
            sleep=lambda _delay: None,
            resolver=lambda _host, _port: ("93.184.216.34",),
        )
    )
    capped_request = _request(tmp_path / "capped", max_http_attempts=2)
    with pytest.raises(Exception, match="HTTP 429"):
        _collector(tmp_path / "capped", capped_connector, capped_request).collect(
            capped_request
        )
    assert capped_transport.calls == 2
    assert len(list((tmp_path / "capped-raw" / "receipts").rglob("*.json"))) == 2


def test_request_is_immutable_and_condition_exact(tmp_path):
    request = _request(tmp_path)
    with pytest.raises(FrozenInstanceError):
        request.condition_id = "x"  # type: ignore[misc]
    for condition in ("foo", "0x" + "1" * 63, "0x" + "g" * 64, [CONDITION_ID]):
        with pytest.raises(ValueError, match="condition_id must be one exact"):
            PolymarketPopulationRequest(condition, START, START, request.source_bound)
