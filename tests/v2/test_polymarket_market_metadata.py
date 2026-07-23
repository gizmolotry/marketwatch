"""Mechanics fixtures for raw-first Gamma metadata clocks, not effectiveness evidence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import inspect
import json

import pytest

from marketleak.ingestion.connectors.http import HttpResponse
from marketleak.ingestion.connectors.polymarket_market import (
    POLYMARKET_GAMMA_API,
    PolymarketGammaMarketMetadataCollector,
    PolymarketMarketMetadataStatus,
    PolymarketGammaMarketTarget,
)
from marketleak.ingestion.raw_store import RawArtifactStore


NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)
CONDITION_ID = "0x" + "ab" * 32
OFFICIAL_URL = f"{POLYMARKET_GAMMA_API}/markets/123"


class RecordingTransport:
    def __init__(self, response: HttpResponse | Exception):
        self.response = response
        self.calls: list[tuple[str, str, dict[str, object], float]] = []

    def request(self, method: str, url: str, *, params, timeout: float) -> HttpResponse:
        self.calls.append((method, url, dict(params or {}), timeout))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _body(**overrides):
    payload = {
        "id": "123",
        "conditionId": CONDITION_ID,
        "createdAt": "2026-07-18T12:00:00Z",
        "startDate": "2026-07-18T13:00:00Z",
        "updatedAt": "2026-07-19T11:00:00Z",
        "readyTimestamp": "2026-07-18T14:00:00Z",
        "fundedTimestamp": "2026-07-18T15:00:00Z",
        "acceptingOrdersTimestamp": "2026-07-18T16:00:00Z",
        "events": [{"published_at": "2026-07-18T11:00:00Z"}],
    }
    payload.update(overrides)
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _collector(tmp_path, response: HttpResponse | Exception):
    transport = RecordingTransport(response)
    return PolymarketGammaMarketMetadataCollector(
        raw_store=RawArtifactStore(tmp_path / "raw"), transport=transport
    ), transport


def _http(body: bytes, *, status: int = 200, url: str = OFFICIAL_URL, headers=None) -> HttpResponse:
    return HttpResponse(status, body, headers or {}, url)


def _collect(collector):
    return collector.collect(
        target=PolymarketGammaMarketTarget("123", CONDITION_ID),
        retrieved_at=NOW - timedelta(seconds=1),
        ingested_at=NOW,
    )


def test_collects_one_raw_lineaged_gamma_market_and_keeps_publication_unmapped(tmp_path):
    body = _body()
    collector, transport = _collector(
        tmp_path,
        _http(body, headers={"Date": "x", "Authorization": "secret", "X-Request-ID": "abc"}),
    )

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.COLLECTED_PUBLICATION_UNMAPPED
    assert result.record is not None and result.raw_capture is not None
    record = result.record
    assert transport.calls == [("GET", f"{POLYMARKET_GAMMA_API}/markets/123", {}, 10.0)]
    assert record.raw_sha256 == hashlib.sha256(body).hexdigest()
    assert record.raw_artifact_uid == f"polymarket:raw/{record.raw_sha256}"
    assert record.raw_byte_length == len(body)
    assert hashlib.sha256(result.raw_capture.receipt_path.read_bytes()).hexdigest() == record.receipt_sha256
    assert record.query_hash == hashlib.sha256(
        b'{"method":"GET","params":{},"url":"https://gamma-api.polymarket.com/markets/123"}'
    ).hexdigest()
    receipt = json.loads(result.raw_capture.receipt_path.read_text(encoding="utf-8"))
    assert receipt["response_metadata"]["headers"] == {"date": "x", "x-request-id": "abc"}
    assert "secret" not in result.raw_capture.receipt_path.read_text(encoding="utf-8").lower()
    assert record.created_at.raw_value == "2026-07-18T12:00:00Z"
    assert record.event_published_at.raw_value == "2026-07-18T11:00:00Z"
    assert record.event_published_at.parsed_at == datetime(2026, 7, 18, 11, tzinfo=UTC)
    assert record.public_visibility_at is None
    assert record.public_visibility_authority == record.public_visibility_status == "unmapped"
    assert record.first_observed_at == record.retrieved_at < record.ingested_at


def test_missing_optional_source_times_stay_none(tmp_path):
    body = _body(**{field: None for field in (
        "createdAt", "startDate", "updatedAt", "readyTimestamp", "fundedTimestamp", "acceptingOrdersTimestamp", "events"
    )})
    collector, _ = _collector(tmp_path, _http(body))

    result = _collect(collector)

    assert result.record is not None
    assert all(
        item.raw_value is None and item.parsed_at is None
        for item in (
            result.record.created_at, result.record.start_date, result.record.updated_at,
            result.record.ready_timestamp, result.record.funded_timestamp,
            result.record.accepting_orders_timestamp, result.record.event_published_at,
        )
    )


def test_raw_is_captured_before_malformed_body_is_rejected(tmp_path):
    collector, _ = _collector(tmp_path, _http(b"{"))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.UNAVAILABLE_MALFORMED_BODY
    assert result.raw_capture is not None
    assert result.raw_capture.object_path.read_bytes() == b"{"
    assert result.raw_capture.receipt_path.exists()


def test_http_error_is_captured_before_failure(tmp_path):
    collector, _ = _collector(tmp_path, _http(_body(), status=429))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.UNAVAILABLE_HTTP
    assert result.raw_capture is not None


def test_wrong_market_or_condition_is_rejected_after_capture(tmp_path):
    collector, _ = _collector(tmp_path, _http(_body(id="124")))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.REJECTED_MISMATCH
    assert result.raw_capture is not None


@pytest.mark.parametrize("market_id", [123, True])
def test_payload_market_id_requires_exact_numeric_json_string(tmp_path, market_id):
    collector, _ = _collector(tmp_path, _http(_body(id=market_id)))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.REJECTED_MISMATCH
    assert result.raw_capture is not None


@pytest.mark.parametrize("market_id", [123, True])
def test_target_market_id_requires_exact_numeric_string(market_id):
    with pytest.raises(ValueError, match="numeric Gamma market ID string"):
        PolymarketGammaMarketTarget(market_id, CONDITION_ID)


def test_malformed_and_future_source_timestamps_fail_closed(tmp_path):
    malformed, _ = _collector(tmp_path / "malformed", _http(_body(updatedAt="not-a-time")))
    future, _ = _collector(tmp_path / "future", _http(_body(updatedAt="2026-07-20T00:00:00Z")))

    assert _collect(malformed).status == PolymarketMarketMetadataStatus.REJECTED_SOURCE_TIMESTAMP
    assert _collect(future).status == PolymarketMarketMetadataStatus.REJECTED_SOURCE_TIMESTAMP


@pytest.mark.parametrize("value", [123, True, [], {}])
def test_non_string_source_timestamps_fail_closed(tmp_path, value):
    collector, _ = _collector(tmp_path, _http(_body(createdAt=value)))

    assert _collect(collector).status == PolymarketMarketMetadataStatus.REJECTED_SOURCE_TIMESTAMP


def test_future_start_date_is_preserved_without_becoming_public_visibility(tmp_path):
    future_start = "2026-07-20T00:00:00Z"
    collector, _ = _collector(tmp_path, _http(_body(startDate=future_start)))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.COLLECTED_PUBLICATION_UNMAPPED
    assert result.record is not None
    assert result.record.start_date.raw_value == future_start
    assert result.record.start_date.parsed_at == datetime(2026, 7, 20, tzinfo=UTC)
    assert result.record.public_visibility_at is None


def test_event_publication_is_never_promoted_and_multiple_events_are_rejected(tmp_path):
    single, _ = _collector(tmp_path / "single", _http(_body()))
    ambiguous, _ = _collector(
        tmp_path / "ambiguous",
        _http(_body(events=[{"published_at": "2026-07-18T11:00:00Z"}, {"published_at": None}])),
    )

    result = _collect(single)
    assert result.record is not None and result.record.public_visibility_at is None
    assert _collect(ambiguous).status == PolymarketMarketMetadataStatus.REJECTED_AMBIGUOUS_EVENT


def test_nonmonotone_clocks_fail_before_network_io(tmp_path):
    collector, transport = _collector(tmp_path, _http(_body()))

    result = collector.collect(
        target=PolymarketGammaMarketTarget("123", CONDITION_ID),
        retrieved_at=NOW,
        ingested_at=NOW - timedelta(seconds=1),
    )

    assert result.status == PolymarketMarketMetadataStatus.REJECTED_NONMONOTONE_CLOCK
    assert not transport.calls


def test_first_observation_cannot_be_backdated_through_collect_api():
    parameters = inspect.signature(PolymarketGammaMarketMetadataCollector.collect).parameters

    assert "first_observed_at" not in parameters


@pytest.mark.parametrize(
    "effective_url",
    [
        "https://attacker.test/markets/123?token=secret",
        f"{POLYMARKET_GAMMA_API}/markets/124",
        f"{OFFICIAL_URL}?token=secret",
    ],
)
def test_nonexact_effective_url_is_captured_then_rejected_without_persisting_secret(tmp_path, effective_url):
    collector, _ = _collector(tmp_path, _http(_body(), url=effective_url))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.REJECTED_EFFECTIVE_URL
    assert result.raw_capture is not None
    receipt_text = result.raw_capture.receipt_path.read_text(encoding="utf-8")
    receipt = json.loads(receipt_text)
    assert receipt["response_metadata"]["effective_url_matches_request"] is False
    assert receipt["response_metadata"]["url"] == "<redacted-nonmatching-url>"
    assert "attacker" not in receipt_text and "token" not in receipt_text and "secret" not in receipt_text


def test_redirect_status_fails_closed_after_capture(tmp_path):
    collector, _ = _collector(tmp_path, _http(_body(), status=302))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.UNAVAILABLE_HTTP
    assert result.raw_capture is not None


@pytest.mark.parametrize(
    "condition_id",
    [123, True, "0xabc", "0x" + "gg" * 32],
)
def test_payload_condition_id_requires_exact_json_string_format(tmp_path, condition_id):
    collector, _ = _collector(tmp_path, _http(_body(conditionId=condition_id)))

    assert _collect(collector).status == PolymarketMarketMetadataStatus.REJECTED_MISMATCH


@pytest.mark.parametrize("condition_id", [123, True, "0xabc", "0x" + "gg" * 32])
def test_target_condition_id_requires_exact_string_format(condition_id):
    with pytest.raises(ValueError, match="64-hex-character"):
        PolymarketGammaMarketTarget("123", condition_id)


def test_condition_id_is_compared_and_stored_canonically(tmp_path):
    uppercase = "0x" + "AB" * 32
    collector, _ = _collector(tmp_path, _http(_body(conditionId=uppercase)))

    result = collector.collect(
        target=PolymarketGammaMarketTarget("123", uppercase),
        retrieved_at=NOW - timedelta(seconds=1),
        ingested_at=NOW,
    )

    assert result.record is not None
    assert result.record.condition_id == CONDITION_ID


def test_transport_exception_is_one_bounded_call_without_capture(tmp_path):
    collector, transport = _collector(tmp_path, RuntimeError("offline"))

    result = _collect(collector)

    assert result.status == PolymarketMarketMetadataStatus.UNAVAILABLE_HTTP
    assert result.raw_capture is None
    assert transport.calls == [("GET", OFFICIAL_URL, {}, 10.0)]


def test_construction_is_network_free_and_request_is_deterministic(tmp_path):
    response = _http(_body())
    collector, transport = _collector(tmp_path, response)

    assert not transport.calls
    first = _collect(collector)
    second = _collect(collector)

    assert first.record is not None and second.record is not None
    assert first.record.query_hash == second.record.query_hash
    assert first.record.endpoint_url == second.record.endpoint_url
    assert len(transport.calls) == 2
