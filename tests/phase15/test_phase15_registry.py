from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from marketleak.ingestion.config.phase15_registry import (
    Phase15SourceRegistry,
    load_phase15_source_registry,
)


def valid_registry_payload() -> dict[str, object]:
    return {
        "registry_uid": "registry:phase15-approved-v1",
        "schema_version": "phase15-source-registry-v1",
        "targets": [
            {
                "target_uid": "target:polymarket-window-01",
                "venue": "polymarket",
                "market_uid": "polymarket:condition/0xabc123",
                "polymarket_asset_ids": ["12345678901234567890", "22345678901234567890"],
                "metadata_ref": "config:polymarket-market-metadata-v1",
                "stream_config_ref": "config:polymarket-public-market-stream-v1",
                "stream_settings": {"max_messages": 1000, "duration_seconds": 60.0, "max_reconnects": 2},
                "reference_mapping_uid": "mapping:polymarket-window-btc-usd",
            },
            {
                "target_uid": "target:kalshi-window-01",
                "venue": "kalshi",
                "market_uid": "kalshi:KXBTC-26JUL13-B100000",
                "kalshi_tickers": ["KXBTC-26JUL13-B100000"],
                "metadata_ref": "config:kalshi-market-metadata-v1",
                "stream_config_ref": "config:kalshi-signed-market-stream-v1",
                "stream_settings": {"max_messages": 250, "duration_seconds": 30.0, "max_reconnects": 1},
                "reference_mapping_uid": None,
            },
        ],
        "documented_btc_reference_mappings": [
            {
                "mapping_uid": "mapping:polymarket-window-btc-usd",
                "target_uid": "target:polymarket-window-01",
                "market_rule_source_uid": "source:polymarket-market-rules",
                "market_rule_source_url": "https://docs.polymarket.com/market-rules/window-01",
                "settlement_source_uid": "source:polymarket-documented-benchmark",
                "settlement_source_url": "https://docs.polymarket.com/market-rules/window-01/benchmark",
                "primary_source_uid": "source:coinbase-exchange-btc-usd",
                "primary_source_url": "https://docs.cdp.coinbase.com/exchange/websocket-feed/overview",
                "instrument": "BTC-USD",
                "configured_endpoint_ref": "config:coinbase-exchange-btc-usd-public-feed-v1",
            }
        ],
    }


def parse(payload: dict[str, object]) -> Phase15SourceRegistry:
    return Phase15SourceRegistry.model_validate_json(json.dumps(payload))


def test_registry_accepts_only_fixed_venue_targets_and_documented_one_to_one_btc_mappings(tmp_path: Path) -> None:
    path = tmp_path / "approved-sources.json"
    path.write_text(json.dumps(valid_registry_payload()), encoding="utf-8")

    registry = load_phase15_source_registry(path)

    polymarket = registry.target("target:polymarket-window-01")
    assert polymarket.venue == "polymarket"
    assert polymarket.polymarket_asset_ids == ("12345678901234567890", "22345678901234567890")
    assert registry.reference_mapping("mapping:polymarket-window-btc-usd").instrument == "BTC-USD"
    with pytest.raises(KeyError, match="not approved"):
        registry.target("target:unconfigured")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload: payload["targets"][0].update(  # type: ignore[index,union-attr]
                {"polymarket_asset_ids": None, "kalshi_tickers": ["KXBTC-26JUL13-B100000"]}
            ),
            "polymarket target",
        ),
        (
            lambda payload: payload["targets"][1].update(  # type: ignore[index,union-attr]
                {"kalshi_tickers": ["KXBTC-26JUL13-B100000"], "polymarket_asset_ids": ["12345678", "22345678"]}
            ),
            "kalshi target",
        ),
        (
            lambda payload: payload["targets"][0]["stream_settings"].update({"duration_seconds": 3601.0}),  # type: ignore[index,union-attr]
            "less than or equal",
        ),
        (
            lambda payload: payload["documented_btc_reference_mappings"][0].update(  # type: ignore[index,union-attr]
                {"primary_source_url": "http://not-https.invalid/source"}
            ),
            "HTTPS",
        ),
        (
            lambda payload: payload["documented_btc_reference_mappings"][0].update(  # type: ignore[index,union-attr]
                {"instrument": "ETH-USD"}
            ),
            "BTC-USD",
        ),
    ],
)
def test_registry_rejects_wrong_venue_selectors_unbounded_streams_urls_and_symbols(mutate, message: str) -> None:
    payload = valid_registry_payload()
    mutate(payload)

    with pytest.raises(ValidationError, match=message):
        parse(payload)


def test_registry_fails_closed_on_generic_source_placeholders_unknown_secret_and_mapping_ambiguity() -> None:
    generic = valid_registry_payload()
    generic["documented_btc_reference_mappings"][0]["primary_source_uid"] = "source:generic-btc-usd"  # type: ignore[index]
    with pytest.raises(ValidationError, match="generic fallback"):
        parse(generic)

    unknown = valid_registry_payload()
    unknown["targets"][0]["api_key"] = "never-allowed"  # type: ignore[index]
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        parse(unknown)

    ambiguous = valid_registry_payload()
    duplicate = dict(ambiguous["documented_btc_reference_mappings"][0])  # type: ignore[index]
    duplicate["mapping_uid"] = "mapping:second-mapping"
    ambiguous["documented_btc_reference_mappings"].append(duplicate)  # type: ignore[index]
    with pytest.raises(ValidationError, match="one-to-one"):
        parse(ambiguous)

    mismatched = valid_registry_payload()
    mismatched["targets"][0]["reference_mapping_uid"] = "mapping:not-the-configured-one"  # type: ignore[index]
    with pytest.raises(ValidationError, match="exactly match"):
        parse(mismatched)


def test_loader_is_json_only_and_intentionally_nonrunnable_example_is_rejected(tmp_path: Path) -> None:
    non_json = tmp_path / "registry.yaml"
    non_json.write_text("targets: []", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON document"):
        load_phase15_source_registry(non_json)

    root = Path(__file__).resolve().parents[2]
    with pytest.raises(ValidationError):
        load_phase15_source_registry(root / "configs" / "phase15" / "approved_sources.example.json")
