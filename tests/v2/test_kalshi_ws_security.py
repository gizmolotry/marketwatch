from __future__ import annotations

from collections.abc import Iterator, Mapping

import pytest

from marketleak.ingestion.connectors.kalshi_ws import (
    KALSHI_WEBSOCKET_URL,
    KalshiWebSocketConfig,
    KalshiWebSocketConfigurationError,
)


class _CredentialsThatMustNotBeRead(Mapping[str, str]):
    """A mechanics fixture proving endpoint validation precedes secret access."""

    def __getitem__(self, key: str) -> str:
        raise AssertionError("credentials were read before endpoint validation")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("credentials were read before endpoint validation")

    def __len__(self) -> int:
        raise AssertionError("credentials were read before endpoint validation")


@pytest.mark.parametrize(
    "websocket_url",
    (
        "ws://external-api-ws.kalshi.com/trade-api/ws/v2",
        "wss://external-api-ws.kalshi.com.evil.test/trade-api/ws/v2",
        "wss://attacker@external-api-ws.kalshi.com/trade-api/ws/v2",
        "wss://external-api-ws.kalshi.com/trade-api/ws/v2?redirect=evil",
        "wss://external-api-ws.kalshi.com/trade-api/ws/v2#evil",
        "wss://external-api-ws.kalshi.com/other",
    ),
)
def test_kalshi_rejects_unapproved_endpoint_before_reading_credentials(websocket_url):
    with pytest.raises(KalshiWebSocketConfigurationError, match="approved production endpoint"):
        KalshiWebSocketConfig(
            market_tickers=("FIXTURE-TICKER",),
            websocket_url=websocket_url,
            auth_headers=_CredentialsThatMustNotBeRead(),
        )


def test_kalshi_accepts_exact_documented_production_endpoint():
    config = KalshiWebSocketConfig(
        market_tickers=("FIXTURE-TICKER",),
        websocket_url=KALSHI_WEBSOCKET_URL,
        auth_headers={},
    )

    assert config.websocket_url == KALSHI_WEBSOCKET_URL
