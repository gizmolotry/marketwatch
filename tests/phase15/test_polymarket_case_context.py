from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from marketleak.ingestion.connectors.http import HttpResponse
from marketleak.ingestion.connectors.polymarket_case_context import (
    PolymarketCaseContextCollector,
    PolymarketCaseContextStatus,
    PolymarketCaseContextTarget,
    load_polymarket_case_context_target,
    run_approved_polymarket_case_context,
)


T0 = datetime(2026, 7, 13, 12, tzinfo=UTC)
CONDITION = "0xc9c9790c8f26dd9c8cabae9dd76be37aa86a6ded7de660e1da9d19324cf618d4"
YES = "37863990088639017224129863896084706036599112986230542056774425461928248691792"
NO = "10222646434785930729270432914646282814690567121763865370487081241993787794954"
QUESTION = "Will Bitcoin reach $65,000 in July?"
RULE = "BTC/USDT final 1m candle High rule."


class FakeTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[str] = []

    def request(self, method: str, url: str, *, params, timeout: float) -> HttpResponse:
        assert method == "GET" and params is None and timeout == 10.0
        self.calls.append(url)
        return self.responses.pop(0)


def _response(payload: dict[str, object], url: str) -> HttpResponse:
    return HttpResponse(200, json.dumps(payload).encode("utf-8"), {}, url)


def _target() -> PolymarketCaseContextTarget:
    return PolymarketCaseContextTarget("2758340", CONDITION, QUESTION, RULE, YES, NO)


def _gamma(*, rule: str = RULE, source=None, include_resolution_source: bool = True, updated_at: datetime = T0 - timedelta(seconds=1)) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": "2758340",
        "conditionId": CONDITION,
        "question": QUESTION,
        "description": rule,
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps([YES, NO]),
        "endDate": (T0 + timedelta(days=1)).isoformat(),
        "updatedAt": updated_at.isoformat(),
    }
    if include_resolution_source:
        payload["resolutionSource"] = source
    return payload


def _clob(*, no_token: str = NO) -> dict[str, object]:
    return {"c": CONDITION, "t": [{"o": "Yes", "t": YES}, {"o": "No", "t": no_token}]}


def test_context_capture_preserves_null_resolution_source_and_verifies_raw_lineage(tmp_path: Path) -> None:
    transport = FakeTransport(
        [
            _response(_gamma(), "https://gamma-api.polymarket.com/markets/2758340"),
            _response(_clob(), f"https://clob.polymarket.com/clob-markets/{CONDITION}"),
        ]
    )

    result = PolymarketCaseContextCollector(transport=transport).collect(
        target=_target(), capture_root=tmp_path, as_of=T0, received_at=T0
    )

    assert result.status == PolymarketCaseContextStatus.COLLECTED
    assert result.document["market"]["resolution_source"] is None
    assert result.document["market"]["resolution_source_status"] == "null"
    assert result.document["market"]["question"] == QUESTION
    assert result.document["market"]["rule"] == RULE
    assert result.document["availability"]["available"] is True
    assert [entry["source_uid"] for entry in result.document["raw_lineage"]] == [
        "polymarket:source/gamma-market",
        "polymarket:source/clob-market-info",
    ]
    assert all((tmp_path / "raw" / "objects" / "sha256" / entry["sha256"][:2] / entry["sha256"][2:4] / f"{entry['sha256']}.raw").exists() for entry in result.document["raw_lineage"])
    assert transport.calls == [
        "https://gamma-api.polymarket.com/markets/2758340",
        f"https://clob.polymarket.com/clob-markets/{CONDITION}",
    ]


def test_context_capture_accepts_absent_resolution_source_and_records_distinction(tmp_path: Path) -> None:
    transport = FakeTransport(
        [
            _response(_gamma(include_resolution_source=False), "https://gamma-api.polymarket.com/markets/2758340"),
            _response(_clob(), f"https://clob.polymarket.com/clob-markets/{CONDITION}"),
        ]
    )

    result = PolymarketCaseContextCollector(transport=transport).collect(
        target=_target(), capture_root=tmp_path, as_of=T0, received_at=T0
    )

    assert result.status == PolymarketCaseContextStatus.COLLECTED
    assert result.document["market"]["resolution_source"] is None
    assert result.document["market"]["resolution_source_status"] == "absent"


def test_context_rule_or_token_mismatch_is_captured_but_rejected(tmp_path: Path) -> None:
    changed_rule = PolymarketCaseContextCollector(
        transport=FakeTransport([_response(_gamma(rule="changed"), "https://gamma-api.polymarket.com/markets/2758340")])
    ).collect(target=_target(), capture_root=tmp_path / "rule", as_of=T0, received_at=T0)
    assert changed_rule.status == PolymarketCaseContextStatus.REJECTED_MISMATCH
    assert changed_rule.document["availability"]["available"] is False
    assert len(changed_rule.raw_captures) == 1

    non_null_source = PolymarketCaseContextCollector(
        transport=FakeTransport([_response(_gamma(source="https://unexpected.example/rule"), "https://gamma-api.polymarket.com/markets/2758340")])
    ).collect(target=_target(), capture_root=tmp_path / "source", as_of=T0, received_at=T0)
    assert non_null_source.status == PolymarketCaseContextStatus.REJECTED_MISMATCH
    assert len(non_null_source.raw_captures) == 1

    changed_tokens = PolymarketCaseContextCollector(
        transport=FakeTransport(
            [
                _response(_gamma(), "https://gamma-api.polymarket.com/markets/2758340"),
                _response(_clob(no_token="999"), f"https://clob.polymarket.com/clob-markets/{CONDITION}"),
            ]
        )
    ).collect(target=_target(), capture_root=tmp_path / "token", as_of=T0, received_at=T0)
    assert changed_tokens.status == PolymarketCaseContextStatus.REJECTED_MISMATCH
    assert len(changed_tokens.raw_captures) == 2


def test_context_future_gamma_update_is_unavailable_not_admitted(tmp_path: Path) -> None:
    result = PolymarketCaseContextCollector(
        transport=FakeTransport([_response(_gamma(updated_at=T0 + timedelta(seconds=1)), "https://gamma-api.polymarket.com/markets/2758340")])
    ).collect(target=_target(), capture_root=tmp_path, as_of=T0, received_at=T0)

    assert result.status == PolymarketCaseContextStatus.UNAVAILABLE_LATE
    assert result.document["availability"]["available"] is False


def test_public_runner_uses_only_registry_target_and_sibling_config(tmp_path: Path) -> None:
    registry = Path(__file__).resolve().parents[2] / "configs" / "phase15" / "polymarket_btc65k_live.json"
    config = Path(__file__).resolve().parents[2] / "configs" / "phase15" / "polymarket_btc65k_case_context.json"
    target = load_polymarket_case_context_target(config)
    assert target.condition_id == CONDITION
    transport = FakeTransport(
        [
            _response(_gamma(rule=target.rule), "https://gamma-api.polymarket.com/markets/2758340"),
            _response(_clob(), f"https://clob.polymarket.com/clob-markets/{CONDITION}"),
        ]
    )

    result = run_approved_polymarket_case_context(registry, "market:polymarket-btc65k-july", tmp_path, transport=transport, now=T0)

    assert result["status"] == "collected"
    assert result["available"] is True
    assert Path(result["paths"]["case_context"]).is_file()
