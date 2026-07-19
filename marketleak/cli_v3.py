"""Phase 15 orchestration and explicitly bounded collection CLI.

The planning commands consume JSON only.  The Bitcoin context command is an
explicit exception: it reads a local watchlist, records raw public responses,
and writes its resulting snapshot below the requested local output directory.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from pydantic import ValidationError

from marketleak.ingestion.connectors.polymarket_ws_live import run_approved_polymarket_market_channel
from marketleak.ingestion.connectors.binance_btcusdt_kline import run_approved_binance_btcusdt_reference_collection
from marketleak.multimodal.event_store import EventMemoryStore
from marketleak.multimodal.orchestration import (
    BaselinePlan,
    FeatureAssemblyInput,
    assess_readiness,
    build_as_of_assembly,
    train_baseline_candidate,
)
from marketleak.multimodal.review_cases import FrozenReviewCaseRepository
from marketleak.multimodal.schemas import MarketStateSlice, OnChainSettlementFact, PublicDocumentClaim
from marketleak.multimodal.streaming import redacted_stream_request
from marketleak.onchain.bitcoin_context import collect_bitcoin_context_once


def _parse_event(value: Any):
    if not isinstance(value, dict):
        raise ValueError("events must contain JSON objects")
    modality = value.get("modality")
    types = {
        "market_state": MarketStateSlice,
        "public_evidence": PublicDocumentClaim,
        "onchain_settlement": OnChainSettlementFact,
    }
    model = types.get(modality)
    if model is None:
        raise ValueError("event modality is unsupported")
    return model.model_validate_json(json.dumps(value))


def _parse_json_input(path: str | Path):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("input must be a JSON object")
    if set(raw) - {"events", "features", "plan"}:
        raise ValueError("input contains unsupported top-level fields")
    events = raw.get("events", [])
    features = raw.get("features", [])
    if not isinstance(events, list) or not isinstance(features, list):
        raise ValueError("events and features must be JSON arrays")
    store = EventMemoryStore(_parse_event(item) for item in events)
    parsed_features = tuple(
        FeatureAssemblyInput.model_validate_json(json.dumps(item)) for item in features
    )
    plan_payload = raw.get("plan")
    plan = None if plan_payload is None else BaselinePlan.model_validate_json(json.dumps(plan_payload))
    return store, parsed_features, plan


def _summary(assembly) -> dict[str, Any]:
    return {
        "run_uid": assembly.run_uid,
        "as_of": assembly.as_of,
        "snapshot_uid": assembly.event_snapshot.manifest.snapshot_uid,
        "snapshot_record_count": len(assembly.event_snapshot.records),
        "feature_row_count": len(assembly.rows),
        "excluded": [item.model_dump(mode="json") for item in assembly.excluded],
        "input_hash": assembly.input_hash,
    }


def run_command(command: str, *, input_path: str | Path, as_of: str) -> dict[str, Any]:
    """Execute an in-memory request path; no external side effect is permitted."""

    from marketleak.multimodal.schemas import _utc
    from datetime import datetime

    cutoff = _utc(datetime.fromisoformat(as_of.replace("Z", "+00:00")), field_name="as_of")
    store, inputs, plan = _parse_json_input(input_path)
    assembly = build_as_of_assembly(store, inputs, as_of=cutoff)
    if command == "assemble":
        return {
            "command": command,
            "status": "assembled" if assembly.rows else "not_ready",
            "assembly": _summary(assembly),
        }
    readiness = assess_readiness(assembly, plan=plan)
    if command == "readiness":
        return {
            "command": command,
            "status": readiness.status,
            "assembly": _summary(assembly),
            "readiness": readiness.model_dump(mode="json"),
        }
    result = train_baseline_candidate(assembly, plan=plan)
    return {
        "command": command,
        "status": result.status,
        "assembly": _summary(assembly),
        "readiness": result.readiness.model_dump(mode="json"),
        "candidate": result.candidate.model_dump(mode="json"),
        "evaluation": [item.model_dump(mode="json") for item in result.evaluation],
    }


def run_stream_market_command(
    *,
    platform: str,
    polymarket_tokens: Sequence[str],
    kalshi_tickers: Sequence[str],
    kalshi_server_auth_configured: bool,
    max_messages: int,
    duration_seconds: float,
) -> dict[str, Any]:
    """Validate a bounded stream request without constructing a transport.

    Live transports are dependency-injected by server code, never by this
    command.  This makes the CLI a safe preflight surface: users get an
    explicit configuration result, while any server-only configuration locator
    and its values remain absent from stdout.
    """

    if platform not in {"polymarket", "kalshi"}:
        raise ValueError("platform is unsupported")
    if max_messages < 1 or max_messages > 10_000:
        raise ValueError("max_messages must be in [1, 10000]")
    if duration_seconds <= 0 or duration_seconds > 3_600:
        raise ValueError("duration_seconds must be in (0, 3600]")

    tokens = tuple(item.strip() for item in polymarket_tokens if item.strip())
    tickers = tuple(item.strip() for item in kalshi_tickers if item.strip())
    if platform == "polymarket" and not tokens:
        status = "unavailable_missing_market_tokens"
    elif platform == "kalshi" and not tickers:
        status = "unavailable_missing_market_tickers"
    elif platform == "kalshi" and not kalshi_server_auth_configured:
        # Fail before a socket factory could be selected or invoked.  The
        # configured server component is responsible for resolving auth data.
        status = "unavailable_authentication_required"
    else:
        status = "unavailable_transport_not_configured"
    return redacted_stream_request(
        platform=platform,  # type: ignore[arg-type]
        token_count=len(tokens),
        ticker_count=len(tickers),
        max_messages=max_messages,
        duration_seconds=duration_seconds,
        status=status,
    )


def run_bitcoin_context_collection_command(
    *,
    watchlist_path: str | Path,
    output_dir: str | Path,
    max_confirmed_pages: int,
    max_items_per_response: int,
    max_timeline_entries: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Run one local-watchlist, bounded collection and return no addresses."""

    snapshot = collect_bitcoin_context_once(
        watchlist_path=watchlist_path,
        output_dir=output_dir,
        max_confirmed_pages=max_confirmed_pages,
        max_items_per_response=max_items_per_response,
        max_timeline_entries=max_timeline_entries,
        timeout_seconds=timeout_seconds,
    )
    root = Path(output_dir)
    return {
        "command": "collect-bitcoin-context",
        "status": "collected",
        "snapshot_uid": snapshot.snapshot_uid,
        "registrations_count": len(snapshot.registrations),
        "summary_count": len(snapshot.summaries),
        "utxo_count": len(snapshot.utxos),
        "timeline_count": len(snapshot.timeline),
        "paths": {
            "snapshot": str(root / "bitcoin-context-snapshot.json"),
        },
    }


def run_polymarket_ws_collection_command(
    *,
    registry_path: str | Path,
    target_uid: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Run only the fixed registry-approved public Polymarket channel."""

    return run_approved_polymarket_market_channel(
        registry_path=registry_path,
        target_uid=target_uid,
        output_dir=output_dir,
    )


def run_binance_btcusdt_reference_collection_command(
    *,
    registry_path: str | Path,
    target_uid: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Collect only the fixed registry-approved Binance BTCUSDT candle feed."""

    return run_approved_binance_btcusdt_reference_collection(
        registry_path=registry_path,
        target_uid=target_uid,
        output_dir=output_dir,
    )


def run_polymarket_case_context_collection_command(
    *,
    registry_path: str | Path,
    target_uid: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Collect the fixed companion Polymarket case context on demand."""

    # Imported only when the command is used so Phase 15 planning commands do
    # not acquire a runtime dependency on the optional case-context collector.
    from marketleak.ingestion.connectors.polymarket_case_context import run_approved_polymarket_case_context

    return run_approved_polymarket_case_context(
        registry_path=registry_path,
        target_uid=target_uid,
        output_dir=output_dir,
    )


def run_review_case_command(
    *,
    config_path: str | Path,
    as_of: str,
    case_uid: str | None = None,
) -> dict[str, Any]:
    """Read one frozen review-case configuration without collection or inference."""

    from datetime import datetime

    from marketleak.multimodal.schemas import _utc

    cutoff = _utc(datetime.fromisoformat(as_of.replace("Z", "+00:00")), field_name="as_of")
    repository = FrozenReviewCaseRepository(config_path)
    packet = (
        repository.payload(as_of=cutoff)
        if case_uid is None
        else repository.case_payload(case_uid, as_of=cutoff)
    )
    return {
        "command": "review-case",
        "as_of": cutoff.isoformat().replace("+00:00", "Z"),
        **packet,
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketleak-v3", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("readiness", "assemble", "train-baseline"):
        command = commands.add_parser(name)
        command.add_argument("--input", required=True, help="Read-only Phase 15 JSON input")
        command.add_argument("--as-of", required=True, help="UTC ISO-8601 causal cutoff")
    review_case = commands.add_parser(
        "review-case",
        help="Read one hash-checked frozen review-case configuration",
    )
    review_case.add_argument("--config", required=True, help="Frozen review-case configuration JSON")
    review_case.add_argument("--as-of", required=True, help="UTC ISO-8601 causal cutoff")
    review_case.add_argument(
        "--case-uid",
        help="Optional exact case UID; omission returns all cases admitted by the cutoff",
    )
    stream = commands.add_parser(
        "stream-market",
        help="Validate a bounded, server-transport-injected market stream request",
    )
    stream.add_argument("--platform", required=True, choices=("polymarket", "kalshi"))
    stream.add_argument(
        "--polymarket-token",
        "--token",
        dest="polymarket_tokens",
        action="append",
        default=[],
        metavar="TOKEN",
        help="Explicit public Polymarket market token (repeatable)",
    )
    stream.add_argument(
        "--kalshi-ticker",
        "--ticker",
        dest="kalshi_tickers",
        action="append",
        default=[],
        metavar="TICKER",
        help="Explicit Kalshi market ticker (repeatable)",
    )
    stream.add_argument(
        "--kalshi-server-auth-config",
        dest="kalshi_server_auth_config",
        default=None,
        metavar="CONFIG_REF",
        help="Server-side configuration reference; its value is never displayed",
    )
    stream.add_argument("--max-messages", required=True, type=int)
    stream.add_argument("--duration-seconds", required=True, type=float)
    polymarket_ws = commands.add_parser(
        "collect-polymarket-ws",
        help="Collect one fixed bounded public Polymarket market-channel target",
    )
    polymarket_ws.add_argument("--registry", required=True, help="Static approved-source registry JSON")
    polymarket_ws.add_argument("--target-uid", required=True, help="Approved fixed target UID")
    polymarket_ws.add_argument("--output-dir", required=True, help="Local raw and normalized output directory")
    binance_reference = commands.add_parser(
        "collect-binance-btcusdt-reference",
        help="Collect one fixed bounded Binance Spot BTCUSDT closed-candle-high reference",
    )
    binance_reference.add_argument("--registry", required=True, help="Static approved-source registry JSON")
    binance_reference.add_argument("--target-uid", required=True, help="Approved fixed target UID")
    binance_reference.add_argument("--output-dir", required=True, help="Local raw, coverage, and manifest output directory")
    case_context = commands.add_parser(
        "collect-polymarket-case-context",
        help="Collect one fixed bounded Polymarket market-rule and metadata context",
    )
    case_context.add_argument("--registry", required=True, help="Static approved-source registry JSON")
    case_context.add_argument("--target-uid", required=True, help="Approved fixed target UID")
    case_context.add_argument("--output-dir", required=True, help="Local raw and context output directory")
    bitcoin = commands.add_parser(
        "collect-bitcoin-context",
        help="Collect a bounded public Bitcoin context from a local JSON watchlist",
        description=(
            "Read a local watchlist only. Expected JSON: "
            '{"registrations":[{"registration_uid":"bitcoin:watch/example","address":"<valid-mainnet-address>",'
            '"registered_at":"2026-07-13T12:00:00Z","network":"bitcoin-mainnet"}]}. '
            "UI or configuration examples with a placeholder address or link-policy fields are deliberately non-runnable."
        ),
    )
    bitcoin.add_argument(
        "--watchlist",
        required=True,
        help="Local JSON only: registrations with registration_uid, address, registered_at, and network",
    )
    bitcoin.add_argument("--output-dir", required=True, help="Local output directory for raw artifacts and snapshot")
    bitcoin.add_argument("--max-confirmed-pages", type=int, default=2)
    bitcoin.add_argument("--max-items-per-response", type=int, default=1_000)
    bitcoin.add_argument("--max-timeline-entries", type=int, default=500)
    bitcoin.add_argument("--timeout-seconds", type=float, default=15.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "stream-market":
            payload = run_stream_market_command(
                platform=args.platform,
                polymarket_tokens=args.polymarket_tokens,
                kalshi_tickers=args.kalshi_tickers,
                kalshi_server_auth_configured=bool(args.kalshi_server_auth_config),
                max_messages=args.max_messages,
                duration_seconds=args.duration_seconds,
            )
        elif args.command == "collect-bitcoin-context":
            payload = run_bitcoin_context_collection_command(
                watchlist_path=args.watchlist,
                output_dir=args.output_dir,
                max_confirmed_pages=args.max_confirmed_pages,
                max_items_per_response=args.max_items_per_response,
                max_timeline_entries=args.max_timeline_entries,
                timeout_seconds=args.timeout_seconds,
            )
        elif args.command == "collect-polymarket-ws":
            payload = run_polymarket_ws_collection_command(
                registry_path=args.registry,
                target_uid=args.target_uid,
                output_dir=args.output_dir,
            )
        elif args.command == "collect-binance-btcusdt-reference":
            payload = run_binance_btcusdt_reference_collection_command(
                registry_path=args.registry,
                target_uid=args.target_uid,
                output_dir=args.output_dir,
            )
        elif args.command == "collect-polymarket-case-context":
            payload = run_polymarket_case_context_collection_command(
                registry_path=args.registry,
                target_uid=args.target_uid,
                output_dir=args.output_dir,
            )
        elif args.command == "review-case":
            payload = run_review_case_command(
                config_path=args.config,
                as_of=args.as_of,
                case_uid=args.case_uid,
            )
        else:
            payload = run_command(args.command, input_path=args.input, as_of=args.as_of)
    except (OSError, ValueError, ValidationError, json.JSONDecodeError) as exc:
        payload = {
            "command": args.command,
            "status": "unavailable_invalid_input",
            "reason_code": type(exc).__name__,
        }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str))
    if args.command == "review-case" and payload.get("status") != "available_precomputed_review_cases":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
