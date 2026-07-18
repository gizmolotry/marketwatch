from __future__ import annotations

import json
import math
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from marketleak.agents.anomaly_agent import run_anomaly_detection
from marketleak.agents.blockchain_agent import BlockchainAgent
from marketleak.agents.rag_agent import RAGAgent
from marketleak.agents.synthesis_agent import SynthesisAgent, verify_truth_safe_memo
from marketleak.graph.repository import GraphRepository
from marketleak.leakrisk.features import EventFeatures
from marketleak.leakrisk.model import LeakRiskModel
from marketleak.models import AlertPacket, AnomalyCandidate, GraphEnrichment
from marketleak.multimodal.bitcoin_policy import PublishedBitcoinContextRepository
from marketleak.multimodal.review_cases import FrozenReviewCaseRepository
from marketleak.multimodal.serving import ServingBundleRepository
from marketleak.scoring import query_market_ticks
from marketleak.pipeline_v2 import CAPABILITIES_V2, PipelineV2Result, run_validation_pipeline


REPORTS_DIR = Path("reports").resolve()
_global_rag_agent: RAGAgent | None = None
_global_leak_model: LeakRiskModel | None = None
_model_init_lock = threading.Lock()
_global_v2_result: PipelineV2Result | None = None
_v2_init_lock = threading.Lock()
_global_v3_repository: ServingBundleRepository | None = None
_v3_init_lock = threading.Lock()
_global_v3_bitcoin_context_repository: PublishedBitcoinContextRepository | None = None
_v3_bitcoin_context_init_lock = threading.Lock()
_global_v3_review_case_repository: FrozenReviewCaseRepository | None = None
_v3_review_case_init_lock = threading.Lock()

app = FastAPI(
    title="MarketLeak API",
    description="Validation-first market-surveillance API for human investigative review.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_rag_agent() -> RAGAgent:
    global _global_rag_agent
    if _global_rag_agent is None:
        with _model_init_lock:
            if _global_rag_agent is None:
                _global_rag_agent = RAGAgent()
    return _global_rag_agent


def _get_leak_model() -> LeakRiskModel:
    global _global_leak_model
    if _global_leak_model is None:
        with _model_init_lock:
            if _global_leak_model is None:
                _global_leak_model = LeakRiskModel()
    return _global_leak_model


def _prepare_pipeline_models() -> None:
    _get_leak_model()
    _get_rag_agent()


def get_v2_pipeline_result() -> PipelineV2Result:
    """FastAPI dependency for validation-first run state.

    Tests and deployments can replace this dependency without live network
    access. The default fixture result is cached because canonical inputs are
    immutable during an API process.
    """

    global _global_v2_result
    if _global_v2_result is None:
        with _v2_init_lock:
            if _global_v2_result is None:
                _global_v2_result = run_validation_pipeline()
    return _global_v2_result


def get_v3_serving_repository() -> ServingBundleRepository:
    """Read only the immutable v3 pointer/manifest surface on demand.

    No model artifact, retrieval index, external source, or legacy workflow is
    touched by this dependency. Deployments and tests may replace it with a
    preconstructed read-only repository.
    """

    global _global_v3_repository
    if _global_v3_repository is None:
        with _v3_init_lock:
            if _global_v3_repository is None:
                root = os.getenv("MARKETLEAK_V3_BUNDLE_ROOT", "data/serving-bundles")
                _global_v3_repository = ServingBundleRepository(root)
    return _global_v3_repository


def get_v3_bitcoin_context_repository() -> PublishedBitcoinContextRepository:
    """Read a configured published Bitcoin-context snapshot only.

    The dependency has no address input and never constructs a network client.
    A missing server-side configuration is a neutral, explicit response.
    """

    global _global_v3_bitcoin_context_repository
    if _global_v3_bitcoin_context_repository is None:
        with _v3_bitcoin_context_init_lock:
            if _global_v3_bitcoin_context_repository is None:
                configured_path = os.getenv("MARKETLEAK_V3_BITCOIN_CONTEXT_CONFIG")
                _global_v3_bitcoin_context_repository = PublishedBitcoinContextRepository(configured_path)
    return _global_v3_bitcoin_context_repository


def get_v3_review_case_repository() -> FrozenReviewCaseRepository:
    """Read only the explicitly configured frozen review-case packets.

    This dependency never initializes a collector, model, or market lookup.
    The absent-configuration path remains an explicit unavailable response.
    """

    global _global_v3_review_case_repository
    if _global_v3_review_case_repository is None:
        with _v3_review_case_init_lock:
            if _global_v3_review_case_repository is None:
                configured_path = os.getenv("MARKETLEAK_V3_REVIEW_CASES_CONFIG")
                _global_v3_review_case_repository = FrozenReviewCaseRepository(configured_path)
    return _global_v3_review_case_repository


def _v1_validation_additions(result: PipelineV2Result | None = None) -> dict[str, Any]:
    result = result or get_v2_pipeline_result()
    return {
        "assessment_v2": result.assessments_payload(),
        "data_quality": dict(result.data_quality),
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


class ChatRequest(BaseModel):
    prompt: str = Field(..., min_length=1)


def _records(df: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    """Convert a dataframe to JSON-safe records."""
    if limit is not None:
        df = df.tail(limit)
    cleaned = df.replace([math.inf, -math.inf], pd.NA)
    return json.loads(cleaned.to_json(orient="records", date_format="iso"))


def _load_feed() -> pd.DataFrame:
    try:
        return query_market_ticks()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Market tick feed unavailable: {exc}") from exc


def _load_anomalies() -> pd.DataFrame:
    try:
        anomalies = run_anomaly_detection()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Anomaly detection failed: {exc}") from exc
    if anomalies is None:
        return pd.DataFrame()
    return anomalies


def _row_to_market_payload(row: pd.Series) -> dict[str, Any]:
    return {
        "question": row.get("question", ""),
        "slug": row.get("market_slug", "unknown_market"),
        "market_slug": row.get("market_slug", "unknown_market"),
        "shock_timestamp": row.get("timestamp", 0),
        "close_time": row.get("close_time", 0),
        "shock_magnitude": row.get("belief_shock", 0),
    }


def _find_anomaly(market_slug: str) -> pd.Series:
    anomalies = _load_anomalies()
    if anomalies.empty or "market_slug" not in anomalies.columns:
        raise HTTPException(status_code=404, detail="No anomalies are currently available.")

    matches = anomalies[anomalies["market_slug"].astype(str) == market_slug]
    if matches.empty:
        raise HTTPException(status_code=404, detail=f"No anomaly found for market_slug '{market_slug}'.")
    return matches.iloc[0]


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _safe_str(value: Any, default: str = "") -> str:
    if _is_missing(value):
        return default
    text = str(value).strip()
    return text or default


def _safe_float(value: Any, default: float = 0.0) -> float:
    if _is_missing(value):
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isfinite(number):
        return number
    return default


def _evidence_unavailable_rag_result(
    market_payload: dict[str, Any],
    *,
    error: Exception | None = None,
) -> dict[str, Any]:
    """Return the stable, non-actionable API contract for absent RAG evidence."""
    return {
        "evidence_text": None,
        "evidence_url": None,
        "evidence_date": None,
        "shock_timestamp": _safe_float(market_payload.get("shock_timestamp")),
        "news_timestamp": 0.0,
        "lead_time_hours": 0.0,
        "shock_magnitude": _safe_float(market_payload.get("shock_magnitude")),
        "ppim_score": 0.0,
        "search_query": None,
        "search_strategy": "unavailable",
        "fallback_used": False,
        "evidence_scope": "unavailable",
        "timing_valid": False,
        "timing_issue": "evidence_unavailable",
        "ppim_suppression_reason": "evidence_unavailable",
        "evidence_error": str(error) if error is not None else None,
    }


def _candidate_from_row(row: pd.Series) -> AnomalyCandidate:
    market_slug = _safe_str(row.get("market_slug"), "unknown_market")
    market_uid = _safe_str(row.get("market_uid"), market_slug)
    return AnomalyCandidate(
        alert_uid=str(uuid.uuid4()),
        market_uid=market_uid,
        market_slug=market_slug,
        question=_safe_str(row.get("question"), market_slug),
        shock_timestamp=_safe_float(row.get("timestamp")),
        price=_safe_float(row.get("price")),
        logit_belief=_safe_float(row.get("logit_belief")),
        belief_shock=_safe_float(row.get("belief_shock")),
        rolling_mean=_safe_float(row.get("rolling_mean")),
        rolling_std=_safe_float(row.get("rolling_std")),
        z_score=_safe_float(row.get("z_score")),
        z_threshold=2.5,
    )


def _event_features_for_row(row: pd.Series) -> EventFeatures:
    return EventFeatures()


def _paths_to_graph_enrichment(
    alert_uid: str,
    paths: list[tuple[list[str], float]],
) -> GraphEnrichment | None:
    if not paths:
        return None

    return GraphEnrichment(
        alert_uid=alert_uid,
        candidate_paths=paths,
        max_path_score=float(paths[0][1]),
        path_count=len(paths),
        highest_evidence_band="B",
        counter_evidence_count=0,
        stale_edge_count=0,
    )


def _wallet_nodes_for_market(graph_repo: GraphRepository, market_uid: str) -> list[str]:
    with graph_repo._lock:
        return [
            node_id
            for node_id, data in graph_repo.graph.nodes(data=True)
            if data.get("label") == "Wallet" and data.get("market_uid") == market_uid
        ]


def _graph_node_label(node_id: str, data: dict[str, Any]) -> str:
    node_type = data.get("label")
    if node_type == "Wallet":
        address = _safe_str(data.get("address"), node_id.removeprefix("wallet:"))
        return f"Wallet {address[:8]}...{address[-4:]}" if len(address) > 14 else f"Wallet {address}"
    if node_type == "Transaction":
        tx_hash = _safe_str(data.get("hash"), node_id.removeprefix("polygon_tx:"))
        return f"Tx {tx_hash[:10]}..." if len(tx_hash) > 12 else f"Tx {tx_hash}"
    if node_type == "Event":
        return _safe_str(data.get("market_slug"), node_id)
    if node_type == "Token":
        return _safe_str(data.get("token_symbol"), node_id)
    if node_type == "RPCState":
        return "Polygon RPC state"
    if node_type == "Blockchain":
        return _safe_str(data.get("name"), node_id)
    return node_id


def _graph_data_from_paths(
    graph_repo: GraphRepository,
    paths: list[tuple[list[str], float]],
    wallet_node_ids: list[str],
    event_node_id: str,
) -> dict[str, list[dict[str, Any]]]:
    included_node_ids: set[str] = {event_node_id, *wallet_node_ids}
    path_edges = set()
    for path, _score in paths:
        included_node_ids.update(path)
        path_edges.update(zip(path, path[1:]))

    relevant_node_labels = {"Wallet", "Transaction", "Event", "Token", "RPCState", "Blockchain"}
    relevant_edge_types = {
        "ASSOCIATED_WITH_ANOMALY",
        "OBSERVED_ON_CHAIN",
        "HAS_RPC_STATE",
        "SENT_TRANSACTION",
        "RECEIVED_BY",
        "TRANSFERRED_TO",
        "SENT_TOKEN_TRANSFER",
        "TRANSFERS_TOKEN",
        "TOKEN_RECEIVED_BY",
        "TOKEN_TRANSFERRED_TO",
    }

    with graph_repo._lock:
        node_lookup = {node_id: dict(data) for node_id, data in graph_repo.graph.nodes(data=True)}
        edge_rows = [
            (source, target, dict(data))
            for source, target, data in graph_repo.graph.edges(data=True)
        ]

    selected_edges: list[tuple[str, str, dict[str, Any]]] = []
    selected_edge_keys: set[tuple[str, str, str]] = set()

    def add_edge(source: str, target: str, data: dict[str, Any]) -> None:
        edge_type = _safe_str(data.get("type"), "RELATED_TO")
        key = (source, target, edge_type)
        if key in selected_edge_keys:
            return
        selected_edge_keys.add(key)
        selected_edges.append((source, target, data))
        included_node_ids.add(source)
        included_node_ids.add(target)

    for _ in range(2):
        for source, target, data in edge_rows:
            source_label = node_lookup.get(source, {}).get("label")
            target_label = node_lookup.get(target, {}).get("label")
            edge_type = data.get("type")
            is_path_edge = (source, target) in path_edges
            is_relevant_edge = edge_type in relevant_edge_types
            is_relevant_node_edge = source_label in relevant_node_labels or target_label in relevant_node_labels
            if is_path_edge or (
                (source in included_node_ids or target in included_node_ids)
                and (is_relevant_edge or is_relevant_node_edge)
            ):
                add_edge(source, target, data)

    nodes = [
        {
            "id": node_id,
            "label": _graph_node_label(node_id, node_lookup.get(node_id, {})),
            "kind": _safe_str(node_lookup.get(node_id, {}).get("label"), "Unknown"),
        }
        for node_id in sorted(included_node_ids)
        if node_id in node_lookup
    ]
    links = [
        {
            "source": source,
            "target": target,
            "type": _safe_str(data.get("type"), "RELATED_TO"),
        }
        for source, target, data in selected_edges
        if source in included_node_ids and target in included_node_ids
    ]
    return {"nodes": nodes, "links": links}


def _run_pipeline_for_row(row: pd.Series) -> dict[str, Any]:
    market_payload = _row_to_market_payload(row)
    candidate = _candidate_from_row(row)
    event_uid = f"event:{candidate.market_uid}"

    event_features = _event_features_for_row(row)
    leak_prior = _get_leak_model().forecast_risk(candidate.market_uid, event_uid, event_features)

    graph_repo = GraphRepository()
    blockchain_agent = BlockchainAgent()
    blockchain_error: str | None = None
    blockchain_result = None
    paths: list[tuple[list[str], float]] = []

    try:
        blockchain_result = blockchain_agent.enrich_graph_for_anomaly(
            graph_repo,
            candidate,
            row,
            event_uid=event_uid,
        )
        for wallet_node_id in blockchain_result.wallet_node_ids:
            paths.extend(graph_repo.find_evidence_paths(wallet_node_id, ["Event"]))
        paths.sort(key=lambda item: item[1], reverse=True)
    except Exception as exc:
        blockchain_error = str(exc)

    wallet_node_ids = list(blockchain_result.wallet_node_ids) if blockchain_result is not None else []
    if not wallet_node_ids:
        wallet_node_ids = _wallet_nodes_for_market(graph_repo, candidate.market_uid)

    graph_enrichment = _paths_to_graph_enrichment(candidate.alert_uid, paths)

    try:
        rag_result = _get_rag_agent().fetch_and_score(market_payload)
    except Exception as exc:
        rag_result = _evidence_unavailable_rag_result(market_payload, error=exc)
    if rag_result is None:
        rag_result = _evidence_unavailable_rag_result(market_payload)

    rag_result["market_slug"] = candidate.market_slug
    rag_result["question"] = candidate.question

    packet = AlertPacket(
        alert_uid=candidate.alert_uid,
        anomaly_candidate=candidate,
        leak_risk_prior=leak_prior,
        graph_enrichment=graph_enrichment,
        rag_result=rag_result,
    )

    report_text: str | None = None
    report_path: str | None = None
    report_artifact: dict[str, Any] | None = None
    synthesis_error: str | None = None
    legacy_sar_enabled = os.getenv("MARKETLEAK_ENABLE_LEGACY_SAR", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if legacy_sar_enabled and _safe_float(rag_result.get("ppim_score")) > 1.0:
        try:
            generated_report, generated_path = SynthesisAgent().generate_sar(packet)
            report_view = read_report_for_product(Path(generated_path), generated_report)
            report_artifact = {
                key: value for key, value in report_view.items() if key != "content"
            }
            if report_view["truth_firewall_verified"] and report_view["content_available"]:
                report_text = report_view["content"]
                report_path = str(Path(generated_path))
            else:
                synthesis_error = "Generated memo was withheld by the v2 truth firewall."
        except Exception as _exc:
            withheld = read_report_for_product(Path("withheld.md"), "")
            report_artifact = {
                key: value for key, value in withheld.items() if key != "content"
            }
            synthesis_error = "Generated memo was unavailable or withheld by the v2 truth firewall."

    graph_data = _graph_data_from_paths(graph_repo, paths, wallet_node_ids, event_uid)

    return {
        "market_slug": candidate.market_slug,
        "question": candidate.question,
        "anomaly": _records(pd.DataFrame([row]))[0],
        "rag_result": rag_result,
        "lead_time_hours": rag_result.get("lead_time_hours"),
        "ppim_score": rag_result.get("ppim_score"),
        "ppim_deprecated": True,
        "leak_risk_forecast": float(leak_prior.score),
        "leak_risk_deprecated": True,
        "graph_data": graph_data,
        "graph_enrichment": graph_enrichment.model_dump() if graph_enrichment else None,
        "blockchain_summary": {
            "wallet_count": len(wallet_node_ids),
            "normal_transaction_count": (
                blockchain_result.normal_transaction_count if blockchain_result is not None else 0
            ),
            "token_transfer_count": blockchain_result.token_transfer_count if blockchain_result is not None else 0,
            "rpc_observation_count": blockchain_result.rpc_observation_count if blockchain_result is not None else 0,
            "errors": blockchain_result.errors if blockchain_result is not None else [],
        },
        "blockchain_error": blockchain_error,
        "report": report_text,
        "report_path": report_path,
        "report_artifact": report_artifact,
        "synthesis_error": synthesis_error,
        "assessment_v2": {
            "activity": {"status": "legacy_demo_unvalidated"},
            "public_explanation": {"status": "unknown_coverage"},
            "actor_evidence": {"status": "no_actor_data"},
            "not_proof_of_fraud": True,
            "effectiveness_unknown": True,
        },
        "not_proof_of_fraud": True,
    }


def _safe_report_path(filename: str) -> Path:
    if Path(filename).name != filename or not filename.endswith(".md"):
        raise HTTPException(status_code=400, detail="Report filename must be a markdown file name.")

    path = (REPORTS_DIR / filename).resolve()
    if path.parent != REPORTS_DIR:
        raise HTTPException(status_code=400, detail="Invalid report path.")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Report not found.")
    return path


_QUARANTINED_REPORT_PLACEHOLDER = (
    "# Artifact quarantined\n\n"
    "This legacy or unverified artifact is unavailable through MarketLeak product surfaces. "
    "Its contents did not pass the v2 provenance, integrity, and truth-firewall checks.\n"
)


def read_report_for_product(path: Path, content: str | None = None) -> dict[str, Any]:
    """Return only integrity-bound v2 memo content; quarantine everything else."""

    report_text = content if content is not None else path.read_text(encoding="utf-8", errors="replace")
    verified = verify_truth_safe_memo(report_text)
    if verified is None:
        return {
            "content": _QUARANTINED_REPORT_PLACEHOLDER,
            "content_available": False,
            "content_redacted": True,
            "artifact_status": "quarantined",
            "artifact_schema_version": None,
            "truth_firewall_verified": False,
            "legacy_demo_artifact": True,
            "evidence_classification": "quarantined_untrusted_artifact",
            "usable_as_validation_evidence": False,
            "quarantine_reason": "v2_artifact_verification_failed",
        }
    metadata, body = verified
    return {
        "content": body,
        "content_available": True,
        "content_redacted": False,
        "artifact_status": "truth_safe_investigative_memo",
        "artifact_schema_version": metadata.schema_version,
        "artifact_uid": metadata.artifact_uid,
        "source_uid": metadata.source_uid,
        "generator_version": metadata.generator_version,
        "generated_at": metadata.generated_at.isoformat(),
        "truth_firewall_verified": True,
        "legacy_demo_artifact": False,
        "evidence_classification": "investigative_memo_not_validation_evidence",
        "usable_as_validation_evidence": False,
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
        "human_review_required": True,
    }


def _report_metadata(path: Path, content: str | None = None) -> dict[str, Any]:
    view = read_report_for_product(path, content)
    view.pop("content", None)
    return view


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v2/capabilities")
def capabilities_v2() -> dict[str, Any]:
    return dict(CAPABILITIES_V2)


@app.get("/api/v2/data-quality")
def data_quality_v2(result: PipelineV2Result = Depends(get_v2_pipeline_result)) -> dict[str, Any]:
    return {
        "schema_version": "2.0.0",
        "run_uid": result.run_uid,
        "status": result.status,
        "data_quality": dict(result.data_quality),
        "actor_availability": dict(result.actor_availability),
        "source_availability": dict(result.source_availability),
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


@app.get("/api/v2/assessments")
def assessments_v2(result: PipelineV2Result = Depends(get_v2_pipeline_result)) -> dict[str, Any]:
    return {
        "schema_version": "2.0.0",
        "run_uid": result.run_uid,
        "run_status": result.status,
        "count": len(result.assessments),
        "assessments": result.assessments_payload(),
        "actor_availability": dict(result.actor_availability),
        "source_availability": dict(result.source_availability),
        "ppim_score": None,
        "ppim_deprecated": True,
        "leak_risk_forecast": None,
        "leak_risk_deprecated": True,
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


@app.get("/api/v2/validation-status")
@app.get("/api/v2/validation", include_in_schema=False)
def validation_status_v2(result: PipelineV2Result = Depends(get_v2_pipeline_result)) -> dict[str, Any]:
    return {
        "schema_version": "2.0.0",
        "run_uid": result.run_uid,
        "run_status": result.status,
        "validation": dict(result.validation_status),
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


def _v3_envelope(payload: dict[str, Any], repository: ServingBundleRepository) -> dict[str, Any]:
    """Attach the invariant boundary to every v3 response."""

    return {
        **repository.response_context(),
        **payload,
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


@app.get("/api/v3/readiness")
def readiness_v3(repository: ServingBundleRepository = Depends(get_v3_serving_repository)) -> dict[str, Any]:
    """Report immutable-bundle readiness without loading a model artifact."""

    return _v3_envelope(dict(repository.readiness_payload()), repository)


@app.get("/api/v3/model-status")
def model_status_v3(repository: ServingBundleRepository = Depends(get_v3_serving_repository)) -> dict[str, Any]:
    """Expose verified identifiers only; never model bytes or an unsafe artifact."""

    return _v3_envelope(dict(repository.model_status_payload()), repository)


@app.get("/api/v3/assessments")
def assessments_v3(repository: ServingBundleRepository = Depends(get_v3_serving_repository)) -> dict[str, Any]:
    """Return frozen routing/abstention records, never live inference."""

    return _v3_envelope(dict(repository.assessments_payload()), repository)


@app.get("/api/v3/review-cases")
def review_cases_v3(
    repository: ServingBundleRepository = Depends(get_v3_serving_repository),
    review_cases: FrozenReviewCaseRepository = Depends(get_v3_review_case_repository),
) -> dict[str, Any]:
    """Return only hash-checked, frozen market-mechanism review packets."""

    return _v3_envelope(dict(review_cases.payload(as_of=repository.as_of())), repository)


@app.get("/api/v3/review-cases/{case_uid}")
def review_case_v3(
    case_uid: str,
    repository: ServingBundleRepository = Depends(get_v3_serving_repository),
    review_cases: FrozenReviewCaseRepository = Depends(get_v3_review_case_repository),
) -> dict[str, Any]:
    """Return one exact frozen case; the path is not a market lookup."""

    return _v3_envelope(dict(review_cases.case_payload(case_uid, as_of=repository.as_of())), repository)


@app.get("/api/v3/retrieval/{event_uid}")
def retrieval_v3(
    event_uid: str,
    repository: ServingBundleRepository = Depends(get_v3_serving_repository),
) -> dict[str, Any]:
    """Return point-in-time, safe retrieval references only."""

    payload = repository.retrieval_payload(event_uid)
    if payload is None:
        state = repository.bundle_state()
        payload = {
            "status": "unavailable",
            "reason": state.reason or "bundle_unavailable",
            "event_uid": event_uid,
            "records": [],
        }
    return _v3_envelope(dict(payload), repository)


@app.get("/api/v3/events/{event_uid}")
def event_v3(
    event_uid: str,
    repository: ServingBundleRepository = Depends(get_v3_serving_repository),
) -> dict[str, Any]:
    """Return a safe event lineage summary only when available at the cutoff."""

    payload = repository.event_payload(event_uid)
    if payload is None:
        state = repository.bundle_state()
        return _v3_envelope(
            {
                "status": "unavailable",
                "reason": state.reason or "event_not_available_at_as_of",
                "event_uid": event_uid,
                "record": None,
            },
            repository,
        )
    return _v3_envelope(
        {
            "status": "available_metadata_only",
            "event_uid": event_uid,
            "record": payload,
        },
        repository,
    )


@app.get("/api/v3/bitcoin-context")
def bitcoin_context_v3(
    repository: ServingBundleRepository = Depends(get_v3_serving_repository),
    bitcoin_context: PublishedBitcoinContextRepository = Depends(get_v3_bitcoin_context_repository),
) -> dict[str, Any]:
    """Expose only configured redacted, precomputed context; never look up an address."""

    return _v3_envelope(dict(bitcoin_context.payload(as_of=repository.as_of())), repository)


import duckdb

@app.get("/api/overview")
def overview() -> dict[str, Any]:
    con = duckdb.connect(database=":memory:")
    try:
        from marketleak.scoring import resolve_tick_parquet_files
        ticks_files = resolve_tick_parquet_files()
        
        res = con.execute("SELECT count(*), max(timestamp) FROM read_parquet(?)", [ticks_files]).fetchone()
        data_points = int(res[0]) if res else 0
        latest_timestamp = int(res[1]) if res and res[1] is not None else None

        try:
            res_markets = con.execute("SELECT count(*) FROM read_parquet('demo_data/markets.parquet')").fetchone()
            tracked_markets = int(res_markets[0]) if res_markets else 0

            res_platforms = con.execute("SELECT DISTINCT platform FROM read_parquet('demo_data/markets.parquet')").fetchall()
            platforms = sorted(str(r[0]) for r in res_platforms)
        except Exception:
            tracked_markets = 0
            platforms = []

        metrics = {
            "data_points": data_points,
            "tracked_markets": tracked_markets,
            "platforms": platforms,
            "platform_count": len(platforms),
            "latest_timestamp": latest_timestamp,
        }

        hist_df = con.execute("SELECT CAST(timestamp/3600 AS BIGINT)*3600 AS timestamp, count(*) as count FROM read_parquet(?) GROUP BY 1 ORDER BY 1", [ticks_files]).df()
        feed_histogram = _records(hist_df)
        
        recent_df = con.execute("SELECT m.market_slug, m.question, t.timestamp, t.price FROM read_parquet(?) AS t LEFT JOIN read_parquet('demo_data/markets.parquet') AS m ON t.market_uid = m.market_uid ORDER BY t.timestamp DESC LIMIT 60", [ticks_files]).df()
        recent_feed = _records(recent_df)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Market tick feed unavailable: {exc}") from exc
    finally:
        con.close()

    return {
        "metrics": metrics,
        "feed_histogram": feed_histogram,
        "feed": recent_feed,
        **_v1_validation_additions(),
    }


@app.get("/api/anomalies")
def anomalies() -> dict[str, Any]:
    anomaly_df = _load_anomalies()
    return {
        "count": int(len(anomaly_df)),
        "anomalies": _records(anomaly_df),
        **_v1_validation_additions(),
    }


@app.post("/api/analyze/sweep")
def analyze_sweep() -> dict[str, Any]:
    top_anomalies = _load_anomalies().head(10)
    anomaly_rows = [row for _, row in top_anomalies.iterrows()]

    if anomaly_rows:
        _prepare_pipeline_models()
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(_run_pipeline_for_row, anomaly_rows))
    else:
        results = []

    return {"results": results, **_v1_validation_additions()}


@app.post("/api/analyze/{market_slug}")
def analyze_market(market_slug: str) -> dict[str, Any]:
    row = _find_anomaly(market_slug)
    return {**_run_pipeline_for_row(row), **_v1_validation_additions()}


@app.get("/api/reports")
def list_reports() -> dict[str, Any]:
    if not REPORTS_DIR.exists():
        return {"reports": []}

    reports = []
    for path in sorted(REPORTS_DIR.glob("*.md"), key=lambda item: item.stat().st_mtime, reverse=True):
        stat = path.stat()
        reports.append(
            {
                "filename": path.name,
                "size_bytes": stat.st_size,
                "modified_at": stat.st_mtime,
                **_report_metadata(path),
            }
        )
    return {"reports": reports}


@app.get("/api/reports/{filename}")
def get_report(filename: str) -> dict[str, Any]:
    path = _safe_report_path(filename)
    view = read_report_for_product(path)
    return {
        "filename": path.name,
        **view,
    }


@app.post("/api/chat")
def chat(request: ChatRequest) -> dict[str, Any]:
    prompt = request.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=422, detail="Prompt cannot be blank.")

    df = _load_feed()

    question_matches = pd.Series(False, index=df.index)
    slug_matches = pd.Series(False, index=df.index)
    if "question" in df.columns:
        question_matches = df["question"].astype(str).str.contains(prompt, case=False, na=False, regex=False)
    if "market_slug" in df.columns:
        slug_matches = df["market_slug"].astype(str).str.contains(prompt, case=False, na=False, regex=False)

    matches = df[question_matches | slug_matches]
    if matches.empty:
        return {
            "status": "not_found",
            "message": "No matching market found in the database.",
            "prompt": prompt,
            **_v1_validation_additions(),
        }

    target_market = str(matches["market_slug"].iloc[0])
    target_question = str(matches["question"].iloc[0]) if "question" in matches.columns else target_market
    anomalies_df = _load_anomalies()

    if anomalies_df.empty or "market_slug" not in anomalies_df.columns:
        anomaly_matches = pd.DataFrame()
    else:
        anomaly_matches = anomalies_df[anomalies_df["market_slug"].astype(str) == target_market]
    if anomaly_matches.empty:
        return {
            "status": "normal",
            "message": (
                "No validated activity signal is available; this is not evidence that trading was normal."
            ),
            "prompt": prompt,
            "market_slug": target_market,
            "question": target_question,
            "scorable": None,
            "limitations": {
                "scorable": "This compatibility route does not establish that adequate causal history was available.",
                "coverage": "Point-in-time public-information and actor coverage may be unavailable.",
            },
            **_v1_validation_additions(),
        }

    result = _run_pipeline_for_row(anomaly_matches.iloc[0])
    result["status"] = "analyzed"
    result["prompt"] = prompt
    result.update(_v1_validation_additions())
    return result
