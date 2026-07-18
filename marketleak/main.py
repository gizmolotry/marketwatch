import os
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from marketleak.agents.anomaly_agent import run_anomaly_detection
import json
import uuid
from dotenv import load_dotenv
from marketleak.models import AnomalyCandidate, AlertPacket, GraphEnrichment
from marketleak.leakrisk.model import LeakRiskModel
from marketleak.leakrisk.features import EventFeatures
from marketleak.graph.repository import GraphRepository
from marketleak.agents.blockchain_agent import BlockchainAgent

load_dotenv()

PIPELINE_COMPLETION_MESSAGE = (
    "\n[Done] Pipeline complete. Investigative review memos saved to 'reports/'"
)

def main(
    *,
    markets_path="demo_data/markets.parquet",
    ticks_path="demo_data/ticks.parquet",
    rag=None,
    synthesis=None,
    graph_repo=None,
    blockchain_agent=None,
    max_anomalies=10,
    legacy_demo=None,
    enable_legacy_sar=False,
):
    print("========================================")
    print("MarketLeak: Market Integrity System")
    print("========================================")
    
    explicit_legacy_components = any(
        component is not None for component in (rag, synthesis, graph_repo, blockchain_agent)
    )
    if legacy_demo is None:
        effective_legacy_demo = explicit_legacy_components or os.getenv(
            "MARKETLEAK_LEGACY_DEMO", ""
        ).strip().lower() in {"1", "true", "yes", "on"}
    else:
        effective_legacy_demo = bool(legacy_demo)

    print("\n[Phase 2] Running validation-first causal activity detection...")

    anomalies = run_anomaly_detection(
        markets_path=markets_path,
        ticks_path=ticks_path,
        legacy_demo=effective_legacy_demo,
    )
    if anomalies is None or anomalies.empty:
        print("No actionable activity incidents. This is a normal completed run.")
        return {
            "anomalies": anomalies if anomalies is not None else pd.DataFrame(),
            "final_results": [],
            "graph_repo": graph_repo,
            "blockchain_results": [],
            "v2_results": [],
            "run_status": {
                "status": "completed_no_actionable_activity",
                "legacy_demo": effective_legacy_demo,
                "not_proof_of_fraud": True,
                "effectiveness_unknown": True,
            },
        }
            
    # Take top anomalies for intelligence enrichment.
    top_anomalies = anomalies.head(max_anomalies)
    
    print("\n[Phase 3] Running Phase 2 Intelligence Integrations (Blockchain Graph + Leak Risk + RAG)...")
    if rag is None:
        from marketleak.agents.rag_agent import RAGAgent

        rag = RAGAgent()
    if synthesis is None:
        from marketleak.agents.synthesis_agent import SynthesisAgent

        synthesis = SynthesisAgent()
    
    # Initialize Phase 2 components
    leak_model = LeakRiskModel()
    graph_repo = graph_repo or GraphRepository()
    blockchain_agent = blockchain_agent or BlockchainAgent()
    
    final_results = []
    blockchain_results = []

    def enrich_anomaly(row):
        try:
            # 1. Create Anomaly Candidate
            market_uid = row.get('market_uid', row.get('market_slug', 'unknown'))
            cand = AnomalyCandidate(
                alert_uid=str(uuid.uuid4()),
                market_uid=market_uid,
                market_slug=row.get('market_slug', 'unknown'),
                question=row.get('question', ''),
                shock_timestamp=row.get('timestamp', 0.0),
                price=row.get('price', 0.0),
                logit_belief=row.get('logit_belief', 0.0),
                belief_shock=row.get('belief_shock', 0.0),
                rolling_mean=row.get('rolling_mean', 0.0),
                rolling_std=row.get('rolling_std', 0.0),
                z_score=row.get('z_score', 0.0),
                z_threshold=2.5
            )
            
            # 2. Leak Risk Prior
            event_uid = f"event:{cand.market_uid}"
            features = EventFeatures() # Default features for now
            leak_prior = leak_model.forecast_risk(cand.market_uid, event_uid, features)

            # 3. Blockchain Graph Enrichment
            blockchain_result = blockchain_agent.enrich_graph_for_anomaly(
                graph_repo,
                cand,
                row,
                event_uid=event_uid,
            )
            print(
                "[Blockchain] "
                f"{cand.market_slug}: wallets={len(blockchain_result.wallet_node_ids)}, "
                f"tx={blockchain_result.normal_transaction_count}, "
                f"token_transfers={blockchain_result.token_transfer_count}, "
                f"rpc_observations={blockchain_result.rpc_observation_count}"
            )
            if blockchain_result.errors:
                print(f"[Blockchain] Notes: {'; '.join(blockchain_result.errors[:3])}")
            
            # 4. Graph Enrichment traversal from suspicious Polygon wallets
            paths = []
            for wallet_node_id in blockchain_result.wallet_node_ids:
                paths.extend(graph_repo.find_evidence_paths(wallet_node_id, ["Event"]))
            paths.sort(key=lambda item: item[1], reverse=True)
            if paths:
                max_score = paths[0][1]
                enrichment = GraphEnrichment(
                    alert_uid=cand.alert_uid,
                    candidate_paths=paths,
                    max_path_score=max_score,
                    path_count=len(paths),
                    highest_evidence_band="B",
                    counter_evidence_count=0,
                    stale_edge_count=0
                )
            else:
                enrichment = None

            # 5. Convert row to dict for RAG
            market_data = {
                "question": cand.question,
                "slug": cand.market_slug,
                "shock_timestamp": cand.shock_timestamp,
                "close_time": row.get('close_time', 0.0),
                "shock_magnitude": cand.belief_shock
            }
            
            result = rag.fetch_and_score(market_data)
            if result:
                result['market_slug'] = cand.market_slug
                result['question'] = cand.question

                # 6. Build Alert Packet
                packet = AlertPacket(
                    alert_uid=cand.alert_uid,
                    anomaly_candidate=cand,
                    leak_risk_prior=leak_prior,
                    graph_enrichment=enrichment,
                    rag_result=result
                )
                
                # 7. Synthesis
                legacy_sar_env = os.getenv("MARKETLEAK_ENABLE_LEGACY_SAR", "").strip().lower() in {
                    "1", "true", "yes", "on"
                }
                if (enable_legacy_sar or legacy_sar_env) and result.get('ppim_score', 0) > 1.0:
                    synthesis.generate_sar(packet)

            return result, blockchain_result
        except Exception as e:
            market_id = row.get('market_uid', row.get('market_slug', 'unknown')) if hasattr(row, "get") else "unknown"
            print(f"[Warning] Skipping anomaly {market_id} after enrichment error: {e}")
            return None, None

    anomaly_rows = [row for _, row in top_anomalies.iterrows()]
    if anomaly_rows:
        with ThreadPoolExecutor(max_workers=min(32, len(anomaly_rows))) as executor:
            for result, blockchain_result in executor.map(enrich_anomaly, anomaly_rows):
                if blockchain_result is not None:
                    blockchain_results.append(blockchain_result)
                if result:
                    final_results.append(result)

    print("\n========================================")
    print("Final Information Lead (PPIM) Scores:")
    print("========================================")
    for res in final_results:
        print(f"Market: {res['market_slug']}")
        print(f"Question: {res['question']}")
        print(f"PPIM Score: {res['ppim_score']:.2f}")
        print("-" * 40)
        
    print(PIPELINE_COMPLETION_MESSAGE)
    return {
        "anomalies": anomalies,
        "final_results": final_results,
        "graph_repo": graph_repo,
        "blockchain_results": blockchain_results,
        "v2_results": (
            anomalies.replace({float("inf"): None, float("-inf"): None}).to_dict("records")
            if not effective_legacy_demo
            else []
        ),
        "run_status": {
            "status": "legacy_demo_completed" if effective_legacy_demo else "completed_with_review_candidates",
            "legacy_demo": effective_legacy_demo,
            "not_proof_of_fraud": True,
            "effectiveness_unknown": True,
        },
    }
        
if __name__ == "__main__":
    main()
