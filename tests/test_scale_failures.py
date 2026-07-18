import time
import pytest
import networkx as nx
from marketleak.graph.repository import GraphRepository
from marketleak.agents.blockchain_agent import (
    BlockchainAgent,
    BlockchainEnrichmentResult,
    PolygonscanRateLimitError,
)
from marketleak.models import LeakRiskPrior
from types import SimpleNamespace
from tenacity import wait_none

# ---------------------------------------------------------
# Failure Mode 1: Graph Traversal Exponential Explosion
# ---------------------------------------------------------
def test_graph_scale_exponential_time():
    """
    FAILURE MODE: The GraphRepository uses nx.all_simple_paths to find evidence paths.
    In a highly connected on-chain graph (e.g., wash trading, DeFi liquidity pools, or bot networks),
    the number of paths explodes exponentially.
    At scale, max_depth=4 is enough to cause significant CPU lockup for a mere 100 nodes.
    """
    repo = GraphRepository()
    
    # Create a dense bipartite graph: 20 wallets sending to 20 smart contracts
    num_wallets = 20
    num_contracts = 20
    
    # Add nodes
    repo.upsert_node("event:market-1", "Event")
    for i in range(num_wallets):
        repo.upsert_node(f"wallet:{i}", "Wallet")
        # Connect each wallet to the event
        repo.upsert_edge(f"wallet:{i}", "event:market-1", "ASSOCIATED_WITH_ANOMALY")
        
    for j in range(num_contracts):
        repo.upsert_node(f"contract:{j}", "Wallet")
        
    # Fully connect wallets to contracts
    for i in range(num_wallets):
        for j in range(num_contracts):
            repo.upsert_edge(f"wallet:{i}", f"contract:{j}", "TRANSFERRED_TO")
            # And contracts back to wallets
            repo.upsert_edge(f"contract:{j}", f"wallet:{i}", "TRANSFERRED_TO")

    start_time = time.time()
    
    # Attempt to find evidence paths from wallet:0
    # nx.all_simple_paths has a cutoff of max_depth=4.
    # From wallet:0 -> contract:X -> wallet:Y -> contract:Z -> event:market-1
    paths = repo.find_evidence_paths("wallet:0", ["Event"], max_depth=4)
    duration = time.time() - start_time
    
    print(f"Graph traversal found {len(paths)} paths in {duration:.3f}s for {num_wallets+num_contracts} nodes.")
    assert duration < 0.5, "Graph traversal should be bounded and fast on dense graphs."
    assert len(paths) <= GraphRepository.MAX_EVIDENCE_PATHS_PER_TARGET
    assert all(len(path) - 1 <= 4 for path, _ in paths)
    assert any(path == ["wallet:0", "event:market-1"] for path, _ in paths)


# ---------------------------------------------------------
# Failure Mode 2: Polygon API Rate Limit Silent Data Loss
# ---------------------------------------------------------
class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class TransientRateLimitedSession:
    def __init__(self):
        self.action_attempts = {}

    def get(self, url, params=None, timeout=None):
        action = params["action"]
        self.action_attempts[action] = self.action_attempts.get(action, 0) + 1
        if action == "txlist" and self.action_attempts[action] == 1:
            return FakeResponse({
                "status": "0",
                "message": "Max rate limit reached",
                "result": None,
            })
        if action == "txlist":
            return FakeResponse({
                "status": "1",
                "message": "OK",
                "result": [{
                    "hash": "0xabc",
                    "from": "0x1234567890123456789012345678901234567890",
                    "to": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    "blockNumber": "1",
                    "timeStamp": "2",
                    "value": "3",
                }],
            })
        return FakeResponse({
            "status": "0",
            "message": "No transactions found",
            "result": [],
        })

    def post(self, url, json=None, timeout=None):
        return FakeResponse({"result": "0x0"})


class AlwaysRateLimitedSession(TransientRateLimitedSession):
    def get(self, url, params=None, timeout=None):
        action = params["action"]
        self.action_attempts[action] = self.action_attempts.get(action, 0) + 1
        return FakeResponse({
            "status": "0",
            "message": "Max rate limit reached",
            "result": None,
        })

def test_polygon_rate_limit_retries_without_data_loss():
    """
    FAILURE MODE: The BlockchainAgent calls the Polygonscan API sequentially.
    The free tier is 5 requests/sec. At scale, this is easily exceeded.
    When the rate limit is hit, the API returns {"status": "0", "message": "Max rate limit reached"}.
    The agent silently swallows this, records an error string, and returns ZERO transactions.
    This results in SILENT DATA LOSS where critical insider wallets are completely ignored in the graph.
    """
    repo = GraphRepository()
    session = TransientRateLimitedSession()
    agent = BlockchainAgent(
        api_key="test",
        session=session,
        polygonscan_retry_attempts=3,
        polygonscan_retry_wait=wait_none(),
    )
    
    candidate = SimpleNamespace(market_uid="market-1", market_slug="slug", question="?", shock_timestamp=0)
    row = {"market_uid": "market-1", "wallet_address": "0x1234567890123456789012345678901234567890"}
    
    result = agent.enrich_graph_for_anomaly(repo, candidate, row, event_uid="event:market-1")
    
    assert session.action_attempts["txlist"] == 2
    assert session.action_attempts["tokentx"] == 1
    assert result.normal_transaction_count == 1
    assert result.token_transfer_count == 0
    assert not any("rate limit" in error.lower() for error in result.errors)


def test_polygon_rate_limit_fails_loud_after_retries():
    repo = GraphRepository()
    session = AlwaysRateLimitedSession()
    agent = BlockchainAgent(
        api_key="test",
        session=session,
        polygonscan_retry_attempts=2,
        polygonscan_retry_wait=wait_none(),
    )

    candidate = SimpleNamespace(market_uid="market-1", market_slug="slug", question="?", shock_timestamp=0)
    row = {"market_uid": "market-1", "wallet_address": "0x1234567890123456789012345678901234567890"}

    with pytest.raises(PolygonscanRateLimitError, match="rate limited"):
        agent.enrich_graph_for_anomaly(repo, candidate, row, event_uid="event:market-1")

    assert session.action_attempts["txlist"] == 2


# ---------------------------------------------------------
# Failure Mode 3: Sequential Throughput Bottleneck
# ---------------------------------------------------------
class MockPipelineComponents:
    class MockLeakModel:
        def forecast_risk(self, *args, **kwargs):
            time.sleep(0.01)
            return LeakRiskPrior(
                market_uid="m1",
                event_uid="e1",
                score=0.5,
                score_version="test",
                feature_snapshot_uid="test",
                computed_at=time.time(),
                drivers={},
            )
    class MockBlockchainAgent:
        def enrich_graph_for_anomaly(self, *args, **kwargs):
            time.sleep(0.02) # Simulate Network IO
            return BlockchainEnrichmentResult(market_uid="m1", event_node_id="e1", wallet_node_ids=["w1"])
    class MockRAG:
        def fetch_and_score(self, *args, **kwargs):
            time.sleep(0.02) # Simulate Search IO
            return {"ppim_score": 0.5}
    class MockSynthesis:
        def generate_sar(self, *args, **kwargs):
            time.sleep(0.02) # Simulate LLM IO
            return None
    class MockGraph:
        def find_evidence_paths(self, *args, **kwargs):
            return [ (["a"], 1.0) ]
        def upsert_node(self, *args, **kwargs): pass
        def upsert_edge(self, *args, **kwargs): pass

def test_sequential_throughput_bottleneck(monkeypatch):
    """
    FAILURE MODE: `main.py` processes anomalies strictly sequentially using a `for` loop.
    For N anomalies, total execution time scales linearly O(N * IO_Time).
    If we scale to 10,000 alerts globally, the pipeline would take hours per batch.
    """
    import pandas as pd
    from marketleak.main import main
    import marketleak.main
    
    # Create 20 fake anomalies
    rows = [{"market_uid": f"m{i}", "market_slug": f"s{i}", "price": 0.5, "belief_shock": 2.0} for i in range(20)]
    df = pd.DataFrame(rows)
    
    # Mock anomaly detection to return our 20 rows
    monkeypatch.setattr("marketleak.main.run_anomaly_detection", lambda **kwargs: df)
    
    # Mock LeakRiskModel to use our fast mock
    monkeypatch.setattr("marketleak.main.LeakRiskModel", MockPipelineComponents.MockLeakModel)
    
    comps = MockPipelineComponents()
    
    start = time.time()
    output = main(
        markets_path="dummy",
        ticks_path="dummy",
        rag=comps.MockRAG(),
        synthesis=comps.MockSynthesis(),
        graph_repo=comps.MockGraph(),
        blockchain_agent=comps.MockBlockchainAgent(),
        max_anomalies=20
    )
    duration = time.time() - start
    
    # 20 anomalies * ~0.04s total IO wait time per anomaly = ~0.8 seconds minimum
    # If the pipeline used asyncio or ThreadPoolExecutor, 20 anomalies could be processed
    # in ~0.04s concurrently.
    print(f"Sequential processing of 20 anomalies took {duration:.2f}s")
    assert len(output["final_results"]) == 20
    assert len(output["blockchain_results"]) == 20
    assert {res["market_slug"] for res in output["final_results"]} == {f"s{i}" for i in range(20)}
    assert duration < 0.5, "The pipeline should process I/O-bound anomaly enrichment concurrently."
