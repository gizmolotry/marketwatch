import pandas as pd

from marketleak.agents.blockchain_agent import BlockchainAgent
from marketleak.main import main


WALLET_A = "0x1111111111111111111111111111111111111111"
WALLET_B = "0x2222222222222222222222222222222222222222"
TOKEN = "0x3333333333333333333333333333333333333333"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakePolygonSession:
    def get(self, url, params=None, timeout=None):
        action = params["action"]
        if action == "txlist":
            return FakeResponse(
                {
                    "status": "1",
                    "message": "OK",
                    "result": [
                        {
                            "blockNumber": "700",
                            "timeStamp": "1783200000",
                            "hash": "0xaaa",
                            "from": WALLET_A,
                            "to": WALLET_B,
                            "value": "1000000000000000000",
                            "methodId": "0x",
                            "functionName": "",
                            "isError": "0",
                        }
                    ],
                }
            )
        if action == "tokentx":
            return FakeResponse(
                {
                    "status": "1",
                    "message": "OK",
                    "result": [
                        {
                            "blockNumber": "701",
                            "timeStamp": "1783200010",
                            "hash": "0xbbb",
                            "from": WALLET_B,
                            "to": WALLET_A,
                            "contractAddress": TOKEN,
                            "value": "5000000",
                            "tokenName": "USD Coin",
                            "tokenSymbol": "USDC",
                            "tokenDecimal": "6",
                        }
                    ],
                }
            )
        raise AssertionError(f"Unexpected action: {action}")

    def post(self, url, json=None, timeout=None):
        if json["method"] == "eth_getBalance":
            return FakeResponse({"jsonrpc": "2.0", "id": json["id"], "result": "0xde0b6b3a7640000"})
        if json["method"] == "eth_getTransactionCount":
            return FakeResponse({"jsonrpc": "2.0", "id": json["id"], "result": "0x2"})
        raise AssertionError(f"Unexpected RPC method: {json['method']}")


class FakeRAG:
    def fetch_and_score(self, market_data):
        return {
            "evidence_text": "Synthetic public evidence for pipeline verification.",
            "evidence_url": "https://example.test/evidence",
            "evidence_date": "Sat, 04 Jul 2026 20:00:00 GMT",
            "shock_timestamp": market_data["shock_timestamp"],
            "news_timestamp": market_data["shock_timestamp"] + 3600,
            "lead_time_hours": 1.0,
            "shock_magnitude": market_data["shock_magnitude"],
            "ppim_score": 0.0,
        }


class FakeSynthesis:
    def generate_sar(self, alert_packet):
        raise AssertionError("Synthesis should not run when ppim_score is 0.0")


def test_pipeline_e2e_enriches_graph_with_polygon_blockchain_data(tmp_path):
    markets_path = tmp_path / "markets.parquet"
    ticks_path = tmp_path / "ticks.parquet"

    market_uid = "market-polygon-1"
    markets = pd.DataFrame(
        [
            {
                "market_uid": market_uid,
                "market_slug": "polygon-e2e-market",
                "question": "Will the Polygon E2E test market resolve yes?",
                "close_time": 1785794000.0,
                "wallet_address": WALLET_A,
            }
        ]
    )
    prices = [0.5] * 50 + [0.95]
    ticks = pd.DataFrame(
        [
            {
                "tick_uid": f"tick-{index}",
                "market_uid": market_uid,
                "timestamp": 1783200000 + index * 60,
                "price": price,
            }
            for index, price in enumerate(prices)
        ]
    )
    markets.to_parquet(markets_path, index=False)
    ticks.to_parquet(ticks_path, index=False)

    blockchain_agent = BlockchainAgent(
        api_key="test",
        session=FakePolygonSession(),
        tx_limit=2,
        raw_evidence_dir=tmp_path / "raw_evidence",
    )

    result = main(
        markets_path=str(markets_path),
        ticks_path=str(ticks_path),
        rag=FakeRAG(),
        synthesis=FakeSynthesis(),
        blockchain_agent=blockchain_agent,
        max_anomalies=1,
    )

    graph = result["graph_repo"].graph
    blockchain_result = result["blockchain_results"][0]

    assert blockchain_result.wallet_node_ids == [f"wallet:{WALLET_A.lower()}"]
    assert blockchain_result.normal_transaction_count == 1
    assert blockchain_result.token_transfer_count == 1
    assert blockchain_result.rpc_observation_count == 1
    assert f"wallet:{WALLET_A.lower()}" in graph.nodes
    assert f"wallet:{WALLET_B.lower()}" in graph.nodes
    assert "polygon_tx:0xaaa" in graph.nodes
    assert "polygon_tx:0xbbb" in graph.nodes
    assert f"polygon_token:{TOKEN.lower()}" in graph.nodes
    assert f"polygon_rpc_state:{WALLET_A.lower()}" in graph.nodes
    assert graph.nodes[f"polygon_token:{TOKEN.lower()}"]["label"] == "Token"
    assert graph.nodes[f"polygon_rpc_state:{WALLET_A.lower()}"]["label"] == "RPCState"

    edge_types = [data["type"] for _, _, data in graph.edges(data=True)]
    assert "ASSOCIATED_WITH_ANOMALY" in edge_types
    assert "TRANSFERRED_TO" in edge_types
    assert "TOKEN_TRANSFERRED_TO" in edge_types
    assert "HAS_RPC_STATE" in edge_types

    event_node = graph.nodes[blockchain_result.event_node_id]
    assert event_node["label"] == "Event"
