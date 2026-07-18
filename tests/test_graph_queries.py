import pytest
from marketleak.graph.repository import GraphRepository

def test_graph_upsert_and_query():
    repo = GraphRepository()
    
    # Upsert nodes
    repo.upsert_node("wallet_1", "Wallet")
    repo.upsert_node("discord_1", "Handle")
    repo.upsert_node("event_1", "Event")
    
    # Upsert edges
    repo.upsert_edge("wallet_1", "discord_1", "CLAIMS_CONTROL_OF")
    repo.upsert_edge("discord_1", "event_1", "HAS_PLAUSIBLE_ACCESS_TO")
    
    # Test path finding
    paths = repo.find_evidence_paths("wallet_1", ["Event"])
    
    assert len(paths) == 1
    path, score = paths[0]
    assert path == ["wallet_1", "discord_1", "event_1"]
    assert score == 1.0 / 3.0

def test_hub_penalty():
    repo = GraphRepository()
    
    # Create a hub node (degree > 10)
    repo.upsert_node("start_1", "Wallet")
    repo.upsert_node("hub", "Organization")
    repo.upsert_node("event_1", "Event")
    
    repo.upsert_edge("start_1", "hub", "MEMBER")
    repo.upsert_edge("hub", "event_1", "INVOLVED")
    
    # Add many dummy connections to hub
    for i in range(15):
        repo.upsert_node(f"dummy_{i}", "Wallet")
        repo.upsert_edge(f"dummy_{i}", "hub", "MEMBER")
        
    paths = repo.find_evidence_paths("start_1", ["Event"])
    assert len(paths) == 1
    path, score = paths[0]
    
    # Path length is 3 (start -> hub -> event)
    # Hub degree is 1 (from start) + 1 (to event) + 15 (dummy) = 17
    # Hub penalty = (17 - 10) * 0.1 = 0.7
    # Score should be 1.0 / (3 + 0.7) = 1.0 / 3.7 = 0.27027...
    assert path == ["start_1", "hub", "event_1"]
    assert score < (1.0 / 3.0)  # Should be lower than unpenalized score
