import duckdb
import pandas as pd
from typing import List, Dict, Any
from marketleak.graph.repository import GraphRepository, GraphRepositoryUnavailableError

class WalletClustering:
    def __init__(self, db_path=":memory:"):
        self.con = duckdb.connect(db_path)
        
    def cluster_proxy_wallets(self, graph_repo: GraphRepository) -> Dict[str, str]:
        """
        Uses DuckDB to cluster overlapping participant wallets into proxy_wallet 
        equivalence classes based on co-occurrence in funding/trade networks.
        """
        graph_repo.require_persisted_graph()
        edges = []
        with graph_repo._lock:
            for u, v, data in graph_repo.graph.edges(data=True):
                edge_type = data.get("type", "")
                if edge_type in ("TRANSFERRED_TO", "TOKEN_TRANSFERRED_TO", "TRADED_WITH"):
                    edges.append({
                        "source": u.replace("wallet:", ""),
                        "target": v.replace("wallet:", ""),
                        "value": float(data.get("value_wei", data.get("value", 1))),
                        "timestamp": data.get("timestamp", 0)
                    })
                    
        if not edges:
            return {}
            
        df_edges = pd.DataFrame(edges)
        
        # Load edges into DuckDB
        self.con.execute("CREATE OR REPLACE TABLE transfers AS SELECT * FROM df_edges")
        
        # We define a "proxy_wallet" equivalence class if two wallets have a direct transfer
        # This is a connected components problem. We can solve it using a recursive CTE in DuckDB.
        
        query = """
        WITH RECURSIVE
        undirected_edges AS (
            SELECT source AS u, target AS v FROM transfers
            UNION
            SELECT target AS u, source AS v FROM transfers
        ),
        connected_components AS (
            -- Base case: every node is in its own component
            SELECT u AS node, u AS component_id FROM undirected_edges
            UNION
            SELECT v AS node, v AS component_id FROM undirected_edges
            
            UNION
            
            -- Recursive step: propagate the smallest component_id to neighbors
            SELECT e.v AS node, c.component_id
            FROM undirected_edges e
            JOIN connected_components c ON e.u = c.node
            -- We restrict recursion depth or use a trick.
            -- Actually, recursive CTEs for connected components can infinite loop in standard SQL without cycle detection.
        )
        -- Since DuckDB might struggle with full connected components recursively,
        -- we use a math-heavy co-occurrence scoring instead.
        """
        
        # Let's use a co-occurrence threshold:
        # If A and B transact with each other, or share the exact same funding source (C -> A and C -> B),
        # they are proxies.
        
        query_co_occurrence = """
        WITH funding AS (
            SELECT source AS funder, target AS funded, COUNT(*) as tx_count, SUM(value) as total_value
            FROM transfers
            GROUP BY 1, 2
        ),
        shared_funding AS (
            SELECT f1.funded AS wallet1, f2.funded AS wallet2, f1.funder
            FROM funding f1
            JOIN funding f2 ON f1.funder = f2.funder AND f1.funded < f2.funded
            WHERE f1.tx_count > 0 AND f2.tx_count > 0
        ),
        direct_transfers AS (
            SELECT source AS wallet1, target AS wallet2
            FROM transfers
            WHERE source < target
        ),
        all_pairs AS (
            SELECT wallet1, wallet2 FROM shared_funding
            UNION
            SELECT wallet1, wallet2 FROM direct_transfers
        )
        SELECT wallet1, wallet2 FROM all_pairs
        """
        
        pairs_df = self.con.execute(query_co_occurrence).df()
        
        # Now we assign proxy IDs
        # To do the final grouping, we can just use a fast union-find in python over the filtered pairs.
        parent = {}
        
        def find(i):
            if parent.setdefault(i, i) == i:
                return i
            parent[i] = find(parent[i])
            return parent[i]
            
        def union(i, j):
            root_i = find(i)
            root_j = find(j)
            if root_i != root_j:
                parent[root_i] = root_j

        for _, row in pairs_df.iterrows():
            union(row['wallet1'], row['wallet2'])
            
        proxy_map = {}
        for node in parent.keys():
            proxy_map[node] = find(node)
            
        # Update the graph repository with proxy_wallet properties
        for wallet, proxy in proxy_map.items():
            graph_repo.upsert_node(f"wallet:{wallet}", "Wallet", {"proxy_wallet": proxy})
            
        return proxy_map

if __name__ == "__main__":
    print("Testing WalletClustering with persistent GraphRepository...")
    repo = GraphRepository()
    clustering = WalletClustering()
    try:
        proxies = clustering.cluster_proxy_wallets(repo)
    except GraphRepositoryUnavailableError as exc:
        print(f"Proxy clustering unavailable: {exc}")
    else:
        print(f"Identified {len(set(proxies.values()))} proxy clusters across {len(proxies)} wallets.")
