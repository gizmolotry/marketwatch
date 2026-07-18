import os
import pickle
from itertools import islice
from threading import RLock
from typing import List, Dict, Any, Tuple

import networkx as nx


class GraphRepository:
    MAX_EVIDENCE_PATHS_PER_TARGET = 50

    def __init__(self, persist_path: str = "demo_data/graph.pkl"):
        self.persist_path = persist_path
        self._lock = RLock()
        self.graph = self._load()

    def _load(self) -> nx.MultiDiGraph:
        if os.path.exists(self.persist_path):
            try:
                with open(self.persist_path, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"Warning: Failed to load graph from {self.persist_path}: {e}")
        return nx.MultiDiGraph()

    def save(self):
        """Persist the graph to disk."""
        with self._lock:
            os.makedirs(os.path.dirname(self.persist_path), exist_ok=True)
            with open(self.persist_path, "wb") as f:
                pickle.dump(self.graph, f)

    def upsert_node(self, node_id: str, label: str, properties: Dict[str, Any] = None):
        """Insert or update a node with a specific label."""
        properties = dict(properties or {})
        properties["label"] = label

        with self._lock:
            if self.graph.has_node(node_id):
                # Update properties
                for k, v in properties.items():
                    self.graph.nodes[node_id][k] = v
            else:
                self.graph.add_node(node_id, **properties)

    def upsert_edge(self, src_id: str, dst_id: str, relationship_type: str, properties: Dict[str, Any] = None):
        """Insert or update an edge representing a relationship."""
        properties = dict(properties or {})
        properties["type"] = relationship_type

        with self._lock:
            # Ensure nodes exist, even without full properties
            if not self.graph.has_node(src_id):
                self.upsert_node(src_id, "Unknown")
            if not self.graph.has_node(dst_id):
                self.upsert_node(dst_id, "Unknown")

            self.graph.add_edge(src_id, dst_id, **properties)

    def find_evidence_paths(self, start_node_id: str, target_node_labels: List[str], max_depth: int = 4) -> List[Tuple[List[str], float]]:
        """
        Find bounded paths from start_node_id to any node with a label in target_node_labels.
        Returns a list of tuples: (path, score).
        """
        if max_depth < 0:
            return []

        with self._lock:
            if not self.graph.has_node(start_node_id):
                return []

            search_graph = nx.DiGraph()
            search_graph.add_nodes_from((node, data.copy()) for node, data in self.graph.nodes(data=True))
            search_graph.add_edges_from(self.graph.edges())
            degree_by_node = dict(self.graph.degree())

        target_nodes = [
            n for n, attr in search_graph.nodes(data=True)
            if attr.get("label") in target_node_labels
        ]

        paths_with_scores = []
        for target in target_nodes:
            if start_node_id == target:
                continue

            try:
                simple_paths = nx.shortest_simple_paths(search_graph, source=start_node_id, target=target)
                for path in islice(simple_paths, self.MAX_EVIDENCE_PATHS_PER_TARGET):
                    if len(path) > max_depth + 1:
                        break

                    # Calculate hub penalty based on degrees of intermediate nodes
                    hub_penalty = 0.0
                    for node in path[1:-1]:
                        degree = degree_by_node.get(node, 0)
                        if degree > 10:
                            hub_penalty += (degree - 10) * 0.1

                    # Calculate path score
                    path_score = 1.0 / (len(path) + hub_penalty)
                    paths_with_scores.append((path, path_score))
            except nx.NetworkXNoPath:
                continue

        # Sort by score descending
        paths_with_scores.sort(key=lambda x: x[1], reverse=True)
        return paths_with_scores
