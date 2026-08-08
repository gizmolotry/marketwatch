from itertools import islice
from pathlib import Path
from threading import RLock
from typing import List, Dict, Any, Tuple

import networkx as nx

from marketleak.graph.safe_persistence import (
    ArtifactCorruptError,
    ArtifactUnavailableError,
    load_graph,
    save_graph,
)


class GraphRepositoryUnavailableError(RuntimeError):
    """A persisted graph was not verified and cannot support absence claims."""


class GraphRepository:
    MAX_EVIDENCE_PATHS_PER_TARGET = 50

    def __init__(self, persist_path: str = "demo_data/graph.json"):
        requested = Path(persist_path)
        self.legacy_persist_path: Path | None = None
        if requested.suffix.casefold() in {".pkl", ".pickle"}:
            self.legacy_persist_path = requested
            requested = requested.with_suffix(".json")
        self.persist_path = str(requested)
        self.load_status = "unavailable"
        self.load_error: str | None = None
        self._lock = RLock()
        self.graph = self._load()
        self._persisted_node_count = self.graph.number_of_nodes() if self.load_status == "loaded" else None
        self._persisted_edge_count = self.graph.number_of_edges() if self.load_status == "loaded" else None

    @property
    def persisted_graph_available(self) -> bool:
        return self.load_status == "loaded"

    def availability_payload(self) -> dict[str, object]:
        """Describe storage availability without recasting missing data as zero."""

        available = self.persisted_graph_available
        return {
            "status": "available" if available else "unavailable",
            "storage_state": self.load_status,
            "reason": "verified_graph_artifact_loaded" if available else self.load_error,
            "persisted_graph_available": available,
            "persisted_node_count": self._persisted_node_count,
            "persisted_edge_count": self._persisted_edge_count,
            "empty_graph_observed": (
                self._persisted_node_count == 0 and self._persisted_edge_count == 0
                if available
                else None
            ),
            "absence_claim_eligible": available,
        }

    def require_persisted_graph(self) -> None:
        if not self.persisted_graph_available:
            raise GraphRepositoryUnavailableError(self.load_error or "graph_artifact_unavailable")

    def _load(self) -> nx.MultiDiGraph:
        legacy = self.legacy_persist_path or Path(self.persist_path).with_suffix(".pkl")
        if legacy.exists():
            print(f"Warning: Ignoring unsupported legacy pickle graph artifact at {legacy}")
        try:
            graph = load_graph(self.persist_path)
        except ArtifactUnavailableError:
            self.load_status = "unavailable"
            self.load_error = "graph_artifact_unavailable"
            return nx.MultiDiGraph()
        except ArtifactCorruptError:
            self.load_status = "corrupt"
            self.load_error = "graph_artifact_corrupt"
            print(f"Warning: Refusing corrupt graph artifact at {self.persist_path}")
            return nx.MultiDiGraph()
        self.load_status = "loaded"
        self.load_error = None
        return graph

    def save(self):
        """Persist the graph as bounded canonical JSON."""
        with self._lock:
            save_graph(self.graph, self.persist_path)
            self.load_status = "loaded"
            self.load_error = None
            self._persisted_node_count = self.graph.number_of_nodes()
            self._persisted_edge_count = self.graph.number_of_edges()

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
