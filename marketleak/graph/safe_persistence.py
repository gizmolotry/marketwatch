"""Bounded, non-executable persistence for graph and wallet-cluster artifacts."""

from __future__ import annotations

import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

import networkx as nx


GRAPH_FORMAT = "marketleak-graph/v1"
CLUSTER_FORMAT = "marketleak-wallet-clusters/v1"
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_GRAPH_NODES = 100_000
MAX_GRAPH_EDGES = 500_000
MAX_CLUSTER_RECORDS = 250_000
MAX_NESTING = 8
MAX_STRING_LENGTH = 16_384
MAX_CONTAINER_ITEMS = 1_000_000


class SafePersistenceError(RuntimeError):
    """Base class for safe persistence failures."""


class ArtifactUnavailableError(SafePersistenceError):
    """The requested artifact does not exist or is deliberately unsupported."""


class ArtifactCorruptError(SafePersistenceError):
    """The artifact is malformed, unsupported, or outside declared bounds."""


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ArtifactCorruptError("artifact contains a non-JSON value") from exc


def _validate_json_value(
    value: Any,
    *,
    field: str,
    depth: int = 0,
    budget: list[int] | None = None,
) -> None:
    if budget is None:
        budget = [MAX_CONTAINER_ITEMS]
    if depth > MAX_NESTING:
        raise ArtifactCorruptError(f"{field} exceeds maximum nesting depth")
    budget[0] -= 1
    if budget[0] < 0:
        raise ArtifactCorruptError("artifact exceeds maximum container items")
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ArtifactCorruptError(f"{field} contains a non-finite number")
        return
    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            raise ArtifactCorruptError(f"{field} exceeds maximum string length")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, field=f"{field}[{index}]", depth=depth + 1, budget=budget)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > MAX_STRING_LENGTH:
                raise ArtifactCorruptError(f"{field} contains an invalid object key")
            _validate_json_value(item, field=f"{field}.{key}", depth=depth + 1, budget=budget)
        return
    raise ArtifactCorruptError(f"{field} contains unsupported type {type(value).__name__}")


def _bounded_read(path: str | Path, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> bytes:
    artifact = Path(path)
    if artifact.suffix.casefold() in {".pkl", ".pickle"}:
        raise ArtifactUnavailableError("legacy pickle artifacts are ignored")
    if artifact.is_symlink():
        raise ArtifactCorruptError("artifact symlinks are not accepted")
    try:
        size = artifact.stat().st_size
    except FileNotFoundError as exc:
        raise ArtifactUnavailableError("artifact is unavailable") from exc
    if size > max_bytes:
        raise ArtifactCorruptError("artifact exceeds maximum byte size")
    try:
        with artifact.open("rb") as stream:
            payload = stream.read(max_bytes + 1)
    except OSError as exc:
        raise ArtifactUnavailableError("artifact cannot be read") from exc
    if len(payload) > max_bytes:
        raise ArtifactCorruptError("artifact exceeds maximum byte size")
    return payload


def _load_object(path: str | Path, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> dict[str, Any]:
    payload = _bounded_read(path, max_bytes=max_bytes)
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactCorruptError("artifact is not valid UTF-8 canonical JSON") from exc
    if not isinstance(value, dict):
        raise ArtifactCorruptError("artifact root must be an object")
    _validate_json_value(value, field="artifact")
    if payload != _canonical_bytes(value):
        raise ArtifactCorruptError("artifact is not canonical JSON")
    return value


def _atomic_write(path: str | Path, payload: bytes, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> None:
    artifact = Path(path)
    if artifact.suffix.casefold() in {".pkl", ".pickle"}:
        raise ArtifactUnavailableError("refusing to write a legacy pickle artifact")
    if len(payload) > max_bytes:
        raise ArtifactCorruptError("artifact exceeds maximum byte size")
    artifact.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{artifact.name}.", suffix=".tmp", dir=artifact.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, artifact)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def save_graph(graph: nx.MultiDiGraph, path: str | Path) -> None:
    if not isinstance(graph, nx.MultiDiGraph):
        raise ArtifactCorruptError("graph must be a networkx.MultiDiGraph")
    if graph.number_of_nodes() > MAX_GRAPH_NODES or graph.number_of_edges() > MAX_GRAPH_EDGES:
        raise ArtifactCorruptError("graph exceeds node or edge record bounds")
    nodes: list[dict[str, Any]] = []
    for node_id, properties in graph.nodes(data=True):
        if not isinstance(node_id, str) or not node_id or len(node_id) > MAX_STRING_LENGTH:
            raise ArtifactCorruptError("graph node identifiers must be bounded non-empty strings")
        props = dict(properties)
        _validate_json_value(props, field=f"node[{node_id}].properties")
        nodes.append({"id": node_id, "properties": props})
    nodes.sort(key=lambda item: item["id"])

    edges: list[dict[str, Any]] = []
    for source, target, key, properties in graph.edges(keys=True, data=True):
        if not isinstance(source, str) or not isinstance(target, str):
            raise ArtifactCorruptError("graph edge endpoints must be strings")
        if isinstance(key, bool) or not isinstance(key, (str, int)):
            raise ArtifactCorruptError("graph edge keys must be strings or integers")
        if isinstance(key, str) and (not key or len(key) > MAX_STRING_LENGTH):
            raise ArtifactCorruptError("graph edge string keys must be bounded and non-empty")
        props = dict(properties)
        _validate_json_value(props, field=f"edge[{source},{target}].properties")
        edges.append({"source": source, "target": target, "key": key, "properties": props})
    edges.sort(key=lambda item: (item["source"], item["target"], type(item["key"]).__name__, str(item["key"])))

    document = {"format": GRAPH_FORMAT, "directed": True, "multigraph": True, "nodes": nodes, "edges": edges}
    _validate_json_value(document, field="graph")
    _atomic_write(path, _canonical_bytes(document))


def load_graph(path: str | Path, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> nx.MultiDiGraph:
    document = _load_object(path, max_bytes=max_bytes)
    if set(document) != {"format", "directed", "multigraph", "nodes", "edges"}:
        raise ArtifactCorruptError("graph artifact fields do not match the frozen schema")
    if document["format"] != GRAPH_FORMAT or document["directed"] is not True or document["multigraph"] is not True:
        raise ArtifactCorruptError("graph artifact schema or graph type is unsupported")
    nodes = document["nodes"]
    edges = document["edges"]
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise ArtifactCorruptError("graph nodes and edges must be arrays")
    if len(nodes) > MAX_GRAPH_NODES or len(edges) > MAX_GRAPH_EDGES:
        raise ArtifactCorruptError("graph exceeds node or edge record bounds")

    graph = nx.MultiDiGraph()
    seen_nodes: set[str] = set()
    for row in nodes:
        if not isinstance(row, dict) or set(row) != {"id", "properties"}:
            raise ArtifactCorruptError("graph node record is malformed")
        node_id = row["id"]
        properties = row["properties"]
        if not isinstance(node_id, str) or not node_id or node_id in seen_nodes:
            raise ArtifactCorruptError("graph node identifier is invalid or duplicated")
        if not isinstance(properties, dict):
            raise ArtifactCorruptError("graph node properties must be an object")
        seen_nodes.add(node_id)
        graph.add_node(node_id, **properties)
    seen_edges: set[tuple[str, str, str, Any]] = set()
    for row in edges:
        if not isinstance(row, dict) or set(row) != {"source", "target", "key", "properties"}:
            raise ArtifactCorruptError("graph edge record is malformed")
        source, target, key, properties = row["source"], row["target"], row["key"], row["properties"]
        if source not in seen_nodes or target not in seen_nodes:
            raise ArtifactCorruptError("graph edge references an unknown node")
        if isinstance(key, bool) or not isinstance(key, (str, int)) or not isinstance(properties, dict):
            raise ArtifactCorruptError("graph edge key or properties are malformed")
        identity = (source, target, type(key).__name__, key)
        if identity in seen_edges:
            raise ArtifactCorruptError("graph edge identity is duplicated")
        seen_edges.add(identity)
        graph.add_edge(source, target, key=key, **properties)
    return graph


def save_cluster_map(mapping: Mapping[str, str], path: str | Path) -> None:
    if len(mapping) > MAX_CLUSTER_RECORDS:
        raise ArtifactCorruptError("cluster map exceeds maximum record count")
    records: list[dict[str, str]] = []
    for wallet, cluster_id in mapping.items():
        if not isinstance(wallet, str) or not wallet or len(wallet) > MAX_STRING_LENGTH:
            raise ArtifactCorruptError("cluster wallet identifiers must be bounded non-empty strings")
        if not isinstance(cluster_id, str) or not cluster_id or len(cluster_id) > MAX_STRING_LENGTH:
            raise ArtifactCorruptError("cluster identifiers must be bounded non-empty strings")
        records.append({"wallet": wallet, "cluster_id": cluster_id})
    records.sort(key=lambda item: item["wallet"])
    document = {"format": CLUSTER_FORMAT, "clusters": records}
    _validate_json_value(document, field="cluster_map")
    _atomic_write(path, _canonical_bytes(document))


def load_cluster_map(path: str | Path, *, max_bytes: int = MAX_ARTIFACT_BYTES) -> dict[str, str]:
    document = _load_object(path, max_bytes=max_bytes)
    if set(document) != {"format", "clusters"} or document["format"] != CLUSTER_FORMAT:
        raise ArtifactCorruptError("cluster artifact schema is unsupported")
    records = document["clusters"]
    if not isinstance(records, list) or len(records) > MAX_CLUSTER_RECORDS:
        raise ArtifactCorruptError("cluster records are malformed or exceed bounds")
    mapping: dict[str, str] = {}
    for row in records:
        if not isinstance(row, dict) or set(row) != {"wallet", "cluster_id"}:
            raise ArtifactCorruptError("cluster record is malformed")
        wallet, cluster_id = row["wallet"], row["cluster_id"]
        if not isinstance(wallet, str) or not wallet or wallet in mapping:
            raise ArtifactCorruptError("cluster wallet identifier is invalid or duplicated")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ArtifactCorruptError("cluster identifier is invalid")
        mapping[wallet] = cluster_id
    return mapping


__all__ = [
    "ArtifactCorruptError",
    "ArtifactUnavailableError",
    "CLUSTER_FORMAT",
    "GRAPH_FORMAT",
    "SafePersistenceError",
    "load_cluster_map",
    "load_graph",
    "save_cluster_map",
    "save_graph",
]
