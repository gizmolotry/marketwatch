"""Persistence mechanics fixtures; these do not measure MarketLeak effectiveness."""

from __future__ import annotations

import json
from pathlib import Path

import networkx as nx
import pytest

from marketleak.graph.repository import GraphRepository
from marketleak.graph.repository import GraphRepositoryUnavailableError
from marketleak.graph.clustering import WalletClustering
from marketleak.graph import safe_persistence
from marketleak.graph.safe_persistence import (
    ArtifactCorruptError,
    ArtifactUnavailableError,
    CLUSTER_FORMAT,
    GRAPH_FORMAT,
    load_cluster_map,
    load_graph,
    save_cluster_map,
    save_graph,
)


def test_graph_round_trip_is_canonical_and_preserves_multiedges(tmp_path: Path) -> None:
    path = tmp_path / "graph.json"
    graph = nx.MultiDiGraph()
    graph.add_node("wallet:one", label="Wallet", count=2)
    graph.add_node("event:one", label="Event", active=True)
    graph.add_edge("wallet:one", "event:one", key=0, type="OBSERVED_IN", weight=0.5)
    graph.add_edge("wallet:one", "event:one", key="audit", type="AUDITED_BY", note=None)

    save_graph(graph, path)
    restored = load_graph(path)

    assert restored.nodes == graph.nodes
    assert list(restored.edges(keys=True, data=True)) == list(graph.edges(keys=True, data=True))
    payload = path.read_bytes()
    assert payload == json.dumps(
        json.loads(payload), ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert json.loads(payload)["format"] == GRAPH_FORMAT


def test_graph_repository_interface_round_trips_through_json(tmp_path: Path) -> None:
    path = tmp_path / "graph.json"
    repository = GraphRepository(str(path))
    repository.upsert_node("wallet:one", "Wallet", {"balance": 7})
    repository.upsert_edge("wallet:one", "event:one", "OBSERVED_IN", {"weight": 1.0})
    repository.save()
    assert repository.persisted_graph_available is True
    assert repository.availability_payload()["absence_claim_eligible"] is True

    restored = GraphRepository(str(path))

    assert restored.load_status == "loaded"
    assert restored.graph.nodes["wallet:one"] == {"balance": 7, "label": "Wallet"}
    assert restored.graph.nodes["event:one"] == {"label": "Unknown"}
    assert restored.graph["wallet:one"]["event:one"][0] == {"weight": 1.0, "type": "OBSERVED_IN"}


def test_loaded_empty_graph_is_observed_zero_but_missing_graph_is_not(tmp_path: Path) -> None:
    missing = GraphRepository(str(tmp_path / "missing.json"))
    missing_payload = missing.availability_payload()
    assert missing_payload["status"] == "unavailable"
    assert missing_payload["empty_graph_observed"] is None
    assert missing_payload["persisted_node_count"] is None
    with pytest.raises(GraphRepositoryUnavailableError, match="graph_artifact_unavailable"):
        missing.require_persisted_graph()
    with pytest.raises(GraphRepositoryUnavailableError, match="graph_artifact_unavailable"):
        WalletClustering().cluster_proxy_wallets(missing)

    loaded_path = tmp_path / "loaded-empty.json"
    save_graph(nx.MultiDiGraph(), loaded_path)
    loaded = GraphRepository(str(loaded_path))
    loaded_payload = loaded.availability_payload()
    assert loaded_payload["status"] == "available"
    assert loaded_payload["empty_graph_observed"] is True
    assert loaded_payload["persisted_node_count"] == 0
    assert WalletClustering().cluster_proxy_wallets(loaded) == {}


def test_repository_ignores_legacy_pickle_without_reading_or_executing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    legacy = tmp_path / "graph.pkl"
    marker = tmp_path / "executed"
    legacy.write_bytes(b"cos\nsystem\n(S'echo compromised'\ntR.")
    original_open = Path.open

    def guarded_open(self: Path, *args, **kwargs):
        if self == legacy:
            raise AssertionError("legacy pickle must never be opened")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    repository = GraphRepository(str(legacy))

    assert repository.graph.number_of_nodes() == 0
    assert repository.load_status == "unavailable"
    assert repository.persist_path == str(tmp_path / "graph.json")
    assert not marker.exists()
    with pytest.raises(ArtifactUnavailableError, match="legacy pickle"):
        load_cluster_map(legacy)


def test_cluster_map_round_trip_rejects_corrupt_unknown_and_oversized_artifacts(tmp_path: Path) -> None:
    valid = tmp_path / "clusters.json"
    save_cluster_map({"0xabc": "cluster:one", "0xdef": "cluster:one"}, valid)
    assert load_cluster_map(valid) == {"0xabc": "cluster:one", "0xdef": "cluster:one"}
    assert json.loads(valid.read_bytes())["format"] == CLUSTER_FORMAT

    corrupt = tmp_path / "corrupt.json"
    corrupt.write_bytes(b"not-json")
    with pytest.raises(ArtifactCorruptError, match="valid UTF-8 canonical JSON"):
        load_cluster_map(corrupt)

    unknown = tmp_path / "unknown.json"
    unknown.write_text('{"clusters":[],"format":"marketleak-wallet-clusters/v2"}', encoding="utf-8")
    with pytest.raises(ArtifactCorruptError, match="schema"):
        load_cluster_map(unknown)

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"{}" * 33)
    with pytest.raises(ArtifactCorruptError, match="maximum byte size"):
        load_cluster_map(oversized, max_bytes=32)


def test_graph_repository_marks_corruption_and_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "graph.json"
    path.write_text('{"directed":true,"edges":[],"format":"wrong","multigraph":true,"nodes":[]}', encoding="utf-8")

    repository = GraphRepository(str(path))

    assert repository.load_status == "corrupt"
    assert repository.load_error == "graph_artifact_corrupt"
    assert repository.graph.number_of_nodes() == 0


def test_atomic_write_keeps_prior_cluster_artifact_when_replace_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "clusters.json"
    save_cluster_map({"wallet:one": "cluster:one"}, path)
    before = path.read_bytes()

    def fail_replace(_source, _target):
        raise OSError("replace unavailable")

    monkeypatch.setattr(safe_persistence.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace unavailable"):
        save_cluster_map({"wallet:two": "cluster:two"}, path)

    assert path.read_bytes() == before
    assert not tuple(tmp_path.glob("*.tmp"))


def test_nested_and_nonfinite_properties_are_rejected_before_write(tmp_path: Path) -> None:
    nested: object = "leaf"
    for _ in range(safe_persistence.MAX_NESTING + 1):
        nested = {"next": nested}
    graph = nx.MultiDiGraph()
    graph.add_node("wallet:one", label="Wallet", nested=nested)
    with pytest.raises(ArtifactCorruptError, match="nesting"):
        save_graph(graph, tmp_path / "nested.json")

    graph = nx.MultiDiGraph()
    graph.add_node("wallet:one", label="Wallet", score=float("nan"))
    with pytest.raises(ArtifactCorruptError, match="non-finite"):
        save_graph(graph, tmp_path / "nan.json")


def test_cluster_record_and_string_bounds_are_enforced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(safe_persistence, "MAX_CLUSTER_RECORDS", 1)
    with pytest.raises(ArtifactCorruptError, match="record count"):
        save_cluster_map({"wallet:one": "cluster:one", "wallet:two": "cluster:two"}, tmp_path / "many.json")

    long_identifier = "x" * (safe_persistence.MAX_STRING_LENGTH + 1)
    with pytest.raises(ArtifactCorruptError, match="bounded non-empty strings"):
        save_cluster_map({long_identifier: "cluster:one"}, tmp_path / "long.json")
