from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import marketleak.api as api
from marketleak.multimodal.bitcoin_policy import (
    BitcoinAddressObservation,
    BitcoinContextStatus,
    ExternalMarketLink,
    LinkApprovalStatus,
    PublishedBitcoinContextRepository,
    PublishedBitcoinContextSnapshot,
    snapshot_config_document,
)
from marketleak.multimodal.schemas import Provenance, ReliabilityTier, SourceClass, SourceReliability
from marketleak.multimodal.serving import ServingBundleRepository


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
ADDRESS_UID = "btcaddr:observed-001"
RAW_ADDRESS = "bc1qrawaddressmustneverbeexposed"


def provenance(*, source_uid: str, raw_uid: str, minute: int = 0) -> Provenance:
    return Provenance(
        source_uid=source_uid,
        raw_artifact_uid=raw_uid,
        parser_version="phase15-bitcoin-policy-test",
        content_hash="a" * 64,
        retrieved_at=T0 + timedelta(minutes=minute),
        source_url="https://example.test/raw",
    )


def observation() -> BitcoinAddressObservation:
    return BitcoinAddressObservation(
        address_uid=ADDRESS_UID,
        observed_at=T0,
        first_seen_at=T0,
        retrieved_at=T0,
        ingested_at=T0,
        provenance=provenance(source_uid="source:btc-observation", raw_uid="raw:btc-observation"),
    )


def link(
    *,
    approval: LinkApprovalStatus = LinkApprovalStatus.APPROVED,
    polygon_address_uid: str | None = "polygonaddr:observed-001",
    event_uid: str | None = "event:observed-market",
    observed_at: datetime = T0,
    evidence_minute: int = 0,
    approved_at: datetime | None = T0 + timedelta(minutes=1),
    decision_as_of: datetime = T0 + timedelta(minutes=5),
) -> ExternalMarketLink:
    return ExternalMarketLink(
        link_uid="link:external-001",
        bitcoin_address_uid=ADDRESS_UID,
        polygon_address_uid=polygon_address_uid,
        event_uid=event_uid,
        approval_status=approval,
        observed_at=observed_at,
        first_seen_at=observed_at,
        approved_at=approved_at if approval == LinkApprovalStatus.APPROVED else None,
        decision_as_of=decision_as_of,
        evidence=provenance(
            source_uid="source:external-link-evidence",
            raw_uid="raw:external-link-evidence",
            minute=evidence_minute,
        ),
    )


def snapshot(*, links: tuple[ExternalMarketLink, ...] = ()) -> PublishedBitcoinContextSnapshot:
    return PublishedBitcoinContextSnapshot(
        snapshot_uid="snapshot:bitcoin-context-001",
        decision_as_of=T0 + timedelta(minutes=5),
        published_at=T0 + timedelta(minutes=6),
        address_observations=(observation(),),
        observed_polygon_address_uids=("polygonaddr:observed-001",),
        observed_event_uids=("event:observed-market",),
        external_links=links,
    )


def write_config(path: Path, value: PublishedBitcoinContextSnapshot) -> PublishedBitcoinContextRepository:
    path.write_text(json.dumps(snapshot_config_document(value)), encoding="utf-8")
    return PublishedBitcoinContextRepository(path)


def test_observed_address_is_isolated_until_a_predecision_approved_link_targets_observed_context() -> None:
    isolated = snapshot(links=(link(approval=LinkApprovalStatus.UNAPPROVED),))
    isolated_payload = isolated.safe_payload()

    assert isolated.attachable_links() == ()
    assert isolated_payload["contexts"] == [
        {
            "address_uid": ADDRESS_UID,
            "attachment_status": "isolated_context",
            "polygon_address_uids": [],
            "market_address_uids": [],
            "event_uids": [],
        }
    ]

    attached = snapshot(links=(link(),)).safe_payload()["contexts"][0]
    assert attached["address_uid"] == ADDRESS_UID
    assert attached["attachment_status"] == "attached_precomputed_external_context"
    assert attached["event_uids"] == ["event:observed-market"]


def test_missing_tampered_or_late_link_fails_closed(tmp_path: Path) -> None:
    missing_target = snapshot(
        links=(link(polygon_address_uid="polygonaddr:not-observed", event_uid="event:not-observed"),)
    )
    assert missing_target.attachable_links() == ()
    assert missing_target.safe_payload()["contexts"][0]["attachment_status"] == "isolated_context"

    with pytest.raises(ValidationError, match="after the decision cutoff"):
        link(evidence_minute=6, approved_at=T0 + timedelta(minutes=7))

    config_path = tmp_path / "bitcoin-context.json"
    config = snapshot_config_document(snapshot(links=(link(),)))
    config["snapshot"]["external_links"][0]["approval_status"] = "unapproved"  # type: ignore[index]
    config_path.write_text(json.dumps(config), encoding="utf-8")
    state = PublishedBitcoinContextRepository(config_path).load()
    assert state.status == BitcoinContextStatus.UNAVAILABLE
    assert state.snapshot is None


def test_repository_requires_published_snapshot_and_redacts_raw_addresses(tmp_path: Path) -> None:
    absent = PublishedBitcoinContextRepository(None).payload(as_of=T0 + timedelta(minutes=10))
    assert absent["status"] == "watchlist_not_configured"
    assert absent["contexts"] == []
    assert absent["live_fetch"] is False

    repository = write_config(tmp_path / "published.json", snapshot(links=(link(),)))
    payload = repository.payload(as_of=T0 + timedelta(minutes=10))
    encoded = json.dumps(payload).lower()

    assert payload["status"] == "available_precomputed_context"
    assert RAW_ADDRESS not in encoded
    assert payload["contexts"][0]["address_uid"] == ADDRESS_UID
    assert "raw_artifact_uid" not in encoded
    assert "content_hash" not in encoded


def test_v3_endpoint_has_no_address_lookup_and_returns_neutral_unconfigured_response(tmp_path: Path) -> None:
    serving = ServingBundleRepository(tmp_path / "empty-bundles", clock=lambda: T0 + timedelta(minutes=10))
    bitcoin_context = PublishedBitcoinContextRepository(None)
    api.app.dependency_overrides[api.get_v3_serving_repository] = lambda: serving
    api.app.dependency_overrides[api.get_v3_bitcoin_context_repository] = lambda: bitcoin_context
    client = TestClient(api.app)
    try:
        normal = client.get("/api/v3/bitcoin-context")
        attempted_lookup = client.get(f"/api/v3/bitcoin-context?address={RAW_ADDRESS}")
        path_lookup = client.get(f"/api/v3/bitcoin-context/{RAW_ADDRESS}")

        assert normal.status_code == attempted_lookup.status_code == 200
        assert path_lookup.status_code == 404
        assert normal.json()["status"] == "watchlist_not_configured"
        assert attempted_lookup.json() == normal.json()
        body = normal.json()
        assert body["live_fetch"] is False
        assert body["contexts"] == []
        assert RAW_ADDRESS not in json.dumps(body)
        for field in ("risk", "ownership", "owner", "identity", "fraud_probability"):
            assert field not in body
    finally:
        api.app.dependency_overrides.clear()
