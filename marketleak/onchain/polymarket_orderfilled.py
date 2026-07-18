"""Raw-first Polygon CTF Exchange V2 ``OrderFilled`` corroboration.

The implementation follows Polymarket's published Polygon contract registry
and CTF Exchange V2 event source.  It decodes only the V2 ``OrderFilled`` ABI
shape, requires immutable raw-log lineage, and links a log only to one exact
canonical venue fill.  It deliberately emits settlement context, never an
identity, ownership, intent, price, or legal conclusion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
import re
from typing import Any, Iterable, Mapping, Sequence

from web3 import Web3

from marketleak.domain import TradeFill


POLYGON_CHAIN_ID = 137
CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310f59"
OFFICIAL_EXCHANGE_CONTRACTS = frozenset({CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2})

# Polymarket/ctf-exchange-v2/src/exchange/mixins/Events.sol:
# OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)
ORDER_FILLED_V2_SIGNATURE = "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
ORDER_FILLED_V2_TOPIC = Web3.to_hex(Web3.keccak(text=ORDER_FILLED_V2_SIGNATURE)).lower()
TRANSFER_SINGLE_TOPIC = Web3.to_hex(
    Web3.keccak(text="TransferSingle(address,address,address,uint256,uint256)")
).lower()

_HEX_RE = re.compile(r"^0x[0-9a-fA-F]*$")
_HASH_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_FIELD_FRAGMENTS = ("bitcoin", "btc", "identity", "intent")


class OrderFilledRejection(ValueError):
    """A log or join that cannot become settlement corroboration."""


def _utc(value: datetime | str | int | float) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = datetime.fromtimestamp(value, tz=UTC)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise OrderFilledRejection("block timestamp must be an ISO-8601 string or Unix timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OrderFilledRejection("block timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _normal_hash(value: object, *, field: str) -> str:
    text = str(value).strip()
    if not _HASH_RE.fullmatch(text):
        raise OrderFilledRejection(f"{field} must be a 32-byte 0x hash")
    return text.lower()


def _normal_address(value: object, *, field: str) -> str:
    text = str(value).strip()
    if not _ADDRESS_RE.fullmatch(text):
        raise OrderFilledRejection(f"{field} must be a 20-byte 0x address")
    return text.lower()


def _as_int(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise OrderFilledRejection(f"{field} must be an integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str):
        try:
            result = int(value, 0)
        except ValueError as exc:
            raise OrderFilledRejection(f"{field} must be an integer") from exc
    else:
        raise OrderFilledRejection(f"{field} must be an integer")
    if result < 0:
        raise OrderFilledRejection(f"{field} must be non-negative")
    return result


def _required_alias(payload: Mapping[str, object], *names: str) -> object:
    found = [(name, payload[name]) for name in names if name in payload]
    if not found:
        raise OrderFilledRejection(f"missing required raw log field: {names[0]}")
    if len({repr(value) for _, value in found}) != 1:
        raise OrderFilledRejection(f"ambiguous duplicate raw log field: {names[0]}")
    return found[0][1]


def _word(data: str, index: int) -> int:
    start = 2 + index * 64
    return int(data[start : start + 64], 16)


def _word_hex(data: str, index: int) -> str:
    start = 2 + index * 64
    return "0x" + data[start : start + 64].lower()


def _topic_address(topic: str, *, field: str) -> str:
    normalized = _normal_hash(topic, field=field)
    if int(normalized[2:-40], 16) != 0:
        raise OrderFilledRejection(f"{field} must be ABI-encoded indexed address")
    return "0x" + normalized[-40:]


def _reject_unsupported_claim_fields(payload: Mapping[str, object]) -> None:
    for name in payload:
        normalized = str(name).casefold()
        if normalized in {"price", "placeholder_price", "implied_price", "settlement_price"}:
            raise OrderFilledRejection("OrderFilled corroboration never accepts a price or price placeholder")
        if any(fragment in normalized for fragment in _FORBIDDEN_FIELD_FRAGMENTS):
            raise OrderFilledRejection("generic Bitcoin, identity, and intent claims are not settlement evidence")
    event_name = payload.get("event") or payload.get("event_type")
    if event_name is not None and str(event_name) != "OrderFilled":
        if str(event_name) == "TransferSingle":
            raise OrderFilledRejection("legacy TransferSingle logs are not OrderFilled settlement evidence")
        raise OrderFilledRejection("only documented OrderFilled logs are accepted")
    chain = payload.get("chain")
    if chain is not None and str(chain).casefold() != "polygon":
        raise OrderFilledRejection("generic Bitcoin and non-Polygon claims are not settlement evidence")


@dataclass(frozen=True)
class RawLogLineage:
    """Proof that a response was captured before on-chain decoding began."""

    raw_artifact_uid: str
    raw_sha256: str
    source_uid: str
    parser_version: str
    retrieved_at: datetime

    def __post_init__(self) -> None:
        if not str(self.raw_artifact_uid).strip() or not str(self.source_uid).strip() or not str(self.parser_version).strip():
            raise OrderFilledRejection("raw artifact UID, source UID, and parser version are required")
        if not _SHA256_RE.fullmatch(str(self.raw_sha256)):
            raise OrderFilledRejection("raw_sha256 must be a lowercase SHA-256 digest")
        object.__setattr__(self, "retrieved_at", _utc(self.retrieved_at))


@dataclass(frozen=True)
class OrderFilledV2Log:
    """Documented V2 fields from one official Polygon exchange log."""

    chain_id: int
    contract_address: str
    transaction_hash: str
    log_index: int
    block_hash: str
    block_number: int
    block_timestamp: datetime
    order_hash: str
    maker_address: str
    taker_address: str
    side: str
    token_id: int
    maker_amount_filled: int
    taker_amount_filled: int
    fee: int
    builder: str
    metadata: str
    lineage: RawLogLineage

    def __post_init__(self) -> None:
        if self.chain_id != POLYGON_CHAIN_ID:
            raise OrderFilledRejection("only Polygon chain_id=137 logs are accepted")
        object.__setattr__(self, "contract_address", _normal_address(self.contract_address, field="contract_address"))
        if self.contract_address not in OFFICIAL_EXCHANGE_CONTRACTS:
            raise OrderFilledRejection("log contract is not an official Polymarket CTF Exchange V2 contract")
        object.__setattr__(self, "transaction_hash", _normal_hash(self.transaction_hash, field="transaction_hash"))
        object.__setattr__(self, "block_hash", _normal_hash(self.block_hash, field="block_hash"))
        object.__setattr__(self, "order_hash", _normal_hash(self.order_hash, field="order_hash"))
        object.__setattr__(self, "maker_address", _normal_address(self.maker_address, field="maker_address"))
        object.__setattr__(self, "taker_address", _normal_address(self.taker_address, field="taker_address"))
        object.__setattr__(self, "builder", _normal_hash(self.builder, field="builder"))
        object.__setattr__(self, "metadata", _normal_hash(self.metadata, field="metadata"))
        object.__setattr__(self, "block_timestamp", _utc(self.block_timestamp))
        if self.log_index < 0 or self.block_number < 0 or self.token_id <= 0:
            raise OrderFilledRejection("log index, block number, and token ID must be valid")
        if self.side not in {"buy", "sell"}:
            raise OrderFilledRejection("OrderFilled V2 side must be buy or sell")
        if self.maker_amount_filled <= 0 or self.taker_amount_filled <= 0 or self.fee < 0:
            raise OrderFilledRejection("OrderFilled V2 amounts must be positive and fee non-negative")

    @property
    def venue_transaction_uid(self) -> str:
        return f"polymarket:tx/{self.transaction_hash}"

    @property
    def canonical_outcome_uid(self) -> str:
        return f"polymarket:outcome/{self.token_id}"


@dataclass(frozen=True)
class CanonicalVenueFill:
    """Minimal immutable canonical fill reference required for a join."""

    fill_uid: str
    transaction_hash: str
    market_uid: str
    outcome_uid: str

    def __post_init__(self) -> None:
        if not all(str(value).strip() for value in (self.fill_uid, self.market_uid, self.outcome_uid)):
            raise OrderFilledRejection("canonical fill UID, market UID, and outcome UID are required")
        object.__setattr__(self, "transaction_hash", _normal_hash(self.transaction_hash, field="canonical transaction_hash"))

    @classmethod
    def from_trade_fill(cls, fill: TradeFill) -> "CanonicalVenueFill":
        if fill.platform != "polymarket" or not fill.transaction_uid:
            raise OrderFilledRejection("only canonical Polymarket fills with transaction lineage can be joined")
        prefix = "polymarket:tx/"
        if not fill.transaction_uid.startswith(prefix):
            raise OrderFilledRejection("canonical Polymarket transaction UID is malformed")
        return cls(
            fill_uid=fill.fill_uid,
            transaction_hash=fill.transaction_uid.removeprefix(prefix),
            market_uid=fill.market_uid,
            outcome_uid=fill.outcome_uid,
        )


@dataclass(frozen=True)
class SettlementCorroboration:
    """An exact settlement-to-venue-fill association, not a claim about a person."""

    status: str
    reason: str | None
    log: OrderFilledV2Log
    fill_uid: str | None
    market_uid: str | None
    outcome_uid: str | None
    identity_inferred: bool = False
    intent_inferred: bool = False

    def __post_init__(self) -> None:
        if self.status not in {"corroborated", "unmatched", "ambiguous"}:
            raise OrderFilledRejection("unknown settlement corroboration status")
        if self.status == "corroborated":
            if not all((self.fill_uid, self.market_uid, self.outcome_uid)) or self.reason is not None:
                raise OrderFilledRejection("corroborated settlement requires one exact canonical fill")
        elif self.fill_uid is not None or self.market_uid is not None or self.outcome_uid is not None:
            raise OrderFilledRejection("unmatched or ambiguous settlement cannot name a canonical fill")
        if self.identity_inferred or self.intent_inferred:
            raise OrderFilledRejection("settlement corroboration cannot infer identity or intent")


def parse_order_filled_v2(raw_log: Mapping[str, object], *, lineage: RawLogLineage) -> OrderFilledV2Log:
    """Decode exactly one raw Polygon V2 OrderFilled log with no RPC access."""

    if not isinstance(raw_log, Mapping):
        raise OrderFilledRejection("raw log must be a mapping captured before parsing")
    _reject_unsupported_claim_fields(raw_log)
    chain_id = _as_int(_required_alias(raw_log, "chain_id", "chainId"), field="chain_id")
    if chain_id != POLYGON_CHAIN_ID:
        raise OrderFilledRejection("only Polygon chain_id=137 logs are accepted")
    contract_address = _normal_address(_required_alias(raw_log, "address", "contract_address"), field="contract_address")
    if contract_address not in OFFICIAL_EXCHANGE_CONTRACTS:
        raise OrderFilledRejection("log contract is not an official Polymarket CTF Exchange V2 contract")
    topics = _required_alias(raw_log, "topics")
    if not isinstance(topics, Sequence) or isinstance(topics, (str, bytes)) or len(topics) != 4:
        raise OrderFilledRejection("OrderFilled V2 requires exactly four topics")
    normalized_topics = tuple(_normal_hash(item, field=f"topics[{index}]") for index, item in enumerate(topics))
    if normalized_topics[0] == TRANSFER_SINGLE_TOPIC:
        raise OrderFilledRejection("legacy TransferSingle logs are not OrderFilled settlement evidence")
    if normalized_topics[0] != ORDER_FILLED_V2_TOPIC:
        raise OrderFilledRejection("log topic is not the documented OrderFilled V2 event")
    data = str(_required_alias(raw_log, "data")).strip()
    if not _HEX_RE.fullmatch(data) or len(data) != 2 + 7 * 64:
        raise OrderFilledRejection("OrderFilled V2 data must contain exactly seven ABI words")
    side_code = _word(data, 0)
    if side_code not in {0, 1}:
        raise OrderFilledRejection("OrderFilled V2 side enum is invalid")
    return OrderFilledV2Log(
        chain_id=chain_id,
        contract_address=contract_address,
        transaction_hash=_required_alias(raw_log, "transaction_hash", "transactionHash"),
        log_index=_as_int(_required_alias(raw_log, "log_index", "logIndex"), field="log_index"),
        block_hash=_required_alias(raw_log, "block_hash", "blockHash"),
        block_number=_as_int(_required_alias(raw_log, "block_number", "blockNumber"), field="block_number"),
        block_timestamp=_utc(_required_alias(raw_log, "block_timestamp", "blockTimestamp")),
        order_hash=normalized_topics[1],
        maker_address=_topic_address(normalized_topics[2], field="topics[2]"),
        taker_address=_topic_address(normalized_topics[3], field="topics[3]"),
        side="buy" if side_code == 0 else "sell",
        token_id=_word(data, 1),
        maker_amount_filled=_word(data, 2),
        taker_amount_filled=_word(data, 3),
        fee=_word(data, 4),
        builder=_word_hex(data, 5),
        metadata=_word_hex(data, 6),
        lineage=lineage,
    )


def reconcile_order_filled(
    log: OrderFilledV2Log,
    canonical_fills: Iterable[CanonicalVenueFill | TradeFill],
) -> SettlementCorroboration:
    """Join only one matching canonical transaction and outcome-token fill."""

    fills = [
        item if isinstance(item, CanonicalVenueFill) else CanonicalVenueFill.from_trade_fill(item)
        for item in canonical_fills
    ]
    matches = [
        item
        for item in fills
        if item.transaction_hash == log.transaction_hash and item.outcome_uid == log.canonical_outcome_uid
    ]
    if not matches:
        return SettlementCorroboration(
            status="unmatched",
            reason="no_exact_canonical_transaction_and_outcome_join",
            log=log,
            fill_uid=None,
            market_uid=None,
            outcome_uid=None,
        )
    if len(matches) != 1:
        return SettlementCorroboration(
            status="ambiguous",
            reason="multiple_canonical_fills_match_transaction_and_outcome",
            log=log,
            fill_uid=None,
            market_uid=None,
            outcome_uid=None,
        )
    fill = matches[0]
    return SettlementCorroboration(
        status="corroborated",
        reason=None,
        log=log,
        fill_uid=fill.fill_uid,
        market_uid=fill.market_uid,
        outcome_uid=fill.outcome_uid,
    )


def parse_and_reconcile_order_filled(
    raw_log: Mapping[str, object],
    *,
    lineage: RawLogLineage,
    canonical_fills: Iterable[CanonicalVenueFill | TradeFill],
) -> SettlementCorroboration:
    """Raw-first convenience entry point; parsing always precedes joining."""

    return reconcile_order_filled(parse_order_filled_v2(raw_log, lineage=lineage), canonical_fills)


__all__ = [
    "CTF_EXCHANGE_V2",
    "CanonicalVenueFill",
    "NEG_RISK_CTF_EXCHANGE_V2",
    "OFFICIAL_EXCHANGE_CONTRACTS",
    "ORDER_FILLED_V2_SIGNATURE",
    "ORDER_FILLED_V2_TOPIC",
    "OrderFilledRejection",
    "OrderFilledV2Log",
    "POLYGON_CHAIN_ID",
    "RawLogLineage",
    "SettlementCorroboration",
    "parse_and_reconcile_order_filled",
    "parse_order_filled_v2",
    "reconcile_order_filled",
]
