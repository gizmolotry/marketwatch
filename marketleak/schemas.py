"""Legacy v1 ingestion schemas.

Canonical validation-first contracts live in :mod:`marketleak.domain`.  These
names remain stable for existing ingestion and API callers while explicitly
representing fields that a price snapshot does not provide.
"""

from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr, model_validator


class Market(BaseModel):
    """Normalized prediction market metadata."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )

    market_uid: StrictStr = Field(min_length=1)
    platform: StrictStr = Field(min_length=1)
    market_slug: StrictStr = Field(min_length=1)
    question: StrictStr = Field(min_length=1)
    close_time: StrictStr = Field(min_length=1)


class MarketTick(BaseModel):
    """Legacy market price tick or snapshot.

    ``size``, ``maker`` and ``taker`` are optional because platform snapshots
    do not contain trade attribution.  Missing values must remain ``None``;
    fabricating zero size or placeholder actors would corrupt downstream
    coverage and actor-evidence analysis.
    """

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )

    tick_uid: StrictStr = Field(min_length=1)
    market_uid: StrictStr = Field(min_length=1)
    timestamp: StrictInt = Field(ge=0)
    price: StrictFloat = Field(ge=0.0, le=1.0)
    size: StrictFloat | None = Field(default=None, ge=0.0)
    maker: StrictStr | None = Field(default=None, min_length=42, max_length=42)
    taker: StrictStr | None = Field(default=None, min_length=42, max_length=42)
    platform: StrictStr = Field(min_length=1)

    @model_validator(mode="after")
    def require_complete_trade_attribution(self) -> "MarketTick":
        if (self.maker is None) != (self.taker is None):
            raise ValueError("maker and taker must either both be present or both be absent")
        return self
