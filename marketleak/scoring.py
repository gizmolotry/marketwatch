import duckdb
import numpy as np
import pandas as pd
from pathlib import Path

from marketleak.detectors import DetectionBatch, DetectorConfig, detect_legacy_ticks


DEFAULT_MARKETS_PARQUET = "demo_data/markets.parquet"
DEFAULT_TICKS_PARQUET = "demo_data/ticks.parquet"
DEFAULT_ONCHAIN_TICKS_DIR = "demo_data/onchain_ticks/published"
DEFAULT_ONCHAIN_TICKS_GLOB = "demo_data/onchain_ticks/published/*.parquet"

OPTIONAL_WALLET_COLUMNS = (
    "wallet_address",
    "wallet",
    "trader_wallet",
    "trader_address",
    "proxy_wallet",
    "proxyWallet",
    "submitted_by",
    "resolvedBy",
    "marketMakerAddress",
)


def _parquet_columns(con, parquet_path):
    """Return parquet column names without loading the full dataset."""
    if isinstance(parquet_path, list):
        path_str = "[" + ", ".join(f"'{p}'" for p in parquet_path) + "]"
    else:
        path_str = f"'{parquet_path}'"
        
    describe_df = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({path_str})"
    ).df()
    return set(describe_df["column_name"].tolist())


def resolve_tick_parquet_files(ticks_path=DEFAULT_TICKS_PARQUET):
    """Return tick parquet files, adding optional on-chain ticks for the default feed."""
    if isinstance(ticks_path, list):
        return ticks_path
        
    files = [str(ticks_path)]
    if str(ticks_path) == DEFAULT_TICKS_PARQUET:
        published_dir = Path(DEFAULT_ONCHAIN_TICKS_DIR)
        if published_dir.exists() and any(published_dir.glob("*.parquet")):
            files.append(DEFAULT_ONCHAIN_TICKS_GLOB)
    return files


def query_market_ticks(
    markets_path=DEFAULT_MARKETS_PARQUET,
    ticks_path=DEFAULT_TICKS_PARQUET,
    limit=None,
):
    """Load normalized market ticks joined with market metadata from Parquet."""
    con = duckdb.connect(database=":memory:")
    try:
        market_columns = _parquet_columns(con, markets_path)
        ticks_files = resolve_tick_parquet_files(ticks_path)
        tick_columns = _parquet_columns(con, ticks_files)

        optional_selects = []
        for column in OPTIONAL_WALLET_COLUMNS:
            if column in tick_columns:
                optional_selects.append(f't."{column}" AS "{column}"')
            elif column in market_columns:
                optional_selects.append(f'm."{column}" AS "{column}"')

        optional_sql = ""
        if optional_selects:
            optional_sql = ",\n            " + ",\n            ".join(optional_selects)

        market_uid_select = "m.market_uid," if "market_uid" in market_columns else "t.market_uid,"

        if isinstance(ticks_files, list):
            ticks_str = "[" + ", ".join(f"'{p}'" for p in ticks_files) + "]"
        else:
            ticks_str = f"'{ticks_files}'"
            
        markets_str = f"'{markets_path}'"

        query = f"""
            SELECT
                {market_uid_select}
                m.market_slug,
                m.question,
                m.close_time,
                t.timestamp,
                t.price{optional_sql}
            FROM read_parquet({ticks_str}, union_by_name=True) AS t
            INNER JOIN read_parquet({markets_str}) AS m
                ON t.market_uid = m.market_uid
            ORDER BY
                m.market_slug,
                t.timestamp,
                t.tick_uid
        """
        params = []
        if limit is not None:
            query += "\n            LIMIT ?"
            params.append(int(limit))

        return con.execute(query, params).df()
    finally:
        con.close()

def compute_logit(p):
    """Convert price probability to log-odds belief."""
    p = np.clip(p, 0.001, 0.999)
    return np.log(p / (1 - p))


def detect_belief_shocks(df, window=12, z_threshold=2.5):
    """
    Legacy v1 compatibility adapter.

    This observation-count rolling z-score is retained only for existing
    callers and reproducibility. New surveillance code must use
    :func:`detect_activity_v2`, whose baseline is causal and time-based.
    """
    df = df.copy()

    if "market_slug" not in df.columns:
        df["market_slug"] = "__default_market__"

    df = df.sort_values(["market_slug", "timestamp"]).reset_index(drop=True)

    df["logit_belief"] = compute_logit(df["price"])

    df["belief_shock"] = df.groupby("market_slug")["logit_belief"].diff()

    df["rolling_mean"] = df.groupby("market_slug")["belief_shock"].transform(
        lambda x: x.shift(1).rolling(window=window, min_periods=window).mean()
    )
    df["rolling_std"] = df.groupby("market_slug")["belief_shock"].transform(
        lambda x: x.shift(1).rolling(window=window, min_periods=window).std()
    )

    std_safe = df["rolling_std"].replace(0, np.nan).fillna(0.01)
    df["z_score"] = (df["belief_shock"] - df["rolling_mean"]) / std_safe

    df["is_anomaly"] = np.abs(df["z_score"]) > z_threshold

    return df


def detect_belief_shocks_from_parquet(
    markets_path=DEFAULT_MARKETS_PARQUET,
    ticks_path=DEFAULT_TICKS_PARQUET,
    window=12,
    z_threshold=2.5,
):
    """Query Parquet data with DuckDB, then run belief shock detection."""
    df = query_market_ticks(markets_path=markets_path, ticks_path=ticks_path)
    return detect_belief_shocks(df, window=window, z_threshold=z_threshold)


def get_top_anomalies(df):
    """Legacy v1 extraction adapter for ``detect_belief_shocks`` output."""
    anomalies = df[df["is_anomaly"]].copy()
    anomalies["shock_magnitude"] = np.abs(anomalies["z_score"])
    return anomalies.sort_values(by="shock_magnitude", ascending=False)


# Explicit name for audit/replay code that opts into the former algorithm.
detect_belief_shocks_legacy = detect_belief_shocks


def detect_activity_v2(
    ticks,
    *,
    config: DetectorConfig | None = None,
) -> DetectionBatch:
    """Run validation-first activity detection on legacy snapshot rows.

    The adapter preserves absent trade/actor information as unavailable and
    applies semantic deduplication, elapsed-history gates, and causal scoring.
    """

    return detect_legacy_ticks(ticks, config=config)


def _read_tick_files_v2(ticks_path=DEFAULT_TICKS_PARQUET) -> pd.DataFrame:
    files = ticks_path if isinstance(ticks_path, list) else [ticks_path]
    con = duckdb.connect(database=":memory:")
    try:
        path_sql = "[" + ", ".join(f"'{str(path)}'" for path in files) + "]"
        return con.execute(
            f"""
            SELECT tick_uid, market_uid, timestamp, price
            FROM read_parquet({path_sql}, union_by_name=true)
            WHERE tick_uid IS NOT NULL
              AND market_uid IS NOT NULL
              AND timestamp IS NOT NULL
              AND price IS NOT NULL
            """
        ).df()
    finally:
        con.close()


def detect_activity_v2_from_parquet(
    *,
    ticks_path=DEFAULT_TICKS_PARQUET,
    config: DetectorConfig | None = None,
) -> DetectionBatch:
    """Load snapshot rows and run the v2 causal detector."""

    return detect_activity_v2(_read_tick_files_v2(ticks_path), config=config)


def v2_actionable_frame(
    batch: DetectionBatch,
    *,
    markets_path=DEFAULT_MARKETS_PARQUET,
) -> pd.DataFrame:
    """Return a legacy-shaped frame for review candidates only."""

    columns = [
        "market_uid",
        "market_slug",
        "question",
        "close_time",
        "outcome_uid",
        "timestamp",
        "activity_status",
        "diagnostic_score",
        "p_value",
        "q_value",
        "rank",
        "incident_uid",
        "not_proof_of_fraud",
        "effectiveness_unknown",
    ]
    abnormal = [signal for signal in batch.signals if signal.actionable]
    if not abnormal:
        return pd.DataFrame(columns=columns)
    metadata: dict[str, dict] = {}
    try:
        markets = pd.read_parquet(markets_path)
        metadata = {
            str(row["market_uid"]): row
            for row in markets.to_dict("records")
        }
    except (FileNotFoundError, OSError, KeyError):
        metadata = {}
    rows = []
    for signal in abnormal:
        market = metadata.get(signal.market_uid, {})
        rows.append(
            {
                "market_uid": signal.market_uid,
                "market_slug": market.get("market_slug", signal.market_uid),
                "question": market.get("question", ""),
                "close_time": market.get("close_time"),
                "outcome_uid": signal.outcome_uid,
                "timestamp": int(signal.bucket_time.timestamp()),
                "activity_status": signal.status.value,
                "diagnostic_score": signal.score,
                "p_value": signal.p_value,
                "q_value": signal.q_value,
                "rank": signal.rank,
                "incident_uid": signal.incident_uid,
                "not_proof_of_fraud": True,
                "effectiveness_unknown": True,
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values(
        ["q_value", "diagnostic_score"], ascending=[True, False]
    ).reset_index(drop=True)
