import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from html import escape
import os
import sys
from pathlib import Path
# Ensure the parent directory is in the python path so 'marketleak' module can be found
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import glob
from marketleak.agents.anomaly_agent import run_anomaly_detection
from marketleak.agents.rag_agent import RAGAgent
from marketleak.agents.synthesis_agent import SynthesisAgent
from marketleak.scoring import query_market_ticks
from marketleak.agents.predict_agent import PredictAgent
from marketleak.graph.repository import GraphRepository
from marketleak.graph.clustering import WalletClustering
from marketleak.graph.safe_persistence import SafePersistenceError, load_cluster_map
from marketleak.api import read_report_for_product

st.set_page_config(page_title="MarketLeak", layout="wide", page_icon="🕵️‍♂️")

# Premium CSS for dark mode glassmorphism theme
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        font-family: 'Inter', sans-serif;
        color: #f8fafc;
    }
    h1, h2, h3 {
        color: #38bdf8 !important;
        font-weight: 600;
    }
    .stMarkdown, p, span {
        color: #e2e8f0;
    }
    .metric-box {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1), box-shadow 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        margin-bottom: 20px;
    }
    .metric-box:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 24px rgba(0, 0, 0, 0.3);
        border: 1px solid rgba(56, 189, 248, 0.3);
    }
    .metric-value {
        font-size: 2.8em;
        font-weight: 700;
        background: linear-gradient(to right, #38bdf8, #818cf8);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-top: 10px;
    }
    .metric-label {
        color: #94a3b8;
        font-size: 0.9em;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
    }
    /* Buttons */
    .stButton>button {
        background: linear-gradient(135deg, #38bdf8, #818cf8);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        transform: scale(1.02) translateY(-2px);
        box-shadow: 0 8px 20px rgba(56, 189, 248, 0.4);
    }
    /* Sidebar: stable Streamlit test-id selectors with explicit contrast. */
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] > div:first-child {
        background: linear-gradient(180deg, #0b1220 0%, #111c30 100%) !important;
    }
    [data-testid="stSidebar"] {
        border-right: 1px solid rgba(56, 189, 248, 0.20);
    }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] span,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h2,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] label span {
        color: #e2e8f0 !important;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label {
        padding: 7px 9px;
        border-radius: 9px;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:hover {
        background: rgba(56, 189, 248, 0.10);
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
        background: rgba(56, 189, 248, 0.14);
        box-shadow: inset 3px 0 0 #38bdf8;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) span {
        color: #7dd3fc !important;
        font-weight: 600;
    }
    [data-testid="stSidebar"] input[type="radio"] {
        accent-color: #38bdf8;
    }
    .ml-forensic-hero {
        position: relative;
        overflow: hidden;
        padding: 28px 30px;
        margin: 4px 0 22px;
        border: 1px solid rgba(56, 189, 248, 0.22);
        border-radius: 20px;
        background:
            radial-gradient(circle at 92% 18%, rgba(56, 189, 248, 0.20), transparent 34%),
            linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(30, 41, 59, 0.88));
        box-shadow: 0 22px 55px rgba(2, 6, 23, 0.28);
    }
    .ml-forensic-hero::after {
        content: "";
        position: absolute;
        width: 180px;
        height: 180px;
        right: -70px;
        bottom: -110px;
        border: 1px solid rgba(129, 140, 248, 0.30);
        border-radius: 50%;
    }
    .ml-forensic-eyebrow {
        color: #7dd3fc;
        font-size: 0.74rem;
        font-weight: 700;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        margin-bottom: 8px;
    }
    .ml-forensic-title {
        color: #f8fafc;
        font-size: clamp(1.55rem, 3vw, 2.35rem);
        line-height: 1.12;
        font-weight: 700;
        margin-bottom: 9px;
    }
    .ml-forensic-copy {
        color: #cbd5e1;
        max-width: 820px;
        font-size: 0.98rem;
        line-height: 1.6;
    }
    .ml-visual-key {
        display: flex;
        flex-wrap: wrap;
        gap: 9px 18px;
        align-items: center;
        padding: 11px 14px;
        margin: 8px 0 14px;
        border: 1px solid rgba(148, 163, 184, 0.14);
        border-radius: 12px;
        background: rgba(15, 23, 42, 0.54);
        color: #cbd5e1;
        font-size: 0.78rem;
    }
    .ml-key-item {
        display: inline-flex;
        align-items: center;
        gap: 7px;
    }
    .ml-key-mark {
        display: inline-block;
        width: 9px;
        height: 9px;
        border-radius: 50%;
        box-shadow: 0 0 10px currentColor;
    }
    .ml-key-mark.price { color: #38bdf8; background: #38bdf8; }
    .ml-key-mark.shock {
        color: #fb7185;
        background: #fb7185;
        border-radius: 1px;
        transform: rotate(45deg);
    }
    .ml-key-mark.evidence { color: #34d399; background: #34d399; }
    .ml-key-mark.context { color: #94a3b8; background: #94a3b8; }
    .ml-key-mark.gap {
        width: 18px;
        height: 5px;
        color: #f97316;
        background: #f97316;
        border-radius: 4px;
    }
    .ml-forensic-metric {
        min-height: 160px;
        padding: 18px 18px 16px;
        border: 1px solid rgba(148, 163, 184, 0.16);
        border-top: 3px solid var(--ml-tone);
        border-radius: 14px;
        background: linear-gradient(145deg, rgba(30, 41, 59, 0.76), rgba(15, 23, 42, 0.76));
        box-shadow: 0 13px 30px rgba(2, 6, 23, 0.18);
    }
    .ml-forensic-metric.tone-cyan { --ml-tone: #38bdf8; }
    .ml-forensic-metric.tone-red { --ml-tone: #fb7185; }
    .ml-forensic-metric.tone-orange { --ml-tone: #f97316; }
    .ml-forensic-metric.tone-green { --ml-tone: #34d399; }
    .ml-forensic-metric.tone-neutral { --ml-tone: #94a3b8; }
    .ml-forensic-metric-label {
        color: #94a3b8;
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.13em;
        text-transform: uppercase;
    }
    .ml-forensic-metric-value {
        color: #f8fafc;
        font-size: clamp(1.35rem, 2.5vw, 2rem);
        line-height: 1.18;
        font-weight: 700;
        margin: 9px 0 8px;
        font-variant-numeric: tabular-nums;
    }
    .ml-forensic-metric-detail {
        color: #aebdce;
        font-size: 0.78rem;
        line-height: 1.42;
    }
    </style>
""", unsafe_allow_html=True)

from dotenv import load_dotenv
load_dotenv()

st.title("🕵️‍♂️ MarketLeak: Market Integrity System")
st.error(
    "LEGACY / UNVALIDATED DEMO — effectiveness unknown. Outputs are triage aids only, "
    "not proof of fraud, MNPI use, insider trading, innocence, normal trading, or absence "
    "of misconduct. Human review and the validated v2 pipeline are required."
)
st.markdown(
    "Legacy anomaly heuristics, deprecated uncalibrated risk context, and an "
    "unvalidated contextual wallet graph."
)

# Sidebar
st.sidebar.header("Navigation")
menu = st.sidebar.radio("Select View:", [
    "1. System Overview", 
    "2. Live Market Defense", 
    "3. Contextual Wallet Graph (Unvalidated)"
])

@st.cache_data
def load_data():
    try:
        return query_market_ticks()
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def get_rag_agent():
    """Reuse the local embedding model across Streamlit reruns."""
    return RAGAgent()


def _safe_float(value):
    """Return a finite float, otherwise None."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _to_utc_timestamp(value):
    """Normalize epoch or date-like values to a timezone-aware UTC Timestamp."""
    if value is None:
        return None

    numeric = _safe_float(value)
    if numeric is not None:
        absolute = abs(numeric)
        if absolute >= 1e17:
            unit = "ns"
        elif absolute >= 1e14:
            unit = "us"
        elif absolute >= 1e11:
            unit = "ms"
        else:
            unit = "s"
        parsed = pd.to_datetime(numeric, unit=unit, utc=True, errors="coerce")
    else:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")

    if parsed is None or pd.isna(parsed):
        return None
    return pd.Timestamp(parsed)


def _prepare_market_timeline(raw_market_data):
    """Sanitize timestamps/prices and collapse duplicate observations."""
    columns = ["datetime", "price"]
    quality = {
        "source_rows": 0,
        "valid_rows": 0,
        "invalid_rows": 0,
        "duplicate_rows": 0,
    }
    if not isinstance(raw_market_data, pd.DataFrame) or raw_market_data.empty:
        return pd.DataFrame(columns=columns), quality
    if not {"timestamp", "price"}.issubset(raw_market_data.columns):
        quality["source_rows"] = len(raw_market_data)
        quality["invalid_rows"] = len(raw_market_data)
        return pd.DataFrame(columns=columns), quality

    work = raw_market_data.loc[:, ["timestamp", "price"]].copy().reset_index(drop=True)
    quality["source_rows"] = len(work)
    work["datetime"] = work["timestamp"].map(_to_utc_timestamp)
    work["price"] = pd.to_numeric(work["price"], errors="coerce")

    finite_price = np.isfinite(work["price"].to_numpy(dtype=float, na_value=np.nan))
    valid = work["datetime"].notna() & finite_price & work["price"].between(0.0, 1.0)
    quality["invalid_rows"] = int((~valid).sum())
    work = work.loc[valid, columns].sort_values("datetime", kind="mergesort")

    quality["duplicate_rows"] = int(work.duplicated("datetime", keep="last").sum())
    work = work.drop_duplicates("datetime", keep="last").reset_index(drop=True)
    quality["valid_rows"] = len(work)
    return work, quality


def _format_duration(hours):
    hours = abs(float(hours))
    if hours < (1 / 60):
        return "<1m"
    if hours < 1:
        return f"{hours * 60:.0f}m"
    if hours < 48:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def _lead_statement(lead_hours):
    if lead_hours is None:
        return "No timestamped public evidence is available for comparison."
    if abs(lead_hours) < (1 / 60):
        return "Market shock and public evidence were effectively simultaneous."
    duration = _format_duration(lead_hours)
    if lead_hours > 0:
        return (
            f"The market timestamp preceded this retrieved context publication by {duration}. "
            "This does not establish the earliest public disclosure or information leakage."
        )
    return (
        f"The supplied publication timestamp preceded the market move by {duration}. "
        "This timing comparison is not exonerating."
    )


def _lead_metric_value(lead_hours):
    if lead_hours is None:
        return "No evidence"
    if abs(lead_hours) < (1 / 60):
        return "0h · simultaneous"
    sign = "+" if lead_hours > 0 else "−"
    return f"{sign}{_format_duration(lead_hours)}"


def _strict_true(value):
    """Accept actual boolean truth without treating strings such as 'false' as true."""
    return isinstance(value, (bool, np.bool_)) and bool(value)


def _friendly_evidence_reason(
    evidence_scope,
    search_strategy,
    timing_valid,
    timing_issue,
    suppression_reason,
):
    """Translate retrieval controls into an analyst-facing attribution reason."""
    if (
        timing_issue == "shock_news_interval_exceeds_30_days"
        or suppression_reason == "shock_news_interval_exceeds_30_days"
    ):
        return (
            "Context publication is outside the 30-day attribution window; "
            "no information lead attributed."
        )
    if (
        timing_issue == "invalid_or_missing_shock_timestamp"
        or suppression_reason == "invalid_or_missing_shock_timestamp"
    ):
        return (
            "Shock timing is invalid or missing; the publication is context only "
            "and no information lead is attributed."
        )
    if suppression_reason == "evidence_unavailable" or evidence_scope == "unavailable":
        return "No timestamped public evidence is available."
    if (
        suppression_reason == "fallback_evidence_not_actionable"
        or evidence_scope != "market_specific"
        or search_strategy != "exact"
    ):
        return (
            "Broader-search evidence is context only — no information lead attributed."
        )
    if not timing_valid:
        return (
            "Evidence timing did not pass validation; no information lead attributed."
        )
    if suppression_reason:
        readable = str(suppression_reason).replace("_", " ")
        return f"Deprecated legacy PPIM heuristic suppressed ({readable}); no information lead attributed."
    return "Exact-query context with parseable timing; deprecated uncalibrated legacy heuristic only."


def _classify_rag_evidence(rag_result):
    """Classify evidence as absent, attributable, or contextual."""
    if not isinstance(rag_result, dict) or not rag_result:
        return {
            "state": "no_evidence",
            "attribution_eligible": False,
            "evidence_scope": "unavailable",
            "search_strategy": "unavailable",
            "timing_valid": False,
            "timing_issue": "evidence_unavailable",
            "suppression_reason": "evidence_unavailable",
            "status_message": "No timestamped public evidence is available.",
        }

    evidence_scope = str(rag_result.get("evidence_scope") or "unavailable").strip()
    search_strategy = str(rag_result.get("search_strategy") or "unavailable").strip()
    timing_valid = _strict_true(rag_result.get("timing_valid"))
    timing_issue = rag_result.get("timing_issue") or None
    suppression_reason = rag_result.get("ppim_suppression_reason") or None
    has_publication = any(
        rag_result.get(field)
        for field in ("news_timestamp", "evidence_date", "evidence_text", "evidence_url")
    )

    evidence_unavailable = (
        evidence_scope == "unavailable"
        or suppression_reason == "evidence_unavailable"
        or not has_publication
    )
    if evidence_unavailable:
        state = "no_evidence"
        attribution_eligible = False
    else:
        exact_market_evidence = (
            search_strategy == "exact" and evidence_scope == "market_specific"
        )
        attribution_eligible = (
            exact_market_evidence and timing_valid and suppression_reason is None
        )
        state = "attributed" if attribution_eligible else "context"

    return {
        "state": state,
        "attribution_eligible": attribution_eligible,
        "evidence_scope": evidence_scope,
        "search_strategy": search_strategy,
        "timing_valid": timing_valid,
        "timing_issue": timing_issue,
        "suppression_reason": suppression_reason,
        "status_message": _friendly_evidence_reason(
            evidence_scope,
            search_strategy,
            timing_valid,
            timing_issue,
            suppression_reason,
        ),
    }


def _sar_eligibility(rag_result, threshold=1.0):
    """Gate legacy UI memo generation behind an explicit environment opt-in."""
    if os.getenv("MARKETLEAK_ENABLE_LEGACY_REPORTS", "").strip() != "1":
        return (
            False,
            "Legacy memo generation is disabled by default. Set "
            "MARKETLEAK_ENABLE_LEGACY_REPORTS=1 only for acknowledged human review.",
        )
    evidence = _classify_rag_evidence(rag_result)
    if evidence["state"] == "no_evidence":
        return False, evidence["status_message"]
    if not evidence["attribution_eligible"]:
        return False, evidence["status_message"]

    score = _safe_float(rag_result.get("ppim_score"))
    if score is None:
        return False, "Deprecated legacy PPIM is unavailable; no investigative memo was generated."
    if score <= threshold:
        return (
            False,
            f"Deprecated uncalibrated legacy PPIM {score:.2f} does not exceed the "
            f"{threshold:.1f} legacy memo threshold; no investigative memo was generated.",
        )
    return (
        True,
        f"Deprecated uncalibrated legacy PPIM {score:.2f} exceeds the {threshold:.1f} "
        "legacy memo threshold; this remains unvalidated and requires human review.",
    )


def _format_utc(timestamp):
    if timestamp is None:
        return "Publication time unavailable"
    return timestamp.strftime("%d %b %Y · %H:%M:%S UTC")


def _render_forensic_metric(label, value, detail, tone="cyan"):
    """Render a styled card; all dynamic strings are HTML-escaped."""
    safe_tone = tone if tone in {"cyan", "red", "orange", "green", "neutral"} else "neutral"
    st.markdown(
        f"""
        <div class="ml-forensic-metric tone-{safe_tone}">
            <div class="ml-forensic-metric-label">{escape(str(label))}</div>
            <div class="ml-forensic-metric-value">{escape(str(value))}</div>
            <div class="ml-forensic-metric-detail">{escape(str(detail))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _build_forensic_timeline(
    market_data,
    shock_time,
    shock_magnitude,
    shock_z_score,
    rag_result,
):
    """Build price and evidence clocks on independent x-axes."""
    shock_delta_text = (
        f"{shock_magnitude:+.3f}" if shock_magnitude is not None else "unavailable"
    )
    shock_z_text = (
        f"{shock_z_score:+.2f}" if shock_z_score is not None else "unavailable"
    )
    evidence = _classify_rag_evidence(rag_result)
    news_time = None
    lead_hours = None
    if isinstance(rag_result, dict):
        news_time = _to_utc_timestamp(
            rag_result.get("news_timestamp", rag_result.get("evidence_date"))
        )
        lead_hours = _safe_float(rag_result.get("lead_time_hours"))

    if news_time is not None and shock_time is not None:
        lead_hours = (news_time - shock_time).total_seconds() / 3600.0

    if evidence["attribution_eligible"] and (shock_time is None or lead_hours is None):
        evidence.update(
            state="context",
            attribution_eligible=False,
            timing_valid=False,
            timing_issue="invalid_or_missing_shock_timestamp",
            suppression_reason="invalid_or_missing_shock_timestamp",
            status_message=(
                "Shock timing is invalid or missing; the publication is context only "
                "and no information lead is attributed."
            ),
        )

    attribution_eligible = evidence["attribution_eligible"]
    context_only = evidence["state"] == "context"
    invalid_shock_timing = (
        evidence["timing_issue"] == "invalid_or_missing_shock_timestamp"
        or evidence["suppression_reason"] == "invalid_or_missing_shock_timestamp"
        or (evidence["state"] != "no_evidence" and shock_time is None)
    )
    relative_lead_hours = None if invalid_shock_timing else lead_hours
    has_relative_publication = (
        evidence["state"] != "no_evidence" and relative_lead_hours is not None
    )
    lead_text = (
        _lead_statement(relative_lead_hours)
        if attribution_eligible
        else evidence["status_message"]
    )

    if not attribution_eligible:
        gap_color = "#64748b"
        gap_fill = "rgba(100, 116, 139, 0.18)"
    elif relative_lead_hours > (1 / 60):
        gap_color = "#f97316"
        gap_fill = "rgba(249, 115, 22, 0.24)"
    elif relative_lead_hours < -(1 / 60):
        gap_color = "#34d399"
        gap_fill = "rgba(52, 211, 153, 0.22)"
    else:
        gap_color = "#94a3b8"
        gap_fill = "rgba(148, 163, 184, 0.22)"

    if attribution_eligible:
        evidence_panel_title = "Attributed evidence clock · hours relative to market shock"
    elif context_only:
        evidence_panel_title = "Context publication · no information lead attributed"
    else:
        evidence_panel_title = "Evidence clock · no public evidence available"

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.19,
        row_heights=[0.70, 0.30],
        subplot_titles=(
            "Implied probability around the detected shock",
            evidence_panel_title,
        ),
    )

    shock_in_window = False
    evidence_in_window = False
    if not market_data.empty:
        fig.add_trace(
            go.Scatter(
                x=market_data["datetime"],
                y=market_data["price"],
                mode="lines",
                name="Price probability",
                line=dict(color="#38bdf8", width=3),
                fill="tozeroy",
                fillcolor="rgba(56, 189, 248, 0.10)",
                hovertemplate=(
                    "<b>%{y:.1%}</b> implied probability"
                    "<br>%{x|%Y-%m-%d %H:%M:%S} UTC"
                    "<extra>Price probability</extra>"
                ),
            ),
            row=1,
            col=1,
        )

        first_time = market_data["datetime"].iloc[0]
        last_time = market_data["datetime"].iloc[-1]
        if first_time == last_time:
            price_padding = pd.Timedelta(hours=1)
        else:
            price_padding = max((last_time - first_time) * 0.035, pd.Timedelta(minutes=5))
        price_start = first_time - price_padding
        price_end = last_time + price_padding

        fig.add_shape(
            type="line",
            x0=price_start.to_pydatetime(),
            x1=price_end.to_pydatetime(),
            y0=0.5,
            y1=0.5,
            xref="x",
            yref="y",
            line=dict(color="rgba(148, 163, 184, 0.45)", width=1, dash="dot"),
            layer="below",
        )
        fig.add_annotation(
            x=price_end.to_pydatetime(),
            y=0.5,
            xref="x",
            yref="y",
            text="50% threshold",
            showarrow=False,
            xanchor="right",
            yshift=12,
            font=dict(size=10, color="#94a3b8"),
        )

        shock_in_window = (
            shock_time is not None and first_time <= shock_time <= last_time
        )
        if shock_in_window:
            seconds_from_shock = (
                market_data["datetime"] - shock_time
            ).dt.total_seconds().abs()
            shock_index = seconds_from_shock.idxmin()
            shock_price = float(market_data.loc[shock_index, "price"])

            fig.add_shape(
                type="line",
                x0=shock_time.to_pydatetime(),
                x1=shock_time.to_pydatetime(),
                y0=0,
                y1=1,
                xref="x",
                yref="y",
                line=dict(color="rgba(251, 113, 133, 0.72)", width=2, dash="dash"),
                layer="below",
            )
            fig.add_trace(
                go.Scatter(
                    x=[shock_time],
                    y=[shock_price],
                    mode="markers",
                    name="Belief shock",
                    marker=dict(
                        color="#fb7185",
                        size=15,
                        symbol="diamond",
                        line=dict(color="#fff1f2", width=2),
                    ),
                    hovertemplate=(
                        "<b>Belief shock</b>"
                        f"<br>Δ log-odds: {shock_delta_text}"
                        f"<br>z-score: {shock_z_text}"
                        "<br>%{x|%Y-%m-%d %H:%M:%S} UTC<extra></extra>"
                    ),
                ),
                row=1,
                col=1,
            )
            fig.add_annotation(
                x=shock_time.to_pydatetime(),
                y=shock_price,
                xref="x",
                yref="y",
                text=(
                    "<b>Belief shock</b>"
                    f"<br>{shock_delta_text} Δ log-odds · z {shock_z_text}"
                ),
                showarrow=True,
                arrowcolor="#fb7185",
                arrowwidth=1.5,
                arrowhead=2,
                ax=28,
                ay=-64 if shock_price < 0.72 else 64,
                bgcolor="rgba(30, 41, 59, 0.96)",
                bordercolor="rgba(251, 113, 133, 0.62)",
                borderpad=7,
                font=dict(size=11, color="#f8fafc"),
            )

        evidence_in_window = (
            news_time is not None and first_time <= news_time <= last_time
        )
        if evidence_in_window:
            publication_line_color = (
                "rgba(52, 211, 153, 0.68)"
                if attribution_eligible
                else "rgba(148, 163, 184, 0.58)"
            )
            fig.add_shape(
                type="line",
                x0=news_time.to_pydatetime(),
                x1=news_time.to_pydatetime(),
                y0=0,
                y1=1,
                xref="x",
                yref="y",
                line=dict(color=publication_line_color, width=1.5, dash="dot"),
                layer="below",
            )

        fig.update_xaxes(
            range=[price_start.to_pydatetime(), price_end.to_pydatetime()],
            tickformat="%b %d<br>%H:%M",
            title_text="Market observation time (UTC)",
            row=1,
            col=1,
        )
    else:
        fig.add_annotation(
            x=0.5,
            y=0.76,
            xref="paper",
            yref="paper",
            text=(
                "<b>No valid price observations</b>"
                "<br><span style='color:#94a3b8'>The evidence clock remains available below.</span>"
            ),
            showarrow=False,
            align="center",
            font=dict(size=13, color="#e2e8f0"),
        )
        fig.update_xaxes(visible=False, row=1, col=1)

    # The second panel deliberately uses relative hours. Distant news therefore
    # never stretches or compresses the price history in the first panel.
    relative_axis_visible = True
    if has_relative_publication:
        if abs(relative_lead_hours) < (1 / 60):
            lane_min, lane_max = -1.0, 1.0
            fig.add_shape(
                type="line",
                x0=0,
                x1=0,
                y0=0.32,
                y1=0.68,
                xref="x2",
                yref="y2",
                line=dict(color=gap_color, width=8),
            )
            shock_y, evidence_y = 0.34, 0.66
        else:
            lane_span = max(abs(relative_lead_hours), 1.0)
            lane_pad = max(lane_span * 0.16, 0.25)
            lane_min = min(0.0, relative_lead_hours) - lane_pad
            lane_max = max(0.0, relative_lead_hours) + lane_pad
            shock_y = evidence_y = 0.5
            fig.add_shape(
                type="rect",
                x0=min(0.0, relative_lead_hours),
                x1=max(0.0, relative_lead_hours),
                y0=0.41,
                y1=0.59,
                xref="x2",
                yref="y2",
                line=dict(width=0),
                fillcolor=gap_fill,
                layer="below",
            )
            fig.add_trace(
                go.Scatter(
                    x=[0.0, relative_lead_hours],
                    y=[0.5, 0.5],
                    mode="lines",
                    line=dict(
                        color=gap_color,
                        width=5 if attribution_eligible else 3,
                        dash="solid" if attribution_eligible else "dot",
                    ),
                    name=(
                        "Signed information gap"
                        if attribution_eligible
                        else "Context interval (not attributed)"
                    ),
                    hovertemplate=f"{lead_text}<extra></extra>",
                ),
                row=2,
                col=1,
            )

        fig.add_trace(
            go.Scatter(
                x=[0.0],
                y=[shock_y],
                mode="markers",
                name="Market shock time",
                marker=dict(
                    color="#fb7185",
                    size=16,
                    symbol="diamond",
                    line=dict(color="#fff1f2", width=2),
                ),
                hovertemplate="<b>Market shock</b><br>0h reference<extra></extra>",
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=[relative_lead_hours],
                y=[evidence_y],
                mode="markers",
                name=(
                    "Public evidence time"
                    if attribution_eligible
                    else "Context publication"
                ),
                marker=dict(
                    color="#34d399" if attribution_eligible else "#94a3b8",
                    size=15,
                    symbol="circle",
                    line=dict(
                        color="#ecfdf5" if attribution_eligible else "#e2e8f0",
                        width=2,
                    ),
                ),
                hovertemplate=(
                    (
                        "<b>Public evidence</b>"
                        if attribution_eligible
                        else "<b>Context publication</b>"
                    )
                    + f"<br>{relative_lead_hours:+.2f}h relative to shock"
                    + (
                        "<extra></extra>"
                        if attribution_eligible
                        else "<br>No information lead attributed<extra></extra>"
                    )
                ),
            ),
            row=2,
            col=1,
        )
        fig.add_annotation(
            x=0.0,
            y=shock_y,
            xref="x2",
            yref="y2",
            text="Market shock · 0h",
            showarrow=False,
            yshift=-25,
            font=dict(size=10, color="#fecdd3"),
        )
        fig.add_annotation(
            x=relative_lead_hours,
            y=evidence_y,
            xref="x2",
            yref="y2",
            text=(
                f"Public evidence · {relative_lead_hours:+.1f}h"
                if attribution_eligible
                else f"Context publication · {relative_lead_hours:+.1f}h"
            ),
            showarrow=False,
            yshift=25,
            font=dict(
                size=10,
                color="#a7f3d0" if attribution_eligible else "#cbd5e1",
            ),
        )
        fig.add_annotation(
            x=(
                relative_lead_hours / 2.0
                if abs(relative_lead_hours) >= (1 / 60)
                else 0.0
            ),
            y=0.91,
            xref="x2",
            yref="y2",
            text=f"<b>{lead_text}</b>",
            showarrow=False,
            font=dict(size=11, color=gap_color),
        )
    elif context_only:
        # Invalid/missing shock timing cannot be represented as a zero-hour or
        # simultaneous event. Preserve the publication as neutral context only.
        lane_min, lane_max = -1.0, 1.0
        relative_axis_visible = False
        fig.add_trace(
            go.Scatter(
                x=[0.0],
                y=[0.48],
                mode="markers",
                name="Context publication",
                marker=dict(
                    color="#94a3b8",
                    size=15,
                    symbol="circle",
                    line=dict(color="#e2e8f0", width=2),
                ),
                hovertemplate=(
                    "<b>Context publication</b>"
                    "<br>Relative timing unavailable"
                    "<extra></extra>"
                ),
            ),
            row=2,
            col=1,
        )
        fig.add_annotation(
            x=0.0,
            y=0.72,
            xref="x2",
            yref="y2",
            text=(
                "<b>Context publication · relative timing unavailable</b>"
                f"<br>{evidence['status_message']}"
            ),
            showarrow=False,
            align="center",
            font=dict(size=11, color="#94a3b8"),
        )
    else:
        lane_min, lane_max = -1.0, 1.0
        fig.add_trace(
            go.Scatter(
                x=[0.0],
                y=[0.5],
                mode="markers",
                name="Market shock time",
                marker=dict(
                    color="#fb7185",
                    size=16,
                    symbol="diamond",
                    line=dict(color="#fff1f2", width=2),
                ),
                hovertemplate="<b>Market shock</b><br>0h reference<extra></extra>",
            ),
            row=2,
            col=1,
        )
        fig.add_annotation(
            x=0.0,
            y=0.5,
            xref="x2",
            yref="y2",
            text="No timestamped public evidence returned",
            showarrow=False,
            xshift=145,
            font=dict(size=11, color="#94a3b8"),
        )

    fig.update_yaxes(
        range=[0, 1],
        tickformat=".0%",
        dtick=0.25,
        title_text="Implied probability",
        gridcolor="rgba(148, 163, 184, 0.12)",
        zeroline=False,
        row=1,
        col=1,
    )
    fig.update_xaxes(
        range=[lane_min, lane_max],
        title_text=(
            "Hours relative to detected market shock (shock = 0h)"
            if attribution_eligible or evidence["state"] == "no_evidence"
            else "Context publication offset (hours; not attributed)"
        ),
        ticksuffix="h",
        showgrid=True,
        gridcolor="rgba(148, 163, 184, 0.10)",
        zeroline=True,
        zerolinecolor="rgba(251, 113, 133, 0.32)",
        zerolinewidth=1,
        visible=relative_axis_visible,
        row=2,
        col=1,
    )
    fig.update_yaxes(visible=False, range=[0, 1], row=2, col=1)
    fig.update_xaxes(
        showline=True,
        linecolor="rgba(148, 163, 184, 0.22)",
        tickfont=dict(color="#94a3b8", size=10),
        title_font=dict(color="#94a3b8", size=11),
    )

    for title_annotation in fig.layout.annotations[:2]:
        title_annotation.update(
            font=dict(size=13, color="#e2e8f0"),
            x=0.0,
            xanchor="left",
        )

    fig.update_layout(
        height=650,
        paper_bgcolor="#0b1220",
        plot_bgcolor="#0b1220",
        font=dict(family="Inter, sans-serif", color="#cbd5e1"),
        margin=dict(l=62, r=28, t=78, b=48),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="#111827",
            bordercolor="#334155",
            font=dict(color="#f8fafc", size=12),
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.08,
            xanchor="left",
            x=0,
            bgcolor="rgba(15, 23, 42, 0)",
            font=dict(size=10, color="#cbd5e1"),
        ),
        modebar=dict(bgcolor="rgba(15, 23, 42, 0.75)", color="#94a3b8"),
    )

    return fig, {
        "lead_hours": lead_hours,
        "lead_statement": lead_text,
        "news_time": news_time,
        "shock_in_window": shock_in_window,
        "evidence_in_window": evidence_in_window,
        "has_timed_evidence": has_relative_publication,
        "relative_timing_available": relative_lead_hours is not None,
        "attribution_state": evidence["state"],
        "attribution_eligible": attribution_eligible,
        "status_message": evidence["status_message"],
        "evidence_scope": evidence["evidence_scope"],
        "search_strategy": evidence["search_strategy"],
        "timing_valid": evidence["timing_valid"],
        "timing_issue": evidence["timing_issue"],
        "ppim_suppression_reason": evidence["suppression_reason"],
    }

df = load_data()

if menu == "1. System Overview":
    st.header("Phase 1: Multi-Platform Ingestion")
    if df is not None:
        total_rows = len(df)
        total_markets = df['market_slug'].nunique()
        platforms = df['platform'].unique() if 'platform' in df.columns else ["Polymarket", "Kalshi"]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"<div class='metric-box'><div class='metric-label'>Data Points</div><div class='metric-value'>{total_rows:,}</div></div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div class='metric-box'><div class='metric-label'>Tracked Markets</div><div class='metric-value'>{total_markets:,}</div></div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div class='metric-box'><div class='metric-label'>Platforms</div><div class='metric-value'>{len(platforms)}</div></div>", unsafe_allow_html=True)
            
        st.subheader("Raw Ingestion Feed")
        st.dataframe(df.tail(100), use_container_width=True)
    else:
        st.error("No data found! Run the Ingestion Engine first.")

elif menu == "2. Live Market Defense":
    tab1, tab2, tab3, tab4 = st.tabs([
        "Legacy Anomaly Sandbox",
        "Legacy Memo Archive (Not Validation Evidence)",
        "Legacy Inference Agent",
        "Deprecated Risk Heuristic",
    ])

    with tab1:
        st.markdown(
            """
            <section class="ml-forensic-hero">
                <div class="ml-forensic-eyebrow">Phase 2 · Shock Detection / Phase 3 · Evidence Timing</div>
                <div class="ml-forensic-title">Forensic information-lead timeline</div>
                <div class="ml-forensic-copy">
                    Compare the market's probability path with the detected belief shock and the
                    retrieved context publication. This is not proof of the earliest public source.
                    The lower evidence clock uses relative hours,
                    so distant publication dates never distort the market-price view.
                </div>
            </section>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div class="ml-visual-key">
                <span class="ml-key-item"><span class="ml-key-mark price"></span>Price probability</span>
                <span class="ml-key-item"><span class="ml-key-mark shock"></span>Belief shock</span>
                <span class="ml-key-item"><span class="ml-key-mark evidence"></span>Exact public evidence</span>
                <span class="ml-key-item"><span class="ml-key-mark gap"></span>Eligible signed lead</span>
                <span class="ml-key-item"><span class="ml-key-mark context"></span>Context publication</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if df is None:
            st.error(
                "The market tick feed is unavailable. Restore the ingestion data to render "
                "the forensic timeline."
            )
        else:
            cache_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "demo_data",
                "cache_anomalies.parquet",
            )
            if not os.path.exists(cache_path):
                st.warning("Anomaly cache not built yet. Please run master_daemon.py.")
            else:
                try:
                    anomalies = pd.read_parquet(cache_path)
                except Exception as exc:
                    anomalies = pd.DataFrame()
                    st.error(f"The anomaly cache could not be read: {exc}")

                required_anomaly_columns = {
                    "market_slug",
                    "question",
                    "timestamp",
                    "belief_shock",
                }
                if anomalies.empty:
                    st.info("No flagged anomalies are available in the current cache.")
                elif not required_anomaly_columns.issubset(anomalies.columns):
                    st.error("The anomaly cache is missing fields required by this view.")
                else:
                    top_markets = (
                        anomalies["market_slug"]
                        .dropna()
                        .astype(str)
                        .drop_duplicates()
                        .head(10)
                        .tolist()
                    )
                    question_lookup = (
                        anomalies.dropna(subset=["market_slug"])
                        .drop_duplicates("market_slug")
                        .set_index("market_slug")["question"]
                        .astype(str)
                        .to_dict()
                    )

                    if not top_markets:
                        st.info("No market identifiers are available in the anomaly cache.")
                    else:
                        selected_market = st.selectbox(
                            "Flagged market to investigate",
                            top_markets,
                            format_func=lambda slug: question_lookup.get(slug, slug),
                            help=(
                                "The ten highest-ranked cached anomalies are available here. "
                                "Select one to reconstruct its market and evidence clocks."
                            ),
                        )

                        selected_anomalies = anomalies[
                            anomalies["market_slug"].astype(str) == selected_market
                        ]
                        anomaly_row = selected_anomalies.iloc[0]
                        market_question = str(anomaly_row.get("question", selected_market))
                        st.subheader(market_question)
                        st.caption(f"Market identifier · {selected_market}")

                        if "market_slug" in df.columns:
                            raw_market_data = df[
                                df["market_slug"].astype(str) == selected_market
                            ].copy()
                        else:
                            raw_market_data = pd.DataFrame()
                        market_data, data_quality = _prepare_market_timeline(raw_market_data)

                        shock_time = _to_utc_timestamp(anomaly_row.get("timestamp"))
                        shock_magnitude = _safe_float(anomaly_row.get("belief_shock"))
                        shock_z_score = _safe_float(anomaly_row.get("z_score"))
                        plot_shock_magnitude = (
                            shock_magnitude if shock_magnitude is not None else 0.0
                        )

                        st.markdown("##### Reconstructing public-evidence timing")
                        rag_result = None
                        rag_error = None
                        with st.spinner(
                            "Searching Google News RSS and indexing candidate evidence..."
                        ):
                            try:
                                rag = get_rag_agent()
                                row_dict = {
                                    "question": market_question,
                                    "slug": selected_market,
                                    "shock_timestamp": anomaly_row.get("timestamp"),
                                    "close_time": anomaly_row.get("close_time"),
                                    "shock_magnitude": plot_shock_magnitude,
                                }
                                candidate_result = rag.fetch_and_score(row_dict)
                                if isinstance(candidate_result, dict):
                                    rag_result = candidate_result
                            except Exception as exc:
                                rag_error = str(exc)

                        fig, timeline = _build_forensic_timeline(
                            market_data=market_data,
                            shock_time=shock_time,
                            shock_magnitude=shock_magnitude,
                            shock_z_score=shock_z_score,
                            rag_result=rag_result,
                        )

                        lead_hours = timeline["lead_hours"]
                        attribution_state = timeline["attribution_state"]
                        if timeline["attribution_eligible"]:
                            timing_value = _lead_metric_value(lead_hours)
                            timing_detail = timeline["lead_statement"]
                            if lead_hours is None or abs(lead_hours) < (1 / 60):
                                lead_tone = "neutral"
                            elif lead_hours > 0:
                                lead_tone = "orange"
                            else:
                                lead_tone = "green"
                        elif attribution_state == "context":
                            timing_value = "Context only"
                            timing_detail = timeline["status_message"]
                            lead_tone = "neutral"
                        else:
                            timing_value = "No evidence"
                            timing_detail = "No timestamped public evidence is available."
                            lead_tone = "neutral"

                        if shock_magnitude is None:
                            shock_value = "Unavailable"
                        else:
                            shock_value = f"{shock_magnitude:+.3f} Δ log-odds"
                        shock_detail = (
                            f"Signed belief update · z-score {shock_z_score:+.2f}"
                            if shock_z_score is not None
                            else "Signed belief update · z-score unavailable"
                        )

                        ppim_score = (
                            _safe_float(rag_result.get("ppim_score"))
                            if isinstance(rag_result, dict)
                            else None
                        )
                        if timeline["attribution_eligible"]:
                            ppim_value = (
                                f"{ppim_score:+.2f}"
                                if ppim_score is not None
                                else "Unavailable"
                            )
                            ppim_detail = (
                                "Signed shock × evidence interval; attribution requires exact, "
                                "valid timing."
                            )
                            ppim_tone = "cyan"
                        elif attribution_state == "context":
                            ppim_value = "Suppressed"
                            ppim_detail = timeline["status_message"]
                            ppim_tone = "neutral"
                        else:
                            ppim_value = "Unavailable"
                            ppim_detail = "Requires timestamped public evidence."
                            ppim_tone = "neutral"

                        metric_col1, metric_col2, metric_col3 = st.columns(3)
                        with metric_col1:
                            _render_forensic_metric(
                                "Evidence timing",
                                timing_value,
                                timing_detail,
                                lead_tone,
                            )
                        with metric_col2:
                            _render_forensic_metric(
                                "Belief shock",
                                shock_value,
                                shock_detail,
                                "red",
                            )
                        with metric_col3:
                            _render_forensic_metric(
                                "Legacy PPIM heuristic",
                                ppim_value,
                                ppim_detail,
                                ppim_tone,
                            )

                        st.plotly_chart(
                            fig,
                            width="stretch",
                            theme=None,
                            config={
                                "displaylogo": False,
                                "responsive": True,
                                "scrollZoom": False,
                                "modeBarButtonsToRemove": ["lasso2d", "select2d"],
                                "toImageButtonOptions": {
                                    "format": "png",
                                    "filename": "marketleak-forensic-timeline",
                                    "scale": 2,
                                },
                            },
                        )

                        quality_parts = [
                            f"{data_quality['valid_rows']:,} usable observations",
                            f"{data_quality['duplicate_rows']:,} duplicates collapsed",
                            f"{data_quality['invalid_rows']:,} invalid rows omitted",
                        ]
                        st.caption("Timeline normalized to UTC · " + " · ".join(quality_parts))

                        if market_data.empty:
                            st.warning(
                                "No valid in-range probabilities are available for this market. "
                                "The lower evidence clock is still rendered."
                            )
                        elif shock_time is None:
                            st.warning(
                                "The anomaly has no valid shock timestamp, so the price-panel "
                                "shock marker cannot be positioned."
                            )
                        elif not timeline["shock_in_window"]:
                            st.warning(
                                "The available price feed does not cover the recorded shock time. "
                                "The probability series is shown without forcing the off-window "
                                "shock onto its axis."
                            )

                        news_time = timeline["news_time"]
                        if (
                            news_time is not None
                            and not market_data.empty
                            and not timeline["evidence_in_window"]
                        ):
                            st.caption(
                                "The evidence publication is outside the displayed price window; "
                                "its true timing is preserved on the independent evidence clock."
                            )

                        with st.container(border=True):
                            st.markdown("#### Retrieved public evidence")
                            if attribution_state == "no_evidence":
                                st.info(
                                    "No timestamped evidence was returned. The market timeline "
                                    "remains visible so the detected shock can still be reviewed."
                                )
                                if rag_error:
                                    st.caption(f"Evidence retrieval error · {rag_error}")
                            else:
                                if timeline["attribution_eligible"]:
                                    st.success(
                                        "Legacy exact-query heuristic conditions were met. This is "
                                        "unvalidated context, not information-lead attribution."
                                    )
                                else:
                                    st.info(timeline["status_message"])

                                evidence_col, metadata_col = st.columns([1.5, 1])
                                with evidence_col:
                                    st.caption(
                                        "PUBLIC EVIDENCE (UTC)"
                                        if timeline["attribution_eligible"]
                                        else "CONTEXT PUBLICATION (UTC)"
                                    )
                                    st.write(_format_utc(news_time))
                                with metadata_col:
                                    st.caption("SEARCH STRATEGY")
                                    st.write(timeline["search_strategy"])
                                    st.caption("EVIDENCE SCOPE")
                                    st.write(timeline["evidence_scope"])

                                timing_col, suppression_col = st.columns(2)
                                with timing_col:
                                    st.caption("TIMING VALIDATION")
                                    st.write(
                                        "Valid"
                                        if timeline["timing_valid"]
                                        else "Not valid for attribution"
                                    )
                                    if timeline["timing_issue"]:
                                        st.caption("TIMING ISSUE")
                                        st.write(str(timeline["timing_issue"]))
                                with suppression_col:
                                    st.caption("LEGACY PPIM HEURISTIC (DEPRECATED / UNCALIBRATED)")
                                    st.write(
                                        "Computed (unvalidated)"
                                        if timeline["attribution_eligible"]
                                        else "Suppressed"
                                    )
                                    if timeline["ppim_suppression_reason"]:
                                        st.caption("SUPPRESSION REASON")
                                        st.write(
                                            str(timeline["ppim_suppression_reason"])
                                        )

                                st.markdown("**Evidence excerpt**")
                                evidence_text = rag_result.get("evidence_text")
                                if evidence_text:
                                    st.write(str(evidence_text))
                                else:
                                    st.caption("No evidence excerpt was supplied.")

                                evidence_url = rag_result.get("evidence_url")
                                if evidence_url:
                                    evidence_url = str(evidence_url).strip()
                                    if evidence_url.lower().startswith(("https://", "http://")):
                                        st.link_button(
                                            "Open source article ↗",
                                            evidence_url,
                                            type="primary",
                                        )
                                    else:
                                        st.caption("The evidence source URL is not a valid web link.")

    with tab2:
        st.header("Legacy Investigative Memo Archive — Not Validation Evidence")
        st.warning(
            "Archived reports are legacy generated artifacts. They are not validated labels, "
            "regulatory SARs, proof of misconduct, or evidence of system effectiveness."
        )

        reports_dir = "reports"
        if os.path.exists(reports_dir):
            files = glob.glob(os.path.join(reports_dir, "*.md"))
            if not files:
                st.warning("No reports generated yet.")
            else:
                selected_file = st.selectbox("Select a legacy investigative review memo:", files)
                report_view = read_report_for_product(Path(selected_file))
                st.markdown("---")
                if report_view["truth_firewall_verified"] and report_view["content_available"]:
                    st.markdown(report_view["content"])
                else:
                    st.error(
                        "Artifact quarantined. Its original content is not displayed because it did "
                        "not pass the v2 provenance, integrity, and truth-firewall checks."
                    )
        else:
            st.error("Reports directory not found.")

    with tab3:
        st.header("Legacy Inference Bot (Unvalidated)")
        st.markdown(
            "Query the legacy heuristic for human triage. It cannot establish or exclude "
            "information leakage or misconduct."
        )
        legacy_reports_enabled = (
            os.getenv("MARKETLEAK_ENABLE_LEGACY_REPORTS", "").strip() == "1"
        )
        legacy_report_human_review = st.checkbox(
            "I acknowledge this is an unvalidated legacy memo and will perform human review",
            value=False,
            disabled=not legacy_reports_enabled,
            help=(
                "Memo generation also requires the explicit "
                "MARKETLEAK_ENABLE_LEGACY_REPORTS=1 environment opt-in."
            ),
        )

        if "messages" not in st.session_state:
            st.session_state.messages = []

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if prompt := st.chat_input("Enter a market name to analyze (e.g., 'Will Gold hit $4050?'):"):
            st.chat_message("user").markdown(prompt)
            st.session_state.messages.append({"role": "user", "content": prompt})

            with st.chat_message("assistant"):
                st.markdown(f"**Agent initialized.** Searching database for market matching: `{prompt}`...")

                if df is not None:
                    matches = df[df['question'].str.contains(prompt, case=False, na=False)]
                    if matches.empty:
                        st.error("No matching market found in database.")
                        st.session_state.messages.append({"role": "assistant", "content": "No matching market found in database."})
                    else:
                        target_market = matches['market_slug'].iloc[0]
                        target_question = matches['question'].iloc[0]
                        st.success(f"Matched Market: **{target_question}**")

                        st.markdown("Running Belief Shock Detection (Rolling Z-Score)...")
                        anomalies = run_anomaly_detection()
                        if anomalies is not None and not anomalies.empty:
                            if target_market in anomalies['market_slug'].values:
                                anomaly_row = anomalies[anomalies['market_slug'] == target_market].iloc[0]
                                st.warning(f"**Belief Shock Detected!** Magnitude: {anomaly_row['belief_shock']:.2f}")

                                st.markdown("Fetching live news via Google News RSS and analyzing timestamps...")
                                rag = RAGAgent()
                                row_dict = {
                                    "question": anomaly_row['question'],
                                    "slug": anomaly_row['market_slug'],
                                    "shock_timestamp": anomaly_row['timestamp'],
                                    "close_time": anomaly_row['close_time'],
                                    "shock_magnitude": anomaly_row['belief_shock']
                                }
                                rag_result = rag.fetch_and_score(row_dict)

                                if rag_result:
                                    sar_eligible, sar_reason = _sar_eligibility(rag_result)
                                    if not sar_eligible:
                                        context_message = (
                                            "Evidence retained as non-actionable context. "
                                            f"{sar_reason} No legacy investigative memo was generated."
                                        )
                                        st.info(context_message)
                                        st.session_state.messages.append(
                                            {"role": "assistant", "content": context_message}
                                        )
                                    elif not legacy_report_human_review:
                                        context_message = (
                                            "Legacy memo generation also requires the human-review "
                                            "acknowledgment checkbox. No memo was generated."
                                        )
                                        st.info(context_message)
                                        st.session_state.messages.append(
                                            {"role": "assistant", "content": context_message}
                                        )
                                    else:
                                        st.markdown("Generating Legacy Investigative Review Memo...")
                                        synthesis = SynthesisAgent()
                                        try:
                                            generated_report, generated_path = synthesis.generate_sar(rag_result)
                                            report_view = read_report_for_product(
                                                Path(generated_path), generated_report
                                            )
                                        except Exception:
                                            report_view = read_report_for_product(
                                                Path("withheld.md"), ""
                                            )

                                        if (
                                            report_view["truth_firewall_verified"]
                                            and report_view["content_available"]
                                        ):
                                            st.markdown("---")
                                            st.markdown(report_view["content"])
                                            st.session_state.messages.append(
                                                {
                                                    "role": "assistant",
                                                    "content": report_view["content"],
                                                }
                                            )
                                        else:
                                            withheld_message = (
                                                "Generated memo was withheld by the v2 provenance, "
                                                "integrity, and truth-firewall checks."
                                            )
                                            st.error(withheld_message)
                                            st.session_state.messages.append(
                                                {"role": "assistant", "content": withheld_message}
                                            )
                                else:
                                    st.warning("No RAG evidence found for this market.")
                            else:
                                st.info(
                                    "The legacy detector did not flag this market. This does not establish "
                                    "normal trading, absence of misconduct, or scorability under the validated v2 detector."
                                )
                else:
                    st.error("Data source unavailable.")

    with tab4:
        st.header("Deprecated Uncalibrated Legacy Leak-Risk Heuristic")
        st.warning(
            "This hand-weighted legacy heuristic is not a trained or calibrated probability and "
            "must not be used to conclude information leakage or safety."
        )

        if df is not None:
            target_question = st.selectbox("Select a Market to Forecast:", df['question'].unique())
            if st.button("Run Deprecated Legacy Risk Heuristic"):
                with st.spinner("Analyzing event topology with Gemini 2.5..."):
                    predictor = PredictAgent()
                    features = predictor._extract_features(target_question)
                    prior = predictor.risk_model.forecast_risk("ui_test", "ui_event", features)

                    col1, col2 = st.columns([1, 2])
                    with col1:
                        # Color gradient based on risk
                        risk_color = "#ef4444" if prior.score > 0.6 else ("#eab308" if prior.score > 0.3 else "#22c55e")
                        st.markdown(f"<div class='metric-box'><div class='metric-label'>Legacy Leak-Risk Heuristic (Uncalibrated)</div><div class='metric-value' style='background: {risk_color}; -webkit-background-clip: text;'>{prior.score:.3f}</div></div>", unsafe_allow_html=True)
                    with col2:
                        st.subheader("Legacy Heuristic Contributions (Uncalibrated)")
                        for driver, val in sorted(prior.drivers.items(), key=lambda x: abs(x[1]), reverse=True)[:5]:
                            st.markdown(f"- **{driver}**: `{val:.3f}`")

                    st.subheader("Extracted Event Topology (JSON)")
                    st.json(features.to_dict())
        else:
            st.error("No data available.")

elif menu == "3. Contextual Wallet Graph (Unvalidated)":
    st.header("Phase 5: Contextual Wallet Graph (Unvalidated)")
    st.warning(
        "Graph proximity and wallet clustering are unvalidated context only. They do not establish "
        "identity, common control, event access, intent, or misconduct."
    )
    st.markdown("Legacy DuckDB proxy-wallet clustering based on transaction co-occurrence and funding context.")
    
    if st.button("Run Clustering Engine"):
        cache_path = "demo_data/cache_clusters.json"
        if not os.path.exists(cache_path):
            legacy_exists = os.path.exists("demo_data/cache_clusters.pkl")
            suffix = " Legacy pickle cache ignored." if legacy_exists else ""
            st.warning(f"Cluster cache not built yet. Please run master_daemon.py.{suffix}")
            st.stop()

        try:
            proxies = load_cluster_map(cache_path)
        except SafePersistenceError:
            st.error("Proxy map is unavailable because its cache is corrupt or unsupported.")
            st.stop()
            
        num_proxies = len(set(proxies.values()))
        num_wallets = len(proxies)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"<div class='metric-box'><div class='metric-label'>Tracked Wallets</div><div class='metric-value'>{num_wallets:,}</div></div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div class='metric-box'><div class='metric-label'>Proxy Clusters</div><div class='metric-value'>{num_proxies:,}</div></div>", unsafe_allow_html=True)

        if num_proxies > 0:
            st.info(
                f"The legacy heuristic grouped {num_wallets} wallets into {num_proxies} "
                "candidate proxy clusters. These are not verified entities or identities."
            )
            df_proxies = pd.DataFrame(list(proxies.items()), columns=["Wallet Address", "Proxy Entity ID"])
            st.dataframe(df_proxies, use_container_width=True)
        else:
            st.info("No proxy clusters found. The graph repository (`demo_data/graph.json`) currently has no transactional overlap. Run the blockchain indexer to ingest more data.")
