import { useEffect, useMemo, useRef, useState } from "react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Scatter,
} from "recharts";
import ForceGraph2D from "react-force-graph-2d";
import { api, compactText, formatNumber, formatTime } from "../api";
import MetricCard from "../components/MetricCard.jsx";
import { ErrorState, LoadingState } from "../components/StateBlock.jsx";

function buildSeries(feed, marketSlug) {
  return (feed || [])
    .filter((row) => row.market_slug === marketSlug)
    .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
    .map((row) => ({
      timestamp: Number(row.timestamp),
      price: Number(row.price),
      label: formatTime(row.timestamp),
    }));
}

function getResultSlug(result) {
  return result?.market_slug || result?.anomaly?.market_slug || "";
}

function getPpimScore(result) {
  const rawValue = result?.ppim_score ?? result?.rag_result?.ppim_score;
  if (rawValue === null || rawValue === undefined) return null;
  const value = Number(rawValue);
  return Number.isFinite(value) ? value : null;
}

function ContextGraph({ graphData }) {
  const containerRef = useRef(null);
  const [width, setWidth] = useState(640);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return undefined;

    const updateWidth = () => setWidth(Math.max(260, Math.floor(container.clientWidth)));
    updateWidth();
    const observer = new ResizeObserver(updateWidth);
    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  return (
    <div className="context-graph" ref={containerRef} role="img" aria-label="Contextual relationship graph; connections do not establish access, identity, intent, or misconduct">
      <ForceGraph2D
        graphData={graphData}
        backgroundColor="transparent"
        nodeLabel="label"
        nodeColor={(node) => node.kind === "Wallet" ? "#ff9b77" : node.kind === "Transaction" ? "#35f7b2" : "#8aa0a6"}
        linkColor={() => "rgba(138, 160, 166, 0.3)"}
        width={width}
        height={310}
      />
    </div>
  );
}

export default function Sandbox() {
  const [overview, setOverview] = useState(null);
  const [anomalyPayload, setAnomalyPayload] = useState(null);
  const [v2Quality, setV2Quality] = useState(null);
  const [selectedSlug, setSelectedSlug] = useState("");
  const [analysis, setAnalysis] = useState(null);
  const [sweepResults, setSweepResults] = useState([]);
  const [sweeping, setSweeping] = useState(false);
  const [sweepError, setSweepError] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    Promise.all([api.overview(), api.anomalies(), api.dataQuality()])
      .then(([overviewData, anomalyData, qualityData]) => {
        setOverview(overviewData);
        setAnomalyPayload(anomalyData);
        setV2Quality(qualityData);
        const first = anomalyData.anomalies?.[0]?.market_slug || "";
        setSelectedSlug(first);
      })
      .catch((err) => setError(err.message));
  }, []);

  const anomalies = anomalyPayload?.anomalies || [];
  const hasSweepResults = sweepResults.length > 0;
  const selectedSweepResult = hasSweepResults
    ? sweepResults.find((result) => getResultSlug(result) === selectedSlug) || sweepResults[0]
    : null;
  const selected = selectedSweepResult?.anomaly || anomalies.find((row) => row.market_slug === selectedSlug) || anomalies[0];
  const selectedAnalysis = selectedSweepResult || analysis;
  const series = useMemo(() => buildSeries(overview?.feed, selected?.market_slug), [overview, selected]);
  const highScoreResults = useMemo(
    () => sweepResults.filter((result) => (getPpimScore(result) ?? -Infinity) > 1.0),
    [sweepResults],
  );
  const lowerScoreResults = useMemo(
    () => sweepResults.filter((result) => (getPpimScore(result) ?? -Infinity) <= 1.0),
    [sweepResults],
  );

  function runSweep() {
    setSweeping(true);
    setSweepError("");
    setAnalysis(null);
    api
      .analyzeSweep()
      .then((payload) => {
        const results = Array.isArray(payload.results) ? payload.results : [];
        setSweepResults(results);
        const firstSlug = getResultSlug(results[0]);
        if (firstSlug) {
          setSelectedSlug(firstSlug);
        }
      })
      .catch((err) => {
        setSweepResults([]);
        setSweepError(err.message);
      })
      .finally(() => setSweeping(false));
  }

  function selectSweepResult(result) {
    const slug = getResultSlug(result);
    if (slug) {
      setSelectedSlug(slug);
    }
    setAnalysis(result);
  }

  function renderSweepGroup(title, results, tone) {
    return (
      <div className={`sweep-group ${tone}`}>
        <div className="sweep-group-heading">
          <span>{title}</span>
          <strong>{results.length}</strong>
        </div>
        <div className="signal-stack">
          {results.map((result) => {
            const slug = getResultSlug(result);
            const active = slug === selected?.market_slug;
            return (
              <button
                type="button"
                className={active ? `signal-row sweep-row ${tone} active` : `signal-row sweep-row ${tone}`}
                key={`${slug}-${result.anomaly?.timestamp || result.lead_time_hours || "sweep"}`}
                onClick={() => selectSweepResult(result)}
              >
                <span>{compactText(result.question || result.anomaly?.question, 86)}</span>
                <strong>Legacy PPIM {formatNumber(getPpimScore(result), { maximumFractionDigits: 2 })}</strong>
                <small>
                  Deprecated forecast {formatNumber(result.leak_risk_forecast, { style: "percent", maximumFractionDigits: 1 })}
                </small>
              </button>
            );
          })}
        </div>
      </div>
    );
  }

  if (error) {
    return <ErrorState message={error} />;
  }

  if (!overview || !anomalyPayload || !v2Quality) {
    return <LoadingState label="Loading activity signals and validation state..." />;
  }

  const v2GatePassed = v2Quality.data_quality?.gate_passed === true;
  const v2Blocked = v2Quality.status === "blocked_by_data_quality" || v2Quality.data_quality?.gate_passed === false;

  return (
    <section className="page-grid sandbox-grid">
      <div className="page-header">
        <p className="eyebrow">Activity Review Sandbox</p>
        <h2>Inspect signals without overclaiming.</h2>
        <p>
          Select an anomalous window, inspect its price trace, and review legacy model and graph context.
          These outputs prioritize human review; they do not establish fraud, identity, access, or intent.
        </p>
      </div>

      <div className={`validation-callout sandbox-validation ${v2Blocked ? "blocked" : v2GatePassed ? "pass" : "unknown"}`} role="status">
        <div>
          <p className="eyebrow">V2 assessment state</p>
          <h3>
            {v2Blocked
              ? "Blocked and unscorable — not exonerated"
              : v2GatePassed
                ? "Lineage gate passed"
                : "Validation state unavailable"}
          </h3>
          <p>
            {v2Blocked
              ? "The current source lacks required lineage, so v2 produced no assessment. Legacy PPIM and forecast fields below are deprecated demo context, not evidence."
              : v2GatePassed
                ? "Assessment may proceed, but all outputs still require human review and are not proof of fraud."
                : "The validation gate was not assessed or did not return a recognized state. No conclusion can be drawn from this absence."}
          </p>
        </div>
        <span className={`status-pill ${v2Blocked ? "fail" : v2GatePassed ? "pass" : "unknown"}`}>
          {v2Blocked ? "v2 blocked" : v2GatePassed ? "gate passed" : "not assessed"}
        </span>
      </div>

      <div className="metrics-grid">
        <MetricCard label="Activity windows" value={anomalyPayload.count} tone="green" detail="detector review queue" />
        <MetricCard label="Selected Price" value={formatNumber(selected?.price, { style: "percent", maximumFractionDigits: 1 })} tone="cyan" detail="shock tick" />
        <MetricCard label="Diagnostic z-score" value={formatNumber(selected?.z_score, { maximumFractionDigits: 2 })} tone="violet" detail="not a fraud probability" />
        <MetricCard label="Price shock" value={formatNumber(selected?.belief_shock, { maximumFractionDigits: 2 })} tone="amber" detail="log-odds movement" />
      </div>

      <div className="glass-panel anomaly-list">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Review queue</p>
            <h3>Activity windows</h3>
          </div>
        </div>
        <button type="button" className="primary-action sweep-action" disabled={sweeping || !anomalies.length} onClick={runSweep}>
          {sweeping ? "Running legacy sweep..." : "Run legacy context sweep"}
        </button>
        {sweepError ? <p className="warning-text">{sweepError}</p> : null}

        {hasSweepResults ? (
          <div className="sweep-results">
            {renderSweepGroup("Legacy high-score bucket (unvalidated)", highScoreResults, "leak")}
            {renderSweepGroup("Legacy lower-score bucket (not exonerated)", lowerScoreResults, "public")}
          </div>
        ) : (
          <div className="queue-with-boundary">
            {v2Blocked ? (
              <div className="empty-evidence-state compact">
                <strong>No v2 assessment is available</strong>
                <p>The queue contains legacy detector windows. Unscorable does not mean cleared or benign.</p>
              </div>
            ) : null}
            <div className="signal-stack">
              {anomalies.slice(0, 12).map((row) => (
                <button
                  type="button"
                  className={row.market_slug === selected?.market_slug ? "signal-row active" : "signal-row"}
                  key={`${row.market_slug}-${row.timestamp}`}
                  onClick={() => {
                    setSelectedSlug(row.market_slug);
                    setAnalysis(null);
                  }}
                >
                  <span>{compactText(row.question, 86)}</span>
                  <strong>{formatNumber(row.shock_magnitude, { maximumFractionDigits: 2 })}</strong>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="glass-panel chart-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Price Shock Trace</p>
            <h3>{compactText(selected?.question, 108)}</h3>
          </div>
        </div>

        <div className="chart-frame">
          <ResponsiveContainer width="100%" height={360}>
            <ComposedChart data={series} margin={{ top: 18, right: 24, left: 0, bottom: 8 }}>
              <defs>
                <linearGradient id="priceGlow" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#35f7b2" stopOpacity={0.34} />
                  <stop offset="100%" stopColor="#35f7b2" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="rgba(142, 255, 223, 0.12)" vertical={false} />
              <XAxis
                dataKey="timestamp"
                type="number"
                domain={["dataMin", "dataMax"]}
                tickFormatter={formatTime}
                stroke="#8aa0a6"
                minTickGap={36}
              />
              <YAxis
                domain={[0, 1]}
                tickFormatter={(value) => `${Math.round(value * 100)}%`}
                stroke="#8aa0a6"
                width={44}
              />
              <Tooltip
                contentStyle={{
                  background: "rgba(7, 16, 20, 0.92)",
                  border: "1px solid rgba(49, 247, 178, 0.35)",
                  borderRadius: 12,
                  color: "#eafdf8",
                }}
                formatter={(value) => [formatNumber(value, { style: "percent", maximumFractionDigits: 2 }), "Price"]}
                labelFormatter={formatTime}
              />
              <Area type="stepAfter" dataKey="price" stroke="none" fill="url(#priceGlow)" />
              <Line type="stepAfter" dataKey="price" stroke="#35f7b2" strokeWidth={3} dot={false} activeDot={{ r: 5 }} />
              <Scatter dataKey="price" fill="#ff4d6d" r={2} />
              {selected?.timestamp ? (
                <ReferenceLine x={Number(selected.timestamp)} stroke="#ff4d6d" strokeDasharray="5 5" label="Shock" />
              ) : null}
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {selectedAnalysis ? (
          <div className={selectedAnalysis.error ? "analysis-card error-state" : "analysis-card"}>
            {selectedAnalysis.error ? (
              <p>{selectedAnalysis.error}</p>
            ) : (
              <>
                <div className="analysis-metrics">
                  <MetricCard label="Timing gap (context)" value={`${formatNumber(selectedAnalysis.lead_time_hours, { maximumFractionDigits: 2 })}h`} tone="green" detail="depends on source coverage" />
                  <MetricCard label="Legacy PPIM" value={formatNumber(getPpimScore(selectedAnalysis), { maximumFractionDigits: 2 })} tone="cyan" detail="deprecated; not a probability" />
                  <MetricCard label="Legacy forecast" value={formatNumber(selectedAnalysis.leak_risk_forecast, { style: "percent", maximumFractionDigits: 1 })} tone="amber" detail="unvalidated demo output" />
                  <MetricCard label="Graph paths" value={selectedAnalysis.graph_enrichment?.path_count || 0} tone="violet" detail="context, not actor proof" />
                </div>
                <div className="analysis-boundary">
                  <strong>Human-review context only.</strong>
                  <span>These legacy values do not establish fraud, access, intent, or actor identity.</span>
                </div>
                {selectedAnalysis.graph_data && selectedAnalysis.graph_data.nodes && selectedAnalysis.graph_data.nodes.length > 0 && (
                  <div className="glass-panel graph-panel">
                    <div className="graph-heading">
                      <p className="eyebrow">Contextual relationship graph</p>
                      <small>Links do not establish common control, access, intent, or misconduct.</small>
                    </div>
                    <ContextGraph graphData={selectedAnalysis.graph_data} />
                  </div>
                )}
                <p className="evidence-line">{selectedAnalysis.rag_result?.evidence_text}</p>
                {selectedAnalysis.rag_result?.evidence_url ? (
                  <a href={selectedAnalysis.rag_result.evidence_url} target="_blank" rel="noreferrer">
                    Context source
                  </a>
                ) : null}
                {selectedAnalysis.synthesis_error ? <p className="warning-text">{selectedAnalysis.synthesis_error}</p> : null}
                {selectedAnalysis.report ? (
                  <div className="legacy-report-block">
                    <span className="status-pill unknown">Legacy synthesis artifact · not validation evidence</span>
                    <pre className="report-preview">{selectedAnalysis.report}</pre>
                  </div>
                ) : null}
              </>
            )}
          </div>
        ) : null}
      </div>
    </section>
  );
}
