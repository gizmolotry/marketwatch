import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api, compactText, formatNumber, formatTime } from "../api";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import MetricCard from "../components/MetricCard.jsx";
import { ErrorState, LoadingState } from "../components/StateBlock.jsx";

export default function Home() {
  const [overview, setOverview] = useState(null);
  const [health, setHealth] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    Promise.all([api.overview(), api.health()])
      .then(([overviewPayload, healthPayload]) => {
        setOverview(overviewPayload);
        setHealth(healthPayload);
      })
      .catch((err) => setError(err.message));
  }, []);

  const feed = useMemo(() => {
    const rows = overview?.feed || [];
    return [...rows].sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0)).slice(0, 60);
  }, [overview]);

  const volumeData = useMemo(() => {
    if (!overview?.feed_histogram) return [];
    return overview.feed_histogram.map(row => ({
      timestamp: Number(row.timestamp),
      count: Number(row.count)
    })).sort((a, b) => a.timestamp - b.timestamp);
  }, [overview]);

  if (error) {
    return <ErrorState message={error} />;
  }

  if (!overview) {
    return <LoadingState />;
  }

  const metrics = overview.metrics || {};
  const quality = overview.data_quality || {};
  const gatePassed = quality.gate_passed === true;
  const gateBlocked = quality.gate_passed === false;
  const platformText = metrics.platforms?.length ? metrics.platforms.join(" / ") : "Normalized feed";

  return (
    <section className="page-grid">
      <div className="page-header">
        <p className="eyebrow">Phase 1 Command Surface</p>
        <h2>Prediction-market surveillance in one console.</h2>
        <p>
          Observed ingestion, market breadth, and raw tick telemetry from the normalized parquet feed.
        </p>
      </div>

      <div className={`validation-banner ${gateBlocked ? "blocked" : gatePassed ? "pass" : "unknown"}`} role="status">
        <div>
          <p className="eyebrow">Validation state · API {health?.status === "ok" ? "healthy" : "unavailable"}</p>
          <h3>
            {gateBlocked
              ? "V2 assessment blocked by the lineage gate"
              : gatePassed
                ? "V2 lineage gate passed"
                : "Validation status unavailable"}
          </h3>
          <p>
            {gateBlocked
              ? "Current legacy snapshots are unscorable for validation. That is not an exoneration, and legacy scores are not evidence of fraud."
              : gatePassed
                ? "The source met the current quality gate; outputs still require human review and are not proof of fraud."
                : "No data-quality gate result was returned. This feed has not been assessed and must not be treated as validated."}
          </p>
        </div>
        <Link className="secondary-action" to="/validation">Review validation</Link>
      </div>

      <div className="metrics-grid">
        <MetricCard label="Data Points" value={metrics.data_points} tone="green" detail="normalized ticks" />
        <MetricCard label="Tracked Markets" value={metrics.tracked_markets} tone="cyan" detail={platformText} />
        <MetricCard label="Platforms" value={metrics.platform_count || "—"} tone="violet" detail="source coverage" />
        <MetricCard label="Latest Tick" value={formatTime(metrics.latest_timestamp)} tone="amber" detail="feed timestamp" />
      </div>

      <div className="glass-panel chart-panel" style={{ marginBottom: "1.5rem" }}>
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Global Ingestion Volume</p>
            <h3>Ingestion Volume Histogram</h3>
          </div>
        </div>
        <div className="chart-frame" style={{ padding: "0 1rem 1rem 1rem" }}>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={volumeData} margin={{ top: 18, right: 24, left: 0, bottom: 8 }}>
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
                dataKey="count" 
                type="number" 
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
                formatter={(value) => [formatNumber(value), "Ticks Ingested"]}
                labelFormatter={formatTime}
              />
              <Bar dataKey="count" fill="#35f7b2" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="glass-panel wide-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Raw Ingestion Feed</p>
            <h3>Recent Tick Stream</h3>
          </div>
          <span className="chip">{formatNumber(feed.length)} rows shown</span>
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Market</th>
                <th>Question</th>
                <th>Price</th>
              </tr>
            </thead>
            <tbody>
              {feed.map((row, index) => (
                <tr key={`${row.market_slug}-${row.timestamp}-${index}`}>
                  <td>{formatTime(row.timestamp)}</td>
                  <td className="mono-cell">{compactText(row.market_slug, 38)}</td>
                  <td>{compactText(row.question, 88)}</td>
                  <td className="price-cell">{formatNumber(row.price, { style: "percent", maximumFractionDigits: 1 })}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
