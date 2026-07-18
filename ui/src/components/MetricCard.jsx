import { formatNumber } from "../api";

export default function MetricCard({ label, value, tone = "cyan", detail }) {
  return (
    <div className={`metric-card tone-${tone}`}>
      <span className="metric-label">{label}</span>
      <strong>{typeof value === "number" ? formatNumber(value) : value}</strong>
      {detail ? <small>{detail}</small> : null}
    </div>
  );
}
