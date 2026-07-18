import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { ErrorState, LoadingState } from "../components/StateBlock.jsx";

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function titleCase(value) {
  return String(value || "unavailable").replaceAll("_", " ");
}

function statusState(value) {
  const text = String(value || "").toLowerCase();
  if (text === "available_precomputed_context") return "context-observed";
  if (["blocked", "missing", "required", "unavailable", "watchlist_not_configured"].includes(text)) return "context-blocked";
  return "context-unknown";
}

function safeText(value, fallback = "Not supplied") {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function Timestamp({ label, value }) {
  return (
    <div className="bitcoin-timestamp">
      <span>{label}</span>
      <strong>{safeText(value, "Not recorded")}</strong>
    </div>
  );
}

function NeutralStatus({ status }) {
  return <span className={`status-pill ${statusState(status)}`}>{titleCase(status)}</span>;
}

function UidList({ label, values }) {
  const items = Array.isArray(values) ? values.filter((value) => typeof value === "string" && value.trim()) : [];
  return (
    <div className="bitcoin-target-list">
      <span>{label}</span>
      {items.length ? (
        <ul>{items.map((value) => <li key={value}>{value}</li>)}</ul>
      ) : (
        <p>None attached.</p>
      )}
    </div>
  );
}

function ContextAttachment({ context }) {
  const item = asObject(context);
  return (
    <article className="bitcoin-fact-row">
      <div>
        <span className="metric-label">Redacted context UID</span>
        <strong>{safeText(item.address_uid, "No redacted context UID")}</strong>
      </div>
      <div className="bitcoin-fact-meta">
        <span>Attachment state</span>
        <NeutralStatus status={item.attachment_status} />
      </div>
      <UidList label="Polygon target UIDs" values={item.polygon_address_uids} />
      <UidList label="Market target UIDs" values={item.market_address_uids} />
      <UidList label="Event target UIDs" values={item.event_uids} />
    </article>
  );
}

function normalize(payload) {
  const source = asObject(payload?.bitcoin_context || payload);
  return {
    endpointAvailable: payload?._phase15_endpoint_available === true,
    status: source.status || "unavailable",
    reason: source.reason,
    snapshotUid: source.snapshot_uid,
    decisionAsOf: source.decision_as_of,
    publishedAt: source.published_at,
    contexts: Array.isArray(source.contexts) ? source.contexts : [],
  };
}

export default function BitcoinContext() {
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;
    api
      .bitcoinContext()
      .then((response) => {
        if (active) setPayload(response);
      })
      .catch((requestError) => {
        if (active) setError(requestError.message);
      });
    return () => {
      active = false;
    };
  }, []);

  const view = useMemo(() => (payload ? normalize(payload) : null), [payload]);

  if (error) return <ErrorState message={error} />;
  if (!view) return <LoadingState label="Checking read-only Bitcoin context availability..." />;

  const available = view.status === "available_precomputed_context";

  return (
    <section className="page-grid bitcoin-context-grid">
      <div className="page-header bitcoin-context-header">
        <p className="eyebrow">Phase 15 · Bitcoin Context</p>
        <h2>Precomputed public-chain context, bounded by policy.</h2>
        <p>
          This read-only view displays a policy-redacted snapshot. It cannot search addresses, identify owners, calculate a wallet score, or create a market link.
        </p>
      </div>

      <div className={`bitcoin-context-callout ${available ? "context-observed" : "context-blocked"}`} role="status">
        <div>
          <p className="eyebrow">Context state</p>
          <h3>{available ? "Precomputed context available" : "Bitcoin context unavailable"}</h3>
          <p>
            {available
              ? "The server returned a fixed snapshot whose attachments were approved before its decision cutoff. It remains contextual only and establishes no ownership, access, intent, or conclusion about any person."
              : safeText(view.reason, "No precomputed Bitcoin context is assumed.")}
          </p>
        </div>
        <NeutralStatus status={view.status} />
      </div>

      <div className="bitcoin-context-banner" role="note">
        <strong>Not an attribution or risk tool.</strong>
        <span>Polymarket settlement corroboration is Polygon `OrderFilled` only. Bitcoin context cannot replace that evidence.</span>
      </div>

      <article className="glass-panel bitcoin-context-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Published snapshot</p>
            <h3>Point-in-time context boundary</h3>
          </div>
          <NeutralStatus status={view.status} />
        </div>
        <dl className="bitcoin-detail-list">
          <div><dt>Snapshot UID</dt><dd>{safeText(view.snapshotUid, "No published snapshot")}</dd></div>
          <div><dt>Endpoint</dt><dd>{view.endpointAvailable ? "Read-only v3 endpoint" : "Unavailable; no fallback context"}</dd></div>
        </dl>
        <div className="bitcoin-timestamp-grid">
          <Timestamp label="Decision as of" value={view.decisionAsOf} />
          <Timestamp label="Published at" value={view.publishedAt} />
        </div>
      </article>

      <article className="glass-panel bitcoin-context-panel bitcoin-facts-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Context attachments</p>
            <h3>{view.contexts.length ? `${view.contexts.length} redacted context record${view.contexts.length === 1 ? "" : "s"}` : "No context attachments available"}</h3>
          </div>
        </div>
        {view.contexts.length ? (
          <div className="bitcoin-fact-stack">
            {view.contexts.map((context, index) => <ContextAttachment key={asObject(context).address_uid || index} context={context} />)}
          </div>
        ) : (
          <p className="muted-copy">An unavailable or unconfigured snapshot remains unavailable. The UI will not perform a live lookup or infer an attachment.</p>
        )}
      </article>
    </section>
  );
}
