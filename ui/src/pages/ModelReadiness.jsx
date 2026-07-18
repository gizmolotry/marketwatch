import { useEffect, useMemo, useState } from "react";
import { api, formatNumber } from "../api";
import { ErrorState, LoadingState } from "../components/StateBlock.jsx";

const MODALITIES = [
  ["market_state", "Market microstructure"],
  ["public_evidence", "Point-in-time public evidence"],
  ["onchain_settlement", "On-chain settlement"],
];

const POLICIES = [
  ["ood", "Out-of-distribution policy"],
  ["abstention", "Abstention policy"],
  ["coverage", "Coverage policy"],
];

function titleCase(value) {
  return String(value || "unavailable").replaceAll("_", " ");
}

function nestedValue(payload, ...keys) {
  for (const key of keys) {
    const value = payload?.[key];
    if (value !== undefined && value !== null) return value;
  }
  return null;
}

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function explicitState(value) {
  const candidate = String(
    typeof value === "string" ? value : value?.status || value?.state || value?.approval_status || "",
  ).toLowerCase();
  if (["approved", "ready", "available", "observed", "active", "enabled", "complete", "passed"].includes(candidate)) {
    return "ready";
  }
  if (["blocked", "rejected", "invalid", "disabled", "failed", "insufficient", "not_approved"].includes(candidate)) {
    return "blocked";
  }
  return "unknown";
}

function stateLabel(state) {
  return state === "ready" ? "ready" : state === "blocked" ? "blocked" : "unavailable";
}

function statusDetail(value, fallback) {
  if (typeof value === "string") return value;
  const item = asObject(value);
  return item.reason || item.detail || item.message || fallback;
}

function isApproved(bundle) {
  const item = asObject(bundle);
  return item.approved === true || String(item.approval_status || item.status || "").toLowerCase() === "approved";
}

function isExplicitlyBlocked(value) {
  const item = asObject(value);
  return item.sufficient === false || item.passed === false || item.enabled === false || explicitState(value) === "blocked";
}

function labelsAreSufficient(value) {
  const item = asObject(value);
  return item.sufficient === true || item.passed === true || String(item.status || "").toLowerCase() === "adequate";
}

function modalityState(value) {
  const item = asObject(value);
  if (item.available === true || item.observed === true) return "ready";
  if (isExplicitlyBlocked(value)) return "blocked";
  // An explicit false availability flag tells us that the modality is absent,
  // not that a substitute source makes it usable.
  if (item.available === false || item.observed === false) return "unknown";
  return explicitState(value);
}

function normalizeReadiness(readinessPayload, modelPayload) {
  const readiness = asObject(readinessPayload.readiness || readinessPayload);
  const model = asObject(modelPayload.model_status || modelPayload.model || modelPayload);
  const bundle = nestedValue(readiness, "bundle", "serving_bundle", "model_bundle") || nestedValue(model, "bundle", "serving_bundle", "model_bundle") || {};
  const labels = nestedValue(readiness, "label_sufficiency", "labels", "label_status") || nestedValue(model, "label_sufficiency", "labels") || {};
  const modalities = nestedValue(readiness, "modality_availability", "modalities", "availability") || nestedValue(model, "modality_availability", "modalities") || {};
  const policies = nestedValue(readiness, "policies", "policy") || nestedValue(model, "policies", "policy") || {};

  return {
    bundle,
    labels,
    modalities: asObject(modalities),
    policies: asObject(policies),
    readinessEndpointAvailable: readinessPayload._phase15_endpoint_available === true,
    statusEndpointAvailable: modelPayload._phase15_endpoint_available === true,
    readinessSource: readinessPayload.readiness_source || (readinessPayload._phase15_endpoint_available ? "v3" : "v2 fallback"),
    effectivenessUnknown: readiness.effectiveness_unknown !== false && model.effectiveness_unknown !== false,
  };
}

function StatusPill({ state }) {
  return <span className={`status-pill readiness-${state}`}>{stateLabel(state)}</span>;
}

function ReadinessRow({ label, state, detail }) {
  return (
    <div className="readiness-row">
      <div>
        <span className="metric-label">{label}</span>
        <strong>{stateLabel(state)}</strong>
      </div>
      <div>
        <StatusPill state={state} />
        <small>{detail}</small>
      </div>
    </div>
  );
}

export default function ModelReadiness() {
  const [payloads, setPayloads] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;
    Promise.all([api.modelReadiness(), api.modelStatus()])
      .then(([readiness, modelStatus]) => {
        if (active) setPayloads({ readiness, modelStatus });
      })
      .catch((requestError) => {
        if (active) setError(requestError.message);
      });
    return () => {
      active = false;
    };
  }, []);

  const view = useMemo(
    () => (payloads ? normalizeReadiness(payloads.readiness, payloads.modelStatus) : null),
    [payloads],
  );

  if (error) {
    return <ErrorState message={error} />;
  }

  if (!view) {
    return <LoadingState label="Checking model-bundle readiness gates..." />;
  }

  const bundleState = isApproved(view.bundle) ? "ready" : isExplicitlyBlocked(view.bundle) ? "blocked" : "unknown";
  const labelsState = labelsAreSufficient(view.labels) ? "ready" : isExplicitlyBlocked(view.labels) ? "blocked" : "unknown";
  const bundleUsable = bundleState === "ready" && labelsState === "ready";
  const overallState = bundleUsable ? "ready" : bundleState === "blocked" || labelsState === "blocked" ? "blocked" : "unknown";
  const labeledCount = nestedValue(view.labels, "labeled_case_count", "count", "labeled_count");

  return (
    <section className="page-grid readiness-grid">
      <div className="page-header readiness-header">
        <p className="eyebrow">Phase 15 · Model Readiness</p>
        <h2>Readiness before use.</h2>
        <p>
          This page reports whether an immutable model bundle and its supporting gates are available. It does not render a model score or a conclusion about any market participant.
        </p>
      </div>

      <div className={`readiness-callout ${overallState}`} role="status">
        <div>
          <p className="eyebrow">Serving boundary</p>
          <h3>{overallState === "ready" ? "Approved bundle and label gate reported" : overallState === "blocked" ? "Bundle use is blocked" : "Model readiness unavailable"}</h3>
          <p>
            A model bundle cannot be used until it is approved and every readiness gate is explicitly satisfied. Missing endpoint fields are treated as unavailable, never as a pass.
          </p>
        </div>
        <StatusPill state={overallState} />
      </div>

      <div className="readiness-banner" role="note">
        <strong>Effectiveness is unknown.</strong>
        <span>Readiness artifacts, coverage, and model status do not establish predictive performance.</span>
      </div>

      <div className="readiness-summary-grid">
        <article className="glass-panel readiness-summary-card">
          <span className="metric-label">Bundle</span>
          <strong>{stateLabel(bundleState)}</strong>
          <small>{statusDetail(view.bundle, "No approved bundle metadata was supplied.")}</small>
        </article>
        <article className="glass-panel readiness-summary-card">
          <span className="metric-label">Label sufficiency</span>
          <strong>{stateLabel(labelsState)}</strong>
          <small>{labeledCount === null || labeledCount === undefined ? "No usable label-count evidence was supplied." : `${formatNumber(labeledCount)} labeled cases reported; sufficiency is independently gated.`}</small>
        </article>
        <article className="glass-panel readiness-summary-card">
          <span className="metric-label">Readiness source</span>
          <strong>{view.readinessEndpointAvailable && view.statusEndpointAvailable ? "v3" : "fallback"}</strong>
          <small>{view.readinessSource}. Missing Phase 15 endpoint data remains unavailable.</small>
        </article>
      </div>

      <div className="glass-panel readiness-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Modality availability</p>
            <h3>Observed inputs and explicit gaps</h3>
          </div>
        </div>
        <div className="readiness-stack">
          {MODALITIES.map(([key, label]) => {
            const value = view.modalities[key] || view.modalities[key.replaceAll("_", "-")] || {};
            const state = modalityState(value);
            return <ReadinessRow key={key} label={label} state={state} detail={statusDetail(value, "No Phase 15 availability record was supplied.")} />;
          })}
        </div>
      </div>

      <div className="glass-panel readiness-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Restraint policy</p>
            <h3>OOD, abstention, and coverage gates</h3>
          </div>
        </div>
        <div className="readiness-stack">
          {POLICIES.map(([key, label]) => {
            const value = view.policies[key] || view.policies[`${key}_policy`] || {};
            const state = value?.enabled === true ? "ready" : isExplicitlyBlocked(value) ? "blocked" : explicitState(value);
            return <ReadinessRow key={key} label={label} state={state} detail={statusDetail(value, "No approved policy artifact was supplied.")} />;
          })}
        </div>
      </div>

      <div className="glass-panel readiness-panel readiness-boundary-panel">
        <p className="eyebrow">Operational boundary</p>
        <h3>Use requires an approved immutable bundle.</h3>
        <p>
          A readiness result is only a record of bundle, label, modality, and policy availability. It does not substitute for prospective evaluation, human review, or point-in-time evidence controls.
        </p>
      </div>
    </section>
  );
}
