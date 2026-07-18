import { useEffect, useMemo, useState } from "react";
import { api, formatNumber } from "../api";
import MetricCard from "../components/MetricCard.jsx";
import { ErrorState, LoadingState } from "../components/StateBlock.jsx";

const TARGET_COPY = {
  A: {
    title: "Abnormal market activity",
    detail: "Detects unusual price or trading behavior for human review. It does not identify intent.",
  },
  B: {
    title: "Point-in-time public explanation",
    detail: "Checks whether time-bounded public information may explain the activity. Unknown coverage is not absence of news.",
  },
  C: {
    title: "Actor-specific access context",
    detail: "Evaluates independently sourced actor and access claims when actor-attributed data exists.",
  },
};

function titleCase(value) {
  return String(value || "unknown").replaceAll("_", " ");
}

function Availability({ label, available, status, detail }) {
  return (
    <div className="availability-row">
      <div>
        <span className="metric-label">{label}</span>
        <strong>{available ? "Available" : "Not available"}</strong>
      </div>
      <div>
        <span className={`status-pill ${available ? "pass" : "unknown"}`}>{titleCase(status)}</span>
        <small>{detail}</small>
      </div>
    </div>
  );
}

export default function Validation() {
  const [payloads, setPayloads] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    Promise.all([api.capabilities(), api.dataQuality(), api.assessments(), api.validation()])
      .then(([capabilities, quality, assessments, validation]) => {
        setPayloads({ capabilities, quality, assessments, validation });
      })
      .catch((requestError) => setError(requestError.message));
  }, []);

  const targetEntries = useMemo(() => {
    const targets = payloads?.capabilities?.targets || {};
    return ["A", "B", "C"].map((key) => [key, targets[key]]);
  }, [payloads]);

  if (error) {
    return <ErrorState message={error} />;
  }

  if (!payloads) {
    return <LoadingState label="Checking evidence and validation gates..." />;
  }

  const { capabilities, quality, assessments, validation } = payloads;
  const gate = quality.data_quality || {};
  const validationState = validation.validation || {};
  const actor = quality.actor_availability || {};
  const sources = quality.source_availability || {};
  const runStatus = quality.status || assessments.run_status || validation.run_status || "unknown";
  const gatePassed = gate.gate_passed === true;
  const blocked = runStatus === "blocked_by_data_quality" || gate.gate_passed === false;
  const validationStateKnown = blocked || gatePassed;
  const assessmentRows = assessments.assessments || [];

  return (
    <section className="page-grid validation-grid">
      <div className="page-header">
        <p className="eyebrow">Validation &amp; Claim Boundary</p>
        <h2>Evidence before escalation.</h2>
        <p>
          The validation-first pipeline separates activity, public explanation, and actor context, then blocks
          assessment when source lineage is insufficient.
        </p>
      </div>

      <div className={`validation-callout ${blocked ? "blocked" : gatePassed ? "pass" : "unknown"}`} role="status">
        <div>
          <p className="eyebrow">Current run</p>
          <h3>
            {blocked
              ? "Blocked and unscorable — not exonerated"
              : gatePassed
                ? titleCase(runStatus)
                : "Validation state unavailable"}
          </h3>
          <p>
            {blocked
              ? "The lineage gate stopped v2 assessment. No score or empty result may be interpreted as evidence that activity is benign."
              : gatePassed
                ? "The quality gate passed. Any resulting assessment still requires human review."
                : "The data-quality gate was not assessed or did not return a recognized state. No conclusion can be drawn."}
          </p>
        </div>
        <span className={`status-pill ${blocked ? "fail" : gatePassed ? "pass" : "unknown"}`}>
          {validationStateKnown ? titleCase(runStatus) : "not assessed"}
        </span>
      </div>

      <div className="metrics-grid">
        <MetricCard label="Rows received" value={gate.received} tone="cyan" detail={gate.source || "current source"} />
        <MetricCard label="Rows normalized" value={gate.normalized} tone="green" detail={`${formatNumber(gate.duplicates || 0)} duplicates`} />
        <MetricCard label="Assessments" value={assessments.count || 0} tone="violet" detail={blocked ? "blocked by lineage gate" : gatePassed ? "review candidates" : "validation state unavailable"} />
        <MetricCard label="Validated" value={validationState.validated === true ? "Yes" : validationState.validated === false ? "No" : "Unknown"} tone={validationState.validated === true ? "green" : "amber"} detail="effectiveness claim" />
      </div>

      <div className="glass-panel validation-panel lineage-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Data lineage gate</p>
            <h3>{gatePassed ? "Passed" : blocked ? "Failed" : "Not assessed"}</h3>
          </div>
          <span className={`status-pill ${gatePassed ? "pass" : blocked ? "fail" : "unknown"}`}>
            {gatePassed ? "lineage accepted" : blocked ? "assessment blocked" : "state unavailable"}
          </span>
        </div>
        {gate.gate_reasons?.length ? (
          <ul className="reason-list">
            {gate.gate_reasons.map((reason) => (
              <li key={reason}>{reason}</li>
            ))}
          </ul>
        ) : (
          <p className="muted-copy">
            {validationStateKnown ? "No gate failure reasons were returned." : "No data-quality gate result was returned; no conclusion can be drawn."}
          </p>
        )}
        <p className="boundary-note">
          Legacy price snapshots do not provide immutable raw-artifact, source, or parser lineage and cannot
          establish trade or actor availability.
        </p>
      </div>

      <div className="glass-panel validation-panel target-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Independent assessment targets</p>
            <h3>A / B / C are not a single fraud score</h3>
          </div>
        </div>
        <div className="target-grid">
          {targetEntries.map(([key, apiName]) => (
            <article className="target-card" key={key}>
              <span className="target-letter">{key}</span>
              <div>
                <h3>{TARGET_COPY[key].title}</h3>
                <p>{TARGET_COPY[key].detail}</p>
                <small>{apiName ? titleCase(apiName) : "Capability unavailable"}</small>
              </div>
            </article>
          ))}
        </div>
      </div>

      <div className="glass-panel validation-panel availability-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Evidence availability</p>
            <h3>What this run can actually observe</h3>
          </div>
        </div>
        <Availability
          label="Actor-attributed evidence"
          available={Boolean(actor.available)}
          status={actor.visibility}
          detail={actor.derived_from_price_snapshots ? "Derived from price snapshots" : "Not derived from price snapshots"}
        />
        <Availability
          label="Point-in-time public sources"
          available={Boolean(sources.point_in_time_public_sources_available)}
          status={sources.coverage_status}
          detail="Unknown coverage cannot support a no-public-explanation claim"
        />
      </div>

      <div className="glass-panel validation-panel claims-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Effectiveness &amp; claim limits</p>
            <h3>Human review required</h3>
          </div>
        </div>
        <dl className="claim-list">
          <div><dt>Effectiveness</dt><dd>{validationState.effectiveness_unknown || capabilities.effectiveness_unknown ? "Unknown" : "Measured"}</dd></div>
          <div><dt>Calibration</dt><dd>{validationState.calibration_available ? "Available" : "Not available"}</dd></div>
          <div><dt>Prospective shadow runs</dt><dd>{formatNumber(validationState.prospective_shadow_runs_completed || 0)}</dd></div>
          <div><dt>Labeled cases</dt><dd>{formatNumber(validationState.labeled_case_count || 0)}</dd></div>
          <div><dt>Claim scope</dt><dd>{titleCase(validationState.claim_scope)}</dd></div>
        </dl>
        <div className="claim-boundary">
          <strong>Not proof of fraud.</strong>
          <span>Scores are not probabilities, automatic fraud finding is disabled, and effectiveness is unknown.</span>
        </div>
      </div>

      <div className="glass-panel validation-panel assessment-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">V2 assessments</p>
            <h3>{formatNumber(assessmentRows.length)} review candidates</h3>
          </div>
        </div>
        {assessmentRows.length ? (
          <div className="assessment-stack">
            {assessmentRows.map((item) => (
              <article className="assessment-row" key={item.assessment_uid}>
                <strong>{item.market_uid}</strong>
                <span>{titleCase(item.activity?.status)}</span>
                <small>Human review required; not proof of fraud.</small>
              </article>
            ))}
          </div>
        ) : (
          <div className="empty-evidence-state">
            <strong>{blocked ? "Unscorable — not exonerated" : gatePassed ? "No review candidates" : "Validation not assessed"}</strong>
            <p>
              {blocked
                ? "There are zero v2 assessments because the data-quality gate blocked evaluation, not because the markets were cleared."
                : gatePassed
                  ? "No actionable activity was produced for this run. This is not a finding of innocence or absence of risk."
                  : "No recognized data-quality gate state was returned. No conclusion can be drawn from the absence of assessments."}
            </p>
          </div>
        )}
      </div>
    </section>
  );
}
