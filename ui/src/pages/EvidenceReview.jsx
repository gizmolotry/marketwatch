import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { ErrorState, LoadingState } from "../components/StateBlock.jsx";

const unavailable = "unavailable";

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asList(value) {
  return Array.isArray(value) ? value : [];
}

function firstPresent(object, keys) {
  const source = asObject(object);
  return keys.map((key) => source[key]).find((value) => value !== null && value !== undefined && value !== "");
}

function readSafeText(value, fallback = unavailable) {
  if (typeof value !== "string" && typeof value !== "number") return fallback;
  const text = String(value).trim();
  if (!text) return fallback;

  return text
    .replace(/0x[a-f0-9]{12,}/gi, "redacted identifier")
    .replace(/(?:bc1|[13])[a-z0-9]{20,}/gi, "redacted identifier")
    .replace(/\b(fraud|insider|identity|intent|actor|wallet|address)\b/gi, "restricted term");
}

function label(value) {
  return readSafeText(value).replaceAll("_", " ");
}

function statusTone(value) {
  const text = String(value || "").toLowerCase();
  if (["observed", "available", "complete", "recorded", "covered"].includes(text)) return "review-observed";
  if (["partial", "blocked", "withheld", "hold", "restricted", "requires_review", "abstain_insufficient_evidence"].includes(text)) return "review-restrained";
  return "review-unavailable";
}

function StatusPill({ value }) {
  const status = readSafeText(value);
  return <span className={`status-pill ${statusTone(status)}`}>{label(status)}</span>;
}

function BoundaryValue({ value }) {
  if (typeof value === "boolean") return value ? "true" : "false";
  return unavailable;
}

function DetailList({ items }) {
  return (
    <dl className="review-detail-list">
      {items.map(({ label: itemLabel, value }) => (
        <div key={itemLabel}>
          <dt>{itemLabel}</dt>
          <dd>{readSafeText(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

function stageStatus(value) {
  return firstPresent(value, ["overall_status", "status", "state", "coverage_status", "decision"]) || unavailable;
}

function StageOne({ trigger }) {
  const item = asObject(trigger);
  const status = Object.keys(item).length ? "recorded" : unavailable;
  return (
    <article className="glass-panel review-stage-card">
      <div className="review-stage-heading">
        <span className="review-stage-index">01</span>
        <div>
          <p className="eyebrow">Observed activity trigger</p>
          <h3>Recorded activity</h3>
        </div>
        <StatusPill value={status} />
      </div>
      <DetailList
        items={[
          { label: "Window starts", value: firstPresent(item, ["window_starts_at", "window_start", "started_at"]) },
          { label: "Window ends", value: firstPresent(item, ["window_ends_at", "window_end", "ended_at"]) },
          { label: "Price open", value: firstPresent(item, ["price_open", "open_price"]) },
          { label: "Price close", value: firstPresent(item, ["price_close", "close_price"]) },
          { label: "Price change", value: firstPresent(item, ["price_change", "change"]) },
          { label: "Observations", value: firstPresent(item, ["observation_count", "observations_count"]) },
          { label: "Fills", value: firstPresent(item, ["fill_count", "fills_count"]) },
          { label: "Book snapshots", value: firstPresent(item, ["orderbook_snapshot_count", "book_snapshot_count"]) },
          { label: "Trade notional", value: firstPresent(item, ["trade_notional", "notional"]) },
          { label: "Raw lineage entries", value: asList(item.raw_artifact_uids).length || unavailable },
        ]}
      />
    </article>
  );
}

function StageTwo({ checks }) {
  const items = asList(checks).map(asObject);
  const status = items.length ? "recorded" : unavailable;
  return (
    <article className="glass-panel review-stage-card">
      <div className="review-stage-heading">
        <span className="review-stage-index">02</span>
        <div>
          <p className="eyebrow">Ordinary explanation checks</p>
          <h3>{items.length ? "Recorded checks" : "No checks supplied"}</h3>
        </div>
        <StatusPill value={status} />
      </div>
      {items.length ? (
        <div className="review-check-list">
          {items.map((check, index) => (
            <div className="review-check-row" key={`${firstPresent(check, ["check_uid", "check_kind", "kind"]) || "check"}-${index}`}>
              <div>
                <span className="metric-label">Check kind</span>
                <strong>{label(firstPresent(check, ["check_kind", "kind", "type"]))}</strong>
              </div>
              <StatusPill value={stageStatus(check)} />
              <span>{readSafeText(firstPresent(check, ["summary", "reason", "checked_at", "as_of", "recorded_at"]))}</span>
            </div>
          ))}
        </div>
      ) : (
        <p className="muted-copy">No ordinary-explanation record was supplied. This stage remains unavailable.</p>
      )}
    </article>
  );
}

function StageThree({ coverage, routing }) {
  const coverageItem = asObject(coverage);
  const routingItem = asObject(routing);
  const coverageState = stageStatus(coverageItem);
  const routingState = firstPresent(routingItem, ["decision", "status", "state", "coverage_status"]) || unavailable;
  return (
    <article className="glass-panel review-stage-card">
      <div className="review-stage-heading">
        <span className="review-stage-index">03</span>
        <div>
          <p className="eyebrow">Coverage and restraint decision</p>
          <h3>Policy boundary</h3>
        </div>
        <StatusPill value={routingState !== unavailable ? routingState : coverageState} />
      </div>
      <div className="review-decision-grid">
        <div>
          <span className="metric-label">Coverage</span>
          <strong>{label(coverageState)}</strong>
          <small>{readSafeText(firstPresent(coverageItem, ["raw_receipt_count", "as_of", "decision_as_of", "recorded_at"]))}</small>
        </div>
        <div>
          <span className="metric-label">Routing</span>
          <strong>{label(routingState)}</strong>
          <small>{readSafeText(firstPresent(routingItem, ["decision", "route", "state"]))}</small>
        </div>
      </div>
      <DetailList
        items={[
          { label: "Raw receipts", value: firstPresent(coverageItem, ["raw_receipt_count", "receipt_count"]) },
          { label: "Coverage gaps", value: asList(coverageItem.gap_reasons).map((reason) => label(reason)).join(", ") || unavailable },
        ]}
      />
      {asList(coverageItem.modality_states).length ? (
        <div className="review-check-list">
          {asList(coverageItem.modality_states).map((state, index) => {
            const modality = asObject(state);
            return (
              <div className="review-check-row" key={`${firstPresent(modality, ["modality"]) || "modality"}-${index}`}>
                <div>
                  <span className="metric-label">Modality</span>
                  <strong>{label(firstPresent(modality, ["modality", "kind"]))}</strong>
                </div>
                <StatusPill value={stageStatus(modality)} />
                <span>{readSafeText(firstPresent(modality, ["reason", "summary"]))}</span>
              </div>
            );
          })}
        </div>
      ) : null}
    </article>
  );
}

function StageFour({ entries }) {
  const items = asList(entries).map(asObject);
  return (
    <article className="glass-panel review-stage-card review-ledger-panel">
      <div className="review-stage-heading">
        <span className="review-stage-index">04</span>
        <div>
          <p className="eyebrow">Redacted evidence ledger</p>
          <h3>{items.length ? "Recorded evidence references" : "No ledger entries supplied"}</h3>
        </div>
        <StatusPill value={items.length ? "recorded" : unavailable} />
      </div>
      {items.length ? (
        <div className="review-ledger-list">
          {items.map((entry, index) => (
            <div className="review-ledger-row" key={`${firstPresent(entry, ["ledger_uid", "evidence_uid", "record_uid"]) || "entry"}-${index}`}>
              <div>
                <span className="metric-label">Modality</span>
                <strong>{label(firstPresent(entry, ["modality", "record_kind", "evidence_kind", "kind", "type"]))}</strong>
              </div>
              <div>
                <span className="metric-label">Reference</span>
                <strong>{firstPresent(entry, ["evidence_uid", "ledger_uid", "record_uid"]) ? "redacted evidence reference" : unavailable}</strong>
              </div>
              <div>
                <span className="metric-label">Event time</span>
                <strong>{readSafeText(firstPresent(entry, ["event_time", "recorded_at", "observed_at", "as_of", "created_at"]))}</strong>
              </div>
              <div>
                <span className="metric-label">Available at</span>
                <strong>{readSafeText(firstPresent(entry, ["available_at"]))}</strong>
              </div>
              <div>
                <span className="metric-label">Reliability</span>
                <strong>{label(firstPresent(entry, ["reliability_tier"]))}</strong>
              </div>
              <div>
                <span className="metric-label">Raw lineage</span>
                <strong>{firstPresent(entry, ["source_uid"]) && firstPresent(entry, ["raw_artifact_uid"]) ? "redacted recorded artifact" : unavailable}</strong>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="muted-copy">No redacted evidence ledger was supplied. This stage remains unavailable.</p>
      )}
    </article>
  );
}

function isReviewCase(value) {
  const item = asObject(value);
  return ["case_kind", "trigger", "ordinary_checks", "coverage", "routing", "evidence_ledger"].some((key) => Object.hasOwn(item, key));
}

function normalize(payload) {
  const source = asObject(payload);
  const listEnvelope = asObject(source.review_cases);
  const detailEnvelope = asObject(source.review_case);
  const rawCases = Array.isArray(source.review_cases)
    ? source.review_cases
    : Array.isArray(source.cases)
      ? source.cases
      : Array.isArray(listEnvelope.cases)
        ? listEnvelope.cases
        : Array.isArray(detailEnvelope.cases)
          ? detailEnvelope.cases
          : isReviewCase(source.case)
            ? [source.case]
            : isReviewCase(detailEnvelope.case)
              ? [detailEnvelope.case]
              : isReviewCase(detailEnvelope)
                ? [detailEnvelope]
                : [];
  const cases = rawCases.filter(isReviewCase).map(asObject);

  return {
    status: firstPresent(source, ["status"]) ?? firstPresent(listEnvelope, ["status"]) ?? firstPresent(detailEnvelope, ["status"]) ?? unavailable,
    cases,
    notProof: firstPresent(source, ["not_proof_of_fraud"]) ?? firstPresent(listEnvelope, ["not_proof_of_fraud"]) ?? firstPresent(detailEnvelope, ["not_proof_of_fraud"]),
    effectiveness: firstPresent(source, ["effectiveness_unknown"]) ?? firstPresent(listEnvelope, ["effectiveness_unknown"]) ?? firstPresent(detailEnvelope, ["effectiveness_unknown"]),
  };
}

function CaseFunnel({ reviewCase, index }) {
  const item = asObject(reviewCase);
  return (
    <section className="review-case-funnel" aria-labelledby={`review-case-${index}`}>
      <div className="review-case-heading">
        <div>
          <p className="eyebrow">Review case {index + 1}</p>
          <h3 id={`review-case-${index}`}>{label(item.case_kind)}</h3>
        </div>
        <div className="review-case-meta">
          <span>As of</span>
          <strong>{readSafeText(item.as_of)}</strong>
        </div>
        <div className="review-case-meta">
          <span>Venue</span>
          <strong>{readSafeText(item.venue)}</strong>
        </div>
        <div className="review-case-meta">
          <span>Market UID</span>
          <strong>{readSafeText(firstPresent(item, ["market_uid", "market"]))}</strong>
        </div>
        <div className="review-case-meta">
          <span>Outcome UID</span>
          <strong>{readSafeText(item.outcome_uid)}</strong>
        </div>
        <div className="review-case-meta">
          <span>Question</span>
          <strong>{readSafeText(item.question)}</strong>
        </div>
      </div>
      <div className="review-stage-grid">
        <StageOne trigger={item.trigger} />
        <StageTwo checks={item.ordinary_checks} />
        <StageThree coverage={item.coverage} routing={item.routing} />
        <StageFour entries={item.evidence_ledger} />
      </div>
    </section>
  );
}

export default function EvidenceReview() {
  const [payload, setPayload] = useState(null);
  const [hasRequestError, setHasRequestError] = useState(false);

  useEffect(() => {
    let active = true;
    api
      .reviewCases()
      .then((response) => {
        if (active) setPayload(response);
      })
      .catch(() => {
        if (active) setHasRequestError(true);
      });
    return () => {
      active = false;
    };
  }, []);

  const view = useMemo(() => (payload ? normalize(payload) : null), [payload]);

  if (hasRequestError) return <ErrorState message="The review-case service is unavailable. No case data is shown." />;
  if (!view) return <LoadingState label="Loading read-only evidence review cases..." />;

  return (
    <section className="page-grid evidence-review-grid">
      <div className="page-header evidence-review-header">
        <p className="eyebrow">V3 · Evidence Review</p>
        <h2>Follow the recorded path from activity to restraint.</h2>
        <p>This read-only page presents supplied review cases without external lookups, added inputs, or inferred records.</p>
      </div>

      <div className="evidence-review-callout" role="status">
        <div>
          <p className="eyebrow">Case service state</p>
          <h3>{label(view.status)}</h3>
          <p>Unknown fields remain unavailable throughout the review path.</p>
        </div>
        <StatusPill value={view.status} />
      </div>

      <article className="glass-panel review-boundary-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">System boundaries</p>
            <h3>Server-provided limits</h3>
          </div>
        </div>
        <dl className="review-boundary-list">
          <div><dt>case_kind</dt><dd>{view.cases.length ? label(view.cases[0].case_kind) : unavailable}</dd></div>
          <div><dt>not_proof_of_fraud</dt><dd>{BoundaryValue({ value: view.notProof })}</dd></div>
          <div><dt>effectiveness_unknown</dt><dd>{BoundaryValue({ value: view.effectiveness })}</dd></div>
        </dl>
      </article>

      {view.cases.length ? (
        view.cases.map((reviewCase, index) => <CaseFunnel key={firstPresent(reviewCase, ["case_uid"]) || index} reviewCase={reviewCase} index={index} />)
      ) : (
        <article className="glass-panel review-empty-panel">
          <p className="eyebrow">No published review case</p>
          <h3>Every funnel stage is unavailable.</h3>
          <p>No case was supplied by the V3 service. This page will not create activity records, ordinary checks, coverage decisions, or ledger entries on its own.</p>
          <div className="review-empty-stages" aria-label="Unavailable review stages">
            <span>01 Observed activity trigger · unavailable</span>
            <span>02 Ordinary explanation checks · unavailable</span>
            <span>03 Coverage and restraint decision · unavailable</span>
            <span>04 Redacted evidence ledger · unavailable</span>
          </div>
        </article>
      )}
    </section>
  );
}
