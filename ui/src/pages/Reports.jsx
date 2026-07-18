import { useEffect, useState } from "react";
import { api, formatNumber, formatTime } from "../api";
import { ErrorState, LoadingState } from "../components/StateBlock.jsx";

export default function Reports() {
  const [reports, setReports] = useState(null);
  const [selected, setSelected] = useState(null);
  const [content, setContent] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    api
      .reports()
      .then((payload) => {
        setReports(payload.reports || []);
        if (payload.reports?.[0]) {
          setSelected(payload.reports[0].filename);
        }
      })
      .catch((err) => setError(err.message));
  }, []);

  useEffect(() => {
    if (!selected) {
      return;
    }
    setContent(null);
    api
      .report(selected)
      .then(setContent)
      .catch((err) => setError(err.message));
  }, [selected]);

  if (error) {
    return <ErrorState message={error} />;
  }

  if (!reports) {
    return <LoadingState label="Loading generated analysis artifacts..." />;
  }

  const selectedReport = reports.find((report) => report.filename === selected);

  return (
    <section className="page-grid reports-grid">
      <div className="page-header">
        <p className="eyebrow">Generated Artifact Archive</p>
        <h2>Verified investigative memos only.</h2>
        <p>
          Unmarked and legacy artifacts are quarantined and never returned verbatim. Verified memos remain human-review
          drafts, not regulatory filings, validation evidence, or proof of fraud.
        </p>
      </div>

      <div className="glass-panel report-list">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Artifact files</p>
            <h3>{formatNumber(reports.length)} Drafts</h3>
          </div>
        </div>
        <div className="signal-stack">
          {reports.map((report) => (
            <button
              type="button"
              className={selected === report.filename ? "signal-row active" : "signal-row"}
              key={report.filename}
              onClick={() => setSelected(report.filename)}
            >
              <span>{report.filename}</span>
              <small>
                {report.truth_firewall_verified ? "Verified memo · " : "Quarantined · "}
                {report.usable_as_validation_evidence === false ? "not evidence · " : ""}
                {formatTime(report.modified_at)}
              </small>
            </button>
          ))}
        </div>
      </div>

      <article className="glass-panel report-reader">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Artifact viewer</p>
            <h3>{content?.filename || selected || "No report selected"}</h3>
          </div>
        </div>
        {content ? (
          <>
            <div className="artifact-boundary" role="note">
              <span className="status-pill unknown">
                {content.truth_firewall_verified && content.content_available ? "Verified investigative memo" : "Artifact quarantined"}
              </span>
              <strong>Not validation evidence · not a regulatory filing · not proof of fraud</strong>
              <small>Classification: {content.evidence_classification || selectedReport?.evidence_classification || "unreviewed_artifact"}</small>
            </div>
            {content.truth_firewall_verified && content.content_available ? (
              <pre>{content.content}</pre>
            ) : (
              <div className="empty-evidence-state">
                <strong>Original content withheld</strong>
                <p>This file did not pass the v2 provenance, integrity, and truth-firewall checks.</p>
              </div>
            )}
          </>
        ) : <LoadingState label="Opening artifact..." />}
      </article>
    </section>
  );
}
