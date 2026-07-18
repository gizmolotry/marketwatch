import { useState } from "react";
import { api, formatNumber } from "../api";

function AssistantResult({ result }) {
  if (!result) {
    return null;
  }
  if (result.error) {
    return <p className="warning-text">{result.error}</p>;
  }
  if (result.status !== "analyzed") {
    return <p>{result.message}</p>;
  }
  return (
    <div className="assistant-result">
      <div className="analysis-metrics">
        <div>
          <span>Timing gap (context)</span>
          <strong>{formatNumber(result.lead_time_hours, { maximumFractionDigits: 2 })}h</strong>
        </div>
        <div>
          <span>Legacy PPIM</span>
          <strong>{formatNumber(result.ppim_score, { maximumFractionDigits: 2 })}</strong>
        </div>
      </div>
      <div className="analysis-boundary">
        <strong>Not a fraud finding.</strong>
        <span>Legacy PPIM is deprecated, unvalidated, and is not a probability. Human review is required.</span>
      </div>
      <p>{result.rag_result?.evidence_text}</p>
      {result.report && result.report_artifact?.truth_firewall_verified && result.report_artifact?.content_available ? (
        <div className="legacy-report-block">
          <span className="status-pill unknown">Verified investigative memo · not validation evidence</span>
          <pre className="report-preview">{result.report}</pre>
        </div>
      ) : null}
      {result.report_artifact && !result.report_artifact.truth_firewall_verified ? (
        <div className="legacy-report-block">
          <span className="status-pill unknown">Generated artifact withheld</span>
          <p>The memo did not pass the v2 provenance, integrity, and truth-firewall checks.</p>
        </div>
      ) : null}
      {result.synthesis_error ? <p className="warning-text">{result.synthesis_error}</p> : null}
    </div>
  );
}

export default function Inference() {
  const [prompt, setPrompt] = useState("");
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      content: "Ask for a market by question text or slug. I will match it against the local feed and return activity-review context when a detector window exists. Outputs require human review and do not establish fraud.",
    },
  ]);
  const [pending, setPending] = useState(false);

  async function submitPrompt(event) {
    event.preventDefault();
    const cleanPrompt = prompt.trim();
    if (!cleanPrompt || pending) {
      return;
    }
    setPrompt("");
    setMessages((items) => [...items, { role: "user", content: cleanPrompt }]);
    setPending(true);
    try {
      const result = await api.chat(cleanPrompt);
      setMessages((items) => [
        ...items,
        {
          role: "assistant",
          content: result.status === "analyzed" ? `Matched ${result.question} for activity-context review.` : result.message,
          result,
        },
      ]);
    } catch (err) {
      setMessages((items) => [
        ...items,
        {
          role: "assistant",
          content: "The inference request failed.",
          result: { error: err.message },
        },
      ]);
    } finally {
      setPending(false);
    }
  }

  return (
    <section className="page-grid inference-grid">
      <div className="page-header">
        <p className="eyebrow">Activity Context Assistant</p>
        <h2>Ask for review context, not a verdict.</h2>
        <p>
          The assistant searches the local feed and summarizes detector context. It does not determine fraud,
          identify an actor, or produce a regulatory filing.
        </p>
      </div>

      <div className="glass-panel chat-panel">
        <div className="message-stack" aria-live="polite">
          {messages.map((message, index) => (
            <div className={`message-bubble ${message.role}`} key={`${message.role}-${index}`}>
              <span>{message.role}</span>
              <p>{message.content}</p>
              <AssistantResult result={message.result} />
            </div>
          ))}
          {pending ? (
            <div className="message-bubble assistant">
              <span>assistant</span>
              <p>Searching the feed and checking activity windows...</p>
            </div>
          ) : null}
        </div>

        <form className="chat-input" onSubmit={submitPrompt}>
          <input
            aria-label="Market question or slug"
            value={prompt}
            onChange={(event) => setPrompt(event.target.value)}
            placeholder="Enter a market question or slug"
          />
          <button className="primary-action" disabled={pending || !prompt.trim()}>
            Send
          </button>
        </form>
      </div>
    </section>
  );
}
