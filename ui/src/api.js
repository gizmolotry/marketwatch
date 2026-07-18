export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const detail = typeof payload === "object" && payload?.detail ? payload.detail : response.statusText;
    const error = new Error(detail);
    error.status = response.status;
    throw error;
  }

  return payload;
}

async function requestV2(path, fallback) {
  try {
    return await request(path);
  } catch (error) {
    if (![404, 405].includes(error.status) || !fallback) {
      throw error;
    }
    return fallback(await request("/api/overview"));
  }
}

async function requestV3(path, fallback) {
  try {
    const payload = await request(path);
    return {
      ...(payload && typeof payload === "object" ? payload : { payload }),
      _phase15_endpoint_available: true,
    };
  } catch (error) {
    if (![404, 405].includes(error.status) || !fallback) {
      throw error;
    }
    return fallback();
  }
}

const compatibilityClaims = (payload) => ({
  not_proof_of_fraud: payload?.not_proof_of_fraud !== false,
  effectiveness_unknown: payload?.effectiveness_unknown !== false,
});

async function phase15V2Fallback() {
  const [quality, validation] = await Promise.all([api.dataQuality(), api.validation()]);
  const actor = quality?.actor_availability || {};
  const sources = quality?.source_availability || {};
  const validationState = validation?.validation || {};
  const labeledCount = validationState.labeled_case_count;

  return {
    schema_version: "v2-fallback",
    _phase15_endpoint_available: false,
    readiness_source: "v2_fallback",
    bundle: {
      status: "unavailable",
      approval_status: "unavailable",
      reason: "The Phase 15 readiness endpoint is unavailable, so no serving bundle is assumed.",
    },
    label_sufficiency: {
      status: "unavailable",
      labeled_case_count: labeledCount ?? null,
      reason: "V2 does not expose Phase 15 label-sufficiency evidence.",
    },
    modality_availability: {
      market_state: {
        status: "unknown",
        reason: "V2 fallback does not expose a frozen Phase 15 market-feature snapshot.",
      },
      public_evidence: {
        status: sources.coverage_status || "unknown",
        available: Boolean(sources.point_in_time_public_sources_available),
        reason: "V2 source availability is not a Phase 15 event-memory coverage decision.",
      },
      onchain_settlement: {
        status: actor.visibility || "unknown",
        available: false,
        reason: "V2 actor availability does not establish Phase 15 on-chain settlement corroboration.",
      },
    },
    policies: {
      ood: { status: "unavailable", reason: "No Phase 15 OOD policy was supplied." },
      abstention: { status: "unavailable", reason: "No Phase 15 abstention policy was supplied." },
      coverage: { status: "unavailable", reason: "No Phase 15 coverage policy was supplied." },
    },
    effectiveness_unknown: true,
  };
}

function bitcoinContextFallback() {
  return {
    schema_version: "v3-bitcoin-context-fallback",
    _phase15_endpoint_available: false,
    status: "unavailable",
    reason: "The read-only Bitcoin Context endpoint is unavailable. No precomputed context is assumed.",
    read_only: true,
    live_fetch: false,
    contexts: [],
    not_proof_of_fraud: true,
    effectiveness_unknown: true,
  };
}

export const api = {
  health: () => request("/api/health"),
  overview: () => request("/api/overview"),
  anomalies: () => request("/api/anomalies"),
  analyzeSweep: () => request("/api/analyze/sweep", { method: "POST" }),
  analyze: (marketSlug) => request(`/api/analyze/${encodeURIComponent(marketSlug)}`, { method: "POST" }),
  reports: () => request("/api/reports"),
  report: (filename) => request(`/api/reports/${encodeURIComponent(filename)}`),
  chat: (prompt) =>
    request("/api/chat", {
      method: "POST",
      body: JSON.stringify({ prompt }),
    }),
  capabilities: () =>
    requestV2("/api/v2/capabilities", (payload) => ({
      schema_version: "1.x-compatible",
      targets: {
        A: "abnormal_market_activity",
        B: "point_in_time_public_explanation",
        C: "actor_specific_access_context",
      },
      targets_are_independent: true,
      scores_are_probabilities: false,
      fraud_prediction: false,
      automatic_fraud_finding: false,
      human_review_required: true,
      ...compatibilityClaims(payload),
    })),
  dataQuality: () =>
    requestV2("/api/v2/data-quality", (payload) => ({
      schema_version: "1.x-compatible",
      run_uid: null,
      status: payload?.data_quality?.gate_passed ? "legacy_gate_passed" : "blocked_by_data_quality",
      data_quality: payload?.data_quality || {},
      actor_availability: {
        available: false,
        visibility: "not_available",
        derived_from_price_snapshots: false,
      },
      source_availability: {
        point_in_time_public_sources_available: false,
        coverage_status: "unknown_coverage",
      },
      ...compatibilityClaims(payload),
    })),
  assessments: () =>
    requestV2("/api/v2/assessments", (payload) => ({
      schema_version: "1.x-compatible",
      run_uid: null,
      run_status: payload?.data_quality?.gate_passed ? "legacy_gate_passed" : "blocked_by_data_quality",
      count: Array.isArray(payload?.assessment_v2) ? payload.assessment_v2.length : 0,
      assessments: Array.isArray(payload?.assessment_v2) ? payload.assessment_v2 : [],
      ppim_score: null,
      ppim_deprecated: true,
      leak_risk_forecast: null,
      leak_risk_deprecated: true,
      ...compatibilityClaims(payload),
    })),
  validation: () =>
    requestV2("/api/v2/validation-status", (payload) => ({
      schema_version: "1.x-compatible",
      run_uid: null,
      run_status: payload?.data_quality?.gate_passed ? "legacy_gate_passed" : "blocked_by_data_quality",
      validation: {
        validated: false,
        effectiveness_unknown: payload?.effectiveness_unknown !== false,
        calibration_available: false,
        prospective_shadow_runs_completed: 0,
        labeled_case_count: 0,
        claim_scope: "activity_surveillance_for_human_review",
        not_proof_of_fraud: payload?.not_proof_of_fraud !== false,
      },
      ...compatibilityClaims(payload),
    })),
  modelReadiness: () => requestV3("/api/v3/readiness", phase15V2Fallback),
  modelStatus: () => requestV3("/api/v3/model-status", phase15V2Fallback),
  bitcoinContext: () => requestV3("/api/v3/bitcoin-context", bitcoinContextFallback),
  reviewCases: () => request("/api/v3/review-cases"),
  reviewCase: (caseUid) => request(`/api/v3/review-cases/${encodeURIComponent(caseUid)}`),
};

export function formatNumber(value, options = {}) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", options).format(Number(value));
}

export function formatTime(seconds) {
  if (!seconds) {
    return "—";
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(Number(seconds) * 1000));
}

export function compactText(value, length = 92) {
  const text = value ? String(value) : "";
  return text.length > length ? `${text.slice(0, length - 1)}...` : text;
}
