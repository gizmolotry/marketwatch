import { useEffect, useState } from "react";
import { NavLink, Route, Routes } from "react-router-dom";
import { api, API_BASE_URL } from "./api";
import Home from "./pages/Home.jsx";
import Sandbox from "./pages/Sandbox.jsx";
import Reports from "./pages/Reports.jsx";
import Inference from "./pages/Inference.jsx";
import Validation from "./pages/Validation.jsx";
import ModelReadiness from "./pages/ModelReadiness.jsx";
import BitcoinContext from "./pages/BitcoinContext.jsx";
import EvidenceReview from "./pages/EvidenceReview.jsx";
import "./styles.css";

const navItems = [
  { to: "/", label: "Evidence Review" },
  { to: "/overview", label: "Overview" },
  { to: "/sandbox", label: "Sandbox" },
  { to: "/validation", label: "Validation" },
  { to: "/model-readiness", label: "Model Readiness" },
  { to: "/bitcoin-context", label: "Bitcoin Context" },
  { to: "/reports", label: "Reports" },
  { to: "/inference", label: "Inference" },
];

export default function App() {
  const [healthStatus, setHealthStatus] = useState("checking");

  useEffect(() => {
    let active = true;

    const checkHealth = () => {
      api
        .health()
        .then((payload) => {
          if (active) setHealthStatus(payload?.status === "ok" ? "healthy" : "unavailable");
        })
        .catch(() => {
          if (active) setHealthStatus("unavailable");
        });
    };

    checkHealth();
    const timer = window.setInterval(checkHealth, 30000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  const healthLabel = {
    checking: "Checking API",
    healthy: "API healthy",
    unavailable: "API unavailable",
  }[healthStatus];

  return (
    <div className="app-shell">
      <aside className="sidebar glass-panel">
        <div className="brand-block">
          <div className="brand-mark">ML</div>
          <div>
            <p className="eyebrow">Market Integrity</p>
            <h1>MarketLeak</h1>
          </div>
        </div>

        <nav className="nav-list" aria-label="Primary navigation">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => (isActive ? "nav-item active" : "nav-item")}
              end={item.to === "/"}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className={`system-card health-${healthStatus}`} aria-live="polite">
          <span className={`health-indicator ${healthStatus}`} aria-hidden="true" />
          <div>
            <strong>{healthLabel}</strong>
            <small>{API_BASE_URL.replace(/^https?:\/\//, "")}</small>
          </div>
        </div>
      </aside>

      <main className="content-shell">
        <Routes>
          <Route path="/" element={<EvidenceReview />} />
          <Route path="/overview" element={<Home />} />
          <Route path="/sandbox" element={<Sandbox />} />
          <Route path="/validation" element={<Validation />} />
          <Route path="/model-readiness" element={<ModelReadiness />} />
          <Route path="/evidence-review" element={<EvidenceReview />} />
          <Route path="/bitcoin-context" element={<BitcoinContext />} />
          <Route path="/reports" element={<Reports />} />
          <Route path="/inference" element={<Inference />} />
        </Routes>
      </main>
    </div>
  );
}
