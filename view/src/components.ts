// ============ REUSABLE COMPONENTS ============

/**
 * Chat message component
 */
export function ChatMessageComponent(content: string, type: "user" | "ai") {
  return `
    <div class="chat-message ${type}">
      <div class="message-content">
        ${content}
      </div>
    </div>
  `;
}

/**
 * Ambiguity detection component with chips
 * 
 * Note: This component is now mainly for reference.
 * The actual ambiguity handling is done via HTMX in the backend response
 * from /api/chat endpoint. Dynamic options are generated from the API response.
 */
export function AmbiguityComponent(
  question: string,
  options: Array<{ label: string; value: string }>
) {
  const chips = options
    .map(
      (opt) => `
    <button class="clarification-chip" hx-post="/api/chat" hx-target="#chat-history" hx-swap="beforeend" hx-vals='{"selectedOption": "${opt.value}"}'>
      ${opt.label}
    </button>
  `
    )
    .join("");

  return `
    <div class="chat-message ai">
      <div class="message-content">
        <p>${question}</p>
        <div class="clarification-options">
          ${chips}
        </div>
      </div>
    </div>
  `;
}

/**
 * Warning/Contradiction box component
 */
export function WarningBoxComponent(contradictions: Array<string>) {
  const items = contradictions
    .map((contradiction) => `<p>• ${contradiction}</p>`)
    .join("");

  return `
    <div class="warning-box">
      <div class="warning-box-title">
        <span class="warning-icon">⚠️</span>
        <span>Limites et risques d'interprétation</span>
      </div>
      <div class="warning-box-content">
        ${items}
      </div>
    </div>
  `;
}

/**
 * Dashboard card component
 */
export function DashboardCardComponent(
  title: string,
  contentId: string,
  className: string
) {
  return `
    <div class="dashboard-card ${className}">
      <h3>${title}</h3>
      <div id="${contentId}" class="chart-container">
        <div class="placeholder">Chargement...</div>
      </div>
    </div>
  `;
}

/**
 * Briefing modal component
 */
export function BriefingModalComponent(briefingData: {
  topHotspots: Array<{ name: string; count: number }>;
  trends: Array<{ category: string; trend: number }>;
  recommendations: Array<string>;
}) {
  const hotspots = briefingData.topHotspots
    .slice(0, 5)
    .map(
      (spot, i) => `
    <div class="briefing-item">
      <div class="briefing-item-title">
        ${i + 1}. ${spot.name}
      </div>
      <div class="briefing-item-content">
        ${spot.count} incidents
      </div>
    </div>
  `
    )
    .join("");

  const trends = briefingData.trends
    .map((t) => {
      const isUp = t.trend > 0;
      return `
    <div class="briefing-item">
      <div class="briefing-item-title">
        <span class="${isUp ? "trend-up" : "trend-down"}">
          ${isUp ? "↑" : "↓"} ${t.category}
        </span>
      </div>
      <div class="briefing-item-content">
        ${Math.abs(t.trend)}% ${isUp ? "augmentation" : "réduction"}
      </div>
    </div>
  `;
    })
    .join("");

  const recommendations = briefingData.recommendations
    .map(
      (rec) => `
    <div class="briefing-item">
      <div class="briefing-item-content">
        ${rec}
      </div>
    </div>
  `
    )
    .join("");

  return `
    <div id="briefing-modal" class="modal-overlay active">
      <div class="modal-card">
        <div class="modal-header">
          <h2>📊 Briefing Hebdomadaire</h2>
          <button class="modal-close" onclick="closeBriefingModal()">✕</button>
        </div>

        <div>
          <h3>🔴 Top 5 Hotspots</h3>
          ${hotspots}

          <h3 style="margin-top: 1.5rem;">📈 Tendances</h3>
          ${trends}

          <h3 style="margin-top: 1.5rem;">💡 Recommandations</h3>
          ${recommendations}
        </div>

        <button class="btn btn-primary" onclick="closeBriefingModal()" style="margin-top: 1.5rem;">
          Fermer
        </button>
      </div>
    </div>
  `;
}

/**
 * Loading spinner component
 */
export function LoadingSpinnerComponent() {
  return `<div class="loading"></div>`;
}

/**
 * Error message component
 */
export function ErrorMessageComponent(message: string) {
  return `
    <div class="error-message">
      ${message}
    </div>
  `;
}

/**
 * Success message component
 */
export function SuccessMessageComponent(message: string) {
  return `
    <div style="
      background-color: #D1FAE5;
      color: #065F46;
      padding: 0.75rem 1rem;
      border-radius: 8px;
      border-left: 4px solid #10B981;
      font-size: 0.875rem;
    ">
      ✓ ${message}
    </div>
  `;
}
