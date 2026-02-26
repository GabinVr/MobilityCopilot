// ============ DASHBOARD INITIALIZATION ============

let currentUserType = "public";
let heatmapMap = null;
let heatmapLayer = null;

// Initialize on page load
document.addEventListener("DOMContentLoaded", function () {
  const bodyUserType = document.body?.dataset?.userType;
  if (bodyUserType === "municipality") {
    currentUserType = "municipality";
  }

  initializeDashboard();
  loadDashboardData();
  setupFilters();
  setupHTMXInterceptors();
  setupChatMessageHandling();
});

function initializeDashboard() {
  const modeDisplay = document.getElementById("mode-display");
  if (modeDisplay) {
    modeDisplay.textContent =
      currentUserType === "municipality" ? "Municipalité" : "Public";
  }

  // Initialize charts and maps
  initializeHeatmap();
  initializeWordCloud();
  // initializeWeatherChart(); // Commented out - weather chart removed
  initializeTrends();
  initializeWeeklyReports();
}

// ============ MODE DISPLAY ============

// ============ LOAD DASHBOARD DATA ============

async function loadDashboardData() {
  try {
    const heatmapWeatherData = await loadHeatmapWeatherData();
    const wordcloudData = await loadWordcloudData();
    const trendsData = await loadTrendsData();
    const weeklyReportsData = await loadWeeklyReportsData();

    if (heatmapWeatherData) {
      updateHeatmapData(heatmapWeatherData.heatmapData);
      // updateWeatherChart(heatmapWeatherData.weatherCorrelation); // Commented out - weather chart removed
    }

    if (wordcloudData) {
      updateWordCloudData(wordcloudData.wordCloudData);
    }

    if (trendsData) {
      updateTrendsData(trendsData);
    }

    // Always update weekly reports, even if null (will show placeholder)
    updateWeeklyReportsData(weeklyReportsData);
  } catch (error) {
    console.error("Error loading dashboard data:", error);
    showDashboardError();
  }
}

async function loadHeatmapWeatherData() {
  try {
    const { timeRange, severity, startDate, endDate } = getFilterValues();
    const response = await fetch(
      `/api/heatmap-weather/${currentUserType}?timeRange=${encodeURIComponent(timeRange)}&severity=${encodeURIComponent(severity)}&startDate=${encodeURIComponent(startDate)}&endDate=${encodeURIComponent(endDate)}`
    );

    if (!response.ok) {
      throw new Error("Heatmap/weather data request failed");
    }

    return await response.json();
  } catch (error) {
    console.error("Error loading heatmap/weather data:", error);
    return null;
  }
}

async function loadWordcloudData() {
  try {
    const { wordRange } = getFilterValues();
    const response = await fetch(
      `/api/wordcloud/${currentUserType}?wordRange=${encodeURIComponent(wordRange)}`
    );

    if (!response.ok) {
      throw new Error("Wordcloud data request failed");
    }

    return await response.json();
  } catch (error) {
    console.error("Error loading wordcloud data:", error);
    return null;
  }
}

function getFilterValues() {
  const startDateInput = document.getElementById("start-date");
  const endDateInput = document.getElementById("end-date");
  const severitySelect = document.getElementById("severity-filter");
  const wordRangeSelect = document.getElementById("wordcloud-range");

  const startDate = startDateInput?.value || "";
  const endDate = endDateInput?.value || "";
  const timeRange = startDate && endDate ? `${startDate} to ${endDate}` : "2015-01-01 to 2015-12-31";

  return {
    timeRange,
    severity: severitySelect?.value || "all",
    startDate,
    endDate,
    wordRange: wordRangeSelect?.value || "last_month",
  };
}

function showDashboardError() {
  const heatmap = document.getElementById("heatmap-container");
  const wordcloud = document.getElementById("wordcloud-container");
  // const weather = document.getElementById("weather-chart-container");
  const trends = document.getElementById("trends-container");
  const weeklyReports = document.getElementById("weekly-reports-container");

  const errorHTML = `<div class="viz-placeholder">Erreur de chargement</div>`;

  if (heatmap) heatmap.innerHTML = errorHTML;
  if (wordcloud) wordcloud.innerHTML = errorHTML;
  // if (weather) weather.innerHTML = errorHTML;
  if (trends) trends.innerHTML = errorHTML;
  if (weeklyReports) weeklyReports.innerHTML = errorHTML;
}

// ============ TRENDS DATA ============

async function loadTrendsData() {
  try {
    const trendsDateInput = document.getElementById("trends-date");
    const asOfDate = trendsDateInput?.value || new Date().toISOString().split("T")[0];

    const response = await fetch(
      `/api/trends/${currentUserType}?as_of_date=${encodeURIComponent(asOfDate)}`
    );

    if (!response.ok) {
      throw new Error("Trends data request failed");
    }

    return await response.json();
  } catch (error) {
    console.error("Error loading trends data:", error);
    return null;
  }
}

function updateTrendsData(data) {
  const container = document.getElementById("trends-container");
  if (!container) return;

  try {
    const html = `
      <div class="trends-content">
        <div class="trends-section">
          <h4>Collision Mensuelles</h4>
          <div id="trends-monthly-chart" class="trend-chart"></div>
        </div>
        <div class="trends-section">
          <h4>Comparaison Piétons</h4>
          <div id="trends-pedestrian-chart" class="trend-chart"></div>
        </div>
        <div class="trends-section">
          <h4>Pic Horaire</h4>
          <div id="trends-hourly-chart" class="trend-chart"></div>
        </div>
        <div class="trends-section">
          <h4>Insights</h4>
          <div id="trends-insights" class="trends-insights"></div>
        </div>
      </div>
    `;

    container.innerHTML = html;

    // Render charts if data exists
    if (data.monthly_collisions) {
      renderMonthlyCollisionsChart(data.monthly_collisions);
    }
    if (data.pedestrian_3m_vs_last_year) {
      renderPedestrianChart(data.pedestrian_3m_vs_last_year);
    }
    if (data.hourly_peak_shift) {
      renderHourlyPeakChart(data.hourly_peak_shift);
    }
    if (data.insights) {
      renderInsights(data.insights);
    }
  } catch (error) {
    console.error("Error updating trends data:", error);
    container.innerHTML = `<div class="viz-placeholder">Erreur d'affichage des tendances</div>`;
  }
}

function renderMonthlyCollisionsChart(data) {
  const chartDiv = document.getElementById("trends-monthly-chart");
  if (!chartDiv || !window.Plotly) return;

  const trace = {
    x: data.months || [],
    y: data.values || [],
    type: "scatter",
    mode: "lines+markers",
    fill: "tozeroy",
    line: { color: "var(--accent-primary)" },
  };

  Plotly.newPlot(chartDiv, [trace], { responsive: true, displayModeBar: false }, { responsive: true });
}

function renderPedestrianChart(data) {
  const chartDiv = document.getElementById("trends-pedestrian-chart");
  if (!chartDiv || !window.Plotly) return;

  const trace = {
    x: ["3M", "Année dernière"],
    y: [data["3m"] || 0, data["last_year"] || 0],
    type: "bar",
    marker: { color: ["#3B82F6", "#10B981"] },
  };

  Plotly.newPlot(chartDiv, [trace], { responsive: true, displayModeBar: false }, { responsive: true });
}

function renderHourlyPeakChart(data) {
  const chartDiv = document.getElementById("trends-hourly-chart");
  if (!chartDiv) return;

  chartDiv.innerHTML = `<div class="trend-text">${data.description || "Changement du pic horaire détecté"}</div>`;
}

function renderInsights(insights) {
  const insightsDiv = document.getElementById("trends-insights");
  if (!insightsDiv) return;

  const insightsList = Array.isArray(insights) ? insights : [insights];
  insightsDiv.innerHTML = insightsList
    .map((insight) => `<div class="insight-item">• ${insight}</div>`)
    .join("");
}

// ============ WEEKLY REPORTS DATA ============

async function loadWeeklyReportsData() {
  // Always report available - validation happens on click
  return true;
}

function updateWeeklyReportsData(available) {
  const container = document.getElementById("weekly-reports-container");
  if (!container) return;

  try {
    const currentLang = window.currentLanguage || "fr";
    const reportTitle = window.t ? window.t("hotspotsAndWeakSignals") : "Rapport Points Chauds & Signaux Faibles";
    const downloadLabel = window.t ? window.t("downloadReport") : "Télécharger le Rapport PDF";
    const sectionsLabel = window.t ? window.t("sections") : "Sections: Points Chauds et Signaux Faibles";
    
    if (!available) {
      // Display disabled button when API fails or report unavailable
      const unavailableMsg = window.t ? window.t("noReportAvailable") : "Rapport en cours de génération...";
      const html = `
        <div class="weekly-reports-content">
          <div class="report-section">
            <h4>${reportTitle}</h4>
            <div class="report-body">
              <p>${sectionsLabel}</p>
              <div class="report-metadata">
                <small>${unavailableMsg}</small>
              </div>
              <div class="report-actions">
                <button class="btn-download" disabled title="${unavailableMsg}">
                  📄 ${downloadLabel}
                </button>
              </div>
            </div>
          </div>
        </div>
      `;
      container.innerHTML = html;
      return;
    }
    
    // PDF endpoint serves file directly for download
    const downloadUrl = `/api/weekly-reports/${currentUserType}?language=${encodeURIComponent(currentLang)}`;
    const html = `
      <div class="weekly-reports-content">
        <div class="report-section">
          <h4>${reportTitle}</h4>
          <div class="report-body">
            <p>${sectionsLabel}</p>
            <div class="report-actions">
              <a href="${downloadUrl}" download class="btn-download">
                📄 ${downloadLabel}
              </a>
            </div>
          </div>
        </div>
      </div>
    `;

    container.innerHTML = html;
  } catch (error) {
    console.error("Error updating weekly reports data:", error);
    container.innerHTML = `<div class="viz-placeholder">${window.t ? window.t("reportError") : "Erreur lors du chargement du rapport."}</div>`;
  }
}

function setupFilters() {
  const applyFiltersBtn = document.getElementById("apply-filters");
  const applyWordcloudBtn = document.getElementById("apply-wordcloud");
  const applyTrendsBtn = document.getElementById("apply-trends");
  const startDateInput = document.getElementById("start-date");
  const endDateInput = document.getElementById("end-date");
  const trendsDateInput = document.getElementById("trends-date");

  if (startDateInput && endDateInput) {
    const startDate = new Date(2015, 0, 1);
    const endDate = new Date(2015, 11, 31);

    if (!startDateInput.value) {
      startDateInput.value = formatDateInput(startDate);
    }
    if (!endDateInput.value) {
      endDateInput.value = formatDateInput(endDate);
    }
  }

  if (trendsDateInput) {
    if (!trendsDateInput.value) {
      const oneWeekAgo = new Date();
      oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
      trendsDateInput.value = formatDateInput(oneWeekAgo);
    }
  }

  if (applyFiltersBtn) {
    applyFiltersBtn.addEventListener("click", async () => {
      const heatmapWeatherData = await loadHeatmapWeatherData();
      if (heatmapWeatherData) {
        updateHeatmapData(heatmapWeatherData.heatmapData);
      }
    });
  }

  if (applyWordcloudBtn) {
    applyWordcloudBtn.addEventListener("click", async () => {
      const wordcloudData = await loadWordcloudData();
      if (wordcloudData) {
        updateWordCloudData(wordcloudData.wordCloudData);
      }
    });
  }

  if (applyTrendsBtn) {
    applyTrendsBtn.addEventListener("click", async () => {
      const trendsData = await loadTrendsData();
      if (trendsData) {
        updateTrendsData(trendsData);
      }
    });
  }
}

function formatDateInput(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

// ============ HEATMAP INITIALIZATION ============

function initializeHeatmap() {
  const container = document.getElementById("heatmap-container");
  if (!container) return;
  if (window.L) {
    heatmapMap = L.map(container, {
      zoomControl: false,
      attributionControl: false,
    }).setView([45.5017, -73.5673], 11);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 18,
    }).addTo(heatmapMap);

    heatmapLayer = L.layerGroup().addTo(heatmapMap);
  } else {
    container.innerHTML = `
      <div class="viz-placeholder">Chargement des collisions...</div>
    `;
  }
}

function updateHeatmapData(data) {
  const container = document.getElementById("heatmap-container");
  if (!container) return;

  // Always ensure Leaflet map is available
  if (window.L && heatmapMap && heatmapLayer) {
    heatmapLayer.clearLayers();

    if (data && Array.isArray(data.collisions) && data.collisions.length > 0) {
      const bounds = [];
      data.collisions.slice(0, 1500).forEach((c) => {
        const color = getSeverityColor(c.severity);
        const intensity =
          1 +
          (c.deaths || 0) * 2 +
          (c.severely_injured || 0) * 0.8 +
          (c.lightly_injured || 0) * 0.3;
        const radius = Math.max(3, Math.min(16, 4 + intensity));

        const marker = L.circleMarker([c.lat, c.lon], {
          radius,
          fillColor: color,
          color: color,
          weight: 0.5,
          fillOpacity: 0.45,
        });

        marker.addTo(heatmapLayer);
        bounds.push([c.lat, c.lon]);
      });

      if (bounds.length) {
        heatmapMap.fitBounds(bounds, { padding: [18, 18] });
      }
    } else {
      // No data - just show the base map
      heatmapMap.setView([45.5017, -73.5673], 11);
    }

    return;
  }

  // Fallback for non-Leaflet display (shouldn't be used in normal operation)
  container.innerHTML = `<div class="viz-placeholder">Carte non disponible</div>`;
}

function getSeverityColor(severity) {
  switch (severity) {
    case "Mortel":
      return "#B91C1C";
    case "Grave":
      return "#F97316";
    case "Léger":
      return "#FBBF24";
    case "Dommages matériels seulement":
      return "#38BDF8";
    case "Dommages matériels inférieurs au seuil de rapportage":
      return "#94A3B8";
    default:
      return "#A78BFA";
  }
}

// ============ WORD CLOUD INITIALIZATION ============

function initializeWordCloud() {
  const container = document.getElementById("wordcloud-container");
  if (!container) return;
  container.innerHTML = `
    <div class="viz-placeholder">Chargement des requetes 311...</div>
  `;
}

function updateWordCloudData(data) {
  const container = document.getElementById("wordcloud-container");
  if (!container) return;

  if (!data || !Array.isArray(data.top_words) || data.top_words.length === 0) {
    container.innerHTML = `
      <div class="viz-placeholder">Aucune requete disponible</div>
    `;
    return;
  }

  const words = data.top_words.slice(0, 24);
  const maxCount = Math.max(...words.map((w) => w.count));

  // Create a grid to avoid overlap
  const gridCols = 4;
  const gridRows = 4;
  const usedCells = new Set();

  const wordSpans = words
    .map((word, index) => {
      const size = 12 + (word.count / (maxCount || 1)) * 22;
      
      // Find a random unused cell in the grid
      let cellCol, cellRow;
      let attempts = 0;
      do {
        cellCol = Math.floor(Math.random() * gridCols);
        cellRow = Math.floor(Math.random() * gridRows);
        attempts++;
      } while (usedCells.has(`${cellCol},${cellRow}`) && attempts < 20);
      
      usedCells.add(`${cellCol},${cellRow}`);
      
      // Calculate position based on grid cell with small randomness
      const cellWidth = 100 / gridCols;
      const cellHeight = 100 / gridRows;
      const x = cellCol * cellWidth + Math.random() * (cellWidth * 0.7);
      const y = cellRow * cellHeight + Math.random() * (cellHeight * 0.7);
      
      const delay = Math.min(1, index * 0.05);

      return `
        <span class="wordcloud-word" style="left:${x.toFixed(1)}%; top:${y.toFixed(1)}%; font-size:${size.toFixed(0)}px; animation-delay:${delay}s, ${delay + 0.6}s">
          ${word.word}
        </span>
      `;
    })
    .join("");

  container.innerHTML = `
    <div class="wordcloud-wrap fade-in">
      ${wordSpans}
    </div>
  `;
}

// ============ WEATHER CHART INITIALIZATION ============

// function initializeWeatherChart() {
//   const container = document.getElementById("weather-chart-container");
//   if (!container) return;
//   container.innerHTML = `
//     <div class="viz-placeholder">Chargement de la meteo...</div>
//   `;
// }

// function updateWeatherChart(data) {
//   const container = document.getElementById("weather-chart-container");
//   if (!container) return;

//   if (!data || !Array.isArray(data.correlations) || data.correlations.length === 0) {
//     container.innerHTML = `
//       <div class="viz-placeholder">Aucune correlation disponible</div>
//     `;
//     return;
//   }

//   const series = data.correlations.filter((p) => p.weather && p.collisions).slice(0, 16);
//   const temps = series.map((p) => p.weather.mean_temp_c ?? 0);
//   const collisions = series.map((p) => p.collisions.total || 0);
//   const minTemp = Math.min(...temps);
//   const maxTemp = Math.max(...temps);
//   const minColl = Math.min(...collisions);
//   const maxColl = Math.max(...collisions);

//   if (!series.length || maxColl === 0) {
//     container.innerHTML = `
//       <div class="viz-placeholder">Aucune collision pour la periode</div>
//     `;
//     return;
//   }

//   const width = 420;
//   const height = 260;
//   const padding = 36;
//   const innerW = width - padding * 2;
//   const innerH = height - padding * 2;

//   const points = series.map((p, index) => {
//     const temp = p.weather.mean_temp_c ?? 0;
//     const total = p.collisions.total || 0;
//     const x = padding + ((temp - minTemp) / (maxTemp - minTemp || 1)) * innerW;
//     const y = padding + (1 - (total - minColl) / (maxColl - minColl || 1)) * innerH;
//     const delay = Math.min(1, index * 0.05);
//     return { x, y, temp, total, delay };
//   });

//   const { slope, intercept } = linearRegression(points);
//   const lineX1 = padding;
//   const lineX2 = padding + innerW;
//   const lineY1 = padding + (1 - (slope * minTemp + intercept - minColl) / (maxColl - minColl || 1)) * innerH;
//   const lineY2 = padding + (1 - (slope * maxTemp + intercept - minColl) / (maxColl - minColl || 1)) * innerH;

//   const dots = points
//     .map(
//       (p) => `
//         <circle class="weather-dot" cx="${p.x.toFixed(2)}" cy="${p.y.toFixed(2)}" r="5" style="animation-delay:${p.delay}s"></circle>
//       `
//     )
//     .join("");

//   container.innerHTML = `
//     <div class="weather-chart fade-in">
//       <svg class="weather-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet">
//         <line class="weather-axis" x1="${padding}" y1="${padding}" x2="${padding}" y2="${padding + innerH}"></line>
//         <line class="weather-axis" x1="${padding}" y1="${padding + innerH}" x2="${padding + innerW}" y2="${padding + innerH}"></line>
//         <line class="weather-trend" x1="${lineX1.toFixed(2)}" y1="${lineY1.toFixed(2)}" x2="${lineX2.toFixed(2)}" y2="${lineY2.toFixed(2)}"></line>
//         ${dots}
//         <text class="weather-axis-label" x="${padding}" y="${padding - 10}">Collisions</text>
//         <text class="weather-axis-label" x="${padding + innerW}" y="${padding + innerH + 26}" text-anchor="end">Temperature moyenne (°C)</text>
//       </svg>
//     </div>
//   `;
// }

// function linearRegression(points) {
//   const n = points.length;
//   const xs = points.map((p) => p.temp);
//   const ys = points.map((p) => p.total);
//   const sumX = xs.reduce((a, b) => a + b, 0);
//   const sumY = ys.reduce((a, b) => a + b, 0);
//   const sumXY = xs.reduce((a, b, i) => a + b * ys[i], 0);
//   const sumX2 = xs.reduce((a, b) => a + b * b, 0);

//   const denom = n * sumX2 - sumX * sumX || 1;
//   const slope = (n * sumXY - sumX * sumY) / denom;
//   const intercept = (sumY - slope * sumX) / n;

//   return { slope, intercept };
// }

// ============ TRENDS INITIALIZATION ============

function initializeTrends() {
  const container = document.getElementById("trends-container");
  if (!container) return;
  container.innerHTML = `
    <div class="viz-placeholder">Chargement des tendances...</div>
  `;
}

// ============ WEEKLY REPORTS INITIALIZATION ============

function initializeWeeklyReports() {
  const container = document.getElementById("weekly-reports-container");
  if (!container) return;
  container.innerHTML = `
    <div class="viz-placeholder">Chargement des rapports...</div>
  `;
}

// ============ HTMX INTERCEPTORS ============

function setupHTMXInterceptors() {
  // Handle chat form submission
  const chatForm = document.querySelector(".chat-form");
  if (chatForm) {
    chatForm.addEventListener("htmx:afterRequest", function (event) {
      // Auto-scroll chat history to bottom
      const chatHistory = document.getElementById("chat-history");
      if (chatHistory) {
        chatHistory.scrollTop = chatHistory.scrollHeight;
      }
    });
  }

  // Handle logout
  const logoutBtn = document.querySelector(".btn-logout");
  if (logoutBtn) {
    logoutBtn.addEventListener("htmx:beforeRequest", function (event) {
      // Clear local storage on logout
    });
  }
}

// ============ CHAT MESSAGE HANDLING ============

function setupChatMessageHandling() {
  const chatForm = document.getElementById("chat-form");
  if (!chatForm) return;

  // Handle form submission with HTMX
  chatForm.addEventListener("htmx:beforeSend", function (e) {
    const input = this.querySelector(".chat-input");
    const message = input.value.trim();

    if (message) {
      // Affiche le message utilisateur immédiatement
      addChatMessage(message, "user");
      
      // Vide le champ input immédiatement
      input.value = "";
      
      // Affiche l'animation de chargement
      addLoadingAnimation();
    }
  });

  // Supprime l'animation AVANT d'ajouter la réponse
  chatForm.addEventListener("htmx:beforeSwap", function (e) {
    removeLoadingAnimation();
  });

  // Nettoie après la réponse et traite les nouvelles réponses
  chatForm.addEventListener("htmx:afterSwap", function (e) {
    // Réinitialise le formulaire
    this.reset();
    
    // Traite les nouvelles réponses (contradictor notes, markdown, etc.)
    processNewChatMessages();
    
    // Scroll vers le bas
    scrollChatToBottom();
  });

  // Listener global pour les clics sur les chips de clarification
  document.addEventListener("click", function (e) {
    const chip = e.target.closest(".clarification-chip");
    if (chip) {
      const message = chip.textContent.trim();
      // Affiche le message utilisateur
      addChatMessage(message, "user");
      // Affiche l'animation de chargement
      addLoadingAnimation();
    }
  });

  // Listeners globaux pour HTMX (s'applique aux chips aussi)
  document.addEventListener("htmx:beforeSwap", function (e) {
    // Supprime l'animation avant d'ajouter la réponse
    removeLoadingAnimation();
  });

  document.addEventListener("htmx:afterSwap", function (e) {
    // Traite les nouvelles réponses
    processNewChatMessages();
    // Scroll vers le bas après la réponse
    scrollChatToBottom();
  });
}

function addChatMessage(content, type) {
  const chatHistory = document.getElementById("chat-history");
  const messageDiv = document.createElement("div");
  messageDiv.className = `chat-message ${type}`;

  const messageContent = document.createElement("div");
  messageContent.className = "message-content";
  messageContent.textContent = content;

  messageDiv.appendChild(messageContent);
  chatHistory.appendChild(messageDiv);

  scrollChatToBottom();
}

function addLoadingAnimation() {
  const chatHistory = document.getElementById("chat-history");
  const loadingDiv = document.createElement("div");
  loadingDiv.id = "loading-animation";
  loadingDiv.className = "chat-message ai";

  const messageContent = document.createElement("div");
  messageContent.className = "message-content loading-dots";
  
  for (let i = 0; i < 3; i++) {
    const dot = document.createElement("div");
    dot.className = "loading-dot";
    messageContent.appendChild(dot);
  }

  loadingDiv.appendChild(messageContent);
  chatHistory.appendChild(loadingDiv);

  scrollChatToBottom();
}

function removeLoadingAnimation() {
  const loadingDiv = document.getElementById("loading-animation");
  if (loadingDiv) {
    loadingDiv.remove();
  }
}

function scrollChatToBottom() {
  const chatHistory = document.getElementById("chat-history");
  if (chatHistory) {
    chatHistory.scrollTop = chatHistory.scrollHeight;
  }
}

/**
 * Post-process new chat messages to:
 * - Convert .warning-note elements to <details> dropdowns
 * - Handle markdown formatting
 */
function processNewChatMessages() {
  const chatHistory = document.getElementById("chat-history");
  if (!chatHistory) return;

  // Get all AI messages (most recent ones that were just added)
  const aiMessages = chatHistory.querySelectorAll(".chat-message.ai:not([data-processed])");
  
  aiMessages.forEach((messageDiv) => {
    messageDiv.dataset.processed = "true";
    
    // Extract thread_id from response and store in hidden input
    const threadId = messageDiv.dataset.threadId;
    if (threadId) {
      const hiddenInput = document.getElementById("chat-thread-id");
      if (hiddenInput) {
        hiddenInput.value = threadId;
      }
    }
    
    // Process markdown in message content
    const markdownSource = messageDiv.querySelector(".markdown-source");
    const markdownRendered = messageDiv.querySelector(".markdown-rendered");
    
    if (markdownSource && markdownRendered && markdownSource.textContent) {
      try {
        // Resolve the correct marked.parse function (handles different UMD export shapes)
        const markedLib = window.marked;
        const DOMPurify = window.DOMPurify;
        const parseFn = markedLib
          ? (markedLib.parse ? markedLib.parse.bind(markedLib) : (markedLib.marked ? markedLib.marked.parse.bind(markedLib.marked) : null))
          : null;
        
        if (parseFn && DOMPurify) {
          // .textContent already decodes HTML entities, no need to unescape manually
          const rawMarkdown = markdownSource.textContent;
          const htmlContent = parseFn(rawMarkdown);
          const cleanHtml = DOMPurify.sanitize(htmlContent);
          markdownRendered.innerHTML = cleanHtml;
        } else {
          // Fallback: show raw text with line breaks
          markdownRendered.innerHTML = markdownSource.textContent.replace(/\n/g, '<br>');
        }
      } catch (error) {
        console.error("Error processing markdown:", error);
        // Keep original text on error, with line breaks
        markdownRendered.innerHTML = markdownSource.textContent.replace(/\n/g, '<br>');
      }
    }
    
    // Find and convert .warning-note to <details>
    const warningNotes = messageDiv.querySelectorAll(".warning-note");
    warningNotes.forEach((warningNote) => {
      const details = document.createElement("details");
      details.className = "contradictory-notes-dropdown";
      
      const summary = document.createElement("summary");
      summary.className = "contradictory-notes-label";
      // Use translated label
      summary.textContent = window.t ? window.t("contradictorNotesLabel") : "L'assistant peut faire des erreurs, consultez les notes contradictoires";
      
      const content = document.createElement("div");
      content.className = "contradictory-notes-content";
      content.innerHTML = warningNote.innerHTML;
      
      details.appendChild(summary);
      details.appendChild(content);
      
      // Replace the warning-note with the details element
      warningNote.replaceWith(details);
    });
  });
}


function openBriefingModal() {
  const modal = document.getElementById("briefing-modal");
  if (modal) {
    modal.classList.add("active");
  }
}

function closeBriefingModal() {
  const modal = document.getElementById("briefing-modal");
  if (modal) {
    modal.classList.remove("active");
  }
}

// ============ UTILITIES ============

function formatDate(date) {
  return new Date(date).toLocaleDateString("fr-FR", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

function getTrendIcon(trendsValue) {
  return trendsValue > 0 ? "↑" : "↓";
}

function getTrendClass(trendsValue) {
  return trendsValue > 0 ? "trend-up" : "trend-down";
}

// Export functions for use in templates/HTMX
window.openBriefingModal = openBriefingModal;
window.closeBriefingModal = closeBriefingModal;
