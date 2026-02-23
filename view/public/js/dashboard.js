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
  initializeWeatherChart();
}

// ============ MODE DISPLAY ============

// ============ LOAD DASHBOARD DATA ============

async function loadDashboardData() {
  try {
    const heatmapWeatherData = await loadHeatmapWeatherData();
    const wordcloudData = await loadWordcloudData();

    if (heatmapWeatherData) {
      updateHeatmapData(heatmapWeatherData.heatmapData);
      updateWeatherChart(heatmapWeatherData.weatherCorrelation);
    }

    if (wordcloudData) {
      updateWordCloudData(wordcloudData.wordCloudData);
    }
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
  const weather = document.getElementById("weather-chart-container");

  if (heatmap) {
    heatmap.innerHTML = `<div class="viz-placeholder">Erreur de chargement</div>`;
  }
  if (wordcloud) {
    wordcloud.innerHTML = `<div class="viz-placeholder">Erreur de chargement</div>`;
  }
  if (weather) {
    weather.innerHTML = `<div class="viz-placeholder">Erreur de chargement</div>`;
  }
}

function setupFilters() {
  const applyFiltersBtn = document.getElementById("apply-filters");
  const applyWordcloudBtn = document.getElementById("apply-wordcloud");
  const startDateInput = document.getElementById("start-date");
  const endDateInput = document.getElementById("end-date");

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

  if (applyFiltersBtn) {
    applyFiltersBtn.addEventListener("click", async () => {
      const heatmapWeatherData = await loadHeatmapWeatherData();
      if (heatmapWeatherData) {
        updateHeatmapData(heatmapWeatherData.heatmapData);
        updateWeatherChart(heatmapWeatherData.weatherCorrelation);
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

function initializeWeatherChart() {
  const container = document.getElementById("weather-chart-container");
  if (!container) return;
  container.innerHTML = `
    <div class="viz-placeholder">Chargement de la meteo...</div>
  `;
}

function updateWeatherChart(data) {
  const container = document.getElementById("weather-chart-container");
  if (!container) return;

  if (!data || !Array.isArray(data.correlations) || data.correlations.length === 0) {
    container.innerHTML = `
      <div class="viz-placeholder">Aucune correlation disponible</div>
    `;
    return;
  }

  const series = data.correlations.filter((p) => p.weather && p.collisions).slice(0, 16);
  const temps = series.map((p) => p.weather.mean_temp_c ?? 0);
  const collisions = series.map((p) => p.collisions.total || 0);
  const minTemp = Math.min(...temps);
  const maxTemp = Math.max(...temps);
  const minColl = Math.min(...collisions);
  const maxColl = Math.max(...collisions);

  if (!series.length || maxColl === 0) {
    container.innerHTML = `
      <div class="viz-placeholder">Aucune collision pour la periode</div>
    `;
    return;
  }

  const width = 420;
  const height = 260;
  const padding = 36;
  const innerW = width - padding * 2;
  const innerH = height - padding * 2;

  const points = series.map((p, index) => {
    const temp = p.weather.mean_temp_c ?? 0;
    const total = p.collisions.total || 0;
    const x = padding + ((temp - minTemp) / (maxTemp - minTemp || 1)) * innerW;
    const y = padding + (1 - (total - minColl) / (maxColl - minColl || 1)) * innerH;
    const delay = Math.min(1, index * 0.05);
    return { x, y, temp, total, delay };
  });

  const { slope, intercept } = linearRegression(points);
  const lineX1 = padding;
  const lineX2 = padding + innerW;
  const lineY1 = padding + (1 - (slope * minTemp + intercept - minColl) / (maxColl - minColl || 1)) * innerH;
  const lineY2 = padding + (1 - (slope * maxTemp + intercept - minColl) / (maxColl - minColl || 1)) * innerH;

  const dots = points
    .map(
      (p) => `
        <circle class="weather-dot" cx="${p.x.toFixed(2)}" cy="${p.y.toFixed(2)}" r="5" style="animation-delay:${p.delay}s"></circle>
      `
    )
    .join("");

  container.innerHTML = `
    <div class="weather-chart fade-in">
      <svg class="weather-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet">
        <line class="weather-axis" x1="${padding}" y1="${padding}" x2="${padding}" y2="${padding + innerH}"></line>
        <line class="weather-axis" x1="${padding}" y1="${padding + innerH}" x2="${padding + innerW}" y2="${padding + innerH}"></line>
        <line class="weather-trend" x1="${lineX1.toFixed(2)}" y1="${lineY1.toFixed(2)}" x2="${lineX2.toFixed(2)}" y2="${lineY2.toFixed(2)}"></line>
        ${dots}
        <text class="weather-axis-label" x="${padding}" y="${padding - 10}">Collisions</text>
        <text class="weather-axis-label" x="${padding + innerW}" y="${padding + innerH + 26}" text-anchor="end">Temperature moyenne (°C)</text>
      </svg>
    </div>
  `;
}

function linearRegression(points) {
  const n = points.length;
  const xs = points.map((p) => p.temp);
  const ys = points.map((p) => p.total);
  const sumX = xs.reduce((a, b) => a + b, 0);
  const sumY = ys.reduce((a, b) => a + b, 0);
  const sumXY = xs.reduce((a, b, i) => a + b * ys[i], 0);
  const sumX2 = xs.reduce((a, b) => a + b * b, 0);

  const denom = n * sumX2 - sumX * sumX || 1;
  const slope = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;

  return { slope, intercept };
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

  // Nettoie après la réponse
  chatForm.addEventListener("htmx:afterSwap", function (e) {
    // Réinitialise le formulaire
    this.reset();
    
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

// ============ BRIEFING MODAL ============

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
