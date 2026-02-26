// Translation library for English/French bilingual support
export type Language = "en" | "fr";

export const translations = {
  en: {
    // Demo mode
    demoModeWarning: "Demo Mode",
    demoModeLoginMessage: "You can log in with any email/password.",
    demoModeSignupMessage: "You can create an account with any email/password.",

    // Auth pages - Login
    loginTitle: "Log In",
    loginSubtitle: "Sign in to access your dashboard",
    email: "Email",
    emailPlaceholder: "your@email.com",
    password: "Password",
    passwordPlaceholder: "••••••••",
    loginButton: "Sign In",
    noAccountText: "Don't have an account?",
    signupLink: "Sign Up",

    // Auth pages - Signup
    signupTitle: "Create Account",
    passwordMinLength: "At least 6 characters",
    userType: "User Type",
    userTypePublic: "Public",
    userTypePublicDescription: "Mobility information",
    userTypeMunicipality: "Municipality",
    userTypeMunicipalityDescription: "Operational analysis",
    signupButton: "Sign Up",
    haveAccountText: "Already have an account?",
    loginLink: "Sign In",
    backToLogin: "Back to Login",

    // Dashboard
    assistantTitle: "MobilityCopilot Assistant",
    mode: "Mode",
    modePublic: "Public",
    modeMunicipality: "Municipality",
    dashboardTitle: "Dashboard",
    logout: "Log Out",

    // Chat
    chatGreeting: "Hello! I'm your MobilityCopilot assistant. How can I help you today?",
    chatPlaceholder: "Ask your question...",
    contradictorNotesLabel: "The LLM can make mistakes, check the contradictory notes",
    loading: "Loading...",

    // Dashboard Filters
    startDate: "Start Date",
    endDate: "End Date",
    severity: "Severity",
    severityAll: "All",
    severityFatal: "Fatal",
    severitySerious: "Serious",
    severityMinor: "Minor",
    severityDamage: "Damage",
    severityMaterial: "Material",
    applyFilters: "Apply",

    // Dashboard Cards
    collisionMap: "Collision Map",
    loadingMap: "Loading map...",
    weatherCorrelation: "Weather Correlation",
    loadingChart: "Loading chart...",
    requests311: "311 Requests",
    lastWeek: "Last Week",
    lastMonth: "Last Month",
    loadingWordcloud: "Loading word cloud...",

    // Trends
    trends: "Trends",
    monthlyCollisions: "Monthly Collisions",
    pedestrianComparison: "Pedestrian Comparison (3M vs Last Year)",
    hourlyPeakShift: "Hourly Peak Shift",
    weakSignals: "Weak Signals",
    insights: "Insights",
    loadingTrends: "Loading trends...",

    // Weekly Reports
    weeklyReports: "Weekly Reports",
    downloadReport: "Download PDF Report",
    reportGeneratedAt: "Generated at:",
    hotspotsAndWeakSignals: "Hotspots & Weak Signals Report",
    sections: "Sections: Hotspots and Weak Signals",
    loadingReports: "Loading report...",
    noReportAvailable: "No report available yet.",
    reportError: "Error loading report.",

    // Error messages
    errorMessage: "An error occurred",
    errorLoading: "Error loading data. Please try again.",
  },
  fr: {
    // Demo mode
    demoModeWarning: "Mode Démo",
    demoModeLoginMessage: "Vous pouvez vous connecter avec n'importe quel email/mot de passe.",
    demoModeSignupMessage: "Vous pouvez créer un compte avec n'importe quel email/mot de passe.",

    // Auth pages - Login
    loginTitle: "Connexion",
    loginSubtitle: "Connectez-vous pour accéder au tableau de bord",
    email: "Email",
    emailPlaceholder: "votre@email.com",
    password: "Mot de passe",
    passwordPlaceholder: "••••••••",
    loginButton: "Se connecter",
    noAccountText: "Pas encore de compte?",
    signupLink: "S'inscrire",

    // Auth pages - Signup
    signupTitle: "Créez votre compte",
    passwordMinLength: "Au moins 6 caractères",
    userType: "Type d'utilisateur",
    userTypePublic: "Grand Public",
    userTypePublicDescription: "Information sur la mobilité",
    userTypeMunicipality: "Municipalité",
    userTypeMunicipalityDescription: "Analyse opérationnelle",
    signupButton: "S'inscrire",
    haveAccountText: "Vous avez un compte?",
    loginLink: "Se connecter",
    backToLogin: "Retour à la connexion",

    // Dashboard
    assistantTitle: "Assistant MobilityCopilot",
    mode: "Mode",
    modePublic: "Public",
    modeMunicipality: "Municipalité",
    dashboardTitle: "Tableau de Bord",
    logout: "Déconnexion",

    // Chat
    chatGreeting: "Bonjour! Je suis votre assistant MobilityCopilot. Comment puis-je vous aider aujourd'hui?",
    chatPlaceholder: "Posez votre question...",
    contradictorNotesLabel: "L'assistant peut faire des erreurs, consultez les notes contradictoires",
    loading: "Chargement...",

    // Dashboard Filters
    startDate: "Début",
    endDate: "Fin",
    severity: "Gravité",
    severityAll: "Toutes",
    severityFatal: "Mortel",
    severitySerious: "Grave",
    severityMinor: "Léger",
    severityDamage: "Dommages",
    severityMaterial: "Matériel",
    applyFilters: "Appliquer",

    // Dashboard Cards
    collisionMap: "Carte des Collisions",
    loadingMap: "Chargement de la carte...",
    weatherCorrelation: "Corrélation Météo",
    loadingChart: "Chargement du graphique...",
    requests311: "Requêtes 311",
    lastWeek: "Dernière semaine",
    lastMonth: "Dernier mois",
    loadingWordcloud: "Chargement du nuage de mots...",

    // Trends
    trends: "Tendances",
    monthlyCollisions: "Collisions Mensuelles",
    pedestrianComparison: "Comparaison Piétons (3M vs l'an dernier)",
    hourlyPeakShift: "Changement de Pic Horaire",
    weakSignals: "Signaux Faibles",
    insights: "Insights",
    loadingTrends: "Chargement des tendances...",

    // Weekly Reports
    weeklyReports: "Rapports Hebdomadaires",
    downloadReport: "Télécharger le Rapport PDF",
    reportGeneratedAt: "Généré le:",
    hotspotsAndWeakSignals: "Rapport Points Chauds & Signaux Faibles",
    sections: "Sections: Points Chauds et Signaux Faibles",
    loadingReports: "Chargement du rapport...",
    noReportAvailable: "Aucun rapport disponible pour le moment.",
    reportError: "Erreur lors du chargement du rapport.",

    // Error messages
    errorMessage: "Une erreur s'est produite",
    errorLoading: "Erreur lors du chargement des données. Veuillez réessayer.",
  },
};

/**
 * Detect user's preferred language from browser
 */
export function detectLanguage(): Language {
  const stored = localStorage.getItem("language");
  if (stored === "en" || stored === "fr") {
    return stored;
  }

  const browserLang = navigator.language || navigator.userLanguage;
  if (browserLang.startsWith("en")) {
    return "en";
  }

  return "fr";
}

/**
 * Save language preference to localStorage
 */
export function setLanguage(lang: Language): void {
  localStorage.setItem("language", lang);
}

/**
 * Get translation for a key in the specified language
 */
export function t(key: keyof typeof translations.en, lang: Language = "fr"): string {
  return translations[lang][key] || translations.fr[key] || key;
}

/**
 * Initialize language support for frontend
 */
export function initializeLanguage(): Language {
  const lang = detectLanguage();
  document.documentElement.lang = lang === "en" ? "en" : "fr";
  return lang;
}
