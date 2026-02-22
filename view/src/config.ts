/**
 * Configuration Management
 * Centralized configuration for the application
 */

interface Config {
  supabase: {
    url: string;
    anonKey: string;
    serviceRoleKey?: string;
  };
  server: {
    port: number;
    host: string;
    env: "development" | "production" | "test";
  };
  backend: {
    apiUrl: string;
    timeout: number;
  };
  security: {
    corsOrigins: string[];
    secureCookies: boolean;
    jwtSecret?: string;
  };
  features: {
    enableAmbiguityDetection: boolean;
    enableConradictionDetection: boolean;
    enableBriefing: boolean;
    enableMapbox: boolean;
    enableWordCloud: boolean;
  };
}

function validateConfig(config: Config): boolean {
  // Validate Supabase
  if (!config.supabase.url || !config.supabase.anonKey) {
    console.error("❌ Missing Supabase configuration");
    return false;
  }

  // Validate server config
  if (!config.server.port || config.server.port < 1 || config.server.port > 65535) {
    console.error("❌ Invalid server port");
    return false;
  }

  // Validate backend URL
  try {
    new URL(config.backend.apiUrl);
  } catch {
    console.error("❌ Invalid backend API URL");
    return false;
  }

  return true;
}

function loadConfig(): Config {
  const env = process.env.NODE_ENV || "development";

  const config: Config = {
    supabase: {
      url: process.env.SUPABASE_URL || "",
      anonKey: process.env.SUPABASE_ANON_KEY || "",
      serviceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
    },
    server: {
      port: parseInt(process.env.PORT || "3000"),
      host: process.env.HOST || "0.0.0.0",
      env: (env as "development" | "production" | "test") || "development",
    },
    backend: {
      apiUrl: process.env.BACKEND_API_URL || "http://localhost:8000",
      timeout: parseInt(process.env.BACKEND_TIMEOUT || "30000"),
    },
    security: {
      corsOrigins: (process.env.ALLOWED_ORIGINS || "http://localhost:3000").split(","),
      secureCookies: env === "production",
      jwtSecret: process.env.JWT_SECRET,
    },
    features: {
      enableAmbiguityDetection: process.env.ENABLE_AMBIGUITY_DETECTION !== "false",
      enableConradictionDetection: process.env.ENABLE_CONTRADICTION_DETECTION !== "false",
      enableBriefing: process.env.ENABLE_BRIEFING !== "false",
      enableMapbox: process.env.ENABLE_MAPBOX !== "false",
      enableWordCloud: process.env.ENABLE_WORDCLOUD !== "false",
    },
  };

  if (!validateConfig(config)) {
    throw new Error("Invalid configuration");
  }

  return config;
}

class AppConfig {
  private static instance: Config | null = null;

  static getInstance(): Config {
    if (!AppConfig.instance) {
      AppConfig.instance = loadConfig();
    }
    return AppConfig.instance;
  }

  static reset(): void {
    AppConfig.instance = null;
  }
}

export function getConfig(): Config {
  return AppConfig.getInstance();
}

export function printConfig(): void {
  const config = getConfig();

  console.log("📋 Application Configuration:");
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(`🔹 Environment: ${config.server.env}`);
  console.log(`🔹 Server: ${config.server.host}:${config.server.port}`);
  console.log(`🔹 Backend: ${config.backend.apiUrl}`);
  console.log(`🔹 Supabase: ${config.supabase.url}`);
  console.log("🔹 Features:");
  console.log(`  ├─ Ambiguity Detection: ${config.features.enableAmbiguityDetection ? "✓" : "✗"}`);
  console.log(`  ├─ Contradiction Detection: ${config.features.enableConradictionDetection ? "✓" : "✗"}`);
  console.log(`  ├─ Briefing: ${config.features.enableBriefing ? "✓" : "✗"}`);
  console.log(`  ├─ Mapbox: ${config.features.enableMapbox ? "✓" : "✗"}`);
  console.log(`  └─ Word Cloud: ${config.features.enableWordCloud ? "✓" : "✗"}`);
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

export default getConfig;
