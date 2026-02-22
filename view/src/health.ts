/**
 * Health Check & Diagnostics
 * Endpoints for monitoring application health
 */

import { getSupabaseClient } from "./auth";
import { getConfig } from "./config";

export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  timestamp: string;
  uptime: number;
  services: {
    database: ServiceStatus;
    backend: ServiceStatus;
    cache?: ServiceStatus;
  };
  version: string;
}

export interface ServiceStatus {
  status: "available" | "unavailable" | "degraded";
  latency: number;
  error?: string;
}

/**
 * Check database connectivity
 */
export async function checkDatabase(): Promise<ServiceStatus> {
  const startTime = Date.now();

  try {
    const supabase = getSupabaseClient();

    // Simple query to test connection
    const { error } = await supabase.from("users").select("count()", { count: "exact", head: true });

    const latency = Date.now() - startTime;

    if (error) {
      return {
        status: "unavailable",
        latency,
        error: error.message,
      };
    }

    return {
      status: "available",
      latency,
    };
  } catch (error) {
    return {
      status: "unavailable",
      latency: Date.now() - startTime,
      error: error instanceof Error ? error.message : "Unknown error",
    };
  }
}

/**
 * Check backend service connectivity
 */
export async function checkBackend(): Promise<ServiceStatus> {
  const config = getConfig();
  const startTime = Date.now();

  try {
    const response = await fetch(`${config.backend.apiUrl}/health`, {
      timeout: 5000,
    });

    const latency = Date.now() - startTime;

    if (!response.ok) {
      return {
        status: "degraded",
        latency,
        error: `HTTP ${response.status}`,
      };
    }

    return {
      status: "available",
      latency,
    };
  } catch (error) {
    return {
      status: "unavailable",
      latency: Date.now() - startTime,
      error: error instanceof Error ? error.message : "Connection failed",
    };
  }
}

/**
 * Get overall health status
 */
export async function getHealthStatus(): Promise<HealthStatus> {
  const startTime = process.uptime();

  const [dbStatus, backendStatus] = await Promise.all([
    checkDatabase(),
    checkBackend(),
  ]);

  // Determine overall status
  let overallStatus: "healthy" | "degraded" | "unhealthy";

  if (dbStatus.status === "available" && backendStatus.status === "available") {
    overallStatus = "healthy";
  } else if (
    (dbStatus.status === "available" || dbStatus.status === "degraded") &&
    (backendStatus.status === "available" || backendStatus.status === "degraded")
  ) {
    overallStatus = "degraded";
  } else {
    overallStatus = "unhealthy";
  }

  return {
    status: overallStatus,
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    services: {
      database: dbStatus,
      backend: backendStatus,
    },
    version: "1.0.0",
  };
}

/**
 * Generate health check report
 */
export function generateHealthReport(health: HealthStatus): string {
  return `
Health Status Report
${new Date(health.timestamp).toLocaleString()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Overall Status: ${health.status.toUpperCase()}
Version: ${health.version}
Uptime: ${Math.floor(health.uptime / 60)}m ${Math.floor(health.uptime % 60)}s

Services:
  Database:
    Status: ${health.services.database.status}
    Latency: ${health.services.database.latency}ms
    ${health.services.database.error ? `Error: ${health.services.database.error}` : ""}
  
  Backend:
    Status: ${health.services.backend.status}
    Latency: ${health.services.backend.latency}ms
    ${health.services.backend.error ? `Error: ${health.services.backend.error}` : ""}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  `;
}
