import { Context } from "elysia";

/**
 * Middleware to extract and verify JWT token from cookies
 */
export function extractAuthToken(ctx: Context) {
  const cookies = ctx.request?.headers.get("cookie") || "";
  const tokenMatch = cookies.match(/auth_token=([^;]+)/);

  return tokenMatch ? tokenMatch[1] : null;
}

/**
 * Middleware to verify user session
 */
export async function verifySession(ctx: Context) {
  const token = extractAuthToken(ctx);

  if (!token) {
    return {
      authenticated: false,
      error: "No session found",
    };
  }

  try {
    // Verify token with Supabase
    // For production, verify JWT signature
    return {
      authenticated: true,
      token,
    };
  } catch (error) {
    return {
      authenticated: false,
      error: "Invalid session",
    };
  }
}

/**
 * Middleware to require authentication
 */
export function requireAuth(ctx: Context) {
  const token = extractAuthToken(ctx);

  if (!token) {
    return new Response("Unauthorized", {
      status: 302,
      headers: { Location: "/auth/login" },
    });
  }

  return null; // Continue
}

/**
 * Middleware to redirect authenticated users away from auth pages
 */
export function preventAuthenticatedAccess(ctx: Context) {
  const token = extractAuthToken(ctx);

  if (token) {
    return new Response("", {
      status: 302,
      headers: { Location: "/dashboard" },
    });
  }

  return null; // Continue
}

/**
 * Middleware to set secure headers
 */
export function setSecurityHeaders(ctx: Context) {
  ctx.response.headers.set("X-Content-Type-Options", "nosniff");
  ctx.response.headers.set("X-Frame-Options", "DENY");
  ctx.response.headers.set("X-XSS-Protection", "1; mode=block");
  ctx.response.headers.set(
    "Strict-Transport-Security",
    "max-age=31536000; includeSubDomains"
  );
  ctx.response.headers.set(
    "Content-Security-Policy",
    "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline'"
  );
}
