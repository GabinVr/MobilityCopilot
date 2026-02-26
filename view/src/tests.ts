import { describe, it, expect, beforeAll, afterAll } from "bun:test";

/**
 * Auth Service Tests
 */
describe("Auth Service", () => {
  it("should signup a new user", async () => {
    const response = await fetch("http://localhost:3000/auth/signup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: "test@example.com",
        password: "securePassword123",
        userType: "public",
      }),
    });

    expect(response.status).toBe(200);
  });

  it("should login existing user", async () => {
    const response = await fetch("http://localhost:3000/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: "test@example.com",
        password: "securePassword123",
      }),
    });

    expect(response.status).toBe(200);
    const cookies = response.headers.get("set-cookie");
    expect(cookies).toContain("auth_token");
  });

  it("should reject invalid credentials", async () => {
    const response = await fetch("http://localhost:3000/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: "test@example.com",
        password: "wrongPassword",
      }),
    });

    expect(response.status).toBeGreaterThanOrEqual(400);
  });
});

/**
 * Dashboard Tests
 */
describe("Dashboard", () => {
  it("should load dashboard page with valid session", async () => {
    const response = await fetch("http://localhost:3000/dashboard");
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/html");
  });

  it("should redirect to login without session", async () => {
    const response = await fetch("http://localhost:3000/dashboard", {
      redirect: "manual",
    });
    expect(response.status).toBe(302);
    expect(response.headers.get("location")).toContain("/auth/login");
  });
});

/**
 * Chat API Tests
 */
describe("Chat API", () => {
  it("should accept chat messages", async () => {
    const response = await fetch("http://localhost:3000/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: "Show me collision hotspots",
        userType: "public",
      }),
    });

    expect(response.status).toBe(200);
    const data = await response.json();
    expect(data.message).toBeDefined();
    expect(data.userType).toBe("public");
  });
});

/**
 * Dashboard Data API Tests
 */
describe("Dashboard Data API", () => {
  it("should fetch data for public user type", async () => {
    const response = await fetch(
      "http://localhost:3000/api/dashboard-data/public"
    );

    expect(response.status).toBe(200);
    const data = await response.json();
    expect(data.heatmapData).toBeDefined();
    expect(data.wordCloudData).toBeDefined();
    expect(data.userType).toBe("public");
  });

  it("should fetch data for municipality user type", async () => {
    const response = await fetch(
      "http://localhost:3000/api/dashboard-data/municipality"
    );

    expect(response.status).toBe(403);
  });
});

/**
 * Page Structure Tests
 */
describe("Page Structure", () => {
  it("should have valid auth page structure", async () => {
    const response = await fetch("http://localhost:3000/auth/login");
    const html = await response.text();

    expect(html).toContain("MobilityCopilot");
    expect(html).toContain("<form");
    expect(html).toContain("email");
    expect(html).toContain("password");
  });

  it("should have valid signup page structure", async () => {
    const response = await fetch("http://localhost:3000/auth/signup");
    const html = await response.text();

    expect(html).toContain("user-type-selector");
    expect(html).toContain("public");
    expect(html).toContain("municipality");
  });

  it("should load CSS and JavaScript", async () => {
    const response = await fetch("http://localhost:3000/dashboard");
    const html = await response.text();

    expect(html).toContain("style.css");
    expect(html).toContain("colors-public.css");
    expect(html).toContain("dashboard.js");
    expect(html).toContain("htmx");
  });
});
