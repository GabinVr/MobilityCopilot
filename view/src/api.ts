/**
 * API Client Utilities
 * Handles communication with backend services
 */

export class ApiClient {
  private baseUrl: string;
  private authToken?: string;

  constructor(baseUrl = "/api") {
    this.baseUrl = baseUrl;
    this.loadToken();
  }

  /**
   * Load authentication token from storage
   */
  private loadToken() {
    if (typeof window !== "undefined") {
      const cookies = document.cookie.split(";");
      const tokenCookie = cookies.find((c) =>
        c.trim().startsWith("auth_token=")
      );
      if (tokenCookie) {
        this.authToken = tokenCookie.split("=")[1];
      }
    }
  }

  /**
   * Set authentication token
   */
  setToken(token: string) {
    this.authToken = token;
  }

  /**
   * Generic fetch wrapper with error handling
   */
  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    const headers: HeadersInit = {
      "Content-Type": "application/json",
      ...options.headers,
    };

    if (this.authToken) {
      headers["Authorization"] = `Bearer ${this.authToken}`;
    }

    try {
      const response = await fetch(url, {
        ...options,
        headers,
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || `HTTP Error: ${response.status}`);
      }

      if (response.status === 204) {
        return {} as T;
      }

      return await response.json();
    } catch (error) {
      console.error(`API Error [${endpoint}]:`, error);
      throw error;
    }
  }

  /**
   * GET request
   */
  async get<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: "GET" });
  }

  /**
   * POST request
   */
  async post<T>(endpoint: string, body: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: "POST",
      body: JSON.stringify(body),
    });
  }

  /**
   * PUT request
   */
  async put<T>(endpoint: string, body: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: "PUT",
      body: JSON.stringify(body),
    });
  }

  /**
   * PATCH request
   */
  async patch<T>(endpoint: string, body: any): Promise<T> {
    return this.request<T>(endpoint, {
      method: "PATCH",
      body: JSON.stringify(body),
    });
  }

  /**
   * DELETE request
   */
  async delete<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: "DELETE" });
  }
}

/**
 * Chat API methods
 */
export class ChatApi {
  private client: ApiClient;
  private threadId?: string;

  constructor(client: ApiClient) {
    this.client = client;
    this.loadThreadId();
  }

  /**
   * Load thread ID from localStorage if available
   */
  private loadThreadId() {
    if (typeof window !== "undefined") {
      const saved = localStorage.getItem("chat_thread_id");
      if (saved) {
        this.threadId = saved;
      }
    }
  }

  /**
   * Save thread ID to localStorage
   */
  private saveThreadId() {
    if (typeof window !== "undefined" && this.threadId) {
      localStorage.setItem("chat_thread_id", this.threadId);
    }
  }

  /**
   * Get current thread ID
   */
  getThreadId(): string | undefined {
    return this.threadId;
  }

  /**
   * Set thread ID explicitly (useful for continuing existing conversations)
   */
  setThreadId(threadId: string) {
    this.threadId = threadId;
    this.saveThreadId();
  }

  /**
   * Clear thread ID (start new conversation)
   */
  clearThreadId() {
    this.threadId = undefined;
    if (typeof window !== "undefined") {
      localStorage.removeItem("chat_thread_id");
    }
  }

  /**
   * Send chat message
   * Automatically manages thread_id: first message has no thread_id,
   * subsequent messages include it from previous responses
   */
  async sendMessage(query: string, audience: string = "grand_public") {
    const payload: any = { query, audience };
    
    // Include thread_id if we have one from a previous response
    if (this.threadId) {
      payload.thread_id = this.threadId;
    }

    const response = await this.client.post("/chat", payload);

    // Extract and store thread_id from response for next message
    if (response && response.thread_id) {
      this.threadId = response.thread_id;
      this.saveThreadId();
    }

    return response;
  }

  /**
   * Get chat history
   */
  async getHistory(limit = 50, offset = 0) {
    return this.client.get(`/chat/history?limit=${limit}&offset=${offset}`);
  }

  /**
   * Clear chat history
   */
  async clearHistory() {
    return this.client.delete("/chat/history");
  }
}

/**
 * Dashboard API methods
 */
export class DashboardApi {
  private client: ApiClient;

  constructor(client: ApiClient) {
    this.client = client;
  }

  /**
   * Get dashboard data
   */
  async getData(userType: string) {
    return this.client.get(`/dashboard-data/${userType}`);
  }

  /**
   * Get heatmap data
   */
  async getHeatmapData(filters = {}) {
    const query = new URLSearchParams(filters).toString();
    return this.client.get(
      `/dashboard/heatmap${query ? "?" + query : ""}`
    );
  }

  /**
   * Get trends data
   */
  async getTrendsData(userType: string, filters = {}) {
    const query = new URLSearchParams(filters).toString();
    return this.client.get(
      `/trends/${userType}${query ? "?" + query : ""}`
    );
  }

  /**
   * Get weekly reports
   */
  async getWeeklyReports(userType: string, filters = {}) {
    const query = new URLSearchParams(filters).toString();
    return this.client.get(
      `/weekly-reports/${userType}${query ? "?" + query : ""}`
    );
  }

  /**
   * Get word cloud data
   */
  async getWordCloudData(filters = {}) {
    const query = new URLSearchParams(filters).toString();
    return this.client.get(
      `/dashboard/wordcloud${query ? "?" + query : ""}`
    );
  }

  /**
   * Get weather correlation
   */
  async getWeatherCorrelation(filters = {}) {
    const query = new URLSearchParams(filters).toString();
    return this.client.get(
      `/dashboard/weather${query ? "?" + query : ""}`
    );
  }

  /**
   * Get briefing
   */
  async getBriefing() {
    return this.client.get("/dashboard/briefing");
  }

  /**
   * Update widget preferences
   */
  async updatePreferences(preferences: any) {
    return this.client.put("/dashboard/preferences", preferences);
  }
}

/**
 * Auth API methods
 */
export class AuthApi {
  private client: ApiClient;

  constructor(client: ApiClient) {
    this.client = client;
  }

  /**
   * Login
   */
  async login(email: string, password: string) {
    return this.client.post("/auth/login", { email, password });
  }

  /**
   * Signup
   */
  async signup(email: string, password: string, userType: string) {
    return this.client.post("/auth/signup", { email, password, userType });
  }

  /**
   * Logout
   */
  async logout() {
    return this.client.post("/auth/logout", {});
  }

  /**
   * Get current user
   */
  async getCurrentUser() {
    return this.client.get("/auth/user");
  }

  /**
   * Update profile
   */
  async updateProfile(data: any) {
    return this.client.put("/auth/profile", data);
  }
}

/**
 * Create API client instance
 */
let apiClient: ApiClient | null = null;

export function getApiClient(): ApiClient {
  if (!apiClient) {
    const baseUrl =
      typeof window !== "undefined"
        ? "/api"
        : process.env.BACKEND_API_URL || "http://localhost:8000/";
    apiClient = new ApiClient(baseUrl);
  }
  return apiClient;
}

/**
 * Convenience exports
 */
export const useChat = () => new ChatApi(getApiClient());
export const useDashboard = () => new DashboardApi(getApiClient());
export const useAuth = () => new AuthApi(getApiClient());
