/**
 * Shared TypeScript Types
 * Used across backend and frontend
 */

// ============ USER & AUTH ============

export type UserType = "public" | "municipality";

export interface User {
  id: string;
  email: string;
  user_type: UserType;
  created_at: string;
  updated_at?: string;
}

export interface AuthSession {
  access_token: string;
  refresh_token?: string;
  user: User;
  expires_in: number;
}

// ============ CHAT & MESSAGES ============

export type MessageRole = "user" | "ai";

export interface ChatMessage {
  id?: string;
  role: MessageRole;
  content: string;
  timestamp?: string;
  user_id?: string;
}

export interface ChatRequest {
  message: string;
  userType: UserType;
  context?: Record<string, any>;
}

export interface ChatResponse {
  id?: string;
  message: string;
  type: "normal" | "ambiguity" | "error";
  userType: UserType;
  options?: Array<{
    label: string;
    value: string;
  }>;
  contradictions?: string[];
  updateDashboard?: boolean;
  metadata?: Record<string, any>;
}

// ============ AMBIGUITY DETECTION ============

export interface AmbiguityDetection {
  question: string;
  options: Array<{
    label: string;
    value: string;
    description?: string;
  }>;
  confidence: number;
}

// ============ CONTRADICTION ============

export interface Contradiction {
  field: string;
  dataSource1: string;
  value1: any;
  dataSource2: string;
  value2: any;
  severity: "low" | "medium" | "high";
  description: string;
}

// ============ DASHBOARD DATA ============

export interface HeatmapData {
  coordinates: [number, number];
  intensity: number;
  label?: string;
  metadata?: Record<string, any>;
}

export interface WordCloudWord {
  text: string;
  frequency: number;
  category?: string;
}

export interface WeatherCorrelationPoint {
  timestamp: string;
  precipitation?: number;
  temperature?: number;
  condition?: string;
  incidents: number;
}

export interface DashboardData {
  heatmapData: HeatmapData[];
  wordCloudData: WordCloudWord[];
  weatherCorrelation: WeatherCorrelationPoint[];
  userType: UserType;
}

// ============ BRIEFING ============

export interface BriefingHotspot {
  rank: number;
  name: string;
  count: number;
  trend: number;
  severity: "low" | "medium" | "high";
}

export interface BriefingTrend {
  category: string;
  change: number;
  direction: "up" | "down";
  severity: string;
}

export interface Briefing {
  date: string;
  topHotspots: BriefingHotspot[];
  trends: BriefingTrend[];
  recommendations: string[];
  highlights: string;
}

// ============ API RESPONSES ============

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
  timestamp?: string;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

// ============ ERRORS ============

export interface ApiError {
  code: string;
  message: string;
  details?: Record<string, any>;
  timestamp: string;
}

// ============ FILTERS & QUERIES ============

export interface QueryFilters {
  startDate?: string;
  endDate?: string;
  location?: string;
  severity?: string;
  category?: string;
  userType?: UserType;
}

export interface SortOptions {
  field: string;
  direction: "asc" | "desc";
}
