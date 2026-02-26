import { Elysia, t } from "elysia";
import { html } from "@elysiajs/html";
import { staticPlugin } from "@elysiajs/static";
import { createClient } from "@supabase/supabase-js";

// Environment variables
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";
const BACKEND_API_URL = (process.env.BACKEND_API_URL || "http://localhost:8000").replace(/\/$/, "");

// Check if Supabase is configured
const isSupabaseConfigured = SUPABASE_URL && SUPABASE_ANON_KEY && 
  !SUPABASE_URL.includes("your-project") && 
  !SUPABASE_ANON_KEY.includes("your-anon-key");

// Initialize Supabase (with dummy values if not configured)
const supabase = createClient(
  SUPABASE_URL || "https://demo.supabase.co",
  SUPABASE_ANON_KEY || "demo-key"
);

function htmlResponse(body: string, init?: ResponseInit): Response {
  return new Response(body, {
    ...init,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      ...(init?.headers || {}),
    },
  });
}

function getCookieValue(cookies: string, name: string): string | null {
  const cookie = cookies
    .split(";")
    .map((item) => item.trim())
    .find((item) => item.startsWith(`${name}=`));

  return cookie ? cookie.split("=")[1] : null;
}

async function resolveUserType(request: Request): Promise<"public" | "municipality"> {
  if (!isSupabaseConfigured) {
    return "public";
  }

  const cookies = request?.headers.get("cookie") || "";
  const authToken = getCookieValue(cookies, "auth_token");
  if (!authToken) {
    return "public";
  }

  const { data, error } = await supabase.auth.getUser(authToken);
  if (error || !data?.user) {
    return "public";
  }

  const userType = data.user.user_metadata?.user_type;
  return userType === "municipality" ? "municipality" : "public";
}

async function isAuthenticated(request: Request): Promise<boolean> {
  if (!isSupabaseConfigured) {
    return true;
  }

  const cookies = request?.headers.get("cookie") || "";
  const authToken = getCookieValue(cookies, "auth_token");
  if (!authToken) {
    return false;
  }

  const { data, error } = await supabase.auth.getUser(authToken);
  return !error && Boolean(data?.user);
}

function formatDateInput(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function timeRangeToDates(timeRange: string): { startDate: string; endDate: string } {
  const today = new Date();

  if (timeRange === "last_week") {
    const start = new Date(today);
    start.setDate(today.getDate() - 7);
    return { startDate: formatDateInput(start), endDate: formatDateInput(today) };
  }

  if (timeRange === "last_month") {
    const start = new Date(today);
    start.setDate(today.getDate() - 30);
    return { startDate: formatDateInput(start), endDate: formatDateInput(today) };
  }

  const match = timeRange.match(/(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})/);
  if (match) {
    return { startDate: match[1], endDate: match[2] };
  }

  const startFallback = new Date(today);
  startFallback.setDate(today.getDate() - 30);
  return { startDate: formatDateInput(startFallback), endDate: formatDateInput(today) };
}

// Type definitions
interface User {
  id: string;
  email: string;
  user_type: "public" | "municipality";
  created_at: string;
}

interface AuthSession {
  access_token: string;
  user: User;
}

const app = new Elysia()
  // HTML plugin
  .use(html())
  // Static files
  .use(staticPlugin({ prefix: "/public" }))
  // Auth guard
  .onBeforeHandle(async ({ request }) => {
    const { pathname } = new URL(request.url);
    const isPublicRoute =
      pathname === "/" ||
      pathname.startsWith("/auth/") ||
      pathname.startsWith("/public/");

    if (isPublicRoute) {
      return;
    }

    const authed = await isAuthenticated(request);
    if (authed) {
      return;
    }

    return new Response("", {
      status: 302,
      headers: { Location: "/auth/login" },
    });
  })

  // Routes
  .get("/", ({ request }) => {
    const cookies = request?.headers.get("cookie") || "";
    const hasSession = cookies.includes("auth_token");

    if (hasSession) {
      return new Response("", {
        status: 302,
        headers: { Location: "/dashboard" },
      });
    }
    return new Response("", {
      status: 302,
      headers: { Location: "/auth/login" },
    });
  })

  // Auth Routes
  .get("/auth/login", () => htmlResponse(LoginPage()))
  .get("/auth/signup", () => htmlResponse(SignupPage()))
  .post(
    "/auth/login",
    async ({ body, query }) => {
      const { email, password } = body as {
        email: string;
        password: string;
      };

      // Demo mode - bypass authentication
      if (!isSupabaseConfigured) {
        const response = new Response("", {
          status: 302,
          headers: { Location: "/dashboard" },
        });

        response.headers.set(
          "Set-Cookie",
          `auth_token=demo_token; Path=/; HttpOnly; SameSite=Strict`
        );

        return response;
      }

      try {
        const { data, error } = await supabase.auth.signInWithPassword({
          email,
          password,
        });

        if (error) {
          return htmlResponse(LoginPage({ error: error.message }));
        }

        // Set cookie with session
        const response = new Response("", {
          status: 302,
          headers: { Location: "/dashboard" },
        });

        response.headers.set(
          "Set-Cookie",
          `auth_token=${data.session?.access_token}; Path=/; HttpOnly; Secure; SameSite=Strict`
        );

        return response;
      } catch (err) {
        return htmlResponse(LoginPage({ error: "Une erreur s'est produite" }));
      }
    },
    {
      body: t.Object({
        email: t.String({ format: "email" }),
        password: t.String({ minLength: 6 }),
      }),
    }
  )

  .post(
    "/auth/signup",
    async ({ body }) => {
      const { email, password, userType } = body as {
        email: string;
        password: string;
        userType: "public" | "municipality";
      };

      // Demo mode - bypass authentication
      if (!isSupabaseConfigured) {
        return htmlResponse(
          SuccessMessage(
            "Mode démo activé - Connexion automatique !"
          ),
          {
            headers: {
              "Set-Cookie": `auth_token=demo_token; Path=/; HttpOnly; SameSite=Strict`,
            },
          }
        );
      }

      try {
        // Sign up user
        const { data: authData, error: authError } =
          await supabase.auth.signUp({
            email,
            password,
            options: {
              data: {
                user_type: userType,
              },
            },
          });

        if (authError) {
          return htmlResponse(SignupPage({ error: authError.message }));
        }

        if (authData.user?.id) {
          const { error: profileError } = await supabase
            .from("users")
            .insert([
              {
                id: authData.user.id,
                email,
                user_type: userType,
              },
            ]);

          if (profileError) {
            console.warn("Profile creation failed:", profileError.message);
          }
        }

        return htmlResponse(
          SuccessMessage(
            "Inscription réussie! Un email de confirmation vous a été envoyé."
          )
        );
      } catch (err) {
        return htmlResponse(
          SignupPage({
            error: "Une erreur s'est produite lors de l'inscription",
          })
        );
      }
    },
    {
      body: t.Object({
        email: t.String({ format: "email" }),
        password: t.String({ minLength: 6 }),
        userType: t.Union([t.Literal("public"), t.Literal("municipality")]),
      }),
    }
  )

  .get("/auth/logout", () => {
    const response = new Response("", {
      status: 302,
      headers: { Location: "/auth/login" },
    });
    response.headers.set("Set-Cookie", "auth_token=; Path=/; Max-Age=0");
    response.headers.append("Set-Cookie", "chat_thread_id=; Path=/; Max-Age=0");
    return response;
  })
  .post("/auth/logout", () => {
    const response = new Response("", {
      status: 302,
      headers: { Location: "/auth/login" },
    });
    response.headers.set("Set-Cookie", "auth_token=; Path=/; Max-Age=0");
    response.headers.append("Set-Cookie", "chat_thread_id=; Path=/; Max-Age=0");
    return response;
  })

  // Dashboard Route
  .get("/dashboard", async ({ request }) => {
    const userType = await resolveUserType(request);
    return DashboardPage(userType);
  })

  // API Routes for Chat/Dashboard updates
  .post("/api/chat", async ({ body, request }) => {
    const { message, selectedOption, thread_id: bodyThreadId } = body as { message?: string; selectedOption?: string; thread_id?: string };
    const userType = await resolveUserType(request);
    const audience = userType === "municipality" ? "municipalite" : "grand_public";
    const cookies = request?.headers.get("cookie") || "";
    const cookieThreadId = getCookieValue(cookies, "chat_thread_id") || undefined;
    // Use body thread_id as fallback if cookie is not available
    const threadId = cookieThreadId || bodyThreadId || undefined;

    // Use selectedOption if provided (user clicked on clarification), otherwise use message
    const queryText = selectedOption || message || "";
    if (!queryText) {
      return new Response(
        `<div class="chat-message user"><div class="message-content">Erreur: Message vide</div></div>`,
        { status: 200, headers: { "Content-Type": "text/html" } }
      );
    }

    let htmlResponse_str = "";

    try {
      const response = await fetch(`${BACKEND_API_URL}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: queryText,
          audience,
          thread_id: threadId,
        }),
      });

      if (!response.ok) {
        return new Response(
          `
            <div class="chat-message ai">
              <div class="message-content error">❌ Erreur backend: ${response.status}</div>
            </div>
          `,
          { status: 200, headers: { "Content-Type": "text/html" } }
        );
      }

      const data = await response.json();
      
      if (data.is_ambiguous) {
        // Parse options from answer (newline-separated string)
        const options = (data.answer || "")
          .split("\n")
          .map((opt: string) => opt.trim())
          .filter((opt: string) => opt.length > 0);

        const optionsHtml = options
          .map(
            (opt: string) => `
              <button 
                class="clarification-chip" 
                hx-post="/api/chat"
                hx-target="#chat-history"
                hx-swap="beforeend"
                hx-vals='{"selectedOption": "${opt.replace(/"/g, '\\"')}"}'
              >
                ${opt}
              </button>
            `
          )
          .join("");

        htmlResponse_str += `
          <div class="chat-message ai">
            <div class="message-content">
              <p>Pourriez-vous clarifier votre question?</p>
              <div class="clarification-options">
                ${optionsHtml}
              </div>
            </div>
          </div>
        `;
      } else {
        // Normal response
        const answer = data.answer || "Réponse vide";
        const notes = data.contradictor_notes 
          ? `<div class="warning-note">⚠️ ${data.contradictor_notes}</div>` 
          : "";
        
        // Escape HTML entities to prevent XSS in raw markdown
        const escapedAnswer = answer.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

        htmlResponse_str += `
          <div class="chat-message ai" data-thread-id="${data.thread_id || ''}">
            <div class="message-content">
              <div class="markdown-source" style="display:none;">${escapedAnswer}</div>
              <div class="markdown-rendered"></div>
              ${notes}
            </div>
          </div>
        `;
      }

      const chatResponse = new Response(htmlResponse_str, {
        status: 200,
        headers: { "Content-Type": "text/html" },
      });

      if (data?.thread_id) {
        chatResponse.headers.set(
          "Set-Cookie",
          `chat_thread_id=${data.thread_id}; Path=/; HttpOnly; SameSite=Strict`
        );
      }

      return chatResponse;
    } catch (error) {
      return new Response(
        `
          <div class="chat-message ai">
            <div class="message-content error">❌ Erreur: ${(error as Error).message}</div>
          </div>
        `,
        { status: 200, headers: { "Content-Type": "text/html" } }
      );
    }
  })

  // Endpoint for heatmap and weather correlation (same date picker)
  .get("/api/heatmap-weather/:userType", async ({ params, request }) => {
    const resolvedUserType = await resolveUserType(request);
    const { userType } = params as { userType: "public" | "municipality" };

    const url = new URL(request.url);
    const timeRange = url.searchParams.get("timeRange") || "2015-01-01 to 2015-12-31";
    const startDateParam = url.searchParams.get("startDate") || "";
    const endDateParam = url.searchParams.get("endDate") || "";
    const severityFilterRaw = url.searchParams.get("severity");
    const severityFilter = severityFilterRaw && severityFilterRaw !== "all"
      ? Number(severityFilterRaw)
      : undefined;

    if (userType !== resolvedUserType) {
      return new Response(
        JSON.stringify({ message: "Forbidden" }),
        {
          status: 403,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    const dateRange = startDateParam && endDateParam
      ? { startDate: startDateParam, endDate: endDateParam }
      : timeRangeToDates(timeRange);

    const [heatmapRes, weatherRes] = await Promise.all([
      fetch(`${BACKEND_API_URL}/dashboard/collision-heatmap`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          time_range: startDateParam && endDateParam
            ? `${startDateParam} to ${endDateParam}`
            : timeRange,
          severity_filter: Number.isFinite(severityFilter) ? severityFilter : undefined,
        }),
      }),
      fetch(`${BACKEND_API_URL}/dashboard/weather-correlation`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          start_date: dateRange.startDate,
          end_date: dateRange.endDate,
          frequency: "week",
        }),
      }),
    ]);

    let heatmapData = null;
    let weatherCorrelation = null;

    if (heatmapRes.ok) {
      heatmapData = await heatmapRes.json();
    }

    if (weatherRes.ok) {
      weatherCorrelation = await weatherRes.json();
    }

    return {
      heatmapData,
      weatherCorrelation,
      userType: resolvedUserType,
    };
  })

  // Endpoint for wordcloud (separate date picker)
  .get("/api/wordcloud/:userType", async ({ params, request }) => {
    const resolvedUserType = await resolveUserType(request);
    const { userType } = params as { userType: "public" | "municipality" };

    const url = new URL(request.url);
    const wordRange = url.searchParams.get("wordRange") || "last_month";

    if (userType !== resolvedUserType) {
      return new Response(
        JSON.stringify({ message: "Forbidden" }),
        {
          status: 403,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    try {
      const wordCloudRes = await fetch(`${BACKEND_API_URL}/dashboard/wordcloud-311`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          top_n: 10,
          time_range: wordRange,
        }),
      });

      let wordCloudData = null;
      if (wordCloudRes.ok) {
        wordCloudData = await wordCloudRes.json();
      }

      return {
        wordCloudData,
        userType: resolvedUserType,
      };
    } catch (error) {
      return new Response(
        JSON.stringify({ message: "Erreur backend" }),
        {
          status: 502,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  })

  // Backward compatibility endpoint - combines heatmap-weather and wordcloud
  .get("/api/dashboard-data/:userType", async ({ params, request }) => {
    const resolvedUserType = await resolveUserType(request);
    const { userType } = params as { userType: "public" | "municipality" };

    const url = new URL(request.url);
    const timeRange = url.searchParams.get("timeRange") || "2015-01-01 to 2015-12-31";
    const startDateParam = url.searchParams.get("startDate") || "";
    const endDateParam = url.searchParams.get("endDate") || "";
    const wordRange = url.searchParams.get("wordRange") || "last_month";
    const severityFilterRaw = url.searchParams.get("severity");
    const severityFilter = severityFilterRaw && severityFilterRaw !== "all"
      ? Number(severityFilterRaw)
      : undefined;

    if (userType !== resolvedUserType) {
      return new Response(
        JSON.stringify({ message: "Forbidden" }),
        {
          status: 403,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    const dateRange = startDateParam && endDateParam
      ? { startDate: startDateParam, endDate: endDateParam }
      : timeRangeToDates(timeRange);

    try {
      const [heatmapRes, weatherRes, wordCloudRes] = await Promise.all([
        fetch(`${BACKEND_API_URL}/dashboard/collision-heatmap`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            time_range: startDateParam && endDateParam
              ? `${startDateParam} to ${endDateParam}`
              : timeRange,
            severity_filter: Number.isFinite(severityFilter) ? severityFilter : undefined,
          }),
        }),
        fetch(`${BACKEND_API_URL}/dashboard/weather-correlation`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            start_date: dateRange.startDate,
            end_date: dateRange.endDate,
            frequency: "week",
          }),
        }),
        fetch(`${BACKEND_API_URL}/dashboard/wordcloud-311`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            top_n: 10,
            time_range: wordRange,
          }),
        }),
      ]);

      let heatmapData = null;
      let weatherCorrelation = null;
      let wordCloudData = null;

      if (heatmapRes.ok) {
        heatmapData = await heatmapRes.json();
      }

      if (weatherRes.ok) {
        weatherCorrelation = await weatherRes.json();
      }

      if (wordCloudRes.ok) {
        wordCloudData = await wordCloudRes.json();
      }

      return {
        heatmapData,
        wordCloudData,
        weatherCorrelation,
        userType: resolvedUserType,
      };
    } catch (error) {
      return new Response(
        JSON.stringify({ message: "Erreur backend" }),
        {
          status: 502,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  })

  .listen(3000, () => {
    console.log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("🚀 MobilityCopilot Web running at http://localhost:3000");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    if (!isSupabaseConfigured) {
      console.log("\n⚠️  MODE DÉMO ACTIVÉ");
      console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      console.log("Supabase n'est pas configuré.");
      console.log("L'application fonctionne en mode démo sans authentification.");
      console.log("\nPour activer l'authentification:");
      console.log("  1. Créez un projet sur https://supabase.com");
      console.log("  2. Créez un fichier .env avec:");
      console.log("     SUPABASE_URL=https://your-project.supabase.co");
      console.log("     SUPABASE_ANON_KEY=your-anon-key");
      console.log("  3. Redémarrez l'application");
      console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    } else {
      console.log("\n✅ Supabase configuré");
      console.log(`   URL: ${SUPABASE_URL}`);
      console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    }
  });

// ============ COMPONENTS ============

function BaseLayout(
  content: string,
  userType: "public" | "municipality" = "public"
) {
  return `<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MobilityCopilot</title>
    <script src="/public/js/language.js"></script>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <script src="https://cdn.jsdelivr.net/npm/marked@14.1.1/lib/marked.umd.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dompurify@3.0.9/dist/purify.min.js"></script>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
    <link rel="stylesheet" href="/public/css/style.css">
    <link rel="stylesheet" href="/public/css/colors-${userType}.css">
</head>
<body data-user-type="${userType}">
    ${content}
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
</body>
</html>`;
}

function LoginPage(props?: { error?: string }) {
  const demoWarning = !isSupabaseConfigured ? `
    <div style="background-color: #FEF3C7; color: #78350F; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; font-size: 0.875rem;">
      <strong>⚠️ Mode Démo</strong><br>
      Vous pouvez vous connecter avec n'importe quel email/mot de passe.
    </div>
  ` : "";

  return BaseLayout(`
    <div class="auth-container">
      <div class="auth-card">
        <div class="auth-header">
          <h1>🚗 MobilityCopilot</h1>
          <p>Connectez-vous pour accéder au tableau de bord</p>
        </div>

        ${demoWarning}
        ${props?.error ? `<div class="error-message">${props.error}</div>` : ""}

        <form method="post" action="/auth/login" class="auth-form">
          <div class="form-group">
            <label for="email">Email</label>
            <input 
              type="email" 
              id="email" 
              name="email" 
              required
              autocomplete="email"
              placeholder="votre@email.com"
            />
          </div>

          <div class="form-group">
            <label for="password">Mot de passe</label>
            <input 
              type="password" 
              id="password" 
              name="password" 
              required
              autocomplete="current-password"
              placeholder="••••••••"
            />
          </div>

          <button type="submit" class="btn btn-primary">
            Se connecter
          </button>
        </form>

        <div class="auth-footer">
          <p>Pas encore de compte? 
            <a href="/auth/signup">S'inscrire</a>
          </p>
        </div>
      </div>
    </div>
  `);
}

function SignupPage(props?: { error?: string }) {
  const demoWarning = !isSupabaseConfigured ? `
    <div style="background-color: #FEF3C7; color: #78350F; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; font-size: 0.875rem;">
      <strong>⚠️ Mode Démo</strong><br>
      Vous pouvez créer un compte avec n'importe quel email/mot de passe.
    </div>
  ` : "";

  return BaseLayout(`
    <div class="auth-container">
      <div class="auth-card">
        <div class="auth-header">
          <h1>🚗 MobilityCopilot</h1>
          <p>Créez votre compte</p>
        </div>

        ${demoWarning}
        ${props?.error ? `<div class="error-message">${props.error}</div>` : ""}

        <form method="post" action="/auth/signup" class="auth-form">
          <div class="form-group">
            <label for="email">Email</label>
            <input 
              type="email" 
              id="email" 
              name="email" 
              required
              autocomplete="email"
              placeholder="votre@email.com"
            />
          </div>

          <div class="form-group">
            <label for="password">Mot de passe</label>
            <input 
              type="password" 
              id="password" 
              name="password" 
              required
              minlength="6"
              autocomplete="new-password"
              placeholder="Au moins 6 caractères"
            />
          </div>

          <div class="form-group">
            <label for="userType">Type d'utilisateur</label>
            <div class="user-type-selector">
              <input 
                type="radio" 
                id="public" 
                name="userType" 
                value="public"
                checked
              />
              <label for="public" class="radio-label">
                <span class="icon">👥</span>
                <span>Grand Public</span>
                <span class="description">Information sur la mobilité</span>
              </label>

              <input 
                type="radio" 
                id="municipality" 
                name="userType" 
                value="municipality"
              />
              <label for="municipality" class="radio-label">
                <span class="icon">🏛️</span>
                <span>Municipalité</span>
                <span class="description">Analyse opérationnelle</span>
              </label>
            </div>
          </div>

          <button type="submit" class="btn btn-primary">
            S'inscrire
          </button>
        </form>

        <div class="auth-footer">
          <p>Vous avez un compte? 
            <a href="/auth/login">Se connecter</a>
          </p>
        </div>
      </div>
    </div>
  `);
}

function DashboardPage(userType: "public" | "municipality") {
  const modeLabel = userType === "municipality" ? "Municipalité" : "Public";
  return BaseLayout(`
    <!-- Mobile Tab Navigation -->
    <div class="mobile-tabs">
      <button class="tab-button active" data-tab="chat" onclick="switchMobileTab('chat')">
        <span class="tab-icon">💬</span>
        <span class="tab-label">Chat</span>
      </button>
      <button class="tab-button" data-tab="dashboard" onclick="switchMobileTab('dashboard')">
        <span class="tab-icon">📊</span>
        <span class="tab-label">Dashboard</span>
      </button>
    </div>

    <div class="dashboard-container">
      <!-- Left Panel: Chat -->
      <div class="chat-panel active-panel">
        <div class="chat-header">
          <h2>Assistant MobilityCopilot</h2>
          <div class="chat-header-actions">
            <div class="mode-indicator">
              <span class="toggle-label">Mode: <span id="mode-display">${modeLabel}</span></span>
            </div>
            <form method="post" action="/auth/logout" class="logout-form-mobile">
              <button class="btn-logout" type="submit">
                Déconnexion
              </button>
            </form>
          </div>
        </div>

        <div class="chat-history" id="chat-history">
          <div class="chat-message ai">
            <div class="message-content">
              Bonjour! Je suis votre assistant MobilityCopilot. Comment puis-je vous aider aujourd'hui?
            </div>
          </div>
        </div>

        <div class="chat-input-area">
          <form id="chat-form" hx-post="/api/chat" hx-target="#chat-history" hx-swap="beforeend" class="chat-form">
            <input type="hidden" name="thread_id" id="chat-thread-id" value="" />
            <input 
              type="text" 
              name="message" 
              placeholder="Posez votre question..."
              class="chat-input"
              autocomplete="off"
              required
            />
            <button type="submit" class="btn-send">
              <span>➤</span>
            </button>
          </form>
        </div>
      </div>

      <!-- Right Panel: Dashboard -->
      <div class="dashboard-panel">
        <div class="dashboard-header">
          <h2>Tableau de Bord</h2>
          <form method="post" action="/auth/logout">
            <button class="btn-logout" type="submit">
              Déconnexion
            </button>
          </form>
        </div>

        <div class="dashboard-grid">
          <!-- Card 1: Heatmap -->
          <div class="dashboard-card heatmap-card">
            <div class="card-header-row">
              <h3>Carte des Collisions</h3>
            </div>
            <div class="card-controls dashboard-filters">
              <div class="filter-group">
                <label for="start-date" class="filter-label">Debut</label>
                <input id="start-date" type="date" class="filter-input" />
              </div>
              <div class="filter-group">
                <label for="end-date" class="filter-label">Fin</label>
                <input id="end-date" type="date" class="filter-input" />
              </div>
              <div class="filter-group">
                <label for="severity-filter" class="filter-label">Gravite</label>
                <select id="severity-filter" class="filter-select">
                  <option value="all" selected>Toutes</option>
                  <option value="4">Mortel</option>
                  <option value="3">Grave</option>
                  <option value="2">Leger</option>
                  <option value="1">Dommages</option>
                  <option value="0">Materiel</option>
                </select>
              </div>
              <button class="filter-button" id="apply-filters" type="button">Appliquer</button>
            </div>

            <div id="heatmap-container" class="map-container">
              <!-- Mapbox/Leaflet will be mounted here -->
              <div class="placeholder">Chargement de la carte...</div>
            </div>
          </div>

          <!-- Card 2: Weather Correlation -->
          <div class="dashboard-card weather-card">
            <h3>Corrélation Météo</h3>
            <div id="weather-chart-container" class="chart-container">
              <div class="placeholder">Chargement du graphique...</div>
            </div>
          </div>

          <!-- Card 3: Word Cloud -->
          <div class="dashboard-card wordcloud-card">
            <div class="card-header-row">
              <h3>Requêtes 311</h3>
              <div class="card-filters">
                <select id="wordcloud-range" class="filter-select">
                  <option value="last_week">Derniere semaine</option>
                  <option value="last_month" selected>Dernier mois</option>
                </select>
                <button class="filter-button subtle" id="apply-wordcloud" type="button">Appliquer</button>
              </div>
            </div>
            <div id="wordcloud-container" class="chart-container">
              <div class="placeholder">Chargement du nuage de mots...</div>
            </div>
          </div>

          <!-- Card 4: Trends -->
          <div class="dashboard-card trends-card">
            <h3>Tendances</h3>
            <div class="trends-filters">
              <input 
                type="date" 
                id="trends-date" 
                class="filter-input" 
                title="Date pour laquelle calculer les tendances"
              />
              <button class="filter-button subtle" id="apply-trends" type="button">Appliquer</button>
            </div>
            <div id="trends-container" class="chart-container">
              <div class="placeholder">Chargement des tendances...</div>
            </div>
          </div>

          <!-- Card 5: Weekly Reports -->
          <div class="dashboard-card weekly-reports-card">
            <h3>Rapports Hebdomadaires</h3>
            <div id="weekly-reports-container" class="chart-container">
              <div class="placeholder">Chargement des rapports...</div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <script>
      // Mobile tab switching
      function switchMobileTab(tabName) {
        // Update button states
        document.querySelectorAll('.tab-button').forEach(btn => {
          btn.classList.toggle('active', btn.dataset.tab === tabName);
        });
        
        // Show/hide panels
        const chatPanel = document.querySelector('.chat-panel');
        const dashboardPanel = document.querySelector('.dashboard-panel');
        
        if (tabName === 'chat') {
          chatPanel.classList.add('active-panel');
          dashboardPanel.classList.remove('active-panel');
        } else {
          chatPanel.classList.remove('active-panel');
          dashboardPanel.classList.add('active-panel');
        }
        
        // Store preference
        localStorage.setItem('activeMobileTab', tabName);
      }
      
      // Restore last active tab on page load (mobile only)
      document.addEventListener('DOMContentLoaded', function() {
        if (window.innerWidth <= 1024) {
          const lastTab = localStorage.getItem('activeMobileTab') || 'chat';
          switchMobileTab(lastTab);
        }
      });
      
      // Handle window resize
      window.addEventListener('resize', function() {
        if (window.innerWidth > 1024) {
          // Desktop: show both panels
          document.querySelector('.chat-panel').classList.add('active-panel');
          document.querySelector('.dashboard-panel').classList.add('active-panel');
        } else {
          // Mobile: restore saved tab
          const lastTab = localStorage.getItem('activeMobileTab') || 'chat';
          switchMobileTab(lastTab);
        }
      });
      
      // Export for global use
      window.switchMobileTab = switchMobileTab;
    </script>
    <script src="/public/js/dashboard.js?v=9"></script>
  `, userType);
}

function SuccessMessage(message: string) {
  return BaseLayout(`
    <div class="auth-container">
      <div class="auth-card success-card">
        <div class="success-icon">✓</div>
        <p class="success-message">${message}</p>
        <a href="/auth/login" class="btn btn-primary">
          Retour à la connexion
        </a>
      </div>
    </div>
  `);
}

export default app;
