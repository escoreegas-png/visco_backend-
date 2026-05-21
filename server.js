const dotenv = require("dotenv");
dotenv.config({ path: "./esco.env", override: true });


const express = require("express");
const cors = require("cors");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const runPipeline = require("./test2");

// ─────────────────────────────────────────────
// SUPABASE
// ─────────────────────────────────────────────


puppeteer.use(StealthPlugin());

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_KEY
);


// ─────────────────────────────────────────────
// APP
// ─────────────────────────────────────────────
const app = express();
const PORT = process.env.PORT || 3000;

app.set("trust proxy", 1);
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

app.use((req, res, next) => {
  const start = Date.now();
  res.on("finish", () => {
    console.log(
      `[${new Date().toISOString()}] ${req.method} ${req.path} → ${res.statusCode} (${Date.now() - start}ms)`
    );
  });
  next();
});

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function normalizeQuery(q) {
  return String(q || "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s\-]/g, "");
}

// In-memory guard so we don't re-launch the same job twice
const activeJobs = new Set();

// Cache TTL — re-scrape if older than this (in hours)
const CACHE_TTL_HOURS = parseInt(process.env.CACHE_TTL_HOURS || "24", 10);

function isCacheFresh(updatedAt) {
  if (!updatedAt) return false;
  const ageMs = Date.now() - new Date(updatedAt).getTime();
  return ageMs < CACHE_TTL_HOURS * 60 * 60 * 1000;
}

function cleanResults(rawResults) {
  const results = Array.isArray(rawResults) ? rawResults : [];
  return results.map((p) => ({
    canonical_name: p.canonical_name || p.name || "Unknown Product",
    name: p.name || "",
    image: p.image || null,
    best_price: typeof p.best_price === "number" ? p.best_price : null,
    best_source: p.best_source || null,
    prices: p.prices || {},
    links: p.links || {},
    category: p.category || "other",
    match_score: p.match_score || 0,
    confidence: p.confidence || "medium",
    canonical_id: p.canonical_id || null,
    platform_count: p.platform_count || Object.keys(p.prices || {}).length,
    extracted_attributes: p.extracted_attributes || {},
    price_insight: p.price_insight || null,
  }));
}

// ─────────────────────────────────────────────
// BACKGROUND WORKER
// ─────────────────────────────────────────────
async function runBackgroundSearch(query) {
  if (activeJobs.has(query)) {
    console.log(`⏭️  Job already running for "${query}", skipping duplicate launch`);
    return;
  }
  activeJobs.add(query);

  const startTime = Date.now();
  console.log(`\n🚀 [BG] Pipeline started for "${query}"`);

  try {
    const PIPELINE_TIMEOUT_MS = 8 * 60 * 1000;

    const rawResults = await Promise.race([
      runPipeline(query),
      new Promise((_, reject) =>
        setTimeout(
          () => reject(new Error(`Pipeline timeout after ${PIPELINE_TIMEOUT_MS / 1000}s`)),
          PIPELINE_TIMEOUT_MS
        )
      ),
    ]);

    const results = cleanResults(rawResults);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log(`✅ [BG] "${query}" complete: ${results.length} products in ${elapsed}s`);

    const { error } = await supabase
      .from("cached_results")
      .upsert(
        {
          query,
          results,
          status: "completed",
          updated_at: new Date().toISOString(),
        },
        { onConflict: "query" }
      );

    if (error) console.error(`❌ [BG] Supabase upsert failed:`, error.message);
  } catch (err) {
    console.error(`❌ [BG] Pipeline failed for "${query}":`, err.message);

    await supabase
      .from("cached_results")
      .upsert(
        {
          query,
          results: [],
          status: "failed",
          updated_at: new Date().toISOString(),
        },
        { onConflict: "query" }
      )
      .then(({ error }) => {
        if (error) console.error(`❌ [BG] Failed-status upsert error:`, error.message);
      });
  } finally {
    activeJobs.delete(query);
  }
}

// ─────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────
app.get("/health", (req, res) => {
  res.json({
    success: true,
    message: "Server running",
    uptime: process.uptime(),
    activeJobs: activeJobs.size,
    timestamp: new Date().toISOString(),
  });
});

// POST /search → cache-first, never waits for scraping
app.post("/search", async (req, res) => {
  const rawQuery = req.body?.query;
  const query = normalizeQuery(rawQuery);

  if (!query) {
    return res.status(400).json({ success: false, error: "Query is required" });
  }
  if (query.length < 2) {
    return res.status(400).json({ success: false, error: "Query must be at least 2 characters" });
  }
  if (query.length > 200) {
    return res.status(400).json({ success: false, error: "Query too long (max 200 chars)" });
  }

  try {
    const response = await fetch(
  "https://visco.onrender.com/products?q=" + encodeURIComponent(query)
);

const apiData = await response.json();

const data = {
  results: apiData.results || [],
  status: apiData.results?.length ? "completed" : "failed",
  updated_at: new Date().toISOString()
};

const error = null;

    if (error) {
      console.error("Supabase select error:", error.message);
      return res.status(500).json({ success: false, error: "Database error" });
    }

    // CASE 1: fresh completed cache → return instantly
    if (data && data.status === "completed" && isCacheFresh(data.updated_at)) {
      return res.json({
        success: true,
        cached: true,
        query,
        count: (data.results || []).length,
        results: data.results || [],
      });
    }

    // CASE 2: a job is already in flight
    if (data && data.status === "searching") {
      // Re-kick if it's been stuck for >10 minutes (orphaned job)
      const stuck = Date.now() - new Date(data.updated_at).getTime() > 10 * 60 * 1000;
      if (stuck) {
        setImmediate(() => runBackgroundSearch(query));
      }
      return res.json({ success: true, searching: true, query });
    }

    // CASE 3: no cache, stale cache, or previously failed → launch background job
    const { error: upsertError } = await supabase.from("cached_results").upsert(
      {
        query,
        results: data?.results || [],
        status: "searching",
        updated_at: new Date().toISOString(),
      },
      { onConflict: "query" }
    );

    if (upsertError) {
      console.error("Supabase upsert error:", upsertError.message);
      return res.status(500).json({ success: false, error: "Database error" });
    }

    // Fire-and-forget background scrape
    setImmediate(() => runBackgroundSearch(query));

    return res.json({ success: true, searching: true, query });
  } catch (err) {
    console.error("🔥 /search error:", err);
    return res.status(500).json({ success: false, error: "Unexpected server error" });
  }
});

// GET /result?q=... → poll endpoint
app.get("/result", async (req, res) => {
  const query = normalizeQuery(req.query?.q);

  if (!query) {
    return res.status(400).json({ success: false, error: "Query param 'q' is required" });
  }

  try {
    const { data, error } = await supabase
      .from("cached_results")
      .select("results, status, updated_at")
      .eq("query", query)
      .maybeSingle();

    if (error) {
      console.error("Supabase select error:", error.message);
      return res.status(500).json({ success: false, error: "Database error" });
    }

    if (!data) {
      return res.json({ success: true, notFound: true, query });
    }

    if (data.status === "completed") {
      return res.json({
        success: true,
        query,
        count: (data.results || []).length,
        results: data.results || [],
        updated_at: data.updated_at,
      });
    }

    if (data.status === "failed") {
      return res.json({ success: true, failed: true, query });
    }

    return res.json({ success: true, searching: true, query });
  } catch (err) {
    console.error("🔥 /result error:", err);
    return res.status(500).json({ success: false, error: "Unexpected server error" });
  }
});

// ─────────────────────────────────────────────
// ERROR HANDLERS
// ─────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({ success: false, error: "Route not found", path: req.path });
});

app.use((err, req, res, next) => {
  console.error("🔥 Unhandled middleware error:", err);
  if (res.headersSent) return next(err);
  res.status(500).json({ success: false, error: "Unexpected server error" });
});

process.on("unhandledRejection", (reason) => console.error("🔥 Unhandled Rejection:", reason));
process.on("uncaughtException", (err) => console.error("🔥 Uncaught Exception:", err));

// ─────────────────────────────────────────────
// START
// ─────────────────────────────────────────────
const server = app.listen(PORT, "0.0.0.0", () => {
  console.log("\n╔════════════════════════════════════════╗");
  console.log("║   🚀 ESCO CACHE-FIRST SERVER STARTED   ║");
  console.log("╚════════════════════════════════════════╝");
  console.log(`   Port:   ${PORT}`);
  console.log(`   PID:    ${process.pid}`);
  console.log(`   Cache TTL: ${CACHE_TTL_HOURS}h\n`);
});

function shutdown(signal) {
  console.log(`\n📴 ${signal} received — shutting down...`);
  server.close(() => {
    console.log("✅ Server closed");
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 10000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
