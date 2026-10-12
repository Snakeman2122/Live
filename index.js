/**
 * index.js — Flux Voice Agent server
 *
 * A small, dependency-light Node.js application that:
 *   1. Serves static assets from ./public (the browser UI lives there).
 *   2. Exposes a WebSocket bridge at the same host/port.
 *
 * Architecture
 * ------------
 *   Browser  <--WebSocket(binary PCM)-->  Node server  <--SDK socket-->  Deepgram Agent
 *
 * The browser never sees the Deepgram API key. The server holds the key,
 * opens the Deepgram Agent connection, applies the Settings (including the
 * custom OpenAI-compatible LLM under `think`), and relays audio both ways.
 *
 * Audio contract (must match public/index.html):
 *   - Mic input  : 16 kHz, linear16 (raw little-endian PCM)
 *   - Agent out  : 24 kHz, linear16 (raw little-endian PCM)
 *
 * Env vars (see .env):
 *   DEEPGRAM_API_KEY     - required, Deepgram project key
 *   PORT                 - optional, HTTP/WS listen port (default 3000)
 *   CUSTOM_LLM_URL       - optional, OpenAI-compatible chat completions URL
 *   CUSTOM_LLM_API_KEY   - optional, bearer token for the custom endpoint
 *   CUSTOM_LLM_MODEL     - optional, model name sent to the custom endpoint
 *   AGENT_PROMPT         - optional, system prompt for the agent
 */

"use strict";

require('dotenv').config();
const http = require("http");
const fs = require("fs");
const path = require("path");
const { DeepgramClient } = require("@deepgram/sdk");
const { WebSocketServer } = require("ws");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PORT = Number(process.env.PORT) || 3001;
const PUBLIC_DIR = path.join(__dirname, "public");
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;

// Custom LLM (OpenAI-compatible) config. These mirror the official docs'
// `think.provider` + `think.endpoint` shape.
const CUSTOM_LLM_URL =
  process.env.CUSTOM_LLM_URL || "https://api.openai.com/v1/chat/completions";
const CUSTOM_LLM_API_KEY = process.env.CUSTOM_LLM_API_KEY || "";
const CUSTOM_LLM_MODEL = process.env.CUSTOM_LLM_MODEL || "gpt-4o-mini";
// Default system prompt. Tuned for spoken, real-time conversation: replies are
// kept short and free of markup, since the output is synthesized as speech and
// any formatting characters would be read aloud or sound garbled.
const DEFAULT_AGENT_PROMPT = [
  "You are a friendly, helpful voice assistant speaking with a user in real time.",
  "",
  "Because your replies are spoken aloud through text-to-speech:",
  "- Keep responses short and conversational — usually one to three sentences.",
  "- Use plain, natural spoken language. Never use markdown, bullet points, or",
  "  special formatting characters such as *, #, -, _, `, |, or emoji.",
  "- Do not read out code, URLs, or long lists; summarize them in speech instead.",
  "- Ask a brief follow-up question when it helps move the conversation forward.",
  "- If you don't know something, say so briefly and offer to help another way.",
].join("\n");

const AGENT_PROMPT = process.env.AGENT_PROMPT || DEFAULT_AGENT_PROMPT;

// Audio settings shared with the browser UI.
const INPUT_SAMPLE_RATE = 16000; // mic capture rate in the browser
const OUTPUT_SAMPLE_RATE = 24000; // playback rate in the browser

if (!DEEPGRAM_API_KEY) {
  console.error(
    "[fatal] DEEPGRAM_API_KEY is not set. Add it to .env or export it."
  );
  process.exit(1);
}

const deepgram = new DeepgramClient({ apiKey: DEEPGRAM_API_KEY });

// ---------------------------------------------------------------------------
// Static file server
// ---------------------------------------------------------------------------

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".htm": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".txt": "text/plain; charset=utf-8",
  ".map": "application/json; charset=utf-8",
};

/**
 * Resolve a request URL path to an absolute file path inside PUBLIC_DIR.
 * Returns null if the resolved path escapes PUBLIC_DIR (path traversal).
 *
 * @param {string} urlPath - the raw request path (e.g. "/index.html")
 * @returns {string|null}
 */
function resolveStaticPath(urlPath) {
  // Strip query string / fragment, decode, and default to index.html.
  const cleanPath = decodeURIComponent(urlPath.split("?")[0].split("#")[0]);
  const relative = cleanPath === "/" ? "index.html" : cleanPath.replace(/^\/+/, "");
  const resolved = path.resolve(PUBLIC_DIR, relative);

  // Containment check: the resolved path must stay within PUBLIC_DIR.
  const rootWithSep = PUBLIC_DIR + path.sep;
  if (resolved !== PUBLIC_DIR && !resolved.startsWith(rootWithSep)) {
    return null;
  }
  return resolved;
}

/**
 * Handle an HTTP request by streaming a file from PUBLIC_DIR.
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 */
function handleStaticRequest(req, res) {
  const filePath = resolveStaticPath(req.url || "/");

  if (!filePath) {
    res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("403 Forbidden");
    return;
  }

  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("404 Not Found");
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || "application/octet-stream";

    res.writeHead(200, {
      "Content-Type": contentType,
      "Content-Length": stats.size,
      "Cache-Control": "no-cache",
    });

    const stream = fs.createReadStream(filePath);
    stream.on("error", () => {
      // Headers already sent; just terminate the response.
      res.destroy();
    });
    stream.pipe(res);
  });
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

const server = http.createServer((req, res) => {
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("405 Method Not Allowed");
    return;
  }
  handleStaticRequest(req, res);
});

// ---------------------------------------------------------------------------
// WebSocket bridge: browser <-> Deepgram Agent
// ---------------------------------------------------------------------------

const wss = new WebSocketServer({ server });

/**
 * Build the Deepgram Agent `Settings` payload, wiring the custom LLM into
 * the `think` block via provider + endpoint (per official docs).
 *
 * @returns {object} the Settings message
 */
function buildAgentSettings() {
  return {
    type: "Settings",
    audio: {
      input: {
        encoding: "linear16",
        sample_rate: INPUT_SAMPLE_RATE,
      },
      output: {
        encoding: "linear16",
        sample_rate: OUTPUT_SAMPLE_RATE,
        // No container: we stream raw PCM frames, not a WAV file.
      },
    },
    agent: {
      language: "en",
      listen: {
        provider: {
          type: "deepgram",
          model: "nova-3",
        },
      },
      think: {
        provider: {
          type: "open_ai",
          model: CUSTOM_LLM_MODEL,
          temperature: 0.5,
        },
        endpoint: {
          url: CUSTOM_LLM_URL,
          headers: {
            authorization: `Bearer ${CUSTOM_LLM_API_KEY}`,
          },
        },
        prompt: AGENT_PROMPT,
      },
      speak: {
        provider: {
          type: "deepgram",
          model: "aura-2-thalia-en",
        },
      },
      greeting: "Hello! How can I help you today?",
    },
  };
}

/**
 * Wire a single browser socket to a fresh Deepgram Agent connection.
 *
 * @param {import("ws").WebSocket} browserWs
 * @param {http.IncomingMessage} req
 */
async function handleBrowserConnection(browserWs, req) {
  const clientId = `${req.socket.remoteAddress}:${req.socket.remotePort}`;

  let connection = null;
  let keepAliveTimer = null;
  let closed = false;

  /** Idempotent teardown for both sides of the bridge. */
  const cleanup = () => {
    if (closed) return;
    closed = true;

    if (keepAliveTimer) {
      clearInterval(keepAliveTimer);
      keepAliveTimer = null;
    }
    if (connection) {
      try {
        connection.close();
      } catch {
        /* ignore */
      }
      connection = null;
    }
    try {
      if (
        browserWs.readyState === browserWs.OPEN ||
        browserWs.readyState === browserWs.CONNECTING
      ) {
        browserWs.close();
      }
    } catch {
      /* ignore */
    }
  };

  try {
    connection = await deepgram.agent.v1.connect();
  } catch (err) {
    console.error(`[error] Deepgram connect failed (${clientId}):`, err.message);
    browserWs.close(1011, "Deepgram connection failed");
    return;
  }

  // --- Deepgram -> Browser -------------------------------------------------
  connection.on("message", async (data) => {
    // --- Binary audio FIRST -------------------------------------------------
    // IMPORTANT: this must be checked BEFORE the control-message branch below.
    // A Blob is `typeof === "object"` AND has a string `.type` property (its
    // MIME type, "" for raw PCM), so a naive `typeof data.type === "string"`
    // test misclassifies audio as a control message and drops it. Detect every
    // binary form the SDK may surface (Blob / ArrayBuffer / Buffer / TypedArray)
    // up front and forward it to the browser.
    let audioBuffer = null;
    if (typeof Blob !== "undefined" && data instanceof Blob) {
      audioBuffer = Buffer.from(await data.arrayBuffer());
    } else if (data instanceof ArrayBuffer) {
      audioBuffer = Buffer.from(data);
    } else if (Buffer.isBuffer(data)) {
      audioBuffer = data;
    } else if (ArrayBuffer.isView(data)) {
      audioBuffer = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
    }

    if (audioBuffer) {
      if (browserWs.readyState === browserWs.OPEN) {
        browserWs.send(audioBuffer, { binary: true });
      }
      return;
    }

    // --- Control messages (parsed JSON objects) -----------------------------
    if (data && typeof data === "object" && typeof data.type === "string") {
      switch (data.type) {
        case "Welcome":
          // First message from Deepgram — send our Settings (including the
          // custom LLM under `think`) to start the conversation.
          connection.sendSettings(buildAgentSettings());
          break;

        case "ConversationText":
          // Forward the agent's transcript to the browser so the UI can render
          // the response text. User transcripts are ignored (the UI already
          // knows what the user said). Sent as a text JSON frame; audio frames
          // are binary and handled above, so the two never collide.
          if (data.role === "assistant" && typeof data.content === "string") {
            if (browserWs.readyState === browserWs.OPEN) {
              browserWs.send(JSON.stringify({ type: "agent_text", text: data.content }));
            }
          }
          break;

        case "UserStartedSpeaking":
          // Barge-in: the user began talking over the agent. Forward this so
          // the browser can immediately flush any agent audio still queued for
          // playback, making the interruption feel instant. Deepgram itself
          // also stops generating TTS in response to this event.
          if (browserWs.readyState === browserWs.OPEN) {
            browserWs.send(JSON.stringify({ type: "user_started_speaking" }));
          }
          break;

        case "AgentStartedSpeaking":
        case "AgentAudioDone":
          // TTS lifecycle markers are not logged: audio is relayed above and
          // the UI derives its own state from playback.
          break;

        default:
          // Unhandled control types are ignored; audio is handled above.
          break;
      }
      return;
    }

    // Anything else (unknown shape) is ignored.
  });

  connection.on("open", () => {
    /* no-op */
  });

  connection.on("close", () => {
    cleanup();
  });

  connection.on("error", (err) => {
    console.error(`[agent] error (${clientId}):`, err.message);
    cleanup();
  });

  // --- Browser -> Deepgram -------------------------------------------------
  browserWs.on("message", (raw, isBinary) => {
    if (!connection) return;
    if (isBinary && raw.length) {
      connection.sendMedia(raw);
    }
  });

  browserWs.on("close", () => {
    cleanup();
  });

  browserWs.on("error", (err) => {
    console.error(`[ws] browser error (${clientId}):`, err.message);
    cleanup();
  });

  // --- Open the Deepgram socket -------------------------------------------
  connection.connect();
  await connection.waitForOpen();

  // Keep the Deepgram socket alive during quiet periods.
  keepAliveTimer = setInterval(() => {
    if (!connection) return;
    try {
      connection.sendKeepAlive({ type: "KeepAlive" });
    } catch {
      /* ignore — cleanup will follow on close */
    }
  }, 5000);
}

wss.on("connection", (browserWs, req) => {
  handleBrowserConnection(browserWs, req).catch((err) => {
    console.error("[error] bridge setup failed:", err.message);
    try {
      browserWs.close(1011, "Bridge setup failed");
    } catch {
      /* ignore */
    }
  });
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

server.listen(PORT, () => {
  console.log(`Flux Voice Agent listening on http://localhost:${PORT}`);
});

/** Graceful shutdown: stop accepting, close sockets, exit. */
function shutdown(signal) {
  wss.clients.forEach((client) => {
    try {
      client.close(1001, "Server shutting down");
    } catch {
      /* ignore */
    }
  });
  wss.close(() => {
    server.close(() => process.exit(0));
  });
  // Safety net if sockets refuse to close.
  setTimeout(() => process.exit(0), 3000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
