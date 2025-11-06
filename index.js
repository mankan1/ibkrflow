// index.js — TradeFlash drop-in server with IB 429 backoff, caches, and robust pollers.
// Run: NODE_TLS_REJECT_UNAUTHORIZED=0 PORT=8080 node index.js
// Env:
//   PORT=8080
//   IB_BASE=https://127.0.0.1:5000/v1/api
//   MOCK=0|1     (for offline mocking)

import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import { WebSocketServer } from "ws";
import fs from "fs";
import {
  inferActionForOptionTrade,
  resetActionState
} from "./actions.js";
import { buildNotables } from './insights/notables.js';

import { router as watchlistRouter } from "./routes/watchlist.js";
import { loadWatchlistFile, saveWatchlistFile, buildOptionsAroundATM } from "./watchlist.js";

import {
  WATCH, getWatchlist, addEquity, addOptions, normalizeSymbol,
  setBroadcaster, broadcastWatchlist
} from "./state.js";


const ALIASES = new Map([
  ["$SPX", "SPX"],
  ["^GSPC", "SPX"],
  ["/ES", "ES"],
  ["ES1!", "ES"],   // TradingView style
]);
// function canon(sym) {
//   const s = String(sym||"").trim().toUpperCase();
//   return ALIASES.get(s) || s;
// }
// const MIN_SWEEP_QTY      = Number(process.env.MIN_SWEEP_QTY      || 20);
// const MIN_SWEEP_NOTIONAL = Number(process.env.MIN_SWEEP_NOTIONAL || 10000);
// const MIN_BLOCK_QTY      = Number(process.env.MIN_BLOCK_QTY      || 100);
// const MIN_BLOCK_NOTIONAL = Number(process.env.MIN_BLOCK_NOTIONAL || 50000);
// const BIG_PREMIUM_MIN    = Number(process.env.BIG_PREMIUM_MIN    || 75000);
// const OTM_QTY_MIN        = Number(process.env.OTM_QTY_MIN        || 50);
/* ================= CONFIG ================= */
const PORT    = Number(process.env.PORT || 8080);
const IB_BASE = String(process.env.IB_BASE || "https://127.0.0.1:5000/v1/api").replace(/\/+$/,"");
const MOCK    = String(process.env.MOCK || "") === "1";

/* ================= STATE ================= */
// const WATCH = {
//   equities: new Set(),                 // "AAPL","NVDA","MSFT"
//   options: [],                         // {underlying, expiration(YYYY-MM-DD), strike, right(C|P)}
// };
const STATE = {
  equities_ts: [],                     // [{symbol,last,bid,ask,iv?,ts}]
  options_ts:  [],                     // [{underlying,expiration,strike,right,last,bid,ask,ts}]
  sweeps:      [],                     // UI shape for Sweeps screen
  blocks:      [],                     // UI shape for Blocks screen
  prints:      [],   // <— add this
};

const CACHE = {
  conidBySym: new Map(),               // key: "AAPL" -> { conid, ts }
  expBySym:   new Map(),               // key: "AAPL" -> { ts, expirations: [...] }
  sessionPrimed: false,
  primingSince: 0,
};
const ROLLING_MS = 2 * 60_000; // 2 minutes for headlines window

function pruneTapeAges() {
  const cutoff = Date.now() - ROLLING_MS;
  if (STATE.sweeps?.length) STATE.sweeps = STATE.sweeps.filter(x => x.ts >= cutoff);
  if (STATE.blocks?.length) STATE.blocks = STATE.blocks.filter(x => x.ts >= cutoff);
  if (STATE.prints?.length) STATE.prints = STATE.prints.filter(x => x.ts >= cutoff);
}

if (String(process.env.NODE_TLS_REJECT_UNAUTHORIZED) === "0") {
  console.warn("WARNING: TLS verification disabled (self-signed).");
}
console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK?"on":"off"}  IB=${IB_BASE}`);

/* ================= APP/WS ================= */
// const app = express();

// app.use(express.json());
// app.use("/watchlist", watchlistRouter);
// app.use(cors());                // if you need to lock this down: cors({ origin: [/localhost/, /tradeflow\.lol$/] })

const app = express();
app.use(cors({                 // allow your dev client
  origin: ["http://localhost:8081", "http://127.0.0.1:8081"],
  methods: ["GET","POST","DELETE","OPTIONS"],
  allowedHeaders: ["Content-Type","Authorization"],
  credentials: false           // set true only if you use cookies/auth
}));
app.options("*", cors());      // handle preflight for all routes
app.use(express.json());
app.use("/watchlist", watchlistRouter);

app.use(compression());
app.use(morgan("dev"));
const FUT_ALIASES = new Map([["/ES","ES"], ["ES","ES"]]);

/* ================= WATCHLIST STATE ================= */
// const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "AAPL,MSFT,NVDA").split(",").map(s=>s.trim()).filter(Boolean);

// // Optional: “read-only demo” options list for your UI
// // Format matches what your UI renders: UL, Right, Strike, Expiry
// const DEFAULT_OPTIONS = [
//   { ul: "AAPL", right: "C", strike: 200, expiry: "2025-12-19" },
//   { ul: "NVDA", right: "P", strike: 220, expiry: "2025-11-21" },
//   { ul: "MSFT", right: "C", strike: 520, expiry: "2025-12-19" },
// ];

// ===== Sweep/Block thresholds =====
const MIN_SWEEP_QTY       = Number(process.env.MIN_SWEEP_QTY || 50);     // contracts
const MIN_SWEEP_NOTIONAL  = Number(process.env.MIN_SWEEP_NOTIONAL || 20000); // $ (qty * price * 100)
const MIN_BLOCK_QTY       = Number(process.env.MIN_BLOCK_QTY || 250);
const MIN_BLOCK_NOTIONAL  = Number(process.env.MIN_BLOCK_NOTIONAL || 100000);
const DEDUP_MS            = Number(process.env.SWEEP_DEDUP_MS || 3500); // de-dupe bursty repeats

const RECENT_SWEEPS = new Map(); // key -> ts
function seenRecently(key, ms) {
  const now = Date.now();
  const ts = RECENT_SWEEPS.get(key) || 0;
  if (now - ts < ms) return true;
  RECENT_SWEEPS.set(key, now);
  return false;
}
function maybePublishSweepOrBlock(msg) {
  // msg: { symbol/ul, option{expiration,strike,right}, price, qty, side, nbbo? }
  const notional = Math.round((msg.qty * (msg.price || 0) * 100));
  const isSweep  = msg.qty >= MIN_SWEEP_QTY && notional >= MIN_SWEEP_NOTIONAL;
  const isBlock  = msg.qty >= MIN_BLOCK_QTY  && notional >= MIN_BLOCK_NOTIONAL;
  if (!isSweep && !isBlock) return;

  const bid = msg.nbbo?.bid ?? (NBBO_OPT.get(toOcc(msg.symbol, msg.option?.expiration, rightToCP(msg.option?.right), msg.option?.strike))?.bid);
  const ask = msg.nbbo?.ask ?? (NBBO_OPT.get(toOcc(msg.symbol, msg.option?.expiration, rightToCP(msg.option?.right), msg.option?.strike))?.ask);

  let aggressor = "UNKNOWN";
  if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0) {
    const mid = (bid + ask) / 2;
    const bps = ((msg.price - mid) / mid) * 1e4;
    aggressor = bps >= TAPE_CFG.sweep.atAskBps ? "AT_ASK" : (bps <= -TAPE_CFG.sweep.atAskBps ? "AT_BID" : "NEAR_MID");
  }

  const base = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
    ts: Date.now(),
    provider: "ibkr",
    symbol: msg.symbol,
    ul: msg.symbol,  // unify field for gate/helper usage
    right: msg.option?.right,
    strike: msg.option?.strike,
    expiry: msg.option?.expiration,
    side: msg.side,
    qty: msg.qty,
    price: msg.price,
    bid, ask,
    aggressor,
    notional,
  };

  // 🔒 NEW: hard publish gate
  if (!passesPublishGate(base)) return;

  const k = [
    base.symbol, base.right, base.strike, base.expiry,
    base.side, Math.round(base.price*1000)||0
  ].join("|");
  if (seenRecently(k, DEDUP_MS)) return;

  pushAndFanout({ type: isBlock ? "blocks" : "sweeps", ...base });
}
// function maybePublishSweepOrBlock(msg) {
//   // msg is an options_ts-like object: { symbol, option:{expiration,strike,right}, price, qty, side, ... }
//   const notional = Math.round((msg.qty * (msg.price || 0) * 100));
//   const isSweep  = msg.qty >= MIN_SWEEP_QTY && notional >= MIN_SWEEP_NOTIONAL;
//   const isBlock  = msg.qty >= MIN_BLOCK_QTY  && notional >= MIN_BLOCK_NOTIONAL;

//   if (!isSweep && !isBlock) return;

//   // Build a stable-ish key to de-dupe mirrored spam
//   const k = [
//     msg.symbol, msg.option?.right, msg.option?.strike, msg.option?.expiration,
//     msg.side, Math.round(msg.price*1000)||0
//   ].join("|");

//   if (seenRecently(k, DEDUP_MS)) return;

//   // classify aggressor vs mid for display
//   let aggressor = "UNKNOWN";
//   const bid = msg.nbbo?.bid ?? (NBBO_OPT.get(toOcc(msg.symbol, msg.option?.expiration, msg.option?.right, msg.option?.strike))?.bid);
//   const ask = msg.nbbo?.ask ?? (NBBO_OPT.get(toOcc(msg.symbol, msg.option?.expiration, msg.option?.right, msg.option?.strike))?.ask);
//   if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0) {
//     const mid = (bid + ask) / 2;
//     const bps = ((msg.price - mid) / mid) * 1e4;
//     aggressor = bps >= TAPE_CFG.sweep.atAskBps ? "AT_ASK" : (bps <= -TAPE_CFG.sweep.atAskBps ? "AT_BID" : "NEAR_MID");
//   }

//   const base = {
//     id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
//     ts: Date.now(),
//     provider: "ibkr",
//     symbol: msg.symbol,
//     right: msg.option?.right,
//     strike: msg.option?.strike,
//     expiry: msg.option?.expiration,
//     side: msg.side,
//     qty: msg.qty,
//     price: msg.price,
//     bid,
//     ask,
//     aggressor,  // AT_ASK / AT_BID / NEAR_MID / UNKNOWN
//     notional,
//   };

//   if (isBlock) {
//     pushAndFanout({ type: "blocks", ...base });
//   } else if (isSweep) {
//     pushAndFanout({ type: "sweeps", ...base });
//   }
// }

if (!global.STATE) global.STATE = {};
if (!STATE.watchlist) STATE.watchlist = { equities: [], options: [] };

/* ================= SEED & START ================= */
// const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "AAPL,NVDA,MSFT,SPY").split(",")
//   .map(s => s.trim().toUpperCase()).filter(Boolean);

// const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "SPY,QQQ,IWM,AAPL,NVDA,MSFT,SPX,ES");
// const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "SPY,QQQ,IWM,AAPL,NVDA,MSFT,SPX,ES")
//   .split(",")
//   .map(s => s.trim().toUpperCase())
//   .filter(Boolean);
const POPULAR_50 = [
  "SPY","QQQ","IWM","DIA","SPX","ES"
  // ,"TLT","HYG","GLD","SLV","GDX","USO",
  // "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","AMD","NFLX","SMCI","MU","TSM","INTC","CSCO","IBM","ORCL","CRM",
  // "JPM","BAC","WFC","MS","GS","V","MA","PYPL","SQ","COIN",
  // "XOM","CVX","COP","OXY",
  // "UNH","JNJ","MRK","PFE","COST","HD","DIS"
];

const SEED_ON_EMPTY = String(process.env.SEED_ON_EMPTY || "").trim() === "1";
// const DEFAULT_EQUITIES =
//   (process.env.DEFAULT_EQUITIES || POPULAR_50.join(","))
//     .split(",")
//     .map(s => s.trim().toUpperCase())
//     .filter(Boolean);
// DEFAULT_EQUITIES only used for DEMO/SEED paths (not for normal boot)
const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || POPULAR_50.join(","))
  .split(",")
  .map(s => s.trim().toUpperCase())
  .filter(Boolean);
 
// during startup, if your in-mem watchlist is empty:
// if (WATCH.equities.size === 0) {
//   DEFAULT_EQUITIES.forEach(s => addEquity(s));
//   // optionally persist
// }
// ---- Do NOT auto-seed on normal boot ----
// If you really want to seed when empty, set SEED_ON_EMPTY=1
const SEED_MARK = ""; // disable any seed guard logic downstream

if (SEED_ON_EMPTY && (WATCH.equities.size === 0) && !fs.existsSync(SEED_MARK)) {
  (async () => {
    for (const s of DEFAULT_EQUITIES) await addEquity(s);
    fs.writeFileSync(SEED_MARK, new Date().toISOString());
    // wsBroadcast("watchlist", getWatchlist());
    wsBroadcast("watchlist", makeWatchlistPayload());
    console.log("[seed] SEED_ON_EMPTY applied");
  })();
}
// const POPULAR_50 = [
//   // ETFs & index/future
//   "SPY","QQQ","IWM","DIA","TLT","HYG","GLD","SLV","GDX","USO","SPX","ES",
//   // Mega-cap tech + semis
//   "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AVGO","AMD","NFLX","SMCI","MU","TSM","INTC","CSCO","IBM","ORCL","CRM",
//   // Fin/Payments
//   "JPM","BAC","WFC","MS","GS","V","MA","PYPL","SQ","COIN",
//   // Energy
//   "XOM","CVX","COP","OXY",
//   // Health / Staples / Retail / Discretionary
//   "UNH","JNJ","MRK","PFE","COST","HD","DIS"
// ];

// // const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "SPY,QQQ,IWM,AAPL,NVDA,MSFT,SPX,ES")
// //   .split(",")
// //   .map(s => s.trim().toUpperCase())
// //   .filter(Boolean);
// const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || POPULAR_50.join(","))
//   .split(",")
//   .map(s => s.trim().toUpperCase())
//   .filter(Boolean);

function toNumStrict(x) {
  if (typeof x === "number" && Number.isFinite(x)) return x;
  if (x == null) return null;
  const m = String(x).replace(/,/g, "").match(/-?\d+(\.\d+)?/);
  return m ? Number(m[0]) : null;
}
function normalizeIv(ivRaw) {
  const n = toNumStrict(ivRaw);
  if (n == null) return null;
  // handle 0.12 vs 12 — normalize to 0–100 scale
  return n <= 1.5 ? Math.round(n * 100) : Math.round(n);
}

// const DEFAULT_OPTIONS = (process.env.DEFAULT_OPTIONS || "").trim()
//   ? JSON.parse(process.env.DEFAULT_OPTIONS)
//   : [
//       { underlying: "AAPL", expiration: "2025-12-19", strike: 200, right: "C" },
//       { underlying: "NVDA", expiration: "2025-12-19", strike: 150, right: "P" },
//     ];
// const SEED_MARK = ".seeded";
const DEFAULT_OPTIONS = (process.env.DEFAULT_OPTIONS || "").trim()
  ? JSON.parse(process.env.DEFAULT_OPTIONS)
  : []; // no demo seeds

/* ================= IB HELPERS (cached) ================= */
const CONID_TTL_MS = 12*60*60*1000;
const EXP_TTL_MS   = 30*60*1000;

let lastReqEndedAt = 0;
// function seedWatchlist() {
//   // equities
//   const existing = new Set((STATE.watchlist.equities || []).map(s => s.toUpperCase()));
//   for (const s of DEFAULT_EQUITIES) existing.add(s.toUpperCase());
//   STATE.watchlist.equities = Array.from(existing);

//   // options (read-only demo list)
//   STATE.watchlist.options = Array.isArray(STATE.watchlist.options) && STATE.watchlist.options.length
//     ? STATE.watchlist.options
//     : DEFAULT_OPTIONS.slice();

//   console.log("Seeded equities:", STATE.watchlist.equities.join(", "));
//   // console.log("Seeded demo options:", STATE.watchlist.options.map(o => `${o.ul} ${o.right} ${o.strike} ${o.expiry}`).join(" | "));
//   console.log(
//     "Seeded demo options:",
//     (STATE.watchlist.options || [])
//       .map(o => `${o.underlying} ${o.right} ${o.strike} ${o.expiration}`)
//       .join(" | ")
//   );
// }
let wss = /** @type {import('ws').WebSocketServer|null} */ (null);
let wsReady = false;
const WS_QUEUE = []; // payloads buffered until WS is ready

function wsSend(ws, topic, data) {
  try { ws.send(JSON.stringify({ topic, data })); } catch {}
}

function wsBroadcast(topic, data) {
  const payload = JSON.stringify({ topic, data });
  if (!wsReady || !wss) { WS_QUEUE.push(payload); return; }
  for (const c of wss.clients) if (c.readyState === 1) c.send(payload);
}
const DEMO = String(process.env.DEMO || "").trim() === "1";
async function seedWatchlist() {
  // Only seed in explicit DEMO mode
  const envList = (process.env.DEFAULT_EQUITIES || "").trim();
  const desired = (envList ? envList.split(",") : POPULAR_50)
    .map(s => s.trim().toUpperCase())
    .filter(Boolean);
  for (const s of desired) await addEquity(s);
  // Demo-only option legs (optional)
  // if (!Array.isArray(WATCH.options) || WATCH.options.length === 0) {
  //   await addOptions([
  //     { underlying: "AAPL", expiration: "2025-12-19", strike: 200, right: "C" },
  //     { underlying: "NVDA", expiration: "2025-12-19", strike: 150, right: "P" },
  //   ]);
  // }
  // if (DEMO && (!Array.isArray(WATCH.options) || WATCH.options.length === 0)) {
  //   await addOptions([
  //     { underlying: "AAPL", expiration: "2025-12-19", strike: 200, right: "C" },
  //     { underlying: "NVDA", expiration: "2025-12-19", strike: 150, right: "P" },
  //   ]);
  // }  
  // wsBroadcast("watchlist", getWatchlist());
  wsBroadcast("watchlist", makeWatchlistPayload());
}

// async function seedWatchlist() {
//   // const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "SPY,QQQ,IWM,AAPL,NVDA,MSFT,SPX,ES")
//   //   .split(",").map(s => s.trim().toUpperCase()).filter(Boolean);

//   // for (const s of DEFAULT_EQUITIES) addEquity(s);
//   const envList = (process.env.DEFAULT_EQUITIES || "").trim();
//   const desired = (envList ? envList.split(",") : POPULAR_50)
//     .map(s => s.trim().toUpperCase()).filter(Boolean);
//   for (const s of desired) addEquity(s);

//   if (!(WATCH.options?.length)) {
//     addOptions([
//       { underlying: "AAPL", expiration: "2025-12-19", strike: 200, right: "C" },
//       { underlying: "NVDA", expiration: "2025-12-19", strike: 150, right: "P" },
//     ]);
//   }
//   wsBroadcast("watchlist", getWatchlist());
// }

// seedWatchlist();


const server = http.createServer(app);

// ---- Create WS server, then mark ready and flush any queued broadcasts ----
wss = new WebSocketServer({ server, perMessageDeflate: false });
wsReady = true;
setBroadcaster(wsBroadcast);

// Flush anything that tried to broadcast before WS stood up
for (const payload of WS_QUEUE.splice(0)) {
  for (const c of wss.clients) if (c.readyState === 1) c.send(payload);
}

/* ========== DEMO HELPERS (seed options + normalize prints) ========== */
// let wss;
// let wsReady = false;
// const WS_QUEUE = [];
// wss = new WebSocketServer({ server, perMessageDeflate:false });

// wsReady = true;
// flush any pre-init broadcasts
// for (const p of WS_QUEUE.splice(0)) {
//   for (const c of wss.clients) if (c.readyState === 1) c.send(p);
// }

// quick ul price fallback if quotes aren't ready yet
// const DEMO_BASE = { SPY: 510, QQQ: 430, IWM: 205, AAPL: 268, NVDA: 206, MSFT: 516 };

/** Get an underlying's price from STATE.quotes or a fallback */
// function getULPrice(ul){
//   const q = (STATE.quotes && STATE.quotes[ul]) || null;
//   if (q && typeof q.last === "number" && q.last > 0) return q.last;
//   return DEMO_BASE[ul] ?? 100;
// }
/** Get an underlying's price from STATE.quotes; no demo fallback */
function getULPrice(ul){
  const q = (STATE.quotes && STATE.quotes[ul]) || null;
  if (q && typeof q.last === "number" && q.last > 0) return q.last;
  return undefined; // callers must handle undefined
}

/** yyyy-mm-dd */
function fmtDate(d){
  const z = (n)=>String(n).padStart(2,"0");
  return `${d.getFullYear()}-${z(d.getMonth()+1)}-${z(d.getDate())}`;
}

/** next N Fridays from today (or just two expiries) */
function nextFridays(n=2){
  const out = [];
  const d = new Date();
  for (let i=0;i<21 && out.length<n;i++){
    const t = new Date(d.getFullYear(), d.getMonth(), d.getDate()+i);
    if (t.getDay() === 5) out.push(fmtDate(t));
  }
  // safety: if fewer than n found, pad a week apart
  while (out.length < n){
    const last = out[out.length-1] ? new Date(out[out.length-1]) : new Date();
    last.setDate(last.getDate()+7);
    out.push(fmtDate(last));
  }
  return out;
}
let notablesDirty = false;
let notablesTimer = null;
// cache last JSON so we only push when changed
let lastNotablesJson = "";

// =========================================================
// 1) CONFIG + STATE (place near other top-level consts)
// =========================================================
const NOTABLES_DEFAULT = {
  // tweak to taste (these are sensible real-time defaults)
  windowMs: 5 * 60_000,   // look back 5 minutes
  minNotional: 75_000,    // filter out tiny stuff
  topN: 50,               // cap list size
  side: "ANY",            // BUY | SELL | ANY
  expiryMaxDays: 365      // ignore LEAPS if you want
};

function scheduleNotables() {
  notablesDirty = true;
  if (notablesTimer) return;
  notablesTimer = setTimeout(() => {
    notablesTimer = null;
    if (!notablesDirty) return;
    notablesDirty = false;

    const immediate = buildNotables(
      { sweeps: STATE.sweeps || [], blocks: STATE.blocks || [], prints: STATE.prints || [] },
      NOTABLES_DEFAULT
    );

    // Don’t broadcast a totally empty set if the last one had items
    const prev = lastNotablesJson ? JSON.parse(lastNotablesJson) : null;
    const hadStuff = !!(prev?.notables?.length || prev?.all?.length);
    const hasStuff = !!(immediate?.notables?.length || immediate?.all?.length);
    if (!hasStuff && hadStuff) return; // suppress empty wipe

    const s = JSON.stringify(immediate);
    if (s !== lastNotablesJson) {
      wsBroadcast("notables", immediate);
      lastNotablesJson = s;
    }
  }, 250); // ~4Hz
}
// const STATE = {
//   equities_ts: [],
//   options_ts:  [],
//   sweeps:      [],
//   blocks:      [],
//   prints:      [],   // <— add this
// };
// somewhere near your data ingestion
const LAST_TRADES = [];          // ring buffer of recent option prints
const MAX_TRADES  = 5000;
// ===== Publish gates =====
const BIG_PREMIUM_MIN = Number(process.env.BIG_PREMIUM_MIN || 200_000);
const OTM_QTY_MIN     = Number(process.env.OTM_QTY_MIN     || 150);

function rightAsCP(r){
  const s = String(r||"").toUpperCase();
  return s === "CALL" || s === "C" ? "C"
       : s === "PUT"  || s === "P" ? "P" : undefined;
}

function isOTM(ulPx, right, strike){
  if (!Number.isFinite(ulPx)) return false;          // can't judge → not OTM
  const cp = rightAsCP(right);
  if (!cp || !Number.isFinite(+strike)) return false;
  return (cp === "C" ? strike > ulPx : strike < ulPx);
}

/** central gate: allow only big premium OR OTM with large size */
function passesPublishGate(msg){
  const notional = Number(msg?.notional);
  if (Number.isFinite(notional) && notional >= BIG_PREMIUM_MIN) return true;

  const qty = Number(msg?.qty);
  if (!Number.isFinite(qty) || qty < OTM_QTY_MIN) return false;

  // need UL price to decide OTM
  const ul   = msg.ul || msg.symbol;
  const when = Number(msg.ts) || Date.now();
  const ulPx = getUnderlyingLastNear(ul, when) ?? getLast(ul) ?? null;

  return isOTM(ulPx, msg.right || msg?.option?.right, msg.strike ?? msg?.option?.strike);
}

// ---- Underlying last cache (symbol -> { px, ts }) ----
const ulLast = new Map(); // e.g., "NVDA" -> { px: 123.45, ts: 1730812345123 }
const UL_STALE_MS = 5_000; // consider quote valid if within 5s of event ts

const MIN_GAP_MS = 250;
const MAX_RETRIES = 4;

function updateUnderlyingLast(ul, px, ts = Date.now()) {
  if (!ul) return;
  const sym = String(ul).toUpperCase();
  if (!Number.isFinite(px)) return;
  ulLast.set(sym, { px: Number(px), ts: Number(ts) });
}

function getUnderlyingLastNear(ul, eventTs) {
  const sym = String(ul).toUpperCase();
  const row = ulLast.get(sym);
  if (!row) return null;
  const age = Math.abs(Number(eventTs || Date.now()) - row.ts);
  return age <= UL_STALE_MS ? row.px : null;
}

/** Push a normalized trade event into buffer */
function onOptionTrade(ev) {
  // expected ev fields:
  // { ul, right:"C"|"P", strike:number, expiry:"YYYY-MM-DD",
  //   side:"BUY"|"SELL", qty:number, price:number, notional:number, ts:number }
  LAST_TRADES.push(ev);
  if (LAST_TRADES.length > MAX_TRADES) LAST_TRADES.splice(0, LAST_TRADES.length - MAX_TRADES);
}
const DAY_STATS_OPT = new Map(); // occ -> { volume, oi, ts }

// helper stays near toOcc(...)
function updateDayStats(ul, expiration, right, strike, { volume, oi }) {
  const occ = toOcc(ul, expiration, rightToCP(right), strike);
  DAY_STATS_OPT.set(occ, {
    volume: Number.isFinite(volume) ? volume : undefined,
    oi: Number.isFinite(oi) ? oi : undefined,
    ts: Date.now()
  });
}

/**
 * Return large single prints as "blocks".
 */
function synthesizeBlocksFrom(events, { topN = 24, minNotional = 50_000, minQty = 200, windowMs = 5 * 60_000 } = {}) {
  const now = Date.now();
  const rows = [];

  for (const e of events) {
    if ((now - e.ts) > windowMs) continue;              // recent only
    if (e.notional >= minNotional && e.qty >= minQty) {
      rows.push({
        ul: e.ul, right: e.right, strike: e.strike, expiry: e.expiry,
        side: e.side, qty: e.qty, price: e.price,
        notional: e.notional, ts: e.ts, age: Math.max(0, Math.round((now - e.ts)/1000))
      });
    }
  }

  rows.sort((a,b) => b.ts - a.ts);
  return rows.slice(0, topN);
}
/**
 * Collapse many small prints into "sweeps" by time clustering.
 */
function synthesizeSweepsFrom(events, {
  topN = 120,
  minNotional = 25_000,
  minQty = 100,
  legGapMs = 350,        // time gap that keeps a cluster going
  maxClusterMs = 2000,   // stop clustering after this total span
  windowMs = 5 * 60_000  // only look back this far
} = {}) {
  const now = Date.now();

  // group by contract+side
  const byKey = new Map(); // key -> sorted events
  for (const e of events) {
    if ((now - e.ts) > windowMs) continue;
    const key = `${e.ul}|${e.right}|${e.strike}|${e.expiry}|${e.side}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(e);
  }

  const sweeps = [];

  for (const [key, arr] of byKey) {
    // sort by time asc to build clusters
    arr.sort((a,b) => a.ts - b.ts);

    let cluster = [];
    let clusterStart = 0;

    const flush = () => {
      if (!cluster.length) return;
      const qty = cluster.reduce((s,x)=>s+x.qty, 0);
      const notional = cluster.reduce((s,x)=>s+x.notional, 0);
      if (qty >= minQty && notional >= minNotional && cluster.length >= 2) {
        const vwap = cluster.reduce((s,x)=>s + x.price*x.qty, 0) / qty;
        const first = cluster[0], last = cluster[cluster.length-1];
        const [ul, right, strike, expiry, side] = key.split("|");
        sweeps.push({
          ul, right, strike: Number(strike), expiry, side,
          qty, price: vwap, notional,
          ts: last.ts, age: Math.max(0, Math.round((now - last.ts)/1000))
        });
      }
      cluster = [];
      clusterStart = 0;
    };

    for (let i=0;i<arr.length;i++){
      const t = arr[i];
      if (!cluster.length) {
        cluster = [t];
        clusterStart = t.ts;
        continue;
      }
      const gap = t.ts - cluster[cluster.length-1].ts;
      const span = t.ts - clusterStart;

      if (gap <= legGapMs && span <= maxClusterMs) {
        cluster.push(t);
      } else {
        flush();
        cluster = [t];
        clusterStart = t.ts;
      }
    }
    flush();
  }

  sweeps.sort((a,b)=> b.ts - a.ts);
  return sweeps.slice(0, topN);
}

/* ================= HELPERS ================= */
const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));
const clamp=(x,min,max)=>Math.max(min,Math.min(max,x));
const uniq=(a)=>Array.from(new Set(a));

/** Build a small options chain near the money for demo */
function seedDemoOptionsFromULs(uls){
  const expiries = nextFridays(2);          // e.g., ['2025-11-07','2025-11-14']
  const out = [];
  for (const ul of uls){
    const spot = getULPrice(ul);
    const strikes = [-10,-5,0,5,10].map(dx => Math.round((spot+dx) / 5) * 5); // round to 5s
    for (const exp of expiries){
      for (const K of strikes){
        for (const right of ["C","P"]){
          const mid = Math.max(0.05, Math.abs(spot - K) / 20); // crude demo mid
          const spread = Math.max(0.02, mid * 0.05);
          out.push({
            underlying: ul,
            right,                          // "C" | "P"
            strike: K,
            expiration: exp,                // "YYYY-MM-DD"
            last: Number(mid.toFixed(2)),
            bid: Number((mid - spread).toFixed(2)),
            ask: Number((mid + spread).toFixed(2)),
            ts: Date.now()
          });
        }
      }
    }
  }
  return out;
}
// 1) Keep normalizePrint simple but let callers override qty/notional later
// function normalizePrint(t, kind = "SWEEP"){
//   const price = Number((t.last ?? t.bid ?? t.ask ?? 1).toFixed(2));
//   const qty   = 10; // base; will be overridden below
//   const notional = Number((price * 100 * qty).toFixed(2));
//   return {
//     kind: kind.toUpperCase(),           // "SWEEP" | "BLOCK"
//     ul: t.underlying || "UNKNOWN",
//     right: t.right === "C" ? "CALL" : "PUT",
//     strike: t.strike ?? 0,
//     expiry: t.expiration || "2099-12-31",
//     side: "BUY",
//     qty,
//     price,
//     notional,
//     prints: Math.floor(1 + Math.random()*3),
//     venue: "demo",
//     ts: Date.now()
//   };
// }
function inferAggressor({ price, bid, ask }) {
  if (typeof price !== "number") return "UNKNOWN";
  if (typeof bid === "number" && price <= bid + 0.01) return "AT_BID";
  if (typeof ask === "number" && price >= ask - 0.01) return "AT_ASK";
  return "NEAR_MID";
}

/** Strictly normalize a real options trade/print; return null if insufficient */
function normalizePrint(t, kind = "SWEEP") {
  const ul      = t.underlying || t.ul;
  const rightCh = (t.right || t.option_type || "").toString().toUpperCase();
  const right   = rightCh === "C" || rightCh === "CALL" ? "CALL"
                : rightCh === "P" || rightCh === "PUT"  ? "PUT" : undefined;
  const strike  = Number(t.strike ?? t.strike_price);
  const expiry  = (t.expiration || t.expiry || t.expiration_date || "").toString();
  const price   = Number(t.price ?? t.last ?? t.executed_price ?? t.fill_price);
  const qty     = Number(t.qty ?? t.size ?? t.quantity ?? t.trade_size);
  const bid     = (t.bid ?? t.best_bid);
  const ask     = (t.ask ?? t.best_ask);
  const sideRaw = (t.side || t.liquidity || t.order_side || "").toString().toUpperCase();

  if (!ul || !right || !isFinite(strike) || !expiry || !isFinite(price) || price <= 0 || !isFinite(qty) || qty <= 0) {
    return null; // drop bad/partial data
  }

  // Derive side if missing using aggressor vs. bid/ask
  let side = ["BUY","SELL"].includes(sideRaw) ? sideRaw
           : (() => {
               const ag = inferAggressor({ price, bid: Number(bid), ask: Number(ask) });
               return ag === "AT_ASK" ? "BUY" : ag === "AT_BID" ? "SELL" : "UNKNOWN";
             })();

  const multiplier = Number(t.multiplier ?? 100);
  const notional = Math.round(price * qty * multiplier);

  return {
    kind: String(kind || "").toUpperCase(), // "SWEEP" | "BLOCK" | "PRINT"
    ul,
    right,
    strike,
    expiry,
    side,
    qty,
    price: Number(price.toFixed(2)),
    notional,
    prints: Number(t.prints ?? 1),
    aggressor: inferAggressor({ price, bid: Number(bid), ask: Number(ask) }),
    venue: t.venue || t.source || "live",
    ts: Number(t.ts ?? t.timestamp ?? Date.now())
  };
}
/* ========== INITIAL SEEDING ========== */

// Ensure these are the symbols you already seeded
// const SEEDED_EQUITIES = ["SPY","QQQ","IWM","AAPL","NVDA","MSFT"];

// Seed options once at startup (and you can call it again after quotes appear)
// function seedDemoOptions(){
//   const opts = seedDemoOptionsFromULs(SEEDED_EQUITIES);
//   STATE.options_ts = opts;
//   console.log("Seeded demo options:", 
//     opts.slice(0,4).map(o => `${o.underlying} ${o.right} ${o.strike} ${o.expiration}`).join(" | "),
//     `(+${Math.max(0, opts.length-4)} more)`
//   );
// }
// server/index.js (or wherever you compute headlines)
let lastBlocks = "", lastSweeps = "";

function computeBlocks(topN = 20, minNotional = 50_000) {
  // TODO: your real logic; below just transforms STATE.options_ts into blocks
  // Return array like [{ul:"SPY", right:"P", strike:510, expiry:"2025-11-07", side:"BUY", qty:300, notional:150000}]
  return synthesizeBlocksFrom(STATE.options_ts, { topN, minNotional });
}

function computeSweeps(topN = 50, minNotional = 25_000) {
  // Similar shape, but “sweep” logic distinct from blocks
  return synthesizeSweepsFrom(STATE.options_ts, { topN, minNotional });
}

// === ET day-boundary auto-reset for BTO/BTC/STO/STC rolling state ===
const etFmt = new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" });
let lastETDay = null;
function etYMD() {
  const p = etFmt.formatToParts(Date.now());
  const y = p.find(x => x.type==="year")?.value;
  const m = p.find(x => x.type==="month")?.value;
  const d = p.find(x => x.type==="day")?.value;
  return `${y}${m}${d}`;
}
setInterval(() => {
  const ymd = etYMD();
  if (ymd !== lastETDay) {
    resetActionState();
    lastETDay = ymd;
  }
}, 30_000);

setInterval(() => {
  const blocks = synthesizeBlocksFrom(LAST_TRADES, { topN: 24, minNotional: 50_000, minQty: 200 });
  const sweeps = synthesizeSweepsFrom(LAST_TRADES, { topN: 120, minNotional: 25_000, minQty: 100 });

  // 🔹 NEW: attach ul_px to each row
  const enrichUL = (r) => ({
    ...r,
    ul_px: (getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null)
  });
  // const blocksE = blocks.map(enrichUL);
  const blocksE = blocks.map(r => attachDayStats(enrichUL(r)));
  // const sweepsE = sweeps.map(enrichUL);
  const sweepsE = sweeps.map(r => attachDayStats(enrichUL(r)));
  STATE.blocks = blocksE;

  STATE.sweeps = sweepsE;

  scheduleNotables(); // mark dirty so notables recomputes off new STATE

  const bJson = JSON.stringify(blocksE);
  const sJson = JSON.stringify(sweepsE);
  if (bJson !== lastBlocks) { wsBroadcast("blocks", blocksE); lastBlocks = bJson; }
  if (sJson !== lastSweeps) { wsBroadcast("sweeps", sweepsE); lastSweeps = sJson; }
}, 1000);

function seedDemoOptions(){
  if (!DEMO) {
    // In non-demo mode never seed. Make sure options_ts starts empty and real collectors fill it.
    if (!Array.isArray(STATE.options_ts)) STATE.options_ts = [];
    return;
  }
  const wf = loadWatchlistFile();
  const uls = (wf?.equities ?? []).map(s => s.toUpperCase());
  const opts = seedDemoOptionsFromULs(uls.length ? uls : (DEFAULT_EQUITIES || []));
  STATE.options_ts = opts;
  console.log("Seeded DEMO options for", uls.length, "symbols");
}

// function seedDemoOptions(){
//   const wf = loadWatchlistFile();
//   const uls = (wf?.equities ?? []).map(s => s.toUpperCase());
//   const opts = seedDemoOptionsFromULs(uls.length ? uls : DEFAULT_EQUITIES);
//   STATE.options_ts = opts;
//   console.log("Seeded demo options for", uls.length, "symbols");
// }
let lastHeadlinesJson = "";
setInterval(() => {
  const headlines = computeHeadlines({ windowMs: ROLLING_MS, minNotional: 50_000, topN: 12 });
  const s = JSON.stringify(headlines);
  if (s !== lastHeadlinesJson) { wsBroadcast("headlines", headlines); lastHeadlinesJson = s; }
}, 2000);

let FUT_FRONT = new Map(); // "ES" -> { symbol:"ESZ5", conid: 12345, display:"ESZ5" }

async function resolveFrontMonth(symbol){ // symbol like "ES"
  try {
    // 1) Ask IB for futures chain (you already have IB_BASE + auth)
    const fut = await ibFetch(`/trsrv/futures?symbols=${encodeURIComponent(symbol)}`);
    // 2) Pick a front/active contract (pseudo-logic, adapt to your payload)
    const chain = fut?.[symbol]?.contracts || [];
    const active = chain.find(c => c.is_tradable && (c.is_front || c.is_active)) || chain[0];
    if (active?.conid && active?.symbol) {
      FUT_FRONT.set(symbol, { symbol: active.local_symbol || active.symbol, conid: String(active.conid), display: active.local_symbol || active.symbol });
      console.log(`[FUT] ${symbol} -> ${active.local_symbol || active.symbol} (${active.conid})`);
      return FUT_FRONT.get(symbol);
    }
  } catch(e){ console.warn("resolveFrontMonth:", e.message); }
  return null;
}

// on boot (and maybe every few hours)
// await resolveFrontMonth("ES");
await resolveFrontMonthES().catch(()=>{});
// when snapshotting:
function symbolToConid(symbol){
  if (symbol === "/ES" || symbol === "ES"){
    const r = FUT_FRONT.get("ES");
    if (r?.conid) return r.conid;
  }
  // fall back to your existing conid cache for equities/indices
  return CACHE.conidBySym.get(symbol)?.conid;
}
// Call once during boot
// seedDemoOptions();


/* ========== DEMO TAPES (Sweeps/Blocks) ========== */

// 2) Turn options ticks into demo sweeps/blocks with large enough size
// function makeDemoTapeFromOptions(optionsTicks, kind){
//   const out=[];
//   for (const t of optionsTicks.slice(0, 48)){  // little more depth
//     const p = normalizePrint(t, kind);
//     if (kind === "SWEEP"){
//       // Sweeps: multi-print clusters, medium/large clips
//       p.prints = Math.floor(2 + Math.random()*3);     // 2-4 prints
//       p.qty    = Math.floor(50 + Math.random()*200);  // 50-250 contracts
//       p.price  = Number((p.price || 0.5).toFixed(2));
//       p.notional = Math.round(p.qty * p.price * 100); // recompute notional
//     } else {
//       // Blocks: single big negotiated clip
//       p.prints = 1;
//       p.qty    = Math.floor(250 + Math.random()*750); // 250-1000 contracts
//       p.price  = Number((p.price || 1).toFixed(2));
//       p.notional = Math.max(150000, Math.round(p.qty * p.price * 100)); // ≥ $150k
//     }
//     out.push(p);
//   }
//   return out;
// }
function makeDemoTapeFromOptions(optionsTicks, kind){
  const out = [];
  const SWEEP_MIN_QTY = 50;     // matches your UI default
  const SWEEP_MAX_QTY = 800;
  const SWEEP_MIN_NOTIONAL = 25000;  // must be > UI minNotional (20k)
  const SWEEP_MAX_NOTIONAL = 125000; // upper bound for realism

  for (const t of optionsTicks.slice(0, 48)){
    const p = normalizePrint(t, kind);  // gives us ul/right/strike/expiry/price/etc.

    if (kind === "SWEEP") {
      // pick a realistic notional, then derive qty from price
      const targetNotional = Math.round(
        SWEEP_MIN_NOTIONAL + Math.random() * (SWEEP_MAX_NOTIONAL - SWEEP_MIN_NOTIONAL)
      );

      const px = Math.max(0.05, Number(p.price || 0.5)); // keep sane demo price
      let qty = Math.round(targetNotional / (px * 100));

      // enforce sweep-style ranges and UI thresholds
      qty = Math.max(SWEEP_MIN_QTY, Math.min(SWEEP_MAX_QTY, qty));

      // 2–4 prints to feel like a sweep cluster
      p.prints = 2 + Math.floor(Math.random() * 3); // 2..4
      p.qty = qty;
      p.price = Number(px.toFixed(2));
      p.notional = qty * p.price * 100;

      // lean buy-side to make it visible regardless of side filter
      // p.side = "BUY";

      // infer vs mid from NBBO if we have it; else UNKNOWN
      const nbbo = NBBO_OPT.get(toOcc(p.ul || t.underlying, p.expiry || t.expiration, p.right === "CALL" ? "C" : "P", p.strike || t.strike));
      if (nbbo && Number.isFinite(nbbo.bid) && Number.isFinite(nbbo.ask) && nbbo.bid > 0 && nbbo.ask > 0) {
        const mid = (nbbo.bid + nbbo.ask) / 2;
        const bps = ((p.price - mid) / mid) * 1e4;
        p.side = bps >= TAPE_CFG.sweep.atAskBps ? "BUY" : (bps <= -TAPE_CFG.sweep.atAskBps ? "SELL" : "UNKNOWN");
      } else {
        p.side = "UNKNOWN";
      }      
      p.venue = "demo";
      p.ts = Date.now();

    } else {
      // BLOCKS: keep large single prints so they always show
      p.prints = 1;
      p.qty = Math.floor(250 + Math.random() * 750); // 250-1000 contracts
      p.price = Number((p.price || 1).toFixed(2));
      // p.notional = Math.max(150000, Math.round(p.qty * p.price * 100));
      p.notional = Math.round(p.qty * p.price * 100);
      // p.side = "BUY";
      const nbboB = NBBO_OPT.get(toOcc(p.ul || t.underlying, p.expiry || t.expiration, p.right === "CALL" ? "C" : "P", p.strike || t.strike));
      if (nbboB && Number.isFinite(nbboB.bid) && Number.isFinite(nbboB.ask) && nbboB.bid > 0 && nbboB.ask > 0) {
        const mid = (nbboB.bid + nbboB.ask) / 2;
        const bps = ((p.price - mid) / mid) * 1e4;
        p.side = bps >= TAPE_CFG.sweep.atAskBps ? "BUY" : (bps <= -TAPE_CFG.sweep.atAskBps ? "SELL" : "UNKNOWN");
      } else {
        p.side = "UNKNOWN";
      }      
      p.venue = "demo";
      p.ts = Date.now();
    }

    out.push(p);
  }

  return out;
}
// 3) Your pollers already assign STATE.sweeps/blocks and broadcast:
async function pollSweepsOnce(){ try {
  const src = Array.isArray(STATE.options_ts) ? STATE.options_ts : [];
  const next = makeDemoTapeFromOptions(src, "SWEEP");
  const changed = JSON.stringify(next)!==JSON.stringify(STATE.sweeps||[]);
  STATE.sweeps = next;
  if (changed) wsBroadcast("sweeps", next);
} catch(e){ console.warn("pollSweepsOnce:", e.message); } }

async function pollBlocksOnce(){ try {
  const src = Array.isArray(STATE.options_ts) ? STATE.options_ts : [];
  const next = makeDemoTapeFromOptions(src, "BLOCK");
  const changed = JSON.stringify(next)!==JSON.stringify(STATE.blocks||[]);
  STATE.blocks = next;
  if (changed) wsBroadcast("blocks", next);
} catch(e){ console.warn("pollBlocksOnce:", e.message); } }

// const wss = new WebSocketServer({ server, perMessageDeflate:false });

// function wsSend(ws, topic, data){ try{ ws.send(JSON.stringify({ topic, data })); } catch{} }
// function wsBroadcast(topic, data){
//   const s = JSON.stringify({ topic, data });
//   for (const c of wss.clients) if (c.readyState === 1) c.send(s);
// }
// const WS_QUEUE = [];           // payloads to flush when ready

// function wsSend(ws, topic, data){
//   try { ws.send(JSON.stringify({ topic, data })); } catch {}
// }

// function wsBroadcast(topic, data){
//   const payload = JSON.stringify({ topic, data });
//   if (!wsReady || !wss) { WS_QUEUE.push(payload); return; }
//   for (const c of wss.clients) if (c.readyState === 1) c.send(payload);
// }

// const isNonEmptyStr=(s)=>typeof s==="string" && s.trim().length>0;
const num=(x)=> (x==null ? undefined : Number(x));

function rightNormalize(x){
  const s = String(x||"").trim().toUpperCase();
  if (s==="CALL"||s==="C") return "C";
  if (s==="PUT" ||s==="P") return "P";
  return s;
}
function ensureSet(v){
  if (!v) return new Set();
  if (v instanceof Set) return v;
  if (Array.isArray(v)) return new Set(v);
  if (typeof v === "string") return new Set([v]);
  return new Set();
}
function normEquities(W){ return Array.from(ensureSet(W?.equities)).map(s=>String(s).toUpperCase()).filter(Boolean); }
function normOptions(W){
  const A = Array.isArray(W?.options) ? W.options : [];
  return A.map(o=>({
    underlying:String(o?.underlying||"").toUpperCase(),
    expiration:String(o?.expiration||""),
    strike:Number(o?.strike),
    right:rightNormalize(o?.right),
  })).filter(o=>o.underlying && o.right && Number.isFinite(o.strike));
}
// function getWatchlist(){ return { equities: normEquities(WATCH), options: normOptions(WATCH) }; }
// async function watchAddEquities(eqs=[]){
//   const before = normEquities(WATCH).length;
//   for (const s of eqs) { const u=String(s).toUpperCase(); if (u) WATCH.equities.add(u); }
//   const after = normEquities(WATCH).length;
//   if (after>before) wsBroadcast("watchlist", getWatchlist());
//   return Math.max(0, after-before);
// }
// async function watchAddEquities(eqs=[]){
//   const before = normEquities(WATCH).length;
//   for (const s of eqs) { const u = canon(s); if (u) WATCH.equities.add(u); }
//   const after = normEquities(WATCH).length;
//   if (after>before) wsBroadcast("watchlist", getWatchlist());
//   return Math.max(0, after-before);
// }
// async function watchAddEquities(symbols=[]) {
//   for (const raw of symbols) {
//     const sym = normalizeSymbol(raw);
//     WATCH.equities.add(sym);          // keep "ES" (not "/ES"), keep "SPX"
//   }
//   wsBroadcast("watchlist", getWatchlist());
//   return Array.from(WATCH.equities);
// }
async function watchAddEquities(symbols = []) {
  for (const raw of symbols) {
    const sym = normalizeSymbol(raw);   // "/ES" -> "ES"
    WATCH.equities.add(sym);
    if (sym === "ES") {
      FUT_FRONT.delete("ES");
      CACHE.conidBySym.delete("ES");
      await resolveFrontMonthES().catch(()=>{});
    }
  }
  // wsBroadcast("watchlist", getWatchlist());
  wsBroadcast("watchlist", makeWatchlistPayload());
  return Array.from(WATCH.equities);
}
function fixEsSpxCollision(pairs){
  const es = pairs.find(p => p.sym === "ES");
  const spx = pairs.find(p => p.sym === "SPX");
  if (es && spx && String(es.conid) === String(spx.conid)) {
    // bad: ES got the SPX index conid; fix & re-push ES
    CACHE.conidBySym.delete("ES");
    FUT_FRONT.delete("ES");
  }
}
// async function ibConidForSymbol(sym){
//   if (!isNonEmptyStr(sym)) return null;
//   const key = canon(sym);

//   // cache hit?
//   const cached = CACHE.conidBySym.get(key);
//   if (cached && cached.ts && (Date.now()-cached.ts) < CONID_TTL_MS) return cached.conid;

//   // MOCK mode quick map
//   if (MOCK){
//     const demo = { AAPL:"265598", NVDA:"4815747", MSFT:"272093", SPY:"756733", SPX:"416904", ES:"123456" };
//     const c = demo[key] || String(100000+Math.floor(Math.random()*9e5));
//     CACHE.conidBySym.set(key, { conid:c, ts:Date.now() });
//     return c;
//   }

//   // search once
//   let best = null;
//   try {
//     const body = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
//     const list = Array.isArray(body) ? body : (body ? [body] : []);

//     // prefer STK, then IND, then FUT
//     const pick = (secType) =>
//       list.find(r => String(r?.symbol||"").toUpperCase()===key &&
//                      (r?.sections||[]).some(s=>s?.secType===secType) && r?.conid);

//     best = pick("STK") || pick("IND") || pick("FUT") || list.find(r => r?.conid);
//   } catch {}

//   // FUT (ES): choose a front contract if choices exist
//   if (best && (best.sections||[]).some(s=>s.secType==="FUT")) {
//     // some responses include Contracts array; pick nearest expiry
//     try {
//       const info = await ibFetch(`/iserver/secdef/info?conid=${best.conid}`);
//       const contracts = info?.Contracts || info || [];
//       if (Array.isArray(contracts) && contracts.length) {
//         const now = Date.now();
//         contracts.sort((a,b)=>{
//           const ta = Date.parse(a?.expiry || a?.maturity || "2100-01-01");
//           const tb = Date.parse(b?.expiry || b?.maturity || "2100-01-01");
//           return Math.abs(ta-now) - Math.abs(tb-now);
//         });
//         const fut = contracts.find(c=>c?.conid) || contracts[0];
//         if (fut?.conid) best = { ...best, conid: String(fut.conid) };
//       }
//     } catch {}
//   }

//   const conid = best?.conid ? String(best.conid) : null;
//   if (conid) CACHE.conidBySym.set(key, { conid, ts:Date.now() });
//   return conid;
// }
// --- helpers & config (place near your other consts) ---
// const CONID_TTL_MS = 24 * 60 * 60 * 1000;
// const FUT_ALIASES = new Map([["/ES","ES"]]);

function isNonEmptyStr(s){ return typeof s === "string" && s.trim().length > 0; }
function canon(s){ return (FUT_ALIASES.get(String(s).trim().toUpperCase()) || String(s).trim().toUpperCase()); }

// Prefer by secType, but allow a symbol-specific override
function pickBestBySecType(list, key){
  if (!Array.isArray(list) || !list.length) return null;

  const hasType = (r, t) => (r?.sections || []).some(s => s?.secType === t);
  const symEq   = (r) => String(r?.symbol || "").toUpperCase() === key;

  // SPX: prefer IND
  if (key === "SPX") {
    const ind = list.find(r => symEq(r) && hasType(r, "IND") && r?.conid);
    if (ind) return ind;
  }

  // ES: prefer FUT root hit
  if (key === "ES") {
    const fut = list.find(r => symEq(r) && hasType(r, "FUT") && r?.conid);
    if (fut) return fut;
  }

  // General preference: STK -> IND -> FUT -> anything with a conid
  return (
    list.find(r => symEq(r) && hasType(r, "STK") && r?.conid) ||
    list.find(r => symEq(r) && hasType(r, "IND") && r?.conid) ||
    list.find(r => symEq(r) && hasType(r, "FUT") && r?.conid) ||
    list.find(r => r?.conid)
  );
}

// For FUT roots (ES), choose the nearest-dated actual contract
async function chooseFrontMonth(conidMaybeRoot){
  try{
    const info = await ibFetch(`/iserver/secdef/info?conid=${encodeURIComponent(conidMaybeRoot)}`);
    const contracts = Array.isArray(info?.Contracts) ? info.Contracts : (Array.isArray(info) ? info : []);
    if (!contracts.length) return String(conidMaybeRoot);

    const now = Date.now();
    // parse any of expiry|maturity|lastTradingDay; fall back way out if missing
    const getTs = (c) => {
      const d = c?.expiry || c?.maturity || c?.lastTradingDay || "2100-01-01";
      const t = Date.parse(d); 
      return Number.isFinite(t) ? t : Date.parse("2100-01-01");
    };

    // prefer nearest future >= now; else the closest overall
    const future = contracts
      .map(c => ({ c, ts: getTs(c) }))
      .filter(x => x.ts >= now)
      .sort((a,b) => a.ts - b.ts)[0];

    const best = future || contracts
      .map(c => ({ c, ts: getTs(c) }))
      .sort((a,b) => Math.abs(a.ts - now) - Math.abs(b.ts - now))[0];

    return String(best?.c?.conid || conidMaybeRoot);
  }catch{
    return String(conidMaybeRoot);
  }
}

// --- DROP-IN: ibConidForSymbol ---
async function ibConidForSymbol(sym){
  if (!isNonEmptyStr(sym)) return null;
  const key = canon(sym); // "/ES" -> "ES", uppercased

  // cache hit?
  const cached = CACHE.conidBySym.get(key);
  if (cached?.conid && (Date.now() - (cached.ts || 0)) < CONID_TTL_MS) return cached.conid;

  // MOCK mode
  // if (MOCK){
  //   const demo = { AAPL:"265598", NVDA:"4815747", MSFT:"272093", SPY:"756733", SPX:"416904", ES:"123456" };
  //   const c = demo[key] || String(100000 + Math.floor(Math.random()*9e5));
  //   CACHE.conidBySym.set(key, { conid:c, ts:Date.now() });
  //   return c;
  // }

  // Query IB
  let best = null;
  try{
    const body = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
    const list = Array.isArray(body) ? body : (body ? [body] : []);
    best = pickBestBySecType(list, key);
  }catch{ /* ignore */ }

  // If FUT root (ES), translate root to actual front-month contract
  if (best && (best.sections||[]).some(s => s.secType === "FUT")) {
    best = { ...best, conid: await chooseFrontMonth(best.conid) };
  }

  const conid = best?.conid ? String(best.conid) : null;
  if (conid) CACHE.conidBySym.set(key, { conid, ts: Date.now() });
  return conid;
}
async function watchAddOptions(options=[]){
  if (!Array.isArray(WATCH.options)) WATCH.options=[];
  const before = normOptions(WATCH).length;
  for (const raw of options){
    const o = {
      underlying:String(raw?.underlying||"").toUpperCase(),
      expiration:String(raw?.expiration||""),
      strike:Number(raw?.strike),
      right:rightNormalize(raw?.right),
    };
    if (o.underlying && o.right && Number.isFinite(o.strike)) WATCH.options.push(o);
  }
  const after = normOptions(WATCH).length;
  if (after>before) wsBroadcast("watchlist", makeWatchlistPayload());//wsBroadcast("watchlist", getWatchlist());
  return Math.max(0, after-before);
}

/* ================= MONTH PARSING (3rd Friday) ================= */
function thirdFriday(year, m0){
  const d = new Date(Date.UTC(year, m0, 1));
  while (d.getUTCDay()!==5) d.setUTCDate(d.getUTCDate()+1);
  d.setUTCDate(d.getUTCDate()+14);
  const y=d.getUTCFullYear(), m=String(d.getUTCMonth()+1).padStart(2,"0"), dd=String(d.getUTCDate()).padStart(2,"0");
  return `${y}-${m}-${dd}`;
}
function monthsTokensToThirdFridays(tokensStr){
  if (!tokensStr) return [];
  const map={JAN:0,FEB:1,MAR:2,APR:3,MAY:4,JUN:5,JUL:6,AUG:7,SEP:8,OCT:9,NOV:10,DEC:11};
  const out=[];
  for (const tok of tokensStr.split(";")){
    const m = tok.trim().toUpperCase();
    const mt = m.match(/^([A-Z]{3})(\d{2})$/);
    if (!mt) continue;
    const mon=map[mt[1]]; if (mon==null) continue;
    const yr = 2000+Number(mt[2]);
    out.push(thirdFriday(yr, mon));
  }
  return uniq(out).sort();
}

/* ================= REQUEST QUEUE (429-safe) ================= */
async function ibQueued(label, doFetch){
  const now = Date.now();
  const wait = Math.max(0, (lastReqEndedAt + MIN_GAP_MS) - now);
  if (wait>0) await sleep(wait);

  let attempt = 0, res, err;
  while (attempt <= MAX_RETRIES){
    try {
      res = await doFetch();
      break;
    } catch (e) {
      if (String(e?.code)==="IB_429" || String(e?.message||"").includes("IB 429 ")){
        const backoff = clamp(300 * Math.pow(2, attempt) + Math.random()*200, 300, 4000);
        if (attempt < MAX_RETRIES){ await sleep(backoff); attempt++; continue; }
      }
      err = e; break;
    }
  }
  lastReqEndedAt = Date.now();
  if (err) throw err;
  return res;
}

/* ================= IB FETCH CORE ================= */
function safeJson(s){ try { return JSON.parse(s); } catch { return null; } }

async function ibReauth(){ try { await ibFetch("/iserver/reauthenticate",{method:"POST"},true); } catch {} }
async function ibPrimeSession(){
  if (MOCK){ CACHE.sessionPrimed = true; return; }
  const now = Date.now();
  if (CACHE.sessionPrimed) return;
  if (CACHE.primingSince && (now - CACHE.primingSince) < 1000) return;
  CACHE.primingSince = now;
  try { await ibFetch("/iserver/auth/status"); } catch(e){}
  try {
    await ibFetch("/iserver/accounts");
    CACHE.sessionPrimed = true;
    console.log("[IB] session primed.");
  } catch { await sleep(300); try { await ibFetch("/iserver/accounts"); CACHE.sessionPrimed = true; } catch {} }
}
async function ibFetch(path, opts={}, _retry=false){
  if (MOCK) throw new Error("MOCK mode");
  if (!CACHE.sessionPrimed) await ibPrimeSession();
  const url = path.startsWith("http") ? path : `${IB_BASE}${path}`;
  const res = await ibQueued(path, async ()=>{
    let r;
    try { r = await fetch(url, { ...opts, redirect:"follow" }); }
    catch (e) { throw new Error(`IB fetch failed: ${url} :: ${e.message}`); }
    if (r.status === 401 && !_retry){ await ibReauth(); return ibFetch(path, opts, true); }
    const text = await r.text();
    const body = text ? safeJson(text) : null;
    if (r.status === 429){ const err = new Error(`IB 429 ${url} :: ${text||"null"}`); err.code="IB_429"; throw err; }
    if (!r.ok){
      const msg = (typeof body==="object" && body) ? JSON.stringify(body) : text;
      if (r.status===400 && (msg||"").toLowerCase().includes("no bridge")) return { _error:true, status:400, body: body ?? msg };
      throw new Error(`IB ${r.status} ${url} :: ${msg}`);
    }
    return body;
  });
  return res;
}

async function ibConidForStock(sym){
  if (!isNonEmptyStr(sym)) return null;
  const key = sym.toUpperCase();
  const cached = CACHE.conidBySym.get(key);
  if (cached && cached.ts && (Date.now()-cached.ts) < CONID_TTL_MS) return cached.conid;

  if (MOCK){
    const demo = { AAPL:"265598", NVDA:"4815747", MSFT:"272093", SPY:"756733" };
    const c = demo[key] || String(100000+Math.floor(Math.random()*9e5));
    CACHE.conidBySym.set(key, { conid:c, ts:Date.now() });
    return c;
  }

  let conid=null;
  try {
    const body = await ibFetch(`/trsrv/stocks?symbols=${encodeURIComponent(key)}`);
    const arr  = Array.isArray(body) ? body : (body ? [body] : []);
    for (const row of arr){
      if (String(row?.symbol||"").toUpperCase() !== key) continue;
      conid = row?.conid || row?.contracts?.[0]?.conid;
      if (conid) break;
    }
    if (!conid && arr[0]) conid = arr[0]?.conid || arr[0]?.contracts?.[0]?.conid;
  } catch {}
  if (!conid){
    try {
      const body2 = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
      const list  = Array.isArray(body2) ? body2 : (body2 ? [body2] : []);
      let best = null;
      for (const r of list){
        if (String(r?.symbol||"").toUpperCase() !== key) continue;
        const hasSTK = (r?.sections||[]).some(s => s?.secType==="STK");
        if (hasSTK && r?.conid){ best = r; break; }
      }
      if (!best){ best = list.find(r => (r?.sections||[]).some(s=>s?.secType==="STK") && r?.conid); }
      if (best?.conid) conid = String(best.conid);
    } catch {}
  }
  if (conid){ CACHE.conidBySym.set(key, { conid:String(conid), ts:Date.now() }); return String(conid); }
  return null;
}
app.get("/debug/conid", async (req,res)=>{
  try{
    const symbol = String(req.query.symbol||"").toUpperCase();
    if (!symbol) return res.status(400).json({ error:"symbol required" });
    const conid = await ibConidForSymbol(symbol);
    if (!conid) return res.status(404).json({ error:`No conid for ${symbol}` });
    res.json({ symbol, conid:String(conid) });
  }catch(e){ res.status(500).json({ error:e.message }); }
});
async function ibOptMonthsForSymbol(sym){
  const key = canon(sym);
  const hit = CACHE.expBySym.get(key);
  if (hit && (Date.now()-hit.ts) < EXP_TTL_MS) return hit.expirations;

  if (MOCK){
    const exps = [ "2025-11-21","2025-12-19","2026-01-16" ];
    CACHE.expBySym.set(key, { ts:Date.now(), expirations:exps });
    return exps;
  }

  const body = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
  const list = Array.isArray(body) ? body : (body ? [body] : []);
  const rec  = list.find(x => String(x?.symbol||"").toUpperCase()===key) || list[0];
  const optSec = (rec?.sections||[]).find(s => s?.secType==="OPT");
  const tokens = optSec?.months || "";
  const exps = monthsTokensToThirdFridays(tokens);
  CACHE.expBySym.set(key, { ts:Date.now(), expirations:exps });
  return exps;
}

// async function ibOptMonthsForSymbol(sym){
//   const key = sym.toUpperCase();
//   const hit = CACHE.expBySym.get(key);
//   if (hit && (Date.now()-hit.ts) < EXP_TTL_MS) return hit.expirations;

//   if (MOCK){
//     const exps = [
//       "2025-11-21", "2025-12-21", "2026-01-16", "2026-02-20"
//     ];
//     CACHE.expBySym.set(key, { ts:Date.now(), expirations:exps });
//     return exps;
//   }

//   const body = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
//   const list = Array.isArray(body) ? body : (body ? [body] : []);
//   const rec  = list.find(x => String(x?.symbol||"").toUpperCase()===key) || list[0];
//   const optSec = (rec?.sections||[]).find(s => s?.secType==="OPT");
//   const tokens = optSec?.months || "";
//   const exps = monthsTokensToThirdFridays(tokens);
//   CACHE.expBySym.set(key, { ts:Date.now(), expirations:exps });
//   return exps;
// }

/* ================= MAPPERS ================= */
// function mapEquitySnapshot(sym, s){
//   return { symbol:sym, last:num(s["31"]), bid:num(s["84"]), ask:num(s["86"]), iv:num(s["7059"]), ts:Date.now() };
// }
// function mapEquitySnapshot(sym, s){
//   const last = s['31'] ?? null;
//   const bid  = s['84'] ?? null;
//   const ask  = s['86'] ?? null;
//   const iv   = s['7059'] ?? null; // can be null for SPX
//   return { symbol: sym, last, bid, ask, iv, ts: Date.now() };
// }

// function mapOptionTick(ul, expiration, strike, right, s0){
//   return { underlying:ul, expiration, strike, right, last:num(s0["31"]), bid:num(s0["84"]), ask:num(s0["86"]), ts:Date.now() };
// }

// Real-time last price cache: lastBySymbol["SPY"] = 504.23, etc.
const lastBySymbol = Object.create(null);
const getLast = (sym) => {
  const v = lastBySymbol[sym?.toUpperCase()?.replace(/^\//, "")];
  return Number.isFinite(v) ? v : null;
};


function mapEquitySnapshot(sym, s) {
  const last = toNumStrict(s["31"]);
  const bid  = toNumStrict(s["84"]);
  const ask  = toNumStrict(s["86"]);
  const iv   = normalizeIv(s["7059"]);        // may be null (e.g., SPX)
  return {
    symbol: sym,
    last: last ?? null,
    bid:  bid ?? null,
    ask:  ask ?? null,
    iv:   iv ?? null,
    ts: Date.now()
  };
}

// function mapOptionTick(ul, expiration, strike, right, s0) {
//   return {
//     underlying: ul,
//     expiration,
//     strike: Number(strike),
//     right,
//     last: toNumStrict(s0["31"]),
//     bid:  toNumStrict(s0["84"]),
//     ask:  toNumStrict(s0["86"]),
//     ts: Date.now()
//   };
// }
 function mapOptionTick(ul, expiration, strike, right, s0) {
   return {
     underlying: ul,
     expiration,
     strike: Number(strike),
     right,                                  // "C" | "P"
     last:   toNumStrict(s0["31"]),
     bid:    toNumStrict(s0["84"]),
     ask:    toNumStrict(s0["86"]),
     volume: toNumStrict(s0["7295"]),        // NEW
     oi:     toNumStrict(s0["7635"]),        // NEW
     ts: Date.now()
   };
 }

/* ================= POLLERS ================= */
async function ibFetchJson(url, init){
  const r = await fetch(url, init);
  if (!r.ok){ const body = await r.text().catch(()=> ""); throw new Error(`IB ${r.status} ${url} :: ${body || "null"}`); }
  try { return await r.json(); } catch { return {}; }
}
if (typeof fetch === "undefined") { global.fetch = (await import("node-fetch")).default; }

// Resolve ES front-month once in a while (10 min TTL)
async function resolveFrontMonthES(){
  const cur = FUT_FRONT.get("ES");
  if (cur && Date.now() - (cur.ts || 0) < 10 * 60 * 1000) return cur;

  // Try both "ES" and "/ES" depending on your IB resolver
  const conid =
    await ibConidForSymbol("ES").catch(()=>null) ||
    await ibConidForSymbol("/ES").catch(()=>null);

  if (conid){
    const rec = { conid: String(conid), ts: Date.now() };
    FUT_FRONT.set("ES", rec);
    return rec;
  }
  return null;
}

// Memoizing conid fetch for equities/indices (24h TTL)
// async function ensureConid(symbol){
//   const key = (symbol || "").toUpperCase();
//   const cached = CACHE.conidBySym.get(key);
//   if (cached?.conid && Date.now() - (cached.ts || 0) < 24 * 60 * 60 * 1000) {
//     return cached.conid;
//   }
//   const conid = await ibConidForSymbol(key).catch(()=>null);
//   if (conid){
//     CACHE.conidBySym.set(key, { conid: String(conid), ts: Date.now() });
//   }
//   return conid;
// }
async function ensureConid(symbol){
  const key = (symbol || "").toUpperCase();

  if (key === "ES") {
    // always use front-month resolver; never reuse an equity/IND conid
    const r = await resolveFrontMonthES().catch(()=>null);
    return r?.conid || null;
  }

  const cached = CACHE.conidBySym.get(key);
  if (cached?.conid && Date.now() - (cached.ts || 0) < 24 * 60 * 60 * 1000) {
    return cached.conid;
  }
  const conid = await ibConidForSymbol(key).catch(()=>null);
  if (conid){
    CACHE.conidBySym.set(key, { conid: String(conid), ts: Date.now() });
  }
  return conid;
}
// async function ensureConid(symbol){
//   const key = (symbol || "").toUpperCase();

//   // SPECIAL: ES uses front-month resolver
//   if (key === "ES") {
//     const r = await resolveFrontMonthES().catch(()=>null);
//     return r?.conid || null;
//   }

//   // memoized for everything else
//   const cached = CACHE.conidBySym.get(key);
//   if (cached?.conid && Date.now() - (cached.ts || 0) < 24 * 60 * 60 * 1000) {
//     return cached.conid;
//   }
//   const conid = await ibConidForSymbol(key).catch(()=>null);
//   if (conid){
//     CACHE.conidBySym.set(key, { conid: String(conid), ts: Date.now() });
//   }
//   return conid;
// }

// async function ensureConid(symbol){
//   const key = (symbol || "").toUpperCase();
//   if (key === "ES") {
//     const r = await resolveFrontMonthES();
//     return r?.conid || null;
//   }
//   const cached = CACHE.conidBySym.get(key);
//   if (cached?.conid && Date.now() - (cached.ts || 0) < 24*60*60*1000) return cached.conid;
//   const conid = await ibConidForSymbol(key).catch(()=>null);
//   if (conid) CACHE.conidBySym.set(key, { conid: String(conid), ts: Date.now() });
//   return conid;
// }

// function normalizeSymbol(s) {
//   const up = String(s || "").trim().toUpperCase();
//   return FUT_ALIASES.get(up) || up;   // "/ES" -> "ES"
// }
// ===== CONID resolution for indices & futures =====
// const FUT_FRONT = new Map(); // ES -> { conid, ts }

// function symbolToConid(symbol){
//   const s = (symbol||"").toUpperCase();
//   if (s === "ES") {
//     const r = FUT_FRONT.get("ES");
//     if (r?.conid) return r.conid; // front month
//   }
//   return CACHE.conidBySym.get(s)?.conid || null;
// }

// async function resolveFrontMonthES(){
//   const cur = FUT_FRONT.get("ES");
//   if (cur && Date.now() - (cur.ts||0) < 10*60*1000) return cur; // 10 min TTL

//   // Your existing IB symbol lookup; try both variants
//   const conid =
//     await ibConidForSymbol("ES").catch(()=>null) ||
//     await ibConidForSymbol("/ES").catch(()=>null);
//   if (conid){
//     const rec = { conid:String(conid), ts:Date.now() };
//     FUT_FRONT.set("ES", rec);
//     return rec;
//   }
//   return null;
// }

// async function ensureConid(symbol){
//   const key = (symbol||"").toUpperCase();
//   // ES handled by resolveFrontMonthES
//   if (key === "ES") {
//     const r = await resolveFrontMonthES();
//     return r?.conid || null;
//   }
//   const cached = CACHE.conidBySym.get(key);
//   if (cached?.conid && Date.now() - (cached.ts||0) < 24*60*60*1000) return cached.conid;

//   const conid = await ibConidForSymbol(key).catch(()=>null);
//   if (conid) CACHE.conidBySym.set(key, { conid:String(conid), ts:Date.now() });
//   return conid;
// }

// async function pollEquitiesOnce(){
//   try {
//     const symbols = (normEquities?.(WATCH) || []).map(s => (s || "").toUpperCase());
//     if (!symbols.length) return;

//     // 1) Make sure ES front-month is resolved once before polling, if we need it
//     if (symbols.some(s => s === "/ES" || s === "ES")) {
//       const esConid = symbolToConid("ES");
//       if (!esConid) await resolveFrontMonthES().catch(()=>{});
//     }

//     // 2) Build symbol↔conid pairs using symbolToConid, falling back to ensureConid()
//     const pairs = [];
//     for (const sym of symbols){
//       let conid = symbolToConid(sym);
//       if (!conid) {
//         // equities/indices: try normal resolver
//         if (sym !== "/ES" && sym !== "ES") {
//           conid = await ensureConid(sym).catch(()=>null);
//         } else {
//           // futures: re-resolve front month if needed
//           await resolveFrontMonthES().catch(()=>{});
//           conid = symbolToConid(sym);
//         }
//       }
//       if (conid) pairs.push({ sym: sym === "/ES" ? "ES" : sym, conid: String(conid) });
//     }
//     if (!pairs.length) {
//       // Push placeholders so UI shows the symbols even if unresolved this tick
//       const placeholders = symbols.map(sym => ({ symbol: sym, last: null, ts: Date.now() }));
//       STATE.equities_ts = placeholders;
//       wsBroadcast("equity_ts", placeholders);
//       return;
//     }

//     // 3) Fetch snapshots (or mock)
//     let rows = [];
//     if (MOCK) {
//       // simple mock: give something stable-ish per symbol
//       for (const p of pairs) {
//         const base = { SPY: 679, QQQ: 627, IWM: 244, AAPL: 267, NVDA: 205, MSFT: 514, SPX: 5600, ES: 5600 };
//         const last = (base[p.sym] ?? 100) + Math.random()*0.5 - 0.25;
//         rows.push({ symbol: p.sym, last: Number(last.toFixed(2)), bid: last-0.1, ask: last+0.1, iv: 40, ts: Date.now() });
//       }
//     } else {
//       const conids = pairs.map(p => p.conid).join(",");
//       // fields: 31=last, 84=bid, 86=ask, 7059=iv (or whatever your mapper expects)
//       const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
//       for (const p of pairs) {
//         const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid) === String(p.conid)) : null;
//         if (!s) {
//           rows.push({ symbol: p.sym, last: null, ts: Date.now() });
//           continue;
//         }
//         rows.push(mapEquitySnapshot(p.sym, s));
//       }
//     }

//     // 4) Preserve order as `symbols`, and ensure any missing symbols get placeholders
//     const bySym = new Map(rows.map(r => [r.symbol, r]));
//     const ordered = symbols.map(sym => bySym.get(sym === "/ES" ? "ES" : sym) || { symbol: sym, last: null, ts: Date.now() });

//     STATE.equities_ts = ordered;
//     wsBroadcast("equity_ts", ordered);
//   } catch (e) {
//     console.warn("pollEquitiesOnce:", e?.message || e);
//   }
// }
// async function pollEquitiesOnce(){
//   try{
//     // 1) normalize current watchlist
//     const symbols = Array.from(WATCH.equities || []).map(normalizeSymbol);
//     if (!symbols.length) return;

//     // 2) get conids (memoized) — ES via resolveFrontMonth
//     const pairs = [];
//     for (const sym of symbols){
//       try{
//         const conid = await ensureConid(sym);
//         if (conid) pairs.push({ sym, conid });
//       }catch{}
//     }
//     if (!pairs.length) return;

//     // 3) fetch IB snapshot
//     let rows=[];
//     if (MOCK){
//       rows = pairs.map(p => ({
//         symbol: p.sym,
//         last: 100 + Math.random()*50,
//         bid: 0, ask: 0, iv: p.sym==="MSFT" ? 0.40 : 1.00,
//         ts: Date.now()
//       }));
//     }else{
//       const conids = pairs.map(p=>p.conid).join(",");
//       // 31: last, 84: bid, 86: ask, 7059: implied vol (may be missing for index)
//       const snaps  = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
//       for (const p of pairs){
//         const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid)===String(p.conid)) : null;
//         if (!s) continue;
//         rows.push(mapEquitySnapshot(p.sym, s)); // keep your existing mapper
//       }
//     }

//     if (rows.length){
//       STATE.equities_ts = rows;
//       wsBroadcast("equity_ts", rows);
//     }
//   }catch(e){
//     console.warn("pollEquitiesOnce:", e.message);
//   }
// }
// helper: prefer last; else mid; else NaN
function midOrLast(t) {
  const last = Number(t?.last);
  if (Number.isFinite(last) && last !== 0) return last;
  const bid = Number(t?.bid), ask = Number(t?.ask);
  if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0) {
    return (bid + ask) / 2;
  }
  return NaN;
}
// function midOrLast(row) {
//   const last = Number(row?.last);
//   if (Number.isFinite(last) && last !== 0) return last;
//   const bid = Number(row?.bid), ask = Number(row?.ask);
//   if (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0) return (bid + ask) / 2;
//   return NaN;
// }

function fresh(row, ms=60_000) { return row?.ts && (Date.now() - row.ts) < ms; }
function safeMapEquity(sym, snap) {
  const r = mapEquitySnapshot(sym, snap); // your existing mapper (31/84/86/7059)
  if (r && r.last != null && (r.bid == null || r.ask == null)) {
    const px = Number(r.last);
    if (Number.isFinite(px)) {
      // 2bp synthetic spread
      r.bid ??= Number((px * 0.9998).toFixed(2));
      r.ask ??= Number((px * 1.0002).toFixed(2));
    }
  }
  return r;
}
async function pollEquitiesOnce(){
  try{
    const symbols = Array.from(WATCH.equities || []).map(normalizeSymbol);
    if (!symbols.length) return;

    const pairs = [];
    for (const sym of symbols){
      try {
        const conid = await ensureConid(sym); // ES should resolve to front-month
        if (conid) pairs.push({ sym, conid });
      } catch {}
    }
    if (!pairs.length) return;

    let rows = [];
    if (MOCK){
      rows = pairs.map(p => ({
        symbol: p.sym,
        last: 100 + Math.random()*50,
        bid: 0, ask: 0, iv: p.sym==="MSFT" ? 0.40 : 1.00,
        ts: Date.now()
      }));
    }else{
      const conids = pairs.map(p=>p.conid).join(",");
      // 31: last, 84: bid, 86: ask, 7059: IV
      const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
      for (const p of pairs){
        const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid)===String(p.conid)) : null;
        if (!s) continue;
        // rows.push(mapEquitySnapshot(p.sym, s));
        rows.push(safeMapEquity(p.sym, s));
      }
    }
    // ----- compute ES–SPX basis (points) -----
    let es_spx_basis = null;
    const bySym = new Map(rows.map(r => [normalizeSymbol(r.symbol), r]));
    const es = bySym.get("ES");
    const spx = bySym.get("SPX");
    if (es && spx && fresh(es) && fresh(spx)) {
      const esPx  = midOrLast(es);
      const spxPx = midOrLast(spx);
      if (Number.isFinite(esPx) && Number.isFinite(spxPx)) {
        es_spx_basis = Number((esPx - spxPx).toFixed(2));
      }
      
      for (const r of rows) if (Number.isFinite(r?.last)) onTick(r.symbol, r.last);
      // broadcast the richer payload your UI expects
      // in pollEquitiesOnce(), right after you build `rows`
      for (const r of rows) {
        if (Number.isFinite(r?.last)) {
          updateUnderlyingLast(r.symbol, r.last, r.ts ?? Date.now()); // <— NEW
        }
      }      
      STATE.equities_ts = rows;
      // wsBroadcast("equity_ts", { rows, es_spx_basis });
      wsBroadcast("equity_ts", rows);
      if (es_spx_basis != null) wsBroadcast("basis", { es_spx_basis });      
    }
  }catch(e){
    console.warn("pollEquitiesOnce:", e.message);
  }
}
// app.get('/insights/notables', (req, res) => {
//   try {
//     const cfg = Object.fromEntries(Object.entries(req.query).map(([k,v])=>[k, Number(v)]));
//     // const payload = buildNotables({ sweeps: memory.sweeps||[], blocks: memory.blocks||[] }, cfg);
//     const payload = buildNotables({ sweeps: STATE.sweeps || [], blocks: STATE.blocks || [] }, cfg);
//     res.json(payload);
//   } catch (e) { res.status(500).json({ error: e?.message||'fail' }); }
// });



// const NOTABLES_DEFAULT = { 
//   windowMs: 5*60_000, 
//   minNotional: 75_000, 
//   topN: 50, 
//   side: "ANY", 
//   expiryMaxDays: 365 
// };


app.get("/api/flow/blocks", (req,res)=>{
  const minNotional = Number(req.query.minNotional ?? 50_000);
  const limit = Number(req.query.limit ?? 50);
  const rows = computeBlocks(9999, minNotional)
    .filter(passesPublishGate)                                  // 🔒 NEW
    .slice(0, limit)
    .map(r => ({ ...r, ul_px: (getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null) }));
  res.json({ rows, ts: Date.now() });
});

app.get("/api/flow/sweeps", (req,res)=>{
  const minNotional = Number(req.query.minNotional ?? 25_000);
  const limit = Number(req.query.limit ?? 200);
  const base = computeSweeps(9999, minNotional)
    .filter(passesPublishGate)                                  // 🔒 NEW
    .slice(0, limit);
  const rows = base.map(r => attachDayStats({
    ...r,
    ul_px: (getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null)
  }));
  res.json({ rows, ts: Date.now() });
});

// app.get("/api/flow/blocks", (req,res)=>{
//   const minNotional = Number(req.query.minNotional ?? 50_000);
//   const limit = Number(req.query.limit ?? 50);
//   const rows = computeBlocks(9999, minNotional).slice(0, limit).map(r => ({
//     ...r,
//     ul_px: (getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null)
//   }));
//   res.json({ rows, ts: Date.now() });
// });

// app.get("/api/flow/sweeps", (req,res)=>{
//   const minNotional = Number(req.query.minNotional ?? 25_000);
//   const limit = Number(req.query.limit ?? 200);
//   const base = computeSweeps(9999, minNotional).slice(0, limit);
//   const rows = base.map(r => attachDayStats({
//     ...r,
//     ul_px: (getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null)
//   }));
//   res.json({ rows, ts: Date.now() });
// });

app.get("/debug/snapshot_raw", async (req,res)=>{
  try {
    const conids = String(req.query.conids||"").trim();
    if (!conids) return res.status(400).json({ error:"conids required" });
    const body = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
    res.json(body);
  } catch(e){ res.status(500).json({ error:e.message }); }
});

// async function pollEquitiesOnce(){
//   try{
//     // 1) normalize & de-dup watchlist ("/ES" -> "ES")
//     const symbols = Array.from(new Set(Array.from(WATCH.equities || []).map(normalizeSymbol)));
//     if (!symbols.length) return;

//     // 2) resolve conids (ES via front-month resolver in ensureConid)
//     let pairs = [];
//     for (const sym of symbols){
//       try{
//         const conid = await ensureConid(sym);
//         if (conid) pairs.push({ sym: sym.toUpperCase(), conid: String(conid) });
//       } catch {}
//     }
//     if (!pairs.length) return;

//     // 2a) guard against ES/SPX conid collisions (ES accidentally resolved as SPX index)
//     const es = pairs.find(p => p.sym === "ES");
//     const spx = pairs.find(p => p.sym === "SPX");
//     if (es && spx && es.conid === spx.conid){
//       // nuke ES caches so front-month resolver runs cleanly
//       try { FUT_FRONT?.delete?.("ES"); } catch {}
//       try { CACHE?.conidBySym?.delete?.("ES"); } catch {}
//       // re-resolve ES once
//       try {
//         const fixed = await ensureConid("ES");
//         if (fixed && String(fixed) !== spx.conid){
//           // replace the bad ES pair
//           pairs = pairs.map(p => p.sym === "ES" ? ({ sym: "ES", conid: String(fixed) }) : p);
//         } else {
//           // if still colliding, drop ES for this tick; it will fix on next loop
//           pairs = pairs.filter(p => p.sym !== "ES");
//         }
//       } catch {
//         pairs = pairs.filter(p => p.sym !== "ES");
//       }
//     }

//     if (!pairs.length) return;

//     // 3) fetch snapshots
//     let rows = [];
//     if (MOCK){
//       const now = Date.now();
//       rows = pairs.map(p => ({
//         symbol: p.sym,
//         last: 100 + Math.random()*50 + (p.sym === "ES" ? 0.37 : 0), // make ES visibly different in mock
//         bid: null, ask: null,
//         iv: p.sym==="MSFT" ? 0.40 : null,
//         ts: now
//       }));
//     } else {
//       const conids = pairs.map(p=>p.conid).join(",");
//       const snaps  = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
//       const arr = Array.isArray(snaps) ? snaps : (snaps?.data ?? []); // be tolerant

//       // Build a fast lookup by conid
//       const byConid = new Map();
//       for (const s of arr) {
//         if (s && (s.conid || s.conid === 0)) byConid.set(String(s.conid), s);
//       }

//       // wrapper around your existing mapper with optional synthetic spread
//       function safeMap(sym, snap){
//         const r = mapEquitySnapshot(sym, snap); // uses 31/84/86/7059
//         if (r && r.last != null && (r.bid == null || r.ask == null)) {
//           // synthesize a tiny spread so UI isn't empty when IB omits bid/ask (indexes/futures)
//           const px = Number(r.last);
//           if (!Number.isNaN(px)) {
//             r.bid ??= Number((px * 0.999).toFixed(2));
//             r.ask ??= Number((px * 1.001).toFixed(2));
//           }
//         }
//         return r;
//       }

//       for (const p of pairs){
//         const s = byConid.get(p.conid);
//         if (!s) continue;
//         const row = safeMap(p.sym, s);
//         if (row) rows.push(row);
//       }
//     }

//     // 4) publish
//     // if (rows.length){
//     //   STATE.equities_ts = rows;
//     //   wsBroadcast("equity_ts", rows);
//     // }
//     if (rows.length){
//       const enriched = rows.map(enrichRow);
//       const basis = computeBasis(enriched); // e.g., +2.15 means ES > SPX by 2.15 pts
//       STATE.equities_ts = enriched;
//       wsBroadcast("equity_ts", { rows: enriched, es_spx_basis: basis });
//     }    
//   }catch(e){
//     console.warn("pollEquitiesOnce:", e?.message || e);
//   }
// }
function enrichRow(r){
  const kind = (r.symbol === "ES") ? "future" :
               (r.symbol === "SPX") ? "index"  : "equity";
  return { ...r, kind };
}
function computeBasis(rows){
  const spx = rows.find(r => r.symbol === "SPX" && r.last != null);
  const es  = rows.find(r => r.symbol === "ES"  && r.last != null);
  if (!spx || !es) return null;
  return Number((es.last - spx.last).toFixed(2)); // ES - SPX
}
// async function pollEquitiesOnce(){
//   try{
//     const symbols = normEquities(WATCH);
//     if (!symbols.length) return;

//     const pairs = [];
//     for (const sym of symbols){
//       try { const conid = await ibConidForSymbol(sym); if (conid) pairs.push({ sym, conid }); }
//       catch {}
//     }
//     if (!pairs.length) return;

//     let rows = [];
//     if (MOCK){
//       rows = pairs.map(p => ({ symbol:p.sym, last:100+Math.random()*50, bid:0, ask:0, ts:Date.now(), iv: p.sym==="MSFT" ? 0.40 : 1.00 }));
//     } else {
//       const conids = pairs.map(p=>p.conid).join(",");
//       const snaps  = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
//       for (const p of pairs){
//         const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid)===String(p.conid)) : null;
//         if (!s) continue;
//         rows.push(mapEquitySnapshot(p.sym, s));
//       }
//     }
//     if (rows.length){ STATE.equities_ts = rows; wsBroadcast("equity_ts", rows); }
//   }catch(e){ console.warn("pollEquitiesOnce:", e.message); }
// }
/* ===== NBBO + OCC key + raw print fanout ===== */
const NBBO_OPT = new Map(); // key: occ -> { bid, ask, ts }

/** Stable key for a given option */
function toOcc(ul, expiration, right, strike) {
  return `${String(ul).toUpperCase()}|${expiration}|${String(right).toUpperCase()}|${Number(strike)}`;
}

/** Convert bar-derived tick to your “raw options print” shape */
function normOptionsPrint({ ul, exp, right, strike, price, qty, nbbo }) {
  const bid = Number(nbbo?.bid ?? NaN);
  const ask = Number(nbbo?.ask ?? NaN);
  const ts  = Date.now();
  const side = (() => {
    if (Number.isFinite(bid) && Number.isFinite(ask)) {
      const mid = (bid + ask) / 2;
      const bps = ((price - mid) / mid) * 1e4;
      if (bps >= 15) return "BUY";
      if (bps <= -15) return "SELL";
    }
    return "UNKNOWN";
  })();
  return {
    // minimal raw print shape
    type: "print",
    symbol: ul,
    option: { expiration: exp, strike, right },
    price, qty, side,
    nbbo: { bid: bid || 0, ask: ask || 0 },
    ts
  };
}

/** Fanout any message to STATE and WS.
 *  - raw prints (type==="print") → broadcast on "prints" topic (optional)
 *  - sweeps/blocks (type==="sweeps"/"blocks") → append to STATE & broadcast
 */
// function pushAndFanout(msg) {
//   try {
//     // If not already enriched, infer now
//     if (!msg.action) {
//       const occ = toOcc(msg.symbol, msg?.option?.expiration, msg?.option?.right, msg?.option?.strike);
//       const bid = Number(msg?.nbbo?.bid), ask = Number(msg?.nbbo?.ask);
//       const mid = (Number.isFinite(bid) && Number.isFinite(ask) && bid>0 && ask>0) ? (bid+ask)/2 : undefined;
//       const inf = inferActionForOptionTrade({
//         occ,
//         qty: Number(msg.qty) || 0,
//         price: Number(msg.price) || 0,
//         side: String(msg.side || "UNKNOWN"),
//         book: { bid, ask, mid },
//         cumulativeVol: undefined, // unknown at this stage
//         oi: undefined,            // unknown at this stage
//         asOfYMD: etYMD()
//       });
//       msg = Object.assign(msg, inf);
//     }
//   } catch (_) {}
//   if (msg?.type === "sweeps") {
//     STATE.sweeps = [...(STATE.sweeps || []), msg];
//     wsBroadcast("sweeps", STATE.sweeps.slice(-500));
//     return;
//   }
//   if (msg?.type === "blocks") {
//     STATE.blocks = [...(STATE.blocks || []), msg];
//     wsBroadcast("blocks", STATE.blocks.slice(-500));
//     return;
//   }
//   // Optional: expose raw prints to clients
//   if (msg?.type === "print") {
//     // you can keep a rolling buffer if you want
//     // e.g., STATE.prints = [...(STATE.prints||[]), msg].slice(-2000);
//     STATE.prints = [...(STATE.prints || []), msg].slice(-2000);
//     wsBroadcast("prints", [msg]); // light push; clients can aggregate if needed
//   }
// }
// Helpers to normalize structure differences across topics
function legFromMsg(msg) {
  // UL + leg info can be in slightly different places per vendor/topic
  const ul = msg.ul || msg.symbol || msg.underlying || (msg.option && msg.option.underlying) || "";
  const right =
    msg.right ||
    (msg.option && (msg.option.right === "CALL" || msg.option.right === "PUT"
      ? msg.option.right
      : msg.option.right)) ||
    (msg?.option?.right) || msg?.r || msg?.R;

  const strike = msg.strike ?? msg?.option?.strike;
  const expiry = msg.expiry || msg.expiration || msg?.option?.expiration;
  return { ul, right, strike, expiry };
}

function rightToCP(r) {
  if (!r) return undefined;
  const s = String(r).toUpperCase();
  if (s === "CALL" || s === "C") return "C";
  if (s === "PUT"  || s === "P") return "P";
  return undefined;
}

function sideAsTape(s) {
  // Accept BOT/SLD or BUY/SELL, leave UNKNOWN otherwise
  const x = String(s || "").toUpperCase();
  if (x === "BOT" || x === "BUY") return "BUY";
  if (x === "SLD" || x === "SELL") return "SELL";
  return "UNKNOWN";
}

function nbboFrom(msg) {
  const b = Number(msg?.nbbo?.bid);
  const a = Number(msg?.nbbo?.ask);
  const mid = Number.isFinite(b) && Number.isFinite(a) && b > 0 && a > 0 ? (b + a) / 2 : undefined;
  return {
    bid: Number.isFinite(b) ? b : undefined,
    ask: Number.isFinite(a) ? a : undefined,
    mid
  };
}
/* ========== /prices (GET) ========== */
/**
 * GET /prices
 *   ?symbols=SPY,QQQ,ES,SPX   // optional; defaults to current watchlist equities
 *   → { rows:[{symbol,last,bid,ask,iv,ts}], es_spx_basis:number|null, ts:number }
 *
 * Notes:
 * - Uses ensureConid() so ES resolves to front-month.
 * - Preserves input order and includes placeholders when a conid can’t be resolved.
 * - In MOCK mode returns stable-ish synthetic prices.
 */
app.get("/prices", async (req, res) => {
  try {
    const csv = String(req.query.symbols || "").trim();
    const inputSyms = csv
      ? csv.split(",").map((s) => normalizeSymbol(s))
      : Array.from(WATCH.equities || []).map((s) => normalizeSymbol(s));

    // Nothing to do
    const symbols = Array.from(new Set(inputSyms)).filter(Boolean);
    if (!symbols.length) return res.json({ rows: [], es_spx_basis: null, ts: Date.now() });

    // Resolve conids (ES handled inside ensureConid)
    const pairs = [];
    for (const sym of symbols) {
      try {
        const conid = await ensureConid(sym);
        if (conid) pairs.push({ sym, conid: String(conid) });
      } catch (_) {
        // leave unresolved; we'll make a placeholder below
      }
    }

    let rows = [];
    if (MOCK) {
      const base = { SPY: 510, QQQ: 430, IWM: 205, SPX: 5600, ES: 5602 };
      const now = Date.now();
      rows = symbols.map((s) => ({
        symbol: s,
        last: Number(((base[s] ?? 100) + Math.random() * 0.4 - 0.2).toFixed(2)),
        bid: null,
        ask: null,
        iv: s === "MSFT" ? 40 : null,
        ts: now,
      }));
    } else {
      // Snapshot all resolvable conids
      const bySym = new Map();
      if (pairs.length) {
        const conids = pairs.map((p) => p.conid).join(",");
        const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);

        // Map by conid once for quick lookup
        const arr = Array.isArray(snaps) ? snaps : (snaps?.data ?? []);
        const byConid = new Map(arr.map((r) => [String(r.conid), r]));

        // Build rows (preserve exact input order, and add placeholders if needed)
        for (const sym of symbols) {
          const pair = pairs.find((p) => p.sym === sym);
          if (!pair) {
            rows.push({ symbol: sym, last: null, bid: null, ask: null, iv: null, ts: Date.now() });
            continue;
          }
          const snap = byConid.get(pair.conid);
          if (!snap) {
            rows.push({ symbol: sym, last: null, bid: null, ask: null, iv: null, ts: Date.now() });
            continue;
          }
          rows.push(safeMapEquity(sym, snap)); // uses your mapper + synthetic tiny spread when needed
        }
      } else {
        // No conids resolved; still return placeholders
        rows = symbols.map((sym) => ({ symbol: sym, last: null, bid: null, ask: null, iv: null, ts: Date.now() }));
      }
    }

    // Compute ES–SPX basis if both are fresh
    let es_spx_basis = null;
    const bySymOut = new Map(rows.map((r) => [normalizeSymbol(r.symbol), r]));
    const es = bySymOut.get("ES");
    const spx = bySymOut.get("SPX");
    if (es && spx && fresh(es) && fresh(spx)) {
      const esPx = midOrLast(es);
      const spxPx = midOrLast(spx);
      if (Number.isFinite(esPx) && Number.isFinite(spxPx)) {
        es_spx_basis = Number((esPx - spxPx).toFixed(2));
      }
    }

    // Update ultra-light last cache used by other parts of your server
    for (const r of rows) {
      if (Number.isFinite(r?.last)) updateUnderlyingLast(r.symbol, r.last, r.ts ?? Date.now());
    }

    res.json({ rows, es_spx_basis, ts: Date.now() });
  } catch (e) {
    res.status(500).json({ error: e?.message || "failed" });
  }
});

/* Convenience: single-symbol alias */
app.get("/prices/:symbol", async (req, res) => {
  const sym = normalizeSymbol(req.params.symbol || "");
  req.query.symbols = sym;
  return app._router.handle(req, res, () => {}, "get", "/prices");
});
function enrichWithAction(msg) {
  // Already enriched?
  if (msg && (msg.action || msg.action_conf || msg.at)) return msg;

  const { ul, right, strike, expiry } = legFromMsg(msg);
  // If we don't have a proper option leg, skip enrichment gracefully
  if (!ul || !right || !strike || !expiry) return msg;

  const occ = toOcc(ul, expiry, rightToCP(right), strike);
  const book = nbboFrom(msg);

  const inf = inferActionForOptionTrade({
    occ,
    qty: Number(msg.qty) || 0,
    price: Number(msg.price) || 0,
    side: sideAsTape(msg.side),
    book,
    cumulativeVol: Number.isFinite(+msg.cumulativeVol) ? +msg.cumulativeVol : undefined,
    oi: Number.isFinite(+msg.oi) ? +msg.oi : undefined,
    asOfYMD: etYMD()
  });

  // Attach minimal leg fields if missing (so UI can render uniformly)
  if (!msg.ul)       msg.ul = ul;
  if (!msg.right)    msg.right = (rightToCP(right) === "C" ? "CALL" : "PUT");
  if (!msg.strike)   msg.strike = strike;
  if (!msg.expiry)   msg.expiry = expiry;

  // Add enrichment
  return Object.assign(msg, inf);
}

function headlineOfTrade(t /* print|sweep|block */) {
  const right = t.right || t.option?.right;      // "C" | "P" | "CALL" | "PUT"
  const strike = t.strike ?? t.option?.strike;
  const expiry = t.expiry ?? t.option?.expiration;
  const side = (t.side || "UNKNOWN").toUpperCase();

  return {
    type: (t.type || "PRINT").toUpperCase(),     // "PRINT" | "SWEEP" | "BLOCK"
    ul: t.ul || t.symbol || "",
    right,
    strike,
    expiry,
    side,
    notional: Math.round((t.notional ?? (t.qty||0)*(t.price||0)*100) || 0),
    ts: Number(t.ts) || Date.now(),

    // 🔹 carry through enrichment so the badge can render
    action: t.action,             // "BTO" | "BTC" | "STO" | "STC" | ...
    action_conf: t.action_conf,   // "high" | "medium" | "low"
    at: t.at                      // "bid" | "ask" | "mid" | "between"
  };
}
function attachDayStats(msg) {
  const { ul, right, strike, expiry } = legFromMsg(msg);
  if (!ul || !right || !strike || !expiry) return msg;

  const occ = toOcc(ul, expiry, rightToCP(right), strike);
  const ds = DAY_STATS_OPT.get(occ);

  if (ds) {
    msg.cumulativeVol ??= ds.volume;
    msg.oi ??= ds.oi;
    const v = Number(msg.cumulativeVol);
    const o = Number(msg.oi);
    msg.volOiRatio =
      Number.isFinite(o) && o > 0 ? v / o
    : Number.isFinite(v) && v > 0 ? Infinity
    : 0;
  }
  return msg;
}
function pushAndFanout(msg) {
  try {
    // ensure enrichment exists once (prints, sweeps, blocks)
    if (!msg.action) {
      const occ = toOcc(msg.symbol, msg?.option?.expiration, rightToCP(msg?.option?.right), msg?.option?.strike);
      const bid = Number(msg?.nbbo?.bid), ask = Number(msg?.nbbo?.ask);
      const mid = (Number.isFinite(bid) && Number.isFinite(ask) && bid>0 && ask>0) ? (bid+ask)/2 : undefined;
      const inf = inferActionForOptionTrade({
        occ,
        qty: Number(msg.qty) || 0,
        price: Number(msg.price) || 0,
        side: String(msg.side || "UNKNOWN"),
        book: { bid, ask, mid },
        cumulativeVol: msg.cumulativeVol,   // if known, else undefined
        oi: msg.oi,                         // if known, else undefined
        asOfYMD: etYMD()
      });
      Object.assign(msg, inf); // adds action, action_conf, at, priorVol, oi, reason
    }

    // NEW: attach day stats and ratio
    attachDayStats(msg);

    // 🔹 NEW: attach UL price *at event time*
    const ul = msg.ul || msg.symbol;
    const ts = Number(msg.ts) || Date.now();
    const ulPx =
      getUnderlyingLastNear(ul, ts) ??
      getLast(ul) ??                    // very-light fallback
      null;

    msg.ul_px = Number.isFinite(ulPx) ? Number(ulPx) : null;
    // const immediate = computeNotables(NOTABLES_DEFAULT);
    // const immediate = buildNotables(
    //   { sweeps: STATE.sweeps || [], blocks: STATE.blocks || [], prints: STATE.prints||[] },
    //   NOTABLES_DEFAULT
    // );    
    // const immediate = scheduleNotables()
    // const s = JSON.stringify(immediate);
    // if (s !== lastNotablesJson) {
    //   wsBroadcast("notables", immediate);
    //   lastNotablesJson = s;
    // }    
    scheduleNotables();
  } catch (_) {}

  // 🔒 NEW: apply publish gate to sweeps/blocks globally
  if (msg?.type === "sweeps" || msg?.type === "blocks") {
    if (!passesPublishGate(msg)) return;   // drop quietly
  }

  // keep your existing topic handling
  if (msg?.type === "sweeps") {
    STATE.sweeps = [...(STATE.sweeps || []), msg];
    wsBroadcast("sweeps", STATE.sweeps.slice(-500));
    // optional: also headline
    const h = headlineOfTrade(msg); wsBroadcast("headlines", [h]);
    return;
  } else if (msg?.type === "blocks") {
    STATE.blocks = [...(STATE.blocks || []), msg];
    wsBroadcast("blocks", STATE.blocks.slice(-500));
    const h = headlineOfTrade(msg); wsBroadcast("headlines", [h]);
    return;
  } else if (msg?.type === "print") {
    STATE.prints = [...(STATE.prints || []), msg].slice(-2000);
    wsBroadcast("prints", [msg]); // light push for clients
    const h = headlineOfTrade(msg); wsBroadcast("headlines", [h]);
    return;
  }

  // 🔹 NEW: always publish an enriched headline for *any* trade
  const h = headlineOfTrade(msg);
  wsBroadcast("headlines", [h]);
}

// Nice-to-have alias under /api:
// app.get('/api/insights/notables', (req, res) => {
//   try {
//     const cfg = Object.fromEntries(
//       Object.entries(req.query).map(([k, v]) => [k, Number(v)])
//     );
//     const payload = computeNotables(Object.keys(cfg).length ? cfg : NOTABLES_DEFAULT);
//     res.json(payload);
//   } catch (e) {
//     res.status(500).json({ error: e?.message || 'fail' });
//   }
// });
// app.get('/api/insights/notables', (req, res) => {
//   try {
//     const cfg = Object.fromEntries(Object.entries(req.query).map(([k,v]) => [k, Number(v)]));
//     const payload = buildNotables(
//       { sweeps: STATE.sweeps || [], blocks: STATE.blocks || [], prints: STATE.prints||[] },
//       Object.keys(cfg).length ? cfg : NOTABLES_DEFAULT
//     );
//     res.json(payload);
//   } catch (e) { res.status(500).json({ error: e?.message || 'fail' }); }
// });
app.get('/api/insights/notables', (req, res) => {
  try {
    const q = req.query || {};
    const cfg = {
      windowMs:      q.windowMs      ? Number(q.windowMs)      : undefined,
      minNotional:   q.minNotional   ? Number(q.minNotional)   : undefined,
      topN:          q.topN          ? Number(q.topN)          : undefined,
      expiryMaxDays: q.expiryMaxDays ? Number(q.expiryMaxDays) : undefined,
      side:          q.side && typeof q.side === 'string' ? q.side.toUpperCase() : undefined, // "BUY"|"SELL"|"ANY"
    };
    // Drop undefined keys; fall back to defaults
    const clean = Object.fromEntries(Object.entries(cfg).filter(([,v]) => v !== undefined));
    const merged = { ...NOTABLES_DEFAULT, ...clean };

    const payload = buildNotables(
      { sweeps: STATE.sweeps || [], blocks: STATE.blocks || [], prints: STATE.prints || [] },
      merged
    );
    res.json(payload);
  } catch (e) {
    res.status(500).json({ error: e?.message || 'fail' });
  }
});
// app.get("/api/flow/sweeps", (req,res)=>{
//   const minNotional = Number(req.query.minNotional ?? 25_000);
//   const limit = Number(req.query.limit ?? 200);
//   const base = computeSweeps(9999, minNotional).slice(0, limit);
//   const rows = base.map(r => attachDayStats({ ...r }));
//   res.json({ rows, ts: Date.now() });
// });

// ===================== NEW pushAndFanout =====================
// function pushAndFanout(msg) {
//   try {
//     // Enrich (prints, sweeps, blocks, notables) with BTO/BTC/STO/STC, at, oi, priorVol, reason
//     if (msg && (msg.type === "print" || msg.type === "sweeps" || msg.type === "blocks" || msg.type === "notables")) {
//       msg = enrichWithAction(msg);
//     }

//     // Also enrich headlines if they carry an option leg (so chips can show action)
//     if (msg && msg.type === "headlines") {
//       // single headline object or array? Support both
//       const batch = Array.isArray(msg.data) ? msg.data : [msg];
//       for (const h of batch) enrichWithAction(h);
//     }
//   } catch (e) {
//     // swallow — we still want to fanout raw messages if inference fails
//   }

//   // Route by topic
//   if (msg?.type === "sweeps") {
//     STATE.sweeps = [...(STATE.sweeps || []), msg].slice(-500);
//     wsBroadcast("sweeps", STATE.sweeps);
//     return;
//   }

//   if (msg?.type === "blocks") {
//     STATE.blocks = [...(STATE.blocks || []), msg].slice(-500);
//     wsBroadcast("blocks", STATE.blocks);
//     return;
//   }

//   if (msg?.type === "notables") {
//     STATE.notables = [...(STATE.notables || []), msg].slice(-500);
//     wsBroadcast("notables", STATE.notables);
//     return;
//   }

//   if (msg?.type === "print") {
//     STATE.prints = [...(STATE.prints || []), msg].slice(-2000);
//     // small, frequent pushes; clients can aggregate
//     wsBroadcast("prints", [msg]);
//     return;
//   }

//   if (msg?.type === "headlines") {
//     // If you publish headlines as a batch: ensure items already enriched
//     // Attach only the top-N or pass through
//     const arr = Array.isArray(msg.data) ? msg.data : [msg];
//     wsBroadcast("headlines", arr);
//     return;
//   }

//   // Fallback passthrough (if some future type shows up)
//   if (msg?.topic && msg?.data !== undefined) {
//     wsBroadcast(msg.topic, msg.data);
//   }
// }
function toNum(x){ const n = Number(x); return Number.isFinite(n) ? n : null; }

function normalizeQuoteFields(raw){
  // raw = IB snapshot keyed object (strings). Field names may differ; we defensively pull all common ones.
  const last = toNum(raw.last) ?? toNum(raw.lastPrice) ?? toNum(raw.trade) ?? null;
  const close = toNum(raw.close) ?? toNum(raw.prevClose) ?? null;
  const bid = toNum(raw.bid) ?? toNum(raw.bestBid) ?? null;
  const ask = toNum(raw.ask) ?? toNum(raw.bestAsk) ?? null;

  let lastFixed = last ?? (bid && ask ? (bid + ask) / 2 : close ?? null);

  // sanity: sometimes indices/futures come in cents or odd scales. If it looks tiny vs. SPY/QQQ levels, prefer mid.
  if (lastFixed !== null && ask !== null && bid !== null) {
    const mid = (bid + ask) / 2;
    // if last is way off from mid (e.g., badly scaled), trust mid
    if (Math.abs(lastFixed - mid) / Math.max(1, mid) > 0.5) lastFixed = mid;
  }

  const iv = toNum(raw.impliedVol) ?? toNum(raw.iv) ?? null;

  return {
    last: lastFixed !== null ? Number(lastFixed.toFixed(2)) : null,
    bid:  bid  !== null ? Number(bid.toFixed(2))  : null,
    ask:  ask  !== null ? Number(ask.toFixed(2))  : null,
    iv:   iv   !== null ? Math.round(iv * (iv < 5 ? 100 : 1)) : null, // handle 0.12 vs 12%
  };
}
const LAST_BAR_OPT = new Map(); // conid -> { t, c, v }
async function pollOptionsOnce(){
  try{
    // union of underlyings
    const ulSet = new Set(normEquities(WATCH).map(s => s.toUpperCase()));
    for (const o of normOptions(WATCH)) if (o?.underlying) ulSet.add(String(o.underlying).toUpperCase());
    const underlyings = Array.from(ulSet);
    if (!underlyings.length) return;

    const ticks = [];
    for (const ul of underlyings){
      const conid = await ibConidForSymbol(ul); if (!conid) continue;

      const snap = MOCK
        ? [{ "31": 100+Math.random()*50 }]
        : await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${conid}&fields=31`);
      const underPx = Number((Array.isArray(snap) && snap[0] && snap[0]["31"]) ?? NaN);
      if (!Number.isFinite(underPx)) continue;

      // update equity last cache so ATM builder can work immediately
      onTick(ul, underPx);

      let expiries = await ibOptMonthsForSymbol(ul);
      expiries = (expiries || []).slice(0,2);

      const grid = underPx < 50 ? 1 : 5;
      const atm  = Math.round(underPx / grid) * grid;
      const rel  = [-2,-1,0,1,2].map(i => atm + i*grid);

      for (const expiry of expiries){
        const yyyy = expiry.slice(0,4), mm = expiry.slice(5,7), month = `${yyyy}${mm}`;
        for (const right of ["C","P"]){
          for (const strike of rel){
            try {
              let s0;
              let optConid = null;

              if (MOCK) {
                s0 = { "31": Math.max(0.05, Math.random()*20), "84": Math.max(0.01, Math.random()*20-0.1), "86": Math.random()*20+0.1 };
              } else {
                const infoUrl = `${IB_BASE}/iserver/secdef/info?conid=${conid}&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${strike}`;
                const info   = await ibFetchJson(infoUrl);
                const arr    = Array.isArray(info) ? info
                            : Array.isArray(info?.Contracts) ? info.Contracts
                            : (info ? [info] : []);
                optConid = arr.find(x => x?.conid)?.conid;
                if (!optConid) continue;

                // NBBO + day stats snapshot for this OCC (include vol/oi fields)
                // 31=last, 84=bid, 86=ask, 7295=volume, 7635=openInterest (IB field set)
                const osnap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86,7295,7635`);
                s0 = Array.isArray(osnap) && osnap[0] ? osnap[0] : {};
              }

              // Update NBBO cache (so classifier has context)
              const occ = toOcc(ul, expiry, rightToCP(right), strike);
              const bid0 = Number(s0["84"]), ask0 = Number(s0["86"]);
              if (Number.isFinite(bid0) || Number.isFinite(ask0)) {
                NBBO_OPT.set(occ, { bid: bid0 || 0, ask: ask0 || 0, ts: Date.now() });
              }

              // Emit the UI "tick" row you already use
              ticks.push(mapOptionTick(ul, expiry, strike, right, s0));

              // add:
              updateDayStats(ul, expiry, right, strike, {
                volume: toNumStrict(s0["7295"]),
                oi:     toNumStrict(s0["7635"]),
              });
              // Synthesize recent prints from 1-minute bars
              if (!MOCK && optConid) {
                let bars;
                try {
                  bars = await ibFetchJson(`${IB_BASE}/iserver/marketdata/history?conid=${optConid}&period=1d&bar=1min&outsideRth=true`);
                } catch { bars = null; }

                const list = bars?.data || bars?.points || bars?.bars || [];
                if (Array.isArray(list) && list.length) {
                  const lastSeen = LAST_BAR_OPT.get(optConid)?.t || 0;
                  const tail = list.slice(-5);

                  for (const b of tail) {
                    const t = Number(b.t || b.time || Date.parse(b.timestamp || "")) || 0;
                    if (!t || t <= lastSeen) continue;

                    const c = Number(b.c || b.close || b.price || b.last || 0);
                    const v = Number(b.v || b.volume || 0);
                    if (!(v > 0 && Number.isFinite(c))) { LAST_BAR_OPT.set(optConid, { t, c, v }); continue; }

                    // --- Fresh book + day stats for better inference (light snapshot) ---
                    let bid = bid0, ask = ask0, cumVol = toNumStrict(s0["7295"]), oiOpt = toNumStrict(s0["7635"]);
                    try {
                      const lite = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86,7295,7635`);
                      const r = Array.isArray(lite) && lite[0] ? lite[0] : {};
                      // prefer freshest, fallback to previous
                      bid = Number.isFinite(+r["84"]) ? +r["84"] : bid;
                      ask = Number.isFinite(+r["86"]) ? +r["86"] : ask;
                      cumVol = Number.isFinite(+r["7295"]) ? +r["7295"] : cumVol;
                      oiOpt = Number.isFinite(+r["7635"]) ? +r["7635"] : oiOpt;
                    } catch { /* non-fatal: keep s0 values */ }

                    // Fallback to cache if snapshot missing
                    const nbbo = NBBO_OPT.get(occ);
                    if (!Number.isFinite(bid)) bid = Number.isFinite(+nbbo?.bid) ? +nbbo.bid : undefined;
                    if (!Number.isFinite(ask)) ask = Number.isFinite(+nbbo?.ask) ? +nbbo.ask : undefined;
                    const mid = (Number.isFinite(bid) && Number.isFinite(ask) && bid>0 && ask>0) ? (bid+ask)/2 : undefined;

                    // Normalize → message
                    let msg = normOptionsPrint({
                      ul, exp: expiry, right, strike, price: c, qty: v, nbbo: { bid, ask }
                    });
                    // Ensure type + leg present (some normalizers differ)
                    msg.type = "print";
                    msg.ul = msg.ul || ul;
                    msg.right = msg.right || right;
                    msg.strike = msg.strike ?? strike;
                    msg.expiry = msg.expiry || expiry;

                    // Infer BTO/BTC/STO/STC (this attaches at/priorVol/oi/action/action_conf/reason)
                    try {
                      const inf = inferActionForOptionTrade({
                        occ,
                        qty: v,
                        price: c,
                        side: msg.side || "UNKNOWN",        // OK to be UNKNOWN; classifier uses 'at' too
                        book: { bid, ask, mid },
                        cumulativeVol: Number.isFinite(cumVol) ? cumVol : undefined, // day cumulative
                        oi: Number.isFinite(oiOpt) ? oiOpt : undefined,
                        asOfYMD: etYMD()
                      });
                      msg = { ...msg, ...inf };
                    } catch { /* swallow and keep raw msg */ }

                    pushAndFanout(msg);
                    maybePublishSweepOrBlock(msg); // your existing promotion logic

                    LAST_BAR_OPT.set(optConid, { t, c, v });
                  }
                }
              }
            } catch { /* ignore one leg error to keep loop healthy */ }
          }
        }
      }
    }
    STATE.options_ts = ticks;
    wsBroadcast("options_ts", ticks);
  }catch(e){ console.warn("pollOptionsOnce:", e?.message || e); }
}

// async function pollOptionsOnce(){
//   try{
//     // union of underlyings
//     const ulSet = new Set(normEquities(WATCH).map(s => s.toUpperCase()));
//     for (const o of normOptions(WATCH)) if (o?.underlying) ulSet.add(String(o.underlying).toUpperCase());
//     const underlyings = Array.from(ulSet);
//     if (!underlyings.length) return;

//     const ticks = [];
//     for (const ul of underlyings){
//       const conid = await ibConidForSymbol(ul); if (!conid) continue;

//       const snap = MOCK
//         ? [{ "31": 100+Math.random()*50 }]
//         : await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${conid}&fields=31`);
//       const underPx = Number((Array.isArray(snap) && snap[0] && snap[0]["31"]) ?? NaN);
//       if (!Number.isFinite(underPx)) continue;

//       // update equity last cache so ATM builder can work immediately
//       onTick(ul, underPx);
//       let expiries = await ibOptMonthsForSymbol(ul);
//       expiries = (expiries || []).slice(0,2);

//       const grid = underPx < 50 ? 1 : 5;
//       const atm  = Math.round(underPx / grid) * grid;
//       const rel  = [-2,-1,0,1,2].map(i => atm + i*grid);

//       for (const expiry of expiries){
//         const yyyy = expiry.slice(0,4), mm = expiry.slice(5,7), month = `${yyyy}${mm}`;
//         for (const right of ["C","P"]){
//           for (const strike of rel){
//             try {
//               let s0;
//               let optConid = null;

//               if (MOCK) {
//                 s0 = { "31": Math.max(0.05, Math.random()*20), "84": Math.max(0.01, Math.random()*20-0.1), "86": Math.random()*20+0.1 };
//               } else {
//                 const infoUrl = `${IB_BASE}/iserver/secdef/info?conid=${conid}&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${strike}`;
//                 const info   = await ibFetchJson(infoUrl);
//                 const arr    = Array.isArray(info) ? info
//                             : Array.isArray(info?.Contracts) ? info.Contracts
//                             : (info ? [info] : []);
//                 optConid = arr.find(x => x?.conid)?.conid;
//                 if (!optConid) continue;

//                 // 1) NBBO snapshot for this OCC
//                 const osnap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86,7295,7635`);
//                 s0 = Array.isArray(osnap) && osnap[0] ? osnap[0] : {};
//               }

//               // Update NBBO cache (so classifyAggressor / intent has context)
//               const occ = toOcc(ul, expiry, right, strike);
//               const bid = Number(s0["84"]), ask = Number(s0["86"]);
//               if (Number.isFinite(bid) || Number.isFinite(ask)) {
//                 NBBO_OPT.set(occ, { bid: bid || 0, ask: ask || 0, ts: Date.now() });
//               }

//               // 2) Emit the UI "tick" row you already use (keeps your “Options (read-only demo)” table healthy)
//               ticks.push(mapOptionTick(ul, expiry, strike, right, s0));

//               // 3) Turn 1-minute bars into prints (qty = volume, price = close)
//               if (!MOCK && optConid) {
//                 let bars;
//                 try {
//                   // history endpoint: synthesize prints from the last few minutes only
//                   bars = await ibFetchJson(`${IB_BASE}/iserver/marketdata/history?conid=${optConid}&period=1d&bar=1min&outsideRth=true`);
//                 } catch (_) {
//                   // If history is temporarily unavailable (e.g., IB 503), skip quietly
//                   bars = null;
//                 }

//                 const list = bars?.data || bars?.points || bars?.bars || [];
//                 if (Array.isArray(list) && list.length) {
//                   const lastSeen = LAST_BAR_OPT.get(optConid)?.t || 0;
//                   // Only the tail to avoid flooding + ensure monotonicity
//                   const tail = list.slice(-5);
//                   for (const b of tail) {
//                     const t = Number(b.t || b.time || Date.parse(b.timestamp || "")) || 0;
//                     if (!t || t <= lastSeen) continue;

//                     const c = Number(b.c || b.close || b.price || b.last || 0);
//                     const v = Number(b.v || b.volume || 0);

//                     if (v > 0 && Number.isFinite(c)) {
//                       const nbbo = NBBO_OPT.get(occ);
//                       // === This is the bit you asked to “show integration” for ===
//                       // const msg = normOptionsPrint({
//                       //   ul, exp: expiry, right, strike, price: c, qty: v, nbbo
//                       // });
//                       const bidN = Number(nbbo?.bid);
//                       const askN = Number(nbbo?.ask);
//                       const midN = (Number.isFinite(bidN) && Number.isFinite(askN) && bidN > 0 && askN > 0) ? (bidN + askN) / 2 : undefined;

//                       // IB sometimes exposes cumulative volume / OI on the option snapshot; pull if present
//                       const cumVol = toNumStrict(s0["7295"] ?? s0["volume"] ?? s0["tradingVolume"]);
//                       const oiOpt  = toNumStrict(s0["7635"] ?? s0["openInterest"] ?? s0["open_interest"]);

//                       let msg = normOptionsPrint({
//                         ul, exp: expiry, right, strike, price: c, qty: v, nbbo
//                       });

//                       // Infer BTO/BTC/STO/STC and attach to the print
//                       try {
//                         const inf = inferActionForOptionTrade({
//                           occ,
//                           qty: v,
//                           price: c,
//                           side: msg.side || "UNKNOWN",
//                           book: { bid: bidN, ask: askN, mid: midN },
//                           cumulativeVol: cumVol,
//                           oi: oiOpt,
//                           asOfYMD: etYMD()
//                         });
//                         msg = { ...msg, ...inf }; // add: at, priorVol, oi, action, action_conf, reason
//                       } catch (_) { /* non-fatal */ }

//                       pushAndFanout(msg);
//                       // promote large prints to sweeps/blocks (thresholded)
//                       maybePublishSweepOrBlock(msg);
//                     }
//                     LAST_BAR_OPT.set(optConid, { t, c, v });
//                   }
//                 }
//               }
//             } catch (_) { /* ignore one leg error to keep loop healthy */ }
//           }
//         }
//       }
//     }
//     STATE.options_ts = ticks;
//     wsBroadcast("options_ts", ticks);
//   }catch(e){ console.warn("pollOptionsOnce:", e.message); }
// }

/* ==== Tape classification config (tune freely) ==== */
const TAPE_CFG = {
  sweep: {
    windowMs: 1500,       // cluster prints within this window as one sweep
    maxGapMs: 400,        // max gap between consecutive prints in the cluster
    minPrints: 2,         // must have at least N prints to be a sweep
    minNotional: 75000,   // min $ notional for the aggregated sweep
    atAskBps: 15,         // trade >= mid + X bps → BUY; <= mid - X bps → SELL
  },
  block: {
    lookbackMs: 60_000,   // consider last 60s of trades
    minContracts: 250,    // large single print (contracts)
    minNotional: 250_000, // or large notional
    nearMidBps: 10,       // |trade-mid| <= X bps → likely negotiated block
  },
  filtersDefault: {
    timeframeMs: 60_000,      // publish last 60s by default
    minNotional: 50_000,      // UI only sees notional >= this
    side: "ANY",              // BUY | SELL | ANY
    expiryMaxDays: 180,       // ignore very far-dated by default
  }
};

// type OptPrint = {
//   underlying: string;
//   right: "C" | "P";
//   strike: number;
//   expiration: string;          // "YYYY-MM-DD"
//   last: number; bid?: number; ask?: number;
//   size?: number;               // contracts; synthesize if missing
//   venue?: string;              // "ibkr","tradier","cboe", etc.; synth if missing
//   ts: number;                  // ms epoch
// };
/* ================= WATCHLIST ROUTES ================= */
// app.get("/watchlist/equities", (_req, res) => {
//   res.json({ equities: STATE.watchlist.equities || [] });
// });

// app.post("/watchlist/equities", express.json(), async (req, res) => {
//   const raw = (req.body?.symbol || "").toString().trim();
//   if (!raw) return res.status(400).json({ error: "symbol required" });
//   await watchAddEquities([normalizeSymbol(raw)]);
//   wsBroadcast("watchlist", getWatchlist());
//   res.json({ ok: true, watchlist: getWatchlist() });
// });
app.post("/watchlist/equities", (req, res) => {
  const sym = String(req.body?.symbol || "").toUpperCase().replace(/^\//, "");
  if (!/^[A-Z0-9.\-_/]{1,10}$/.test(sym)) {
    return res.status(400).json({ error: "bad symbol" });
  }
  const wf = loadWatchlistFile();
  if (!wf.equities.includes(sym)) wf.equities.push(sym);
  saveWatchlistFile(wf);

  const payload = makeWatchlistPayload();
  safeBroadcast({ topic: "watchlist", data: payload });
  res.json({ ok: true, ...payload });
});


// app.post("/watchlist/equities", express.json(), (req, res) => {
//   const raw = (req.body?.symbol || "").toString().trim();
//   if (!raw) return res.status(400).json({ error: "symbol required" });
//   const sym = raw.toUpperCase();

//   const set = new Set(STATE.watchlist.equities || []);
//   set.add(sym);
//   STATE.watchlist.equities = Array.from(set);

//   res.json({ ok: true, equities: STATE.watchlist.equities });
// });
// app.post("/watchlist/equities", express.json(), async (req, res) => {
//   const raw = (req.body?.symbol || "").toString().trim();
//   if (!raw) return res.status(400).json({ error: "symbol required" });
//   await watchAddEquities([raw.toUpperCase()]);
//   wsBroadcast("watchlist", getWatchlist());
//   res.json({ ok: true, watchlist: getWatchlist() });
// });

// app.delete("/watchlist/equities/:symbol", (req, res) => {
//   const sym = (req.params.symbol || "").toUpperCase();
//   const next = (STATE.watchlist.equities || []).filter(s => s !== sym);
//   STATE.watchlist.equities = next;
//   res.json({ ok: true, equities: next });
// });
// app.delete("/watchlist/equities/:symbol", async (req, res) => {
//   const sym = (req.params.symbol || "").toUpperCase();
//   WATCH.equities.delete(sym);
//   wsBroadcast("watchlist", getWatchlist());
//   res.json({ ok: true, watchlist: getWatchlist() });
// });
// app.delete("/watchlist/equities/:symbol", async (req, res) => {
//   const sym = normalizeSymbol(req.params.symbol || "");
//   WATCH.equities.delete(sym);
//   wsBroadcast("watchlist", getWatchlist());
//   res.json({ ok: true, watchlist: getWatchlist() });
// });

// Remove equity (persists to file)
app.delete("/watchlist/equities/:sym", (req, res) => {
  const sym = String(req.params.sym || "").toUpperCase().replace(/^\//, "");
  const wf = loadWatchlistFile();
  wf.equities = wf.equities.filter((s) => s !== sym);
  saveWatchlistFile(wf);

  const payload = makeWatchlistPayload();
  safeBroadcast({ topic: "watchlist", data: payload });
  res.json({ ok: true, ...payload });
});

/* ---------- Tick updates: update lastBySymbol then (optionally) rebroadcast ---------- */
function onTick(symbol, last) {
  lastBySymbol[symbol.toUpperCase().replace(/^\//, "")] = Number(last);
  // Optional (can be throttled): recompute to move the ATM band if price crossed a strike
  // safeBroadcast({ topic: "watchlist", data: makeWatchlistPayload() });
}

/* ---------- WS broadcast wrapper (no-op if you don’t have it here) ---------- */
function safeBroadcast(msg) {
  try {
    if (global.broadcast) global.broadcast(msg);
    // else import your broadcast and call it
  } catch {}
}

/* Optional: read-only demo options endpoint */
// app.get("/watchlist/options", (_req, res) => {
//   res.json({ options: STATE.watchlist.options || [] });
// });
function makeWatchlistPayload() {
  const wf = loadWatchlistFile();
  const options = wf.equities.flatMap((ul) => buildOptionsAroundATM(ul, getLast));
  return { equities: wf.equities, options };
}

/* ---------- REST: single source of truth + generated options ---------- */
app.get("/watchlist", (_req, res) => {
  res.json(makeWatchlistPayload());
});


// function normalizePrint(p) {
//   return {
//     ...p,
//     size: Math.max(1, Math.floor(p.size || (1 + Math.random()*30))), // 1..30
//     venue: p.venue || (Math.random() < 0.5 ? "cboe" : "nyse-arcx"),
//     ts: p.ts || Date.now()
//   };
// }
const MAX_BUFFER_MS = 5 * 60_000; // keep 5 min
function pruneBuffer() {
  const cutoff = Date.now() - MAX_BUFFER_MS;
  STATE.options_ts = (STATE.options_ts || []).filter(p => p.ts >= cutoff);
}
function relToMidBps(last, bid, ask) {
  const mid = (Number(bid) + Number(ask)) / 2 || Number(last);
  if (!mid) return 0;
  return ((last - mid) / mid) * 10_000; // basis points
}
function inferSide(last, bid, ask, atAskBps) {
  const bps = relToMidBps(last, bid, ask);
  if (bps >= atAskBps) return "BUY";
  if (bps <= -atAskBps) return "SELL";
  return "UNKNOWN";
}
function notional(contracts, price){ return contracts * 100 * price; }
function daysToExpiry(exp) {
  const d = new Date(exp + "T00:00:00Z");
  return Math.max(0, (d - Date.now()) / 86_400_000);
}
function buildSweepsFromTrades(trades, cfg = TAPE_CFG) {
  const out = [];
  const byContract = new Map();

  for (const t of trades) {
    const key = `${t.underlying}|${t.right}|${t.strike}|${t.expiration}`;
    if (!byContract.has(key)) byContract.set(key, []);
    byContract.get(key).push(t);
  }

  for (const [key, arr] of byContract.entries()) {
    arr.sort((a,b)=>a.ts-b.ts);
    let cluster = [arr[0]];
    for (let i=1;i<arr.length;i++){
      const prev = arr[i-1], cur = arr[i];
      const withinWindow = cur.ts - cluster[0].ts <= cfg.sweep.windowMs;
      const smallGap = cur.ts - prev.ts <= cfg.sweep.maxGapMs;

      if (withinWindow && smallGap) {
        cluster.push(cur);
      } else {
        maybePushSweep(cluster, cfg, out);
        cluster = [cur];
      }
    }
    maybePushSweep(cluster, cfg, out);
  }
  return out;
}

function maybePushSweep(cluster, cfg, out) {
  if (!cluster || cluster.length < cfg.sweep.minPrints) return;

  // aggregate
  const first = cluster[0];
  const ul = first.underlying;
  const right = first.right === "C" ? "CALL" : "PUT";
  const strike = first.strike;
  const expiry = first.expiration;

  const qty = cluster.reduce((s,t)=>s + (t.size||1), 0);
  const vwap = cluster.reduce((s,t)=>s + (t.last * (t.size||1)), 0) / qty;
  const totalNotional = notional(qty, vwap);
  if (totalNotional < cfg.sweep.minNotional) return;

  // infer dominant side by median bps
  const bpsList = cluster.map(t => relToMidBps(t.last, t.bid, t.ask));
  bpsList.sort((a,b)=>a-b);
  const medianBps = bpsList[Math.floor(bpsList.length/2)];
  const side = medianBps >= cfg.sweep.atAskBps ? "BUY"
             : medianBps <= -cfg.sweep.atAskBps ? "SELL"
             : "UNKNOWN";

  out.push({
    id: `${ul}:${expiry}:${right}:${strike}:${cluster[0].ts}`,
    kind: "SWEEP",
    ul, right, strike, expiry,
    side, prints: cluster.length,
    qty, price: Number(vwap.toFixed(2)),
    notional: Number(totalNotional.toFixed(2)),
    venues: Array.from(new Set(cluster.map(t=>t.venue||"n/a"))),
    ts: cluster[cluster.length-1].ts
  });
}
function buildBlocksFromTrades(trades, cfg = TAPE_CFG) {
  const now = Date.now();
  const recent = trades.filter(t => now - t.ts <= cfg.block.lookbackMs);

  const out = [];
  for (const t of recent) {
    const contracts = t.size || 1;
    const price = t.last || 0;
    const n = notional(contracts, price);
    if (contracts < cfg.block.minContracts && n < cfg.block.minNotional) continue;

    const bps = Math.abs(relToMidBps(price, t.bid, t.ask));
    if (bps > cfg.block.nearMidBps) continue; // not near-mid → likely not a negotiated block

    const side = inferSide(price, t.bid, t.ask, cfg.sweep.atAskBps);
    out.push({
      id: `${t.underlying}:${t.expiration}:${t.right}:${t.strike}:${t.ts}:BLOCK`,
      kind: "BLOCK",
      ul: t.underlying,
      right: t.right === "C" ? "CALL" : "PUT",
      strike: t.strike,
      expiry: t.expiration,
      side,
      qty: contracts,
      price: Number(price.toFixed(2)),
      notional: Number(n.toFixed(2)),
      prints: 1,
      venue: t.venue || "n/a",
      ts: t.ts
    });
  }
  return out;
}
let TAPE_FILTERS = { ...TAPE_CFG.filtersDefault };

function filterTape(items, f=TAPE_FILTERS) {
  const now = Date.now();
  const maxAge = f.timeframeMs || 60_000;
  return items.filter(it => {
    if (now - it.ts > maxAge) return false;
    if (it.notional < (f.minNotional ?? 0)) return false;
    if (f.side && f.side !== "ANY" && it.side !== f.side) return false;
    if (f.expiryMaxDays != null && daysToExpiry(it.expiry) > f.expiryMaxDays) return false;
    return true;
  });
}

// Optional: REST to tweak filters from UI
app.get("/tape/filters", (_req,res)=>res.json(TAPE_FILTERS));
app.post("/tape/filters", (req,res)=>{
  TAPE_FILTERS = { ...TAPE_FILTERS, ...(req.body||{}) };
  res.json({ ok:true, filters: TAPE_FILTERS });
});
// async function pollSweepsOnce(){ 
//   try {
//     pruneBuffer();
//     const trades = (STATE.options_ts||[]).map(normalizePrint);
//     const sweeps = buildSweepsFromTrades(trades, TAPE_CFG);
//     const next = filterTape(sweeps);
//     const changed = JSON.stringify(next)!==JSON.stringify(STATE.sweeps||[]);
//     STATE.sweeps = next;
//     if (changed) wsBroadcast("sweeps", next);
//   } catch(e){ console.warn("pollSweepsOnce:", e.message); }
// }

// async function pollBlocksOnce(){ 
//   try {
//     pruneBuffer();
//     const trades = (STATE.options_ts||[]).map(normalizePrint);
//     const blocks = buildBlocksFromTrades(trades, TAPE_CFG);
//     const next = filterTape(blocks);
//     const changed = JSON.stringify(next)!==JSON.stringify(STATE.blocks||[]);
//     STATE.blocks = next;
//     if (changed) wsBroadcast("blocks", next);
//   } catch(e){ console.warn("pollBlocksOnce:", e.message); }
// }

// /* === (Optional) demo sweep/block fillers so your UI lists show content === */
// function demoTapeFromOptions(optionsTicks, kind){
//   // produce large-looking prints like your screenshots
//   const out=[];
//   for (const t of optionsTicks.slice(0, 12)){
//     const notional = (Math.abs(t.last||0) * 100 * 10) || (Math.random()*250000+80000);
//     out.push({
//       kind: kind.toUpperCase(),                 // "SWEEP" | "BLOCK"
//       ul: t.underlying,
//       right: t.right==="C" ? "CALL" : "PUT",
//       strike: t.strike,
//       expiry: t.expiration,
//       side: "BUY",
//       qty: 10,
//       price: Math.max(0.01, Number((t.last||1).toFixed(2))),
//       notional: Number(notional.toFixed(2)),
//       prints: Math.floor(1+Math.random()*3),
//       venue: "tradier",
//       ts: Date.now()
//     });
//   }
//   return out;
// }
app.get("/tape", (_req,res)=>{
  res.json({ sweeps: STATE.sweeps||[], blocks: STATE.blocks||[] });
});

// async function pollSweepsOnce(){ try {
//   const next = demoTapeFromOptions(STATE.options_ts||[], "SWEEP");
//   const changed = JSON.stringify(next)!==JSON.stringify(STATE.sweeps||[]);
//   STATE.sweeps = next; if (changed) wsBroadcast("sweeps", next);
// } catch(e){ console.warn("pollSweepsOnce:", e.message); } }
// async function pollBlocksOnce(){ try {
//   const next = demoTapeFromOptions(STATE.options_ts||[], "BLOCK");
//   const changed = JSON.stringify(next)!==JSON.stringify(STATE.blocks||[]);
//   STATE.blocks = next; if (changed) wsBroadcast("blocks", next);
// } catch(e){ console.warn("pollBlocksOnce:", e.message); } }

/* ================= WS bootstrap ================= */
wss.on("connection", (ws) => {
  wsSend(ws, "equity_ts", STATE.equities_ts || []);
  wsSend(ws, "options_ts", STATE.options_ts || []);
  wsSend(ws, "sweeps",    STATE.sweeps    || []);
  wsSend(ws, "blocks",    STATE.blocks    || []);
  // wsSend(ws, "watchlist", getWatchlist()); // <-- shared
  wsSend(ws, "watchlist", makeWatchlistPayload());
  wsSend(ws, "headlines", computeHeadlines());   // <— add this

  // NEW: seed "notables" immediately so the widget doesn’t wait 2s tick
  const bootNotables = computeNotables(NOTABLES_DEFAULT);
  wsSend(ws, "notables", bootNotables);  
});
(async () => {
  try { await seedWatchlist(); } catch (e) { console.warn("seedWatchlist:", e.message); }
})();
// small helper so both REST + WS use the same builder
function computeNotables(cfg = NOTABLES_DEFAULT) {
  return buildNotables(
    { sweeps: STATE.sweeps || [], blocks: STATE.blocks || [], prints: STATE.prints||[]  },
    cfg
  );
}

const STOP = { value: false };
process.on("SIGTERM", () => (STOP.value = true));
process.on("SIGINT",  () => (STOP.value = true));

// const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jitter = (ms, pct = 0.10) => {
  const j = ms * pct;
  return ms + (Math.random() * 2 * j - j);
};

// Runs fn → waits (with jitter/backoff) → runs again; no overlap/drift.
function runLoop(name, fn, { intervalMs, jitterPct = 0.10, maxBackoffMs = 30_000 } = {}) {
  let running = false;
  let backoffMs = 0;

  (async function loop() {
    while (!STOP.value) {
      if (!running) {
        running = true;
        try {
          await fn();
          backoffMs = 0; // reset on success
        } catch (err) {
          console.error(`[${name}]`, err?.message || err);
          backoffMs = Math.min((backoffMs || 1_000) * 2, maxBackoffMs);
        } finally {
          running = false;
        }
      }
      const base = backoffMs || intervalMs;
      await sleep(jitter(base, jitterPct));
    }
  })();
}

// Stagger the initial kicks slightly to avoid synchronized bursts.
setTimeout(() => runLoop("equities", pollEquitiesOnce, { intervalMs: 10_000 }),  0);
setTimeout(() => runLoop("options",  pollOptionsOnce,  { intervalMs: 13_000 }), 250);
setTimeout(() => runLoop("sweeps",   pollSweepsOnce,   { intervalMs: 5_000  }), 500);
setTimeout(() => runLoop("blocks",   pollBlocksOnce,   { intervalMs: 5_000  }), 750);
setTimeout(() => runLoop("prune",    pruneTapeAges,    { intervalMs: 2_000  }), 1000);

// Notables: compute → broadcast only on JSON diff (already debounced by compare)
// let lastNotablesJson = "";
runLoop("notables", async () => {
  const notables = computeNotables(NOTABLES_DEFAULT);
  const s = JSON.stringify(notables);
  if (s !== lastNotablesJson) {
    wsBroadcast("notables", notables);
    lastNotablesJson = s;
  }
}, { intervalMs: 2_000 });

// /* ================= SCHEDULERS ================= */
// setInterval(pollEquitiesOnce, 10_000);
// setInterval(pollOptionsOnce,  13_000);
// setInterval(pollSweepsOnce,    5_000);
// setInterval(pollBlocksOnce,    5_000);
// setInterval(pruneTapeAges, 2_000);
// setInterval(() => {
//   const notables = computeNotables(NOTABLES_DEFAULT);
//   const s = JSON.stringify(notables);
//   if (s !== lastNotablesJson) {
//     wsBroadcast("notables", notables);
//     lastNotablesJson = s;
//   }
// }, 2000); // lightweight, debounced by JSON compare

function computeHeadlines({ windowMs = ROLLING_MS, minNotional = 50_000, topN = 12 } = {}) {
  const now = Date.now();
  const inWindow = (t) => now - (t?.ts || 0) <= windowMs;

  const items = [];

  // 1) Sweeps
  for (const s of (STATE.sweeps || [])) {
    if (!inWindow(s)) continue;
    if ((s.notional || 0) < minNotional) continue;
    items.push({ ...s, type: "SWEEP" });
  }

  // 2) Blocks
  for (const b of (STATE.blocks || [])) {
    if (!inWindow(b)) continue;
    if ((b.notional || 0) < minNotional) continue;
    items.push({ ...b, type: "BLOCK" });
  }

  // 3) Prints (raw → synthesize notional = qty * price * 100)
  for (const p of (STATE.prints || [])) {
    if (!inWindow(p)) continue;
    const n = Math.round((p.qty || 0) * (p.price || 0) * 100);
    if (n < minNotional) continue;
    items.push({
      type: "PRINT",
      ul: p.symbol,
      right: p.option?.right,
      strike: p.option?.strike,
      expiry: p.option?.expiration,
      side: p.side || "UNKNOWN",
      price: Number(p.price || 0),
      qty: Number(p.qty || 0),
      notional: n,
      ts: p.ts,
    });
  }

  items.sort((a, b) => (b.notional || 0) - (a.notional || 0));
  return items.slice(0, topN);
}
// WS broadcast every 2s
setInterval(() => {
  const headlines = computeHeadlines({ windowMs: ROLLING_MS, minNotional: 50_000, topN: 12 });
  wsBroadcast("headlines", headlines);
}, 2_000);

setInterval(() => {
  const payload = buildNotables({ sweeps: STATE.sweeps||[], blocks: STATE.blocks||[], prints: STATE.prints||[] }, NOTABLES_DEFAULT)//buildNotables({ sweeps: STATE.sweeps||[], blocks: STATE.blocks||[] }, NOTABLES_DEFAULT);
  
  const s = JSON.stringify(payload);
  if (s !== lastNotablesJson) { wsBroadcast("notables", payload); lastNotablesJson = s; }
}, 1500);

// REST endpoint (optional)
app.get("/api/flow/headlines", (_req, res) => {
  const top = computeHeadlines({ windowMs: ROLLING_MS, minNotional: 50_000, topN: 20 });
  res.json(top);
});

/* ================= ROUTES ================= */
app.get("/", (req,res)=>res.type("text/plain").send("TradeFlash server up\n"));

app.post("/watch/alpaca", async (req, res) => {
  const equities = Array.isArray(req.body?.equities) ? req.body.equities : [];
  const added = await watchAddEquities(equities);
  res.json({ ok:true, watching:getWatchlist(), added });
});
app.post("/watch/tradier", async (req, res) => {
  const options = Array.isArray(req.body?.options) ? req.body.options : [];
  const added = await watchAddOptions(options);
  res.json({ ok:true, watching:getWatchlist(), added });
});
// app.get("/watchlist", (req,res)=>res.json(getWatchlist()));

app.get("/api/flow/chains", async (req,res)=>{
  try{
    const param = String(req.query.symbols||"").trim();
    const syms = param ? uniq(param.split(",").map(s=>s.trim().toUpperCase()).filter(Boolean))
                       : normEquities(WATCH);
    if (!syms.length) return res.json([]);
    const out=[];
    for (const sym of syms){
      const conid = await ibConidForSymbol(sym);
      if (!conid) continue;
      const expirations = await ibOptMonthsForSymbol(sym);
      out.push({ symbol:sym, conid, expirations });
    }
    res.json(out);
  }catch(e){ console.warn("chains error:", e.message); res.status(500).json({ error:"chains failed" }); }
});
app.get("/api/flow/equity_ts",  (req,res)=>res.json(STATE.equities_ts ?? []));
app.get("/api/flow/options_ts", (req,res)=>res.json(STATE.options_ts  ?? []));
// app.get("/api/flow/sweeps",     (req,res)=>res.json(STATE.sweeps      ?? []));
// app.get("/api/flow/blocks",     (req,res)=>res.json(STATE.blocks      ?? []));
 // GET /api/insights/oi-vs-vol?ul=NVDA&limit=50
app.get("/api/insights/oi-vs-vol", (req, res) => {
  const ul = String(req.query.ul || "").toUpperCase();
  const limit = Math.max(1, Math.min(200, Number(req.query.limit || 50)));
  const rows = Array.isArray(STATE.options_ts) ? STATE.options_ts : [];
  const filtered = rows.filter(r =>
    (!ul || String(r.underlying).toUpperCase() === ul) &&
    Number.isFinite(r?.volume) && Number.isFinite(r?.oi) && r.oi >= 0
  );

  const scored = filtered.map(r => {
    const ratio = (r.oi > 0) ? (r.volume / r.oi) : (r.volume > 0 ? Infinity : 0);
    return {
      underlying: r.underlying,
      expiration: r.expiration,
      right: r.right,
      strike: r.strike,
      last: r.last,
      bid: r.bid, ask: r.ask,
      volume: r.volume, oi: r.oi,
      ratio,
     ts: r.ts
   };
 }).sort((a,b) => (b.ratio - a.ratio) || (b.volume - a.volume));

 res.json({ rows: scored.slice(0, limit), ts: Date.now() });
});

app.get("/debug/expirations", async (req,res)=>{
  try {
    const symbol = String(req.query.symbol||"").toUpperCase();
    if (!symbol) return res.status(400).json({ error:"symbol required" });
    const conid = await ibConidForSymbol(symbol);
    if (!conid) return res.status(500).json({ error:`No conid for ${symbol}` });
    const expirations = await ibOptMonthsForSymbol(symbol);
    res.json({ symbol, conid, expirations });
  } catch (e) { res.status(500).json({ error: e.message||"expirations failed" }); }
});
app.get("/health", (req,res)=>{
  res.json({ ok:true, primed:CACHE.sessionPrimed, watch:getWatchlist(),
    eq_len: STATE.equities_ts?.length||0, opt_len: STATE.options_ts?.length||0 });
});

async function seedDefaultsOnce() {
  try {
//     if (fs.existsSync(SEED_MARK)) return;
//     const curEq = new Set(normEquities(WATCH));
//     const toAddEq = DEFAULT_EQUITIES.filter(s => !curEq.has(s));
//     if (toAddEq.length) await watchAddEquities(toAddEq);

//     const curOptsKey = new Set(normOptions(WATCH).map(o => `${o.underlying}:${o.expiration}:${o.strike}:${o.right}`));
//     const toAddOpts = DEFAULT_OPTIONS.filter(o => {
//       const k = `${String(o.underlying).toUpperCase()}:${o.expiration}:${o.strike}:${String(o.right).toUpperCase()[0]}`;
//       return !curOptsKey.has(k);
//     });
//     if (toAddOpts.length) await watchAddOptions(toAddOpts);

//     // kick first polls so UI has data immediately
//     await pollEquitiesOnce();
//     await pollOptionsOnce();
//     wsBroadcast("watchlist", getWatchlist());
//     if (STATE.equities_ts?.length) wsBroadcast("equity_ts", STATE.equities_ts);
//     if (STATE.options_ts?.length)  wsBroadcast("options_ts", STATE.options_ts);

//     fs.writeFileSync(SEED_MARK, new Date().toISOString());
//     console.log(`[seed] equities=${JSON.stringify(toAddEq)} options=${toAddOpts.length}`);
//   } catch (e) { console.warn("seedDefaultsOnce:", e.message); }
// }
    // Only seed once when either DEMO is on, or explicit SEED_ON_EMPTY is set AND the watchlist is empty.
    const isEmpty = (WATCH.equities?.size || 0) === 0 && (WATCH.options?.length || 0) === 0;
    if (fs.existsSync(SEED_MARK)) return;
    if (!(DEMO || (SEED_ON_EMPTY && isEmpty))) return;

    const curEq = new Set(normEquities(WATCH));
    const envList = (process.env.DEFAULT_EQUITIES || "").trim();
    const desiredEq = (envList ? envList.split(",") : POPULAR_50)
      .map(s => s.trim().toUpperCase())
      .filter(Boolean);
    const toAddEq = desiredEq.filter(s => !curEq.has(s));
    if (toAddEq.length) await watchAddEquities(toAddEq);

    // Only add default options in DEMO
    let toAddOpts = [];
    if (DEMO) {
      const curOptsKey = new Set(
        normOptions(WATCH).map(o => `${o.underlying}:${o.expiration}:${o.strike}:${o.right}`)
      );
      toAddOpts = DEFAULT_OPTIONS.filter(o => {
        const k = `${String(o.underlying).toUpperCase()}:${o.expiration}:${o.strike}:${String(o.right).toUpperCase()[0]}`;
        return !curOptsKey.has(k);
      });
      if (toAddOpts.length) await watchAddOptions(toAddOpts);
    }

    // Kick first polls ONLY if we actually added something
    if (toAddEq.length) await pollEquitiesOnce();
    if (toAddOpts.length) await pollOptionsOnce();

    // wsBroadcast("watchlist", getWatchlist());
    wsBroadcast("watchlist", makeWatchlistPayload());
    if (Array.isArray(STATE.equities_ts) && STATE.equities_ts.length) wsBroadcast("equity_ts", STATE.equities_ts);
    if (Array.isArray(STATE.options_ts)  && STATE.options_ts.length)  wsBroadcast("options_ts",  STATE.options_ts);

    fs.writeFileSync(SEED_MARK, new Date().toISOString());
    console.log(`[seed] mode=${DEMO ? "DEMO" : "SEED_ON_EMPTY"} equities=${toAddEq.length} options=${toAddOpts.length}`);
   } catch (e) { console.warn("seedDefaultsOnce:", e.message); }
 }
 
server.listen(PORT, async () => {
  console.log(`Listening on http://localhost:${PORT}`);
  await seedDefaultsOnce();
});

/* ============ QUICK CURLS =============
# seed watch
curl -s -X POST localhost:8080/watch/alpaca -H 'Content-Type: application/json' \
  -d '{"equities":["AAPL","NVDA","MSFT"]}' | jq .
curl -s -X POST localhost:8080/watch/tradier -H 'Content-Type: application/json' \
  -d '{"options":[{"underlying":"AAPL","expiration":"2025-12-19","strike":200,"right":"CALL"}]}' | jq .

# verify feeds
curl -s localhost:8080/watchlist | jq .
curl -s "http://localhost:8080/api/flow/chains?symbols=AAPL,NVDA,MSFT" | jq .
curl -s localhost:8080/api/flow/equity_ts | jq .
curl -s localhost:8080/api/flow/options_ts | jq .
========================================= */
