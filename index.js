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

import { router as watchlistRouter } from "./routes/watchlist.js";

const ALIASES = new Map([
  ["$SPX", "SPX"],
  ["^GSPC", "SPX"],
  ["/ES", "ES"],
  ["ES1!", "ES"],   // TradingView style
]);
function canon(sym) {
  const s = String(sym||"").trim().toUpperCase();
  return ALIASES.get(s) || s;
}

/* ================= CONFIG ================= */
const PORT    = Number(process.env.PORT || 8080);
const IB_BASE = String(process.env.IB_BASE || "https://127.0.0.1:5000/v1/api").replace(/\/+$/,"");
const MOCK    = String(process.env.MOCK || "") === "1";

/* ================= STATE ================= */
const WATCH = {
  equities: new Set(),                 // "AAPL","NVDA","MSFT"
  options: [],                         // {underlying, expiration(YYYY-MM-DD), strike, right(C|P)}
};
const STATE = {
  equities_ts: [],                     // [{symbol,last,bid,ask,iv?,ts}]
  options_ts:  [],                     // [{underlying,expiration,strike,right,last,bid,ask,ts}]
  sweeps:      [],                     // UI shape for Sweeps screen
  blocks:      [],                     // UI shape for Blocks screen
};
const CACHE = {
  conidBySym: new Map(),               // key: "AAPL" -> { conid, ts }
  expBySym:   new Map(),               // key: "AAPL" -> { ts, expirations: [...] }
  sessionPrimed: false,
  primingSince: 0,
};

if (String(process.env.NODE_TLS_REJECT_UNAUTHORIZED) === "0") {
  console.warn("WARNING: TLS verification disabled (self-signed).");
}
console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK?"on":"off"}  IB=${IB_BASE}`);

/* ================= APP/WS ================= */
const app = express();
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
  // msg is an options_ts-like object: { symbol, option:{expiration,strike,right}, price, qty, side, ... }
  const notional = Math.round((msg.qty * (msg.price || 0) * 100));
  const isSweep  = msg.qty >= MIN_SWEEP_QTY && notional >= MIN_SWEEP_NOTIONAL;
  const isBlock  = msg.qty >= MIN_BLOCK_QTY  && notional >= MIN_BLOCK_NOTIONAL;

  if (!isSweep && !isBlock) return;

  // Build a stable-ish key to de-dupe mirrored spam
  const k = [
    msg.symbol, msg.option?.right, msg.option?.strike, msg.option?.expiration,
    msg.side, Math.round(msg.price*1000)||0
  ].join("|");

  if (seenRecently(k, DEDUP_MS)) return;

  const base = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
    ts: Date.now(),
    provider: "ibkr",
    symbol: msg.symbol,
    right: msg.option?.right,
    strike: msg.option?.strike,
    expiry: msg.option?.expiration,
    side: msg.side,
    qty: msg.qty,
    price: msg.price,
    notional,
  };

  if (isBlock) {
    pushAndFanout({ type: "blocks", ...base });
  } else if (isSweep) {
    pushAndFanout({ type: "sweeps", ...base });
  }
}

if (!global.STATE) global.STATE = {};
if (!STATE.watchlist) STATE.watchlist = { equities: [], options: [] };

/* ================= SEED & START ================= */
const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "AAPL,NVDA,MSFT,SPY").split(",")
  .map(s => s.trim().toUpperCase()).filter(Boolean);
const DEFAULT_OPTIONS = (process.env.DEFAULT_OPTIONS || "").trim()
  ? JSON.parse(process.env.DEFAULT_OPTIONS)
  : [
      { underlying: "AAPL", expiration: "2025-12-19", strike: 200, right: "C" },
      { underlying: "NVDA", expiration: "2025-12-19", strike: 150, right: "P" },
    ];
const SEED_MARK = ".seeded";

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

async function seedWatchlist() {
  await watchAddEquities(DEFAULT_EQUITIES);
  WATCH.options = Array.isArray(WATCH.options) ? WATCH.options : [];
  if (!WATCH.options.length) WATCH.options = DEFAULT_OPTIONS.slice();
  wsBroadcast("watchlist", getWatchlist());
}

seedWatchlist();

app.use(express.json());
app.use("/watchlist", watchlistRouter);
app.use(cors());                // if you need to lock this down: cors({ origin: [/localhost/, /tradeflow\.lol$/] })
app.use(compression());
app.use(morgan("dev"));

const server = http.createServer(app);

// ---- Create WS server, then mark ready and flush any queued broadcasts ----
wss = new WebSocketServer({ server, perMessageDeflate: false });
wsReady = true;

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
const DEMO_BASE = { SPY: 510, QQQ: 430, IWM: 205, AAPL: 268, NVDA: 206, MSFT: 516 };

/** Get an underlying's price from STATE.quotes or a fallback */
function getULPrice(ul){
  const q = (STATE.quotes && STATE.quotes[ul]) || null;
  if (q && typeof q.last === "number" && q.last > 0) return q.last;
  return DEMO_BASE[ul] ?? 100;
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
function normalizePrint(t, kind = "SWEEP"){
  const price = Number((t.last ?? t.bid ?? t.ask ?? 1).toFixed(2));
  const qty   = 10; // base; will be overridden below
  const notional = Number((price * 100 * qty).toFixed(2));
  return {
    kind: kind.toUpperCase(),           // "SWEEP" | "BLOCK"
    ul: t.underlying || "UNKNOWN",
    right: t.right === "C" ? "CALL" : "PUT",
    strike: t.strike ?? 0,
    expiry: t.expiration || "2099-12-31",
    side: "BUY",
    qty,
    price,
    notional,
    prints: Math.floor(1 + Math.random()*3),
    venue: "demo",
    ts: Date.now()
  };
}

/* ========== INITIAL SEEDING ========== */

// Ensure these are the symbols you already seeded
const SEEDED_EQUITIES = ["SPY","QQQ","IWM","AAPL","NVDA","MSFT"];

// Seed options once at startup (and you can call it again after quotes appear)
function seedDemoOptions(){
  const opts = seedDemoOptionsFromULs(SEEDED_EQUITIES);
  STATE.options_ts = opts;
  console.log("Seeded demo options:", 
    opts.slice(0,4).map(o => `${o.underlying} ${o.right} ${o.strike} ${o.expiration}`).join(" | "),
    `(+${Math.max(0, opts.length-4)} more)`
  );
}

// Call once during boot
seedDemoOptions();

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
      p.side = "BUY";
      p.venue = "demo";
      p.ts = Date.now();

    } else {
      // BLOCKS: keep large single prints so they always show
      p.prints = 1;
      p.qty = Math.floor(250 + Math.random() * 750); // 250-1000 contracts
      p.price = Number((p.price || 1).toFixed(2));
      p.notional = Math.max(150000, Math.round(p.qty * p.price * 100));
      p.side = "BUY";
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

/* ================= HELPERS ================= */
const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));
const clamp=(x,min,max)=>Math.max(min,Math.min(max,x));
const uniq=(a)=>Array.from(new Set(a));
const isNonEmptyStr=(s)=>typeof s==="string" && s.trim().length>0;
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
function getWatchlist(){ return { equities: normEquities(WATCH), options: normOptions(WATCH) }; }
// async function watchAddEquities(eqs=[]){
//   const before = normEquities(WATCH).length;
//   for (const s of eqs) { const u=String(s).toUpperCase(); if (u) WATCH.equities.add(u); }
//   const after = normEquities(WATCH).length;
//   if (after>before) wsBroadcast("watchlist", getWatchlist());
//   return Math.max(0, after-before);
// }
async function watchAddEquities(eqs=[]){
  const before = normEquities(WATCH).length;
  for (const s of eqs) { const u = canon(s); if (u) WATCH.equities.add(u); }
  const after = normEquities(WATCH).length;
  if (after>before) wsBroadcast("watchlist", getWatchlist());
  return Math.max(0, after-before);
}
async function ibConidForSymbol(sym){
  if (!isNonEmptyStr(sym)) return null;
  const key = canon(sym);

  // cache hit?
  const cached = CACHE.conidBySym.get(key);
  if (cached && cached.ts && (Date.now()-cached.ts) < CONID_TTL_MS) return cached.conid;

  // MOCK mode quick map
  if (MOCK){
    const demo = { AAPL:"265598", NVDA:"4815747", MSFT:"272093", SPY:"756733", SPX:"416904", ES:"123456" };
    const c = demo[key] || String(100000+Math.floor(Math.random()*9e5));
    CACHE.conidBySym.set(key, { conid:c, ts:Date.now() });
    return c;
  }

  // search once
  let best = null;
  try {
    const body = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
    const list = Array.isArray(body) ? body : (body ? [body] : []);

    // prefer STK, then IND, then FUT
    const pick = (secType) =>
      list.find(r => String(r?.symbol||"").toUpperCase()===key &&
                     (r?.sections||[]).some(s=>s?.secType===secType) && r?.conid);

    best = pick("STK") || pick("IND") || pick("FUT") || list.find(r => r?.conid);
  } catch {}

  // FUT (ES): choose a front contract if choices exist
  if (best && (best.sections||[]).some(s=>s.secType==="FUT")) {
    // some responses include Contracts array; pick nearest expiry
    try {
      const info = await ibFetch(`/iserver/secdef/info?conid=${best.conid}`);
      const contracts = info?.Contracts || info || [];
      if (Array.isArray(contracts) && contracts.length) {
        const now = Date.now();
        contracts.sort((a,b)=>{
          const ta = Date.parse(a?.expiry || a?.maturity || "2100-01-01");
          const tb = Date.parse(b?.expiry || b?.maturity || "2100-01-01");
          return Math.abs(ta-now) - Math.abs(tb-now);
        });
        const fut = contracts.find(c=>c?.conid) || contracts[0];
        if (fut?.conid) best = { ...best, conid: String(fut.conid) };
      }
    } catch {}
  }

  const conid = best?.conid ? String(best.conid) : null;
  if (conid) CACHE.conidBySym.set(key, { conid, ts:Date.now() });
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
  if (after>before) wsBroadcast("watchlist", getWatchlist());
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
let lastReqEndedAt = 0;
const MIN_GAP_MS = 250;
const MAX_RETRIES = 4;
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

/* ================= IB HELPERS (cached) ================= */
const CONID_TTL_MS = 12*60*60*1000;
const EXP_TTL_MS   = 30*60*1000;

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
function mapEquitySnapshot(sym, s){
  return { symbol:sym, last:num(s["31"]), bid:num(s["84"]), ask:num(s["86"]), iv:num(s["7059"]), ts:Date.now() };
}
function mapOptionTick(ul, expiration, strike, right, s0){
  return { underlying:ul, expiration, strike, right, last:num(s0["31"]), bid:num(s0["84"]), ask:num(s0["86"]), ts:Date.now() };
}

/* ================= POLLERS ================= */
async function ibFetchJson(url, init){
  const r = await fetch(url, init);
  if (!r.ok){ const body = await r.text().catch(()=> ""); throw new Error(`IB ${r.status} ${url} :: ${body || "null"}`); }
  try { return await r.json(); } catch { return {}; }
}
if (typeof fetch === "undefined") { global.fetch = (await import("node-fetch")).default; }

async function pollEquitiesOnce(){
  try{
    const symbols = normEquities(WATCH);
    if (!symbols.length) return;

    const pairs = [];
    for (const sym of symbols){
      try { const conid = await ibConidForSymbol(sym); if (conid) pairs.push({ sym, conid }); }
      catch {}
    }
    if (!pairs.length) return;

    let rows = [];
    if (MOCK){
      rows = pairs.map(p => ({ symbol:p.sym, last:100+Math.random()*50, bid:0, ask:0, ts:Date.now(), iv: p.sym==="MSFT" ? 0.40 : 1.00 }));
    } else {
      const conids = pairs.map(p=>p.conid).join(",");
      const snaps  = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
      for (const p of pairs){
        const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid)===String(p.conid)) : null;
        if (!s) continue;
        rows.push(mapEquitySnapshot(p.sym, s));
      }
    }
    if (rows.length){ STATE.equities_ts = rows; wsBroadcast("equity_ts", rows); }
  }catch(e){ console.warn("pollEquitiesOnce:", e.message); }
}
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
function pushAndFanout(msg) {
  if (msg?.type === "sweeps") {
    STATE.sweeps = [...(STATE.sweeps || []), msg];
    wsBroadcast("sweeps", STATE.sweeps.slice(-500));
    return;
  }
  if (msg?.type === "blocks") {
    STATE.blocks = [...(STATE.blocks || []), msg];
    wsBroadcast("blocks", STATE.blocks.slice(-500));
    return;
  }
  // Optional: expose raw prints to clients
  if (msg?.type === "print") {
    // you can keep a rolling buffer if you want
    // e.g., STATE.prints = [...(STATE.prints||[]), msg].slice(-2000);
    wsBroadcast("prints", [msg]); // light push; clients can aggregate if needed
  }
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

                // 1) NBBO snapshot for this OCC
                const osnap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86`);
                s0 = Array.isArray(osnap) && osnap[0] ? osnap[0] : {};
              }

              // Update NBBO cache (so classifyAggressor / intent has context)
              const occ = toOcc(ul, expiry, right, strike);
              const bid = Number(s0["84"]), ask = Number(s0["86"]);
              if (Number.isFinite(bid) || Number.isFinite(ask)) {
                NBBO_OPT.set(occ, { bid: bid || 0, ask: ask || 0, ts: Date.now() });
              }

              // 2) Emit the UI "tick" row you already use (keeps your “Options (read-only demo)” table healthy)
              ticks.push(mapOptionTick(ul, expiry, strike, right, s0));

              // 3) Turn 1-minute bars into prints (qty = volume, price = close)
              if (!MOCK && optConid) {
                let bars;
                try {
                  // history endpoint: synthesize prints from the last few minutes only
                  bars = await ibFetchJson(`${IB_BASE}/iserver/marketdata/history?conid=${optConid}&period=1d&bar=1min&outsideRth=true`);
                } catch (_) {
                  // If history is temporarily unavailable (e.g., IB 503), skip quietly
                  bars = null;
                }

                const list = bars?.data || bars?.points || bars?.bars || [];
                if (Array.isArray(list) && list.length) {
                  const lastSeen = LAST_BAR_OPT.get(optConid)?.t || 0;
                  // Only the tail to avoid flooding + ensure monotonicity
                  const tail = list.slice(-5);
                  for (const b of tail) {
                    const t = Number(b.t || b.time || Date.parse(b.timestamp || "")) || 0;
                    if (!t || t <= lastSeen) continue;

                    const c = Number(b.c || b.close || b.price || b.last || 0);
                    const v = Number(b.v || b.volume || 0);

                    if (v > 0 && Number.isFinite(c)) {
                      const nbbo = NBBO_OPT.get(occ);
                      // === This is the bit you asked to “show integration” for ===
                      const msg = normOptionsPrint({
                        ul, exp: expiry, right, strike, price: c, qty: v, nbbo
                      });
                      pushAndFanout(msg);
                      // promote large prints to sweeps/blocks (thresholded)
                      maybePublishSweepOrBlock(msg);
                    }
                    LAST_BAR_OPT.set(optConid, { t, c, v });
                  }
                }
              }
            } catch (_) { /* ignore one leg error to keep loop healthy */ }
          }
        }
      }
    }
    STATE.options_ts = ticks;
    wsBroadcast("options_ts", ticks);
  }catch(e){ console.warn("pollOptionsOnce:", e.message); }
}

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
app.get("/watchlist/equities", (_req, res) => {
  res.json({ equities: STATE.watchlist.equities || [] });
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
app.post("/watchlist/equities", express.json(), async (req, res) => {
  const raw = (req.body?.symbol || "").toString().trim();
  if (!raw) return res.status(400).json({ error: "symbol required" });
  await watchAddEquities([raw.toUpperCase()]);
  wsBroadcast("watchlist", getWatchlist());
  res.json({ ok: true, watchlist: getWatchlist() });
});

// app.delete("/watchlist/equities/:symbol", (req, res) => {
//   const sym = (req.params.symbol || "").toUpperCase();
//   const next = (STATE.watchlist.equities || []).filter(s => s !== sym);
//   STATE.watchlist.equities = next;
//   res.json({ ok: true, equities: next });
// });
app.delete("/watchlist/equities/:symbol", async (req, res) => {
  const sym = (req.params.symbol || "").toUpperCase();
  WATCH.equities.delete(sym);
  wsBroadcast("watchlist", getWatchlist());
  res.json({ ok: true, watchlist: getWatchlist() });
});

/* Optional: read-only demo options endpoint */
app.get("/watchlist/options", (_req, res) => {
  res.json({ options: STATE.watchlist.options || [] });
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
  wsSend(ws, "watchlist", getWatchlist());
});
(async () => {
  try { await seedWatchlist(); } catch (e) { console.warn("seedWatchlist:", e.message); }
})();

/* ================= SCHEDULERS ================= */
setInterval(pollEquitiesOnce, 10_000);
setInterval(pollOptionsOnce,  13_000);
setInterval(pollSweepsOnce,    5_000);
setInterval(pollBlocksOnce,    5_000);

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
app.get("/watchlist", (req,res)=>res.json(getWatchlist()));

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
app.get("/api/flow/sweeps",     (req,res)=>res.json(STATE.sweeps      ?? []));
app.get("/api/flow/blocks",     (req,res)=>res.json(STATE.blocks      ?? []));

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
    if (fs.existsSync(SEED_MARK)) return;
    const curEq = new Set(normEquities(WATCH));
    const toAddEq = DEFAULT_EQUITIES.filter(s => !curEq.has(s));
    if (toAddEq.length) await watchAddEquities(toAddEq);

    const curOptsKey = new Set(normOptions(WATCH).map(o => `${o.underlying}:${o.expiration}:${o.strike}:${o.right}`));
    const toAddOpts = DEFAULT_OPTIONS.filter(o => {
      const k = `${String(o.underlying).toUpperCase()}:${o.expiration}:${o.strike}:${String(o.right).toUpperCase()[0]}`;
      return !curOptsKey.has(k);
    });
    if (toAddOpts.length) await watchAddOptions(toAddOpts);

    // kick first polls so UI has data immediately
    await pollEquitiesOnce();
    await pollOptionsOnce();
    wsBroadcast("watchlist", getWatchlist());
    if (STATE.equities_ts?.length) wsBroadcast("equity_ts", STATE.equities_ts);
    if (STATE.options_ts?.length)  wsBroadcast("options_ts", STATE.options_ts);

    fs.writeFileSync(SEED_MARK, new Date().toISOString());
    console.log(`[seed] equities=${JSON.stringify(toAddEq)} options=${toAddOpts.length}`);
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
