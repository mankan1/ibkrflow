import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import { WebSocketServer } from "ws";

import makePrintsRouter from "./routes/prints.js";
import makeHeadlinesRouter from "./routes/headlines.js";
import { makeBus } from "./ws/bus.js";
import { d } from "./debug/logger.js";

import fs from "fs";
import {
  inferActionForOptionTrade,
  resetActionState
} from "./actions.js";
import { buildNotables } from './insights/notables.js';

import { router as watchlistRouter } from "./routes/watchlist.js";
import { loadWatchlistFile, saveWatchlistFile } from "./watchlist.js";

import {
  WATCH, getWatchlist, addEquity, addOptions, 
  setBroadcaster, broadcastWatchlist
} from "./state.js";


const ALIASES = new Map([
  ["$SPX", "SPX"],
  ["^GSPC", "SPX"],
  ["/ES", "ES"],
  ["ES1!", "ES"],   // TradingView style
]);

/** Aliases -> canonical ticker. Keep keys canonical and values as all accepted aliases. */
const CANON_ALIASES = {
  // Indexes
  SPX: ["SPX", "SPX.X", "/SPX", "^GSPC", "$SPX"],
  NDX: ["NDX", "^NDX", "$NDX"],
  DJX: ["DJX", "DJIA", "^DJI", "$DJI", "$DJX"],
  // ETFs (common)
  SPY: ["SPY", "$SPY"],
  QQQ: ["QQQ", "$QQQ"],
  IWM: ["IWM", "$IWM"],
  // Futures (pick one canonical — here we use "ES")
  ES:  ["ES", "/ES", "ES=F", "ES1!", "$ES", "MES"], // treat MES as ES for top-level quote use
  NQ:  ["NQ", "/NQ", "NQ=F", "NQ1!", "$NQ", "MNQ"],
  YM:  ["YM", "/YM", "YM=F", "YM1!", "$YM", "MYM"],
};

/** Fast lookup map (alias -> canonical). */
const ALIAS_TO_CANON = (() => {
  const m = new Map();
  for (const [canon, aliases] of Object.entries(CANON_ALIASES)) {
    for (const a of aliases) m.set(a.toUpperCase(), canon);
  }
  return m;
})();
// put near IB helpers
let IB_COOKIES = ""; // e.g. "x-sess-uuid=...; Path=/; Secure; HttpOnly"
function captureCookies(res) {
  try {
    const set = res.headers.raw?.()["set-cookie"] || res.headers.getSetCookie?.() || [];
    if (!set.length) return;
    // Keep only "name=value" pairs; drop attributes
    const pairs = set.map(s => String(s).split(";")[0]).filter(Boolean);
    if (pairs.length) {
      // Merge with existing cookies, de-dupe by cookie name
      const cur = new Map(IB_COOKIES.split("; ").filter(Boolean).map(kv => kv.split("=",2)));
      for (const p of pairs) {
        const [k,v] = p.split("=",2);
        if (k) cur.set(k, v ?? "");
      }
      IB_COOKIES = Array.from(cur.entries()).map(([k,v]) => `${k}=${v}`).join("; ");
    }
  } catch {}
}
function cookieHeader() {
  return IB_COOKIES ? { Cookie: IB_COOKIES } : {};
}
/** Attempt to extract underlying from OCC-style option symbol (very lenient). */
function extractUnderlyingFromOCC(s) {
  // Examples: "SPX   251121C00500000", "AAPL  251121P00190000"
  // If it looks like "UNDERLYING<spaces>YYMMDD[CP]strike" return the first token.
  const m = String(s).trim().match(/^([A-Za-z.$/^]+)\s{2,}\d{6}[CP]/);
  return m ? m[1] : null;
}

/** Canonicalize user-entered symbols for consistent keys & lookups. */
function normalizeSymbol(input) {
  if (!input) return "";
  // Uppercase, trim, strip leading $, spaces
  let s = String(input).trim().toUpperCase().replace(/^\$/, "");

  // If it's an OCC-like option root, peel off the underlying
  const occUl = extractUnderlyingFromOCC(s);
  if (occUl) s = occUl.toUpperCase().replace(/^\$/, "");

  // Simple alias resolution
  if (ALIAS_TO_CANON.has(s)) return ALIAS_TO_CANON.get(s);

  // Handle leading slash on futures generically ("/ES" -> "ES")
  if (s.startsWith("/")) s = s.slice(1);

  // Strip common suffixes some vendors add (e.g., ".X")
  s = s.replace(/\.X$/, "");

  return s;
}

/**
 * Deduplicate by canonical symbol. Keeps the first occurrence for each symbol.
 * Accepts either [{symbol, ...}] rows or [{sym, conid}] pairs — pass a projector to read the symbol.
 */
function fixCollision(items, getSym = (x) => x.symbol, setSym = (x, s) => (x.symbol = s)) {
  const seen = new Set();
  const out = [];
  for (const it of items) {
    const canon = normalizeSymbol(getSym(it));
    if (seen.has(canon)) continue;
    seen.add(canon);
    setSym(it, canon);
    out.push(it);
  }
  return out;
}

/* ================= CONFIG ================= */
const PORT    = Number(process.env.PORT || 8080);
const IB_BASE = String(process.env.IB_BASE || "https://127.0.0.1:5000/v1/api").replace(/\/+$/,"");
const MOCK    = String(process.env.MOCK || "") === "1";

const STATE = {
  equities_ts: [],                     // [{symbol,last,bid,ask,iv?,ts}]
  options_ts:  [],                     // [{underlying,expiration,strike,right,last,bid,ask,ts}]
  sweeps:      [],                     // UI shape for Sweeps screen
  blocks:      [],                     // UI shape for Blocks screen
  prints:      [],   // <— add this
};
const latestUL = new Map(); // "NVDA" -> 196.95
// Resolve an option contract conid given (ul, expiry "YYYY-MM-DD", right "C|P", strike number)
const OPTION_CONID_TTL_MS = 24*60*60*1000;
const cacheOptConid = new Map(); // key: "AAPL|2025-12-19|C|200" -> { conid, ts }

// ===== Option quotes buffer (debounced) -> pushes topic: option_quotes
const optionQuoteBuf = new Map(); // occ -> { occ, bid, ask, mid, ts }
const OPTION_QUOTES_FLUSH_MS = 500;
const FUT_ROOTS = new Set(["ES", "NQ", "YM", "RTY", "CL", "GC"]); // add what you want
const isFutures = (ul) => FUT_ROOTS.has(ul.toUpperCase().replace(/^\//,""));
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

const server = http.createServer(app);
let wsReady = false;
let wss = /** @type {import('ws').WebSocketServer|null} */ (null);

// ---- Create WS server, then mark ready and flush any queued broadcasts ----
wss = new WebSocketServer({ server, perMessageDeflate: false });
wsReady = true;

// ===== Sweep/Block thresholds =====
const MIN_SWEEP_QTY       = Number(process.env.MIN_SWEEP_QTY || 50);     // contracts
const MIN_SWEEP_NOTIONAL  = Number(process.env.MIN_SWEEP_NOTIONAL || 20000); // $ (qty * price * 100)
const MIN_BLOCK_QTY       = Number(process.env.MIN_BLOCK_QTY || 250);
const MIN_BLOCK_NOTIONAL  = Number(process.env.MIN_BLOCK_NOTIONAL || 100000);
const DEDUP_MS            = Number(process.env.SWEEP_DEDUP_MS || 3500); // de-dupe bursty repeats
const AUTOTRADE = {
  enabled: true,
  // Trade filters (the flow must pass all)
  minNotional: 100000,         // e.g. $100k notional for "big" sweep/block
  minQty: 200,                 // minimum contract prints qty on the event
  minActionConf: 0.7,          // inferred action confidence threshold [0..1]
  allowedTypes: ["sweep", "block"], // act only on sweeps/blocks
  allowShortOptions: true,    // if false, skip short STO/BTC (or convert—see below)

  // Sizing
  fixedQty: 1,                 // buy/sell this many contracts per signal (simple)

  // Risk/Stops (ATR on the underlying)
  atrLen: 14,                  // ATR window (days)
  stopATR: 1.0,                // hard stop at 1.0 * ATR adverse move
  trailATR: 1.5,               // trail stop offset of 1.5 * ATR

  // Order behavior
  orderType: "MKT",            // "MKT" or "LMT" (LMT uses mid)
  maxOpenPositions: 10,        // safety limit
};

async function getATRForUnderlying(ul, len = AUTOTRADE.atrLen) {
  if (isFutures(ul)) return getATRForFutures(futRoot(ul), len);
  const cached = ATR_CACHE.get(ul);
  const now = Date.now();
  if (cached && (now - cached.lastCalcTs) < 60_000) return cached.atr; // 1-min reuse

  // Pull recent daily bars (need len+1 for TR calc)
  let bars;
  try {
    // IBKR "1d" period & "1day" bar sometimes varies; this version works for "period=2w" (two weeks)
    // bars = await ibFetchJson(`${IB_BASE}/iserver/marketdata/history?conid=${await ibConidForSymbol(ul)}&period=3M&bar=1day&outsideRth=true`);
    bars = await ibFetch(`${IB_BASE}/iserver/marketdata/history?conid=${await ibConidForSymbol(ul)}&period=3M&bar=1day&outsideRth=true`);
  } catch { bars = null; }

  const list = (bars?.data || bars?.points || bars?.bars || []).slice(-Math.max(2*len, len+1));
  if (list.length < len + 1) return undefined;

  // Normalize fields; compute TR then ATR (RMA)
  const norm = list.map(b => ({
    t: Number(b.t || b.time || Date.parse(b.timestamp || "")) || 0,
    o: Number(b.o ?? b.open ?? NaN),
    h: Number(b.h ?? b.high ?? NaN),
    l: Number(b.l ?? b.low ?? NaN),
    c: Number(b.c ?? b.close ?? b.last ?? NaN),
  })).filter(x => Number.isFinite(x.o) && Number.isFinite(x.h) && Number.isFinite(x.l) && Number.isFinite(x.c));

  if (norm.length < len + 1) return undefined;

  const tr = [];
  for (let i = 1; i < norm.length; i++) {
    const hi = norm[i].h, lo = norm[i].l, pc = norm[i-1].c;
    const v = Math.max(hi - lo, Math.abs(hi - pc), Math.abs(lo - pc));
    tr.push(v);
  }

  // Wilder's RMA
  let atr = 0;
  const seed = tr.slice(0, len).reduce((a,b)=>a+b,0) / len;
  atr = seed;
  for (let i = len; i < tr.length; i++) {
    atr = (atr * (len - 1) + tr[i]) / len;
  }

  ATR_CACHE.set(ul, { atr, ymd: etYMD(), lastCalcTs: now });
  return atr;
}
function isHighConviction(msg) {
  // Try to be robust to your message shapes:
  const kind = (msg.kind || msg.type || "").toLowerCase();  // "sweep"/"block"/"print"
  const isSweep = kind.includes("sweep") || msg.sweep === true || msg.is_sweep === true;
  const isBlock = kind.includes("block") || msg.block === true || msg.is_block === true;

  const flowTypeOk = AUTOTRADE.allowedTypes.includes(isSweep ? "sweep" : isBlock ? "block" : "other");
  if (!flowTypeOk) return false;

  // Notional/qty (fallback to price*qty if notional absent)
  const price = Number(msg.price ?? msg.last ?? msg.fill_price ?? NaN);
  const qty   = Number(msg.qty ?? msg.size ?? msg.contracts ?? NaN);
  if (!(qty > 0)) return false;

  const notional = Number(msg.notional ?? (Number.isFinite(price) ? price * qty * 100 : NaN));
  if (!(notional >= AUTOTRADE.minNotional)) return false;
  if (!(qty >= AUTOTRADE.minQty)) return false;

  // Action confidence (0..1)
  const conf = Number(msg.action_conf ?? msg.confidence ?? NaN);
  if (Number.isFinite(conf) && conf < AUTOTRADE.minActionConf) return false;

  // Must have a clear action + option identity
  if (!msg.action || !msg.right || !msg.expiry || !Number.isFinite(Number(msg.strike))) return false;

  return true;
}
async function resolveOptConid(ul, expiryISO, right, strike) {
  // if (isFutures(ul) || /^[A-Z]{1,3}$/.test(ul) && ul.length<=3 && ul===ul.toUpperCase()){ // crude check
  //   // const root = isFutureSym(ul) ? futRoot(ul) : ul;
  //   // const active = ACTIVE_FUT.get(root) || await ibActiveFuture(root);
  //   // if (!active) return null;
  //   // const month = expiryISO.replace(/-/g,"").slice(0,6); // YYYYMM
  //   // return resolveFOPConid(root, active.conid, month, right, strike);
  //   return resolveFopConid(ul, expiryISO, right, strike)
  // }  
  if (isFutures(ul)) {
    // normalize to root ("/ES" -> "ES") before calling
    const root = futRoot(ul);
    return resolveFopConid(root, expiryISO, right, strike);
  }  
  const yyyy = expiryISO.slice(0,4), mm = expiryISO.slice(5,7);
  const month = `${yyyy}${mm}`;
  const conid = await ibConidForSymbol(ul);
  if (!conid) return null;

  const url = `${IB_BASE}/iserver/secdef/info?conid=${conid}&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${strike}`;
  // const info = await ibFetchJson(url);
  const info = await ibFetch(url);
  const arr = Array.isArray(info) ? info :
              Array.isArray(info?.Contracts) ? info.Contracts :
              info ? [info] : [];
  return (arr.find(r => r?.conid)?.conid) || null;
}

// Place option order (very simple market/limit)
async function ibPlaceOptionOrder({ optConid, action, qty, lmtPrice }) {
  // action: "BUY" or "SELL"
  const order = {
    conid: optConid,
    secType: "OPT",
    side: action,
    quantity: qty,
    type: AUTOTRADE.orderType === "LMT" ? "LMT" : "MKT",
    tif: "GTC",
    outsideRth: true,
    ...(AUTOTRADE.orderType === "LMT" && Number.isFinite(lmtPrice) ? { lmtPrice: +lmtPrice } : {}),
  };
  try {
    // const r = await ibFetchJson(`${IB_BASE}/iserver/account/orders`, {
    //   method: "POST",
    //   headers: { "Content-Type":"application/json" },
    //   body: JSON.stringify([order]),
    // });
    const r = await ibFetch(`${IB_BASE}/iserver/account/orders`, {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify([order]),
    });    
    return r;
  } catch (e) {
    console.warn("ibPlaceOptionOrder error:", e?.message || e);
    return null;
  }
}

function computeMidFromNBBO(occ) {
  const q = NBBO_OPT.get(occ);
  if (q && q.bid > 0 && q.ask > q.bid) return (q.bid + q.ask) / 2;
  return undefined;
}

// ---------- Optional: reuse around-ATM builder elsewhere ----------
function buildOptionsAroundATM(sym, getLastFn, around = 10) {
  const s = String(sym).toUpperCase();
  const last = typeof getLastFn === "function" ? getLastFn(s) : refPrice(s);
  if (!Number.isFinite(last)) return [];

  const step = stepFor(s);
  const atm  = roundToStep(last, step);
  const scheme = isFutureSymbol(s) ? "futures" : "equity";

  // next 2 expiries by scheme
  const expiries = [nearestExpiry(0, scheme), nearestExpiry(7, scheme)];

  const out = [];
  for (const expiration of expiries) {
    for (let i = -around; i <= around; i++) {
      const strike = +(atm + i * step).toFixed(2);
      out.push({ underlying: s, expiration, strike, right: "C" });
      out.push({ underlying: s, expiration, strike, right: "P" });
    }
  }
  return dedupeOptions(out);
}

async function autoTradeOnFlow(msg, underPx) {
  try {
    if (!AUTOTRADE?.enabled) return;
    if (typeof OPEN_COUNT !== "number") OPEN_COUNT = 0;
    if (OPEN_COUNT >= (AUTOTRADE.maxOpenPositions ?? 0)) return;
    if (!isHighConviction || !isHighConviction(msg)) return;

    // --- Normalize core fields safely ---
    const symbol = (msg?.ul || msg?.symbol || msg?.underlying || "").toString().toUpperCase();
    if (!symbol) return;

    const rightRaw = (msg?.right || "").toString().toUpperCase();
    const right = rightRaw === "C" || rightRaw === "CALL" ? "C"
               : rightRaw === "P" || rightRaw === "PUT"  ? "P"
               : null;
    if (!right) return;

    const strike = Number(msg?.strike);
    if (!Number.isFinite(strike)) return;

    const expiry = (msg?.expiry || "").toString(); // "YYYY-MM-DD" expected
    if (!/^\d{4}-\d{2}-\d{2}$/.test(expiry)) return;

    const action = (msg?.action || "").toString().toUpperCase(); // BTO/BTC/STO/STC
    if (!action) return;

    const occ = toOcc(symbol, expiry, rightToCP(right), strike);

    // Map action -> direction for *same option*
    // Ignore close actions by default (keep as you prefer)
    let dir = null;
    if (action === "BTO") dir = "BUY";
    else if (action === "STO") dir = "SELL";
    else return;

    if (dir === "SELL" && AUTOTRADE.allowShortOptions === false) {
      // If you don't want to short options, you could flip to a defined-risk substitute.
      // For now, just bail out.
      return;
    }

    // --- ATR of the underlying for risk logic ---
    const atr = await getATRForUnderlying(symbol, AUTOTRADE.atrLen);
    if (!Number.isFinite(atr) || atr <= 0) return;

    // --- Resolve option conid for this exact OCC ---
    const optConid = await resolveOptConid(symbol, expiry, right, strike);
    if (!optConid) return;

    // --- Price for LMT: attempt NBBO → fallback mid → undefined for MKT if you allow it ---
    let mid = computeMidFromNBBO ? computeMidFromNBBO(occ) : undefined;
    if (!Number.isFinite(mid)) {
      const nbbo = NBBO_OPT?.get ? NBBO_OPT.get(occ) : null;
      if (nbbo && Number.isFinite(nbbo.bid) && Number.isFinite(nbbo.ask) && nbbo.bid > 0 && nbbo.ask > 0) {
        mid = (nbbo.bid + nbbo.ask) / 2;
      }
    }
    if (AUTOTRADE.forceLimit && !Number.isFinite(mid)) return; // obey a "must use LMT" setting if you have it

    const qty = Number(AUTOTRADE.fixedQty ?? 1);
    if (!(qty > 0)) return;

    // --- Place the entry order ---
    const orderRes = await ibPlaceOptionOrder({
      optConid,
      action: dir,   // "BUY" | "SELL"
      qty,
      lmtPrice: Number.isFinite(mid) ? mid : undefined,
    });
    if (!orderRes) return;

    // --- Record a managed position keyed by occ + ts ---
    const entryUnderPx = Number(underPx);
    if (!Number.isFinite(entryUnderPx)) return;

    const trailOffset = Number(AUTOTRADE.trailATR ?? 0) * atr;
    const bullish = right === "C";  // calls like up, puts like down (for long)
    const dirSign = (dir === "BUY") ? (bullish ? +1 : -1) : (bullish ? -1 : +1);
    const trailLine = entryUnderPx - dirSign * trailOffset; // initial trailing line behind favorable move

    const posId = `${occ}#${Date.now()}`;
    if (!POSITIONS || typeof POSITIONS.set !== "function") {
      // ensure POSITIONS is a Map
      POSITIONS = new Map();
    }
    POSITIONS.set(posId, {
      id: posId,
      occ,
      ul: symbol,
      right,
      strike,
      dir,           // BUY or SELL
      qty,
      optConid,
      entryUnderPx,
      atr,
      trailLine,
      openedAt: Date.now(),
      // optional extras:
      // entryFill: orderRes.fillPrice,  // if your ibPlaceOptionOrder returns fills
      // entryOccMid: mid,
    });

    OPEN_COUNT += 1;
    console.log("[AUTOTRADE] Opened", posId, dir, occ, "ATR=", atr.toFixed(3));
  } catch (e) {
    console.warn("[AUTOTRADE] autoTradeOnFlow error:", e?.message || e);
  }
}
// Real-time last price cache: lastBySymbol["SPY"] = 504.23, etc.
const lastBySymbol = Object.create(null);

async function evaluateStopsOnTick(ul, px) {
  if (!AUTOTRADE.enabled || POSITIONS.size === 0) return;

  const toClose = [];

  for (const [posId, p] of POSITIONS) {
    if (p.ul !== ul) continue;

    // Recompute ATR occasionally if desired; here we reuse cached ATR:
    const atr = p.atr;

    const bullish = (p.right === "C");
    const dirSign = (p.dir === "BUY") ? (bullish ? +1 : -1)
                                      : (bullish ? -1 : +1);

    // Hard stop: adverse move from entry >= stopATR * atr
    const adverseMove = (px - p.entryUnderPx) * dirSign * -1; // positive if adverse
    const hardStopHit = adverseMove >= (AUTOTRADE.stopATR * atr);

    // Trailing: update trailLine on favorable move; exit if px crosses back
    const favorableMove = (px - p.entryUnderPx) * dirSign; // positive if favorable
    let newTrail = p.trailLine;

    if (favorableMove > 0) {
      const trailOffset = AUTOTRADE.trailATR * atr;
      const candidate = px - dirSign * trailOffset;
      // only ratchet in the favorable direction
      if (dirSign > 0) newTrail = Math.max(candidate, p.trailLine);
      else             newTrail = Math.min(candidate, p.trailLine);
    }

    const trailHit = (dirSign > 0) ? (px <= newTrail) : (px >= newTrail);

    // Decide exit
    if (hardStopHit || trailHit) {
      toClose.push([posId, p]);
    } else {
      // persist ratchet
      if (newTrail !== p.trailLine) {
        p.trailLine = newTrail;
        POSITIONS.set(posId, p);
      }
    }
  }

  // Close any hit positions
  for (const [posId, p] of toClose) {
    const closeSide = (p.dir === "BUY") ? "SELL" : "BUY";
    const mid = computeMidFromNBBO(p.occ);
    await ibPlaceOptionOrder({ optConid: p.optConid, action: closeSide, qty: p.qty, lmtPrice: mid });
    POSITIONS.delete(posId);
    OPEN_COUNT = Math.max(0, OPEN_COUNT - 1);
    console.log("[AUTOTRADE] Closed", posId, "by", (/* hard/ trail? */ "rule"));
  }
}

// ========= RUNTIME STATE ======================================================
const ATR_CACHE = new Map();       // ul -> { atr, ymd, lastCalcTs }
const POSITIONS = new Map();       // posId -> { occ, ul, right, strike, dir, qty, entryUnderPx, atr, trailLine, openedAt, optConid }
let OPEN_COUNT = 0;
const RECENT_SWEEPS = new Map(); // key -> ts
function seenRecently(key, ms) {
  const now = Date.now();
  const ts = RECENT_SWEEPS.get(key) || 0;
  if (now - ts < ms) return true;
  RECENT_SWEEPS.set(key, now);
  return false;
}
// ===== Notables broadcast guard =====
function safeBroadcastNotables(data) {
  // Normalize to array and drop falsy entries
  const arr = Array.isArray(data) ? data.filter(Boolean) : [];

  // 1) Skip empty payloads
  if (arr.length === 0) return;

  // 2) Skip duplicates (same content as last send)
  const s = JSON.stringify(arr);
  if (s === lastNotablesJson) return;

  // 3) Broadcast & record
  wsBroadcast("notables", arr);
  lastNotablesJson = s;
}

async function ibConidForOption(ul, expiry, right, strike) {
  const key = `${String(ul).toUpperCase()}|${expiry}|${String(right).toUpperCase()}|${Number(strike)}`;
  const hit = cacheOptConid.get(key);
  if (hit?.conid && (Date.now() - hit.ts) < OPTION_CONID_TTL_MS) return hit.conid;

  const occLocal = (() => {
    const root = (String(ul).toUpperCase() + "      ").slice(0, 6);
    const yymmdd = String(expiry).replace(/-/g,"").slice(2,8);
    const cp = String(right).toUpperCase().startsWith("C") ? "C" : "P";
    const k = String(Math.round(Number(strike) * 1000)).padStart(8, "0");
    return `${root}${yymmdd}${cp}${k}`;
  })();

  try {
    const list = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(occLocal)}`);
    if (Array.isArray(list)) {
      const best = list.find(r => r?.conid) || null;
      if (best?.conid) {
        const c = String(best.conid);
        cacheOptConid.set(key, { conid: c, ts: Date.now() });
        return c;
      }
    }
  } catch { /* fall through */ }

  // 2) Walk via UL’s option chain and match the exact (expiry/right/strike).
  try {
    const ulConid = await ensureConid(ul);
    if (!ulConid) return null;
    // IB returns option contracts in /iserver/secdef/info for an equity/ind conid as Contracts array.
    const info = await ibFetch(`/iserver/secdef/info?conid=${encodeURIComponent(ulConid)}`);
    const contracts = Array.isArray(info?.Contracts) ? info.Contracts : (Array.isArray(info) ? info : []);
    if (!contracts.length) return null;

    // Normalize matching
    const wantCP = String(right).toUpperCase().startsWith("C") ? "C" : "P";
    const wantStrike = Number(strike);
    const wantExp = String(expiry).replace(/-/g, ""); // "YYYYMMDD"

    // Try to find exact match (expiry/strike/right). IB variably uses fields: strike, right, expiry|maturity
    const pick = contracts.find(c => {
      const cp  = String(c?.right || c?.cp || "").toUpperCase();
      const k   = Number(c?.strike || c?.k);
      const exp = (c?.expiry || c?.maturity || "").replace(/-/g,"");
      return cp === wantCP && Number.isFinite(k) && Math.abs(k - wantStrike) < 1e-6 && exp.startsWith(wantExp);
    });

    const out = pick?.conid ? String(pick.conid) : null;
    if (out) cacheOptConid.set(key, { conid: out, ts: Date.now() });
    return out;
  } catch { return null; }
}
const getLast = (sym) => {
  const v = lastBySymbol[sym?.toUpperCase()?.replace(/^\//, "")];
  return Number.isFinite(v) ? v : null;
};
async function buildNearATMOptionConidsForUL(ul) {
  const rows = [];
  const spot = getUnderlyingLastNear(ul, Date.now()) ?? getLast(ul);
  if (!Number.isFinite(spot)) return rows;

  const expiries = nextFridays(2); // you already defined this helper
  const strikes = [-10,-5,0,5,10].map(dx => Math.round((spot + dx) / 5) * 5);

  for (const exp of expiries) {
    for (const K of strikes) {
      for (const cp of ["C","P"]) {
        const conid = await ibConidForOption(ul, exp, cp, K).catch(()=>null);
        if (!conid) continue;
        rows.push({ ul, exp, right: cp, strike: K, conid });
      }
    }
  }
  return rows;
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

if (!global.STATE) global.STATE = {};
if (!STATE.watchlist) STATE.watchlist = { equities: [], options: [] };
// 1) Single source of truth
// ---------- seed ----------
if (!STATE.watchlist) STATE.watchlist = { equities: [], options: [] };

// 1) Single source of truth
// const EQUITIES = ["SPY", "AAPL", "NVDA", "SPX", "/ES", "/NQ", "/YM"]; // what you want in equity_ts
// const OPTIONABLE = new Set(["SPY", "AAPL", "NVDA", "SPX", "/ES", "/NQ", "/YM"]); // your provider supports
const EQUITIES = ["/ES", "/NQ"]; // what you want in equity_ts
const OPTIONABLE = new Set(["/ES", "/NQ"]); // your provider supports

const OPTION_UNDERLYINGS = EQUITIES.filter(s => OPTIONABLE.has(s)); // derive

// ---------- date helpers ----------
const toISO = d => new Date(d.getTime() - d.getTimezoneOffset()*60000).toISOString().slice(0, 10);
const addDays = (d, n) => { const x = new Date(d); x.setDate(x.getDate() + n); return x; };

function nextWeekdayOnOrAfter(startDate, weekdays) {
  const want = new Set(weekdays);
  const d = new Date(startDate);
  for (let i = 0; i < 28; i++) {
    if (want.has(d.getDay())) return toISO(d);
    d.setDate(d.getDate() + 1);
  }
  return toISO(d);
}

// ---------- expiry pickers ----------
function nearestExpiry(days = 0, scheme = "equity") {
  const start = addDays(new Date(), days);
  const weekdays = (scheme === "futures") ? [1, 3, 5] : [5]; // Mon/Wed/Fri vs Fri
  return nextWeekdayOnOrAfter(start, weekdays);
}

// ---------- symbol / price step helpers ----------
function isFutureSymbol(sym) {
  if (!sym) return false;
  const s = String(sym).toUpperCase();
  if (s.startsWith("/")) return true;         // /ES, /NQ, ...
  const FUTS = new Set(["ES","NQ","RTY","YM","CL","GC","SI","ZN","ZB","ZF","ZT","HG","NG"]);
  return FUTS.has(s);
}
const FUTS_ALIASES = new Map([
  // choose ONE canonical — here we canonicalize to "/ES", "/NQ", "/YM"
  ["ES", "/ES"], ["/ES", "/ES"],
  ["NQ", "/NQ"], ["/NQ", "/NQ"],
  ["YM", "/YM"], ["/YM", "/YM"],
  // add others as needed
]);

// export function normalizeSymbol(s) {
//   if (!s) return "";
//   const t = String(s).trim().toUpperCase();
//   return FUTS_ALIASES.get(t) ?? t;
// }

function stepFor(sym) {
  const s = String(sym).toUpperCase();
  if (s === "SPX") return 1;          // whole-dollar strikes
  if (s === "SPY") return 1;          // $1 strikes near ATM
  if (s === "QQQ" || s === "IWM") return 1;
  if (isFutureSymbol(s)) {
    // very rough defaults; adjust to your venue:
    if (s.includes("ES")) return 5;   // ES options $5 strikes
    if (s.includes("NQ")) return 10;  // NQ options $10 strikes
    if (s.includes("YM")) return 5;
    return 5;
  }
  return 1;
}

function roundToStep(value, step) {
  return Math.round(value / step) * step;
}

// Fallback ref price (replace with your own cache/NBBO)
function refPrice(sym) {
  const s = String(sym).toUpperCase();
  if (typeof getLast === "function") {
    const px = getLast(s);
    if (Number.isFinite(px)) return px;
  }
  // last resort: look in a cache you maintain
  if (STATE && STATE.last && Number.isFinite(STATE.last[s])) return STATE.last[s];
  return NaN;
}

function dedupeOptions(rows) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const key = `${r.underlying}|${r.expiration}|${r.strike}|${r.right}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  return out;
}

// ---------- build a thin options list (nearest expiries/strikes) ----------
function makeOptionList(underlyings, { dte = [1, 7], strikes = 61 } = {}) {
  const out = [];
  for (const ul of underlyings) {
    const scheme = isFutureSymbol(ul) ? "futures" : "equity";  // FIX: use `ul` not `sym`
    const step = stepFor(ul);
    const center = refPrice(ul);
    if (!Number.isFinite(center)) continue;

    for (const days of dte) {
      const expiry = nearestExpiry(days, scheme);
      for (let k = -Math.floor(strikes / 2); k <= Math.floor(strikes / 2); k++) {
        const strike = roundToStep(center + k * step, step);
        out.push({ underlying: ul, expiration: expiry, strike, right: "C" });
        out.push({ underlying: ul, expiration: expiry, strike, right: "P" });
      }
    }
  }
  return dedupeOptions(out);
}

// ---------- watchlist payload + broadcast ----------
const WATCHLIST = {
  equities: EQUITIES,
  options: makeOptionList(OPTION_UNDERLYINGS, { dte: [1, 8], strikes: 61 })
};

// IMPORTANT: your wsBroadcast is (topic, data)
// wsBroadcast("watchlist", WATCHLIST);


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

const DEFAULT_OPTIONS = (process.env.DEFAULT_OPTIONS || "").trim()
  ? JSON.parse(process.env.DEFAULT_OPTIONS)
  : []; // no demo seeds

/* ================= IB HELPERS (cached) ================= */
const CONID_TTL_MS = 12*60*60*1000;
const EXP_TTL_MS   = 30*60*1000;

let lastReqEndedAt = 0;

const WS_QUEUE = []; // payloads buffered until WS is ready

const FUT_EXCH = "GLOBEX";

function isFutureSym(s) { return typeof s === "string" && s.trim().startsWith("/"); }
function futRoot(s)     { return s.replace(/^\//, "").toUpperCase(); } // "/ES" -> "ES"

const ACTIVE_FUT = new Map(); // key: "ES" -> { conid, symbol: "ESZ5", ymd: "2025-12-??" }

async function ibActiveFuture(root){ // pick the front/active contract
  // Option A (simple/robust): search secdefs and pick nearest-dated GLOBEX future
  // const q = await ibFetchJson(`${IB_BASE}/iserver/secdef/search?symbol=${root}&sectype=FUT&exchange=${FUT_EXCH}`);
  const q = await ibFetch(`${IB_BASE}/iserver/secdef/search?symbol=${root}&sectype=FUT&exchange=${FUT_EXCH}`);
  const arr = Array.isArray(q) ? q : (q ? [q] : []);
  // Filter to tradingClass === root and has conid & maturity
  const futs = arr.filter(x => x?.conid && x?.exchange === FUT_EXCH && (x?.tradingClass === root || x?.symbol === root));
  // Pick the nearest maturity (YYYYMM format lives in x?.months or x?.maturity)
  futs.sort((a,b) => (a.maturity||"").localeCompare(b.maturity||""));
  const best = futs[0];
  if (!best) return null;

  // Resolve snapshot to be safe
  ACTIVE_FUT.set(root, { conid: best.conid, symbol: best.localSymbol || best.symbol, maturity: best.maturity });
  return ACTIVE_FUT.get(root);
}
// Make a unique key for cache/nbbo – futures options don't have OCC; make our own
function toFopKey(root, futMaturityYYYYMM, optYYYYMMDD, rightCP, strike){
  return `${root} ${futMaturityYYYYMM} FOP ${optYYYYMMDD} ${rightCP} ${String(strike)}`;
}

async function resolveFopConid(ulOrRoot, expiryISO, right, strike) {
  const root = futRoot(ulOrRoot); // "/ES" -> "ES"

  // Use your front-month resolver so we’re anchored to the correct FUT
  const active = ACTIVE_FUT.get(root) || await ibActiveFuture(root);
  const futConid = active?.conid;
  if (!futConid) return null;

  const month = expiryISO.slice(0,4) + expiryISO.slice(5,7); // YYYYMM

  // FOP lives on GLOBEX
  const url = `${IB_BASE}/iserver/secdef/info?conid=${futConid}` +
              `&sectype=FOP&month=${month}&exchange=GLOBEX&right=${right}&strike=${strike}`;
  // const info = await ibFetchJson(url);
  const info = await ibFetch(url);
  const arr  = Array.isArray(info) ? info
             : Array.isArray(info?.Contracts) ? info.Contracts
             : info ? [info] : [];
  return arr.find(x => x?.conid)?.conid || null;
}
async function getFuturesUnderPx(root){
  const active = ACTIVE_FUT.get(root) || await ibActiveFuture(root);
  if (!active) return undefined;
  // const snap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${active.conid}&fields=31`);
  const snap = await ibFetch(`${IB_BASE}/iserver/marketdata/snapshot?conids=${active.conid}&fields=31`);
  return Number(Array.isArray(snap) && snap[0] && snap[0]["31"]);
}

async function getATRForFutures(root, len=14){
  const active = ACTIVE_FUT.get(root) || await ibActiveFuture(root);
  if (!active) return undefined;
  // const hist = await ibFetchJson(`${IB_BASE}/iserver/marketdata/history?conid=${active.conid}&period=2w&bar=1day&outsideRth=true`);
  const hist = await ibFetch(`${IB_BASE}/iserver/marketdata/history?conid=${active.conid}&period=2w&bar=1day&outsideRth=true`);
  const bars = hist?.data || hist?.points || hist?.bars || [];
  // compute ATR classic
  const trs = [];
  for (let i=1;i<bars.length;i++){
    const h = +bars[i].h ?? +bars[i].high, l = +bars[i].l ?? +bars[i].low, c1 = +bars[i-1].c ?? +bars[i-1].close;
    if (!Number.isFinite(h) || !Number.isFinite(l) || !Number.isFinite(c1)) continue;
    trs.push(Math.max(h-l, Math.abs(h-c1), Math.abs(l-c1)));
  }
  if (!trs.length) return undefined;
  const n = Math.min(len, trs.length);
  return trs.slice(-n).reduce((a,b)=>a+b,0)/n;
}
function esStrikeGrid(underPx){
  const grid = 5; // ES FOP strikes
  const atm  = Math.round(underPx / grid) * grid;
  return [-2,-1,0,1,2].map(i => atm + i*grid);
}

// Dedup + burst-friendly broadcast
function flushOptionQuotes() {
  if (!optionQuoteBuf.size) return;
  const data = Array.from(optionQuoteBuf.values());
  optionQuoteBuf.clear();
  wsBroadcast("option_quotes", data);
}
setInterval(flushOptionQuotes, OPTION_QUOTES_FLUSH_MS);

// Ingest a single live option quote (from your vendor handlers)
function ingestOptionQuote({ occ, bid, ask, mid, ts }) {
  if (!occ) return;
  const m = Number.isFinite(+bid) && Number.isFinite(+ask) ? (Number(bid)+Number(ask))/2 : Number(mid);
  optionQuoteBuf.set(occ, {
    occ,
    bid: Number.isFinite(+bid) ? Number(bid) : undefined,
    ask: Number.isFinite(+ask) ? Number(ask) : undefined,
    mid: Number.isFinite(+m)   ? Number(m)   : undefined,
    ts: ts || Date.now()
  });
}

// UL last seeding
function seedUL(symbol, price) {
  if (!symbol) return;
  const s = String(symbol).toUpperCase();
  if (Number.isFinite(+price)) latestUL.set(s, Number(price));
}

// yyyymmdd (UTC is fine for display; use ET if you prefer)
function asOfYMD(d = new Date()) {
  return d.toISOString().slice(0,10).replace(/-/g,"");
}

// OCC builder if your feed doesn’t give one
function occFromParts(ul, expiry, right, strike) {
  if (!ul || !expiry || !right || !Number.isFinite(+strike)) return null;
  const root = (ul + "      ").slice(0, 6);
  const yymmdd = String(expiry).replace(/-/g, "").slice(2, 8);
  const cp = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike) * 1000)).padStart(8, "0");
  return `${root}${yymmdd}${cp}${k}`;
}

function num(v) { const n = Number(v); return Number.isFinite(n) ? n : undefined; }
function normUL(x) { return String(x || "").trim().toUpperCase(); }
function toOptKey({ occWire, ul, expiry, right, strike }) {
  try {
    if (typeof optKey === "function") {
      if (occWire && typeof parseOCC === "function") {
        const p = parseOCC(occWire); // -> { ul, expiry, right, strike }
        if (p && p.ul && p.expiry && p.right && Number.isFinite(p.strike)) {
          return optKey(p.ul, p.expiry, p.right, p.strike);
        }
      }
      if (ul && expiry && right && Number.isFinite(strike)) {
        return optKey(ul, expiry, right, strike);
      }
    }
  } catch (_) {}
  return occWire; // graceful fallback
}

function normalizeOptionTrade(raw) {
  const ul     = normUL(raw.ul || raw.underlying || raw.ul_symbol || raw.symbol || raw.root || raw.underlyingSymbol);
  const rightR = String(raw.right || raw.r || "").toUpperCase();
  const sideR  = String(raw.side  || raw.action || "").toUpperCase();
  const strike = num(raw.strike ?? raw.k);
  const expiry = String(raw.expiry ?? raw.expiration ?? raw.exp ?? "");
  const qty    = num(raw.qty ?? raw.size ?? raw.quantity) || 0;
  const price  = num(raw.price ?? raw.px ?? raw.last ?? raw.trade_price ?? raw.bid ?? raw.ask) || 0;

  // canonical right first (needed for keys)
  const right  = (rightR === "C" || rightR === "CALL") ? "CALL" : "PUT";

  // book
  const bid = num(raw.bid);
  const ask = num(raw.ask);
  const mid = (Number.isFinite(bid) && Number.isFinite(ask)) ? (bid + ask) / 2 : num(raw.mid);

  // OCC wire & built
  const occWire  = raw.occ || undefined;
  const occBuilt = (ul && expiry && Number.isFinite(strike) && right)
    ? occFromParts(ul, expiry, right, strike)
    : undefined;
  const occHuman = occWire || occBuilt;

  // opaque option key for inference/caching
  const occKey = toOptKey({ occWire, ul, expiry, right, strike });

  // compute 'at' and inference
  const info = inferActionForOptionTrade({
    occ: occKey,                                   // opaque id expected by infer*
    qty,
    price,
    side: sideR,                                   // BUY/SELL expected
    book: {
      bid: Number.isFinite(bid) ? bid : undefined,
      ask: Number.isFinite(ask) ? ask : undefined,
      mid: Number.isFinite(mid) ? mid : undefined
    },
    cumulativeVol: num(raw.volume ?? raw.day_volume), // if your feed sends cumulative day volume
    oi: num(raw.oi),
    asOfYMD: asOfYMD()
  });

  // UL price at trade time (if given), else last known
  const ul_px = num(raw.ul_px ?? raw.underlyingPrice ?? raw.underlying_last);
  if (ul && Number.isFinite(ul_px)) seedUL(ul, ul_px);

  return {
    ul,
    right,
    strike,
    expiry,
    side: (sideR === "BUY" || sideR === "B") ? "BUY" : (sideR === "SELL" || sideR === "S") ? "SELL" : "UNKNOWN",
    qty,
    price,
    notional: Math.round(qty * price * 100),
    prints: num(raw.prints ?? raw.parts) || 1,
    ts: num(raw.ts ?? raw.time) || Date.now(),

    occ: occHuman,                                  // keep human-friendly OCC for UI/debug
    bid, ask, mid,
    at: info.at,                                    // "bid" | "ask" | "mid" | "between"
    action: info.action,                            // "BTO" | "BTC" | "STO" | "STC" | ...
    action_conf: info.action_conf,                  // "high" | "medium" | "low"
    reason: info.reason,

    oi: num(raw.oi) ?? null,
    priorVol: info.priorVol ?? null,
    volume: num(raw.volume ?? raw.day_volume) ?? null,
    ul_px: Number.isFinite(ul_px) ? ul_px : (ul ? latestUL.get(ul) : undefined)
  };
}
// function publishFlow(topic, rawRows) {
//   const rows = (Array.isArray(rawRows) ? rawRows : [rawRows]).map(normalizeOptionTrade);

//   // seed UL map from payloads & attach a small ul_prices map so clients can backfill
//   const ul_prices = {};
//   for (const r of rows) {
//     if (r.ul && Number.isFinite(r.ul_px)) ul_prices[r.ul] = r.ul_px;
//     else if (r.ul && latestUL.has(r.ul))  ul_prices[r.ul] = latestUL.get(r.ul);
//   }
//   wsBroadcast({ topic, data: rows, ul_prices });
// }

// // convenience wrappers
// export function publishBlocks(rawRows) { publishFlow("blocks", rawRows); }
// export function publishSweeps(rawRows) { publishFlow("sweeps", rawRows); }
// export function publishPrints(rawRows) { publishFlow("prints", rawRows); }
function publishFlow(topic, rawRows) {
  const rows = (Array.isArray(rawRows) ? rawRows : [rawRows]).map(normalizeOptionTrade);

  // seed UL map from payloads & attach a small ul_prices map so clients can backfill
  const ul_prices = {};
  for (const r of rows) {
    if (r.ul && Number.isFinite(r.ul_px)) {
      ul_prices[r.ul] = r.ul_px;
    } else if (r.ul && latestUL.has(r.ul)) {
      ul_prices[r.ul] = latestUL.get(r.ul);
    }
  }

  // send as { topic, data: { rows, ul_prices } }
  wsBroadcast(topic, { rows, ul_prices });
}

// convenience wrappers
export function publishBlocks(rawRows) { publishFlow("blocks", rawRows); }
export function publishSweeps(rawRows) { publishFlow("sweeps", rawRows); }
export function publishPrints(rawRows) { publishFlow("prints", rawRows); }

function wsSend(ws, topic, data) {
  try { ws.send(JSON.stringify({ topic, data })); } catch {}
}

function wsBroadcast(topic, data) {
  const payload = JSON.stringify({ topic, data });
  if (!wsReady || !wss) { WS_QUEUE.push(payload); return; }
  for (const c of wss.clients) if (c.readyState === 1) c.send(payload);
}
const DEMO = String(process.env.DEMO || "").trim() === "1";
setBroadcaster(wsBroadcast);
// Kickoff loops (tune cadences as desired)
async function startIBLoops() {
  try { await ibPrimeSession(); } catch {}

  // Warm up ES front month if needed
  try { await resolveFrontMonthES(); } catch {}

  // Pollers
  setInterval(() => { pollEquitiesOnce().catch(()=>{}); }, 1_500);  // ~0.7 Hz
  setInterval(() => { pollOptionsOnce().catch(()=>{});  }, 1_500);  // ~0.4 Hz

  // Your existing synthesizer timers are already running (every 1s / 5s)
  console.log("[IB] loops started.");
}
// Refresh NBBO for all known option keys without rebuilding chains
async function refreshOptionNBBOFast() {
  try {
    // Collect known option keys → conids we already resolved
    const keys = Array.from(NBBO_OPT.keys());
    if (!keys.length) return;

    // If you cached conids per key, use that. If not, skip (still safe).
    const conids = [];
    for (const k of keys) {
      const meta = cacheOptConid.get?.(k) || null; // if you put occ→conid here
      if (meta?.conid) conids.push(meta.conid);
    }
    if (!conids.length) return;

    const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids.join(","))}&fields=84,86`);
    const arr = Array.isArray(snaps) ? snaps : [];
    const byConid = new Map(arr.map(r => [String(r.conid), r]));

    for (const k of keys) {
      const meta = cacheOptConid.get?.(k);
      if (!meta?.conid) continue;
      const s = byConid.get(String(meta.conid));
      if (!s) continue;
      const bid = Number(s["84"]), ask = Number(s["86"]);
      if (Number.isFinite(bid) || Number.isFinite(ask)) {
        NBBO_OPT.set(k, { bid: bid || 0, ask: ask || 0, ts: Date.now() });
        ingestOptionQuote({
          occ: k,
          bid: Number.isFinite(bid) ? bid : undefined,
          ask: Number.isFinite(ask) ? ask : undefined,
          mid: (Number.isFinite(bid)&&Number.isFinite(ask)&&bid>0&&ask>0) ? (bid+ask)/2 : undefined,
          ts: Date.now()
        });
      }
    }
  } catch (_) {}
}

// run it at the same cadence as equities
setInterval(() => { refreshOptionNBBOFast().catch(()=>{}); }, 1_500);

startIBLoops().catch(()=>{});
// Flush anything that tried to broadcast before WS stood up
for (const payload of WS_QUEUE.splice(0)) {
  for (const c of wss.clients) if (c.readyState === 1) c.send(payload);
}

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
      safeBroadcastNotables(immediate.notables || []);
    }
  }, 250); // ~4Hz
}
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
  const isFut = (msg.ul||msg.symbol||"").toUpperCase().replace(/^\//,"") === "ES";
  const premMin = isFut ? Math.max(BIG_PREMIUM_MIN, 250_000) : BIG_PREMIUM_MIN;
  if (Number.isFinite(notional) && notional >= premMin) return true;

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
  // const occ = toOcc(ul, expiration, rightToCP(right), strike);
  const k = optKey(ul, expiration, rightToCP(right), strike);
  DAY_STATS_OPT.set(k, {
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

function refreshBlocks() {
  const rows = computeBlocks(9999, 50_000)
    .filter(passesPublishGate)
    .slice(0, 50)
    .map(r => ({ ...r, ul_px: (getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null) }))
    .map(withMark);

  publishBlocks(rows); // 🔔 push to subscribers
}

function refreshSweeps() {
  const rows = computeSweeps(9999, 25_000)
    .filter(passesPublishGate)
    .slice(0, 200)
    .map(r => attachDayStats({
      ...r,
      ul_px: (getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null)
    }))
    .map(withMark);

  publishSweeps(rows); // 🔔 push to subscribers
}

// run on an interval or after each NBBO/tape update
setInterval(() => {
  refreshBlocks();
  refreshSweeps();
}, 5_000);

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
function fixEsSpxCollision(pairs){
  const es = pairs.find(p => p.sym === "ES");
  const spx = pairs.find(p => p.sym === "SPX");
  if (es && spx && String(es.conid) === String(spx.conid)) {
    // bad: ES got the SPX index conid; fix & re-push ES
    CACHE.conidBySym.delete("ES");
    FUT_FRONT.delete("ES");
  }
}
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
// async function ibPrimeSession(){
//   if (MOCK){ CACHE.sessionPrimed = true; return; }
//   const now = Date.now();
//   if (CACHE.sessionPrimed) return;
//   if (CACHE.primingSince && (now - CACHE.primingSince) < 1000) return;
//   CACHE.primingSince = now;
//   try { await ibFetch("/iserver/auth/status"); } catch(e){}
//   try {
//     await ibFetch("/iserver/accounts");
//     CACHE.sessionPrimed = true;
//     console.log("[IB] session primed.");
//   } catch { await sleep(300); try { await ibFetch("/iserver/accounts"); CACHE.sessionPrimed = true; } catch {} }
// }
async function ibPrimeSession(){
  if (MOCK){ CACHE.sessionPrimed = true; return; }
  if (CACHE.sessionPrimed) return;
  if (CACHE.primingSince && (Date.now() - CACHE.primingSince) < 1000) return;
  CACHE.primingSince = Date.now();
  try {
    // Step 0: optional SSO validate (captures cookie on some setups)
    try { await ibFetch("/sso/validate"); } catch {}

    // Step 1: auth status warms session & may set cookies
    await ibFetch("/iserver/auth/status");

    // Step 2: select account context
    await ibFetch("/iserver/accounts");

    // Step 3: heartbeats (keep cookie/session fresh)
    setInterval(() => ibFetch("/tickle").catch(()=>{}), 90_000);

    CACHE.sessionPrimed = true;
    console.log("[IB] session primed.");
  } catch (e) {
    console.warn("[IB] prime failed:", e?.message || e);
  }
}
async function ibFetch(path, opts={}, _retry=false){
  if (MOCK) throw new Error("MOCK mode");
  if (!CACHE.sessionPrimed) await ibPrimeSession();
  const url = path.startsWith("http") ? path : `${IB_BASE}${path}`;
  const res = await ibQueued(path, async ()=>{
    let r;
    try { r = await fetch(url, { ...opts, redirect:"follow" }); }
    catch (e) { throw new Error(`IB fetch failed: ${url} :: ${e.message}`); }

    captureCookies(r); // <-- NEW

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
async function pollEquitiesOnce() {
  try {
    // 1) Normalize requested symbols up front
    const symbols = Array.from(WATCH.equities || []).map(normalizeSymbol);
    if (!symbols.length) return;

    // 2) Resolve conids for each (normalize BEFORE ensureConid)
    const pairs = [];
    for (const sym of symbols) {
      try {
        const conid = await ensureConid(sym); // ES (or /ES) => front-month
        if (conid) pairs.push({ sym: normalizeSymbol(sym), conid });
      } catch {}
    }
    if (!pairs.length) return;

    // (optional) any dedupe/collision logic you want
    fixEsSpxCollision(pairs);

    // 3) Build rows (always normalized)
    let rows = [];
    if (MOCK) {
      rows = pairs.map(p => ({
        symbol: normalizeSymbol(p.sym),
        last: 100 + Math.random() * 50,
        bid: 0, ask: 0,
        iv: p.sym === "MSFT" ? 0.40 : 1.00,
        ts: Date.now()
      }));
    } else {
      const conids = pairs.map(p => p.conid).join(",");
      // 31: last, 84: bid, 86: ask, 7059: IV
      const snaps = await ibFetch(
        `/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`
      );
      for (const p of pairs) {
        const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid) === String(p.conid)) : null;
        if (!s) continue;
        // Force canonical symbol on the mapped row
        const r = safeMapEquity(normalizeSymbol(p.sym), s);
        rows.push(r);
      }
    }

    // 4) Compute ES–SPX basis using canonical keys
    let es_spx_basis = null;
    const bySym = new Map(rows.map(r => [normalizeSymbol(r.symbol), r]));
    const ES_KEY  = normalizeSymbol("ES");   // e.g., "/ES" if you canonicalize with slash
    const SPX_KEY = normalizeSymbol("SPX");  // stays "SPX"

    const es  = bySym.get(ES_KEY);
    const spx = bySym.get(SPX_KEY);

    if (es && spx && fresh(es) && fresh(spx)) {
      const esPx  = midOrLast(es);
      const spxPx = midOrLast(spx);
      if (Number.isFinite(esPx) && Number.isFinite(spxPx)) {
        es_spx_basis = Number((esPx - spxPx).toFixed(2));
      }
    }

    // 5) Update tick caches regardless of basis
    for (const r of rows) if (Number.isFinite(r?.last)) await onTick(r.symbol, r.last);
    for (const r of rows) if (Number.isFinite(r?.last)) updateUnderlyingLast(r.symbol, r.last, r.ts ?? Date.now());

    // 6) Persist & broadcast (rows already canonical)
    rows = rows.map(r => ({ ...r, symbol: normalizeSymbol(r.symbol) }));
    STATE.equities_ts = rows;
    assertCanonicalRows("equity_ts", rows); // temp debug logger
    wsBroadcast("equity_ts", rows);
    if (es_spx_basis != null) wsBroadcast("basis", { es_spx_basis });
  } catch (e) {
    console.warn("pollEquitiesOnce:", e?.message ?? e);
  }
}
// async function pollEquitiesOnce(){
//   try{
//     const symbols = Array.from(WATCH.equities || []).map(normalizeSymbol);
//     if (!symbols.length) return;

//     const pairs = [];
//     for (const sym of symbols){
//       try {
//         const conid = await ensureConid(sym); // ES should resolve to front-month
//         if (conid) pairs.push({ sym, conid });
//       } catch {}
//     }
//     if (!pairs.length) return;
//     fixEsSpxCollision(pairs);
//     let rows = [];
//     if (MOCK){
//       rows = pairs.map(p => ({
//         symbol: p.sym,
//         last: 100 + Math.random()*50,
//         bid: 0, ask: 0, iv: p.sym==="MSFT" ? 0.40 : 1.00,
//         ts: Date.now()
//       }));
//     }else{
//       const conids = pairs.map(p=>p.conid).join(",");
//       // 31: last, 84: bid, 86: ask, 7059: IV
//       const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
//       for (const p of pairs){
//         const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid)===String(p.conid)) : null;
//         if (!s) continue;
//         // rows.push(mapEquitySnapshot(p.sym, s));
//         rows.push(safeMapEquity(p.sym, s));
//       }
//     }
//     // ----- compute ES–SPX basis (points) -----
//     let es_spx_basis = null;
//     const bySym = new Map(rows.map(r => [normalizeSymbol(r.symbol), r]));
//     const es = bySym.get("ES");
//     const spx = bySym.get("SPX");
//     // if (es && spx && fresh(es) && fresh(spx)) {
//     //   const esPx  = midOrLast(es);
//     //   const spxPx = midOrLast(spx);
//     //   if (Number.isFinite(esPx) && Number.isFinite(spxPx)) {
//     //     es_spx_basis = Number((esPx - spxPx).toFixed(2));
//     //   }
      
//     //   for (const r of rows) if (Number.isFinite(r?.last)) await onTick(r.symbol, r.last);
//     //   // broadcast the richer payload your UI expects
//     //   // in pollEquitiesOnce(), right after you build `rows`
//     //   for (const r of rows) {
//     //     if (Number.isFinite(r?.last)) {
//     //       updateUnderlyingLast(r.symbol, r.last, r.ts ?? Date.now()); // <— NEW
//     //     }
//     //   }      
//     //   STATE.equities_ts = rows;
//     //   // wsBroadcast("equity_ts", { rows, es_spx_basis });
//     //   wsBroadcast("equity_ts", rows);
//     //   if (es_spx_basis != null) wsBroadcast("basis", { es_spx_basis });      
//     // }
//    if (es && spx && fresh(es) && fresh(spx)) {
//      const esPx  = midOrLast(es);
//      const spxPx = midOrLast(spx);
//      if (Number.isFinite(esPx) && Number.isFinite(spxPx)) {
//        es_spx_basis = Number((esPx - spxPx).toFixed(2));
//      }
//    }
//    // Always keep last prices + broadcast quotes, independent of SPX presence
//    for (const r of rows) if (Number.isFinite(r?.last)) await onTick(r.symbol, r.last);
//    for (const r of rows) if (Number.isFinite(r?.last)) updateUnderlyingLast(r.symbol, r.last, r.ts ?? Date.now());
//    STATE.equities_ts = rows;

//   //  const data = ticks.map(t => ({ ...t, symbol: normalizeSymbol(t.symbol) }));
//    assertCanonicalRows('equity_ts', rows)
//    wsBroadcast("equity_ts", rows);
//    if (es_spx_basis != null) wsBroadcast("basis", { es_spx_basis });    
//   }catch(e){
//     console.warn("pollEquitiesOnce:", e.message);
//   }
// }

const rightToCPSafe = (r) =>
  r === "C" || r === "P" ? r : (r === "CALL" ? "C" : (r === "PUT" ? "P" : r));
function withMark(row){
  try {
    // const occ  = toOcc(row.ul, row.expiry, rightToCP(row.right), row.strike);
    const k = optKey(row.ul, row.expiry, row.right, row.strike);
    const q    = NBBO_OPT.get?.(k);
    const bid  = q?.bid, ask = q?.ask;

    // Prefer explicit mid if you store it
    const mid  = Number.isFinite(q?.mid) ? q.mid
               : (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0)
                 ? (bid + ask) / 2
                 : undefined;

    return { ...row, mark: mid };
  } catch {
    return { ...row, mark: undefined };
  }
}
app.get("/api/flow/blocks", (req,res)=>{
  const minNotional = Number(req.query.minNotional ?? 50_000);
  const limit       = Number(req.query.limit ?? 50);

  const rows = computeBlocks(9999, minNotional)
    .filter(passesPublishGate)
    .slice(0, limit)
    .map(r => ({
      ...r,
      ul_px: getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null
    }))
    .map(withMark);

  // Optional: only broadcast when requested
  if (req.query.publish === "1") publishBlocks(rows);

  res.json({ rows, ts: Date.now() });
});

app.get("/api/flow/sweeps", (req,res)=>{
  const minNotional = Number(req.query.minNotional ?? 25_000);
  const limit       = Number(req.query.limit ?? 200);

  const rows = computeSweeps(9999, minNotional)
    .filter(passesPublishGate)
    .slice(0, limit)
    .map(r => ({
      ...r,
      ul_px: getUnderlyingLastNear(r.ul, r.ts) ?? getLast(r.ul) ?? null
    }))
    .map(attachDayStats)   // stats after ul_px
    .map(withMark);

  if (req.query.publish === "1") publishSweeps(rows);

  res.json({ rows, ts: Date.now() });
});
app.post('/api/flow/headlines/publish', (req, res) => {
  const out = [];
  for (const r of req.body.rows ?? []) {
    const row = { ...r, ul: normalizeSymbol(r.ul) };
    // (optional) row.asset_class ??= guessFromUL(row.ul);
    store.headlines.push(row);
    out.push(row);
  }
  // broadcast by canonical UL
  for (const row of out) broadcast('headlines', [row]); // send array or your shape
  res.json({ ok: true, count: out.length, dropped: 0, reasons: {} });
});
app.post("/api/flow/prints/publish", (req,res)=>{
  const rows = (req.body?.rows ?? [])
    .filter(passesPublishGate)
    .map(p => ({ ...p, ul_px: getUnderlyingLastNear(p.ul, p.ts) ?? getLast(p.ul) ?? null }))
    .map(withMark);

  publishPrints(rows);
  res.json({ ok:true, count: rows.length });
});
app.get("/debug/snapshot_raw", async (req,res)=>{
  try {
    const conids = String(req.query.conids||"").trim();
    if (!conids) return res.status(400).json({ error:"conids required" });
    const body = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
    res.json(body);
  } catch(e){ res.status(500).json({ error:e.message }); }
});

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

  // const occ = toOcc(ul, expiry, rightToCP(right), strike);
  const occ = optKey(ul, expiry, rightToCP(right), strike);
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

  // const occ = toOcc(ul, expiry, rightToCP(right), strike);
  const occ = optKey(ul, expiry, right, strike);
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
      // const occ = toOcc(msg.symbol, msg?.option?.expiration, rightToCP(msg?.option?.right), msg?.option?.strike);
      const occ = optKey(msg.symbol, msg?.option?.expiration, rightToCP(msg?.option?.right), msg?.option?.strike);
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
    scheduleNotables();
  } catch (_) {}

  // 🔒 NEW: apply publish gate to sweeps/blocks globally
  if (msg?.type === "sweeps" || msg?.type === "blocks") {
    if (!passesPublishGate(msg)) return;   // drop quietly
  }
  // normalize futures UL to "/ES" for UI consistency
  if ((msg.ul || msg.symbol) === "ES") {
    msg.ul = "/ES";
    msg.symbol = "/ES";
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
    // res.json(payload);
    res.json({ rows: payload.notables ?? [], ts: Date.now() });
  } catch (e) {
    res.status(500).json({ error: e?.message || 'fail' });
  }
});
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
// setInterval(() => ibFetchJson(`${IB_BASE}/tickle`).catch(()=>{}), 90000);
setInterval(() => ibFetch(`${IB_BASE}/tickle`).catch(()=>{}), 90000);

function optKey(ul, expiration, right /* "C"|"P" or "CALL"/"PUT" */, strike) {
  const cp = rightToCP(right);
  const u  = String(ul).toUpperCase();
  if (u === "ES" || u === "/ES" || FUT_ROOTS.has(u.replace(/^\//,""))) {
    const root = futRoot(u);
    // Use the active maturity YYYYMM if you have it; else fallback
    const active = ACTIVE_FUT.get(root);
    const futYYYYMM = (active?.maturity || "").slice(0,6);
    return toFopKey(root, futYYYYMM, expiration, cp === "C" ? "CALL" : "PUT", strike);
  }
  return toOcc(u, expiration, cp, strike);
}
async function ibFopMonthsForRoot(root) {
  // Try secdef search scoped to FUT first
  const fut = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(root)}&sectype=FUT&exchange=GLOBEX`).catch(()=>null);
  const list = Array.isArray(fut) ? fut : (fut ? [fut] : []);
  const months = new Set();

  for (const r of list) {
    const m = (r?.maturity || r?.months || "").toString(); // e.g. "202512"
    if (/^\d{6}$/.test(m)) months.add(m);
    // Some IB payloads expose semicolon lists in sections; sweep them too:
    for (const s of (r?.sections || [])) {
      if (s?.months) for (const tok of String(s.months).split(";")) if (/^\d{6}$/.test(tok)) months.add(tok);
    }
  }

  const out = Array.from(months).sort();      // ascending
  return out.slice(0, 2);                     // nearest 2
}

function monthToApproxISO(yyyymm) {
  const y = +yyyymm.slice(0,4), m0 = +yyyymm.slice(4,6) - 1;
  // 3rd Friday in that month (UTC)
  const d = new Date(Date.UTC(y, m0, 1));
  while (d.getUTCDay() !== 5) d.setUTCDate(d.getUTCDate() + 1); // first Friday
  d.setUTCDate(d.getUTCDate() + 14); // +2 weeks = third Friday
  const z = (n)=>String(n).padStart(2,"0");
  return `${d.getUTCFullYear()}-${z(d.getUTCMonth()+1)}-${z(d.getUTCDate())}`;
}

export async function pollOptionsOnce() {
  let currentUL = null; // context for logging
  try {
    // --------- Build union of underlyings safely ----------
    const ulSet = new Set();
    try {
      if (!CACHE.sessionPrimed) { await ibPrimeSession(); if (!CACHE.sessionPrimed) return; }
        // ... rest of your logic      
      const eqsRaw = (typeof normEquities === "function" ? normEquities(WATCH) : []) || [];
      const eqs = eqsRaw.map((s) => String(s || "").toUpperCase()).filter(Boolean);
      for (const s of eqs) ulSet.add(s);

      const opts = (typeof normOptions === "function" ? normOptions(WATCH) : []) || [];
      for (const o of opts) {
        const u = (o && o.underlying != null) ? String(o.underlying).toUpperCase() : "";
        if (u) ulSet.add(u);
      }
    } catch (e) {
      console.warn("pollOptionsOnce: failed to build underlying set", e && (e.message || e));
      return;
    }

    const underlyings = Array.from(ulSet);
    if (!underlyings.length) return;

    const ticks = [];

    // --------- Per-underlying loop ----------
    for (const ul of underlyings) {
      currentUL = ul;
      const sym = canon(ul);
      // if (isFutureSym(sym)) {
      if (isFutures(sym)) {  
        // === FUTURES OPTIONS PATH (FOP) ===
        const root = futRoot(sym);       // "/ES" -> "ES"
        const active = await ibActiveFuture(root); if (!active) continue;

        const underPx = await getFuturesUnderPx(root);
        if (!Number.isFinite(underPx)) continue;

        await onTick(root, underPx);     // store last; your onTick should accept "ES" as key
        updateUnderlyingLast(root, underPx, Date.now()); // <-- add this

        // Option expiry handling:
        // For FOP, IB uses the option month YYYYMM too; you can start by using the
        // same “first two months” approach as equities but anchored to the FUT maturity month.
        // Simpler: just probe the next 2 calendar months from today:
        const optMonths = await ibFopMonthsForRoot(root); // implement like ibOptMonthsForSymbol but sectype=FOP
        const months = (optMonths || []).slice(0, 2);     // ["202511","202512"] for example

        const strikes = esStrikeGrid(underPx);

        for (const month of months) {
          // If you need exact option expiry date (YYYY-MM-DD), you can fetch the chain
          // and read expiry per contract; for now, keep a placeholder "YYYY-MM-??" or
          // set "expiry" to a best known third Friday date finder.
          const expiryISO = monthToApproxISO(month); // e.g., "2025-12-20" (approx)

          for (const right of ["C","P"]) {
            for (const strike of strikes) {
              try {
                // FOP conid lookup
                const optConid = await resolveFopConid(root, expiryISO, right, strike);//resolveFOPConid(root, active.conid, month, right, strike);
                if (!optConid) {
                  console.warn("FOP resolve miss", { root, expiryISO, right, strike });
                  return;
                }                
                if (!optConid) continue;

                // Snapshot for NBBO + day stats
                // const osnap = await ibFetchJson(
                //   `${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86,7295,7635`
                // );
                const osnap = await ibFetch(
                  `${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86,7295,7635`
                );                
                const s0 = Array.isArray(osnap) && osnap[0] ? osnap[0] : {};

                // NBBO cache with FOP key
                // const key = toFopKey(root, active.maturity?.slice(0,6) || month, expiryISO, right==="C"?"CALL":"PUT", strike);
                const key = optKey(root, expiryISO, right, strike);
                const bid0 = Number(s0["84"]), ask0 = Number(s0["86"]);
                if (Number.isFinite(bid0) || Number.isFinite(ask0)) {
                  NBBO_OPT.set(key, { bid: bid0 || 0, ask: ask0 || 0, ts: Date.now() });
                  ingestOptionQuote({
                    occ: key, // reuse field name 'occ' in your code paths
                    bid: Number.isFinite(bid0) ? bid0 : undefined,
                    ask: Number.isFinite(ask0) ? ask0 : undefined,
                    mid: (Number.isFinite(bid0)&&Number.isFinite(ask0)&&bid0>0&&ask0>0) ? (bid0+ask0)/2 : undefined,
                    ts: Date.now(),
                  });
                }

                // UI tick row (use your mapper or a FOP variant)
                ticks.push(mapOptionTick(root, expiryISO, strike, right, s0)); // keep the same shape you expect

                // Day stats for inference
                updateDayStats(root, expiryISO, right, strike, {
                  volume: toNumStrict(s0["7295"]),
                  oi:     toNumStrict(s0["7635"]),
                });

                // Build synthetic prints from 1m bars (same as before)
                let bars=null;
                try{
                  // bars = await ibFetchJson(`${IB_BASE}/iserver/marketdata/history?conid=${optConid}&period=1d&bar=1min&outsideRth=true`);
                  bars = await ibFetch(`${IB_BASE}/iserver/marketdata/history?conid=${optConid}&period=1d&bar=1min&outsideRth=true`);
                }catch{}
                const list = bars?.data || bars?.points || bars?.bars || [];
                if (Array.isArray(list) && list.length){
                  const lastSeen = (LAST_BAR_OPT.get(optConid) || {}).t || 0;
                  const tail = list.slice(-5);
                  for (const b of tail){
                    const t = Number(b.t || b.time || Date.parse(b.timestamp||"")) || 0;
                    if (!t || t <= lastSeen) continue;

                    const c = Number(b.c || b.close || b.price || b.last || 0);
                    const v = Number(b.v || b.volume || 0);
                    if (!(v>0 && Number.isFinite(c))) { LAST_BAR_OPT.set(optConid, { t, c, v }); continue; }

                    // refresh light snapshot (optional)
                    let bid = bid0, ask = ask0, cumVol = toNumStrict(s0["7295"]), oiOpt = toNumStrict(s0["7635"]);
                    try {
                      // const lite = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86,7295,7635`);
                      const lite = await ibFetch(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86,7295,7635`);
                      const r = Array.isArray(lite) && lite[0] ? lite[0] : {};
                      bid   = Number.isFinite(+r["84"]) ? +r["84"] : bid;
                      ask   = Number.isFinite(+r["86"]) ? +r["86"] : ask;
                      cumVol= Number.isFinite(+r["7295"]) ? +r["7295"] : cumVol;
                      oiOpt = Number.isFinite(+r["7635"]) ? +r["7635"] : oiOpt;
                    } catch {}

                    const nbbo = NBBO_OPT.get(key) || {};
                    if (!Number.isFinite(bid) && Number.isFinite(nbbo.bid)) bid = nbbo.bid;
                    if (!Number.isFinite(ask) && Number.isFinite(nbbo.ask)) ask = nbbo.ask;
                    const mid = (Number.isFinite(bid)&&Number.isFinite(ask)&&bid>0&&ask>0) ? (bid+ask)/2 : undefined;

                    // Normalize like before; keep fields your inferer expects
                    let msg = normOptionsPrint({
                      ul: root, exp: expiryISO, right, strike, price: c, qty: v, nbbo: { bid, ask }
                    });
                    msg.type = "print";

                    // Infer action
                    try{
                      const inf = inferActionForOptionTrade({
                        occ: key, qty: v, price: c, side: msg.side || "UNKNOWN",
                        book: { bid, ask, mid },
                        cumulativeVol: Number.isFinite(cumVol) ? cumVol : undefined,
                        oi: Number.isFinite(oiOpt) ? oiOpt : undefined,
                        asOfYMD: etYMD(),
                      });
                      msg = { ...msg, ...inf };
                    }catch{}

                    pushAndFanout(msg);
                    maybePublishSweepOrBlock(msg);

                    // AUTOTRADE: use **futures ATR** and **futures last**:
                    await autoTradeOnFlow(msg, underPx); // your existing function works; see §6 tweak

                    LAST_BAR_OPT.set(optConid, { t, c, v });
                  }
                }
              }catch(err){
                console.warn("FOP leg error", { root, month, right, strike, err: err?.message || err });
              }
            }
          }
        }
        continue; // done with /ES path
      }

      try {
        const conid = await ibConidForSymbol(ul);
        if (!conid) continue;

        const snap = MOCK
          ? [{ "31": 100 + Math.random() * 50 }]
          // : await ibFetchJson(IB_BASE + "/iserver/marketdata/snapshot?conids=" + conid + "&fields=31");
          : await ibFetch(IB_BASE + "/iserver/marketdata/snapshot?conids=" + conid + "&fields=31");

        const underPxRaw = (Array.isArray(snap) && snap[0]) ? snap[0]["31"] : NaN;
        const underPx = Number(underPxRaw);
        if (!Number.isFinite(underPx)) continue;

        // equity last cache so ATM builder works immediately
        try { if (typeof onTick === "function") await onTick(ul, underPx); } catch (e) {
          console.warn("pollOptionsOnce: onTick error", { ul: ul, err: e && (e.message || e) });
        }

        // expiries
        let expiries = [];
        try {
          const months = await ibOptMonthsForSymbol(ul);
          expiries = (months || []).slice(0, 2);
        } catch (e) {
          console.warn("pollOptionsOnce: ibOptMonthsForSymbol error", { ul: ul, err: e && (e.message || e) });
          continue;
        }

        const grid = underPx < 50 ? 1 : 5;
        const atm  = Math.round(underPx / grid) * grid;
        const rel  = [-2, -1, 0, 1, 2].map(function (i) { return atm + i * grid; });

        // --------- Per-expiry loop ----------
        for (const expiry of expiries) {
          const yyyy = String(expiry).slice(0, 4);
          const mm   = String(expiry).slice(5, 7);
          const month = yyyy + mm;

          // --- wrap the whole expiry body in an async IIFE + .catch() ---
          await (async function handleExpiry() {
            for (const right of ["C", "P"]) {
              for (const strike of rel) {
                // --- each leg also wrapped to avoid try/catch keywords ---
                await (async function handleLeg() {
                  let s0;
                  let optConid = null;

                  if (MOCK) {
                    s0 = {
                      "31": Math.max(0.05, Math.random() * 20),
                      "84": Math.max(0.01, Math.random() * 20 - 0.1),
                      "86": Math.random() * 20 + 0.1,
                      "7295": Math.floor(Math.random() * 5000),
                      "7635": Math.floor(Math.random() * 20000),
                    };
                  } else {
                    const infoUrl = IB_BASE +
                      "/iserver/secdef/info?conid=" + conid +
                      "&sectype=OPT&month=" + month +
                      "&exchange=SMART&right=" + right +
                      "&strike=" + strike;

                    // const info = await ibFetchJson(infoUrl);
                    const info = await ibFetch(infoUrl);
                    const arr  = Array.isArray(info) ? info
                              : (info && Array.isArray(info.Contracts)) ? info.Contracts
                              : info ? [info] : [];
                    const found = arr.find(function (x) { return x && x.conid; });
                    optConid = found ? found.conid : null;
                    if (!optConid) return; // continue
                    // const osnap = await ibFetchJson(
                    //   IB_BASE + "/iserver/marketdata/snapshot?conids=" + optConid + "&fields=31,84,86,7295,7635"
                    // );
                    const osnap = await ibFetch(
                      IB_BASE + "/iserver/marketdata/snapshot?conids=" + optConid + "&fields=31,84,86,7295,7635"
                    );                    
                    s0 = (Array.isArray(osnap) && osnap[0]) ? osnap[0] : {};
                  }

                  const occ  = optKey(ul, expiry, rightToCP(right), strike); //toOcc(ul, expiry, rightToCP(right), strike);
                  const bid0 = Number(s0 && s0["84"]);
                  const ask0 = Number(s0 && s0["86"]);

                  if (Number.isFinite(bid0) || Number.isFinite(ask0)) {
                    NBBO_OPT.set(occ, { bid: bid0 || 0, ask: ask0 || 0, ts: Date.now() });
                    if (typeof ingestOptionQuote === "function") {
                      ingestOptionQuote({
                        occ: occ,
                        bid: Number.isFinite(bid0) ? bid0 : undefined,
                        ask: Number.isFinite(ask0) ? ask0 : undefined,
                        mid: (Number.isFinite(bid0) && Number.isFinite(ask0) && bid0 > 0 && ask0 > 0)
                          ? (bid0 + ask0) / 2 : undefined,
                        ts: Date.now()
                      });
                    }
                  }

                  // UI tick row
                  ticks.push(mapOptionTick(ul, expiry, strike, right, s0));

                  // day stats
                  if (typeof updateDayStats === "function") {
                    updateDayStats(ul, expiry, right, strike, {
                      volume: toNumStrict(s0 && s0["7295"]),
                      oi:     toNumStrict(s0 && s0["7635"])
                    });
                  }

                  if (!MOCK && optConid) {
                    let bars = null;
                    try {
                      // bars = await ibFetchJson(
                      //   IB_BASE + "/iserver/marketdata/history?conid=" + optConid + "&period=1d&bar=1min&outsideRth=true"
                      // );
                      bars = await ibFetch(
                        IB_BASE + "/iserver/marketdata/history?conid=" + optConid + "&period=1d&bar=1min&outsideRth=true"
                      );                      
                    } catch (e) { /* ignore */ }

                    const list = (bars && (bars.data || bars.points || bars.bars)) || [];
                    if (Array.isArray(list) && list.length) {
                      const lastSeenObj = LAST_BAR_OPT.get(optConid) || {};
                      const lastSeen    = lastSeenObj.t || 0;
                      const tail        = list.slice(-5);

                      for (const b of tail) {
                        const t = Number(b && (b.t || b.time || Date.parse(b.timestamp || ""))) || 0;
                        if (!t || t <= lastSeen) continue;

                        const c = Number(b && (b.c || b.close || b.price || b.last || 0));
                        const v = Number(b && (b.v || b.volume || 0));
                        if (!(v > 0 && Number.isFinite(c))) {
                          LAST_BAR_OPT.set(optConid, { t: t, c: c, v: v });
                          continue;
                        }

                        // refresh lite book
                        let bid = bid0, ask = ask0;
                        let cumVol = toNumStrict(s0 && s0["7295"]);
                        let oiOpt  = toNumStrict(s0 && s0["7635"]);
                        try {
                          // const lite = await ibFetchJson(
                          //   IB_BASE + "/iserver/marketdata/snapshot?conids=" + optConid + "&fields=31,84,86,7295,7635"
                          // );
                          const lite = await ibFetch(
                            IB_BASE + "/iserver/marketdata/snapshot?conids=" + optConid + "&fields=31,84,86,7295,7635"
                          );                          
                          const r = (Array.isArray(lite) && lite[0]) ? lite[0] : {};
                          if (Number.isFinite(+r["84"]))   bid    = +r["84"];
                          if (Number.isFinite(+r["86"]))   ask    = +r["86"];
                          if (Number.isFinite(+r["7295"])) cumVol = +r["7295"];
                          if (Number.isFinite(+r["7635"])) oiOpt  = +r["7635"];
                        } catch (e) { /* keep s0 */ }

                        const cached = NBBO_OPT.get(occ);
                        if (!Number.isFinite(bid) && cached && Number.isFinite(cached.bid)) bid = cached.bid;
                        if (!Number.isFinite(ask) && cached && Number.isFinite(cached.ask)) ask = cached.ask;
                        const mid = (Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0)
                          ? (bid + ask) / 2 : undefined;

                        let msg = normOptionsPrint({
                          ul: ul, exp: expiry, right: right, strike: strike,
                          price: c, qty: v, nbbo: { bid: bid, ask: ask }
                        });
                        msg.type   = "print";
                        msg.ul     = msg.ul     || ul;
                        msg.right  = msg.right  || right;
                        msg.strike = (msg.strike !== undefined ? msg.strike : strike);
                        msg.expiry = msg.expiry || expiry;

                        try {
                          if (typeof inferActionForOptionTrade === "function") {
                            const inf = inferActionForOptionTrade({
                              occ: occ,
                              qty: v,
                              price: c,
                              side: msg.side || "UNKNOWN",
                              book: { bid: bid, ask: ask, mid: mid },
                              cumulativeVol: Number.isFinite(cumVol) ? cumVol : undefined,
                              oi: Number.isFinite(oiOpt) ? oiOpt : undefined,
                              asOfYMD: etYMD()
                            });
                            msg = Object.assign({}, msg, inf);
                          }
                        } catch (e) { /* keep raw */ }

                        if (typeof pushAndFanout === "function") pushAndFanout(msg);
                        if (typeof maybePublishSweepOrBlock === "function") maybePublishSweepOrBlock(msg);
                        try {
                          if (typeof autoTradeOnFlow === "function") await autoTradeOnFlow(msg, underPx);
                        } catch (e) {
                          console.warn("pollOptionsOnce: autoTradeOnFlow error", { ul: ul, err: e && (e.message || e) });
                        }

                        LAST_BAR_OPT.set(optConid, { t: t, c: c, v: v });
                      }
                    }
                  }
                })().catch(function (err) {
                  console.warn("pollOptionsOnce: leg error", {
                    ul: currentUL, expiry: expiry, right: right, strike: strike, err: err && (err.message || err)
                  });
                }); // end handleLeg
              }
            }
          })().catch(function (err) {
            console.warn("pollOptionsOnce: expiry scope error", {
              ul: currentUL, expiry: expiry, err: err && (err.message || err)
            });
          }); // end handleExpiry
        } // end for expiry
      } catch (err) {
        console.warn("pollOptionsOnce: ul scope error", { ul: currentUL, err: err && (err.message || err) });
        continue;
      }
    }

    STATE.options_ts = ticks;
    if (typeof wsBroadcast === "function") wsBroadcast("options_ts", ticks);
  } catch (e) {
    const payload = { ul: currentUL, err: (e && (e.message || e)) };
    try { if (e && e.stack) payload.stack = e.stack; } catch (_) {}
    console.warn("pollOptionsOnce: top-level error", payload);
  }
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

// ---- IB control & health ----
app.post("/ib/prime", async (_req, res) => {
  try { await ibPrimeSession(); return res.json({ ok:true }); }
  catch (e) { return res.status(500).json({ ok:false, error: e?.message || "fail" }); }
});

app.get("/ib/health", async (_req, res) => {
  try {
    const st = await ibFetch("/iserver/auth/status");
    return res.json({ ok:true, status: st });
  } catch (e) {
    return res.status(500).json({ ok:false, error: e?.message || "fail" });
  }
});

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

async function onTick(symbol, last) {
  // normalize inputs
  const ul = String(symbol || "").toUpperCase().replace(/^\//, "");
  const price = Number(last);

  if (!ul) return;                   // nothing to do
  if (!Number.isFinite(price)) return;

  // cache latest underlying price
  lastBySymbol[ul] = price;

  try {
    const maybePromise = evaluateStopsOnTick?.(ul, price);
    // support both sync and async implementations
    if (maybePromise && typeof maybePromise.then === "function") {
      await maybePromise;
    }
  } catch (err) {
    console.warn("onTick: evaluateStopsOnTick error", { ul, err: err?.message || err });
  }
}

/* ---------- WS broadcast wrapper (no-op if you don’t have it here) ---------- */
function safeBroadcast(msg) {
  try {
    if (global.broadcast) global.broadcast(msg);
    // else import your broadcast and call it
  } catch {}
}

function makeWatchlistPayload() {
  const wf = loadWatchlistFile();
  const options = wf.equities.flatMap((ul) => buildOptionsAroundATM(ul, getLast));
  return { equities: wf.equities, options };
}


app.get("/watchlist", (_req, res) => {
  res.json(makeWatchlistPayload());
});



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
app.get("/tape", (_req,res)=>{
  res.json({ sweeps: STATE.sweeps||[], blocks: STATE.blocks||[] });
});

// wss.onmessage = (ev) => {
//   try { console.log("WS:", JSON.parse(ev.data)); } catch { console.log(ev.data); }
// };
wsBroadcast("watchlist", getWatchlist()); // or WATCHLIST, or broadcastWatchlist()

// seed WATCH from your local EQUITIES and options list:
for (const s of EQUITIES) await addEquity(s);            // populates WATCH.equities
await addOptions(WATCHLIST.options);                      // populates WATCH.options

// (optionally) immediately broadcast the watchlist:
broadcastWatchlist();

/* ================= WS bootstrap ================= */
wss.on("connection", (ws) => {
  wsSend(ws, "equity_ts", STATE.equities_ts || []);
  wsSend(ws, "options_ts", STATE.options_ts || []);
  wsSend(ws, "sweeps",    STATE.sweeps    || []);
  wsSend(ws, "blocks",    STATE.blocks    || []);
  wsBroadcast("watchlist", makeWatchlistPayload());
  // wsSend(ws, "watchlist", getWatchlist()); // <-- shared
  // wsSend(ws, "watchlist", makeWatchlistPayload());
  wsSend(ws, "headlines", computeHeadlines());   // <— add this

  // NEW: seed "notables" immediately so the widget doesn’t wait 2s tick
  const bootNotables = computeNotables(NOTABLES_DEFAULT);
  wsSend(ws, "notables", bootNotables);  
});
(async () => {
  // try { await seedWatchlist(); } catch (e) { console.warn("seedWatchlist:", e.message); }
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

runLoop("notables", async () => {
  const notables = computeNotables(NOTABLES_DEFAULT);
  safeBroadcastNotables(notables || []);
}, { intervalMs: 2_000 });

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
setInterval(() => {
  const fopCount = Array.from(NBBO_OPT.keys()).filter(k => /^ES\s\d{6}\sFOP/.test(k)).length;
  console.log(`[FOP] tracked NBBO keys: ${fopCount}, prints buffer: ${STATE.prints?.length||0}`);
}, 5000);
// WS broadcast every 2s
setInterval(() => {
  const headlines = computeHeadlines({ windowMs: ROLLING_MS, minNotional: 50_000, topN: 12 });
  wsBroadcast("headlines", headlines);
}, 2_000);

setInterval(() => {
  const payload = buildNotables(
    { sweeps: STATE.sweeps||[], blocks: STATE.blocks||[], prints: STATE.prints||[] },
    NOTABLES_DEFAULT
  );
  safeBroadcastNotables(payload || []);
}, 1500);

// REST endpoint (optional)
// app.get("/api/flow/headlines", (_req, res) => {
//   const top = computeHeadlines({ windowMs: ROLLING_MS, minNotional: 50_000, topN: 20 });
//   res.json(top);
// });
app.get('/api/flow/headlines', (req, res) => {
  const ul = normalizeSymbol(req.query.ul ?? "");
  const since = Number(req.query.since ?? 0);
  const rows = store.headlines.filter(h =>
    (!ul || h.ul === ul) && (!since || h.ts >= since)
  );
  res.json(rows);
});
function assertCanonicalRows(topic, rows, key='symbol') {
  for (const r of rows) {
    const n = normalizeSymbol(r[key]);
    if (n !== r[key]) {
      console.warn(`[canon:${topic}] ${r[key]} -> ${n}`);
    }
  }
}
// --- PRICES FEED (where you build equity_ts) ---
function pumpEquityTs(ticks) {
  // normalize all symbols before emit
  const data = ticks.map(t => ({ ...t, symbol: normalizeSymbol(t.symbol) }));
  broadcast('equity_ts', data);
}
/* ================= ROUTES ================= */
app.get("/", (req,res)=>res.type("text/plain").send("TradeFlash server up\n"));

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
// app.get("/health", (req,res)=>{
//   res.json({ ok:true, primed:CACHE.sessionPrimed, watch:getWatchlist(),
//     eq_len: STATE.equities_ts?.length||0, opt_len: STATE.options_ts?.length||0 });
// });

const store = { prints: [], headlines: [] };
const { broadcast, metrics } = makeBus(wss);
app.get('/api/flow/debug/dump', (_req, res) => {
  res.json({
    prints: store.prints?.slice(-50) ?? [],
    headlines: store.headlines?.slice(-50) ?? []
  });
});
// Health + metrics (quick peek if futures are flowing)
app.get("/healthz", (_req, res) => {
  res.json({
    ok: true,
    ws_clients: wss.clients.size,
    metrics
  });
});

app.use("/api/flow/prints", makePrintsRouter({ store, broadcast }));
app.use("/api/flow/headlines", makeHeadlinesRouter({ store, broadcast }));

server.listen(PORT, async () => {
  console.log(`Listening on http://localhost:${PORT}`);
  // await seedDefaultsOnce();
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
