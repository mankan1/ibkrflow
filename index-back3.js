/* eslint-disable no-console */
/**
 * IBKR VERSION — Complete drop-in server for TradeFlash flow.
 * - Uses IBKR Client Portal Web API (equities & options).
 * - Fixes: auth priming, expirations parsing, option conid resolution (info + search),
 *          resilient snapshot calls, non-zero UL price for strikes, chains emission.
 */

import "./instrument-http.js";
import { httpMetrics } from "./instrument-http.js";

import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import WebSocket, { WebSocketServer } from "ws";
import { randomUUID, createHash } from "crypto";

// ===== TLS for local CP Gateway (self-signed) =====
if (process.env.IB_ALLOW_INSECURE === "1") {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

/* ================= CONFIG ================= */
const rawMock = (process.env.MOCK ?? "").toString().trim();
export const MOCK = rawMock === "1";
const PORT = Number(process.env.PORT || 8080);

const IB_HOST = process.env.IB_HOST || "127.0.0.1";
const IB_PORT = Number(process.env.IB_PORT || 5000);
const IB_SSL  = (process.env.IB_SSL ?? "1") !== "0";
const IB_PROTO = IB_SSL ? "https" : "http";
const IB_WS_PROTO = IB_SSL ? "wss" : "ws";
const IB_BASE = `${IB_PROTO}://${IB_HOST}:${IB_PORT}/v1/api`;
const IB_WS_URL = `${IB_WS_PROTO}://${IB_HOST}:${IB_PORT}/v1/api/ws`;
const IB_COOKIE = process.env.IB_COOKIE || ""; // exact Cookie header value from CP Gateway

const MAX_STRIKES_AROUND_ATM = 40;
const MAX_EXPIRY_DAYS        = 30;

const SWEEP_WINDOW_MS        = 600;
const SWEEP_MIN_QTY          = 300;
const SWEEP_MIN_NOTIONAL     = 75000;

const BLOCK_MIN_QTY          = 250;
const BLOCK_MIN_NOTIONAL     = 100000;

const FALLBACK_POLL_EVERY_MS = 2500;

/* ================= APP / WS ================= */
const app = express();
const ORIGINS = [
  "https://www.tradeflow.lol",
  "https://tradeflow.lol",
  "https://tradeflashflow-production.up.railway.app",
  "tradeflashflow-production.up.railway.app",
  /\.vercel\.app$/,
  "http://localhost:5173",
  "http://localhost:3000",
  "http://localhost:19006",
  "http://localhost:8081",
];
const isAllowed = (o) => ORIGINS.some(r => r instanceof RegExp ? r.test(o) : r === o);
const corsMiddleware = cors({
  origin(origin, cb) {
    if (!origin) return cb(null, true);
    return isAllowed(origin) ? cb(null, true) : cb(new Error(`CORS: origin not allowed: ${origin}`));
  },
  methods: ["GET","POST","PUT","PATCH","DELETE","OPTIONS"],
  allowedHeaders: ["Content-Type","Authorization","x-request-id"],
  credentials: false,
  maxAge: 86400,
  optionsSuccessStatus: 204,
});
app.use((req,res,next)=>{ res.header("Vary","Origin"); next(); });
app.use(corsMiddleware);
app.options("*", corsMiddleware);

app.use(express.json({ limit: "2mb" }));
app.use(compression());
app.use(morgan("tiny"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

/* ================= BUFFERS ================= */
const MAX_BUFFER = 400;
const buffers = {
  options_ts: [],
  equity_ts: [],
  sweeps:    [],
  blocks:    [],
  chains:    [],
};
const seenIds = Object.fromEntries(Object.keys(buffers).map(t => [t, new Set()]));

/* ===== OI storage for confirmation (unchanged) ===== */
const eodOiByDateOcc = new Map();
const lastEodDateForOcc = new Map();
// const IB_EXPIRATIONS = new Map();

/* ================= STATE ================= */
const eqQuote  = new Map(); // symbol -> { bid, ask, ts }
const optNBBO  = new Map(); // occ     -> { bid, ask, ts }
const optState = new Map();
const dayKey = () => new Date().toISOString().slice(0,10);

const alpacaSubs = new Set();                // equities (name kept for UI)
const tradierOptWatch = { set: new Set() };  // options   (name kept for UI)

const optLastPrintTs = new Map();
const sweepBuckets   = new Map();
const recentOpenLean = new Map();

const conidBySymbol = new Map();     // "AAPL" -> 265598
const symbolByConid = new Map();     // 265598 -> "AAPL"
const optionMetaByConid = new Map(); // conid(opt) -> { ul, exp, right, strike, occ }
const conidsSubscribed = new Set();
const eqConids = new Map();          // symbol -> conid
const occToConid = new Map();        // occ -> conid

const expirationsCache = new Map();  // symbol -> string[] (YYYY-MM-DD)
const strikesCache = new Map();      // symbol -> number[]

const lastTradeKeyByConid = new Map();

/* ================= UTILS ================= */
function ymdFromTs(ts) {
  const d = new Date(ts);
  return d.toISOString().slice(0,10);
}
function within30Days(dISO) {
  const t = Date.parse(dISO);
  if (!Number.isFinite(t)) return false;
  const now = Date.now();
  return (t - now) <= MAX_EXPIRY_DAYS * 24 * 3600 * 1000 && (t > now - 12*3600*1000);
}

function getOptState(key) {
  const today = dayKey();
  const s = optState.get(key) ?? { oi_before: 0, vol_today_before: 0, last_reset: today };
  if (s.last_reset !== today) { s.vol_today_before = 0; s.last_reset = today; }
  optState.set(key, s);
  return s;
}
const setOptOI  = (k, oi) => { const s = getOptState(k); s.oi_before = Number(oi)||0; optState.set(k, s); };
const setOptVol = (k, v)  => { const s = getOptState(k); s.vol_today_before = Math.max(0, Number(v)||0); optState.set(k, s); };
const bumpOptVol = (k, qty) => { const s = getOptState(k); s.vol_today_before += Number(qty)||0; optState.set(k, s); };

function stableId(msg) {
  const key = JSON.stringify({
    type: msg.type, provider: msg.provider,
    symbol: msg.symbol ?? msg.underlying,
    occ: msg.occ, side: msg.side,
    price: msg.price, size: msg.size,
    ts: msg.ts ?? msg.time ?? msg.at
  });
  return createHash("sha1").update(key).digest("hex");
}
function normalizeForFanout(msg) {
  const now = Date.now();
  const withTs = { ts: typeof msg.ts === "number" ? msg.ts : now, ...msg };
  const id = msg.id ?? stableId(withTs) ?? randomUUID();
  return { id, ...withTs };
}
function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}
function pushAndFanout(msg) {
  const m = normalizeForFanout(msg);
  const arr = buffers[m.type];
  if (!arr) return;
  const seen = seenIds[m.type] || (seenIds[m.type] = new Set());
  if (seen.has(m.id)) return;
  arr.unshift(m);
  seen.add(m.id);
  while (arr.length > MAX_BUFFER) {
    const dropped = arr.pop();
    if (dropped?.id) seen.delete(dropped.id);
  }
  broadcast(m);
}

function toOcc(ul, expISO, right, strike) {
  const yymmdd = expISO.replaceAll("-", "").slice(2);
  const cp = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike) * 1000)).padStart(8, "0");
  return `${ul.toUpperCase()}${yymmdd}${cp}${k}`;
}
function parseOcc(occ) {
  const m = /^([A-Z]+)(\d{6})([CP])(\d{8})$/.exec(occ);
  if (!m) return null;
  const [ , ul, yymmdd, cp, k ] = m;
  const exp = `20${yymmdd.slice(0,2)}-${yymmdd.slice(2,4)}-${yymmdd.slice(4,6)}`;
  return { ul, exp, right: cp === "C" ? "CALL" : "PUT", strike: Number(k)/1000 };
}
async function ibDiscoverExpirationsFromSecdefMonths(symbol /* "AAPL" */) {
  // Pull months list once
  const url = `/iserver/secdef/search?symbol=${encodeURIComponent(symbol)}&sectype=OPT&exchange=SMART`;
  const r = await ibFetch(url);
  const list = Array.isArray(r.data) ? r.data : [];

  // Find the entry that has sections with secType=OPT
  const withOpt = list.find(x =>
    Array.isArray(x.sections) &&
    x.sections.some(s => s.secType === "OPT" && typeof s.months === "string" && s.months.length > 0)
  );
  if (!withOpt) return [];

  const optSec = withOpt.sections.find(s => s.secType === "OPT" && typeof s.months === "string");
  if (!optSec || !optSec.months) return [];

  const tokens = optSec.months.split(";").map(s => s.trim()).filter(Boolean);
  const isoDates = [];
  for (const tok of tokens) {
    const p = parseMonthToken(tok); // → { yyyy, mm } or null
    if (!p) continue;
    isoDates.push(thirdFridayISO(p.yyyy, p.mm));
  }
  return uniqSortedISO(isoDates);
}
function classifyAggressor(price, nbbo) {
  if (!nbbo) return { side: "UNK", at: "MID" };
  const bid = Number(nbbo.bid ?? 0);
  const ask = Number(nbbo.ask ?? 0);
  if (!Number.isFinite(ask) || ask <= 0) return { side: "UNK", at: "MID" };
  const mid = (bid + ask) / 2;
  const eps = Math.max(0.01, (ask - bid) / 20);
  if (price >= Math.max(ask - eps, bid)) return { side: "BUY",  at: "ASK" };
  if (price <= Math.min(bid + eps, ask)) return { side: "SELL", at: "BID" };
  return { side: price >= mid ? "BUY" : "SELL", at: "MID" };
}
function inferIntent(occ, side, qty, price) {
  const s = getOptState(occ);
  const nbbo = optNBBO.get(occ);
  const { side: aggrSide, at } = classifyAggressor(price, nbbo);

  let tag = "UNK", conf = 0.35;
  const reasons = [];
  if (qty > (s.oi_before + s.vol_today_before)) {
    tag = side === "BUY" ? "BTO" : "STO"; conf = 0.8;
    reasons.push("qty > (yday OI + today vol)");
    recentOpenLean.set(occ, tag);
  } else {
    const prior = recentOpenLean.get(occ) || "UNK";
    const qtyWithinOI = qty <= s.oi_before;
    if (qtyWithinOI) reasons.push("qty <= yday OI");
    if (prior === "BTO" && side === "SELL" && (at === "BID" || aggrSide === "SELL")) { tag = "STC"; conf = qtyWithinOI ? 0.7 : 0.55; reasons.push("prior=BTO","sell@bid"); }
    else if (prior === "STO" && side === "BUY" && (at === "ASK" || aggrSide === "BUY")) { tag = "BTC"; conf = qtyWithinOI ? 0.7 : 0.55; reasons.push("prior=STO","buy@ask"); }
    else {
      if (side === "SELL" && at === "BID" && qtyWithinOI) { tag = "STC"; conf = 0.55; reasons.push("sell@bid"); }
      if (side === "BUY"  && at === "ASK" && qtyWithinOI) { tag = "BTC"; conf = 0.55; reasons.push("buy@ask"); }
    }
  }
  return { tag, conf, at, reasons, oi_yday: s.oi_before, vol_before: s.vol_today_before };
}

function normOptionsPrint(provider, p) {
  const now = Date.now();
  const key = `${p.underlying}|${p.option.expiration}|${p.option.strike}|${p.option.right}`;
  if (Number.isFinite(p.oi)) setOptOI(key, Number(p.oi));
  if (Number.isFinite(p.volToday)) setOptVol(key, Number(p.volToday));
  const occ = p.occ ?? toOcc(p.underlying, p.option.expiration, p.option.right, p.option.strike);

  const intent = inferIntent(occ, p.side, p.size, p.price);
  bumpOptVol(key, p.size);

  return {
    id: `${now}-${Math.random().toString(36).slice(2,8)}`,
    ts: now,
    type: "options_ts",
    provider,
    symbol: p.underlying,
    occ,
    option: p.option,
    side: p.side,
    qty: p.size,
    price: p.price,
    oc_intent: intent.tag,
    intent_conf: intent.conf,
    fill_at: intent.at,
    intent_reasons: intent.reasons,
    oi_before: intent.oi_yday,
    vol_before: intent.vol_before,
    venue: p.venue ?? null
  };
}
function normEquityPrint(provider, { symbol, price, size }) {
  const now = Date.now();
  const q = eqQuote.get(symbol);
  const side = (!q?.ask || !q?.bid)
    ? "MID"
    : (price >= q.ask ? "BUY" : (price <= q.bid ? "SELL" : "MID"));
  return {
    id: `${now}-${Math.random().toString(36).slice(2,8)}`,
    ts: now,
    type: "equity_ts",
    provider,
    symbol,
    side, qty: size, price,
    action: "UNK",
    venue: null
  };
}

/* ================= IB PRIMING & HTTP ================= */
let ibPrimed = false;
let ibLastAccount = null;
const sleep = (ms) => new Promise(r=>setTimeout(r, ms));

async function ibFetch(path, opts = {}, retry = 1) {
  const url = `${IB_BASE}${path}`;
  const headers = {
    Accept: "application/json",
    ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}),
    ...(opts.headers || {})
  };
  const resp = await fetch(url, { ...opts, headers });
  const status = resp.status;
  // Only read text ONCE
  const text = await resp.text();
  let data = null; try { data = text ? JSON.parse(text) : null; } catch { data = null; }

  if (status === 401 && retry >= 0) {
    console.warn("[IBHTTP] 401 -> reauth");
    await ibReauth();
    return ibFetch(path, opts, retry - 1);
  }

  if (!resp.ok) {
    console.warn(`[IBHTTP ${opts.method||"GET"}] ${url} -> ${status}`, data || text);
    // small retry for transient 5xx
    if (status >= 500 && retry > 0) {
      await sleep(150);
      return ibFetch(path, opts, retry - 1);
    }
  }
  return { ok: resp.ok, status, data, raw: text };
}

async function ibAuthStatus() {
  return ibFetch(`/iserver/auth/status`);
}
async function ibTickle() {
  return ibFetch(`/iserver/tickle`);
}
async function ibReauth() {
  await ibFetch(`/iserver/reauthenticate`, { method: "POST" });
}
async function ibAccounts() {
  return ibFetch(`/iserver/accounts`);
}
async function ibPortfolioAccounts() {
  return ibFetch(`/iserver/portfolio/accounts`);
}
// async function ibEnsureAccounts() {
//   // Status
//   let st = await ibAuthStatus();
//   if (st.status === 401 || st?.data?.authenticated === false) {
//     await ibReauth();
//     st = await ibAuthStatus();
//   }
//   // tickle & accounts (bridge)
//   await ibTickle();
//   const acc = await ibAccounts();
//   if (!acc.ok) {
//     // Sometimes need tickle→reauth→tickle
//     await ibReauth();
//     await ibTickle();
//   }
//   const acc2 = await ibAccounts();
//   if (Array.isArray(acc2?.data?.accounts) && acc2.data.accounts.length) {
//     ibLastAccount = acc2.data.accounts[0];
//     ibPrimed = true;
//     console.log("[IB] primed session. accounts=" + ibLastAccount);
//   } else {
//     // fallback portfolio/accounts
//     const p = await ibPortfolioAccounts();
//     if (Array.isArray(p?.data) && p.data.length) {
//       ibLastAccount = p.data[0]?.id || null;
//       ibPrimed = true;
//       console.log("[IB] primed via portfolio/accounts. account=" + ibLastAccount);
//     }
//   }
// }
async function ibEnsureAccounts() {
  let st = await ibAuthStatus();
  if (st.status === 401 || st?.data?.authenticated === false) {
    await ibReauth();
    st = await ibAuthStatus();
  }

  // Try tickle but don't fail the flow if missing
  await ibTickleOptional();

  // Hitting /iserver/accounts establishes the bridge
  const acc = await ibAccounts();
  if (Array.isArray(acc?.data?.accounts) && acc.data.accounts.length) {
    ibLastAccount = acc.data.accounts[0];
    ibPrimed = true;
    console.log("[IB] primed session. accounts=" + ibLastAccount);
    return;
  }

  // Fallback: portfolio/accounts
  const p = await ibPortfolioAccounts();
  if (Array.isArray(p?.data) && p.data.length) {
    ibLastAccount = p.data[0]?.id || null;
    ibPrimed = true;
    console.log("[IB] primed via portfolio/accounts. account=" + ibLastAccount);
  }
}

// utils: format helpers
function yyyymmFromDate(d) {
  return String(d.getUTCFullYear()) + String(d.getUTCMonth() + 1).padStart(2, "0");
}
function isoFromYYYYMMDD(s8) {
  return `${s8.slice(0,4)}-${s8.slice(4,6)}-${s8.slice(6,8)}`;
}

// Cache maps (reuse your existing ones if present)
const IB_EXPIRATIONS = new Map(); // key: conid -> Set('YYYY-MM-DD')

// Probe one month and harvest expirations
async function ibHarvestExpirationsForMonth(ulConid, yyyymm, underlyingPx) {
  // underlyingPrice improves results; if missing, pass 0
  const url = `/iserver/secdef/strikes?conid=${ulConid}&sectype=OPT&month=${yyyymm}&exchange=SMART&underlyingPrice=${Number(underlyingPx||0)}`;
  const r = await ibFetch(url);
  // Different gateway builds return slightly different shapes.
  // Look for 'expirations' or 'eligibleExpirations' or nested fields.
  const data = r.data || {};
  const exps =
    Array.isArray(data.expirations) ? data.expirations :
    Array.isArray(data.eligibleExpirations) ? data.eligibleExpirations :
    Array.isArray(data.dates) ? data.dates :
    [];

  // Accept either YYYYMMDD or YYYY-MM-DD; normalize to ISO.
  const out = [];
  for (const e of exps) {
    const s = String(e);
    const iso = /^\d{8}$/.test(s) ? isoFromYYYYMMDD(s) :
               /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
    if (iso) out.push(iso);
  }
  return out;
}

// Public: discover expirations for a given conid (fills IB_EXPIRATIONS)
async function ibDiscoverExpirations(ulConid, underlyingPx) {
  const set = new Set(IB_EXPIRATIONS.get(ulConid) || []);
  const now = new Date();
  // probe next 18 months
  for (let i = 0; i < 18; i++) {
    const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i, 1));
    const yyyymm = yyyymmFromDate(d);
    try {
      const found = await ibHarvestExpirationsForMonth(ulConid, yyyymm, underlyingPx);
      for (const iso of found) set.add(iso);
    } catch (_) {
      // ignore month errors; keep going
    }
  }
  IB_EXPIRATIONS.set(ulConid, set);
  return Array.from(set).sort();
}
// app.get("/debug/expirations", async (req, res) => {
//   try {
//     const symbol = String(req.query.symbol || "").toUpperCase();
//     if (!symbol) return res.status(400).json({ error: "usage: ?symbol=AAPL" });

//     // 1) resolve stock conid
//     const ul = await ibConidForStock(symbol);
//     if (!ul) return res.json({ symbol, conid: null, expirations: [] });

//     // 2) grab an underlying price (helps IB return the right chains)
//     const snap = await ibFetch(`/iserver/marketdata/snapshot?conids=${ul}&fields=31,84,86`);
//     const last = Number(snap?.data?.[0]?.["31"] ?? 0);

//     // 3) discover expirations (fills cache)
//     const exps = await ibDiscoverExpirations(ul, last);
//     return res.json({ symbol, conid: ul, expirations: exps });
//   } catch (e) {
//     console.error(e);
//     return res.status(500).json({ error: String(e?.message || e) });
//   }
// });
app.get("/debug/expirations", async (req, res) => {
  try {
    const symbol = String(req.query.symbol || "").toUpperCase();
    if (!symbol) return res.status(400).json({ error: "usage: ?symbol=AAPL" });

    // Resolve underlying conid
    const ulConid = await ibConidForStock(symbol);
    if (!ulConid) return res.json({ symbol, conid: null, expirations: [] });

    // If cached, return immediately
    const cached = IB_EXPIRATIONS.get(ulConid);
    if (cached && cached.size) {
      return res.json({ symbol, conid: ulConid, expirations: uniqSortedISO([...cached]) });
    }

    // Try to get an underlying price (optional; IB sometimes needs it)
    let last = 0;
    try {
      const snap = await ibFetch(`/iserver/marketdata/snapshot?conids=${ulConid}&fields=31,84,86`);
      last = Number(snap?.data?.[0]?.["31"] ?? 0) || 0;
    } catch (_) { /* ignore */ }

    // Attempt strikes-based harvesting over the next 18 months (some builds return expirations there)
    const strikesBased = [];
    {
      const found = new Set();
      const now = new Date();
      for (let i = 0; i < 18; i++) {
        const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i, 1));
        const yyyymm = String(d.getUTCFullYear()) + String(d.getUTCMonth() + 1).padStart(2, "0");
        try {
          const r = await ibFetch(`/iserver/secdef/strikes?conid=${ulConid}&sectype=OPT&month=${yyyymm}&exchange=SMART&underlyingPrice=${Number(last || 0)}`);
          const data = r.data || {};
          const exps =
              Array.isArray(data.expirations) ? data.expirations :
              Array.isArray(data.eligibleExpirations) ? data.eligibleExpirations :
              Array.isArray(data.dates) ? data.dates : [];
          for (const e of exps) {
            const s = String(e);
            const iso = /^\d{8}$/.test(s) ? `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`
                     : /^\d{4}-\d{2}-\d{2}$/.test(s) ? s
                     : null;
            if (iso) found.add(iso);
          }
        } catch (_) { /* ignore per-month errors */ }
      }
      strikesBased.push(...found);
    }

    let expirations = uniqSortedISO(strikesBased);

    // Fallback: derive from secdef/search months → 3rd Fridays
    if (!expirations.length) {
      try {
        const fromMonths = await ibDiscoverExpirationsFromSecdefMonths(symbol);
        expirations = uniqSortedISO(fromMonths);
      } catch (_) { /* ignore */ }
    }

    // Cache and return (even if empty)
    IB_EXPIRATIONS.set(ulConid, new Set(expirations));
    return res.json({ symbol, conid: ulConid, expirations });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});
function yyyymmFromISO(iso /* YYYY-MM-DD */) {
  return iso.slice(0, 4) + iso.slice(5, 7);
}

async function ibStrikesForMonth(conid, yyyymm, underlyingPrice) {
  // try with underlyingPrice (your gateway seems to like it)
  const url = `/iserver/secdef/strikes?conid=${conid}&sectype=OPT&month=${yyyymm}&exchange=SMART&underlyingPrice=${Number(underlyingPrice||0)}`;
  const r = await ibFetch(url);
  const data = r?.data || {};
  // IB returns different shapes across versions; normalize
  const strikes =
    Array.isArray(data.strikes)    ? data.strikes :
    Array.isArray(data.values)     ? data.values  :
    Array.isArray(data)            ? data         :
    [];
  // cast to numbers, uniq & sort
  const clean = Array.from(new Set(strikes.map(s => Number(s)).filter(n => Number.isFinite(n)))).sort((a,b)=>a-b);
  return clean;
}
async function buildOptionChainForSymbol(symbol /* "AAPL" */) {
  const ulConid = await ibConidForStock(symbol);
  if (!ulConid) return { symbol, conid: null, last: 0, chains: [] };

  // last price (optional)
  let last = 0;
  try {
    const snap = await ibFetch(`/iserver/marketdata/snapshot?conids=${ulConid}&fields=31`);
    last = Number(snap?.data?.[0]?.["31"] ?? 0) || 0;
  } catch (_) {}

  // expirations from our cache (/debug/expirations already seeded it)
  let expSet = IB_EXPIRATIONS.get(ulConid);
  if (!expSet || !expSet.size) {
    // seed it now if needed
    const expRes = await (await fetch(`${API_BASE}/debug/expirations?symbol=${encodeURIComponent(symbol)}`)).json?.().catch(()=>null);
    if (expRes && Array.isArray(expRes.expirations)) expSet = new Set(expRes.expirations);
  }
  const expirations = expSet ? Array.from(expSet).sort() : [];
  if (!expirations.length) return { symbol, conid: ulConid, last, chains: [] };

  // for each expiration → fetch strikes and build a minimal chain node
  const chains = [];
  for (const expISO of expirations) {
    const yyyymm = yyyymmFromISO(expISO);
    let strikes = [];
    try {
      strikes = await ibStrikesForMonth(ulConid, yyyymm, last);
    } catch (_) { /* keep empty */ }

    chains.push({
      expiration: expISO,               // "YYYY-MM-DD"
      strikes: strikes.map(k => ({
        strike: k,
        call: null,                     // placeholders (can fill later)
        put: null
      }))
    });
  }
  return { symbol, conid: ulConid, last, chains };
}
/* ================= CONID & SNAPSHOT ================= */
const F_BID = 84, F_ASK = 86, F_LAST = 31, F_BIDSZ = 88, F_ASKSZ = 85, F_LASTSZ = 7059;

async function ibConidForStock(symbol) {
  symbol = symbol.toUpperCase();
  if (conidBySymbol.has(symbol)) return conidBySymbol.get(symbol);
  await ibEnsureAccounts(); // make sure bridge is up before any call
  const r = await ibFetch(`/trsrv/stocks?symbols=${encodeURIComponent(symbol)}`);
  const arr = r?.data?.[symbol] || [];
  const best = arr.find(x => (x?.contracts||[]).some(c => c.isUS === true)) || arr[0];
  const conid = best?.contracts?.[0]?.conid || best?.contracts?.[0]?.conidEx || best?.conid;
  if (Number.isFinite(Number(conid))) {
    conidBySymbol.set(symbol, Number(conid));
    symbolByConid.set(Number(conid), symbol);
    return Number(conid);
  }
  return null;
}

async function ibSnapshot(conids, fields = [F_LAST, F_BID, F_ASK]) {
  if (!conids?.length) return new Map();
  await ibEnsureAccounts();
  const u = `/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids.join(","))}&fields=${fields.join(",")}`;
  const r = await ibFetch(u);
  const list = Array.isArray(r.data) ? r.data : [];
  const out = new Map();
  for (const row of list) {
    const id = Number(row.conid);
    const o = {
      last: Number(row[String(F_LAST)] ?? row.last ?? 0) || 0,
      bid:  Number(row[String(F_BID)]  ?? row.bid  ?? 0) || 0,
      ask:  Number(row[String(F_ASK)]  ?? row.ask  ?? 0) || 0,
      bidSize: Number(row[String(F_BIDSZ)] ?? 0) || 0,
      askSize: Number(row[String(F_ASKSZ)] ?? 0) || 0,
      lastSize: Number(row[String(F_LASTSZ)] ?? 0) || 0
    };
    out.set(id, o);
  }
  return out;
}

async function ibGetUlLast(symbol, conid) {
  const m = await ibSnapshot([conid], [F_LAST, F_BID, F_ASK]);
  const row = m.get(conid);
  // never pass 0 to strikes
  const last = Number(row?.last) || Number(row?.bid) || Number(row?.ask) || 1;
  return last > 0 ? last : 1;
}

/* ================= EXPIRATIONS & STRIKES ================= */
function collectExpAndStrikesFlex(obj, expirationsSet, strikesSet) {
  if (!obj || typeof obj !== "object") return;
  const keysExp = ["expirations","optExpDate","expirationsMonth","expiration","expiry","expDates"];
  for (const k of keysExp) {
    const v = obj[k];
    if (Array.isArray(v)) for (const e of v) expirationsSet.add(String(e));
    else if (v && typeof v === "object") collectExpAndStrikesFlex(v, expirationsSet, strikesSet);
  }
  const vStr = obj.strikes || obj.strike || obj.strikePrices;
  if (Array.isArray(vStr)) for (const s of vStr) if (Number.isFinite(Number(s))) strikesSet.add(Number(s));
}

function normalizeExpISO(s) {
  const t = String(s);
  if (/^\d{8}$/.test(t)) return `${t.slice(0,4)}-${t.slice(4,6)}-${t.slice(6,8)}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;
  return null;
}

async function ibGetStrikesExp(symbol, conid, ulPrice) {
  const cacheKey = symbol.toUpperCase();
  // quick return from cache to reduce spam
  const cached = expirationsCache.get(cacheKey);
  const cachedStrikes = strikesCache.get(cacheKey);
  if (cached?.length && cachedStrikes?.length) {
    return { expirations: cached, strikes: cachedStrikes };
  }

  const today = new Date();
  const months = [];
  for (let i = 0; i < 4; i++) {
    const d = new Date(today); d.setMonth(d.getMonth() + i);
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2,"0");
    months.push(`${yyyy}${mm}`);
  }

  const expirationsRaw = new Set();
  const strikesSet = new Set();

  for (const mStr of months) {
    const q = `/iserver/secdef/strikes?conid=${conid}&sectype=OPT&month=${mStr}&exchange=SMART` +
              (Number.isFinite(ulPrice) && ulPrice > 0 ? `&underlyingPrice=${encodeURIComponent(ulPrice)}` : "");
    const r = await ibFetch(q);
    if (r?.data) collectExpAndStrikesFlex(r.data, expirationsRaw, strikesSet);
  }

  const exps30d = Array.from(expirationsRaw)
    .map(normalizeExpISO)
    .filter(Boolean)
    .filter(within30Days)
    .sort();

  const strikes = Array.from(strikesSet).sort((a,b)=>a-b);

  if (exps30d.length) expirationsCache.set(cacheKey, exps30d);
  if (strikes.length)  strikesCache.set(cacheKey, strikes);

  return { expirations: exps30d, strikes };
}

function pickStrikesNear(strikes, center, limit = MAX_STRIKES_AROUND_ATM) {
  if (!Number.isFinite(center) || !strikes?.length) return [];
  return strikes
    .map(s => ({ s, d: Math.abs(s - center) }))
    .sort((a,b)=>a.d - b.d)
    .slice(0, Math.min(limit, strikes.length))
    .map(x => x.s)
    .sort((a,b)=>a-b);
}

/* ================= OPTION CONID RESOLUTION ================= */
// async function ibOptionConid(ulConid, { ul, expISO, right, strike }) {
//   await ibEnsureAccounts();

//   const cp = right[0].toUpperCase();     // C or P
//   const yyyymm   = expISO.replace(/-/g, "").slice(0, 6);
//   const yyyymmdd = expISO.replace(/-/g, "");
//   const k = String(Number(strike));      // plain decimal
//   const tradingClass = ul.toUpperCase();

//   const candidates = [
//     `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${yyyymm}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}&tradingClass=${tradingClass}`,
//     `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}&tradingClass=${tradingClass}`,
//     // Some deployments prefer search-by-symbol:
//     `/iserver/secdef/search?symbol=${encodeURIComponent(tradingClass)}&sectype=OPT&month=${yyyymm}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`,
//     `/iserver/secdef/search?symbol=${encodeURIComponent(tradingClass)}&sectype=OPT&month=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`,
//   ];

//   for (const url of candidates) {
//     const r = await ibFetch(url);
//     const list = Array.isArray(r.data) ? r.data
//       : (Array.isArray(r.data?.contracts) ? r.data.contracts : null);
//     if (!list || !list.length) continue;

//     let best = null;
//     for (const c of list) {
//       const cx = c || {};
//       const exp = String(cx.expiration || cx.lastTradingDay || "");
//       const expN = /^\d{8}$/.test(exp) ? `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}` : exp;
//       const okRight  = String(cx.right || cx.optRight || "").toUpperCase().startsWith(cp);
//       const okStrike = Math.abs(Number(cx.strike || cx.strikePrice) - Number(strike)) < 1e-6;
//       const okClass  = (String(cx.tradingClass||"").toUpperCase() === tradingClass) || true; // tolerant
//       if (okRight && okStrike && okClass && expN === expISO) { best = cx; break; }
//     }
//     const conid = Number(best?.conid || best?.conidEx);
//     if (Number.isFinite(conid)) return conid;
//   }
//   return null;
// }
async function ibTickleOptional() {
  // Some gateway builds don't expose /iserver/tickle; ignore 404s
  const r = await ibFetch(`/iserver/tickle`);
  if (r.status === 404) return { ok: true, skipped: true };
  return r;
}

// async function ibEnsureAccounts() {
//   let st = await ibAuthStatus();
//   if (st.status === 401 || st?.data?.authenticated === false) {
//     await ibReauth();
//     st = await ibAuthStatus();
//   }

//   // Try tickle but don't fail the flow if missing
//   await ibTickleOptional();

//   // Hitting /iserver/accounts establishes the bridge
//   const acc = await ibAccounts();
//   if (Array.isArray(acc?.data?.accounts) && acc.data.accounts.length) {
//     ibLastAccount = acc.data.accounts[0];
//     ibPrimed = true;
//     console.log("[IB] primed session. accounts=" + ibLastAccount);
//     return;
//   }

//   // Fallback: portfolio/accounts
//   const p = await ibPortfolioAccounts();
//   if (Array.isArray(p?.data) && p.data.length) {
//     ibLastAccount = p.data[0]?.id || null;
//     ibPrimed = true;
//     console.log("[IB] primed via portfolio/accounts. account=" + ibLastAccount);
//   }
// }
// app.get("/debug/secdef-search", async (req, res) => {
//   const ul  = String(req.query.ul || "").toUpperCase();
//   const exp = String(req.query.exp || "");
//   const cp  = String(req.query.right || "C").toUpperCase()[0];
//   const k   = String(Number(req.query.strike || 0));
//   if (!ul || !/^\d{4}-\d{2}-\d{2}$/.test(exp) || !Number.isFinite(Number(k))) {
//     return res.status(400).json({ error: "usage: ?ul=AAPL&exp=2025-12-19&right=C&strike=200" });
//   }

//   const yyyymm   = exp.replace(/-/g, "").slice(0,6);
//   const yyyymmdd = exp.replace(/-/g, "");
//   const tries = [
//     `/iserver/secdef/search?symbol=${encodeURIComponent(ul)}&sectype=OPT&month=${yyyymm}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`,
//     `/iserver/secdef/search?symbol=${encodeURIComponent(ul)}&sectype=OPT&month=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`
//   ];

//   const results = [];
//   for (const url of tries) {
//     const r = await ibFetch(url);
//     results.push({ url, status: r.status, count: Array.isArray(r.data) ? r.data.length : 0 });
//   }
//   res.json({ ul, exp, right: cp, strike: Number(k), tries: results });
// });
app.get("/debug/secdef-search", async (req, res) => {
  try {
    const ul  = String(req.query.ul || "").toUpperCase();
    const exp = String(req.query.exp || "");
    const cp  = String(req.query.right || "C").toUpperCase()[0];
    const k   = String(Number(req.query.strike || 0));
    if (!ul || !/^\d{4}-\d{2}-\d{2}$/.test(exp) || !Number.isFinite(Number(k))) {
      return res.status(400).json({ error: "usage: ?ul=AAPL&exp=2025-12-19&right=C&strike=200" });
    }

    const yyyymm   = exp.replace(/-/g, "").slice(0,6);
    const yyyymmdd = exp.replace(/-/g, "");

    const urls = [
      `/iserver/secdef/search?symbol=${encodeURIComponent(ul)}&sectype=OPT&month=${yyyymm}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`,
      `/iserver/secdef/search?symbol=${encodeURIComponent(ul)}&sectype=OPT&month=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`
    ];
    const tries = [];
    let chosen = null;

    for (const url of urls) {
      const r = await ibFetch(url);
      const list = Array.isArray(r.data) ? r.data : [];
      // Pick best (SMART preferred)
      const bestSmart = list.find((c) => {
        const expRaw = String(c.expiration || c.lastTradingDay || "");
        const expIso = /^\d{8}$/.test(expRaw) ? isoFromYYYYMMDD(expRaw) : expRaw;
        const okRight  = String(c.right || c.optRight || "").toUpperCase().startsWith(cp);
        const okStrike = Math.abs(Number(c.strike || c.strikePrice) - Number(k)) < 1e-6;
        const ex = String(c.exchange || c.listingExchange || "").toUpperCase();
        return expIso === exp && okRight && okStrike && ex === "SMART";
      });
      const bestAny = bestSmart || list.find((c) => {
        const expRaw = String(c.expiration || c.lastTradingDay || "");
        const expIso = /^\d{8}$/.test(expRaw) ? isoFromYYYYMMDD(expRaw) : expRaw;
        const okRight  = String(c.right || c.optRight || "").toUpperCase().startsWith(cp);
        const okStrike = Math.abs(Number(c.strike || c.strikePrice) - Number(k)) < 1e-6;
        return expIso === exp && okRight && okStrike;
      });

      const chosenConid = Number(bestAny?.conid || bestAny?.conidEx);
      tries.push({ url, status: r.status, count: list.length, sample: list[0] || null });
      if (!chosen && Number.isFinite(chosenConid)) {
        chosen = { conid: chosenConid, picked: bestAny || null };
      }
    }

    res.json({ ul, exp, right: cp, strike: Number(k), tries, chosen });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});
// --- Month abbrev → number ---
const MONTH_ABBR = {
  JAN: 1, FEB: 2, MAR: 3, APR: 4, MAY: 5, JUN: 6,
  JUL: 7, AUG: 8, SEP: 9, OCT: 10, NOV: 11, DEC: 12,
};
function parseMonthToken(tok /* e.g. "NOV25" */) {
  const m = tok.slice(0, 3).toUpperCase();
  const yy = tok.slice(3); // "25"
  const mm = MONTH_ABBR[m];
  if (!mm || !/^\d{2}$/.test(yy)) return null;
  // Assume 20YY (good through 2099)
  const yyyy = 2000 + Number(yy);
  return { yyyy, mm };
}

// --- 3rd Friday of a given year-month -> ISO "YYYY-MM-DD" ---
function thirdFridayISO(yyyy, mm /* 1-12 */) {
  // Find first day of month (UTC-safe)
  const first = new Date(Date.UTC(yyyy, mm - 1, 1));
  // Day of week 0..6 (Sun..Sat)
  const dow = first.getUTCDay();
  // Offset to first Friday
  const offsetToFriday = (5 - dow + 7) % 7;
  const firstFriday = 1 + offsetToFriday; // day-of-month
  const thirdFriday = firstFriday + 14;   // +2 weeks
  const d = new Date(Date.UTC(yyyy, mm - 1, thirdFriday));
  const m2 = String(mm).padStart(2, "0");
  const d2 = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${m2}-${d2}`;
}

function uniqSortedISO(list) {
  return Array.from(new Set(list)).sort();
}
async function ibOptionConid(ulConid, { ul, expISO, right, strike }) {
  await ibEnsureAccounts();

  const cp = right[0].toUpperCase();            // C/P
  const yyyymm   = expISO.replace(/-/g, "").slice(0, 6);
  const yyyymmdd = expISO.replace(/-/g, "");
  const k = String(Number(strike));
  const tradingClass = ul.toUpperCase();

  // 1) Try secdef/info (some gateways support it well)
  const infoCandidates = [
    `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${yyyymm}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}&tradingClass=${tradingClass}`,
    `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}&tradingClass=${tradingClass}`,
  ];
  for (const url of infoCandidates) {
    const r = await ibFetch(url);
    const list = Array.isArray(r.data) ? r.data
      : (Array.isArray(r.data?.contracts) ? r.data.contracts : null);
    if (!list || !list.length) continue;

    const best = list.find((c) => {
      const exp = String(c.expiration || c.lastTradingDay || "");
      const expN = /^\d{8}$/.test(exp) ? `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}` : exp;
      const okRight  = String(c.right || c.optRight || "").toUpperCase().startsWith(cp);
      const okStrike = Math.abs(Number(c.strike || c.strikePrice) - Number(strike)) < 1e-6;
      // prefer SMART if present
      const ex = String(c.exchange || c.listingExchange || "").toUpperCase();
      return (expN === expISO) && okRight && okStrike && (!ex || ex === "SMART");
    }) || list.find((c) => {
      const exp = String(c.expiration || c.lastTradingDay || "");
      const expN = /^\d{8}$/.test(exp) ? `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}` : exp;
      const okRight  = String(c.right || c.optRight || "").toUpperCase().startsWith(cp);
      const okStrike = Math.abs(Number(c.strike || c.strikePrice) - Number(strike)) < 1e-6;
      return (expN === expISO) && okRight && okStrike;
    });

    const conid = Number(best?.conid || best?.conidEx);
    if (Number.isFinite(conid)) return conid;
  }

  // 2) Fallback: secdef/search (works per your logs)
  const searchCandidates = [
    `/iserver/secdef/search?symbol=${encodeURIComponent(tradingClass)}&sectype=OPT&month=${yyyymm}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`,
    `/iserver/secdef/search?symbol=${encodeURIComponent(tradingClass)}&sectype=OPT&month=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`
  ];

  for (const url of searchCandidates) {
    const r = await ibFetch(url);
    const list = Array.isArray(r.data) ? r.data : [];
    if (!list.length) continue;

    // Normalize and match strictly
    const bestSmart = list.find((c) => {
      const exp = String(c.expiration || c.lastTradingDay || "");
      const expN = /^\d{8}$/.test(exp) ? `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}` : exp;
      const okRight  = String(c.right || c.optRight || "").toUpperCase().startsWith(cp);
      const okStrike = Math.abs(Number(c.strike || c.strikePrice) - Number(strike)) < 1e-6;
      const ex = String(c.exchange || c.listingExchange || "").toUpperCase();
      const cls = String(c.tradingClass || c.symbol || "").toUpperCase();
      return (expN === expISO) && okRight && okStrike && cls.startsWith(tradingClass) && (ex === "SMART");
    });

    const bestAny = bestSmart || list.find((c) => {
      const exp = String(c.expiration || c.lastTradingDay || "");
      const expN = /^\d{8}$/.test(exp) ? `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}` : exp;
      const okRight  = String(c.right || c.optRight || "").toUpperCase().startsWith(cp);
      const okStrike = Math.abs(Number(c.strike || c.strikePrice) - Number(strike)) < 1e-6;
      return (expN === expISO) && okRight && okStrike;
    });

    const conid = Number(bestAny?.conid || bestAny?.conidEx);
    if (Number.isFinite(conid)) return conid;
  }

  return null;
}
/* ================= CHAIN EXPANSION ================= */
async function expandAndWatchChainIBKR(ul) {
  const symbol = ul.toUpperCase();
  const ulConid = await ibConidForStock(symbol);
  if (!ulConid) return;

  const last = await ibGetUlLast(symbol, ulConid);
  const { expirations, strikes } = await ibGetStrikesExp(symbol, ulConid, last || 1);
  if (!expirations.length || !strikes.length) return;

  const bestStrikes = pickStrikesNear(strikes, last || strikes[Math.floor(strikes.length/2)] || strikes[0] || 1);

  // emit chains for UI
  for (const exp of expirations) {
    pushAndFanout({
      type: "chains", provider: "ibkr", ts: Date.now(),
      symbol, expiration: exp, strikes: bestStrikes, strikesCount: bestStrikes.length
    });
  }

  // resolve & subscribe options around ATM
  for (const exp of expirations) {
    for (const k of bestStrikes) {
      for (const right of ["CALL","PUT"]) {
        const conid = await ibOptionConid(ulConid, { ul: symbol, expISO: exp, right, strike: k });
        if (!conid) continue;
        const occ = toOcc(symbol, exp, right, k);
        occToConid.set(occ, conid);
        optionMetaByConid.set(conid, { ul: symbol, exp, right, strike: k, occ });
        await ibWsEnsure();
        ibWsSubscribe(conid);
        // seed NBBO (optional)
        const snap = await ibSnapshot([conid], [F_BID, F_ASK]);
        const row = snap.get(conid);
        if (row) optNBBO.set(occ, { bid: row.bid||0, ask: row.ask||0, ts: Date.now() });
      }
    }
  }
}

/* ================= IB WEBSOCKET ================= */
let ibWs = null;
let ibWsReady = false;
let ibWsConnecting = false;
let ibHeartbeatTimer = null;

async function ibWsEnsure() {
  if (MOCK) return;
  if (ibWsReady || ibWsConnecting) return;
  ibWsConnecting = true;

  ibWs = new WebSocket(IB_WS_URL, {
    headers: IB_COOKIE ? { Cookie: IB_COOKIE } : {},
    rejectUnauthorized: process.env.IB_ALLOW_INSECURE === "1" ? false : true
  });

  ibWs.on("open", () => {
    ibWsReady = true; ibWsConnecting = false;
    for (const c of conidsSubscribed) {
      try { ibWs.send(`smd+${c}+{"fields":[${[F_LAST,F_BID,F_ASK,F_BIDSZ,F_ASKSZ,F_LASTSZ].join(",")}]} `); } catch {}
    }
    clearInterval(ibHeartbeatTimer);
    ibHeartbeatTimer = setInterval(() => { try { ibWs?.send?.("ech+hb"); } catch {} }, 10000);
  });

  ibWs.on("message", (buf) => {
    try {
      const s = String(buf);
      const chunks = s.split(/\r?\n/).filter(Boolean);
      for (const line of chunks) {
        let msg; try { msg = JSON.parse(line); } catch { continue; }
        const rows = Array.isArray(msg?.data) ? msg.data : (Array.isArray(msg) ? msg : [msg]);
        for (const row of rows) handleIbTick(row);
      }
    } catch {}
  });

  ibWs.on("close", () => {
    ibWsReady = false; ibWs = null; ibWsConnecting = false;
    clearInterval(ibHeartbeatTimer);
    setTimeout(ibWsEnsure, 1500);
  });
  ibWs.on("error", () => {});
}

function ibWsSubscribe(conid) {
  if (conidsSubscribed.has(conid)) return;
  conidsSubscribed.add(conid);
  if (!ibWsReady) return;
  try {
    ibWs.send(`smd+${conid}+{"fields":[${[F_LAST,F_BID,F_ASK,F_BIDSZ,F_ASKSZ,F_LASTSZ].join(",")}]} `);
  } catch {}
}

function updateNBBOFromRow(conid, data) {
  const bid = Number(data[String(F_BID)] ?? data.bid ?? 0) || 0;
  const ask = Number(data[String(F_ASK)] ?? data.ask ?? 0) || 0;
  const symbol = symbolByConid.get(conid);
  const meta = optionMetaByConid.get(conid);
  if (symbol && (bid || ask)) eqQuote.set(symbol, { bid, ask, ts: Date.now() });
  if (meta?.occ && (bid || ask)) optNBBO.set(meta.occ, { bid, ask, ts: Date.now() });
}

function handleIbTick(row) {
  const conid = Number(row.conid || row.conidEx);
  if (!Number.isFinite(conid)) return;

  updateNBBOFromRow(conid, row);

  const last = Number(row[String(F_LAST)] ?? row.last ?? 0) || 0;
  const lastSize = Number(row[String(F_LASTSZ)] ?? 0) || 0;

  const sym = symbolByConid.get(conid);
  const meta = optionMetaByConid.get(conid);
  const nowKey = `${last}@${lastSize}`;
  const prevKey = lastTradeKeyByConid.get(conid);
  if (last <= 0 || lastSize <= 0 || nowKey === prevKey) return;
  lastTradeKeyByConid.set(conid, nowKey);

  if (meta) {
    const occ = meta.occ;
    const nbbo = optNBBO.get(occ);
    const side = classifyAggressor(last, nbbo).side;
    const out = normOptionsPrint("ibkr", {
      underlying: meta.ul,
      option: { expiration: meta.exp, strike: meta.strike, right: meta.right },
      price: last, size: lastSize, side, venue: null, occ
    });
    pushAndFanout(out);

    const notion = last * lastSize * 100;
    if (lastSize >= BLOCK_MIN_QTY || notion >= BLOCK_MIN_NOTIONAL) {
      pushAndFanout({
        type: "blocks",
        provider: "ibkr",
        ts: out.ts,
        symbol: meta.ul,
        occ,
        option: out.option,
        side: out.side,
        qty: out.qty,
        price: out.price,
        notional: notion,
        oc_intent: out.oc_intent,
        intent_conf: out.intent_conf,
        fill_at: out.fill_at
      });
    }

    const existing = sweepBuckets.get(occ);
    const now = Date.now();
    if (!existing || (now - existing.startTs > SWEEP_WINDOW_MS) || (existing.side !== out.side)) {
      sweepBuckets.set(occ, {
        side: out.side, startTs: now, totalQty: out.qty, notional: notion,
        prints: [{ ts: out.ts, qty: out.qty, price: out.price, venue: out.venue }]
      });
    } else {
      existing.totalQty += out.qty;
      existing.notional += notion;
      existing.prints.push({ ts: out.ts, qty: out.qty, price: out.price, venue: out.venue });
    }
    const bucket = sweepBuckets.get(occ);
    if (bucket && (bucket.totalQty >= SWEEP_MIN_QTY || bucket.notional >= SWEEP_MIN_NOTIONAL)) {
      pushAndFanout({
        type: "sweeps",
        provider: "ibkr",
        ts: now,
        symbol: meta.ul,
        occ,
        option: out.option,
        side: bucket.side,
        totalQty: bucket.totalQty,
        notional: bucket.notional,
        prints: bucket.prints,
        oc_intent: out.oc_intent,
        intent_conf: out.intent_conf,
        fill_at: out.fill_at
      });
      sweepBuckets.delete(occ);
    }
    optLastPrintTs.set(occ, Date.now());
  } else if (sym) {
    const eqMsg = normEquityPrint("ibkr", { symbol: sym, price: last, size: lastSize });
    pushAndFanout(eqMsg);
  }
}

/* ================= SNAPSHOT POLLER ================= */
async function pollSnapshotsOnce() {
  if (MOCK) return;
  const allConids = new Set();
  for (const c of eqConids.values()) allConids.add(c);
  for (const c of optionMetaByConid.keys()) allConids.add(c);
  if (!allConids.size) return;

  const list = Array.from(allConids).slice(0, 50);
  const snap = await ibSnapshot(list, [F_BID, F_ASK, F_LAST, F_LASTSZ]);
  for (const [conid, row] of snap.entries()) {
    updateNBBOFromRow(conid, { [F_BID]: row.bid, [F_ASK]: row.ask });
    const key = `${row.last}@${row.lastSize}`;
    if (row.last > 0 && row.lastSize > 0 && key !== lastTradeKeyByConid.get(conid)) {
      lastTradeKeyByConid.set(conid, key);
      const sym = symbolByConid.get(conid);
      const meta = optionMetaByConid.get(conid);
      if (meta) {
        const occ = meta.occ;
        const nbbo = optNBBO.get(occ);
        const side = classifyAggressor(row.last, nbbo).side;
        pushAndFanout(normOptionsPrint("ibkr", {
          underlying: meta.ul,
          option: { expiration: meta.exp, strike: meta.strike, right: meta.right },
          price: row.last, size: row.lastSize, side, venue: null, occ
        }));
      } else if (sym) {
        pushAndFanout(normEquityPrint("ibkr", { symbol: sym, price: row.last, size: row.lastSize }));
      }
    }
  }
}
setInterval(pollSnapshotsOnce, FALLBACK_POLL_EVERY_MS);

/* ================= WS (client) BOOTSTRAP ================= */
wss.on("connection", (sock) => {
  sock.on("message", (buf) => {
    try {
      const m = JSON.parse(String(buf));
      if (Array.isArray(m.subscribe)) {
        for (const t of m.subscribe) {
          (buffers[t] || []).slice(0, 50).forEach(it => sock.send(JSON.stringify(it)));
        }
      }
    } catch {}
  });
});

/* ================= WATCH ROUTES ================= */
app.get("/watch/symbols", (_req, res) => {
  res.json({
    ok: true,
    watching: {
      equities: Array.from(alpacaSubs),
      options: Array.from(tradierOptWatch.set).map(JSON.parse),
    },
  });
});

app.post("/watch/symbols", async (req, res) => {
  const symbols = new Set([...(req.body?.symbols || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  if (!symbols.size) return res.json({ ok: true, added: 0, watching: { equities: Array.from(alpacaSubs) } });

  let added = 0;
  for (const s of symbols) if (!alpacaSubs.has(s)) { alpacaSubs.add(s); added++; }

  if (!MOCK) {
    await ibEnsureAccounts();
    await ibWsEnsure();
    for (const s of symbols) {
      const conid = await ibConidForStock(s);
      if (conid) {
        eqConids.set(s, conid);
        symbolByConid.set(conid, s);
        ibWsSubscribe(conid);
      }
      await ensureChainsForUL(s); // emit chains even if UI hasn't hit tradier route
    }
  }
  res.json({ ok: true, added, watching: { equities: Array.from(alpacaSubs) } });
});

app.delete("/watch/symbols", async (req, res) => {
  const symbols = new Set([...(req.body?.symbols || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  if (!symbols.size) return res.json({ ok: true, removed: 0, optsRemoved: 0, watching: { equities: Array.from(alpacaSubs) } });

  let removed = 0;
  for (const s of symbols) if ( alpacaSubs.delete(s) ) removed++;

  const before = tradierOptWatch.set.size;
  for (const entry of Array.from(tradierOptWatch.set)) {
    try {
      const o = JSON.parse(entry);
      if (symbols.has(String(o.underlying).toUpperCase())) tradierOptWatch.set.delete(entry);
    } catch {}
  }
  const optsRemoved = before - tradierOptWatch.set.size;

  res.json({ ok: true, removed, optsRemoved, watching: {
    equities: Array.from(alpacaSubs),
    options: Array.from(tradierOptWatch.set).map(JSON.parse)
  } });
});

// app.post("/watch/tradier", async (req, res) => {
//   const options = Array.isArray(req.body?.options) ? req.body.options : [];
//   let added = 0;

//   for (const o of options) {
//     const entry = JSON.stringify({
//       underlying: String(o.underlying).toUpperCase(),
//       expiration: String(o.expiration),
//       strike: Number(o.strike),
//       right: String(o.right).toUpperCase().startsWith("C") ? "C" : "P"
//     });
//     if (!tradierOptWatch.set.has(entry)) { tradierOptWatch.set.add(entry); added++; }
//   }

//   if (!MOCK) {
//     await ibEnsureAccounts();
//     await ibWsEnsure();
//     // Group by UL and ensure chains + specific options
//     const groups = {};
//     for (const o of options) {
//       const ul = String(o.underlying).toUpperCase();
//       groups[ul] = groups[ul] || [];
//       groups[ul].push(o);
//     }
//     for (const [ul, list] of Object.entries(groups)) {
//       await ensureChainsForUL(ul);
//       const ulConid = await ibConidForStock(ul);
//       if (!ulConid) continue;
//       for (const o of list) {
//         const exp = String(o.expiration);
//         const right = String(o.right).toUpperCase().startsWith("C") ? "CALL" : "PUT";
//         const k = Number(o.strike);
//         const conid = await ibOptionConid(ulConid, { ul, expISO: exp, right, strike: k });
//         if (!conid) continue;
//         const occ = toOcc(ul, exp, right, k);
//         occToConid.set(occ, conid);
//         optionMetaByConid.set(conid, { ul, exp, right, strike: k, occ });
//         ibWsSubscribe(conid);
//       }
//     }
//   }

//   res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) }, added });
// });

app.post("/watch/tradier", express.json(), async (req, res) => {
  const opts = Array.isArray(req.body?.options) ? req.body.options : [];
  const added = await watchAddOptions(opts);
  res.json({ ok: true, watching: getWatchlist(), added });
});

app.delete("/watch/tradier", (req, res) => {
  const options = Array.isArray(req.body?.options) ? req.body.options : [];
  let removed = 0;
  for (const o of options) {
    const entry = JSON.stringify({
      underlying: String(o.underlying).toUpperCase(),
      expiration: String(o.expiration),
      strike: Number(o.strike),
      right: String(o.right).toUpperCase().startsWith("C") ? "C" : "P"
    });
    if (tradierOptWatch.set.delete(entry)) removed++;
  }
  res.json({ ok: true, removed, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
});

// app.post("/watch/alpaca", async (req, res) => {
//   const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
//   let added = 0;
//   for (const s of equities) if (!alpacaSubs.has(s)) { alpacaSubs.add(s); added++; }
//   if (!MOCK) {
//     await ibEnsureAccounts();
//     await ibWsEnsure();
//     for (const s of equities) {
//       const conid = await ibConidForStock(s);
//       if (conid) {
//         eqConids.set(s, conid);
//         symbolByConid.set(conid, s);
//         ibWsSubscribe(conid);
//       }
//       await ensureChainsForUL(s);
//     }
//   }
//   res.json({ ok: true, watching: { equities: Array.from(alpacaSubs) }, added });
// });
app.post("/watch/alpaca", express.json(), async (req, res) => {
  const equities = Array.isArray(req.body?.equities) ? req.body.equities : [];
  const added = await watchAddEquities(equities);
  res.json({ ok: true, watching: getWatchlist(), added });
});

app.delete("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  let removed = 0;
  for (const s of equities) if (alpacaSubs.delete(s)) removed++;
  res.json({ ok: true, removed, watching: { equities: Array.from(alpacaSubs) } });
});

// app.get("/watchlist", (_req, res) => {
//   res.json({ equities: Array.from(alpacaSubs), options: Array.from(tradierOptWatch.set).map(JSON.parse) });
// });
app.get("/watchlist", (_req, res) => {
  res.json(getWatchlist());
});

/* ================= CHAIN EMITTER (public helper) ================= */
async function ensureChainsForUL(symbol) {
  const ulConid = await ibConidForStock(symbol);
  if (!ulConid) return;

  const last = await ibGetUlLast(symbol, ulConid);
  const { expirations, strikes } = await ibGetStrikesExp(symbol, ulConid, last);
  if (!expirations.length || !strikes.length) return;

  const bestStrikes = pickStrikesNear(strikes, last, MAX_STRIKES_AROUND_ATM);
  for (const exp of expirations) {
    pushAndFanout({
      type: "chains",
      provider: "ibkr",
      ts: Date.now(),
      symbol,
      expiration: exp,
      strikes: bestStrikes,
      strikesCount: bestStrikes.length
    });
  }
}

/* ================= ANALYTICS / DEBUG ================= */
function confirmOccForDate(occ, dateStr) {
  const d0 = new Date(dateStr);
  const prev = new Date(d0.getTime() - 24*3600*1000);
  const prevDate = prev.toISOString().slice(0,10);

  const oiPrev = eodOiByDateOcc.get(`${prevDate}|${occ}`);
  const oiCurr = eodOiByDateOcc.get(`${dateStr}|${occ}`);

  if (!Number.isFinite(oiPrev) || !Number.isFinite(oiCurr)) return 0;
  const delta = oiCurr - oiPrev;

  const confirmOne = (m) => {
    if (m.occ !== occ) return false;
    const day = ymdFromTs(m.ts || Date.now());
    if (day !== dateStr) return false;

    let oc_confirm = "INCONCLUSIVE";
    let reason = `ΔOI=${delta} from ${prevDate}→${dateStr}`;
    if (delta > 0 && (m.oc_intent === "BTO" || m.oc_intent === "STO")) {
      oc_confirm = "OPEN_CONFIRMED"; reason += " (OI increased → opens)";
    } else if (delta < 0 && (m.oc_intent === "BTC" || m.oc_intent === "STC")) {
      oc_confirm = "CLOSE_CONFIRMED"; reason += " (OI decreased → closes)";
    }
    m.oi_after = oiCurr;
    m.oi_delta = delta;
    m.oc_confirm = oc_confirm;
    m.oc_confirm_reason = reason;
    m.oc_confirm_ts = Date.now();
    return true;
  };

  let touched = 0;
  for (const arrName of ["options_ts", "sweeps", "blocks"]) {
    const arr = buffers[arrName];
    for (const m of arr) if (confirmOne(m)) touched++;
  }
  return touched;
}
function recordEodRows(dateStr, rows) {
  let n = 0;
  for (const r of rows) {
    let occ = r.occ;
    if (!occ && r.underlying && r.expiration && r.right && Number.isFinite(Number(r.strike))) {
      occ = toOcc(String(r.underlying).toUpperCase(), String(r.expiration), String(r.right).toUpperCase(), Number(r.strike));
    }
    const oi = Number(r.oi);
    if (!occ || !Number.isFinite(oi)) continue;
    eodOiByDateOcc.set(`${dateStr}|${occ}`, oi);
    lastEodDateForOcc.set(occ, dateStr);
    n++;
  }
  return n;
}

app.post("/analytics/oi-snapshot", (req, res) => {
  try {
    const dateStr = String(req.body?.date || "").slice(0,10);
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      return res.status(400).json({ error: "date must be YYYY-MM-DD" });
    }
    const recorded = recordEodRows(dateStr, rows);

    let confirmedCount = 0;
    const occs = new Set();
    for (const r of rows) {
      const occ = r.occ
        || (r.underlying && r.expiration && r.right && Number.isFinite(Number(r.strike))
            ? toOcc(String(r.underlying).toUpperCase(), String(r.expiration), String(r.right).toUpperCase(), Number(r.strike))
            : null);
      if (occ) occs.add(occ);
    }
    for (const occ of occs) confirmedCount += confirmOccForDate(occ, dateStr);

    res.json({ ok: true, recorded, confirmed: confirmedCount });
  } catch (e) {
    console.error("/analytics/oi-snapshot error", e);
    res.status(500).json({ error: "internal error" });
  }
});

app.post("/analytics/confirm", (req, res) => {
  try {
    const dateStr = String(req.body?.date || "").slice(0,10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      return res.status(400).json({ error: "date must be YYYY-MM-DD" });
    }
    const provided = Array.isArray(req.body?.occs) ? new Set(req.body.occs.map(String)) : null;

    const occs = new Set();
    for (const key of eodOiByDateOcc.keys()) {
      const [d, occ] = key.split("|");
      if (d === dateStr && (!provided || provided.has(occ))) occs.add(occ);
    }

    let confirmedCount = 0;
    for (const occ of occs) confirmedCount += confirmOccForDate(occ, dateStr);

    res.json({ ok: true, date: dateStr, occs: Array.from(occs), confirmed: confirmedCount });
  } catch (e) {
    console.error("/analytics/confirm error", e);
    res.status(500).json({ error: "internal error" });
  }
});

app.get("/debug/oi", (req, res) => {
  const occ = String(req.query?.occ || "");
  const dateStr = String(req.query?.date || "");
  if (!occ || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
    return res.status(400).json({ error: "need occ and date=YYYY-MM-DD" });
  }
  const prevDate = new Date(dateStr);
  prevDate.setUTCDate(prevDate.getUTCDate() - 1);
  const prev = prevDate.toISOString().slice(0,10);
  const oiPrev = eodOiByDateOcc.get(`${prev}|${occ}`);
  const oiCurr = eodOiByDateOcc.get(`${dateStr}|${occ}`);
  const delta = (Number.isFinite(oiPrev) && Number.isFinite(oiCurr)) ? (oiCurr - oiPrev) : null;
  res.json({ occ, prev, date: dateStr, oiPrev, oiCurr, delta });
});

// Debug: force normalization (optional)
app.post("/debug/normalize-watch", async (_req, res) => {
  for (const s of alpacaSubs) await ensureChainsForUL(s);
  res.json({ ok: true });
});

app.get("/debug/expirations", async (req, res) => {
  const symbol = String(req.query?.symbol || "").toUpperCase();
  if (!symbol) return res.status(400).json({ error: "missing symbol" });
  const conid = await ibConidForStock(symbol);
  if (!conid) return res.json({ symbol, conid: null, expirations: [] });
  const last = await ibGetUlLast(symbol, conid);
  const { expirations } = await ibGetStrikesExp(symbol, conid, last);
  res.json({ symbol, conid, expirations });
});

/* ================= FLOW APIs (unchanged) ================= */
app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
// app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));
// app.get("/api/flow/chains", async (req, res) => {
//   try {
//     const param = String(req.query.symbols || "").trim();
//     const syms = param
//       ? param.split(",").map(s => s.trim().toUpperCase()).filter(Boolean)
//       : Array.from(WATCH.equities || []); // fallback to your watchlist

//     if (!syms.length) return res.json([]);

//     const out = [];
//     for (const s of syms) {
//       try {
//         const built = await buildOptionChainForSymbol(s);
//         // only push if we have at least one expiration (so UI has content)
//         if (built.chains && built.chains.length) out.push(built);
//       } catch (e) {
//         console.warn("chain build failed for", s, e?.message || e);
//       }
//     }
//     res.json(out);
//   } catch (e) {
//     console.error(e);
//     res.status(500).json({ error: String(e?.message || e) });
//   }
// });
app.get("/api/flow/chains", async (req, res) => {
  try {
    const param = String(req.query.symbols || "").trim();
    const syms = param
      ? param.split(",").map(s => s.trim().toUpperCase()).filter(Boolean)
      : Array.from(WATCH.equities || []);   // uses the global singleton

    if (!syms.length) return res.json([]);

    const out = [];
    for (const s of syms) {
      try {
        const built = await buildOptionChainForSymbol(s); // your chain builder
        if (built?.chains?.length) out.push(built);
      } catch (e) {
        console.warn("chain build failed for", s, e?.message || e);
      }
    }
    res.json(out);
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// --- WATCHLIST SINGLETON + PERSISTENCE ---------------------------------
import fs from "fs/promises";

const WATCH_FILE = "./watchlist.json";

// Keep as Sets in-memory (easy dedupe); serialize to arrays on disk.
globalThis.WATCH = globalThis.WATCH ?? {
  equities: new Set(),                 // e.g., "AAPL"
  options: new Set(),                  // JSON-stringified leg objects
};

function normOptionLeg(o) {
  return {
    underlying: String(o.underlying || o.ul || "").toUpperCase(),
    expiration: String(o.expiration || o.exp || ""),
    strike: Number(o.strike),
    right: String(o.right || o.r || "").slice(0,1).toUpperCase(), // C/P
  };
}

async function loadWatch() {
  try {
    const raw = await fs.readFile(WATCH_FILE, "utf8");
    const j = JSON.parse(raw || "{}");
    (j.equities || []).forEach(s => s && WATCH.equities.add(String(s).toUpperCase()));
    (j.options || []).forEach(o => {
      const leg = normOptionLeg(o);
      if (leg.underlying && leg.expiration && Number.isFinite(leg.strike) && (leg.right === "C" || leg.right === "P")) {
        WATCH.options.add(JSON.stringify(leg));
      }
    });
  } catch (_) { /* first run: no file yet */ }
}

async function saveWatch() {
  const out = {
    equities: Array.from(WATCH.equities),
    options: Array.from(WATCH.options).map(s => JSON.parse(s)),
  };
  await fs.writeFile(WATCH_FILE, JSON.stringify(out, null, 2));
}

// Call once during startup (or before app.listen)
await loadWatch();

// Helpers you can use in your existing /watch/* routes:
async function watchAddEquities(list) {
  let added = 0;
  for (const s of list) {
    const sym = String(s).toUpperCase();
    if (sym && !WATCH.equities.has(sym)) { WATCH.equities.add(sym); added++; }
  }
  if (added) await saveWatch();
  return added;
}

async function watchAddOptions(list) {
  let added = 0;
  for (const o of list || []) {
    const leg = normOptionLeg(o);
    const key = JSON.stringify(leg);
    if (!WATCH.options.has(key) && leg.underlying && leg.expiration && Number.isFinite(leg.strike) && (leg.right === "C" || leg.right === "P")) {
      WATCH.options.add(key);
      added++;
    }
  }
  if (added) await saveWatch();
  return added;
}

// Optional: expose current watchlist
function getWatchlist() {
  return {
    equities: Array.from(WATCH.equities),
    options: Array.from(WATCH.options).map(s => JSON.parse(s)),
  };
}


async function ibTickleSafe() {
  try { await ibFetch(`/iserver/tickle`); }
  catch (e) {
    if (String(e?.message || "").includes("404")) return; // ignore
    console.warn("tickle err:", e?.message || e);
  }
}
app.get("/health",               (_, res) => res.json({ ok: true }));
app.get("/debug/metrics",        (_req, res) => res.json({ mock: MOCK, count: httpMetrics.length, last5: httpMetrics.slice(-5) }));

app.use((req, res) => { res.status(404).json({ error: `Not found: ${req.method} ${req.originalUrl}` }); });
app.use((err, req, res, next) => {
  const o = req.headers.origin;
  if (o && isAllowed(o)) { res.setHeader("Access-Control-Allow-Origin", o); res.setHeader("Vary","Origin"); }
  res.status(err.status || 500).json({ ok:false, error: String(err.message || err) });
});

/* ================= START ================= */
server.listen(PORT, () => console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}  IB=${IB_BASE}`));

