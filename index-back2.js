/* eslint-disable no-console */
/**
 * IBKR VERSION — Drop-in replacement for your original server.
 * - Replaces Tradier + Alpaca with Interactive Brokers Client Portal Web API (equities & options).
 * - Keeps your routes, buffers, normalization, blocks/sweeps logic, etc.
 *
 * HOW TO RUN
 * 1) Start IBKR Client Portal Gateway locally and log in.
 * 2) Export env vars, then run:  node index.js
 *
 * REQUIRED ENV
 *   PORT=8080
 *   IB_HOST=127.0.0.1
 *   IB_PORT=5000
 *   IB_SSL=1                  # 1=https, 0=http
 *   IB_COOKIE="ibkr cookies"  # Paste cookie header value from the CP Gateway session (e.g. "ib=...; oneib=...")
 *   IB_ALLOW_INSECURE=1       # optional; allow self-signed TLS from CP Gateway
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

// ===== INSECURE TLS (for IBKR CP Gateway self-signed certs) =====
if (process.env.IB_ALLOW_INSECURE === "1") {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

/* ================= CONFIG ================= */
const rawMock = (process.env.MOCK ?? "").toString().trim();
export const MOCK = rawMock === "1";
const PORT = process.env.PORT || 8080;

// -------- IBKR Client Portal Web API base --------
const IB_HOST = process.env.IB_HOST || "127.0.0.1";
const IB_PORT = Number(process.env.IB_PORT || 5000);
const IB_SSL  = (process.env.IB_SSL ?? "1") !== "0";
const IB_PROTO = IB_SSL ? "https" : "http";
const IB_WS_PROTO = IB_SSL ? "wss" : "ws";
const IB_BASE = `${IB_PROTO}://${IB_HOST}:${IB_PORT}/v1/api`;
const IB_WS_URL = `${IB_WS_PROTO}://${IB_HOST}:${IB_PORT}/v1/api/ws`;
const IB_COOKIE = process.env.IB_COOKIE || ""; // full "Cookie:" value from CP Gateway session

// chain auto-expand
const MAX_STRIKES_AROUND_ATM = 40;   // total (±20)
const MAX_EXPIRY_DAYS        = 30;

// sweep / block thresholds
const SWEEP_WINDOW_MS        = 600;
const SWEEP_MIN_QTY          = 300;
const SWEEP_MIN_NOTIONAL     = 75000;

const BLOCK_MIN_QTY          = 250;
const BLOCK_MIN_NOTIONAL     = 100000;

// fallback polling (IBKR has snapshots; we use low-rate snapshot fallback)
const FALLBACK_POLL_EVERY_MS = 2500;

/* ================= APP / WS ================= */
const app = express();
// CORS — allow your site + local dev, send headers on all responses (incl. 304)
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

/* ===== EOD OI storage for confirmation ===== */
const eodOiByDateOcc = new Map(); // key: `${date}|${occ}` -> number
const lastEodDateForOcc = new Map(); // occ -> last date string we saw for that occ

/* ================= STATE ================= */
// equities NBBO (bid/ask), options NBBO (by OCC or conid)
const eqQuote  = new Map(); // symbol -> { bid, ask, ts }
const optNBBO  = new Map(); // occ     -> { bid, ask, ts }
// OI / VOL baselines per OCC key (UL|YYYY-MM-DD|strike|right)
const optState = new Map();
const dayKey = () => new Date().toISOString().slice(0,10);

// watches (keep original names for client compatibility)
const alpacaSubs = new Set();               // equities (symbol strings)
const tradierOptWatch = { set: new Set() }; // option JSON strings

// fallback + sweeps
const optLastPrintTs = new Map(); // occ -> last ts seen
const sweepBuckets   = new Map(); // occ -> burst bucket

// lean memory of opens
const recentOpenLean = new Map(); // occ -> "BTO" | "STO" | "UNK"

// IBKR-specific caches
const conidBySymbol = new Map();          // "AAPL" -> 265598
const symbolByConid = new Map();          // 265598 -> "AAPL"
const optionMetaByConid = new Map();      // conid(opt) -> { ul, exp, right, strike, occ }
const conidsSubscribed = new Set();       // all conids we subscribed to via ws
const eqConids = new Map();               // symbol -> conid
const occToConid = new Map();             // occ -> conid

// last trade dedupe by conid (price,size)
const lastTradeKeyByConid = new Map();    // conid -> "p@s"

/* ================= SMALL UTILS ================= */
function ymdFromTs(ts) {
  const d = new Date(ts);
  return d.toISOString().slice(0,10);
}
function within30Days(dISO) {
  const t = Date.parse(dISO);
  if (!Number.isFinite(t)) return false;
  const now = Date.now();
  return (t - now) <= MAX_EXPIRY_DAYS * 24 * 3600 * 1000 && (t > now - (12*3600*1000)); // allow today+future
}

/* ================== UNIQUE KEYS (fix duplicate React keys) ================== */
function contentKeyForId(msg) {
  return {
    type: msg.type,
    provider: msg.provider ?? null,
    symbol: msg.symbol ?? msg.underlying ?? null,
    occ: msg.occ ?? null,
    side: msg.side ?? null,
    price: Number(msg.price ?? 0),
    size: Number(msg.qty ?? msg.size ?? 0),
    option: msg.option ? {
      expiration: msg.option.expiration,
      right: msg.option.right,
      strike: Number(msg.option.strike ?? 0),
    } : null,
  };
}
function stableIdFromContent(msg) {
  const keyStr = JSON.stringify(contentKeyForId(msg));
  return createHash("sha1").update(keyStr).digest("hex");
}
const perTypeIdCounts = new Map(); // type -> Map(baseId -> count)
function uniquePerTypeId(type, baseId) {
  let byBase = perTypeIdCounts.get(type);
  if (!byBase) { byBase = new Map(); perTypeIdCounts.set(type, byBase); }
  const n = (byBase.get(baseId) || 0) + 1;
  byBase.set(baseId, n);
  return n === 1 ? `${type}:${baseId}` : `${type}:${baseId}#${n}`;
}
function normalizeForFanout(msg) {
  const withTs = { ts: typeof msg.ts === "number" ? msg.ts : Date.now(), ...msg };
  const base = stableIdFromContent(withTs);
  const id = uniquePerTypeId(withTs.type, base);
  const renderKey = `${id}:${withTs.ts}`; // optional, ultra-unique
  return { id, renderKey, ...withTs };
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

/* ================= HELPERS ================= */
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

/* ---- aggressor from NBBO (for options) ---- */
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

/* ---- opening/closing inference ---- */
function inferIntent(occ, side, qty, price) {
  const s = getOptState(occ);
  const nbbo = optNBBO.get(occ);
  const { side: aggrSide, at } = classifyAggressor(price, nbbo);

  let tag = "UNK"; // "BTO"|"STO"|"BTC"|"STC"|"UNK"
  let conf = 0.35;
  const reasons = [];

  if (qty > (s.oi_before + s.vol_today_before)) {
    tag = side === "BUY" ? "BTO" : "STO";
    conf = 0.8;
    reasons.push("qty > (yday OI + today vol)");
    recentOpenLean.set(occ, tag);
  } else {
    const prior = recentOpenLean.get(occ) || "UNK";
    const qtyWithinOI = qty <= s.oi_before;
    if (qtyWithinOI) reasons.push("qty <= yday OI");

    if (prior === "BTO" && side === "SELL" && (at === "BID" || aggrSide === "SELL")) {
      tag = "STC"; conf = qtyWithinOI ? 0.7 : 0.55;
      reasons.push("prior=open(BTO)", "sell@bid");
    } else if (prior === "STO" && side === "BUY" && (at === "ASK" || aggrSide === "BUY")) {
      tag = "BTC"; conf = qtyWithinOI ? 0.7 : 0.55;
      reasons.push("prior=open(STO)", "buy@ask");
    } else {
      if (side === "SELL" && at === "BID" && qtyWithinOI) { tag = "STC"; conf = 0.55; reasons.push("sell@bid"); }
      if (side === "BUY"  && at === "ASK" && qtyWithinOI) { tag = "BTC"; conf = 0.55; reasons.push("buy@ask"); }
    }
  }

  return { tag, conf, at, reasons, oi_yday: s.oi_before, vol_before: s.vol_today_before };
}

/* ---- normalized prints ---- */
function normOptionsPrint(provider, p) {
  const now = Date.now();
  const key = `${p.underlying}|${p.option.expiration}|${p.option.strike}|${p.option.right}`;
  if (Number.isFinite(p.oi)) setOptOI(key, Number(p.oi));
  if (Number.isFinite(p.volToday)) setOptVol(key, Number(p.volToday));
  const occ = p.occ ?? toOcc(p.underlying, p.option.expiration, p.option.right, p.option.strike);

  const intent = inferIntent(occ, p.side, p.size, p.price);
  bumpOptVol(key, p.size);

  return {
    id: `${now}-${Math.random().toString(36).slice(2,8)}`, // replaced by normalizeForFanout
    ts: now,
    type: "options_ts",
    provider,
    symbol: p.underlying,
    occ,
    option: p.option,
    side: p.side,        // BUY | SELL
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
    id: `${now}-${Math.random().toString(36).slice(2,8)}`, // replaced by normalizeForFanout
    ts: now,
    type: "equity_ts",
    provider,
    symbol,
    side, qty: size, price,
    action: "UNK",
    venue: null
  };
}

/* ================= IBKR SESSION PRIMING (fixes /accounts + bridge) ================= */
let ibPrimed = false;
let ibLastPrime = 0;
let ibAccountIds = [];

async function ibPrimeSession() {
  if (MOCK) { ibPrimed = true; return true; }
  const now = Date.now();
  if (ibPrimed && (now - ibLastPrime) < 30000) return true; // throttle re-prime to 30s

  const st = await ibFetch(`/iserver/auth/status`);
  if (!st.ok) return false;

  const acc = await ibFetch(`/iserver/accounts`);
  if (!acc.ok || !acc.data) return false;

  // Some gateways need /iserver/reauthorize or /iserver/marketdata/snapshot after accounts;
  // doing a harmless echo to warm the "bridge":
  await ibFetch(`/portfolio/accounts`);

  ibAccountIds = (acc.data?.accounts || acc.data?.[0]?.accounts || acc.data?.accountIds || []).map(String);
  ibPrimed = true;
  ibLastPrime = Date.now();
  console.log(`[IB] primed session. accounts=${ibAccountIds.join(",") || "?"}`);
  return true;
}
// === Add near other IBKR globals ===
// let IB_BOOT = { authed: false, accountsReady: false, lastAccountCheck: 0 };

// ---- helpers ----
// async function ibEnsureAuth() {
//   // Lightweight cache ~30s
//   if (IB_BOOT.authed && Date.now() - IB_BOOT.lastAccountCheck < 30000) return true;
//   const r = await fetch(`${IB_BASE}/iserver/auth/status`, {
//     headers: { Accept: "application/json", ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}) }
//   });
//   const data = await r.json().catch(()=>null);
//   IB_BOOT.authed = !!(data && (data.authenticated || data.connected || data.competing || data.requires_twofa === false));
//   return IB_BOOT.authed;
// }

// async function ibEnsureAccounts() {
//   // Throttle to avoid hammering the gateway
//   if (IB_BOOT.accountsReady && Date.now() - IB_BOOT.lastAccountCheck < 30000) return true;
//   await ibEnsureAuth();
//   const r = await fetch(`${IB_BASE}/iserver/accounts`, {
//     headers: { Accept: "application/json", ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}) }
//   });
//   const ok = r.ok;
//   IB_BOOT.accountsReady = ok;
//   IB_BOOT.lastAccountCheck = Date.now();
//   if (!ok) {
//     const t = await r.text();
//     console.warn(`[IBHTTP ACCOUNTS] ${r.status} ${t}`);
//   }
//   return ok;
// }
let IB_BOOT = { authed: false, accountsReady: false, lastCheck: 0 };

async function ibWake() {
  // harmless ping that often clears "no bridge"
  const r = await fetch(`${IB_BASE}/tickle`, {
    headers: { Accept: "application/json", ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}) }
  }).catch(() => null);
  return !!r;
}

async function ibReauth() {
  const r = await fetch(`${IB_BASE}/iserver/reauthenticate`, {
    method: "POST",
    headers: { Accept: "application/json", ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}) }
  }).catch(() => null);
  return !!r && r.ok;
}

async function ibEnsureAuth() {
  if (IB_BOOT.authed && Date.now() - IB_BOOT.lastCheck < 30000) return true;
  const r = await fetch(`${IB_BASE}/iserver/auth/status`, {
    headers: { Accept: "application/json", ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}) }
  }).catch(() => null);
  const data = r && await r.json().catch(() => null);
  IB_BOOT.authed = !!(data && (data.authenticated || data.connected));
  return IB_BOOT.authed;
}

async function ibEnsureAccounts() {
  // throttle checks
  if (IB_BOOT.accountsReady && Date.now() - IB_BOOT.lastCheck < 30000) return true;

  await ibEnsureAuth();
  await ibWake();                 // nudge the backend
  await ibReauth();               // start the brokerage bridge

  const callAccounts = async () => fetch(`${IB_BASE}/iserver/accounts`, {
    headers: { Accept: "application/json", ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}) }
  });

  let r = await callAccounts().catch(() => null);
  if (r && !r.ok) {
    const t = await r.text();
    if (/no bridge/i.test(t)) {
      // try one more time after a brief delay and another reauth
      await new Promise(res => setTimeout(res, 400));
      await ibWake();
      await ibReauth();
      r = await callAccounts().catch(() => null);
    }
  }

  IB_BOOT.accountsReady = !!(r && r.ok);
  IB_BOOT.lastCheck = Date.now();
  if (!IB_BOOT.accountsReady) {
    const t = r ? await r.text() : "no response";
    console.warn(`[IBHTTP ACCOUNTS] ${r ? r.status : "ERR"} ${t}`);
  }
  return IB_BOOT.accountsReady;
}
async function ibFetch(path, opts = {}) {
  const url = `${IB_BASE}${path}`;
  const headers = {
    Accept: "application/json",
    ...(IB_COOKIE ? { Cookie: IB_COOKIE } : {}),
    ...(opts.headers || {})
  };

  // Ensure bridge is ready before endpoints that need brokerage context
  const needsBridge =
    /^\/iserver\/(accounts|marketdata|secdef)\//.test(path) ||
    /^\/trsrv\//.test(path);

  if (needsBridge) {
    await ibEnsureAccounts();
  }

  const doFetch = () => fetch(url, { ...opts, headers });

  let res = await doFetch().catch(() => null);
  let text = res ? await res.text() : "";
  let json = null; try { json = text ? JSON.parse(text) : null; } catch {}

  // Auto-recover if backend says "no bridge"
  if (!(res && res.ok)) {
    const msg = (json && (json.error || json.message)) || text || "";
    if (/no bridge/i.test(msg) || /Please query \/accounts first/i.test(msg)) {
      console.warn(`[IBHTTP RETRY ${opts.method||"GET"}] ${url} -> ${res ? res.status : "ERR"} ${msg}`);
      await ibWake();
      await ibReauth();
      await ibEnsureAccounts();
      res = await doFetch().catch(() => null);
      text = res ? await res.text() : "";
      try { json = text ? JSON.parse(text) : null; } catch {}
    }
  }

  if (!(res && res.ok)) {
    console.warn(`[IBHTTP ${opts.method||"GET"}] ${url} -> ${res ? res.status : "ERR"}`, json || text);
  }
  return { ok: !!(res && res.ok), status: res ? res.status : 0, data: json, raw: text };
}
/* ================= IBKR: CONID resolution ================= */
async function ibConidForStock(symbol) {
  symbol = symbol.toUpperCase();
  if (conidBySymbol.has(symbol)) return conidBySymbol.get(symbol);
  await ibPrimeSession();
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

/* ================= IBKR: snapshot for fields ================= */
const F_BID = 84, F_ASK = 86, F_LAST = 31, F_BIDSZ = 88, F_ASKSZ = 85, F_LASTSZ = 7059;
async function ibSnapshot(conids, fields = [F_LAST, F_BID, F_ASK]) {
  await ibPrimeSession();
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

/* ================= IBKR: options chain expansion ================= */
async function ibGetUlLast(symbol, conid) {
  const m = await ibSnapshot([conid], [F_LAST, F_BID, F_ASK]);
  const row = m.get(conid);
  const last = row?.last || row?.bid || row?.ask || 0;
  return Number(last) || 0;
}

// async function ibGetStrikesExp(symbol, conid, ulPrice) {
//   const today = new Date();
//   const months = [];
//   for (let i=0; i<4; i++) {
//     const d = new Date(today); d.setMonth(d.getMonth()+i);
//     const yyyy = d.getUTCFullYear();
//     const mm = String(d.getUTCMonth()+1).padStart(2,"0");
//     months.push(`${yyyy}${mm}`);
//   }
//   const expirations = new Set();
//   const strikesSet = new Set();

//   for (const mStr of months) {
//     const q = `/iserver/secdef/strikes?conid=${conid}&sectype=OPT&month=${mStr}&exchange=SMART&underlyingPrice=${encodeURIComponent(ulPrice)}`;
//     const r = await ibFetch(q);
//     const exps = r?.data?.expirations || r?.data?.optExpDate || r?.data?.expirationsMonth || [];
//     const strikes = r?.data?.strikes || r?.data?.strike || [];
//     for (const e of exps) expirations.add(String(e));
//     for (const s of strikes) if (Number.isFinite(Number(s))) strikesSet.add(Number(s));
//   }
//   const normalizedExps = Array.from(expirations).map(e => {
//     if (/^\d{4}-\d{2}-\d{2}$/.test(e)) return e;
//     if (/^\d{8}$/.test(e)) return `${e.slice(0,4)}-${e.slice(4,6)}-${e.slice(6,8)}`;
//     return e;
//   }).filter(within30Days);

//   return { expirations: normalizedExps.sort(), strikes: Array.from(strikesSet).sort((a,b)=>a-b) };
// }

// async function ibGetStrikesExp(symbol, conid, ulPrice) {
//   const today = new Date();
//   const months = [];
//   for (let i = 0; i < 4; i++) {
//     const d = new Date(today); d.setMonth(d.getMonth() + i);
//     const yyyy = d.getUTCFullYear();
//     const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
//     months.push(`${yyyy}${mm}`);
//   }
//   const expirations = new Set();
//   const strikesSet = new Set();

//   for (const mStr of months) {
//     // ---- FIX: only include underlyingPrice when > 0
//     const qp = new URLSearchParams({
//       conid: String(conid),
//       sectype: "OPT",
//       month: mStr,
//       exchange: "SMART",
//     });
//     if (Number(ulPrice) > 0) qp.set("underlyingPrice", String(ulPrice));

//     const q = `/iserver/secdef/strikes?${qp.toString()}`;
//     const r = await ibFetch(q);
//     const exps = r?.data?.expirations || r?.data?.optExpDate || r?.data?.expirationsMonth || [];
//     const strikes = r?.data?.strikes || r?.data?.strike || [];
//     for (const e of exps) expirations.add(String(e));
//     for (const s of strikes) if (Number.isFinite(Number(s))) strikesSet.add(Number(s));
//   }

//   const normalizedExps = Array.from(expirations)
//     .map(e => (/^\d{8}$/.test(e) ? `${e.slice(0,4)}-${e.slice(4,6)}-${e.slice(6,8)}` : e))
//     .filter(within30Days)
//     .sort();

//   return { expirations: normalizedExps, strikes: Array.from(strikesSet).sort((a,b) => a - b) };
// }

// function pickStrikesNear(strikes, center, limit = MAX_STRIKES_AROUND_ATM) {
//   if (!Number.isFinite(center) || !strikes?.length) return [];
//   return strikes
//     .map(s => ({ s, d: Math.abs(s - center) }))
//     .sort((a,b)=>a.d - b.d)
//     .slice(0, Math.min(limit, strikes.length))
//     .map(x => x.s)
//     .sort((a,b)=>a-b);
// }
// function pickStrikesNear(strikes, center, limit = MAX_STRIKES_AROUND_ATM) {
//   if (!strikes?.length) return [];
//   const c = Number.isFinite(center) && center > 0
//     ? center
//     : strikes[Math.floor(strikes.length / 2)]; // fallback center
//   return strikes
//     .map(s => ({ s, d: Math.abs(s - c) }))
//     .sort((a,b)=>a.d - b.d)
//     .slice(0, Math.min(limit, strikes.length))
//     .map(x => x.s)
//     .sort((a,b)=>a-b);
// }
// async function ibGetStrikesExp(symbol, conid, ulPrice) {
//   const today = new Date();
//   const months = [];
//   for (let i = 0; i < 4; i++) {
//     const d = new Date(today); d.setMonth(d.getMonth() + i);
//     const yyyy = d.getUTCFullYear();
//     const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
//     months.push(`${yyyy}${mm}`);
//   }

//   const expirations = new Set();
//   const strikesSet = new Set();

//   for (const mStr of months) {
//     const qp = new URLSearchParams({ conid: String(conid), sectype: "OPT", month: mStr, exchange: "SMART" });
//     if (Number(ulPrice) > 0) qp.set("underlyingPrice", String(ulPrice));

//     const q = `/iserver/secdef/strikes?${qp.toString()}`;
//     const r = await ibFetch(q);
//     const exps = r?.data?.expirations || r?.data?.optExpDate || r?.data?.expirationsMonth || [];
//     const strikes = r?.data?.strikes || r?.data?.strike || [];
//     for (const e of exps) expirations.add(String(e));
//     for (const s of strikes) if (Number.isFinite(Number(s))) strikesSet.add(Number(s));
//   }

//   const normalizedExps = Array.from(expirations)
//     .map(e => (/^\d{8}$/.test(e) ? `${e.slice(0,4)}-${e.slice(4,6)}-${e.slice(6,8)}` : e))
//     .filter(within30Days)
//     .sort();

//   return {
//     expirations: normalizedExps,
//     strikes: Array.from(strikesSet).sort((a,b)=>a-b)
//   };
// }

async function ibGetStrikesExp(symbol, conid, ulPrice) {
  // ask several near months to discover expirations; some gateways need month
  const today = new Date();
  const months = [];
  for (let i = 0; i < 4; i++) {
    const d = new Date(today); d.setMonth(d.getMonth() + i);
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    months.push(`${yyyy}${mm}`); // YYYYMM
  }

  const expirationsRaw = new Set();
  const strikesSet = new Set();

  // helper to deeply collect any array of YYYYMMDD/ YYYY-MM-DD under typical keys
  const collect = (obj) => {
    if (!obj || typeof obj !== "object") return;
    const maybeArrs = [
      obj.expirations, obj.optExpDate, obj.expirationsMonth, obj.expiration, obj.expiry, obj.expDates
    ].filter(Boolean);
    for (const v of maybeArrs) {
      if (Array.isArray(v)) for (const e of v) expirationsRaw.add(String(e));
      else if (typeof v === "object") collect(v);
    }
    const strikes = obj.strikes || obj.strike || obj.strikePrices;
    if (Array.isArray(strikes)) for (const s of strikes) if (Number.isFinite(Number(s))) strikesSet.add(Number(s));
  };

  for (const mStr of months) {
    const q = `/iserver/secdef/strikes?conid=${conid}&sectype=OPT&month=${mStr}&exchange=SMART` +
              (Number.isFinite(ulPrice) && ulPrice > 0 ? `&underlyingPrice=${encodeURIComponent(ulPrice)}` : "");
    const r = await ibFetch(q);
    if (r?.data) collect(r.data);
  }

  // normalize expirations -> YYYY-MM-DD and keep only within 30 days
  const normalizeExp = (e) => {
    const s = String(e);
    if (/^\d{8}$/.test(s)) return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    return null;
  };
  const exps30d = Array.from(expirationsRaw)
    .map(normalizeExp)
    .filter(Boolean)
    .filter(within30Days)
    .sort();

  return { expirations: exps30d, strikes: Array.from(strikesSet).sort((a,b)=>a-b) };
}

function pickStrikesNear(strikes, center, limit = MAX_STRIKES_AROUND_ATM) {
  if (!strikes?.length) return [];
  const c = Number.isFinite(center) && center > 0
    ? center
    : strikes[Math.floor(strikes.length / 2)]; // fallback to median
  return strikes
    .map(s => ({ s, d: Math.abs(s - c) }))
    .sort((a,b)=>a.d - b.d)
    .slice(0, Math.min(limit, strikes.length))
    .map(x => x.s)
    .sort((a,b)=>a-b);
}
// async function ensureChainsForUL(symbol) {
//   const ul = String(symbol).toUpperCase();
//   const ulConid = await ibConidForStock(ul);
//   if (!ulConid) return;

//   // try to center strikes on live last; fall back to median
//   let last = 0;
//   try {
//     const snap = await ibSnapshot([ulConid], [F_LAST, F_BID, F_ASK]);
//     const row = snap.get(ulConid);
//     last = (row?.last || row?.bid || row?.ask || 0) || 0;
//   } catch {}

//   const { expirations, strikes } = await ibGetStrikesExp(ul, ulConid, last);
//   if (!expirations.length || !strikes.length) return;

//   const bestStrikes = pickStrikesNear(strikes, last, MAX_STRIKES_AROUND_ATM);
//   const ts = Date.now();
//   for (const exp of expirations) {
//     pushAndFanout({
//       id: `${ul}-${exp}-${ts}-${Math.random().toString(36).slice(2,7)}`,
//       ts,
//       type: "chains",
//       provider: "ibkr",
//       symbol: ul,
//       expiration: exp,
//       strikes: bestStrikes,
//       strikesCount: bestStrikes.length
//     });
//   }
// }
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

app.get("/debug/expirations", async (req, res) => {
  try {
    const symbol = String(req.query.symbol || "").toUpperCase();
    if (!symbol) return res.status(400).json({ error: "symbol required" });
    const conid = await ibConidForStock(symbol);
    if (!conid) return res.status(404).json({ error: "no conid for symbol" });

    const r = await ibFetch(`/iserver/secdef/strikes?conid=${conid}&sectype=OPT&exchange=SMART`);
    const exps = r?.data?.expirations || r?.data?.optExpDate || r?.data?.expirationsMonth || [];
    const out = exps.map(e => (/^\d{8}$/.test(e) ? `${e.slice(0,4)}-${e.slice(4,6)}-${e.slice(6,8)}` : e));
    res.json({ symbol, conid, expirations: out });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// Resolve option contract conid via /iserver/secdef/info
// async function ibOptionConid(ulConid, { expISO, right, strike }) {
//   const month = expISO.slice(0,7).replace("-", "");
//   await ibPrimeSession();
//   const url = `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${month}&right=${right[0]}&strike=${encodeURIComponent(strike)}`;
//   const r = await ibFetch(url);
//   const list = Array.isArray(r.data) ? r.data : (Array.isArray(r.data?.contracts) ? r.data.contracts : []);
//   let best = null;
//   for (const c of list) {
//     const cx = c || {};
//     const cexp = String(cx.expiration || cx.lastTradingDay || "");
//     const expN = /^\d{8}$/.test(cexp) ? `${cexp.slice(0,4)}-${cexp.slice(4,6)}-${cexp.slice(6,8)}` : cexp;
//     const okRight = String(cx.right || cx.optRight || "").toUpperCase().startsWith(right[0]);
//     const okStrike = Math.abs(Number(cx.strike || cx.strikePrice) - Number(strike)) < 1e-6;
//     if (okRight && okStrike && expN === expISO) { best = cx; break; }
//   }
//   const conid = Number(best?.conid || best?.conidEx);
//   return Number.isFinite(conid) ? conid : null;
// }
// Resolve option contract conid via /iserver/secdef/info
// async function ibOptionConid(ulConid, { expISO, right, strike }) {
//   // ---- FIX: remove *all* dashes and pass YYYYMM ----
//   // expISO is "YYYY-MM-DD" -> "YYYYMM"
//   const month = expISO.replace(/-/g, "").slice(0, 6);

//   const url = `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${month}&right=${right[0]}&strike=${encodeURIComponent(strike)}`;
//   const r = await ibFetch(url);
//   const list = Array.isArray(r.data) ? r.data : (Array.isArray(r.data?.contracts) ? r.data.contracts : []);
//   let best = null;
//   for (const c of list) {
//     const cx = c || {};
//     const cexp = String(cx.expiration || cx.lastTradingDay || "");
//     const expN = /^\d{8}$/.test(cexp) ? `${cexp.slice(0,4)}-${cexp.slice(4,6)}-${cexp.slice(6,8)}` : cexp;
//     const okRight = String(cx.right || cx.optRight || "").toUpperCase().startsWith(right[0]);
//     const okStrike = Math.abs(Number(cx.strike || cx.strikePrice) - Number(strike)) < 1e-6;
//     if (okRight && okStrike && expN === expISO) { best = cx; break; }
//   }
//   const conid = Number(best?.conid || best?.conidEx);
//   return Number.isFinite(conid) ? conid : null;
// }
// --- utils used below ---
const toYYYYMM = (expISO) => expISO.replace(/-/g, "").slice(0, 6);
const isYYYYMMDD = (s) => /^\d{4}-\d{2}-\d{2}$/.test(s);
const toYYYY_MM_DD = (yyyymmdd) => `${yyyymmdd.slice(0,4)}-${yyyymmdd.slice(4,6)}-${yyyymmdd.slice(6,8)}`;

// async function ibOptionConid(ulConid, { expISO, right, strike }) {
//   // Normalize params
//   if (!isYYYYMMDD(expISO)) throw new Error(`expISO must be YYYY-MM-DD, got: ${expISO}`);
//   const month = toYYYYMM(expISO);
//   const R = String(right).toUpperCase().startsWith("C") ? "C" : "P";

//   // Helper: attempt a single /secdef/info query
//   const tryInfo = async (strikeStr) => {
//     const url = `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${month}&exchange=SMART&right=${R}&strike=${encodeURIComponent(strikeStr)}`;
//     const r = await ibFetch(url);
//     const arr = Array.isArray(r.data) ? r.data : (Array.isArray(r.data?.contracts) ? r.data.contracts : []);
//     if (!arr.length) return null;

//     // Pick exact expiration = expISO if possible; else best in same month
//     let best = null;
//     let bestScore = Infinity;
//     for (const c of arr) {
//       const rawExp = String(c.expiration || c.lastTradingDay || "");
//       const expN = /^\d{8}$/.test(rawExp) ? toYYYY_MM_DD(rawExp) : rawExp;
//       const k = Number(c.strike ?? c.strikePrice);
//       const score =
//         (expN === expISO ? 0 : 10) + // exact date gets priority
//         Math.abs(Number(k) - Number(strike)); // closest strike
//       if (score < bestScore) { bestScore = score; best = c; }
//     }
//     const conid = Number(best?.conid || best?.conidEx);
//     return Number.isFinite(conid) ? { conid, best } : null;
//   };

//   // Try with integer (e.g., 200) and 3dp (e.g., 200.000)
//   const sInt = String(Number(strike));        // "200"
//   const s3dp = Number(strike).toFixed(3);     // "200.000"
//   let hit = await tryInfo(sInt) || await tryInfo(s3dp);
//   if (hit) return hit.conid;

//   // If no hits, the client may have passed a non-listed date.
//   // Ask IBKR for **listed expirations in this month** and retry on the closest Friday.
//   const monthYYYY = month.slice(0,4);
//   const monthMM   = month.slice(4,6);

//   // Get strikes to also fetch expirations from /secdef/strikes
//   // (it returns both strikes and expirations; underlyingPrice not required here)
//   const r2 = await ibFetch(`/iserver/secdef/strikes?conid=${ulConid}&sectype=OPT&month=${month}&exchange=SMART`);
//   const exps = r2?.data?.expirations || r2?.data?.optExpDate || r2?.data?.expirationsMonth || [];
//   const listed = exps
//     .map(e => /^\d{8}$/.test(e) ? toYYYY_MM_DD(e) : String(e))
//     .filter(e => isYYYYMMDD(e) && e.slice(0,7) === `${monthYYYY}-${monthMM}`);

//   // Choose the listed date closest to the requested expISO
//   let expPick = listed.find(e => e === expISO);
//   if (!expPick && listed.length) {
//     const target = Date.parse(expISO);
//     let bestD = Infinity;
//     for (const e of listed) {
//       const d = Math.abs(Date.parse(e) - target);
//       if (d < bestD) { bestD = d; expPick = e; }
//     }
//   }
//   if (!expPick) return null;

//   // Try again with the chosen **listed** expiration's month (same month) & strike variants
//   const sInt2 = sInt;
//   const s3dp2 = s3dp;
//   hit = await tryInfo(sInt2) || await tryInfo(s3dp2);
//   return hit ? hit.conid : null;
// }

async function ibOptionConid(ulConid, { expISO, right, strike }) {
  // right: "CALL" | "PUT"
  const cp = right[0].toUpperCase(); // "C" or "P"
  const yyyymm   = expISO.replace(/-/g, "").slice(0, 6); // 202512
  const yyyymmdd = expISO.replace(/-/g, "");             // 20251219 (for monthlies/weeklies)
  const k = String(Number(strike));                       // plain decimal (no zero padding)

  const tryUrls = [
    // Many gateways accept month=YYYYMM + exchange
    `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${yyyymm}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`,
    // Some require full expiry via month=YYYYMMDD
    `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&month=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`,
    // Some accept expiry= instead of month=
    `/iserver/secdef/info?conid=${ulConid}&sectype=OPT&expiry=${yyyymmdd}&exchange=SMART&right=${cp}&strike=${encodeURIComponent(k)}`
  ];

  for (const url of tryUrls) {
    const r = await ibFetch(url);
    const list = Array.isArray(r.data) ? r.data
      : (Array.isArray(r.data?.contracts) ? r.data.contracts : null);

    if (!list || !list.length) continue;

    // pick exact match on expiry/right/strike
    let best = null;
    for (const c of list) {
      const cx = c || {};
      const exp = String(cx.expiration || cx.lastTradingDay || "");
      const expN = /^\d{8}$/.test(exp) ? `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}` : exp;
      const okRight  = String(cx.right || cx.optRight || "").toUpperCase().startsWith(cp);
      const okStrike = Math.abs(Number(cx.strike || cx.strikePrice) - Number(strike)) < 1e-6;
      if (okRight && okStrike && expN === expISO) { best = cx; break; }
    }
    const conid = Number(best?.conid || best?.conidEx);
    if (Number.isFinite(conid)) return conid;
  }
  return null;
}

async function expandAndWatchChainIBKR(ul) {
  const symbol = ul.toUpperCase();
  const ulConid = await ibConidForStock(symbol);
  if (!ulConid) return;

  const last = await ibGetUlLast(symbol, ulConid);
  const { expirations, strikes } = await ibGetStrikesExp(symbol, ulConid, last || 0);
  if (!expirations.length || !strikes.length) return;

  const bestStrikes = pickStrikesNear(strikes, last || strikes[Math.floor(strikes.length/2)] || 0);

  for (const exp of expirations) {
    pushAndFanout({
      type: "chains", provider: "ibkr", ts: Date.now(),
      symbol, expiration: exp, strikes: bestStrikes, strikesCount: bestStrikes.length
    });

    for (const k of bestStrikes) {
      for (const right of ["CALL","PUT"]) {
        const conid = await ibOptionConid(ulConid, { expISO: exp, right, strike: k });
        if (!conid) continue;
        const occ = toOcc(symbol, exp, right, k);
        occToConid.set(occ, conid);
        optionMetaByConid.set(conid, { ul: symbol, exp: exp, right, strike: k, occ });
        await ibWsEnsure();
        ibWsSubscribe(conid);
        const snap = await ibSnapshot([conid], [F_BID, F_ASK]);
        const row = snap.get(conid);
        if (row) optNBBO.set(occ, { bid: row.bid||0, ask: row.ask||0, ts: Date.now() });
      }
    }
  }
}

/* ================= IBKR WEBSOCKET ================= */
let ibWs = null;
let ibWsReady = false;
let ibWsConnecting = false;
let ibHeartbeatTimer = null;

async function ibWsEnsure() {
  if (MOCK) return;
  if (ibWsReady || ibWsConnecting) return;
  ibWsConnecting = true;

  await ibPrimeSession();

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
  if (!ibWsReady) { conidsSubscribed.add(conid); return; }
  if (conidsSubscribed.has(conid)) return;
  conidsSubscribed.add(conid);
  try {
    ibWs.send(`smd+${conid}+{"fields":[${[F_LAST,F_BID,F_ASK,F_BIDSZ,F_ASKSZ,F_LASTSZ].join(",")}]} `);
  } catch {}
}

function updateNBBOFromRow(conid, data) {
  const bid = Number(data[String(F_BID)] ?? data.bid ?? 0) || 0;
  const ask = Number(data[String(F_ASK)] ?? data.ask ?? 0) || 0;
  const symbol = symbolByConid.get(conid);
  const meta = optionMetaByConid.get(conid);
  if (symbol) {
    if (bid || ask) eqQuote.set(symbol, { bid, ask, ts: Date.now() });
  }
  if (meta?.occ) {
    if (bid || ask) optNBBO.set(meta.occ, { bid, ask, ts: Date.now() });
  }
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

    // sweeps (per-contract burst)
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

/* ================= FALLBACK SNAPSHOT POLLER (light) ================= */
async function pollSnapshotsOnce() {
  if (MOCK) return;
  const allConids = new Set();
  for (const c of eqConids.values()) allConids.add(c);
  for (const c of optionMetaByConid.keys()) allConids.add(c);
  if (!allConids.size) return;

  const list = Array.from(allConids).slice(0, 50); // tiny batch
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

/* ================= WATCH ENDPOINTS (unchanged shapes) ================= */
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
    await ibWsEnsure();
    for (const s of symbols) {
      const conid = await ibConidForStock(s);
      if (conid) {
        eqConids.set(s, conid);
        symbolByConid.set(conid, s);
        ibWsSubscribe(conid);
      }
      await expandAndWatchChainIBKR(s);
      await ensureChainsForUL(s); // <— add this line
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
app.post("/watch/tradier", async (req, res) => {
  const inList = Array.isArray(req.body?.options) ? req.body.options : [];
  if (!inList.length) return res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) }, added: 0 });

  // helper: normalize
  const canon = (o) => {
    const ul = String(o.underlying || o.ul || "").toUpperCase().trim();
    let exp = String(o.expiration || o.exp || "").trim();
    // allow YYYYMMDD or YYYY-MM-DD
    if (/^\d{8}$/.test(exp)) exp = `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}`;
    const k = Number(o.strike);
    let r = String(o.right || o.cp || "").toUpperCase().trim();
    r = r.startsWith("C") ? "C" : r.startsWith("P") ? "P" : r; // collapse
    return { underlying: ul, expiration: exp, strike: k, right: r };
  };

  // convert to canonical entries, drop invalids
  const wanted = inList.map(canon)
    .filter(o => o.underlying && /^\d{4}-\d{2}-\d{2}$/.test(o.expiration) && Number.isFinite(o.strike) && (o.right === "C" || o.right === "P"));

  // dedupe by OCC key
  const byOcc = new Map();
  for (const o of wanted) {
    const occ = toOcc(o.underlying, o.expiration, o.right === "C" ? "CALL" : "PUT", o.strike);
    byOcc.set(occ, o);
  }

  // add to watch set using canonical JSON
  let added = 0;
  for (const o of byOcc.values()) {
    const entry = JSON.stringify(o); // canonical
    if (!tradierOptWatch.set.has(entry)) { tradierOptWatch.set.add(entry); added++; }
  }

  // subscribe those options in IBKR
  if (!MOCK) {
    await ibWsEnsure();

    // group by UL to resolve ul conid once
    const groups = {};
    for (const o of byOcc.values()) {
      (groups[o.underlying] ||= []).push(o);
    }

    for (const [ul, list] of Object.entries(groups)) {
      const ulConid = await ibConidForStock(ul);
      if (!ulConid) continue;

      for (const o of list) {
        const rightFull = o.right === "C" ? "CALL" : "PUT";
        const conid = await ibOptionConid(ulConid, { expISO: o.expiration, right: rightFull, strike: o.strike });
        if (!conid) continue;
        const occ = toOcc(ul, o.expiration, rightFull, o.strike);
        occToConid.set(occ, conid);
        optionMetaByConid.set(conid, { ul, exp: o.expiration, right: rightFull, strike: o.strike, occ });
        ibWsSubscribe(conid);
      }

      // also emit chains for the UL so your client populates the chains panel
      await ensureChainsForUL(ul);
    }
  }

  res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) }, added });
});
app.post("/debug/normalize-watch", (_req, res) => {
  const items = Array.from(tradierOptWatch.set).map(s => {
    try { return JSON.parse(s); } catch { return null; }
  }).filter(Boolean);

  const canon = (o) => {
    const ul = String(o.underlying || "").toUpperCase().trim();
    let exp = String(o.expiration || "").trim();
    if (/^\d{8}$/.test(exp)) exp = `${exp.slice(0,4)}-${exp.slice(4,6)}-${exp.slice(6,8)}`;
    const k = Number(o.strike);
    let r = String(o.right || "").toUpperCase().trim();
    r = r.startsWith("C") ? "C" : r.startsWith("P") ? "P" : r;
    return { underlying: ul, expiration: exp, strike: k, right: r };
  };

  const byOcc = new Map();
  for (const o of items) {
    const c = canon(o);
    if (!c.underlying || !/^\d{4}-\d{2}-\d{2}$/.test(c.expiration) || !Number.isFinite(c.strike) || !["C","P"].includes(c.right)) continue;
    const occ = toOcc(c.underlying, c.expiration, c.right === "C" ? "CALL" : "PUT", c.strike);
    byOcc.set(occ, c);
  }

  tradierOptWatch.set.clear();
  for (const c of byOcc.values()) tradierOptWatch.set.add(JSON.stringify(c));

  res.json({ ok: true, count: tradierOptWatch.set.size, sample: Array.from(tradierOptWatch.set).slice(0,5).map(JSON.parse) });
});

app.get("/debug/expirations", async (req, res) => {
  const symbol = String(req.query.symbol || "").toUpperCase();
  if (!symbol) return res.status(400).json({ error: "symbol required" });
  const ulConid = await ibConidForStock(symbol);
  if (!ulConid) return res.status(404).json({ error: "unknown symbol" });
  const lastMap = await ibSnapshot([ulConid], [F_LAST, F_BID, F_ASK]);
  const last = (lastMap.get(ulConid)?.last || 0);
  const { expirations } = await ibGetStrikesExp(symbol, ulConid, last);
  res.json({ symbol, expirations });
});

// app.post("/watch/tradier", async (req, res) => {
//   const options = Array.isArray(req.body?.options) ? req.body.options : [];
//   let added = 0;

//   for (const o of options) {
//     const entry = JSON.stringify({
//       underlying: String(o.underlying).toUpperCase(),
//       expiration: String(o.expiration),
//       strike: Number(o.strike),
//       right: String(o.right).toUpperCase()
//     });
//     if (!tradierOptWatch.set.has(entry)) { tradierOptWatch.set.add(entry); added++; }
//   }

//   if (!MOCK) {
//     await ibWsEnsure();
//     const groups = {};
//     for (const o of options) {
//       const ul = String(o.underlying).toUpperCase();
//       groups[ul] = groups[ul] || [];
//       groups[ul].push(o);
//     }
//     for (const [ul, list] of Object.entries(groups)) {
//       const ulConid = await ibConidForStock(ul);
//       if (!ulConid) continue;
//       for (const o of list) {
//         const exp = String(o.expiration);
//         const right = String(o.right).toUpperCase();
//         const k = Number(o.strike);
//         const conid = await ibOptionConid(ulConid, { expISO: exp, right, strike: k });
//         if (!conid) continue;
//         const occ = toOcc(ul, exp, right, k);
//         occToConid.set(occ, conid);
//         optionMetaByConid.set(conid, { ul, exp, right, strike: k, occ });
//         ibWsSubscribe(conid);
//       }
//       await ensureChainsForUL(ul); // <— add this line
//     }
//   }

//   res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) }, added });
// });

app.post("/watch/refresh-chains", async (req, res) => {
  const symbol = String(req.body?.symbol || "").toUpperCase();
  if (!symbol) return res.status(400).json({ ok:false, error: "symbol required" });
  try {
    await ensureChainsForUL(symbol);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok:false, error: String(e) });
  }
});

app.delete("/watch/tradier", (req, res) => {
  const options = Array.isArray(req.body?.options) ? req.body.options : [];
  let removed = 0;
  for (const o of options) {
    const entry = JSON.stringify({
      underlying: String(o.underlying).toUpperCase(),
      expiration: String(o.expiration),
      strike: Number(o.strike),
      right: String(o.right).toUpperCase()
    });
    if (tradierOptWatch.set.delete(entry)) removed++;
  }
  res.json({ ok: true, removed, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
});

// For UI compat (kept)
app.post("/watch/alpaca", async (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  let added = 0;
  for (const s of equities) if (!alpacaSubs.has(s)) { alpacaSubs.add(s); added++; }
  if (!MOCK) {
    await ibWsEnsure();
    for (const s of equities) {
      const conid = await ibConidForStock(s);
      if (conid) {
        eqConids.set(s, conid);
        symbolByConid.set(conid, s);
        ibWsSubscribe(conid);
      }
      await expandAndWatchChainIBKR(s);
      await ensureChainsForUL(s); // <— add this line
    }
  }
  res.json({ ok: true, watching: { equities: Array.from(alpacaSubs) }, added });
});
app.delete("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  let removed = 0;
  for (const s of equities) if (alpacaSubs.delete(s)) removed++;
  res.json({ ok: true, removed, watching: { equities: Array.from(alpacaSubs) } });
});

app.get("/watchlist", (_req, res) => {
  res.json({ equities: Array.from(alpacaSubs), options: Array.from(tradierOptWatch.set).map(JSON.parse) });
});

/* ================= ANALYTICS / DEBUG (unchanged) ================= */
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

app.get("/debug/state", (_req, res) => {
  const occs = Array.from(tradierOptWatch.set).map(JSON.parse)
    .map(o => toOcc(o.underlying, o.expiration, o.right, o.strike));
  res.json({
    mock: MOCK,
    alpacaSubs: Array.from(alpacaSubs),
    watchedOptions: Array.from(tradierOptWatch.set).map(JSON.parse),
    ibkrSymbols: Array.from(new Set([...alpacaSubs, ...occs])),
    ibWsReady
  });
});

app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));
app.get("/health",               (_, res) => res.json({ ok: true }));
app.get("/debug/metrics",        (_req, res) => res.json({ mock: MOCK, count: httpMetrics.length, last5: httpMetrics.slice(-5) }));

app.use((req, res) => { res.status(404).json({ error: `Not found: ${req.method} ${req.originalUrl}` }); });
app.use((err, req, res, next) => {
  const o = req.headers.origin;
  if (o && isAllowed(o)) { res.setHeader("Access-Control-Allow-Origin", o); res.setHeader("Vary","Origin"); }
  res.status(err.status || 500).json({ ok:false, error: String(err.message || err) });
});

/* ================= START ================= */
server.listen(PORT, async () => {
  console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}  IB=${IB_BASE}`);
  await ibPrimeSession(); // proactive prime to avoid first-call errors
});
