// ibkr-tradeflash-server.js — Drop‑in TradeFlash server (IBKR‑only)
// Features
// - IBKR Client Portal Web API only (no Tradier/Alpaca)
// - Equities + Options chain auto‑expand (near ATM, near month)
// - Per‑minute "print" synthesis from IB history bars + NBBO to tag BUY/SELL
// - BTO/STO/BTC/STC intent inference using (yday OI baseline + running vol)
// - EOD OI ingestion + next‑day confirmation (OPEN/CLOSE) like the Tradier build
// - 429‑safe request queue + auth retry
// - HTTP+WS fanout with ring buffers; CORS allow‑list
//
// ⚠️ Notes
// 1) IBKR snapshots use numeric field codes. We request a safe set:
//    31(last), 84(bid), 86(ask), 85(bidSize), 88(askSize), 7059(impVol)
//    (Missing fields are tolerated.)
// 2) We synthesize option/equity "prints" from the latest 1‑minute bar using
//    /iserver/marketdata/history. Each new bar => one consolidated print,
//    with qty = bar.volume and price = bar.close. This is robust & IB‑compliant.
// 3) For OI baselines, either POST them via /analytics/oi-snapshot (recommended)
//    or let the system run with 0 baselines (intent still works heuristically).
// 4) This server does not depend on your earlier Tradier code; endpoints mirror it
//    where sensible so the existing client can connect with minimal changes.

import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import { WebSocketServer } from "ws";
import { randomUUID, createHash } from "crypto";
import fs from "fs";

/* ===================== CONFIG ===================== */
const PORT    = Number(process.env.PORT || 8080);
const IB_BASE = String(process.env.IB_BASE || "https://127.0.0.1:5000/v1/api").replace(/\/+$/, "");
const MOCK    = String(process.env.MOCK || "") === "1";           // offline demo

// Chain selection knobs
const MAX_EXPIRY_DAYS        = Number(process.env.MAX_EXPIRY_DAYS || 30);  // auto‑expand near month(s)
const STRIKES_EACH_SIDE      = Number(process.env.STRIKES_EACH_SIDE || 2); // ± around ATM

// Polling (lightweight; safe for IB queue)
const EQ_POLL_MS             = Number(process.env.EQ_POLL_MS || 10_000);
const OPT_POLL_MS            = Number(process.env.OPT_POLL_MS || 12_000);
const BAR_LOOKBACK_MIN       = Number(process.env.BAR_LOOKBACK_MIN || 10); // how many minutes to fetch when cold

// Request queue / 429 backoff
const MIN_GAP_MS   = Number(process.env.IB_MIN_GAP_MS || 250); // min spacing between ANY IB call
const MAX_RETRIES  = 4;

if (String(process.env.NODE_TLS_REJECT_UNAUTHORIZED) === "0") {
  console.warn("WARNING: TLS verification disabled (self-signed).\n");
}

/* ===================== APP / WS ===================== */
const app = express();
const ORIGINS = [
  "https://www.tradeflow.lol",
  "https://tradeflow.lol",
  /\.vercel\.app$/,
  "http://localhost:5173",
  "http://localhost:3000",
  "http://localhost:19006",
];
const isAllowed = (o) => ORIGINS.some(r => r instanceof RegExp ? r.test(o) : r === o);
const corsMw = cors({
  origin(origin, cb) { if (!origin) return cb(null, true); return isAllowed(origin) ? cb(null,true) : cb(new Error(`CORS: blocked ${origin}`)); },
  methods: ["GET","POST","DELETE","OPTIONS"],
  allowedHeaders: ["Content-Type","Authorization","x-request-id"],
  credentials: false,
  maxAge: 86400,
  optionsSuccessStatus: 204,
});
app.use((req,res,next)=>{ res.header("Vary","Origin"); next(); });
app.use(corsMw); app.options("*", corsMw);
app.use(express.json({ limit: "2mb" }));
app.use(compression());
app.use(morgan("tiny"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate:false });

/* ===================== UTIL ===================== */
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const clamp = (x,a,b)=>Math.max(a,Math.min(b,x));
const uniq  = (a)=>Array.from(new Set(a));
const dayKey = () => new Date().toISOString().slice(0,10);

function stableId(msg) {
  const key = JSON.stringify({
    type: msg.type, symbol: msg.symbol ?? msg.underlying, occ: msg.occ,
    side: msg.side, price: msg.price, size: msg.qty ?? msg.size, ts: msg.ts
  });
  return createHash("sha1").update(key).digest("hex");
}
function toOcc(ul, expISO, right, strike) {
  const yymmdd = String(expISO).replaceAll("-", "").slice(2);
  const cp = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike) * 1000)).padStart(8, "0");
  return `${String(ul).toUpperCase()}${yymmdd}${cp}${k}`;
}
function parseOcc(occ) {
  const m = /^([A-Z]+)(\d{6})([CP])(\d{8})$/.exec(occ);
  if (!m) return null; const [ , ul, yymmdd, cp, k ] = m;
  const exp = `20${yymmdd.slice(0,2)}-${yymmdd.slice(2,4)}-${yymmdd.slice(4,6)}`;
  return { ul, exp, right: cp === "C" ? "CALL" : "PUT", strike: Number(k)/1000 };
}

/* ============ IB REQUEST QUEUE (429‑safe) ============ */
let lastReqEndedAt = 0;
async function ibQueued(label, fn) {
  const now = Date.now();
  const wait = Math.max(0, (lastReqEndedAt + MIN_GAP_MS) - now);
  if (wait>0) await sleep(wait);
  let attempt = 0;
  let out, err;
  while (attempt <= MAX_RETRIES) {
    err = null;
    try { out = await fn(); break; }
    catch (e) {
      const is429 = String(e?.code)==="IB_429" || String(e?.message||"").includes("IB 429");
      if (is429 && attempt < MAX_RETRIES) {
        const backoff = clamp(300 * Math.pow(2, attempt) + Math.random()*200, 300, 4000);
        console.warn(`[429] ${label} retry ${attempt+1}/${MAX_RETRIES} in ${Math.round(backoff)}ms`);
        await sleep(backoff); attempt++; continue;
      }
      err = e; break;
    }
  }
  lastReqEndedAt = Date.now();
  if (err) throw err; return out;
}

if (typeof fetch === "undefined") { global.fetch = (await import("node-fetch")).default; }
function safeJson(s){ try { return JSON.parse(s); } catch { return null; } }

let PRIMED=false, PRIMING=false;
async function ibReauth(){ try { await ibFetch("/iserver/reauthenticate", { method:"POST" }, true); } catch {} }
async function ibPrimeSession(){
  if (MOCK){ PRIMED=true; return; }
  if (PRIMED || PRIMING) return; PRIMING=true;
  try { await ibFetch("/iserver/auth/status"); } catch {}
  try { await ibFetch("/iserver/accounts"); PRIMED=true; console.log("[IB] primed."); }
  catch { await sleep(250); try { await ibFetch("/iserver/accounts"); PRIMED=true; console.log("[IB] primed."); } catch(e){ console.warn("[IB] prime fail:", e.message); } }
  PRIMING=false;
}
async function ibFetch(path, opts={}, _retry=false){
  if (MOCK) throw new Error("MOCK");
  if (!PRIMED) await ibPrimeSession();
  const url = path.startsWith("http") ? path : `${IB_BASE}${path}`;
  return ibQueued(path, async () => {
    let r;
    try { r = await fetch(url, { ...opts, redirect:"follow" }); } catch (e) { throw new Error(`IB fetch failed: ${url} :: ${e.message}`); }
    if (r.status === 401 && !_retry){ await ibReauth(); return ibFetch(path, opts, true); }
    const text = await r.text(); const body = text ? safeJson(text) : null;
    if (r.status === 429){ const err = new Error(`IB 429 ${url} :: ${text||"null"}`); err.code="IB_429"; throw err; }
    if (!r.ok){ const msg = (typeof body==="object" && body) ? JSON.stringify(body) : text; if (r.status===400 && (msg||"").toLowerCase().includes("no bridge")) { console.warn(`IB 400 no bridge: ${url}`); return { _error:true, status:400, body: body ?? msg }; } throw new Error(`IB ${r.status} ${url} :: ${msg}`); }
    return body;
  });
}

/* ================= IB HELPERS (cached) ================= */
const CONID_TTL_MS = 12*60*60*1000; const EXP_TTL_MS = 30*60*1000;
const CACHE = { conidBySym:new Map(), expBySym:new Map() };
const isNonEmptyStr = (s)=>typeof s==="string" && s.trim().length>0;

async function ibConidForStock(sym){
  if (!isNonEmptyStr(sym)) return null; const key = sym.toUpperCase();
  const hit = CACHE.conidBySym.get(key); if (hit && (Date.now()-hit.ts)<CONID_TTL_MS) return hit.conid;
  if (MOCK){ const demo={AAPL:"265598",NVDA:"4815747",MSFT:"272093"}; const c=demo[key]||String(1e6+Math.floor(Math.random()*9e6)); CACHE.conidBySym.set(key,{conid:c,ts:Date.now()}); return c; }
  let conid=null;
  try {
    const body = await ibFetch(`/trsrv/stocks?symbols=${encodeURIComponent(key)}`);
    const arr = Array.isArray(body)?body:(body?[body]:[]);
    for (const row of arr){ if (String(row?.symbol||"").toUpperCase()!==key) continue; conid = row?.conid || row?.contracts?.[0]?.conid; if (conid) break; }
    if (!conid && arr[0]) conid = arr[0]?.conid || arr[0]?.contracts?.[0]?.conid;
  } catch {}
  if (!conid){
    try {
      const body2 = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
      const list = Array.isArray(body2)?body2:(body2?[body2]:[]);
      let best=null; for (const r of list){ if (String(r?.symbol||"").toUpperCase()!==key) continue; const hasSTK=(r?.sections||[]).some(s=>s?.secType==="STK"); if (hasSTK && r?.conid){ best=r; break; } }
      if (!best){ best = list.find(r => (r?.sections||[]).some(s=>s?.secType==="STK") && r?.conid); }
      if (best?.conid) conid = String(best.conid);
    } catch {}
  }
  if (conid){ CACHE.conidBySym.set(key,{conid:String(conid), ts:Date.now()}); return String(conid); }
  return null;
}

function thirdFriday(year, monthIdx0){
  const d = new Date(Date.UTC(year, monthIdx0, 1));
  while (d.getUTCDay()!==5) d.setUTCDate(d.getUTCDate()+1); // first Friday
  d.setUTCDate(d.getUTCDate()+14); // +2 weeks => third Friday
  const y=d.getUTCFullYear(), m=String(d.getUTCMonth()+1).padStart(2,"0"), dd=String(d.getUTCDate()).padStart(2,"0");
  return `${y}-${m}-${dd}`;
}
function monthsTokensToThirdFridays(tokensStr){
  if (!tokensStr) return [];
  const map={JAN:0,FEB:1,MAR:2,APR:3,MAY:4,JUN:5,JUL:6,AUG:7,SEP:8,OCT:9,NOV:10,DEC:11};
  const out=[]; for (const tok of String(tokensStr).split(";")){ const m=tok.trim().toUpperCase(); const mt=m.match(/^([A-Z]{3})(\d{2})$/); if (!mt) continue; const mon=map[mt[1]]; if (mon==null) continue; const yr=2000+Number(mt[2]); out.push(thirdFriday(yr,mon)); }
  return uniq(out).sort();
}
async function ibOptMonthsForSymbol(sym){
  const key = sym.toUpperCase(); const hit = CACHE.expBySym.get(key); if (hit && (Date.now()-hit.ts)<EXP_TTL_MS) return hit.exp;
  if (MOCK){ const ex=["2025-12-19","2026-01-16"]; CACHE.expBySym.set(key,{ts:Date.now(),exp:ex}); return ex; }
  const body = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
  const list = Array.isArray(body)?body:(body?[body]:[]);
  const rec = list.find(x => String(x?.symbol||"").toUpperCase()===key) || list[0];
  const optSec = (rec?.sections||[]).find(s=>s?.secType==="OPT");
  const tokens = optSec?.months || ""; const exps = monthsTokensToThirdFridays(tokens);
  CACHE.expBySym.set(key, { ts:Date.now(), exp:exps }); return exps;
}

/* ================= RING BUFFERS ================= */
const MAX_BUFFER = 400;
const buffers = { options_ts:[], equity_ts:[], sweeps:[], blocks:[], chains:[] };
const seenIds = Object.fromEntries(Object.keys(buffers).map(t=>[t,new Set()]));

function broadcast(obj){ const s = JSON.stringify(obj); for (const c of wss.clients) if (c.readyState===1) c.send(s); }
function pushAndFanout(msg){
  const m = { id: msg.id || stableId(msg), ...msg };
  const arr = buffers[m.type]; if (!arr) return;
  const seen = seenIds[m.type]; if (seen.has(m.id)) return;
  arr.unshift(m); seen.add(m.id);
  while (arr.length>MAX_BUFFER){ const z=arr.pop(); if (z?.id) seen.delete(z.id); }
  broadcast(m);
}

/* ================= STATE ================= */
const WATCH = { equities: new Set(), options: new Set() }; // options store json: {ul,exp,right,strike}
const NBBO_EQ = new Map();            // symbol -> { bid, ask, ts }
const NBBO_OPT = new Map();           // occ    -> { bid, ask, ts }
const LAST_BAR_OPT = new Map();       // conid  -> last { t, c, v }
const LAST_BAR_EQ  = new Map();       // conid  -> last { t, c, v }
const CONID_OPT = new Map();          // key: ul|exp|right|strike -> conid
const CONID_EQ  = new Map();          // symbol -> conid

// Per‑OCC running state (for intent inference)
const optState = new Map(); // occ -> { oi_before, vol_today_before, last_reset }
const setOptOI  = (occ, oi)=>{ const s=getOptState(occ); s.oi_before=Number(oi)||0; optState.set(occ,s); };
const setOptVol = (occ, v)=>{ const s=getOptState(occ); s.vol_today_before=Math.max(0,Number(v)||0); optState.set(occ,s); };
const bumpOptVol= (occ, q)=>{ const s=getOptState(occ); s.vol_today_before += Math.max(0,Number(q)||0); optState.set(occ,s); };
function getOptState(occ){ const today=dayKey(); const s=optState.get(occ)??{oi_before:0,vol_today_before:0,last_reset:today}; if (s.last_reset!==today){ s.vol_today_before=0; s.last_reset=today; } return s; }

/* ===== EOD OI snapshots + confirmation (same semantics as Tradier build) ===== */
const eodOiByDateOcc = new Map(); // `${date}|${occ}` -> oi
function recordEodRows(dateStr, rows){ let n=0; for (const r of rows){ let occ=r.occ; if (!occ && r.underlying && r.expiration && r.right && Number.isFinite(Number(r.strike))){ occ = toOcc(String(r.underlying).toUpperCase(), String(r.expiration), String(r.right).toUpperCase(), Number(r.strike)); }
  const oi=Number(r.oi); if (!occ || !Number.isFinite(oi)) continue; eodOiByDateOcc.set(`${dateStr}|${occ}`, oi); n++; } return n; }
function confirmOccForDate(occ, dateStr){
  const d0=new Date(dateStr); const prev=new Date(d0.getTime()-24*3600*1000); const prevDate=prev.toISOString().slice(0,10);
  const oiPrev=eodOiByDateOcc.get(`${prevDate}|${occ}`); const oiCurr=eodOiByDateOcc.get(`${dateStr}|${occ}`);
  if (!Number.isFinite(oiPrev)||!Number.isFinite(oiCurr)) return 0; const delta=oiCurr-oiPrev;
  let touched=0; const tag=(m)=>{ if (m.occ!==occ) return; const day=new Date(m.ts||Date.now()).toISOString().slice(0,10); if (day!==dateStr) return; let confTag="INCONCLUSIVE", reason=`ΔOI=${delta} ${prevDate}→${dateStr}`; if (delta>0 && (m.oc_intent==="BTO"||m.oc_intent==="STO")){ confTag="OPEN_CONFIRMED"; reason+=" (OI↑)"; } else if (delta<0 && (m.oc_intent==="BTC"||m.oc_intent==="STC")){ confTag="CLOSE_CONFIRMED"; reason+=" (OI↓)"; }
    m.oi_after=oiCurr; m.oi_delta=delta; m.oc_confirm=confTag; m.oc_confirm_reason=reason; m.oc_confirm_ts=Date.now(); touched++; };
  for (const k of ["options_ts","sweeps","blocks"]) for (const m of buffers[k]) tag(m); return touched;
}

/* ================= INFERENCE ================= */
function classifyAggressor(price, nbbo){ if (!nbbo) return { side:"UNK", at:"MID" }; const bid=Number(nbbo.bid??0), ask=Number(nbbo.ask??0); if (!Number.isFinite(ask)||ask<=0) return { side:"UNK", at:"MID" }; const mid=(bid+ask)/2; const eps=Math.max(0.01,(ask-bid)/20); if (price>=Math.max(ask-eps,bid)) return { side:"BUY", at:"ASK" }; if (price<=Math.min(bid+eps,ask)) return { side:"SELL",at:"BID" }; return { side: price>=mid?"BUY":"SELL", at:"MID" }; }
function inferIntent(occ, side, qty, price){
  const s=getOptState(occ); const nbbo=NBBO_OPT.get(occ); const { at } = classifyAggressor(price, nbbo);
  let tag="UNK", conf=0.35; const reasons=[];
  if (qty > (s.oi_before + s.vol_today_before)) { tag = side==="BUY"?"BTO":"STO"; conf=0.8; reasons.push("qty > (OI_yday + Vol_today)"); }
  else {
    const qtyWithin = qty <= s.oi_before; if (qtyWithin) reasons.push("qty <= OI_yday");
    if (side==="SELL" && (at==="BID")) { tag="STC"; conf=qtyWithin?0.7:0.55; reasons.push("sell@bid"); }
    if (side==="BUY"  && (at==="ASK")) { tag="BTC"; conf=qtyWithin?0.7:0.55; reasons.push("buy@ask"); }
  }
  return { tag, conf, at, reasons, oi_yday:s.oi_before, vol_before:s.vol_today_before };
}

/* ================= WATCH / EXPAND ================= */
const DEFAULT_EQUITIES = (process.env.DEFAULT_EQUITIES || "AAPL,NVDA,MSFT").split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);
const SEED_MARK = ".seeded.ib";

function getWatchlist(){
  return {
    equities: Array.from(WATCH.equities),
    options:  Array.from(WATCH.options).map(JSON.parse) // { underlying, expiration, right, strike }
  };
}
async function watchAddEquities(list){ let added=0; for (const s of (list||[])){ const u=String(s).toUpperCase(); if (u && !WATCH.equities.has(u)){ WATCH.equities.add(u); added++; }} return added; }
async function watchAddOptions(list){ let added=0; for (const o of (list||[])){ const e=JSON.stringify({ underlying:String(o.underlying).toUpperCase(), expiration:String(o.expiration), right:String(o.right).toUpperCase(), strike:Number(o.strike) }); if (!WATCH.options.has(e)){ WATCH.options.add(e); added++; }} return added; }

function withinDays(dISO, maxDays){ const t=Date.parse(dISO); const now=Date.now(); return Number.isFinite(t) && (t>now) && ((t-now) <= maxDays*24*3600*1000); }

async function expandChainForUL(ul){
  try {
    const conidUL = await ibConidForStock(ul); if (!conidUL) return;
    // get UL last + NBBO
    const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${conidUL}&fields=31,84,86`);
    const s0 = Array.isArray(snaps)&&snaps[0]?snaps[0]:{}; const ulLast=Number(s0["31"]);
    if (Number.isFinite(s0["84"])||Number.isFinite(s0["86"])) NBBO_EQ.set(ul, { bid:Number(s0["84"])||0, ask:Number(s0["86"])||0, ts:Date.now() });

    // expirations: nearest monthlies (<= MAX_EXPIRY_DAYS)
    let expiries = (await ibOptMonthsForSymbol(ul)).filter(d=>withinDays(d, MAX_EXPIRY_DAYS));
    if (!expiries.length){ expiries = (await ibOptMonthsForSymbol(ul)).slice(0,2); }
    if (!expiries.length) return;

    const grid = Number.isFinite(ulLast) && ulLast<50 ? 1 : 5; const atm = Number.isFinite(ulLast)?Math.round(ulLast/grid)*grid:0;
    const relStrikes = [-STRIKES_EACH_SIDE, ...Array.from({length:STRIKES_EACH_SIDE*2+1}, (_,i)=>i-STRIKES_EACH_SIDE)].map(i=>atm + i*grid);
    const strikes = Array.from(new Set(relStrikes)).filter(x=>Number.isFinite(x));

    for (const exp of expiries.slice(0,2)){
      const month = exp.replaceAll("-","").slice(0,6);
      for (const right of ["C","P"]) {
        for (const k of strikes){
          try {
            const info = await ibFetch(`/iserver/secdef/info?conid=${conidUL}&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${k}`);
            const arr = Array.isArray(info)?info:(Array.isArray(info?.Contracts)?info.Contracts:(info?[info]:[]));
            const optConid = arr.find(x=>x?.conid)?.conid; if (!optConid) continue;
            const key = `${ul}|${exp}|${right}|${k}`; CONID_OPT.set(key, String(optConid));
            const occ = toOcc(ul, exp, right, k);
            // ensure option is in watch set so it gets polled
            WATCH.options.add(JSON.stringify({ underlying:ul, expiration:exp, right, strike:k }));
            // publish chain strike bucket for UI
            pushAndFanout({ type:"chains", provider:"ibkr", ts:Date.now(), symbol:ul, expiration:exp, strikes:[k], strikesCount:1 });
          } catch {}
        }
      }
    }
  } catch (e) { console.warn("expandChainForUL:", e.message); }
}

/* ================= POLLERS (bars→prints) ================= */
function mapEquityPrint(symbol, price, qty){
  const q = NBBO_EQ.get(symbol); const side=(!q?.ask||!q?.bid)?"MID":(price>=q.ask?"BUY":(price<=q.bid?"SELL":"MID"));
  return { id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`, ts:Date.now(), type:"equity_ts", provider:"ibkr", symbol, side, qty, price, action:"UNK", venue:null };
}
function normOptionsPrint({ ul, exp, right, strike, price, qty, nbbo }){
  const occ = toOcc(ul, exp, right, strike);
  const { side, at } = classifyAggressor(price, nbbo);
  const intent = inferIntent(occ, side, qty, price);
  bumpOptVol(occ, qty);
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
    ts: Date.now(), type: "options_ts", provider: "ibkr",
    symbol: ul, occ,
    option: { expiration: exp, strike, right: right==="C"?"CALL":"PUT" },
    side, qty, price,
    oc_intent: intent.tag, intent_conf: intent.conf, fill_at: intent.at,
    intent_reasons: intent.reasons, oi_before: intent.oi_yday, vol_before: intent.vol_before,
    venue: null
  };
}
// --- BEGIN: tolerant options history helpers ---

// const DEV_SYNTH_AFTER_HOURS = String(process.env.DEV_SYNTH_AFTER_HOURS || "") === "1";
// ===== Retry helpers & flags (MUST be defined before pollers) =====
let HMDS_AVAILABLE = true; // auto-false on first HMDS 404

function isChartDataUnavailable(err) {
  const msg = String(err?.message || "").toLowerCase();
  return msg.includes("chart data unavailable");
}
function is503(err) {
  const msg = String(err?.message || "").toLowerCase();
  return msg.includes(" ib 503 ") || msg.includes('"statuscode":503') || msg.includes("service unavailable");
}

// General retry wrapper for transient IB errors (503 + “Chart data unavailable”)
async function withRetry(label, fn, { retries = 3, base = 350, max = 3000 } = {}) {
  let attempt = 0;
  while (true) {
    try { return await fn(); }
    catch (e) {
      if (attempt >= retries || !(is503(e) || isChartDataUnavailable(e))) throw e;
      const backoff = Math.min(max, Math.round(base * Math.pow(2, attempt) + Math.random() * 200));
      console.warn(`[retry] ${label} attempt ${attempt+1}/${retries} in ${backoff}ms :: ${e.message}`);
      await new Promise(r => setTimeout(r, backoff));
      attempt++;
    }
  }
}

// (Optional) dev synth for after-hours UI
const DEV_SYNTH_AFTER_HOURS = String(process.env.DEV_SYNTH_AFTER_HOURS || "") === "1";
// function maybeEmitSyntheticOption(o) {
//   if (!DEV_SYNTH_AFTER_HOURS) return false;
//   const occ = toOcc(o.underlying, o.expiration, o.right, o.strike);
//   const nbbo = NBBO_OPT.get(occ);
//   if (!nbbo) return false;
//   const mid = ((Number(nbbo.bid || 0) + Number(nbbo.ask || 0)) / 2) || 0;
//   if (mid <= 0) return false;
//   const msg = normOptionsPrint({
//     ul: o.underlying, exp: o.expiration, right: o.right, strike: o.strike,
//     price: mid, qty: 1, nbbo
//   });
//   pushAndFanout(msg);
//   return true;
// }

// Options bars fetch with CP specs + HMDS fallback (auto-disable HMDS on 404)
async function fetchOptionBarsTolerant(conid) {
  const cpSpecs = [
    `period=1d&bar=1min&outsideRth=true`,
    `period=2d&bar=5min&outsideRth=true`,
    `period=1w&bar=5min&outsideRth=true`,
  ];

  // Try Client Portal history first (with retry/backoff)
  for (const qs of cpSpecs) {
    try {
      const r = await withRetry(`opt-bars CP ${conid} ${qs}`, () =>
        ibFetch(`/iserver/marketdata/history?conid=${conid}&${qs}`)
      );
      const list = r?.data || r?.points || r?.bars || [];
      if (Array.isArray(list) && list.length) return list;
    } catch (e) {
      if (!(is503(e) || isChartDataUnavailable(e))) throw e;
    }
  }

  // HMDS fallback (if available/exposed)
  if (HMDS_AVAILABLE) {
    for (const qs of cpSpecs) {
      try {
        const r = await withRetry(`opt-bars HMDS ${conid} ${qs}`, () =>
          ibFetch(`/hmds/history?conid=${conid}&${qs}`)
        );
        const list = r?.data || r?.points || r?.bars || [];
        if (Array.isArray(list) && list.length) return list;
      } catch (e) {
        const msg = String(e?.message || "").toLowerCase();
        if (msg.includes("ib 404") || msg.includes("resource not found")) {
          HMDS_AVAILABLE = false;
          console.warn("[hmds] disabling HMDS fallback after 404");
          break;
        }
        if (!(is503(e) || isChartDataUnavailable(e))) throw e;
      }
    }
  }

  return [];
}

// Try CP history with several specs, then HMDS, return normalized bar list or []
async function fetchOptBarsAny(conid) {
  const specs = [
    `period=1d&bar=1min&outsideRth=true`,  // ideal
    `period=2d&bar=5min&outsideRth=true`,  // coarser, longer
    `period=1w&bar=5min&outsideRth=true`,  // even coarser
  ];

  // 1) CP: /iserver/marketdata/history
  for (const qs of specs) {
    try {
      const r = await ibFetch(`/iserver/marketdata/history?conid=${conid}&${qs}`);
      const list = r?.data || r?.points || r?.bars || [];
      if (Array.isArray(list) && list.length) return list;
    } catch (e) {
      if (!isChartDataUnavailable(e)) throw e; // only swallow this known case
    }
  }

  // 2) HMDS fallback
  for (const qs of specs) {
    try {
      const r = await ibFetch(`/hmds/history?conid=${conid}&${qs}`);
      const list = r?.data || r?.points || r?.bars || [];
      if (Array.isArray(list) && list.length) return list;
    } catch (e) {
      if (!isChartDataUnavailable(e)) throw e;
    }
  }

  return [];
}

// If no bars but we have NBBO, optionally emit a tiny synthetic print for UI smoke tests
function maybeEmitSyntheticOption(o) {
  if (!DEV_SYNTH_AFTER_HOURS) return false;
  const occ = toOcc(o.underlying, o.expiration, o.right, o.strike);
  const nbbo = NBBO_OPT.get(occ);
  if (!nbbo) return false;
  const mid = ((Number(nbbo.bid || 0) + Number(nbbo.ask || 0)) / 2) || 0;
  if (mid <= 0) return false;
  const msg = normOptionsPrint({
    ul: o.underlying,
    exp: o.expiration,
    right: o.right,
    strike: o.strike,
    price: mid,
    qty: 1,
    nbbo
  });
  pushAndFanout(msg);
  return true;
}

// --- END: tolerant options history helpers ---

// Track minute bars for equities and options; create one "print" per bar.
async function pollEquitiesBarsOnce(){
  try {
    for (const sym of WATCH.equities){
      const conid = CONID_EQ.get(sym) || await ibConidForStock(sym);
      if (!conid) continue; CONID_EQ.set(sym, String(conid));
      // Update NBBO too (for side classification)
      try {
        const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${conid}&fields=84,86`);
        const s0 = Array.isArray(snaps)&&snaps[0]?snaps[0]:{}; const bid=Number(s0["84"]), ask=Number(s0["86"]);
        if (Number.isFinite(bid)||Number.isFinite(ask)) NBBO_EQ.set(sym, { bid:bid||0, ask:ask||0, ts:Date.now() });
      } catch {}
      // History bars (1min)
      // const bars = await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=1d&bar=1min&outsideRth=true`);
      // const list = bars?.data || bars?.points || bars?.bars || [];
      // if (!Array.isArray(list) || !list.length) continue;

      // const list = await fetchOptBarsAny(conid);
      // if (!Array.isArray(list) || !list.length) {
      //   // If we couldn't get bars (after all fallbacks), optionally emit a synthetic tick for dev UX
      //   if (!maybeEmitSyntheticOption(o)) {
      //     // no data; skip quietly
      //   }
      //   continue;
      // }      

      // // NEW:
      // const bars = await withRetry(`eq-bars ${sym}`, async () => {
      //   try {
      //     return await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=1d&bar=1min&outsideRth=true`);
      //   } catch (e) {
      //     // Try coarser spec on known flakiness
      //     if (is503(e) || isChartDataUnavailable(e)) {
      //       return await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=2d&bar=5min&outsideRth=true`);
      //     }
      //     throw e;
      //   }
      // });      

      // // AFTER:
      // const bars = await withRetry(`eq-bars ${sym}`, async () => {
      //   try {
      //     return await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=1d&bar=1min&outsideRth=true`);
      //   } catch (e) {
      //     if (is503(e) || isChartDataUnavailable(e)) {
      //       return await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=2d&bar=5min&outsideRth=true`);
      //     }
      //     throw e;
      //   }
      // });
      // --- replace the equity history fetch block with this ---
      const barsResp = await withRetry(`eq-bars ${sym}`, async () => {
        try {
          return await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=1d&bar=1min&outsideRth=true`);
        } catch (e) {
          if (is503(e) || isChartDataUnavailable(e)) {
            return await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=2d&bar=5min&outsideRth=true`);
          }
          throw e;
        }
      });

      // normalize to a list of bars
      const list = barsResp?.data || barsResp?.points || barsResp?.bars || [];
      if (!Array.isArray(list) || !list.length) continue;

      const lastSeen = LAST_BAR_EQ.get(conid)?.t || 0;
      // Take the newest BAR_LOOKBACK_MIN bars to avoid flooding
      const tail = list.slice(-BAR_LOOKBACK_MIN);
      for (const b of tail) {
        const t = Number(b.t || b.time || Date.parse(b.timestamp || "")) || 0;
        if (!t || t <= lastSeen) continue;
        const c = Number(b.c || b.close || b.price || b.last || 0);
        const v = Number(b.v || b.volume || 0);
        if (v > 0 && Number.isFinite(c)) {
          const print = mapEquityPrint(sym, c, v);
          pushAndFanout(print);
        }
        LAST_BAR_EQ.set(conid, { t, c, v });
      }      

      // const lastSeen = LAST_BAR_EQ.get(conid)?.t || 0;
      // // Take the newest BAR_LOOKBACK_MIN bars to avoid flooding
      // const tail = list.slice(-BAR_LOOKBACK_MIN);
      // for (const b of tail){
      //   const t = Number(b.t||b.time||Date.parse(b.timestamp||""))||0; if (!t || t <= lastSeen) continue;
      //   const c = Number(b.c||b.close||b.price||b.last||0); const v = Number(b.v||b.volume||0);
      //   if (v>0 && Number.isFinite(c)) {
      //     const print = mapEquityPrint(sym, c, v);
      //     pushAndFanout(print);
      //   }
      //   LAST_BAR_EQ.set(conid, { t, c, v });
      // }
    }
  } catch (e) { console.warn("pollEquitiesBarsOnce:", e.message); }
}
// async function fetchOptionBarsTolerant(conid) {
//   // Try CP history with multiple specs and retries
//   const cpSpecs = [
//     `period=1d&bar=1min&outsideRth=true`,
//     `period=2d&bar=5min&outsideRth=true`,
//     `period=1w&bar=5min&outsideRth=true`,
//   ];
//   for (const qs of cpSpecs) {
//     try {
//       const r = await withRetry(`opt-bars CP ${conid} ${qs}`, () =>
//         ibFetch(`/iserver/marketdata/history?conid=${conid}&${qs}`)
//       );
//       const list = r?.data || r?.points || r?.bars || [];
//       if (Array.isArray(list) && list.length) return list;
//     } catch (e) {
//       // only swallow known transient/empty cases
//       if (!(is503(e) || isChartDataUnavailable(e))) throw e;
//     }
//   }

//   // HMDS fallback (if available)
//   if (HMDS_AVAILABLE) {
//     for (const qs of cpSpecs) {
//       try {
//         const r = await withRetry(`opt-bars HMDS ${conid} ${qs}`, () =>
//           ibFetch(`/hmds/history?conid=${conid}&${qs}`)
//         );
//         const list = r?.data || r?.points || r?.bars || [];
//         if (Array.isArray(list) && list.length) return list;
//       } catch (e) {
//         // If the server literally doesn't have HMDS route, turn it off permanently
//         const msg = String(e?.message || "").toLowerCase();
//         if (msg.includes("ib 404") || msg.includes("resource not found")) {
//           HMDS_AVAILABLE = false;
//           console.warn("[hmds] disabling HMDS fallback after 404");
//           break;
//         }
//         if (!(is503(e) || isChartDataUnavailable(e))) throw e;
//       }
//     }
//   }

//   return [];
// }
async function pollOptionsBarsOnce(){
  try {
    // Ensure NBBO snapshot for watched options
    for (const entry of Array.from(WATCH.options)){
      const o = JSON.parse(entry); const key = `${o.underlying}|${o.expiration}|${o.right}|${o.strike}`; const conid = CONID_OPT.get(key);
      if (!conid) continue;
      try {
        const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${conid}&fields=84,86`);
        const s0 = Array.isArray(snaps)&&snaps[0]?snaps[0]:{}; const bid=Number(s0["84"]), ask=Number(s0["86"]);
        if (Number.isFinite(bid)||Number.isFinite(ask)) NBBO_OPT.set(toOcc(o.underlying,o.expiration,o.right,o.strike), { bid:bid||0, ask:ask||0, ts:Date.now() });
      } catch {}
    }
    // Bars → prints
    for (const entry of Array.from(WATCH.options)){
      const o = JSON.parse(entry);
      const key = `${o.underlying}|${o.expiration}|${o.right}|${o.strike}`;
      let conid = CONID_OPT.get(key);
      if (!conid){
        // Resolve from UL conid each time (cached):
        const conidUL = await ibConidForStock(o.underlying); if (!conidUL) continue;
        const month = o.expiration.replaceAll("-","").slice(0,6);
        const info = await ibFetch(`/iserver/secdef/info?conid=${conidUL}&sectype=OPT&month=${month}&exchange=SMART&right=${o.right}&strike=${o.strike}`);
        const arr = Array.isArray(info)?info:(Array.isArray(info?.Contracts)?info.Contracts:(info?[info]:[]));
        const optConid = arr.find(x=>x?.conid)?.conid; if (!optConid) continue; conid = String(optConid); CONID_OPT.set(key, conid);
      }
      // const bars = await ibFetch(`/iserver/marketdata/history?conid=${conid}&period=1d&bar=1min&outsideRth=true`);
      // const list = bars?.data || bars?.points || bars?.bars || [];
      // if (!Array.isArray(list) || !list.length) continue;

      // const list = await fetchOptBarsAny(conid);
      // if (!Array.isArray(list) || !list.length) {
      //   // If we couldn't get bars (after all fallbacks), optionally emit a synthetic tick for dev UX
      //   if (!maybeEmitSyntheticOption(o)) {
      //     // no data; skip quietly
      //   }
      //   continue;
      // }      

      // const list = await fetchOptionBarsTolerant(conid);
      // if (!Array.isArray(list) || !list.length) {
      //   // Nothing usable right now—skip quietly (devs: you can synth a tick here if you want)
      //   continue;
      // }

      // AFTER:
      const list = await fetchOptionBarsTolerant(conid);
      if (!Array.isArray(list) || !list.length) {
        if (!maybeEmitSyntheticOption(o)) {
          // nothing available right now; skip quietly
        }
        continue;
      }
      const lastSeen = LAST_BAR_OPT.get(conid)?.t || 0;
      const tail = list.slice(-BAR_LOOKBACK_MIN);
      for (const b of tail){
        const t = Number(b.t||b.time||Date.parse(b.timestamp||""))||0; if (!t || t <= lastSeen) continue;
        const c = Number(b.c||b.close||b.price||b.last||0); const v = Number(b.v||b.volume||0);
        if (v>0 && Number.isFinite(c)) {
          const nbbo = NBBO_OPT.get(toOcc(o.underlying,o.expiration,o.right,o.strike));
          const msg = normOptionsPrint({ ul:o.underlying, exp:o.expiration, right:o.right, strike:o.strike, price:c, qty:v, nbbo });
          pushAndFanout(msg);
        }
        LAST_BAR_OPT.set(conid, { t, c, v });
      }
    }
  } catch (e) { console.warn("pollOptionsBarsOnce:", e.message); }
}

/* ================= SCHEDULERS ================= */
setInterval(pollEquitiesBarsOnce, EQ_POLL_MS);
setInterval(pollOptionsBarsOnce,  OPT_POLL_MS);

/* ================= WS BOOTSTRAP ================= */
wss.on("connection", (sock) => {
  try {
    for (const t of ["equity_ts","options_ts","sweeps","blocks","chains"]) {
      buffers[t].slice(0,50).forEach(it => sock.send(JSON.stringify(it)));
    }
  } catch {}
  sock.on("message", (buf) => {
    try { const m = JSON.parse(String(buf)); if (Array.isArray(m.subscribe)) { for (const t of m.subscribe) buffers[t]?.slice(0,50).forEach(it => sock.send(JSON.stringify(it))); } } catch {}
  });
});

/* ================= ROUTES ================= */
app.get("/", (_req,res)=>res.type("text/plain").send("IBKR TradeFlash up\n"));
app.get("/health", (_req,res)=>res.json({ ok:true, watching:getWatchlist(), eq_len:buffers.equity_ts.length, opt_len:buffers.options_ts.length }));
app.get("/watchlist", (_req,res)=>res.json(getWatchlist()));

// Add /watch/symbols to align with your client
app.post("/watch/symbols", async (req,res)=>{
  const symbols = new Set([...(req.body?.symbols||[])].map(s=>String(s).toUpperCase()).filter(Boolean));
  const added = await watchAddEquities(Array.from(symbols));
  // Expand options around ATM for ULs
  for (const s of symbols) await expandChainForUL(s);
  res.json({ ok:true, added, watching:getWatchlist() });
});
app.delete("/watch/symbols", async (req,res)=>{
  const symbols = new Set([...(req.body?.symbols||[])].map(s=>String(s).toUpperCase()).filter(Boolean));
  let removed=0; for (const s of symbols) if (WATCH.equities.delete(s)) removed++;
  // prune options for these ULs
  for (const entry of Array.from(WATCH.options)){
    try { const o=JSON.parse(entry); if (symbols.has(String(o.underlying).toUpperCase())) WATCH.options.delete(entry); } catch {}
  }
  res.json({ ok:true, removed, watching:getWatchlist() });
});

// Directly add explicit option contracts to watch (ul/exp/right/strike)
app.post("/watch/ib", async (req,res)=>{
  const options = Array.isArray(req.body?.options)?req.body.options:[]; const added = await watchAddOptions(options);
  res.json({ ok:true, added, watching:getWatchlist() });
});
app.delete("/watch/ib", (req,res)=>{
  const options = Array.isArray(req.body?.options)?req.body.options:[]; let removed=0;
  for (const o of options){ const e=JSON.stringify({ underlying:String(o.underlying).toUpperCase(), expiration:String(o.expiration), right:String(o.right).toUpperCase(), strike:Number(o.strike) }); if (WATCH.options.delete(e)) removed++; }
  res.json({ ok:true, removed, watching:getWatchlist() });
});

// Chains (expirations) for symbols (IB‑based)
app.get("/api/flow/chains", async (req,res)=>{
  try {
    const param = String(req.query.symbols||"").trim();
    const syms = param ? uniq(param.split(",").map(s=>s.trim().toUpperCase()).filter(Boolean)) : Array.from(WATCH.equities);
    if (!syms.length) return res.json([]);
    const out=[];
    for (const sym of syms){
      const conid = await ibConidForStock(sym); if (!conid) continue;
      const expirations = await ibOptMonthsForSymbol(sym);
      out.push({ symbol:sym, conid, expirations });
    }
    res.json(out);
  } catch (e) { console.warn("chains:", e.message); res.status(500).json({ error:"chains failed" }); }
});

// Time series for client
app.get("/api/flow/equity_ts", (_req,res)=>res.json(buffers.equity_ts));
app.get("/api/flow/options_ts",(_req,res)=>res.json(buffers.options_ts));
app.get("/api/flow/sweeps",    (_req,res)=>res.json(buffers.sweeps));
app.get("/api/flow/blocks",    (_req,res)=>res.json(buffers.blocks));
app.get("/api/flow/chains_ts", (_req,res)=>res.json(buffers.chains));

// Debug helpers
app.get("/debug/conid", async (req,res)=>{ try { const symbol=String(req.query.symbol||"").toUpperCase(); if (!symbol) return res.status(400).json({ error:"symbol required" }); const conid=await ibConidForStock(symbol); if (!conid) return res.status(404).json({ error:`No conid for ${symbol}` }); res.json({ symbol, conid:String(conid) }); } catch (e) { res.status(500).json({ error:e.message }); } });
app.get("/debug/state", (_req,res)=>{ res.json({ watching:getWatchlist(), conids_eq:Array.from(CONID_EQ.entries()), conids_opt_size:CONID_OPT.size }); });

// === OI Analytics (for confirmation) ===
app.post("/analytics/oi-snapshot", (req,res)=>{
  try {
    const dateStr = String(req.body?.date||"").slice(0,10); const rows = Array.isArray(req.body?.rows)?req.body.rows:[];
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return res.status(400).json({ error:"date must be YYYY-MM-DD" });
    const recorded = recordEodRows(dateStr, rows);
    let confirmedCount=0; const occs=new Set();
    for (const r of rows){ const occ = r.occ || (r.underlying && r.expiration && r.right && Number.isFinite(Number(r.strike)) ? toOcc(String(r.underlying).toUpperCase(), String(r.expiration), String(r.right).toUpperCase(), Number(r.strike)) : null); if (occ) occs.add(occ); }
    for (const occ of occs) confirmedCount += confirmOccForDate(occ, dateStr);
    res.json({ ok:true, recorded, confirmed: confirmedCount });
  } catch (e) { console.error("/analytics/oi-snapshot", e); res.status(500).json({ error:"internal error" }); }
});
app.post("/analytics/confirm", (req,res)=>{
  try { const dateStr=String(req.body?.date||"").slice(0,10); if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return res.status(400).json({ error:"date must be YYYY-MM-DD" }); const provided = Array.isArray(req.body?.occs)?new Set(req.body.occs.map(String)):null; const occs=new Set(); for (const key of eodOiByDateOcc.keys()){ const [d,occ]=key.split("|"); if (d===dateStr && (!provided || provided.has(occ))) occs.add(occ); } let confirmed=0; for (const occ of occs) confirmed += confirmOccForDate(occ, dateStr); res.json({ ok:true, date:dateStr, occs:Array.from(occs), confirmed }); } catch (e) { res.status(500).json({ error:"internal error" }); }
});
app.get("/debug/oi", (req,res)=>{ const occ=String(req.query?.occ||""); const dateStr=String(req.query?.date||""); if (!occ||!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return res.status(400).json({ error:"need occ and date=YYYY-MM-DD" }); const prevDate=new Date(dateStr); prevDate.setUTCDate(prevDate.getUTCDate()-1); const prev=prevDate.toISOString().slice(0,10); const oiPrev=eodOiByDateOcc.get(`${prev}|${occ}`); const oiCurr=eodOiByDateOcc.get(`${dateStr}|${occ}`); const delta=(Number.isFinite(oiPrev)&&Number.isFinite(oiCurr))?(oiCurr-oiPrev):null; res.json({ occ, prev, date:dateStr, oiPrev, oiCurr, delta }); });

// Not found handler (CORS‑aware)
app.use((err, req, res, _next)=>{ const o=req.headers.origin; if (o&&isAllowed(o)){ res.setHeader("Access-Control-Allow-Origin", o); res.setHeader("Vary","Origin"); } res.status(err.status||500).json({ ok:false, error:String(err.message||err) }); });
app.use((req,res)=>{ const o=req.headers.origin; if (o&&isAllowed(o)){ res.setHeader("Access-Control-Allow-Origin", o); res.setHeader("Vary","Origin"); } res.status(404).json({ error:`Not found: ${req.method} ${req.originalUrl}` }); });

/* ================= START ================= */
server.listen(PORT, async ()=>{
  console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK?"on":"off"}  IB=${IB_BASE}`);
  try {
    if (!fs.existsSync(SEED_MARK)){
      const added = await watchAddEquities(DEFAULT_EQUITIES);
      for (const s of DEFAULT_EQUITIES) await expandChainForUL(s);
      fs.writeFileSync(SEED_MARK, new Date().toISOString());
      console.log(`[seed] equities added=${added}`);
    }
  } catch (e) { console.warn("seed error:", e.message); }
});
