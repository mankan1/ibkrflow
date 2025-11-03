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

/* ================= CONFIG ================= */
const PORT    = Number(process.env.PORT || 8080);
const IB_BASE = String(process.env.IB_BASE || "https://127.0.0.1:5000/v1/api").replace(/\/+$/,"");
const MOCK    = String(process.env.MOCK || "") === "1";

if (String(process.env.NODE_TLS_REJECT_UNAUTHORIZED) === "0") {
  console.warn("WARNING: TLS verification disabled (self-signed).");
}
console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK?"on":"off"}  IB=${IB_BASE}`);

/* ================= APP/WS ================= */
const app = express();
app.use(express.json());
app.use(cors());
app.use(compression());
app.use(morgan("dev"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate:false });

function wsBroadcast(type, payload) {
  const s = JSON.stringify({ type, payload });
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}

/* ================= STATE ================= */
const WATCH = {
  equities: new Set(),  // "AAPL","NVDA","MSFT"
  options: [],          // {underlying, expiration(YYYY-MM-DD), strike, right(C|P)}
};

const STATE = {
  equities_ts: [],
  options_ts:  [],
};

const CACHE = {
  // conid + expirations caches
  conidBySym: new Map(),           // key: "AAPL" -> "265598"
  expBySym:   new Map(),           // key: "AAPL" -> { ts, expirations: [...] }
  sessionPrimed: false,
  primingSince: 0,
};

/* ================= HELPERS ================= */
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const clamp  = (x,min,max)=>Math.max(min,Math.min(max,x));
const uniq = (a)=>Array.from(new Set(a));
const isNonEmptyStr = (s)=>typeof s==="string" && s.trim().length>0;

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
function normEquities(W){
  return Array.from(ensureSet(W?.equities)).map(s=>String(s).toUpperCase()).filter(Boolean);
}
function normOptions(W){
  const A = Array.isArray(W?.options) ? W.options : [];
  return A.map(o=>({
    underlying: String(o?.underlying||"").toUpperCase(),
    expiration: String(o?.expiration||""),
    strike: Number(o?.strike),
    right: rightNormalize(o?.right),
  })).filter(o=>o.underlying && o.right && Number.isFinite(o.strike));
}
function getWatchlist(){
  return { equities: normEquities(WATCH), options: normOptions(WATCH) };
}
async function watchAddEquities(equities=[]){
  const before = normEquities(WATCH).length;
  for (const s of equities) { const u=String(s).toUpperCase(); if (u) WATCH.equities.add(u); }
  const after = normEquities(WATCH).length;
  const added = Math.max(0, after-before);
  if (added>0) wsBroadcast("watchlist", getWatchlist());
  return added;
}
async function watchAddOptions(options=[]){
  if (!Array.isArray(WATCH.options)) WATCH.options=[];
  const before = normOptions(WATCH).length;
  for (const raw of options){
    const o = {
      underlying: String(raw?.underlying||"").toUpperCase(),
      expiration: String(raw?.expiration||""),
      strike: Number(raw?.strike),
      right: rightNormalize(raw?.right),
    };
    if (o.underlying && o.right && Number.isFinite(o.strike)) WATCH.options.push(o);
  }
  const after = normOptions(WATCH).length;
  const added = Math.max(0, after-before);
  if (added>0) wsBroadcast("watchlist", getWatchlist());
  return added;
}

/* ================= THIRD FRIDAY MONTH PARSER ================= */
function thirdFriday(year, monthIdx0){
  const d = new Date(Date.UTC(year, monthIdx0, 1));
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

/* ================= REQUEST QUEUE (429-friendly) =================
   - Serializes all IB calls
   - Global min gap between calls
   - Retries 429 with exponential backoff + jitter
================================================================= */
let lastReqEndedAt = 0;
const MIN_GAP_MS = 250;     // min gap between IB requests
const MAX_RETRIES = 4;      // on 429
async function ibQueued(fnLabel, doFetch){
  const now = Date.now();
  const wait = Math.max(0, (lastReqEndedAt + MIN_GAP_MS) - now);
  if (wait>0) await sleep(wait);

  let attempt = 0;
  let res, err;
  while (attempt <= MAX_RETRIES){
    err = null;
    try {
      res = await doFetch();
      break;
    } catch (e) {
      // Detect 429 from message pattern we build in ibFetchRaw
      if (String(e?.code)==="IB_429" || String(e?.message||"").includes("IB 429 ")) {
        const backoff = clamp(300 * Math.pow(2, attempt) + Math.random()*200, 300, 4000);
        if (attempt < MAX_RETRIES){
          console.warn(`[429] ${fnLabel} — retry ${attempt+1}/${MAX_RETRIES} in ${Math.round(backoff)}ms`);
          await sleep(backoff);
          attempt++;
          continue;
        }
      }
      err = e; break;
    }
  }
  lastReqEndedAt = Date.now();
  if (err) throw err;
  return res;
}

/* ================= IB CORE FETCH ================= */
function safeJson(s){ try { return JSON.parse(s); } catch { return null; } }

async function ibReauth(){ try { await ibFetch("/iserver/reauthenticate", { method:"POST" }, true); } catch {} }

async function ibPrimeSession(){
  if (MOCK){ CACHE.sessionPrimed = true; return; }
  const now = Date.now();
  if (CACHE.sessionPrimed) return;
  if (CACHE.primingSince && (now - CACHE.primingSince) < 1000) return;
  CACHE.primingSince = now;

  // Try auth, then accounts (twice if needed, with tiny sleep)
  try { await ibFetch("/iserver/auth/status"); } catch(e){ console.warn("auth/status:", e.message); }
  try {
    await ibFetch("/iserver/accounts");
    CACHE.sessionPrimed = true;
    console.log("[IB] session primed.");
    return;
  } catch (e) {
    console.warn("accounts prime:", e.message);
  }
  await sleep(300);
  try {
    await ibFetch("/iserver/accounts");
    CACHE.sessionPrimed = true;
    console.log("[IB] session primed.");
  } catch (e) {
    console.warn("accounts retry:", e.message);
  }
}

async function ibFetch(path, opts={}, _retry=false){
  if (MOCK) throw new Error("MOCK mode");
  if (!CACHE.sessionPrimed) await ibPrimeSession();

  const url = path.startsWith("http") ? path : `${IB_BASE}${path}`;
  const res = await ibQueued(path, async () => {
    let r;
    try { r = await fetch(url, { ...opts, redirect:"follow" }); }
    catch (e) { throw new Error(`IB fetch failed: ${url} :: ${e.message}`); }

    // 401 -> reauth once
    if (r.status === 401 && !_retry){
      await ibReauth();
      return ibFetch(path, opts, true);
    }

    const text = await r.text();
    const body = text ? safeJson(text) : null;

    if (r.status === 429){
      const err = new Error(`IB 429 ${url} :: ${text||"null"}`);
      err.code = "IB_429";
      throw err;
    }
    if (!r.ok){
      const msg = (typeof body==="object" && body) ? JSON.stringify(body) : text;
      // gracefully return body for 400 "no bridge"
      if (r.status===400 && (msg||"").toLowerCase().includes("no bridge")){
        console.warn(`IB 400 no bridge: ${url}`);
        return { _error:true, status:400, body: body ?? msg };
      }
      throw new Error(`IB ${r.status} ${url} :: ${msg}`);
    }
    return body;
  });

  return res;
}

/* ================= IB HELPERS (CACHED) ================= */
const CONID_TTL_MS = 12*60*60*1000; // 12h (rarely changes)
const EXP_TTL_MS   = 30*60*1000;    // 30 min

// async function ibConidForStock(sym){
//   if (!isNonEmptyStr(sym)) return null;
//   const key = sym.toUpperCase();

//   // Cache hit?
//   const cached = CACHE.conidBySym.get(key);
//   if (cached && cached.ts && (Date.now()-cached.ts) < CONID_TTL_MS) return cached.conid;
//   if (MOCK){
//     const demo = { AAPL:"265598", NVDA:"4815747", MSFT:"272093" };
//     const c = demo[key] || String(100000+Math.floor(Math.random()*9e5));
//     CACHE.conidBySym.set(key, { conid:c, ts:Date.now() });
//     return c;
//   }

//   const body = await ibFetch(`/trsrv/stocks?symbols=${encodeURIComponent(key)}`);
//   const arr  = Array.isArray(body) ? body : (body ? [body] : []);
//   // Find a row with contracts or direct conid
//   let conid = null;
//   for (const row of arr){
//     if (String(row?.symbol||"").toUpperCase() !== key) continue;
//     conid = row?.conid || row?.contracts?.[0]?.conid;
//     if (conid) break;
//   }
//   if (!conid && arr[0]) conid = arr[0]?.conid || arr[0]?.contracts?.[0]?.conid;
//   if (conid){
//     CACHE.conidBySym.set(key, { conid:String(conid), ts: Date.now() });
//     return String(conid);
//   }
//   return null;
// }
// --- REPLACE this function entirely ---
async function ibConidForStock(sym){
  if (!isNonEmptyStr(sym)) return null;
  const key = sym.toUpperCase();

  // Cache hit?
  const cached = CACHE.conidBySym.get(key);
  if (cached && cached.ts && (Date.now()-cached.ts) < CONID_TTL_MS) {
    return cached.conid;
  }

  if (MOCK){
    const demo = { AAPL:"265598", NVDA:"4815747", MSFT:"272093" };
    const c = demo[key] || String(100000+Math.floor(Math.random()*9e5));
    CACHE.conidBySym.set(key, { conid:c, ts:Date.now() });
    return c;
  }

  // 1) Try /trsrv/stocks
  let conid = null;
  try {
    const body = await ibFetch(`/trsrv/stocks?symbols=${encodeURIComponent(key)}`);
    const arr  = Array.isArray(body) ? body : (body ? [body] : []);
    for (const row of arr){
      if (String(row?.symbol||"").toUpperCase() !== key) continue;
      conid = row?.conid || row?.contracts?.[0]?.conid;
      if (conid) break;
    }
    if (!conid && arr[0]) conid = arr[0]?.conid || arr[0]?.contracts?.[0]?.conid;
  } catch (e) {
    // swallow; we’ll try secdef/search next
  }

  // 2) Fallback: /iserver/secdef/search (pick the record whose sections include STK)
  if (!conid){
    try {
      const body2 = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
      const list  = Array.isArray(body2) ? body2 : (body2 ? [body2] : []);
      let best = null;

      // Prefer exact symbol match with STK section
      for (const r of list){
        if (String(r?.symbol||"").toUpperCase() !== key) continue;
        const hasSTK = (r?.sections||[]).some(s => s?.secType==="STK");
        if (hasSTK && r?.conid){ best = r; break; }
      }
      // Else take first with STK
      if (!best){
        best = list.find(r => (r?.sections||[]).some(s=>s?.secType==="STK") && r?.conid);
      }
      if (best?.conid) conid = String(best.conid);
    } catch (e) {
      // ignore
    }
  }

  if (conid){
    CACHE.conidBySym.set(key, { conid:String(conid), ts: Date.now() });
    return String(conid);
  }
  return null;
}

// --- ADD this small debug route somewhere near other /debug/* routes ---
app.get("/debug/conid", async (req,res)=>{
  try {
    const symbol = String(req.query.symbol||"").toUpperCase();
    if (!symbol) return res.status(400).json({ error:"symbol required" });
    const conid = await ibConidForStock(symbol);
    res.json({ symbol, conid });
  } catch (e) {
    res.status(500).json({ error: e.message||"failed" });
  }
});

async function ibOptMonthsForSymbol(sym){
  const key = sym.toUpperCase();
  const hit = CACHE.expBySym.get(key);
  if (hit && (Date.now()-hit.ts) < EXP_TTL_MS) return hit.expirations;

  if (MOCK){
    const exps = [
      "2025-12-19","2026-01-16","2026-02-20","2026-03-20","2026-04-17",
      "2026-05-15","2026-06-19","2026-07-17","2026-09-18","2026-10-16",
      "2027-01-15","2027-02-19","2027-07-16","2028-01-21","2028-02-18"
    ];
    CACHE.expBySym.set(key, { ts:Date.now(), expirations:exps });
    return exps;
  }

  // Parse OPT.months from secdef/search
  const body = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`);
  const list = Array.isArray(body) ? body : (body ? [body] : []);
  const rec  = list.find(x => String(x?.symbol||"").toUpperCase()===key) || list[0];
  const optSec = (rec?.sections||[]).find(s => s?.secType==="OPT");
  const tokens = optSec?.months || "";
  const exps = monthsTokensToThirdFridays(tokens);
  CACHE.expBySym.set(key, { ts:Date.now(), expirations:exps });
  return exps;
}

function mapEquitySnapshot(sym, snap){
  const last = Number(snap?.["31"]);
  const bid  = Number(snap?.["84"]);
  const ask  = Number(snap?.["86"]);
  const iv   = Number(snap?.["7059"]); // may be undefined
  return {
    symbol: sym,
    last: Number.isFinite(last)?last:undefined,
    bid:  Number.isFinite(bid)?bid:undefined,
    ask:  Number.isFinite(ask)?ask:undefined,
    iv:   Number.isFinite(iv)?iv:undefined,
    ts:   Date.now()
  };
}
function mapOptionTick(underlying, expiration, strike, right, snap){
  const last = Number(snap?.["31"]);
  const bid  = Number(snap?.["84"]);
  const ask  = Number(snap?.["86"]);
  return {
    underlying, expiration, strike, right,
    last: Number.isFinite(last)?last:undefined,
    bid:  Number.isFinite(bid)?bid:undefined,
    ask:  Number.isFinite(ask)?ask:undefined,
    ts:   Date.now()
  };
}

/* ================= POLLERS =================
   - Keep last known good values even if current cycle fails
   - Small per-call throttle + queue shields from 429 storms
============================================= */
async function pollEquitiesOnce(){
  try {
    const symbols = normEquities(WATCH);
    if (!symbols.length){ return; }

    // resolve conids (cached)
    const pairs = [];
    for (const sym of symbols){
      try {
        const conid = await ibConidForStock(sym);
        if (conid) pairs.push({ sym, conid });
      } catch (e) {
        // swallow per-symbol errors
      }
    }
    if (!pairs.length) return;

    // Snapshot (batch)
    let rows = [];
    if (MOCK){
      rows = pairs.map(p => ({ symbol:p.sym, last:100+Math.random()*50, bid:0, ask:0, ts:Date.now() }));
    } else {
      const conids = pairs.map(p=>p.conid).join(",");
      const snaps  = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
      for (const p of pairs){
        const s = Array.isArray(snaps) ? snaps.find(z => String(z.conid)===String(p.conid)) : null;
        if (!s) continue;
        rows.push(mapEquitySnapshot(p.sym, s));
      }
    }
    if (rows.length){
      STATE.equities_ts = rows;
      wsBroadcast("equity_ts", rows);
    }
  } catch (e) {
    console.warn("pollEquitiesOnce:", e.message);
  }
}

// Lower-traffic options poller:
// 1) If explicit watched options exist -> resolve those only.
// 2) Else sample one ATM C/P for each watched underlying (first monthly).
async function pollOptionsOnce(){
  try {
    const explicit = normOptions(WATCH);
    const underlyings = normEquities(WATCH);

    const ticks = [];

    if (explicit.length){
      for (const o of explicit){
        try {
          const conidUL = await ibConidForStock(o.underlying);
          if (!conidUL) continue;

          // month=YYYYMM
          const month = o.expiration.replaceAll("-","").slice(0,6);
          // /iserver/secdef/info -> option conid
          const info = await ibFetch(`/iserver/secdef/info?conid=${conidUL}&sectype=OPT&month=${month}&exchange=SMART&right=${o.right}&strike=${o.strike}`);
          const arr = Array.isArray(info) ? info
                    : Array.isArray(info?.Contracts) ? info.Contracts
                    : (info ? [info] : []);
          const optConid = arr.find(x=>x?.conid)?.conid;
          if (!optConid) continue;

          const snap = await ibFetch(`/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86`);
          const s0   = Array.isArray(snap)&&snap[0] ? snap[0] : {};
          ticks.push(mapOptionTick(o.underlying, o.expiration, o.strike, o.right, s0));
        } catch (e) {
          // ignore per-option errors
        }
      }
    } else {
      // lightweight sampler: one ATM C/P for each UL
      for (const ul of underlyings){
        try {
          const conidUL = await ibConidForStock(ul);
          if (!conidUL) continue;

          // underlying last
          const snapUL = await ibFetch(`/iserver/marketdata/snapshot?conids=${conidUL}&fields=31`);
          const s0     = Array.isArray(snapUL)&&snapUL[0] ? snapUL[0] : {};
          const ulLast = Number(s0?.["31"]);
          if (!Number.isFinite(ulLast)) continue;

          const expiries = await ibOptMonthsForSymbol(ul);
          if (!expiries.length) continue;
          const expiry = expiries[0]; // nearest monthly
          const grid   = ulLast < 50 ? 1 : 5;
          const strike = Math.round(ulLast / grid) * grid;
          const month  = expiry.replaceAll("-","").slice(0,6);

          for (const right of ["C","P"]){
            try {
              const info = await ibFetch(`/iserver/secdef/info?conid=${conidUL}&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${strike}`);
              const arr = Array.isArray(info) ? info
                        : Array.isArray(info?.Contracts) ? info.Contracts
                        : (info ? [info] : []);
              const optConid = arr.find(x=>x?.conid)?.conid;
              if (!optConid) continue;

              const snap = await ibFetch(`/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86`);
              const s1   = Array.isArray(snap)&&snap[0] ? snap[0] : {};
              ticks.push(mapOptionTick(ul, expiry, strike, right, s1));
            } catch {}
          }
        } catch {}
      }
    }

    if (ticks.length){
      STATE.options_ts = ticks;
      wsBroadcast("options_ts", ticks);
    }
  } catch (e) {
    console.warn("pollOptionsOnce:", e.message);
  }
}

/* ================= SCHEDULERS =================
   - Keep intervals modest to avoid 429
   - You can tighten once things are stable
================================================ */
setInterval(pollEquitiesOnce, 10_000); // 10s
setInterval(pollOptionsOnce, 13_000);  // 13s (desync)

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

// Chains (expirations) for symbols
app.get("/api/flow/chains", async (req, res) => {
  try {
    const param = String(req.query.symbols||"").trim();
    const syms = param ? uniq(param.split(",").map(s=>s.trim().toUpperCase()).filter(Boolean))
                       : normEquities(WATCH);
    if (!syms.length) return res.json([]);

    const out = [];
    for (const sym of syms){
      const conid = await ibConidForStock(sym);
      if (!conid) continue;
      const expirations = await ibOptMonthsForSymbol(sym);
      out.push({ symbol:sym, conid, expirations });
    }
    res.json(out);
  } catch (e) {
    console.warn("chains error:", e.message);
    res.status(500).json({ error:"chains failed" });
  }
});

// Time series used by client
app.get("/api/flow/equity_ts", (req,res)=>res.json(STATE.equities_ts ?? []));
app.get("/api/flow/options_ts", (req,res)=>res.json(STATE.options_ts ?? []));

// Stubs (so UI never 404s)
app.get("/api/flow/sweeps", (req,res)=>res.json([]));
app.get("/api/flow/blocks", (req,res)=>res.json([]));

// Debug
app.get("/debug/expirations", async (req,res)=>{
  try {
    const symbol = String(req.query.symbol||"").toUpperCase();
    if (!symbol) return res.status(400).json({ error:"symbol required" });
    const conid = await ibConidForStock(symbol);
    if (!conid) return res.status(500).json({ error:`No conid for ${symbol}` });
    const expirations = await ibOptMonthsForSymbol(symbol);
    res.json({ symbol, conid, expirations });
  } catch (e) {
    res.status(500).json({ error: e.message||"expirations failed" });
  }
});

app.get("/health", (req,res)=>{
  res.json({
    ok:true,
    primed:CACHE.sessionPrimed,
    watch:getWatchlist(),
    eq_len: STATE.equities_ts?.length||0,
    opt_len: STATE.options_ts?.length||0
  });
});

/* ================= START ================= */
server.listen(PORT, ()=>console.log(`Listening on http://localhost:${PORT}`));

/* ============ QUICK CURLS =============
# seed watch
curl -s -X POST localhost:8080/watch/alpaca -H 'Content-Type: application/json' \
  -d '{"equities":["AAPL","NVDA","MSFT"]}' | jq .

curl -s -X POST localhost:8080/watch/tradier -H 'Content-Type: application/json' \
  -d '{"options":[{"underlying":"AAPL","expiration":"2025-12-19","strike":200,"right":"CALL"}]}' | jq .

# verify
curl -s localhost:8080/watchlist | jq .
curl -s "http://localhost:8080/api/flow/chains?symbols=AAPL,NVDA,MSFT" | jq .
curl -s localhost:8080/api/flow/equity_ts | jq .
curl -s localhost:8080/api/flow/options_ts | jq .
========================================= */

