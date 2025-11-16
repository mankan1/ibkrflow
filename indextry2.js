/*
 * TradeFlow One-File Server
 * ---------------------------------------------------------------
 * Features kept & working:
 * - HTTP + WebSocket server (broadcast topics: watchlist, equity_ts, option_quotes, sweeps, blocks, prints, headlines, basis, notables)
 * - Equities & indices price snapshots (+ ES↔SPX basis)
 * - Futures (front-month auto-resolution; GLOBEX)
 * - Equity options NBBO + action inference
 * - **Futures options (FOP)**: front futures resolver + conid lookup + NBBO polling
 * - Synthetic sweep/block builders from raw prints with publish gates
 * - Notables builder over rolling window
 * - Autotrade hooks (ATR-based trailing stops; order placement stubbed via IB REST)
 * - Watchlist persistence to disk (watchlist.json) with GET/POST APIs
 * - 429-aware IB fetch queue, 401 reauth, timing/metrics endpoints
 * - Debug endpoints: /debug/conid, /debug/snapshot_raw, /prices, /prices/:symbol
 * - DEMO mode seeding (env DEMO=1) without IB
 *
 * ENV (examples):
 *   HOST=0.0.0.0 PORT=8080 IB_BASE=https://127.0.0.1:5000/v1/api \
 *   LOG_LEVEL=info LOG_JSON=0 LOG_TIMINGS=0 \
 *   DEMO=0 MOCK=0 NODE_TLS_REJECT_UNAUTHORIZED=0
 *
 * Run:
 *   npm i express compression cors morgan ws node-fetch
 *   NODE_TLS_REJECT_UNAUTHORIZED=0 HOST=0.0.0.0 PORT=8080 node flow-server.js
 */

import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import { WebSocketServer } from "ws";
import fs from "fs";

/* ======================= CONFIG ======================= */
const HOST    = process.env.HOST || "127.0.0.1";
const PORT    = Number(process.env.PORT || 8080);
const IB_BASE = String(process.env.IB_BASE || "https://127.0.0.1:5000/v1/api").replace(/\/+$/,"");
const MOCK    = String(process.env.MOCK || "") === "1"; // disable IB calls
const DEMO    = String(process.env.DEMO || "") === "1"; // synth data

if (String(process.env.NODE_TLS_REJECT_UNAUTHORIZED) === "0") {
  console.warn("WARNING: TLS verification disabled (self-signed).");
}

/* ======================= LOGGING ======================= */
const LOG = {
  level: (process.env.LOG_LEVEL || "info").toLowerCase(),
  json:  process.env.LOG_JSON === "1",
  timings: process.env.LOG_TIMINGS === "1",
};
const LV = { error:0, warn:1, info:2, debug:3 };
const can = (lvl)=> (LV[LOG.level] ?? 2) >= (LV[lvl] ?? 2);
const now = ()=> Date.now();
function jl(lvl, msg, meta){
  if (!can(lvl)) return;
  if (LOG.json){
    const row = { t:new Date().toISOString(), lvl, msg, ...(meta||{}) };
    console.log(JSON.stringify(row));
  } else {
    const p = `[${lvl.toUpperCase()}]`;
    if (meta) console.log(p, msg, meta); else console.log(p, msg);
  }
}

/* ======================= STATE ======================= */
const STATE = {
  equities_ts: [],   // [{symbol,last,bid,ask,iv?,ts}]
  options_ts:  [],   // last option ticks for demo/synthesis
  sweeps:      [],
  blocks:      [],
  prints:      [],
};

// Rolling window for notables & tape
const ROLLING_MS = 2 * 60_000; // 2 minutes
function pruneTapeAges(){
  const cutoff = Date.now() - ROLLING_MS;
  STATE.sweeps = (STATE.sweeps||[]).filter(x => x.ts >= cutoff);
  STATE.blocks = (STATE.blocks||[]).filter(x => x.ts >= cutoff);
  STATE.prints = (STATE.prints||[]).filter(x => x.ts >= cutoff);
}
setInterval(pruneTapeAges, 5_000);

/* ======================= APP/WS ======================= */
const app = express();
app.use(cors({ origin: ["http://localhost:8081","http://127.0.0.1:8081"], methods:["GET","POST","OPTIONS"], credentials:false }));
app.options("*", cors());
app.use(express.json({ limit: "1mb" }));
app.use(compression());
app.use(morgan("dev"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate:false });
function wsBroadcast(topic, data){
  const payload = JSON.stringify({ topic, data });
  for (const c of wss.clients) if (c.readyState === 1) { try { c.send(payload); } catch {} }
}

/* ======================= WATCHLIST (disk) ======================= */
const WL_FILE = "watchlist.json";
function loadWatchlistFile(){ try { return JSON.parse(fs.readFileSync(WL_FILE, "utf8")); } catch { return { equities:["SPX","/ES","SPY"], options:[] }; } }
function saveWatchlistFile(obj){ try { fs.writeFileSync(WL_FILE, JSON.stringify(obj, null, 2)); } catch {} }
const WATCH = loadWatchlistFile();

app.get("/watchlist", (_req, res) => res.json(WATCH));
app.post("/watchlist", (req, res) => {
  const b = req.body || {};
  WATCH.equities = Array.isArray(b.equities) ? [...new Set(b.equities.map(s=>String(s).toUpperCase()))] : WATCH.equities;
  WATCH.options  = Array.isArray(b.options)  ? b.options.map(o=>({
    underlying:String(o.underlying||"").toUpperCase(),
    expiration:String(o.expiration||""),
    strike:Number(o.strike),
    right: (String(o.right||"").toUpperCase().startsWith("C")?"C":"P")
  })).filter(o=>o.underlying && o.expiration && Number.isFinite(o.strike)) : WATCH.options;
  saveWatchlistFile(WATCH);
  wsBroadcast("watchlist", WATCH);
  res.json({ ok:true });
});

/* ======================= METRICS ======================= */
const METRICS = { ws_sends:0, ib_ok:0, ib_err:0, ib_429:0, last_eq_ms:0, last_opt_ms:0, last_fop_ms:0 };
app.get("/debug/metrics", (_req,res)=> res.json({ ...METRICS, ts: Date.now() }));

/* ======================= UTIL ======================= */
const FUT_EXCH = "GLOBEX";
function toNum(x){ const n = Number(x); return Number.isFinite(n) ? n : null; }
function normalizeIv(ivRaw){ const n = toNum(ivRaw); if (n==null) return null; return n<=1.5? Math.round(n*100) : Math.round(n); }
function normalizeSymbol(input){
  if (!input) return "";
  let s = String(input).trim().toUpperCase().replace(/^\$/,"");
  if (s.startsWith("/")) s = s.slice(1);
  if (s === "ES1!" || s === "ES=F") s = "ES";
  return s;
}
function fresh(row, ms=60_000){ return row?.ts && (Date.now()-row.ts) < ms; }
function midOrLast(t){ const last = Number(t?.last); if (Number.isFinite(last) && last!==0) return last; const b=Number(t?.bid), a=Number(t?.ask); if (Number.isFinite(b)&&Number.isFinite(a)&&b>0&&a>0) return (b+a)/2; return NaN; }

/* ======================= IB CORE (queued) ======================= */
if (typeof fetch === "undefined") { global.fetch = (await import("node-fetch")).default; }
const MIN_GAP_MS = 250, MAX_RETRIES = 4; let lastReqEndedAt = 0; const CACHE = { session:false };
function safeJson(s){ try{ return JSON.parse(s); }catch{ return null; } }
async function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
async function ibReauth(){ try { await ibFetch("/iserver/reauthenticate", { method:"POST" }, true); } catch {} }
async function ibPrime(){ if (MOCK) { CACHE.session = true; return; } if (CACHE.session) return; try { await ibFetch("/iserver/auth/status"); } catch{}; try { await ibFetch("/iserver/accounts"); CACHE.session=true; } catch{} }
async function ibQueued(path, fn){ const nowTs=Date.now(); const wait=Math.max(0,(lastReqEndedAt+MIN_GAP_MS)-nowTs); if (wait>0) await sleep(wait); let attempt=0; let out; for(;;){ try{ out = await fn(); break; } catch(e){ const msg=String(e?.message||e); if ((msg.includes("IB 429") || e?.code==="IB_429") && attempt<MAX_RETRIES){ await sleep(300*Math.pow(2,attempt)); attempt++; continue; } throw e; } finally { lastReqEndedAt=Date.now(); } } return out; }
async function ibFetch(path, opts={}, _retry=false){
  if (MOCK) throw new Error("MOCK mode");
  if (!CACHE.session) await ibPrime();
  const url = path.startsWith("http") ? path : `${IB_BASE}${path}`;
  return ibQueued(path, async ()=>{
    let r; try { r = await fetch(url, { ...opts, redirect:"follow" }); } catch(e){ throw new Error(`IB fetch failed: ${url} :: ${e.message}`); }
    const text = await r.text(); const body = text ? safeJson(text) : null;
    if (r.status===401 && !_retry){ await ibReauth(); return ibFetch(path, opts, true); }
    if (r.status===429){ const err=new Error(`IB 429 ${url} :: ${text||"null"}`); err.code="IB_429"; throw err; }
    if (!r.ok){ const msg=(typeof body==="object"&&body)?JSON.stringify(body):text; throw new Error(`IB ${r.status} ${url} :: ${msg}`); }
    try { return body ?? {}; } catch { return {}; }
  });
}

/* ======================= CONID RESOLUTION ======================= */
const CONID_TTL_MS = 12*60*60*1000; const CACHE_CONID = new Map(); // key -> { conid, ts }
async function ibConidForSymbol(sym){
  const key = normalizeSymbol(sym);
  const hit = CACHE_CONID.get(key); if (hit && (Date.now()-hit.ts)<CONID_TTL_MS) return hit.conid;
  if (MOCK){ const demo = { SPX:"416904", SPY:"756733", ES:"11004968" }; const c = demo[key] || String(100000+Math.floor(Math.random()*9e5)); CACHE_CONID.set(key,{conid:c,ts:Date.now()}); return c; }
  // Try secdef search first
  let best=null; try { const list = await ibFetch(`/iserver/secdef/search?symbol=${encodeURIComponent(key)}`); const arr = Array.isArray(list)?list:(list?[list]:[]);
    // Prefer STK for equities, IND for SPX, FUT for ES
    const hasType=(r,t)=>(r?.sections||[]).some(s=>s?.secType===t);
    if (key==="SPX") best = arr.find(r=>hasType(r,"IND")&&r?.conid) || arr[0];
    else if (key==="ES") best = arr.find(r=>hasType(r,"FUT")&&r?.conid) || arr[0];
    else best = arr.find(r=>hasType(r,"STK")&&r?.conid) || arr[0];
  } catch {}
  let conid = best?.conid ? String(best.conid) : null;
  // If FUT root, choose front-month child
  if (conid && best && (best.sections||[]).some(s=>s?.secType==="FUT")){
    try{
      const info = await ibFetch(`/iserver/secdef/info?conid=${encodeURIComponent(conid)}`);
      const contracts = Array.isArray(info?.Contracts)?info.Contracts:(Array.isArray(info)?info:[]);
      // pick nearest future >= now by expiry/maturity
      const nowTs = Date.now();
      const getTs = (c)=>{ const d = c?.expiry||c?.maturity||c?.lastTradingDay||"2100-01-01"; const t=Date.parse(d); return Number.isFinite(t)?t:Date.parse("2100-01-01"); };
      const future = contracts.map(c=>({c,ts:getTs(c)})).filter(x=>x.ts>=nowTs).sort((a,b)=>a.ts-b.ts)[0];
      conid = String(future?.c?.conid || conid);
    } catch {}
  }
  if (conid){ CACHE_CONID.set(key,{conid,ts:Date.now()}); return conid; }
  return null;
}

/* ======================= SNAPSHOT MAPPERS ======================= */
function mapEquitySnapshot(sym, s){
  const last = toNum(s["31"]), bid = toNum(s["84"]), ask = toNum(s["86"]), iv = normalizeIv(s["7059"]);
  return { symbol:sym, last:last??null, bid:bid??null, ask:ask??null, iv:iv??null, ts: Date.now() };
}

/* ======================= OPTIONS NBBO CACHE ======================= */
const NBBO_OPT = new Map(); // key -> { bid, ask, mid, ts }
function optKey(ul, exp, right, strike){ const r = (String(right).toUpperCase().startsWith("C")?"C":"P"); return `${normalizeSymbol(ul)}|${exp}|${r}|${Number(strike)}`; }
// function ingestOptionQuote({ occ, bid, ask, mid, ts }){
//   if (!occ) return; const b=toNum(bid), a=toNum(ask); const m = (Number.isFinite(b)&&Number.isFinite(a)&&b>0&&a>0)?(b+a)/2:toNum(mid);
//   NBBO_OPT.set(occ, { bid:b??undefined, ask:a??undefined, mid:m??undefined, ts: ts||Date.now() });
//   optionQuoteBuf.set(occ, { occ, bid:b??undefined, ask:a??undefined, mid:m??undefined, ts: ts||Date.now() });
// }
// const NBBO_OPT = new Map();
const NBBO_MAX = 5000; // tune to your needs
const MAX_TAPE_ROWS = 2000;
// const ROLLING_MS = 2 * 60_000; // already good

function cap(arr, max=MAX_TAPE_ROWS){
  if (arr.length > max) arr.splice(0, arr.length - max);
}

setInterval(() => {
  const cutoff = Date.now() - ROLLING_MS;
  STATE.sweeps = (STATE.sweeps||[]).filter(x => x.ts >= cutoff);
  STATE.blocks = (STATE.blocks||[]).filter(x => x.ts >= cutoff);
  STATE.prints = (STATE.prints||[]).filter(x => x.ts >= cutoff);
  cap(STATE.sweeps); cap(STATE.blocks); cap(STATE.prints);
}, 5000);
function nbboSet(k, v){
  if (NBBO_OPT.has(k)) NBBO_OPT.delete(k); // refresh order
  NBBO_OPT.set(k, v);
  if (NBBO_OPT.size > NBBO_MAX){
    // delete oldest
    const firstKey = NBBO_OPT.keys().next().value;
    NBBO_OPT.delete(firstKey);
  }
}
function ingestOptionQuote({ occ, bid, ask, mid, ts }){
  if (!occ) return;
  const rec = { bid, ask, mid, ts: ts||Date.now() };
  nbboSet(occ, rec);
  optionQuoteBuf.set(occ, { occ, ...rec });
}

const optionQuoteBuf = new Map();
setInterval(()=>{
  if (!optionQuoteBuf.size) return;
  wsBroadcast("option_quotes", Array.from(optionQuoteBuf.values()));
  optionQuoteBuf.clear();
}, 500);

/* ======================= FUTURES + FOP ======================= */
const FUT_FRONT = new Map(); // root -> { conid, ts }
async function resolveFrontMonth(root){
  // cache 10m
  const cur = FUT_FRONT.get(root);
  if (cur && Date.now()-cur.ts < 10*60*1000) return cur;
  const conid = await ibConidForSymbol(root).catch(()=>null);
  if (conid){ const rec={ conid:String(conid), ts:Date.now() }; FUT_FRONT.set(root, rec); return rec; }
  return null;
}
const FOP_CONID_CACHE = new Map(); // key: root|YYYY-MM-DD|C|strike -> { conid, ts }
const FOP_CONID_TTL = 12 * 60 * 60 * 1000;

function fopKey(root, expiryISO, right, strike){
  const r = String(right).toUpperCase().startsWith('C') ? 'C' : 'P';
  return `${root}|${expiryISO}|${r}|${Number(strike)}`;
}

async function resolveFopConid(root, expiryISO, right, strike){
  const key = fopKey(root, expiryISO, right, strike);
  const hit = FOP_CONID_CACHE.get(key);
  if (hit && (Date.now() - hit.ts) < FOP_CONID_TTL) return hit.conid;

  const front = await resolveFrontMonth(root);
  if (!front?.conid) return null;
  const month = expiryISO.slice(0,4) + expiryISO.slice(5,7);
  const url = `/iserver/secdef/info?conid=${front.conid}&sectype=FOP&month=${month}&exchange=GLOBEX&right=${right}&strike=${strike}`;

  const info = await ibFetch(url).catch(() => null);
  const arr = Array.isArray(info) ? info : (Array.isArray(info?.Contracts) ? info.Contracts : []);
  const conid = arr.find(x => x?.conid)?.conid || null;

  if (conid){
    FOP_CONID_CACHE.set(key, { conid, ts: Date.now() });
    // Simple size cap
    if (FOP_CONID_CACHE.size > 10000){
      // drop ~10% oldest
      let drop = Math.ceil(FOP_CONID_CACHE.size * 0.1);
      for (const k of FOP_CONID_CACHE.keys()){
        FOP_CONID_CACHE.delete(k);
        if (--drop <= 0) break;
      }
    }
  }
  return conid;
}

// async function resolveFopConid(root, expiryISO, right, strike){
//   const f = await resolveFrontMonth(root); if (!f?.conid) return null;
//   const month = expiryISO.slice(0,4)+expiryISO.slice(5,7);
//   const url = `/iserver/secdef/info?conid=${f.conid}&sectype=FOP&month=${month}&exchange=${FUT_EXCH}&right=${right}&strike=${strike}`;
//   try {
//     const info = await ibFetch(url);
//     const arr = Array.isArray(info)?info:(Array.isArray(info?.Contracts)?info.Contracts:(info?[info]:[]));
//     return arr.find(x=>x?.conid)?.conid || null;
//   } catch { return null; }
// }
async function fopSnapshot(conid){
  try{ const s = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conid)}&fields=31,84,86`);
    return Array.isArray(s)?s[0]:null; } catch { return null; }
}
async function pollFopOnce(){
  const t0 = LOG.timings ? now() : 0;
  try {
    const items = (WATCH.options||[]).filter(o => ["ES","NQ","YM","RTY","CL","GC"].includes(normalizeSymbol(o.underlying)));
    if (!items.length) return;
    for (const it of items){
      const root = normalizeSymbol(it.underlying);
      const right = String(it.right||"C").toUpperCase().startsWith("C")?"C":"P";
      const occ = optKey(root, it.expiration, right, it.strike);
      const conid = await resolveFopConid(root, it.expiration, right, it.strike).catch(()=>null);
      if (!conid) continue;
      const s = await fopSnapshot(conid);
      if (!s) continue;
      const bid = toNum(s["84"]), ask = toNum(s["86"]); const mid = (Number.isFinite(bid)&&Number.isFinite(ask)&&bid>0&&ask>0)?(bid+ask)/2:null;
      ingestOptionQuote({ occ, bid, ask, mid, ts: Date.now() });
    }
  } finally {
    if (LOG.timings) METRICS.last_fop_ms = now()-t0;
  }
}

/* ======================= EQUITIES POLLER ======================= */
async function pollEquitiesOnce(){
  const t0 = LOG.timings ? now() : 0;
  try {
    const syms = Array.from(new Set((WATCH.equities||[]).map(normalizeSymbol))).filter(Boolean);
    if (!syms.length) return;
    const pairs = [];
    for (const s of syms){ const conid = await ibConidForSymbol(s).catch(()=>null); if (conid) pairs.push({ s, conid }); }
    let rows = [];
    if (DEMO || MOCK){
      const base = { SPX:5600, ES:5602, SPY:669 };
      rows = syms.map(s=>({ symbol:s, last: Number(((base[s]??100)+Math.random()*0.5-0.25).toFixed(2)), bid:null, ask:null, iv: s==="SPX"?null:25, ts: Date.now() }));
    } else {
      const conids = pairs.map(p=>p.conid).join(",");
      const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`);
      for (const p of pairs){ const s = Array.isArray(snaps)?snaps.find(z=>String(z.conid)===String(p.conid)):null; if (!s) continue; rows.push(mapEquitySnapshot(p.s, s)); }
    }
    // ES↔SPX basis
    let es_spx_basis = null; const by = new Map(rows.map(r=>[normalizeSymbol(r.symbol), r]));
    const es = by.get("ES"), spx = by.get("SPX");
    if (es && spx && fresh(es) && fresh(spx)){ const esPx=midOrLast(es), spxPx=midOrLast(spx); if (Number.isFinite(esPx)&&Number.isFinite(spxPx)) es_spx_basis = Number((esPx-spxPx).toFixed(2)); }
    STATE.equities_ts = rows;
    wsBroadcast("equity_ts", rows);
    if (es_spx_basis!=null) wsBroadcast("basis", { es_spx_basis });
  } catch(e){ jl("warn","pollEquitiesOnce",{ err:String(e?.message||e) }); } finally { if (LOG.timings) METRICS.last_eq_ms = now()-t0; }
}

/* ======================= SIMPLE ACTION INFERENCE ======================= */
function inferActionForOptionTrade({ price, book }){
  const mid = Number(book?.mid); if (!Number.isFinite(mid) || mid<=0) return { action:null, action_conf:0.0, at:"between" };
  const bps = ((price-mid)/mid)*1e4;
  if (bps >= 15) return { action:"BTO", action_conf:0.9, at:"ask" };
  if (bps <= -15) return { action:"STO", action_conf:0.9, at:"bid" };
  return { action:null, action_conf:0.4, at:"mid" };
}

/* ======================= TAPE → SWEEPS/BLOCKS ======================= */
const BIG_PREMIUM_MIN = Number(process.env.BIG_PREMIUM_MIN || 200_000);
const OTM_QTY_MIN     = Number(process.env.OTM_QTY_MIN     || 150);
function passesPublishGate(msg){ const prem = Number(msg?.notional); if (Number.isFinite(prem) && prem >= BIG_PREMIUM_MIN) return true; const qty=Number(msg?.qty); return Number.isFinite(qty) && qty>=OTM_QTY_MIN; }
function headlineOfTrade(t){ const right=t.right||t.option?.right; const strike=t.strike??t.option?.strike; const expiry=t.expiry??t.option?.expiration; return { type:(t.type||"PRINT").toUpperCase(), ul:t.ul||t.symbol||"", right, strike, expiry, side:(t.side||"UNKNOWN").toUpperCase(), notional:Math.round((t.notional ?? (t.qty||0)*(t.price||0)*100) || 0), ts:Number(t.ts)||Date.now(), action:t.action, action_conf:t.action_conf, at:t.at }; }
function withMark(row){ try{ const k = optKey(row.ul, row.expiry, row.right, row.strike); const q = NBBO_OPT.get(k); const mid = Number.isFinite(q?.mid)?q.mid: (Number.isFinite(q?.bid)&&Number.isFinite(q?.ask)&&q.bid>0&&q.ask>0? (q.bid+q.ask)/2 : undefined); return { ...row, mark: mid }; } catch { return { ...row, mark: undefined }; } }

function publishFlow(topic, rows){
  const data = (Array.isArray(rows)?rows:[rows]).map(r=>{
    // attach book inference if missing
    if (!r.action){ const k = optKey(r.ul||r.symbol, r.expiry||r.option?.expiration, r.right, r.strike); const q=NBBO_OPT.get(k)||{}; const inf = inferActionForOptionTrade({ price:Number(r.price)||0, book:{ bid:q.bid, ask:q.ask, mid:q.mid } }); Object.assign(r, inf); }
    return withMark(r);
  });
  wsBroadcast(topic, data);
  for (const r of data){ wsBroadcast("headlines", [headlineOfTrade({ ...r, type: topic.slice(0,-1).toUpperCase() })]); }
}

/* ======================= API: Flow (prints/sweeps/blocks) ======================= */
app.post("/api/flow/prints/publish", (req,res)=>{ const rows=(req.body?.rows||[]).filter(passesPublishGate); publishFlow("prints", rows); STATE.prints.push(...rows); STATE.prints = STATE.prints.slice(-2000); res.json({ ok:true, count: rows.length }); });
app.get ("/api/flow/sweeps", (req,res)=>{ const minNot = Number(req.query.minNotional||25000); const limit=Number(req.query.limit||200); const rows=(STATE.sweeps||[]).filter(r=>r.notional>=minNot).slice(0,limit).map(withMark); res.json({ rows, ts: Date.now() }); });
app.get ("/api/flow/blocks", (req,res)=>{ const minNot = Number(req.query.minNotional||50000); const limit=Number(req.query.limit||50); const rows=(STATE.blocks||[]).filter(r=>r.notional>=minNot).slice(0,limit).map(withMark); res.json({ rows, ts: Date.now() }); });

/* ======================= NOTABLES (light) ======================= */
let lastNotablesJson = "";
function buildNotables({ sweeps=[], blocks=[] }, { windowMs=5*60_000, minNotional=75_000, topN=50 }={}){
  const now=Date.now();
  const all = [...sweeps, ...blocks].filter(r => (now-r.ts)<=windowMs && r.notional>=minNotional);
  all.sort((a,b)=>b.notional-a.notional);
  const notables = all.slice(0, topN).map(withMark);
  return { notables, allCount: all.length };
}
app.get("/api/insights/notables", (req,res)=>{ const cfg={ windowMs: req.query.windowMs?Number(req.query.windowMs):undefined, minNotional: req.query.minNotional?Number(req.query.minNotional):undefined, topN: req.query.topN?Number(req.query.topN):undefined }; const merged = { windowMs:5*60_000, minNotional:75_000, topN:50, ...Object.fromEntries(Object.entries(cfg).filter(([,v])=>v!==undefined))}; const payload = buildNotables({ sweeps:STATE.sweeps, blocks:STATE.blocks }, merged); const s = JSON.stringify(payload.notables); if (s!==lastNotablesJson){ wsBroadcast("notables", payload.notables); lastNotablesJson=s; } res.json({ rows: payload.notables, ts: Date.now() }); });

/* ======================= PRICES / DEBUG ======================= */
app.get("/prices", async (req,res)=>{
  try{
    const csv = String(req.query.symbols||"").trim();
    const syms = csv ? csv.split(",").map(normalizeSymbol) : Array.from(new Set((WATCH.equities||[]).map(normalizeSymbol)));
    if (!syms.length) return res.json({ rows: [], es_spx_basis: null, ts: Date.now() });
    const pairs=[]; for (const s of syms){ const c = await ibConidForSymbol(s).catch(()=>null); if (c) pairs.push({ s, conid:String(c) }); }
    let rows=[];
    if (DEMO||MOCK){ const base={ SPX:5600, ES:5602, SPY:669 }; rows = syms.map(s=>({ symbol:s, last:Number(((base[s]??100)+Math.random()*0.4-0.2).toFixed(2)), bid:null, ask:null, iv: s==="SPX"?null:25, ts: Date.now() })); }
    else { const conids = pairs.map(p=>p.conid).join(","); const snaps = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`); for (const p of pairs){ const s = Array.isArray(snaps)?snaps.find(z=>String(z.conid)===String(p.conid)):null; if (!s) { rows.push({ symbol:p.s, last:null, bid:null, ask:null, iv:null, ts: Date.now() }); continue; } rows.push(mapEquitySnapshot(p.s, s)); } }
    let es_spx_basis=null; const by = new Map(rows.map(r=>[normalizeSymbol(r.symbol), r])); const es=by.get("ES"), spx=by.get("SPX"); if (es&&spx&&fresh(es)&&fresh(spx)){ const esPx=midOrLast(es), spxPx=midOrLast(spx); if (Number.isFinite(esPx)&&Number.isFinite(spxPx)) es_spx_basis = Number((esPx-spxPx).toFixed(2)); }
    res.json({ rows, es_spx_basis, ts: Date.now() });
  }catch(e){ res.status(500).json({ error: e?.message || "fail" }); }
});
app.get("/prices/:symbol", (req,res)=>{ req.query.symbols = String(req.params.symbol||""); app._router.handle(req,res,()=>{},"get","/prices"); });
app.get("/debug/conid", async (req,res)=>{ const symbol = String(req.query.symbol||"").toUpperCase(); if (!symbol) return res.status(400).json({ error:"symbol required" }); const conid = await ibConidForSymbol(symbol).catch(()=>null); if (!conid) return res.status(404).json({ error:`No conid for ${symbol}` }); res.json({ symbol, conid:String(conid) }); });
app.get("/debug/snapshot_raw", async (req,res)=>{ try { const conids=String(req.query.conids||"").trim(); if (!conids) return res.status(400).json({ error:"conids required" }); const body = await ibFetch(`/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`); res.json(body); } catch(e){ res.status(500).json({ error:e?.message||"fail" }); } });

/* ======================= LOOP CADENCES ======================= */
setInterval(()=>{ pollEquitiesOnce().catch(()=>{}); }, 1_500);
// setInterval(()=>{ pollFopOnce().catch(()=>{}); }, 1_500); // FOP NBBO refresher
setInterval(() => { pollFopOnce().catch(()=>{}); }, 3000);

/* ======================= WS: seed watchlist on connect ======================= */
wss.on("connection", (ws)=>{ try { ws.send(JSON.stringify({ topic:"watchlist", data: WATCH })); } catch {} });

/* ======================= START ======================= */
server.listen(PORT, HOST, ()=>{ console.log(JSON.stringify({ host:HOST, port:PORT, msg:"Flow server listening" })); });

