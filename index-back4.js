// index.js  — TradeFlash drop-in server (HTTP+WS) with IBKR chains fix
// Node >=18 (built-in fetch). If self-signed cert, run with NODE_TLS_REJECT_UNAUTHORIZED=0

import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import fs from "fs/promises";
import { WebSocketServer } from "ws";

/* ================= CONFIG ================= */
const MOCK = String(process.env.MOCK ?? "").trim() === "1";
const PORT = Number(process.env.PORT || 8080);

// IMPORTANT: IB HOST and BASE
// Keep HOST at https://127.0.0.1:5000 when using Client Portal Gateway
const IB_HOST = process.env.IB_HOST || "https://127.0.0.1:5000";
const IB_BASE = process.env.IB_BASE || `${IB_HOST}/v1/api`;          // e.g., https://127.0.0.1:5000/v1/api
const IB_TICKLE = `${IB_HOST}/v1/api/tickle`;                        // correct tickle (NOT /iserver/tickle)

// When developing with the CP Gateway’s self-signed cert, you may need:
//   export NODE_TLS_REJECT_UNAUTHORIZED=0
if (process.env.NODE_TLS_REJECT_UNAUTHORIZED === "0") {
  console.warn("WARNING: TLS verification disabled (self-signed).");
}
// predictable state for UI
const WATCH = global.WATCH || { equities: [], options: [] };
global.WATCH = WATCH;

const FLOW = global.FLOW || {
  equity_ts: [],   // array of rows (must have id)
  options_ts: [],  // array of rows (must have id)
  sweeps: [],
  blocks: []
};
global.FLOW = FLOW;
/* ================ APP + WS ================= */
const app = express();
app.use(cors());
app.use(compression());
app.use(express.json({ limit: "1mb" }));
app.use(morgan("dev"));

// kill implicit 304s on tiny JSON endpoints
app.set("etag", false);

// small helper so every flow route is no-store
function noStore(res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate: false });

// function wsBroadcast(obj) {
//   const s = JSON.stringify(obj);
//   for (const c of wss.clients) if (c.readyState === 1) c.send(s);
// }

/* ============ WATCHLIST (persisted) ============ */
const WATCH_FILE = "./watchlist.json";

globalThis.WATCH = globalThis.WATCH ?? {
  equities: new Set(["AAPL", "NVDA", "MSFT"]),  // default seed (feel free to remove)
  options: new Set(), // each is JSON.stringify({underlying, expiration, strike, right})
};
// ====== IN-MEMORY STATE ======
const STATE = {
  equities_ts: [],   // array<EquityTick>
  options_ts: [],    // array<OptionTick>
  sweeps: [],        // keep shape your UI expects (stubbed)
  blocks: [],        // keep shape your UI expects (stubbed)
  seq: 0,            // unique id seed to avoid duplicate React keys
};

// utility: unique id for list keys
function nextId(prefix) { STATE.seq = (STATE.seq + 1) % 1e9; return `${prefix}_${Date.now()}_${STATE.seq}`; }

// ====== BROADCAST HELPERS ======
function wsBroadcast(topic, payload) {
  const msg = JSON.stringify({ topic, data: payload });
  for (const c of wss.clients) if (c.readyState === 1) c.send(msg);
}

// ====== EQUITY SNAPSHOT POLLER ======
// Uses WATCH.equities if you have it; otherwise hard-code defaults.
// const WATCH = global.WATCH || { equities: new Set(["AAPL","NVDA","MSFT"]), options: [] };

// Maps IB snapshot fields to a UI-friendly row
// function mapEquitySnapshot(symbol, snap) {
//   // IB snapshot (we’re requesting fields 31(last),84(bid),86(ask),7059(market data availability))
//   const last = Number(snap["31"] ?? NaN);
//   const bid  = Number(snap["84"] ?? NaN);
//   const ask  = Number(snap["86"] ?? NaN);
//   return {
//     id: nextId(`eq_${symbol}`),
//     ts: Date.now(),
//     symbol,
//     last: Number.isFinite(last) ? last : null,
//     bid:  Number.isFinite(bid)  ? bid  : null,
//     ask:  Number.isFinite(ask)  ? ask  : null,
//   };
// }
// async function pollEquitiesOnce() {
//   try {
//     const symbols = normEquities(WATCH).map(s => String(s).toUpperCase());
//     if (!symbols.length) return;

//     // resolve conids
//     const pairs = [];
//     for (const sym of symbols) {
//       const conid = await ibConidForStock(sym);
//       if (conid) pairs.push({ sym, conid });
//     }
//     if (!pairs.length) return;

//     const conids = pairs.map(p => p.conid).join(",");
//     const url = `${IB_BASE}/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`;
//     const snaps = await ibFetchJson(url);

//     const rows = [];
//     for (const p of pairs) {
//       const snap = Array.isArray(snaps) ? snaps.find(s => String(s.conid) === String(p.conid)) : null;
//       if (!snap) continue;
//       rows.push(mapEquitySnapshot(p.sym, snap));
//     }

//     if (rows.length) {
//       STATE.equities_ts = rows;
//       wsBroadcast("equity_ts", rows);
//     }
//   } catch (e) {
//     console.warn("pollEquitiesOnce error:", e.message);
//   }
// }
// async function pollEquitiesOnce() {
//   try {
//     const symbols = Array.from(WATCH.equities || []);
//     if (!symbols.length) return;

//     // resolve conids
//     const pairs = [];
//     for (const sym of symbols) {
//       const conid = await ibConidForStock(sym);
//       if (conid) pairs.push({ sym, conid });
//     }
//     if (!pairs.length) return;

//     // snapshot in small batches (IB likes <=50 per call; we have just a few)
//     const conids = pairs.map(p => p.conid).join(",");
//     const url = `${IB_BASE}/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`;
//     const snaps = await ibFetchJson(url);

//     // IB returns an array of { conid, "31": last, "84": bid, "86": ask, ... }
//     const rows = [];
//     for (const p of pairs) {
//       const snap = Array.isArray(snaps) ? snaps.find(s => String(s.conid) === String(p.conid)) : null;
//       if (!snap) continue;
//       rows.push(mapEquitySnapshot(p.sym, snap));
//     }

//     if (rows.length) {
//       STATE.equities_ts = rows;
//       wsBroadcast("equity_ts", rows);
//     }
//   } catch (e) {
//     console.warn("pollEquitiesOnce error:", e.message);
//   }
// }

// Run every 2s during market hours (tweak as needed)
// setInterval(pollEquitiesOnce, 2000);

// ====== OPTIONS SNAPSHOT (MINIMAL DEMO) ======
// function mapOptionTick(ul, expiry, strike, right, snap) {
//   const last = Number(snap["31"] ?? NaN);
//   const bid  = Number(snap["84"] ?? NaN);
//   const ask  = Number(snap["86"] ?? NaN);
//   const occ  = `${ul} ${expiry.replace(/-/g,"").slice(2)} ${right}${strike}`;
//   return {
//     id: nextId(`oc_${ul}`),
//     ts: Date.now(),
//     underlying: ul,
//     expiry,
//     strike,
//     right,          // "C" | "P"
//     occ,            // OCC-like string, used by some UIs
//     last: Number.isFinite(last) ? last : null,
//     bid:  Number.isFinite(bid)  ? bid  : null,
//     ask:  Number.isFinite(ask)  ? ask  : null,
//   };
// }
// ---------- NORMALIZERS (drop this near your WATCH definition) ----------
function normEquities(w) {
  if (!w) return [];
  const v = w.equities;
  if (!v) return [];
  if (Array.isArray(v)) return v;
  if (v instanceof Set) return Array.from(v);
  if (typeof v === "object") return Object.keys(v); // e.g. {AAPL:true}
  return [];
}

function normOptions(w) {
  if (!w) return [];
  const v = w.options;
  if (!v) return [];
  if (Array.isArray(v)) return v;                 // [{underlying, expiry/expiration, strike, right}]
  if (v instanceof Set) return Array.from(v);     // Set of same objects or OCC strings
  if (typeof v === "object") {
    // Support map-like { "AAPL 20251219 C200": true } or { AAPL:[...] }
    const out = [];
    for (const [k, val] of Object.entries(v)) {
      if (typeof val === "boolean" && k.includes(" ")) {
        // OCC-ish key -> try to parse "AAPL 20251219 C200"
        const m = k.match(/^([A-Z]+)\s+(\d{8})\s+([CP])(\d+(?:\.\d+)?)$/);
        if (m) {
          const [, ul, yyyymmdd, r, str] = m;
          out.push({ underlying: ul, expiration: `${yyyymmdd.slice(0,4)}-${yyyymmdd.slice(4,6)}-${yyyymmdd.slice(6,8)}`, strike: Number(str), right: r });
        }
      } else if (Array.isArray(val)) {
        for (const it of val) out.push(it);
      }
    }
    return out;
  }
  return [];
}

// Pick one contract per UL for demo: closest listed monthly (3rd Fri) at ~ATM
// async function pollOptionsOnce() {
//   try {
//     const underlyings = new Set((WATCH.options || []).map(o => o.underlying));
//     // also include equities watch as underlyings for a demo chain
//     for (const s of (WATCH.equities || [])) underlyings.add(s);

//     const ticks = [];

//     for (const ul of underlyings) {
//       // 1) last price
//       const conid = await ibConidForStock(ul);
//       if (!conid) continue;
//       const lastSnap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${conid}&fields=31`);
//       const lastRow  = Array.isArray(lastSnap) && lastSnap[0] ? lastSnap[0] : {};
//       const underPx  = Number(lastRow["31"] ?? NaN);
//       if (!Number.isFinite(underPx)) continue;

//       // 2) expirations (from your new helper)
//       const expiries = await ibOptMonthsForSymbol(ul);
//       if (!expiries.length) continue;

//       // nearest in future
//       expiries.sort();
//       const expiry = expiries[0];

//       // 3) strike ≈ round(underPx/5)*5 (IB strikes usually in $5 grid beyond some ranges)
//       const grid = underPx < 50 ? 1 : underPx < 200 ? 5 : 5;
//       const strike = Math.round(underPx / grid) * grid;

//       // 4) resolve a real option conid via secdef/info
//       // month=YYYYMM and right=C|P and strike (plain, not 200.000)
//       const yyyy = expiry.slice(0, 4);
//       const mm   = expiry.slice(5, 7);
//       const month = `${yyyy}${mm}`;
//       for (const right of ["C", "P"]) {
//         // Try info; if it fails, skip (some gateways omit direct info but strikes + conid chain is doable)
//         try {
//           const infoUrl = `${IB_BASE}/iserver/secdef/info?conid=${conid}&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${strike}`;
//           const info = await ibFetchJson(infoUrl);
//           // some gateways return array of contracts, some object — try to find conid
//           const arr = Array.isArray(info) ? info : (Array.isArray(info?.Contracts) ? info.Contracts : []);
//           const optConid = (arr[0]?.conid) || (info?.conid);
//           if (!optConid) continue;

//           // 5) snap for the option
//           const snap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86`);
//           const s0 = Array.isArray(snap) && snap[0] ? snap[0] : {};
//           const tick = mapOptionTick(ul, expiry, strike, right, s0);
//           ticks.push(tick);
//         } catch {
//           // ignore; not all combos will exist
//         }
//       }
//     }

//     if (ticks.length) {
//       STATE.options_ts = ticks;
//       wsBroadcast("options_ts", ticks);
//     }
//   } catch (e) {
//     console.warn("pollOptionsOnce error:", e.message);
//   }
// }
// ---------- helpers (safe mappers & utils) ----------

function nowTs() { return Date.now(); }

function safeNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

// IB snapshot -> equity row the UI can render
function mapEquitySnapshot(sym, snap) {
  const last = snap?.["31"] ?? snap?.last ?? null;
  const bid  = snap?.["84"] ?? snap?.bid ?? null;
  const ask  = snap?.["86"] ?? snap?.ask ?? null;

  return {
    id: `EQ-${sym}`,
    ts: nowTs(),
    symbol: sym,
    last: safeNum(last),
    bid:  safeNum(bid),
    ask:  safeNum(ask),
    text: last != null ? `${sym} ${last}` : sym
  };
}

// Option tick row for UI
function mapOptionTick(ul, expiryISO, strike, right, snap) {
  // expiryISO like "2025-12-19"
  const last = snap?.["31"] ?? snap?.last ?? null;
  const bid  = snap?.["84"] ?? snap?.bid ?? null;
  const ask  = snap?.["86"] ?? snap?.ask ?? null;

  const yyyymmdd = String(expiryISO).replaceAll("-", "");
  const occ = `${ul} ${yyyymmdd} ${right}${strike}`;

  return {
    id: `OPT-${ul}-${yyyymmdd}-${right}${strike}`,
    ts: nowTs(),
    symbol: ul,
    occ,
    right,
    strike,
    expiration: expiryISO,
    last: safeNum(last),
    bid:  safeNum(bid),
    ask:  safeNum(ask),
    text: `${ul} ${right}${strike} ${expiryISO}`
  };
}

// tolerant JSON fetch (arrays or objects)
// async function ibFetchJson(url) {
//   const r = await fetch(url, { headers: { "Content-Type": "application/json" } });
//   if (!r.ok) {
//     const t = await r.text().catch(() => "");
//     throw new Error(`${r.status} ${t || url}`);
//   }
//   return r.json();
// }

// best-effort way to normalize secdef/info shapes to array of contracts
function asContracts(x) {
  if (Array.isArray(x)) return x;
  if (x && Array.isArray(x.Contracts)) return x.Contracts;
  if (x && x.conid) return [x];
  return [];
}

// async function pollOptionsOnce() {
//   try {
//     const ulSet = new Set(normEquities(WATCH).map(s => s.toUpperCase()));
//     for (const o of normOptions(WATCH)) if (o?.underlying) ulSet.add(String(o.underlying).toUpperCase());

//     const underlyings = Array.from(ulSet);
//     if (!underlyings.length) return;

//     const ticks = [];

//     for (const ul of underlyings) {
//       const conid = await ibConidForStock(ul);
//       if (!conid) continue;

//       const lastSnap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${conid}&fields=31`);
//       const underPx  = Number((Array.isArray(lastSnap) && lastSnap[0] && lastSnap[0]["31"]) ?? NaN);
//       if (!Number.isFinite(underPx)) continue;

//       const expiries = await ibOptMonthsForSymbol(ul);
//       if (!expiries.length) continue;
//       expiries.sort();
//       const expiry = expiries[0];

//       const grid = underPx < 50 ? 1 : 5;
//       const strike = Math.round(underPx / grid) * grid;
//       const yyyy = expiry.slice(0, 4);
//       const mm   = expiry.slice(5, 7);
//       const month = `${yyyy}${mm}`;

//       for (const right of ["C", "P"]) {
//         try {
//           const infoUrl = `${IB_BASE}/iserver/secdef/info?conid=${conid}&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${strike}`;
//           const info = await ibFetchJson(infoUrl);
//           const arr = Array.isArray(info) ? info
//                    : Array.isArray(info?.Contracts) ? info.Contracts
//                    : (info ? [info] : []);
//           const optConid = arr.find(x => x?.conid)?.conid;
//           if (!optConid) continue;

//           const snap = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86`);
//           const s0 = Array.isArray(snap) && snap[0] ? snap[0] : {};
//           const tick = mapOptionTick(ul, expiry, strike, right, s0);
//           ticks.push(tick);
//         } catch {}
//       }
//     }

//     if (ticks.length) {
//       STATE.options_ts = ticks;
//       wsBroadcast("options_ts", ticks);
//     }
//   } catch (e) {
//     console.warn("pollOptionsOnce error:", e.message);
//   }
// }
async function pollEquitiesOnce() {
  try {
    const symbols = normEquities(WATCH).map(s => String(s).toUpperCase());
    if (!symbols.length) return;

    // Resolve all conids in parallel
    const pairPromises = symbols.map(async (sym) => {
      try {
        const conid = await ibConidForStock(sym);
        return conid ? { sym, conid: String(conid) } : null;
      } catch { return null; }
    });

    const pairs = (await Promise.all(pairPromises)).filter(Boolean);
    if (!pairs.length) return;

    const conids = pairs.map(p => p.conid).join(",");
    const url = `${IB_BASE}/iserver/marketdata/snapshot?conids=${encodeURIComponent(conids)}&fields=31,84,86,7059`;

    const snapsRaw = await ibFetchJson(url);
    const snaps = Array.isArray(snapsRaw) ? snapsRaw : [];
    const byConid = new Map(snaps.map(s => [String(s.conid), s]));

    const rows = [];
    for (const p of pairs) {
      const snap = byConid.get(String(p.conid));
      if (!snap) continue;
      rows.push(mapEquitySnapshot(p.sym, snap));
    }

    if (rows.length) {
      STATE.equities_ts = rows;
      wsBroadcast("equity_ts", rows);
    } else {
      // keep UI happy with an empty array (not undefined)
      STATE.equities_ts = [];
      wsBroadcast("equity_ts", []);
    }
  } catch (e) {
    console.warn("pollEquitiesOnce error:", e.message);
    // don't break UI
    if (!Array.isArray(STATE.equities_ts)) {
      STATE.equities_ts = [];
      wsBroadcast("equity_ts", []);
    }
  }
}
async function pollOptionsOnce() {
  try {
    // build the underlying universe
    const ulSet = new Set(normEquities(WATCH).map(s => s.toUpperCase()));
    for (const o of normOptions(WATCH)) if (o?.underlying) ulSet.add(String(o.underlying).toUpperCase());

    const underlyings = Array.from(ulSet);
    if (!underlyings.length) {
      STATE.options_ts = [];
      wsBroadcast("options_ts", []);
      return;
    }

    // Resolve stock conids first (parallel)
    const ulPairs = (await Promise.all(underlyings.map(async (ul) => {
      try {
        const conid = await ibConidForStock(ul);
        return conid ? { ul, conid: String(conid) } : null;
      } catch { return null; }
    }))).filter(Boolean);

    if (!ulPairs.length) {
      STATE.options_ts = [];
      wsBroadcast("options_ts", []);
      return;
    }

    // Get underlying last price per conid (parallel fetch in small batches)
    async function snapLast(conid) {
      const s = await ibFetchJson(`${IB_BASE}/iserver/marketdata/snapshot?conids=${conid}&fields=31`);
      const row = Array.isArray(s) && s[0] ? s[0] : null;
      return safeNum(row?.["31"] ?? row?.last);
    }

    const priced = [];
    for (const pair of ulPairs) {
      try {
        const last = await snapLast(pair.conid);
        if (last != null) priced.push({ ...pair, last });
      } catch {}
    }
    if (!priced.length) {
      STATE.options_ts = [];
      wsBroadcast("options_ts", []);
      return;
    }

    // For each underlying, choose a nearest monthly expiry (you already have ibOptMonthsForSymbol)
    const ticks = [];

    for (const p of priced) {
      const ul = p.ul;
      const ulPx = p.last;

      // get expiries from your cached/parsed months (fast path)
      let expiries = [];
      try {
        expiries = await ibOptMonthsForSymbol(ul); // returns ["2025-12-19", ...]
      } catch {}
      if (!expiries?.length) continue;

      // pick nearest (sorted ISO ascending)
      expiries.sort(); // ISO dates sort lexicographically
      const expiryISO = expiries[0];

      // round strike to a reasonable grid
      const grid = ulPx < 50 ? 1 : 5;
      const strike = Math.round(ulPx / grid) * grid;

      // IB /secdef/info wants month=YYYYMM (not day)
      const yyyy = expiryISO.slice(0, 4);
      const mm   = expiryISO.slice(5, 7);
      const month = `${yyyy}${mm}`;

      // Resolve a single contract per side (C/P), then snapshot
      for (const right of ["C", "P"]) {
        try {
          const infoUrl =
            `${IB_BASE}/iserver/secdef/info?conid=${p.conid}` +
            `&sectype=OPT&month=${month}&exchange=SMART&right=${right}&strike=${strike}`;

          const info = await ibFetchJson(infoUrl);
          const contracts = asContracts(info);
          const optConid = String(contracts.find(c => c?.conid)?.conid || "");

          if (!optConid) continue;

          const snap = await ibFetchJson(
            `${IB_BASE}/iserver/marketdata/snapshot?conids=${optConid}&fields=31,84,86`
          );
          const s0 = Array.isArray(snap) && snap[0] ? snap[0] : {};
          ticks.push(mapOptionTick(ul, expiryISO, strike, right, s0));
        } catch (e) {
          // ignore single-leg failures, continue building tape
        }
      }
    }

    STATE.options_ts = Array.isArray(ticks) ? ticks : [];
    wsBroadcast("options_ts", STATE.options_ts);
  } catch (e) {
    console.warn("pollOptionsOnce error:", e.message);
    if (!Array.isArray(STATE.options_ts)) {
      STATE.options_ts = [];
      wsBroadcast("options_ts", []);
    }
  }
}
setInterval(() => {
  pollEquitiesOnce().catch(()=>{});
  pollOptionsOnce().catch(()=>{});
}, 1500);
// Poll options lighter to respect IB pacing
// setInterval(pollOptionsOnce, 5000);

// ====== REST ROUTES EXPECTED BY YOUR CLIENT ======
// app.get("/api/flow/equity_ts", (req, res) => {
//   res.json(STATE.equities_ts);
// });
// app.get("/api/flow/options_ts", (req, res) => {
//   res.json(STATE.options_ts);
// });
// app.get("/api/flow/sweeps", (req, res) => {
//   // TODO: populate with your real sweep detector
//   res.json(STATE.sweeps);
// });
// app.get("/api/flow/blocks", (req, res) => {
//   // TODO: populate with your real block tape
//   res.json(STATE.blocks);
// });
// ----- FLOW FEEDS (UI pulls these) -----
app.get("/api/flow/equity_ts", (req, res) => {
  noStore(res);
  res.json(Array.isArray(FLOW.equity_ts) ? FLOW.equity_ts : []);
});

app.get("/api/flow/options_ts", (req, res) => {
  noStore(res);
  res.json(Array.isArray(FLOW.options_ts) ? FLOW.options_ts : []);
});

app.get("/api/flow/sweeps", (req, res) => {
  noStore(res);
  res.json(Array.isArray(FLOW.sweeps) ? FLOW.sweeps : []);
});

app.get("/api/flow/blocks", (req, res) => {
  noStore(res);
  res.json(Array.isArray(FLOW.blocks) ? FLOW.blocks : []);
});

// function nowTs() { return Date.now(); }

function equityRowFromSymbol(sym, snap) {
  // snap fields: 31=last, 84=bid, 86=ask, 7059=trading class (if present)
  const last = snap?.["31"] ?? null;
  const bid  = snap?.["84"] ?? null;
  const ask  = snap?.["86"] ?? null;
  return {
    id: `EQ-${sym}`,
    ts: nowTs(),
    symbol: sym,
    last, bid, ask,
    text: last != null ? `${sym} ${last}` : sym
  };
}

function optionRowFromSpec(spec, quote) {
  const { underlying, expiration, strike, right } = spec;
  const key = `${underlying}-${expiration}-${right}${strike}`;
  const last = quote?.last ?? null;
  return {
    id: `OPT-${key}`,
    ts: nowTs(),
    symbol: underlying,
    occ: `${underlying} ${expiration.replaceAll("-","")} ${right}${strike}`,
    right, strike, expiration,
    last,
    text: `${underlying} ${right}${strike} ${expiration}`
  };
}

function normOptionLeg(o) {
  return {
    underlying: String(o.underlying || o.ul || "").toUpperCase(),
    expiration: String(o.expiration || o.exp || ""),
    strike: Number(o.strike),
    right: String(o.right || o.r || "").slice(0, 1).toUpperCase(), // C or P
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
  } catch (_) { /* first run: ignore */ }
}
async function saveWatch() {
  const out = {
    equities: Array.from(WATCH.equities),
    options: Array.from(WATCH.options).map(s => JSON.parse(s)),
  };
  await fs.writeFile(WATCH_FILE, JSON.stringify(out, null, 2));
}
await loadWatch();

// async function watchAddEquities(list) {
//   let added = 0;
//   for (const s of list || []) {
//     const sym = String(s || "").toUpperCase();
//     if (sym && !WATCH.equities.has(sym)) { WATCH.equities.add(sym); added++; }
//   }
//   if (added) await saveWatch();
//   return added;
// }
// async function watchAddOptions(list) {
//   let added = 0;
//   for (const o of list || []) {
//     const leg = normOptionLeg(o);
//     const key = JSON.stringify(leg);
//     if (!WATCH.options.has(key) && leg.underlying && leg.expiration && Number.isFinite(leg.strike) && (leg.right === "C" || leg.right === "P")) {
//       WATCH.options.add(key);
//       added++;
//     }
//   }
//   if (added) await saveWatch();
//   return added;
// }
// function getWatchlist() {
//   return {
//     equities: Array.from(WATCH.equities),
//     options: Array.from(WATCH.options).map(s => JSON.parse(s)),
//   };
// }

/* ============== IB HTTP HELPERS ============== */
async function ibGet(path, query = "") {
  const url = `${IB_BASE}${path}${query ? (path.includes("?") ? "&" : "?") + query : ""}`;
  const r = await fetch(url, { method: "GET" });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`GET ${path} ${r.status}: ${txt || r.statusText}`);
  }
  const ct = r.headers.get("content-type") || "";
  if (ct.includes("application/json")) return r.json();
  return r.text();
}
async function ibPost(path, body) {
  const url = `${IB_BASE}${path}`;
  const r = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`POST ${path} ${r.status}: ${txt || r.statusText}`);
  }
  const ct = r.headers.get("content-type") || "";
  if (ct.includes("application/json")) return r.json();
  return r.text();
}
// --- add/replace this helper ---
async function ibFetchJson(url, opts) {
  const r = await fetch(url, { ...opts });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    throw new Error(`IB ${r.status} ${url} :: ${text || r.statusText}`);
  }
  return r.json();
}

// Prefer US/NASDAQ or SMART listing for the exact symbol
function pickUsPrimaryStock(records, symbol) {
  const S = symbol.toUpperCase();
  const cand = [];
  for (const rec of records || []) {
    // Some IB responses are arrays of objects; others nest under "symbols" etc.
    const sym = (rec.symbol || rec.localSymbol || "").toUpperCase();
    if (sym !== S) continue;

    const header = (rec.companyHeader || "").toUpperCase();
    const desc   = (rec.description || "").toUpperCase();
    const sec    = (rec.sections || []).map(s => (s.secType || "").toUpperCase());

    // must be a stock line OR clearly the equity header row
    const isStocky = sec.includes("STK") || header.includes("NASDAQ") || desc.includes("NASDAQ");
    if (!isStocky) continue;

    // score: prefer NASDAQ / SMART
    let score = 0;
    if (header.includes("NASDAQ") || desc.includes("NASDAQ")) score += 5;
    if (desc.includes("SMART")) score += 2;
    cand.push({ rec, score });
  }
  cand.sort((a,b) => b.score - a.score);
  return cand.length ? cand[0].rec : null;
}

// Try /trsrv/stocks first
async function ibConidFromTrsrv(symbol) {
  const url = `${IB_BASE}/trsrv/stocks?symbols=${encodeURIComponent(symbol)}`;
  const data = await ibFetchJson(url);
  // IB tends to return { AAPL:[{...},{...}] } or an array of objects
  let list = [];
  if (Array.isArray(data)) list = data;
  else if (data && typeof data === "object") {
    const key = Object.keys(data)[0];
    list = Array.isArray(data[key]) ? data[key] : [];
  }
  const chosen = pickUsPrimaryStock(list, symbol);
  const conid = chosen && (chosen.conid || chosen.contractId || chosen.id);
  return conid ? String(conid) : null;
}

// Fallback: /iserver/secdef/search?symbol=SYMBOL and pick the STK header row
async function ibConidFromSecdefSearch(symbol) {
  const url = `${IB_BASE}/iserver/secdef/search?symbol=${encodeURIComponent(symbol)}`;
  const arr = await ibFetchJson(url);
  if (!Array.isArray(arr)) return null;

  const chosen = pickUsPrimaryStock(arr, symbol);
  const conid = chosen && (chosen.conid || chosen.contractId || chosen.id);
  return conid ? String(conid) : null;
}

// Public: robust resolver
async function ibConidForStock(symbol) {
  // 1) trsrv
  try {
    const c1 = await ibConidFromTrsrv(symbol);
    if (c1) return c1;
  } catch (e) {
    console.warn("trsrv lookup failed:", e.message);
  }
  // 2) secdef/search fallback
  try {
    const c2 = await ibConidFromSecdefSearch(symbol);
    if (c2) return c2;
  } catch (e) {
    console.warn("secdef/search lookup failed:", e.message);
  }
  return null;
}
// "NOV25" -> { y:2025, m:11 }
function parseMonthToken(tok) {
  const map = { JAN:1, FEB:2, MAR:3, APR:4, MAY:5, JUN:6, JUL:7, AUG:8, SEP:9, OCT:10, NOV:11, DEC:12 };
  const m3 = tok.slice(0,3).toUpperCase();
  const yy = tok.slice(3); // "25"
  const m = map[m3];
  const y = 2000 + Number(yy);
  if (!m || !y) return null;
  return { y, m };
}
function pad2(n){ return n < 10 ? `0${n}` : String(n); }

// Third Friday (common monthly options standard)
// function thirdFriday(y, m) {
//   // JS months are 0-based
//   const d = new Date(y, m - 1, 1);
//   // 0=Sun … 5=Fri … 6=Sat
//   let fridays = 0;
//   while (d.getMonth() === (m - 1)) {
//     if (d.getDay() === 5) { // Friday
//       fridays++;
//       if (fridays === 3) {
//         return `${d.getFullYear()}-${pad2(d.getMonth()+1)}-${pad2(d.getDate())}`;
//       }
//     }
//     d.setDate(d.getDate() + 1);
//   }
//   // Fallback: end-of-month if something odd happens
//   return `${y}-${pad2(m)}-15`;
// }

// Pull the OPT section months string
async function ibOptMonthsForSymbol(symbol) {
  const url = `${IB_BASE}/iserver/secdef/search?symbol=${encodeURIComponent(symbol)}`;
  const arr = await ibFetchJson(url);
  if (!Array.isArray(arr)) return [];

  // find the row for the equity, then find the OPT section
  const chosen = pickUsPrimaryStock(arr, symbol);
  const sections = (chosen && chosen.sections) || [];
  const opt = sections.find(s => (s.secType || "").toUpperCase() === "OPT");
  if (!opt || !opt.months) return [];

  // months string like "NOV25;DEC25;JAN26;…"
  const toks = String(opt.months).split(";").map(s => s.trim()).filter(Boolean);
  const out = [];
  for (const t of toks) {
    const pm = parseMonthToken(t);
    if (pm) out.push(thirdFriday(pm.y, pm.m));
  }
  return out;
}

/* ============ IB SESSION PRIMING ============ */
async function ibEnsureAuthAndAccounts() {
  // 1) status
  let status = await ibGet("/iserver/auth/status").catch(() => null);

  if (!status || !status.authenticated) {
    // try reauth + tickle
    await ibPost("/iserver/reauthenticate", {}).catch(() => {});
    await fetch(IB_TICKLE).catch(() => {});
    status = await ibGet("/iserver/auth/status").catch(() => null);
    if (!status || !status.authenticated) throw new Error("IB auth failed");
  }

  // 2) accounts
  const acct = await ibGet("/iserver/accounts").catch(() => null);
  if (!acct || !Array.isArray(acct.accounts) || !acct.accounts.length) {
    throw new Error("No IB accounts");
  }
  return acct.accounts;
}

/* ============== CONID + SNAPSHOT CACHE ============== */
const CONID = new Map(); // symbol -> conid
async function conidForStock(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (CONID.has(sym)) return CONID.get(sym);
  const j = await ibGet("/trsrv/stocks", `symbols=${encodeURIComponent(sym)}`);
  const first = Array.isArray(j) ? j[0] : null;
  const c = first?.conid || first?.contracts?.[0]?.conid;
  if (!c) throw new Error(`No conid for ${sym}`);
  CONID.set(sym, Number(c));
  return Number(c);
}
async function lastPriceForConid(conid) {
  const j = await ibGet("/iserver/marketdata/snapshot", `conids=${conid}&fields=31`);
  // IBKR returns an array of objects with {31: price, ...}
  const first = Array.isArray(j) ? j[0] : j?.[0];
  const px = first?.["31"];
  return typeof px === "number" ? px : Number(px ?? 0);
}

/* ======= EXPIRY PARSER (from secdef/search months) ======= */
// Convert "DEC25" -> the 3rd Friday date "2025-12-19"
function thirdFriday(year, month /* 0-based */) {
  const d = new Date(Date.UTC(year, month, 1));
  // find first Friday
  while (d.getUTCDay() !== 5) d.setUTCDate(d.getUTCDate() + 1);
  // third Friday = first Friday + 14 days
  d.setUTCDate(d.getUTCDate() + 14);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}
const MONTHS = {
  JAN: 0, FEB: 1, MAR: 2, APR: 3, MAY: 4, JUN: 5,
  JUL: 6, AUG: 7, SEP: 8, OCT: 9, NOV: 10, DEC: 11,
};
// function parseMonthToken(tok) {
//   // TOK like "DEC25"
//   const m3 = tok.slice(0, 3).toUpperCase();
//   const yy = Number(tok.slice(3));
//   const month = MONTHS[m3];
//   if (month == null || Number.isNaN(yy)) return null;
//   const year = 2000 + yy;
//   return thirdFriday(year, month);
// }

// cache of expiries per symbol
const EXPIRIES = new Map(); // symbol -> array of yyyy-mm-dd
async function expirationsForSymbol(symbol) {
  const sym = String(symbol || "").toUpperCase();
  if (EXPIRIES.has(sym)) return EXPIRIES.get(sym);

  // secdef/search gives "months" tokens on an OPT section
  const tries = [];
  const urls = [
    `/iserver/secdef/search?symbol=${encodeURIComponent(sym)}&sectype=OPT&month=${encodeURIComponent("202512")}&exchange=SMART&right=C&strike=200`,
    `/iserver/secdef/search?symbol=${encodeURIComponent(sym)}&sectype=OPT&month=${encodeURIComponent("20251219")}&exchange=SMART&right=C&strike=200`,
  ];

  let monthsField = null;
  for (const u of urls) {
    const data = await ibGet(u.replace(IB_BASE, "").startsWith("/")
      ? u.replace(IB_BASE, "")
      : u).catch(() => null);
    tries.push({ url: u, ok: !!data });
    if (data?.sections) {
      const opt = data.sections.find(s => s.secType === "OPT");
      if (opt?.months) { monthsField = opt.months; break; }
    }
  }

  let out = [];
  if (monthsField) {
    // months like "NOV25;DEC25;JAN26;..."
    const toks = String(monthsField).split(";").map(s => s.trim()).filter(Boolean);
    out = toks.map(parseMonthToken).filter(Boolean);
  }

  EXPIRIES.set(sym, out);
  return out;
}

/* ============== STRIKES FOR A GIVEN EXPIRY ============== */
async function strikesForExpiry(symbol, expiry, ulPriceHint) {
  const sym = String(symbol || "").toUpperCase();
  const conid = await conidForStock(sym);
  const params = new URLSearchParams({
    conid: String(conid),
    sectype: "OPT",
    month: expiry.replaceAll("-", "").slice(0, 6), // YYYYMM for strikes API
    exchange: "SMART",
  });
  if (ulPriceHint) params.set("underlyingPrice", String(ulPriceHint));
  const data = await ibGet("/iserver/secdef/strikes", params.toString());
  // IB returns { strikes: [ ...numbers... ] } or array of numbers directly depending on GW version
  if (Array.isArray(data)) return data.filter(n => Number.isFinite(Number(n))).map(Number);
  if (data?.strikes) return data.strikes.filter(n => Number.isFinite(Number(n))).map(Number);
  return [];
}

/* ============== CHAIN BUILDER ============== */
// Returns { symbol, last, chains:[{expiry, strikes:[...]}] }
async function buildChainForSymbol(symbol, limitExp = 3) {
  const sym = String(symbol || "").toUpperCase();
  const conid = await conidForStock(sym);
  const last = await lastPriceForConid(conid);

  const exps = await expirationsForSymbol(sym);          // from months -> 3rd Fridays
  const sel = exps.slice(0, limitExp);

  const chains = [];
  for (const exp of sel) {
    try {
      const strikes = await strikesForExpiry(sym, exp, last || undefined);
      // Keep a reasonable window around underlying (e.g., ±20%)
      const lo = last ? last * 0.6 : undefined;
      const hi = last ? last * 1.4 : undefined;
      const filtered = last
        ? strikes.filter(k => k >= lo && k <= hi).slice(0, 200)
        : strikes.slice(0, 200);
      chains.push({ expiry: exp, strikes: filtered });
    } catch (e) {
      console.warn("strikes fetch failed", sym, exp, e.message || e);
    }
  }

  return { symbol: sym, last, chains };
}

/* ================ ROUTES ================= */

// health / banner
console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}  IB=${IB_BASE}`);

// watchlist
app.get("/watchlist", (_req, res) => res.json(getWatchlist()));

// ----------------- WATCH STATE + HELPERS -----------------
// const WATCH = global.WATCH || { equities: [], options: [] };
global.WATCH = WATCH;

function getWatchlist() {
  const eq = Array.isArray(WATCH.equities)
    ? WATCH.equities
    : WATCH.equities instanceof Set
      ? Array.from(WATCH.equities)
      : typeof WATCH.equities === "object"
        ? Object.keys(WATCH.equities)
        : [];
  const opts = Array.isArray(WATCH.options)
    ? WATCH.options
    : WATCH.options instanceof Set
      ? Array.from(WATCH.options)
      : [];
  return { equities: eq, options: opts };
}

function uniqPush(arr, pred, val) {
  if (!arr.some(pred)) arr.push(val);
}

function normalizeSymbol(s) {
  return String(s || "").trim().toUpperCase();
}

function normalizeDateYYYY_MM_DD(d) {
  if (!d) return "";
  const s = String(d).trim();
  // Accept "YYYY-MM-DD", "YYYY/MM/DD", "YYYYMMDD"
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{8}$/.test(s)) return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
  const m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
  if (m) {
    const y = m[1], mm = m[2].padStart(2,"0"), dd = m[3].padStart(2,"0");
    return `${y}-${mm}-${dd}`;
  }
  return ""; // invalid -> caller will skip
}

function normalizeRight(x) {
  const r = String(x || "").trim().toUpperCase();
  return r.startsWith("C") ? "C" : r.startsWith("P") ? "P" : "";
}

// Accepts any of:
//   { underlying, expiration/exp, strike, right/C/P }
//   OCC string like "AAPL 20251219 C200" or "AAPL 2025-12-19 P150"
function normalizeOption(x) {
  if (!x) return null;

  // OCC-ish string?
  if (typeof x === "string") {
    const s = x.trim().toUpperCase();
    // "AAPL 20251219 C200" or "AAPL 2025-12-19 C200"
    let m = s.match(/^([A-Z.]+)\s+(\d{8})\s+([CP])(\d+(?:\.\d+)?)$/);
    if (!m) m = s.match(/^([A-Z.]+)\s+(\d{4}-\d{2}-\d{2})\s+([CP])(\d+(?:\.\d+)?)$/);
    if (m) {
      const [, ul, d, r, str] = m;
      return {
        underlying: normalizeSymbol(ul),
        expiration: normalizeDateYYYY_MM_DD(d),
        strike: Number(str),
        right: r
      };
    }
    return null;
  }

  // object shape
  const underlying = normalizeSymbol(x.underlying || x.ul || x.symbol);
  const expiration = normalizeDateYYYY_MM_DD(x.expiration || x.exp || x.expiry || x.date);
  const strike = Number(x.strike ?? x.k ?? x.str);
  const right = normalizeRight(x.right || x.cp || x.side);
  if (!underlying || !expiration || !Number.isFinite(strike) || !right) return null;

  return { underlying, expiration, strike, right };
}

async function watchAddEquities(equities) {
  const current = Array.isArray(WATCH.equities) ? WATCH.equities : (WATCH.equities = []);
  let added = 0;

  // support array, set, object map
  const list = Array.isArray(equities) ? equities
            : equities instanceof Set ? Array.from(equities)
            : typeof equities === "object" && equities ? Object.keys(equities)
            : [];

  for (const raw of list) {
    const sym = normalizeSymbol(raw);
    if (!sym) continue;
    if (!current.includes(sym)) {
      current.push(sym);
      added++;
    }
  }
  return added;
}

async function watchAddOptions(options) {
  const current = Array.isArray(WATCH.options) ? WATCH.options : (WATCH.options = []);
  let added = 0;

  // allow array, set, object map of occ->true, etc.
  const list = Array.isArray(options) ? options
            : options instanceof Set ? Array.from(options)
            : typeof options === "object" && options ? Object.entries(options).flatMap(([k, v]) => {
                if (v === true) return [k];              // {"AAPL 20251219 C200": true}
                if (Array.isArray(v)) return v;          // {AAPL:[{...}, "... occ ..."]}
                return [];
              })
            : [];

  for (const raw of list) {
    const opt = normalizeOption(raw);
    if (!opt) continue;
    uniqPush(
      current,
      (o) =>
        o.underlying === opt.underlying &&
        o.expiration === opt.expiration &&
        o.strike === opt.strike &&
        o.right === opt.right,
      opt
    );
    added++;
  }
  return added;
}
app.post("/watch/alpaca", async (req, res) => {
  try {
    const equities = req.body?.equities ?? [];
    const added = await watchAddEquities(equities);
    res.json({ ok: true, watching: getWatchlist(), added });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// add options (idempotent; accepts objects OR OCC strings)
app.post("/watch/tradier", async (req, res) => {
  try {
    const options = req.body?.options ?? [];
    const added = await watchAddOptions(options);
    res.json({ ok: true, watching: getWatchlist(), added });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

// OPTION CHAINS
// If ?symbols= is provided, build chains for those equities;
// otherwise, build for currently watched equities.
// app.get("/api/flow/chains", async (req, res) => {
//   try {
//     await ibEnsureAuthAndAccounts(); // keep session primed

//     const param = String(req.query.symbols || "").trim();
//     const syms = param
//       ? param.split(",").map(s => s.trim().toUpperCase()).filter(Boolean)
//       : Array.from(WATCH.equities || []);

//     if (!syms.length) return res.json([]);

//     const out = [];
//     for (const s of syms) {
//       try {
//         const built = await buildChainForSymbol(s, 3);
//         if (built?.chains?.length) out.push(built);
//       } catch (e) {
//         console.warn("chain build failed for", s, e?.message || e);
//       }
//     }
//     res.json(out);
//   } catch (e) {
//     res.status(500).json({ error: String(e?.message || e) });
//   }
// });

app.get("/api/flow/chains", async (req, res) => {
  try {
    const param = String(req.query.symbols || "").trim();
    const syms = param
      ? param.split(",").map(s => s.trim().toUpperCase()).filter(Boolean)
      : Array.from(WATCH.equities || []); // keep your existing fallback

    const results = [];
    for (const sym of syms) {
      const conid = await ibConidForStock(sym);
      if (!conid) {
        results.push({ symbol: sym, error: "no_conid" });
        continue;
      }
      const expirations = await ibOptMonthsForSymbol(sym);
      // (Optional) build per-expiry strikes here using /iserver/secdef/strikes?conid=...&month=YYYYMM
      results.push({ symbol: sym, conid: Number(conid), expirations });
    }
    res.json(results);
  } catch (e) {
    console.error("chains error:", e);
    res.status(500).json({ error: e.message || "internal error" });
  }
});

/* ===== DEBUG endpoints to help verify IB responses ===== */
app.get("/debug/expirations", async (req, res) => {
  try {
    const symbol = String(req.query.symbol || "").trim().toUpperCase();
    if (!symbol) return res.status(400).json({ error: "symbol required" });

    // get a reliable stock conid
    const conid = await ibConidForStock(symbol);
    if (!conid) return res.status(200).json({ error: `No conid for ${symbol}` });

    // derive expirations from secdef/search OPT months
    const expirations = await ibOptMonthsForSymbol(symbol);
    return res.json({ symbol, conid: Number(conid), expirations });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "internal error" });
  }
});

// app.get("/debug/expirations", async (req, res) => {
//   try {
//     await ibEnsureAuthAndAccounts();
//     const symbol = String(req.query.symbol || req.query.symbols || req.query.sym || req.query.s || "AAPL").toUpperCase();
//     const conid = await conidForStock(symbol);
//     const expirations = await expirationsForSymbol(symbol);
//     res.json({ symbol, conid, expirations });
//   } catch (e) {
//     res.status(500).json({ error: String(e?.message || e) });
//   }
// });

app.get("/debug/secdef-search", async (req, res) => {
  try {
    await ibEnsureAuthAndAccounts();
    const ul = String(req.query.ul || req.query.symbol || "AAPL").toUpperCase();
    const exp = String(req.query.exp || "2025-12-19");
    const right = String(req.query.right || "C").slice(0, 1).toUpperCase();
    const strike = Number(req.query.strike || 200);

    const urls = [
      `/iserver/secdef/search?symbol=${encodeURIComponent(ul)}&sectype=OPT&month=${encodeURIComponent(exp.replaceAll("-", "").slice(0, 6))}&exchange=SMART&right=${right}&strike=${strike}`,
      `/iserver/secdef/search?symbol=${encodeURIComponent(ul)}&sectype=OPT&month=${encodeURIComponent(exp.replaceAll("-", ""))}&exchange=SMART&right=${right}&strike=${strike}`,
    ];

    const tries = [];
    let chosen = null;
    for (const u of urls) {
      const data = await ibGet(u).catch(() => null);
      tries.push({ url: u, status: data ? 200 : 500, count: data ? 1 : 0, sample: data });
      if (data?.sections?.some(s => s.secType === "OPT" && s.months)) {
        chosen = data;
        break;
      }
    }
    res.json({ ul, exp, right, strike, tries, chosen: !!chosen });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/* =============== WS ================== */
wss.on("connection", (socket) => {
  socket.send(JSON.stringify({ type: "hello", now: Date.now(), watchlist: getWatchlist() }));
});

/* ============== START ============== */
server.listen(PORT, () => {
  console.log(`Listening on http://localhost:${PORT}`);
});

