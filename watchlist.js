import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// Path to your source-of-truth file
const FILE = process.env.WATCHLIST_FILE || path.join(process.cwd(), "watchlist.json");

// Per-symbol strike steps (tweak as needed)
const STRIKE_STEP = {
  SPY: 1, QQQ: 1, IWM: 1, DIA: 1,
  AAPL: 2.5, MSFT: 2.5, NVDA: 5, AMZN: 2.5, META: 2.5, GOOGL: 2.5,
  TSLA: 2.5, AVGO: 5, AMD: 2.5, NFLX: 5,
  SPX: 5, ES: 5
};
const DEFAULT_STEP = 2.5;
const STRIKES_AROUND_ATM = Number(process.env.STRIKES_AROUND_ATM || 30);

const norm = (s) => (s || "").toUpperCase().replace(/^\//, "");
const stepFor = (sym) => STRIKE_STEP[norm(sym)] ?? DEFAULT_STEP;
const roundToStep = (x, step) => Math.round(x / step) * step;

export function loadWatchlistFile() {
  try {
    const raw = fs.readFileSync(FILE, "utf8");
    const json = JSON.parse(raw);
    const equities = Array.isArray(json?.equities) ? json.equities.map(String) : [];
    return { equities };
  } catch {
    return { equities: [] };
  }
}

export function saveWatchlistFile(wf) {
  fs.writeFileSync(FILE, JSON.stringify({ equities: wf.equities }, null, 2));
}

// ---------- symbol helpers ----------
function isFutureSymbol(sym) {
  if (!sym) return false;
  const s = String(sym).toUpperCase();
  if (s.startsWith("/")) return true; // e.g. /ES, /NQ
  const FUTS = new Set(["ES","NQ","RTY","YM","CL","GC","SI","ZN","ZB","ZF","ZT","HG","NG"]);
  return FUTS.has(s);
}

// export function buildOptionsAroundATM(sym, getLast, around = STRIKES_AROUND_ATM) {
//   const s = norm(sym);
//   const last = getLast(s);
//   if (!Number.isFinite(last)) return [];

//   const step = stepFor(s);
//   const atm  = roundToStep(last, step);

//   const expiries = pickNearExpirations(); // simple: next 2 Fridays
//   const out = [];
//   for (const expiration of expiries) {
//     for (let i = -around; i <= around; i++) {
//       const strike = +(atm + i * step).toFixed(2);
//       out.push({ underlying: s, expiration, strike, right: "C" });
//       out.push({ underlying: s, expiration, strike, right: "P" });
//     }
//   }
//   return out;
// }

function pickNearExpirations() {
  const out = [];
  const today = new Date();
  let found = 0;
  for (let i = 0; i < 30 && found < 2; i++) {
    const d = new Date(today.getTime() + i * 86400000);
    if (d.getDay() === 5) {
      out.push(d.toISOString().slice(0, 10));
      found++;
    }
  }
  return out;
}
