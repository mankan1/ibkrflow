// actions.js
// Stateless helpers + tiny state for OI/volume tracking to infer BTO/BTC/STO/STC.

// export const oiByOcc  = new Map();  // OCC -> open_interest
// export const volByOcc = new Map();  // OCC -> last seen cumulative daily volume
// export const asOfByOcc = new Map(); // OCC -> yyyymmdd string when we last updated vol (daily reset)

// // ===== book/side helpers (you already have similar; lifted and made portable) =====
// export function epsFor({ bid, ask, mid }) {
//   if (Number.isFinite(bid) && Number.isFinite(ask)) return Math.max((ask - bid) * 0.15, 0.01);
//   if (Number.isFinite(mid)) return Math.max(0.001 * mid, 0.01);
//   return 0.01;
// }

// export function whereAt(price, book = {}) {
//   const eps = epsFor(book);
//   if (Number.isFinite(book.ask) && price >= (book.ask - eps)) return "ask";
//   if (Number.isFinite(book.bid) && price <= (book.bid + eps)) return "bid";
//   if (Number.isFinite(book.mid) && Math.abs(price - book.mid) <= eps) return "mid";
//   return "between";
// }

// export function aggressorFrom(side, at) {
//   if (side === "BOT" || at === "ask") return "buy";
//   if (side === "SLD" || at === "bid") return "sell";
//   return "unknown";
// }

// // ===== core classifier (same spirit as your sample, tightened a bit) =====
// // Heuristic thresholds
// const TH_CLOSE_BY_DVOL = 0.8;     // ≥80% of OI already traded today → bias towards close
// const TH_BIG_TRADE_OI  = 1.05;    // qty > 105% of remaining OI → must be OPEN

// export function classifyOpenClose({ qty, oi, priorVol, side, at }) {
//   const q = Number(qty) || 0;
//   const OI = Number(oi)  || 0;
//   const PV = Number(priorVol) || 0;
//   const aggr = aggressorFrom(side, at);

//   // If the print itself exceeds today's remaining OI ⇒ forced OPEN on aggressor
//   // remaining OI ~= max(OI - PV, 0)
//   const rem = Math.max(OI - PV, 0);
//   if (rem > 0 && q >= rem * TH_BIG_TRADE_OI) {
//     if (aggr === "buy")  return { action: "BTO", action_conf: "high", reason: "qty>remaining_OI" };
//     if (aggr === "sell") return { action: "STO", action_conf: "high", reason: "qty>remaining_OI" };
//   }

//   // If today's traded volume is already a large share of OI ⇒ bias to CLOSE
//   const dVolShare = OI > 0 ? (PV / OI) : 0;
//   if (dVolShare >= TH_CLOSE_BY_DVOL) {
//     if (aggr === "buy")  return { action: "BTC", action_conf: "medium", reason: "dayVol>=80%OI" };
//     if (aggr === "sell") return { action: "STC", action_conf: "medium", reason: "dayVol>=80%OI" };
//     return { action: "CLOSE?", action_conf: "low", reason: "dayVolHigh" };
//   }

//   // Otherwise leave as undecided unless aggressor gives a soft OPEN bias
//   if (aggr === "buy")  return { action: "BTO?", action_conf: "low", reason: "aggrBuy" };
//   if (aggr === "sell") return { action: "STO?", action_conf: "low", reason: "aggrSell" };
//   return { action: "—", action_conf: "low", reason: "unknownAggressor" };
// }

// // ===== rolling OI/volume state maintenance =====
// export function updateOI(occ, oi) {
//   if (Number.isFinite(+oi)) oiByOcc.set(occ, Number(oi));
// }

// export function updateDayVolume(occ, seenCumulativeVol, tradeQty, asOfYMD) {
//   // Reset per day boundary
//   const lastDay = asOfByOcc.get(occ);
//   if (asOfYMD && lastDay && lastDay !== asOfYMD) {
//     volByOcc.delete(occ);
//   }
//   if (asOfYMD) asOfByOcc.set(occ, asOfYMD);

//   // priorVol logic: if an exchange gives cumulative, prior = max(seen - qty, 0)
//   let priorVol = volByOcc.has(occ) ? volByOcc.get(occ) : 0;
//   if (Number.isFinite(+seenCumulativeVol)) {
//     priorVol = Math.max(0, Number(seenCumulativeVol) - (Number(tradeQty) || 0));
//     volByOcc.set(occ, Number(seenCumulativeVol));
//   } else {
//     volByOcc.set(occ, (volByOcc.get(occ) || 0) + (Number(tradeQty) || 0));
//   }
//   return priorVol;
// }

// // Convenience: one-shot inference for a single option trade/print
// export function inferActionForOptionTrade({
//   occ, qty, price, side, book, cumulativeVol, oi, asOfYMD
// }) {
//   const at = whereAt(price, book || {});
//   if (Number.isFinite(+oi)) updateOI(occ, +oi);
//   const priorVol = updateDayVolume(occ, cumulativeVol, qty, asOfYMD);
//   const { action, action_conf, reason } = classifyOpenClose({
//     qty, oi: (Number.isFinite(+oi) ? +oi : (oiByOcc.get(occ) || 0)),
//     priorVol, side, at
//   });
//   return { at, priorVol, oi: oiByOcc.get(occ) || null, action, action_conf, reason };
// }

// // Useful when you want to clear memory (e.g., at midnight ET)
// export function resetActionState() {
//   oiByOcc.clear(); volByOcc.clear(); asOfByOcc.clear();
// }
// actions.js
// Stateless helpers + tiny rolling state for OI/volume + short OI history.
// Inspired by "use volume vs OI (today) AND recent OI increases" heuristic.
// Stateless helpers + tiny rolling state for OI/volume + short OI history.
// Produces BTO/BTC/STO/STC/CLOSE? with a confidence and reason.

// export const oiByOcc      = new Map();   // occ -> latest OI snapshot (intraday)
// export const oiOpenByOcc  = new Map();   // occ -> OI at day start (first seen for asOfYMD)
// export const volByOcc     = new Map();   // occ -> cumulative day volume (our running tally)
// export const asOfByOcc    = new Map();   // occ -> yyyymmdd of the running vol
// export const oiHistByOcc  = new Map();   // occ -> [{ ymd, oi } ... up to N]

// // ===== configurable thresholds =====
// const TH_CLOSE_BY_DVOL   = 0.80; // ≥80% of *opening* OI already traded → close bias
// const TH_BIG_TRADE_OI    = 1.05; // qty > 105% of remaining opening OI → must OPEN
// const LOOKBACK_OI_DAYS   = 15;   // recent OI trend window
// const TH_TREND_UP_FACTOR = 1.10; // last OI vs first OI in window must be +10%
// const MIN_OPENING_OI     = 20;   // ignore micro OI when biasing

// ===== in-memory state =====
export const oiByOcc      = new Map();   // occ -> latest OI snapshot (intraday)
export const oiOpenByOcc  = new Map();   // occ -> OI at day start (first seen for asOfYMD)
export const volByOcc     = new Map();   // occ -> cumulative day volume (our running tally)
export const asOfByOcc    = new Map();   // occ -> yyyymmdd of the running vol
export const oiHistByOcc  = new Map();   // occ -> [{ ymd, oi } ... up to N]

// ===== configurable thresholds =====
const TH_CLOSE_BY_DVOL   = 0.80; // ≥80% of *opening* OI already traded → close bias
const TH_BIG_TRADE_OI    = 1.05; // qty > 105% of remaining opening OI → must OPEN
const LOOKBACK_OI_DAYS   = 15;    // recent OI trend window
const TH_TREND_UP_FACTOR = 1.10; // last OI vs first OI in window must be +10%
const MIN_OPENING_OI     = 20;   // ignore micro OI when biasing

// ===== book/side helpers =====
export function epsFor({ bid, ask, mid }) {
  if (Number.isFinite(bid) && Number.isFinite(ask)) return Math.max((ask - bid) * 0.15, 0.01);
  if (Number.isFinite(mid)) return Math.max(0.001 * mid, 0.01);
  return 0.01;
}

export function whereAt(price, book = {}) {
  const eps = epsFor(book);
  if (Number.isFinite(book.ask) && price >= (book.ask - eps)) return "ask";
  if (Number.isFinite(book.bid) && price <= (book.bid + eps)) return "bid";
  if (Number.isFinite(book.mid) && Math.abs(price - book.mid) <= eps) return "mid";
  return "between";
}

// export function aggressorFrom(side, at) {
//   if (side === "BOT" || side === "BUY" || at === "ask") return "buy";
//   if (side === "SLD" || side === "SELL" || at === "bid") return "sell";
//   return "unknown";
// }

export function aggressorFrom(side, at) {
  const s = String(side || "").toUpperCase();
  if (s === "BOT" || s === "BUY" || at === "ask") return "buy";
  if (s === "SLD" || s === "SELL" || at === "bid") return "sell";
  return "unknown";
}

// ===== OI history maintenance (prior expirations / prior sessions) =====
export function updateOIHistory(occ, oi, asOfYMD) {
  if (!Number.isFinite(+oi) || !asOfYMD) return;
  const hist = oiHistByOcc.get(occ) || [];
  const last = hist[hist.length - 1];
  if (!last || last.ymd !== asOfYMD) {
    hist.push({ ymd: asOfYMD, oi: Number(oi) });
    // keep small tail
    while (hist.length > 7) hist.shift();
    oiHistByOcc.set(occ, hist);
  } else {
    // same day update -> keep max (brokers sometimes trickle)
    last.oi = Math.max(last.oi, Number(oi));
  }
}

// function recentOITrendUp(occ) {
//   const hist = oiHistByOcc.get(occ);
//   if (!hist || hist.length < 2) return false;
//   // take last LOOKBACK_OI_DAYS + today (if present)
//   const tail = hist.slice(-Math.min(LOOKBACK_OI_DAYS + 1, hist.length));
//   if (tail.length < 2) return false;
//   const first = tail[0].oi;
//   const last  = tail[tail.length - 1].oi;
//   return Number.isFinite(first) && Number.isFinite(last) && last >= first * TH_TREND_UP_FACTOR;
// }

function recentOITrendUp(occ) {
  const hist = oiHistByOcc.get(occ);
  if (!hist || hist.length < 2) return false;
  const tail = hist.slice(-(LOOKBACK_OI_DAYS + 1));
  if (tail.length < 2) return false;
  const first = tail[0].oi;
  const last  = tail[tail.length - 1].oi;
  return Number.isFinite(first) && Number.isFinite(last) && last >= first * TH_TREND_UP_FACTOR;
}

// ===== rolling OI/volume per-day =====
export function updateOI(occ, oi, asOfYMD) {
  if (!Number.isFinite(+oi)) return;
  const n = Number(oi);
  oiByOcc.set(occ, n);
  // Seed "opening OI" for the day (first OI we see for this asOfYMD)
  const prevDay = asOfByOcc.get(occ);
  if (asOfYMD && prevDay !== asOfYMD) {
    // day rolled → reset daily volume, seed opening OI
    volByOcc.delete(occ);
    oiOpenByOcc.set(occ, n);
    asOfByOcc.set(occ, asOfYMD);
  } else if (!oiOpenByOcc.has(occ) && asOfYMD) {
    oiOpenByOcc.set(occ, n);
    asOfByOcc.set(occ, asOfYMD);
  }
  updateOIHistory(occ, n, asOfYMD);
}

export function updateDayVolume(occ, seenCumulativeVol, tradeQty, asOfYMD) {
  const lastDay = asOfByOcc.get(occ);
  if (asOfYMD && lastDay && lastDay !== asOfYMD) {
    volByOcc.delete(occ);
    asOfByOcc.set(occ, asOfYMD);
  } else if (asOfYMD && !lastDay) {
    asOfByOcc.set(occ, asOfYMD);
  }

  let priorVol = volByOcc.has(occ) ? volByOcc.get(occ) : 0;
  if (Number.isFinite(+seenCumulativeVol)) {
    priorVol = Math.max(0, Number(seenCumulativeVol) - (Number(tradeQty) || 0));
    volByOcc.set(occ, Number(seenCumulativeVol));
  } else {
    volByOcc.set(occ, (volByOcc.get(occ) || 0) + (Number(tradeQty) || 0));
  }
  return priorVol;
}

// ===== core classifier (adds prior-trend bias) =====
export function classifyOpenClose({ qty, at, side, occ }) {
  const q      = Number(qty) || 0;
  const aggr   = aggressorFrom(side, at);
  const OI0    = oiOpenByOcc.get(occ) || oiByOcc.get(occ) || 0;      // opening OI for the session
  const dVol   = volByOcc.get(occ) || 0;                             // day volume BEFORE this trade
  const rem    = Math.max(OI0 - dVol, 0);                            // remaining capacity vs opening OI
  const trendUp = recentOITrendUp(occ);                               // prior sessions OI rising?

  // Hard OPEN rule: this print exceeds remaining opening OI
  if (rem > 0 && q >= rem * TH_BIG_TRADE_OI) {
    if (aggr === "buy")  return { action: "BTO", action_conf: "high", reason: "qty>remaining_OI" };
    if (aggr === "sell") return { action: "STO", action_conf: "high", reason: "qty>remaining_OI" };
  }

  // Close bias #1: Most of opening OI already traded today
  const openOK = OI0 >= MIN_OPENING_OI;
  const dShare = openOK && OI0 > 0 ? (dVol / OI0) : 0;
  if (openOK && dShare >= TH_CLOSE_BY_DVOL) {
    if (aggr === "buy")  return { action: "BTC", action_conf: "medium", reason: "dayVol>=80%openingOI" };
    if (aggr === "sell") return { action: "STC", action_conf: "medium", reason: "dayVol>=80%openingOI" };
    return { action: "CLOSE?", action_conf: "low", reason: "dayVolHigh" };
  }

  // Close bias #2 (video-inspired): prior sessions OI trend ↑ and now notable volume
  // If OI has been building up recently and a big print hits, treat as *likely close*.
  const notable = q >= Math.max(50, OI0 * 0.10); // configurable: “notable” trade
  if (trendUp && notable) {
    if (aggr === "buy")  return { action: "BTC", action_conf: "low", reason: "trendUp+notable BUY" };
    if (aggr === "sell") return { action: "STC", action_conf: "low", reason: "trendUp+notable SELL" };
  }

  // Soft OPEN bias from aggressor + book location
  if (aggr === "buy")  return { action: "BTO?", action_conf: "low", reason: "aggrBuy" };
  if (aggr === "sell") return { action: "STO?", action_conf: "low", reason: "aggrSell" };
  return { action: "—", action_conf: "low", reason: "unknownAggressor" };
}

// Convenience: one-shot inference for a single option print
export function inferActionForOptionTrade({
  occ, qty, price, side, book, cumulativeVol, oi, asOfYMD
}) {
  const at = whereAt(price, book || {});
  if (Number.isFinite(+oi)) updateOI(occ, +oi, asOfYMD);
  const priorVol = updateDayVolume(occ, cumulativeVol, qty, asOfYMD);

  // Ensure we have an opening OI snapshot this day even if broker never sent OI explicitly
  if (!oiOpenByOcc.has(occ) && Number.isFinite(+oi)) {
    oiOpenByOcc.set(occ, Number(oi));
  }

  const { action, action_conf, reason } = classifyOpenClose({
    qty, at, side, occ
  });

  return {
    at,
    priorVol,
    oi: oiByOcc.get(occ) ?? null,
    action,
    action_conf,
    reason
  };
}

export function resetActionState() {
  oiByOcc.clear(); volByOcc.clear(); asOfByOcc.clear();
  oiOpenByOcc.clear(); oiHistByOcc.clear();
}
