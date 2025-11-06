import { differenceInMilliseconds } from "date-fns";


const DEF = {
NOTIONAL_MIN: 100_000,
QTY_MIN: 250,
DTE_MAX: 14,
NEAR_MONEY: 0.02,
BURST_MS: 3000,
SYMBOL_CAP: 5,
};


// export function buildNotables({ sweeps, blocks }, cfg = {}) {
// const C = { ...DEF, ...cfg };
// const now = Date.now();


// const byUL = (arr, kind) => {
// const map = new Map();
// for (const x of arr) {
// const notional = x.notional || (x.price * (x.qty ?? 0) * 100);
// if (notional < C.NOTIONAL_MIN) continue;
// if ((x.qty ?? 0) < C.QTY_MIN) continue;
// const dte = Math.max(0, (new Date(x.expiry) - now) / 86400000);
// if (dte > C.DTE_MAX) continue;
// const moneyness = x.underlying ? (x.strike / x.underlying) : 1;
// if (Math.abs(moneyness - 1) > C.NEAR_MONEY) continue;


// // Burst detection: count prints for same UL within BURST_MS
// const k = x.ul;
// const e = map.get(k) || { ul: k, prints: [], side$: 0, notional$: 0, kind };
// e.prints.push(x);
// e.notional$ += notional;
// e.side$ += (x.side === 'BUY' ? 1 : -1) * notional;
// map.set(k, e);
// }
// // score & compact
// const list = [...map.values()].map(e => {
// const prints = e.prints.sort((a,b)=>+new Date(a.ts)-+new Date(b.ts));
// let burst = 0, best = 0;
// for (let i=1;i<prints.length;i++) {
// const dt = differenceInMilliseconds(new Date(prints[i].ts), new Date(prints[i-1].ts));
// burst = dt <= C.BURST_MS ? burst+1 : 0; best = Math.max(best, burst);
// }
// const burstFactor = Math.min(1, best/5);
// const qty$ = prints.reduce((s,p)=>s+(p.qty||0),0);
// const near$ = prints.reduce((s,p)=>s + (1 - Math.min(1, Math.abs((p.strike/(p.underlying||p.ref||1))-1))), 0) / Math.max(1, prints.length);
// const dteAvg = prints.reduce((s,p)=>s + Math.max(0,(new Date(p.expiry)-now)/86400000),0)/Math.max(1,prints.length);
// const score = Math.log1p(e.notional$) * 1.0 + Math.sqrt(qty$) * 0.6 + burstFactor * 1.0 + near$ * 0.6 + (1/(1+dteAvg))*0.6;
// const side = e.side$ >= 0 ? 'BUY' : 'SELL';
// return { ul: e.ul, kind: e.kind, score, side, notional$: Math.round(e.notional$), qty$, burst: best, dteAvg };
// });
// return list.sort((a,b)=>b.score-a.score);
// };


// const topSweeps = byUL(sweeps, 'sweeps').slice(0, DEF.SYMBOL_CAP);
// const topBlocks = byUL(blocks, 'blocks').slice(0, DEF.SYMBOL_CAP);


// // Build headline strings like: "SPY CALL BUY $3.2M (7 prints, 0-7DTE)"
// const toHeadline = (e) => `${e.ul} ${e.side} $${(e.notional$/1e6).toFixed(1)}M • ${e.qty$|0}x • burst ${e.burst}`;


// }

// /**
//  * Build notable flow per underlying.
//  * @param {{sweeps?: any[], blocks?: any[], prints?: any[]}} param0
//  * @param {Partial<typeof DEF>} cfg
//  * @returns {{ sweeps: any[], blocks: any[], all: any[] }}
//  */
// export function buildNotables(
//   { sweeps = [], blocks = [], prints = [] } = {},
//   cfg = {}
// ) {
//   const C = { ...DEF, ...cfg };
//   const now = Date.now();

//   // Optional: preindex full "prints" firehose by UL for better burst detection
//   const burstIndex = new Map();
//   for (const p of prints) {
//     const k = p && p.ul;
//     if (!k) continue;
//     const arr = burstIndex.get(k) || [];
//     arr.push(p);
//     burstIndex.set(k, arr);
//   }

//   const byUL = (arr, kind) => {
//     const map = new Map();

//     for (const x of arr || []) {
//       const qty = Number(x?.qty ?? 0);
//       const price = Number(x?.price ?? 0);
//       const notional = Number(x?.notional ?? price * qty * 100);
//       if (!Number.isFinite(notional) || notional < C.NOTIONAL_MIN) continue;
//       if (!Number.isFinite(qty) || qty < C.QTY_MIN) continue;

//       const exMs = new Date(x?.expiry).getTime();
//       if (!Number.isFinite(exMs)) continue;
//       const dte = Math.max(0, (exMs - now) / 86_400_000);
//       if (dte > C.DTE_MAX) continue;

//       const under = Number(x?.underlying ?? x?.ref ?? NaN);
//       const strike = Number(x?.strike ?? NaN);
//       const m = (Number.isFinite(under) && under > 0 && Number.isFinite(strike)) ? (strike / under) : 1;
//       if (!Number.isFinite(m) || Math.abs(m - 1) > C.NEAR_MONEY) continue;

//       const k = x?.ul;
//       if (!k) continue;

//       const prev = map.get(k) || { ul: k, prints: [], side$: 0, notional$: 0, kind };
//       prev.prints.push(x);
//       prev.notional$ += notional;

//       const side = String(x?.side || "").toUpperCase();
//       if (side === "BUY") prev.side$ += notional;
//       else if (side === "SELL") prev.side$ -= notional;

//       map.set(k, prev);
//     }

//     const list = [...map.values()].map((e) => {
//       // Use global prints for burst if available, else fall back to local prints
//       const burstPrints = (burstIndex.get(e.ul) || e.prints).slice().sort(
//         (a, b) => new Date(a.ts).getTime() - new Date(b.ts).getTime()
//       );

//       let burstRun = 0, best = 0;
//       for (let i = 1; i < burstPrints.length; i++) {
//         const t1 = new Date(burstPrints[i].ts).getTime();
//         const t0 = new Date(burstPrints[i - 1].ts).getTime();
//         const dt = Math.abs(t1 - t0);
//         burstRun = dt <= C.BURST_MS ? burstRun + 1 : 0;
//         if (burstRun > best) best = burstRun;
//       }
//       const burstFactor = Math.min(1, best / 5);

//       const qty$ = e.prints.reduce((s, p) => s + Number(p?.qty ?? 0), 0);
//       const near$ =
//         e.prints.reduce((s, p) => {
//           const u = Number(p?.underlying ?? p?.ref ?? NaN);
//           const k = Number(p?.strike ?? NaN);
//           if (!Number.isFinite(u) || u <= 0 || !Number.isFinite(k)) return s;
//           const mm = k / u;
//           return s + (1 - Math.min(1, Math.abs(mm - 1)));
//         }, 0) / Math.max(1, e.prints.length);

//       const dteAvg =
//         e.prints.reduce((s, p) => {
//           const ex = new Date(p?.expiry).getTime();
//           return s + (Number.isFinite(ex) ? Math.max(0, (ex - now) / 86_400_000) : 0);
//         }, 0) / Math.max(1, e.prints.length);

//       const score =
//         Math.log1p(e.notional$) * 1.0 +
//         Math.sqrt(Math.max(0, qty$)) * 0.6 +
//         burstFactor * 1.0 +
//         near$ * 0.6 +
//         (1 / (1 + dteAvg)) * 0.6;

//       const side = e.side$ >= 0 ? "BUY" : "SELL";
//       return {
//         ul: e.ul,
//         kind: e.kind,
//         score,
//         side,
//         notional$: Math.round(e.notional$),
//         qty$: Math.round(qty$),
//         burst: best,
//         dteAvg,
//       };
//     });

//     return list.sort((a, b) => b.score - a.score);
//   };

//   const topSweeps = byUL(sweeps, "sweeps").slice(0, C.SYMBOL_CAP);
//   const topBlocks = byUL(blocks, "blocks").slice(0, C.SYMBOL_CAP);

//   const toHeadline = (e) =>
//     `${e.ul} ${e.side} $${(e.notional$ / 1e6).toFixed(1)}M • ${e.qty$}x • burst ${e.burst}`;

//   const sweepsWithHeadlines = topSweeps.map((e) => ({ ...e, headline: toHeadline(e) }));
//   const blocksWithHeadlines = topBlocks.map((e) => ({ ...e, headline: toHeadline(e) }));

//   return {
//     sweeps: sweepsWithHeadlines,
//     blocks: blocksWithHeadlines,
//     all: [...sweepsWithHeadlines, ...blocksWithHeadlines].sort((a, b) => b.score - a.score),
//     notables: [...sweepsWithHeadlines, ...blocksWithHeadlines].sort((a, b) => b.score - a.score),
//   };
// }
export function buildNotables({ sweeps = [], blocks = [], prints = [] } = {}, cfg = {}) {
  const DEF = {
    NOTIONAL_MIN: 100_000,
    QTY_MIN: 200,
    DTE_MAX: 60,
    NEAR_MONEY: 0.15,
    SYMBOL_CAP: 10,
    BURST_MS: 2500,
  };
  const C = { ...DEF, ...cfg };
  const now = Date.now();

  const burstIndex = new Map();
  for (const p of prints) {
    const k = p?.ul;
    if (!k) continue;
    (burstIndex.get(k) ?? burstIndex.set(k, []).get(k)).push(p);
  }

  const byUL = (arr, kind) => {
    const map = new Map();
    for (const x of arr || []) {
      const qty = Number(x?.qty ?? 0);
      const price = Number(x?.price ?? 0);
      const notional = Number(x?.notional ?? price * qty * 100);
      if (!Number.isFinite(notional) || notional < C.NOTIONAL_MIN) continue;
      if (!Number.isFinite(qty) || qty < C.QTY_MIN) continue;

      const exMs = new Date(String(x?.expiry).includes('T') ? x.expiry : `${x.expiry}T00:00:00Z`).getTime();
      if (!Number.isFinite(exMs)) continue;
      const dte = Math.max(0, (exMs - now) / 86_400_000);
      if (dte > C.DTE_MAX) continue;

      const under = Number(x?.underlying ?? x?.ref ?? x?.ul_px ?? NaN);
      const strike = Number(x?.strike ?? NaN);
      const m = (Number.isFinite(under) && under > 0 && Number.isFinite(strike)) ? (strike / under) : 1;
      if (!Number.isFinite(m) || Math.abs(m - 1) > C.NEAR_MONEY) continue;

      const k = x?.ul;
      if (!k) continue;

      const prev = map.get(k) || { ul: k, prints: [], side$: 0, notional$: 0, kind };
      prev.prints.push(x);
      prev.notional$ += notional;

      const side = String(x?.side || "").toUpperCase();
      if (side === "BUY") prev.side$ += notional;
      else if (side === "SELL") prev.side$ -= notional;

      map.set(k, prev);
    }

    return [...map.values()].map((e) => {
      const burstPrints = (burstIndex.get(e.ul) || e.prints).slice().sort(
        (a, b) => new Date(a.ts).getTime() - new Date(b.ts).getTime()
      );

      let burstRun = 0, best = 0;
      for (let i = 1; i < burstPrints.length; i++) {
        const t1 = +new Date(burstPrints[i].ts);
        const t0 = +new Date(burstPrints[i - 1].ts);
        const dt = Math.abs(t1 - t0);
        burstRun = dt <= C.BURST_MS ? burstRun + 1 : 0;
        if (burstRun > best) best = burstRun;
      }
      const burstFactor = Math.min(1, best / 5);

      const qty$ = e.prints.reduce((s, p) => s + Number(p?.qty ?? 0), 0);
      const near$ =
        e.prints.reduce((s, p) => {
          const u = Number(p?.underlying ?? p?.ref ?? p?.ul_px ?? NaN);
          const k = Number(p?.strike ?? NaN);
          if (!Number.isFinite(u) || u <= 0 || !Number.isFinite(k)) return s;
          const mm = k / u;
          return s + (1 - Math.min(1, Math.abs(mm - 1)));
        }, 0) / Math.max(1, e.prints.length);

      const dteAvg =
        e.prints.reduce((s, p) => {
          const ex = new Date(String(p?.expiry).includes('T') ? p.expiry : `${p.expiry}T00:00:00Z`).getTime();
          return s + (Number.isFinite(ex) ? Math.max(0, (ex - now) / 86_400_000) : 0);
        }, 0) / Math.max(1, e.prints.length);

      const score =
        Math.log1p(e.notional$) * 1.0 +
        Math.sqrt(Math.max(0, qty$)) * 0.6 +
        burstFactor * 1.0 +
        near$ * 0.6 +
        (1 / (1 + dteAvg)) * 0.6;

      const side = e.side$ >= 0 ? "BUY" : "SELL";
      return {
        ul: e.ul, kind: e.kind, side,
        score, notional$: Math.round(e.notional$),
        qty$: Math.round(qty$), burst: best, dteAvg,
      };
    }).sort((a, b) => b.score - a.score);
  };

  const topSweeps = byUL(sweeps, "sweeps").slice(0, C.SYMBOL_CAP);
  const topBlocks = byUL(blocks, "blocks").slice(0, C.SYMBOL_CAP);
  const toHeadline = (e) => `${e.ul} ${e.side} $${(e.notional$ / 1e6).toFixed(1)}M • ${e.qty$}x • burst ${e.burst}`;

  const sweepsWithHeadlines = topSweeps.map((e) => ({ ...e, headline: toHeadline(e) }));
  const blocksWithHeadlines = topBlocks.map((e) => ({ ...e, headline: toHeadline(e) }));

  return {
    sweeps: sweepsWithHeadlines,
    blocks: blocksWithHeadlines,
    all: [...sweepsWithHeadlines, ...blocksWithHeadlines].sort((a, b) => b.score - a.score),
    notables: [...sweepsWithHeadlines, ...blocksWithHeadlines].sort((a, b) => b.score - a.score),
  };
}