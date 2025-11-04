import { differenceInMilliseconds } from "date-fns";


const DEF = {
NOTIONAL_MIN: 100_000,
QTY_MIN: 250,
DTE_MAX: 14,
NEAR_MONEY: 0.02,
BURST_MS: 3000,
SYMBOL_CAP: 5,
};


export function buildNotables({ sweeps, blocks }, cfg = {}) {
const C = { ...DEF, ...cfg };
const now = Date.now();


const byUL = (arr, kind) => {
const map = new Map();
for (const x of arr) {
const notional = x.notional || (x.price * (x.qty ?? 0) * 100);
if (notional < C.NOTIONAL_MIN) continue;
if ((x.qty ?? 0) < C.QTY_MIN) continue;
const dte = Math.max(0, (new Date(x.expiry) - now) / 86400000);
if (dte > C.DTE_MAX) continue;
const moneyness = x.underlying ? (x.strike / x.underlying) : 1;
if (Math.abs(moneyness - 1) > C.NEAR_MONEY) continue;


// Burst detection: count prints for same UL within BURST_MS
const k = x.ul;
const e = map.get(k) || { ul: k, prints: [], side$: 0, notional$: 0, kind };
e.prints.push(x);
e.notional$ += notional;
e.side$ += (x.side === 'BUY' ? 1 : -1) * notional;
map.set(k, e);
}
// score & compact
const list = [...map.values()].map(e => {
const prints = e.prints.sort((a,b)=>+new Date(a.ts)-+new Date(b.ts));
let burst = 0, best = 0;
for (let i=1;i<prints.length;i++) {
const dt = differenceInMilliseconds(new Date(prints[i].ts), new Date(prints[i-1].ts));
burst = dt <= C.BURST_MS ? burst+1 : 0; best = Math.max(best, burst);
}
const burstFactor = Math.min(1, best/5);
const qty$ = prints.reduce((s,p)=>s+(p.qty||0),0);
const near$ = prints.reduce((s,p)=>s + (1 - Math.min(1, Math.abs((p.strike/(p.underlying||p.ref||1))-1))), 0) / Math.max(1, prints.length);
const dteAvg = prints.reduce((s,p)=>s + Math.max(0,(new Date(p.expiry)-now)/86400000),0)/Math.max(1,prints.length);
const score = Math.log1p(e.notional$) * 1.0 + Math.sqrt(qty$) * 0.6 + burstFactor * 1.0 + near$ * 0.6 + (1/(1+dteAvg))*0.6;
const side = e.side$ >= 0 ? 'BUY' : 'SELL';
return { ul: e.ul, kind: e.kind, score, side, notional$: Math.round(e.notional$), qty$, burst: best, dteAvg };
});
return list.sort((a,b)=>b.score-a.score);
};


const topSweeps = byUL(sweeps, 'sweeps').slice(0, DEF.SYMBOL_CAP);
const topBlocks = byUL(blocks, 'blocks').slice(0, DEF.SYMBOL_CAP);


// Build headline strings like: "SPY CALL BUY $3.2M (7 prints, 0-7DTE)"
const toHeadline = (e) => `${e.ul} ${e.side} $${(e.notional$/1e6).toFixed(1)}M • ${e.qty$|0}x • burst ${e.burst}`;


}
