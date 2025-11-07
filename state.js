const FUT_ALIASES = new Map([["/ES","ES"], ["ES","ES"]]);
const rightMap = (x) => {
  const s = String(x||"").trim().toUpperCase();
  if (s==="CALL"||s==="C") return "C";
  if (s==="PUT" ||s==="P") return "P";
  return "";
};
// export const normalizeSymbol = (s) => {
//   const up = String(s || "").trim().toUpperCase();
//   return FUT_ALIASES.get(up) || up;
// };
function normalizeSymbol(sym = "") {
  const s = String(sym).trim().toUpperCase();
  // Indexes, ETFs: leave as is (SPX, SPY, QQQ, AAPL, NVDA ...)
  // Futures: force leading slash for root/front-month aliases
  if (s === "ES" || s === "/ES") return "/ES";
  if (s === "NQ" || s === "/NQ") return "/NQ";
  if (s === "YM" || s === "/YM") return "/YM";
  return s;
}

export const WATCH = {
  equities: new Set(),   // single source of truth
  options: [],           // { underlying, expiration, strike, right: "C"|"P" }
};

export const getWatchlist = () => ({
  equities: Array.from(WATCH.equities),
  options: WATCH.options.map(o => ({ ...o })),
});

export function addEquity(sym){
  const s = normalizeSymbol(sym);
  if (!s) return false;
  const sizeBefore = WATCH.equities.size;
  WATCH.equities.add(s);
  return WATCH.equities.size > sizeBefore;
}
export function delEquity(sym){
  WATCH.equities.delete(normalizeSymbol(sym));
}
export function addOptions(arr=[]){
  let added = 0;
  for (const raw of arr){
    const o = {
      underlying: normalizeSymbol(raw?.underlying),
      expiration: String(raw?.expiration || ""),
      strike: Number(raw?.strike),
      right: rightMap(raw?.right),
    };
    if (o.underlying && o.expiration && Number.isFinite(o.strike) && (o.right==="C"||o.right==="P")){
      WATCH.options.push(o); added++;
    }
  }
  return added;
}

// WS broadcaster hook
let _broadcast = null;
export function setBroadcaster(fn){ _broadcast = typeof fn === "function" ? fn : null; }
export function broadcastWatchlist(){ if (_broadcast) _broadcast("watchlist", getWatchlist()); }
