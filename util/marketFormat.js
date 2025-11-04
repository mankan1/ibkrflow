export function normalizeSym(s) {
  return (s || "").toString().trim().toUpperCase().replace(/^\//, "");
}

export function kindFor(symbol) {
  const s = normalizeSym(symbol);
  if (s === "SPX") return "index";
  if (s === "ES")  return "future";
  return "equity";
}

export function fmtIV(iv) {
  if (iv == null || Number.isNaN(iv)) return "—";
  // server sends 0.80 or 80? you had both in mock; handle both:
  const pct = iv > 5 ? Math.round(iv) : Math.round(iv * 100);
  return `${pct}%`;
}

export function timeAgo(ts) {
  if (!ts) return "—";
  const d = Date.now() - Number(ts);
  if (d < 1500) return "now";
  if (d < 60_000) return `${Math.floor(d/1000)}s`;
  if (d < 3_600_000) return `${Math.floor(d/60_000)}m`;
  return `${Math.floor(d/3_600_000)}h`;
}

export function computeEsSpxBasis(rows) {
  const by = Object.fromEntries(rows.map(r => [normalizeSym(r.symbol), r]));
  const es = by.ES?.last, spx = by.SPX?.last;
  if (es == null || spx == null) return null;
  return Number((es - spx).toFixed(2));
}

export function sortOverview(rows) {
  const pin = { SPX:0, ES:1, SPY:2, QQQ:3, IWM:4 };
  return [...rows].sort((a,b) => {
    const A = normalizeSym(a.symbol), B = normalizeSym(b.symbol);
    const pa = A in pin ? pin[A] : 100, pb = B in pin ? pin[B] : 100;
    if (pa !== pb) return pa - pb;
    return A.localeCompare(B);
  });
}
