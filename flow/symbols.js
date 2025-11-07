export function normalizeSymbol(sym = "") {
  const s = String(sym).trim().toUpperCase();
  if (s === "/ES" || s === "ES=F") return "ES";
  if (/^ES[HMUZ]\d{1,2}$/.test(s)) return "ES";
  return s;
}

export function inferAssetClass(ul = "") {
  const u = normalizeSymbol(ul);
  if (u === "ES" || /^ES[HMUZ]\d{1,2}$/.test(ul)) return "futures";
  if (u === "SPX" || u.startsWith("$")) return "index";
  return "equity";
}
