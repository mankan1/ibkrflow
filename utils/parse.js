// Safe parsers for query params
function parseNumberLike(v, fallback) {
  if (v == null) return fallback;
  const s = String(v).trim().replace(/[, $%_]/g, "");
  const n = Number(s);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(n, lo, hi) {
  return Math.min(Math.max(n, lo), hi);
}

module.exports = { parseNumberLike, clamp };

