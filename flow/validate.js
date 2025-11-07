import { normalizeSymbol, inferAssetClass } from "./symbols.js";
import { d, warn } from "../debug/logger.js";

export function validatePrint(row) {
  const reasons = [];
  const out = { ...row };

  out.ul = normalizeSymbol(row.ul);
  if (!out.ul) reasons.push("missing_ul");

  out.asset_class = row.asset_class ?? inferAssetClass(out.ul);
  if (out.asset_class !== "futures" && out.ul === "ES") {
    // ES must classify as futures
    reasons.push(`asset_mismatch:${out.asset_class}`);
  }

  if (!["CALL","PUT"].includes(out.right)) reasons.push("invalid_right");
  if (typeof out.strike !== "number" || !(out.strike > 0)) reasons.push("invalid_strike");
  if (!out.expiry) reasons.push("missing_expiry");
  if (typeof out.qty !== "number" || out.qty <= 0) reasons.push("invalid_qty");
  if (typeof out.price !== "number" || out.price < 0) reasons.push("invalid_price");

  out.ts = Number(out.ts ?? Date.now());
  if (!Number.isFinite(out.ts)) reasons.push("invalid_ts");

  if (reasons.length) {
    warn("futures", "drop_print", { reasons, row });
    return { ok:false, reasons };
  }
  d("futures", "ok_print", { ul: out.ul, strike: out.strike, expiry: out.expiry });
  return { ok:true, doc: out };
}

export function validateHeadline(row) {
  const reasons = [];
  const out = { ...row };
  out.ul = normalizeSymbol(row.ul);
  out.asset_class = row.asset_class ?? inferAssetClass(out.ul);
  out.ts = Number(out.ts ?? Date.now());
  if (!out.ul) reasons.push("missing_ul");
  if (!out.title) reasons.push("missing_title");
  if (out.asset_class !== "futures" && out.ul === "ES") reasons.push("asset_mismatch");
  if (reasons.length) {
    warn("futures", "drop_headline", { reasons, row });
    return { ok:false, reasons };
  }
  d("futures", "ok_headline", { ul: out.ul, title: out.title });
  return { ok:true, doc: out };
}
