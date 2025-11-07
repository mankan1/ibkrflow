import express from "express";
import { validateHeadline } from "../flow/validate.js";
import { d } from "../debug/logger.js";

export default function makeHeadlinesRouter({ store, broadcast }) {
  const r = express.Router();

  r.post("/publish", (req, res) => {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    let ok = 0, dropped = 0, reasonsCount = {};
    for (const row of rows) {
      const v = validateHeadline(row);
      if (!v.ok) {
        dropped++;
        for (const reason of v.reasons) reasonsCount[reason] = (reasonsCount[reason] ?? 0) + 1;
        continue;
      }
      store.headlines.push(v.doc);      // replace with DB insert
      broadcast({ topic:"headlines", data:v.doc });
      ok++;
    }
    d("futures", "publish_result", { ok, dropped, reasonsCount });
    res.json({ ok:true, count: ok, dropped, reasons: reasonsCount });
  });

  r.get("/", (req, res) => {
    const ul = String(req.query.ul ?? "").toUpperCase();
    const since = Number(req.query.since ?? (Date.now() - 24*3600*1000));
    const rows = store.headlines
      .filter(h => (!ul || h.ul === ul) && h.ts >= since)
      .sort((a,b) => b.ts - a.ts);
    res.json({ ok:true, count: rows.length, rows });
  });

  return r;
}
