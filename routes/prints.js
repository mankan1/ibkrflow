import express from "express";
import { validatePrint } from "../flow/validate.js";
import { d } from "../debug/logger.js";

export default function makePrintsRouter({ store, broadcast }) {
  const r = express.Router();

  r.post("/publish", (req, res) => {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    let ok = 0, dropped = 0, reasonsCount = {};
    for (const row of rows) {
      const v = validatePrint(row);
      if (!v.ok) {
        dropped++;
        for (const reason of v.reasons) reasonsCount[reason] = (reasonsCount[reason] ?? 0) + 1;
        continue;
      }
      store.prints.push(v.doc);         // replace with DB insert
      broadcast({ topic:"prints", data:v.doc });
      ok++;
    }
    d("futures", "publish_result", { ok, dropped, reasonsCount });
    //res.json({ ok:true, count: ok, dropped, reasons: reasonsCount });
    res.json({ ok:true, count: ok, dropped, reasons: reasonsCount });
  });

  return r;
}
