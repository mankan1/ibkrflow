import express from "express";
export const router = express.Router();

let watchlist = { equities: ["AAPL","NVDA","MSFT"], options: [
  { underlying: "AAPL", expiration: "2025-12-19", strike: 200, right: "C" }
]};

router.get("/", (_req, res) => res.json(watchlist));

router.post("/equities", (req, res) => {
  const symbol = String(req.body?.symbol || "").trim().toUpperCase();
  if (!symbol) return res.status(400).json({ error: "symbol required" });
  if (!watchlist.equities.includes(symbol)) watchlist.equities.push(symbol);
  // broadcast({ topic: "watchlist", data: watchlist });
  return res.status(201).json({ ok: true, watchlist });
});

router.delete("/equities/:symbol", (req, res) => {
  const s = String(req.params.symbol || "").trim().toUpperCase();
  watchlist.equities = watchlist.equities.filter(x => x !== s);
  // broadcast({ topic: "watchlist", data: watchlist });
  return res.json({ ok: true, watchlist });
});
