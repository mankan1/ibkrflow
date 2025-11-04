import { Router } from "express";
import { getWatchlist, addEquity, delEquity, addOptions, broadcastWatchlist } from "../state.js";

export const router = Router();

router.get("/", (_req, res) => { res.json(getWatchlist()); });

router.post("/equities", (req, res) => {
  const raw = String(req.body?.symbol || "").trim();
  if (!raw) return res.status(400).json({ error: "symbol required" });
  const ok = addEquity(raw);
  if (ok) broadcastWatchlist();
  res.json({ ok, watchlist: getWatchlist() });
});

router.delete("/equities/:symbol", (req, res) => {
  const raw = String(req.params?.symbol || "").trim();
  if (!raw) return res.status(400).json({ error: "symbol required" });
  delEquity(raw);
  broadcastWatchlist();
  res.json({ ok:true, watchlist: getWatchlist() });
});

router.post("/options", (req, res) => {
  const arr = Array.isArray(req.body?.options) ? req.body.options : [];
  const added = addOptions(arr);
  if (added) broadcastWatchlist();
  res.json({ ok:true, added, watchlist: getWatchlist() });
});

