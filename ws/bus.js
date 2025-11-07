import { d } from "../debug/logger.js";
export function makeBus(wss) {
  const metrics = { prints:0, headlines:0 };
  return {
    metrics,
    broadcast(msg) {
      const s = JSON.stringify(msg);
      for (const c of wss.clients) if (c.readyState === 1) c.send(s);
      if (msg?.topic && metrics[msg.topic] !== undefined) metrics[msg.topic]++;
      d("futures", "broadcast", { topic: msg.topic, totalSent: metrics[msg.topic] });
    }
  };
}
