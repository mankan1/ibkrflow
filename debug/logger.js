const DEBUG = process.env.DEBUG?.split(",") ?? [];
const on = (k) => DEBUG.includes(k) || DEBUG.includes("all");

export function d(scope, msg, extra = {}) {
  if (!on(scope)) return;
  const ts = new Date().toISOString();
  console.log(`[${ts}] [${scope}] ${msg}`, Object.keys(extra).length ? extra : "");
}

export function warn(scope, msg, extra = {}) {
  const ts = new Date().toISOString();
  console.warn(`[${ts}] [${scope}] ⚠ ${msg}`, extra);
}
