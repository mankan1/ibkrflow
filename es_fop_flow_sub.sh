#!/usr/bin/env bash
# Subscribe to ES future + nearby FOPs around ATM for a given OCC expiry
# Usage: bash ./es_fop_flow_sub.sh 'ES|YYYY-MM-DD|C|6800' [STRIKE_WINDOW] [EXCH]
# Example: bash ./es_fop_flow_sub.sh 'ES|2025-11-14|C|6800' 8 GLOBEX
set -eo pipefail

OCC="$1"; WIN="${2:-8}"; EXCH_IN="${3:-GLOBEX}"
HOST="${HOST:-https://127.0.0.1:5000}"

if [ -z "${OCC:-}" ]; then
  echo "Usage: bash ./es_fop_flow_sub.sh 'ES|YYYY-MM-DD|C|6800' [STRIKE_WINDOW] [EXCH]" >&2
  exit 1
fi

ROOT="${OCC%%|*}"; REST="${OCC#*|}"
DATE="${REST%%|*}"; REST="${REST#*|}"
RIGHT="${REST%%|*}"; STRIKE_IN="${REST#*|}"
YMD="${DATE//-/}"; MONTH="${YMD:0:6}"

echo "OCC       : $OCC"
echo "ROOT      : $ROOT"
echo "EXPIRY    : $DATE ($YMD)  MONTH=$MONTH"
echo "PREF RIGHT: $RIGHT    SEED STRIKE: $STRIKE_IN"
echo "EXCH(req) : ${EXCH_IN}"
echo "HOST      : $HOST"
echo

echo "[info] Setting market data type to 1 (live) ..."
curl -ksS -X POST "$HOST/v1/api/iserver/marketdata/type" \
  -H 'Content-Type: application/json' -d '{"marketDataType":1}' >/dev/null || true
echo

TMP="$(mktemp -d -t esflow.XXXXXX)"; trap 'rm -rf "$TMP"' EXIT

# ---------- helpers (tiny Node, no jq) ----------
cat >"$TMP/pick_front_for_month.js" <<'JS'
/*
Find FUT conid for ES whose expiry month matches env.MONTH (yyyyMM).
Prefer tradingClass 'ES', else symbol/localSymbol starting 'ES'.
Input: /trsrv/futures?symbols=ES
*/
const month = process.env.MONTH || '';
let d='';process.stdin.on('data',c=>d+=c).on('end',()=>{
  let j; try{ j=JSON.parse(d) }catch{ process.exit(3) }
  const rows = (j && j[0] && Array.isArray(j[0].contracts)) ? j[0].contracts : [];
  function mm(r){
    const raw=(r.expiry||r.lastTradeDate||r.maturity||'')+'';
    return raw.replace(/-/g,'').slice(0,6);
  }
  for(const r of rows){
    if(mm(r)!==month) continue;
    const tc=(r.tradingClass||'')+'';
    const sym=(r.symbol||'')+'';
    const ls =(r.localSymbol||'')+'';
    const ok = tc==='ES' || sym==='ES' || ls.startsWith('ES');
    if(ok && r.conid!=null){ process.stdout.write(String(r.conid)); return; }
  }
});
JS

cat >"$TMP/find_fop_conids.js" <<'JS'
/*
From /iserver/secdef/strikes for a FUT conid, select a ring of strikes around ATM
for BOTH rights (C & P) at a specific expiry (env.YMD). If env.RIGHT is set,
we still subscribe both sides to catch sweeps/blocks symmetrically.
Env: YMD, ATM, WIN (int number of strikes above/below), INCR (strike increment guess)
Output: comma-separated list of conids
*/
const ymd=process.env.YMD, atm=+process.env.ATM, win=+process.env.WIN;
let incr = +process.env.INCR; if(!incr || !isFinite(incr)) incr = 5; // ES FOP strikes are 5pt usually
let d='';process.stdin.on('data',c=>d+=c).on('end',()=>{
  let j; try{ j=JSON.parse(d) }catch{ process.exit(3) }

  // normalize payload shapes
  const expirations = j && (j.expirations || j.Expirations);
  const chains      = j && j.chains;

  const wantStrikes = new Set();
  // Build strike ring around ATM in steps of "incr"
  const base = Math.round(atm / incr) * incr;
  for(let k=-win;k<=win;k++){
    wantStrikes.add(base + k*incr);
  }

  const out = [];
  function scanSide(arr){
    if(!Array.isArray(arr)) return;
    for(const x of arr){
      const st = +x.strike;
      if(wantStrikes.has(st) && x.conid!=null) out.push(String(x.conid));
    }
  }

  if(Array.isArray(expirations)){
    for(const e of expirations){
      const exp = (e.expiry || e.maturity || e.lastTradeDate || '').replace(/-/g,'');
      if(exp!==ymd) continue;
      // Subscribe both calls & puts; flow logic usually wants both
      scanSide(e.call || e.calls);
      scanSide(e.put  || e.puts);
    }
  }else if(Array.isArray(chains)){
    for(const c of chains){
      const exp = (c.expiry || c.maturity || '').replace(/-/g,'');
      if(exp!==ymd) continue;
      const strikes = c.strikes || [];
      for(const s of strikes){
        const st = +s.strike;
        if(wantStrikes.has(st) && s.conid!=null) out.push(String(s.conid));
      }
    }
  }else if(Array.isArray(j)){
    // flat records
    for(const r of j){
      const exp = (r.expiry||r.maturityDate||r.lastTradeDate||r.maturity||'').replace(/-/g,'');
      if(exp!==ymd) continue;
      const st = +(r.strike ?? r.optStrike ?? NaN);
      if(wantStrikes.has(st) && r.conid!=null) out.push(String(r.conid));
    }
  }

  // de-dupe
  const uniq = [...new Set(out)];
  process.stdout.write(uniq.join(','));
});
JS

cat >"$TMP/get_num.js" <<'JS'
let d='';process.stdin.on('data',c=>d+=c).on('end',()=>{
  try{
    const j=JSON.parse(d)[0]||{};
    const v = j["31"]; // last
    const n = (v!=null)? Number(String(v).replace(/^C/,'')) : NaN;
    if(!isFinite(n)) process.exit(2);
    process.stdout.write(String(n));
  }catch{ process.exit(1) }
});
JS

# ---------- 1) FUT resolver via /trsrv/futures ----------
echo "[info] Resolving ES FUT for month $MONTH via /trsrv/futures ..."
curl -ksS "$HOST/v1/api/trsrv/futures?symbols=$ROOT" > "$TMP/fut_list.json"
FUT_CONID="$(MONTH="$MONTH" node "$TMP/pick_front_for_month.js" < "$TMP/fut_list.json" || true)"
if [ -z "$FUT_CONID" ]; then
  echo "[error] Could not find ES FUT for $MONTH in /trsrv/futures"
  sed -n '1,120p' "$TMP/fut_list.json"
  exit 2
fi
echo "[ok] FUT conid: $FUT_CONID"
echo

# ---------- 2) Compute ATM from FUT snapshot ----------
echo "[info] Snapshot FUT last to seed ATM ..."
FUT_LAST="$(curl -ksS "$HOST/v1/api/iserver/marketdata/snapshot?conids=$FUT_CONID&fields=31" | node "$TMP/get_num.js" || true)"
if [ -z "$FUT_LAST" ]; then
  echo "[warn] Could not get FUT last; defaulting ATM to provided strike $STRIKE_IN"
  FUT_LAST="$STRIKE_IN"
fi
echo "[ok] FUT last ~ $FUT_LAST"
echo

# ---------- 3) Pull FOP strikes for the month ----------
echo "[info] Pulling FOP strikes for FUT=$FUT_CONID month=$MONTH on ${EXCH_IN} ..."
QEX=""; [ -n "$EXCH_IN" ] && QEX="&exchange=$EXCH_IN"
curl -ksS "$HOST/v1/api/iserver/secdef/strikes?conid=$FUT_CONID&sectype=FOP&month=$MONTH$QEX" > "$TMP/strikes.json"

# ---------- 4) Choose a ring of strikes around ATM and return conids ----------
echo "[info] Selecting +/- $WIN strikes around ATM and both rights for expiry $YMD ..."
ATM_APPROX="$FUT_LAST"
FOP_CONIDS="$(YMD="$YMD" ATM="$ATM_APPROX" WIN="$WIN" INCR="5" node "$TMP/find_fop_conids.js" < "$TMP/strikes.json" || true)"
if [ -z "$FOP_CONIDS" ]; then
  echo "[error] Could not derive FOP conids for expiry $YMD. Raw (first 200 lines):"
  sed -n '1,200p' "$TMP/strikes.json"
  exit 3
fi
echo "[ok] FOP conids: $(echo "$FOP_CONIDS" | sed 's/,/, /g')"
echo

# ---------- 5) Subscribe FUT + FOPs (stream arrives on your WS) ----------
ALL="$FUT_CONID,$FOP_CONIDS"
echo "[info] Subscribing conids (FUT + FOPs) ..."
curl -ksS -X POST "$HOST/v1/api/iserver/marketdata/subscribe?conids=$ALL" \
  -H 'Content-Type: application/json' -d '{}' >/dev/null

echo "[done] Subscribed."
echo "➡ Keep the WebSocket open (in terminal #1). You should see 'md' events for the FUT and selected FOPs."

