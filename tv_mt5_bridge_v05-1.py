"""
TradingView -> MT5 bridge, v0.7.

v0.5 -> v0.6: TVSignal previously only captured
id/source/symbol/side/signal_type/signal_price/sl/value/value2/signal_time_ms.
The Blitzkrieg Index Algo Matrix V3.4 script's entry alerts also carry a full
exit-management config bundle (DEMA trail, breakeven, stepped profit lock,
exit-Renko, continuation management -- see f_exitManagementPayload in the
Pine script). Because FastAPI/Pydantic ignore fields a model doesn't
declare, v0.5 was accepting those alerts and silently discarding every
exit-management field -- positions would have opened with only an initial
SL and then gone unmanaged. v0.6 declares the full field set and sets
`extra="forbid"` specifically so a future mismatch (e.g. the Pine script
gets edited again) fails loudly with a 422 instead of silently dropping
fields again. /mt5/next's response also changed from a tab-separated line
(fine for 8 fields, not for 30+) to a JSON body.

v0.6 -> v0.7: added GET/POST /mt5/config -- a small key-value settings store
(risk_percent, dry_run, magic_number, broker_symbol, deviation_points,
poll/management intervals) that the execution side (an MT5 EA, or anything
else polling this bridge) reads on every cycle instead of baking those
values into a local config file. This is what lets Aaron -- or Claude,
on his behalf -- change execution-side settings "on the fly" from
anywhere, without touching the VPS/MT5 terminal directly. Auth is the same
MT5_TOKEN already used for /mt5/next and /mt5/ack; CORS is opened up (this
endpoint is token-gated, not origin-gated) so a plain local HTML settings
page can call it directly from a browser.

v0.7 -> v0.7.1: TVSignal now also accepts risk_percent_override (sent with
every V3.4 entry alert) -- was missing from this model, so extra="forbid"
was hard-rejecting every single alert with a 422. Declared here, not yet
persisted to the signals table or used to override the execution-side
risk_percent config -- that's still a later step.

Everything else -- token model, duplicate detection via the id primary key,
stale-signal expiry, the /tv/{token} + /mt5/next + /mt5/ack shape -- is
unchanged from v0.6.
"""

import os
import re
import sqlite3
import time
from contextlib import closing
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field

INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")
MT5_TOKEN = os.environ.get("MT5_TOKEN", "")
MAX_SIGNAL_AGE_SECONDS = int(os.environ.get("MAX_SIGNAL_AGE_SECONDS", "120"))
DB_PATH = os.environ.get("DB_PATH", "./signals.db")

if not INGEST_TOKEN or not MT5_TOKEN:
    raise RuntimeError("INGEST_TOKEN and MT5_TOKEN must both be set.")
if INGEST_TOKEN == MT5_TOKEN:
    raise RuntimeError("Use different tokens for TradingView ingest and MT5 access.")

app = FastAPI(title="Trading Automation Bridge", version="0.7.0")

# /mt5/config is protected by MT5_TOKEN, not by origin, so it's fine to let
# any origin call it -- this is what lets a plain local settings.html page
# (opened as a file, no server of its own) reach this API from a browser.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# BUY/SELL are all the current Blitzkrieg V3.4 script ever sends (grep
# alert( in the Pine script -- six call sites, all entries, side is always
# BUY or SELL). CLOSE_BUY/CLOSE_SELL/MANAGE_BUY/MANAGE_SELL/TRAIL_BUY/
# TRAIL_SELL are kept here, unused, in case an earlier or different script
# still targets this same bridge with that per-action-alert protocol -- if
# nothing does, they're safe to delete.
ALLOWED_SIDES = {
    "BUY", "SELL",
    "CLOSE_BUY", "CLOSE_SELL",
    "MANAGE_BUY", "MANAGE_SELL",
    "TRAIL_BUY", "TRAIL_SELL",
}

EXIT_RENKO_METHODS = {"Traditional", "ATR"}
EXIT_RENKO_ACTIONS = {
    "Partial + BE", "BE Only", "Renko Structure SL", "Partial + Renko SL", "Close Full Position",
}


class TVSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=5, max_length=160)
    source: str
    symbol: str = Field(min_length=1, max_length=64)
    side: str
    signal_type: str = Field(min_length=1, max_length=32)
    signal_price: float
    sl: float
    risk_percent_override: float = 0.0

    # Legacy fields from the earlier per-action-alert protocol (see
    # ALLOWED_SIDES note above). Optional/defaulted so this model doesn't
    # break if something still sends them; Blitzkrieg V3.4 never does.
    value: float = 0.0
    value2: float = 0.0

    # --- Exit-management config, sent with every V3.4 entry alert ---
    management_version: int
    chart_timeframe: str
    dema_trail_enabled: bool
    dema_length: int
    dema_trail_buffer: float
    fixed_tp_enabled: bool
    fixed_tp_r: float
    be_enabled: bool
    be_trigger_r: float
    be_offset: float
    profit_lock_enabled: bool
    profit_lock_start_r: float
    profit_lock_first_sl_r: float
    profit_lock_trigger_every_r: float
    profit_lock_move_sl_by_r: float
    profit_lock_max_moves: int
    exit_renko_enabled: bool
    exit_renko_method: str
    exit_renko_brick_size: float
    exit_renko_atr_length: int
    exit_renko_opposing_bricks: int
    exit_renko_action: str
    exit_renko_partial_close_pct: float
    exit_renko_be_offset: float
    exit_renko_sl_buffer: float
    exit_renko_trail_after_trigger: bool
    c_management_enabled: bool
    c_exit_opposing_candles: int
    c_specific_tp_enabled: bool
    c_buy_tp_r: float
    c_sell_tp_r: float

    signal_time_ms: int


# Columns added in v0.6, in the order they're declared on TVSignal above
# (minus the ones already in the v0.5 table: id/symbol/side/signal_type/
# signal_price/sl/value/value2/signal_time_ms).
V06_COLUMNS: list[tuple[str, str]] = [
    ("management_version", "INTEGER NOT NULL DEFAULT 1"),
    ("chart_timeframe", "TEXT NOT NULL DEFAULT ''"),
    ("dema_trail_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("dema_length", "INTEGER NOT NULL DEFAULT 0"),
    ("dema_trail_buffer", "REAL NOT NULL DEFAULT 0"),
    ("fixed_tp_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("fixed_tp_r", "REAL NOT NULL DEFAULT 0"),
    ("be_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("be_trigger_r", "REAL NOT NULL DEFAULT 0"),
    ("be_offset", "REAL NOT NULL DEFAULT 0"),
    ("profit_lock_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("profit_lock_start_r", "REAL NOT NULL DEFAULT 0"),
    ("profit_lock_first_sl_r", "REAL NOT NULL DEFAULT 0"),
    ("profit_lock_trigger_every_r", "REAL NOT NULL DEFAULT 0"),
    ("profit_lock_move_sl_by_r", "REAL NOT NULL DEFAULT 0"),
    ("profit_lock_max_moves", "INTEGER NOT NULL DEFAULT 0"),
    ("exit_renko_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("exit_renko_method", "TEXT NOT NULL DEFAULT ''"),
    ("exit_renko_brick_size", "REAL NOT NULL DEFAULT 0"),
    ("exit_renko_atr_length", "INTEGER NOT NULL DEFAULT 0"),
    ("exit_renko_opposing_bricks", "INTEGER NOT NULL DEFAULT 0"),
    ("exit_renko_action", "TEXT NOT NULL DEFAULT ''"),
    ("exit_renko_partial_close_pct", "REAL NOT NULL DEFAULT 0"),
    ("exit_renko_be_offset", "REAL NOT NULL DEFAULT 0"),
    ("exit_renko_sl_buffer", "REAL NOT NULL DEFAULT 0"),
    ("exit_renko_trail_after_trigger", "INTEGER NOT NULL DEFAULT 0"),
    ("c_management_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("c_exit_opposing_candles", "INTEGER NOT NULL DEFAULT 0"),
    ("c_specific_tp_enabled", "INTEGER NOT NULL DEFAULT 0"),
    ("c_buy_tp_r", "REAL NOT NULL DEFAULT 0"),
    ("c_sell_tp_r", "REAL NOT NULL DEFAULT 0"),
    ("mt5_ticket", "INTEGER"),
    ("ack_detail", "TEXT"),
]

_BOOL_FIELDS = {
    "dema_trail_enabled", "fixed_tp_enabled", "be_enabled", "profit_lock_enabled",
    "exit_renko_enabled", "exit_renko_trail_after_trigger", "c_management_enabled",
    "c_specific_tp_enabled",
}

# --- /mt5/config defaults -----------------------------------------------
# Execution-side settings that used to live only in the VPS's local
# config.json. Now stored in the `settings` table (below) so they can be
# read/changed remotely; these are just the fallback values used until
# something POSTs an override.
CONFIG_DEFAULTS: dict = {
    "risk_percent": 1.0,
    "dry_run": True,
    "magic_number": 20260907,
    "broker_symbol": "FRA40.r",
    "deviation_points": 20,
    "poll_interval_seconds": 5,
    "management_interval_seconds": 15,
    "rates_lookback_bars": 1500,
}
CONFIG_TYPES: dict = {k: type(v) for k, v in CONFIG_DEFAULTS.items()}


def db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                signal_price REAL NOT NULL,
                sl REAL NOT NULL,
                value REAL NOT NULL DEFAULT 0,
                value2 REAL NOT NULL DEFAULT 0,
                signal_time_ms INTEGER NOT NULL,
                received_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                ack_status TEXT,
                ack_at INTEGER
            )
            """
        )

        # Safe migration from earlier bridge DBs -- same pattern the v0.5
        # file already used for value/value2, extended to every v0.6 column.
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(signals)")}
        if "value" not in cols:
            conn.execute("ALTER TABLE signals ADD COLUMN value REAL NOT NULL DEFAULT 0")
        if "value2" not in cols:
            conn.execute("ALTER TABLE signals ADD COLUMN value2 REAL NOT NULL DEFAULT 0")

        for col_name, col_def in V06_COLUMNS:
            if col_name not in cols:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        conn.commit()


init_db()


def require_mt5_token(auth: Optional[str]):
    expected = f"Bearer {MT5_TOKEN}"
    if auth != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/health")
def health():
    return {"ok": True, "version": "0.7.0"}


@app.post("/tv/{token}")
def tradingview_webhook(token: str, signal: TVSignal):
    if token != INGEST_TOKEN:
        raise HTTPException(status_code=404, detail="not found")

    if signal.source != "tradingview":
        raise HTTPException(status_code=400, detail="invalid source")

    side = signal.side.upper()
    if side not in ALLOWED_SIDES:
        raise HTTPException(status_code=400, detail="invalid side")

    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", signal.id):
        raise HTTPException(status_code=400, detail="invalid id characters")
    if not re.fullmatch(r"[A-Za-z0-9_.#-]+", signal.symbol):
        raise HTTPException(status_code=400, detail="invalid symbol characters")

    if signal.signal_price <= 0 or signal.sl <= 0:
        raise HTTPException(status_code=400, detail="prices must be positive")

    # Entry risk anchor sanity.
    if side == "BUY" and signal.sl >= signal.signal_price:
        raise HTTPException(status_code=400, detail="BUY DEMA risk anchor must be below signal price")
    if side == "SELL" and signal.sl <= signal.signal_price:
        raise HTTPException(status_code=400, detail="SELL DEMA risk anchor must be above signal price")

    # Management-value sanity (legacy value/value2 protocol).
    if signal.signal_type in {"PARTIAL_BE", "PARTIAL_ONLY"}:
        if not (0.0 < signal.value < 100.0):
            raise HTTPException(status_code=400, detail="partial percentage must be between 0 and 100")
    if signal.value2 < 0:
        raise HTTPException(status_code=400, detail="offset cannot be negative")

    # v0.6 sanity checks on the exit-management bundle.
    if signal.management_version != 1:
        raise HTTPException(status_code=400, detail=f"unsupported management_version {signal.management_version}")
    if signal.exit_renko_enabled:
        if signal.exit_renko_method not in EXIT_RENKO_METHODS:
            raise HTTPException(status_code=400, detail="invalid exit_renko_method")
        if signal.exit_renko_action not in EXIT_RENKO_ACTIONS:
            raise HTTPException(status_code=400, detail="invalid exit_renko_action")
    if signal.exit_renko_partial_close_pct and not (0.0 < signal.exit_renko_partial_close_pct < 100.0):
        raise HTTPException(status_code=400, detail="exit_renko_partial_close_pct must be between 0 and 100")

    now = int(time.time())

    column_names = [
        "id", "symbol", "side", "signal_type", "signal_price", "sl", "value", "value2",
        "signal_time_ms", "received_at",
    ] + [name for name, _ in V06_COLUMNS if name not in ("mt5_ticket", "ack_detail")]

    values = [
        signal.id, signal.symbol, side, signal.signal_type, signal.signal_price, signal.sl,
        signal.value, signal.value2, signal.signal_time_ms, now,
    ]
    for name, _ in V06_COLUMNS:
        if name in ("mt5_ticket", "ack_detail"):
            continue
        v = getattr(signal, name)
        values.append(int(v) if name in _BOOL_FIELDS else v)

    placeholders = ",".join("?" for _ in column_names)

    with closing(db()) as conn:
        try:
            conn.execute(
                f"INSERT INTO signals ({','.join(column_names)}) VALUES ({placeholders})",
                values,
            )
            conn.commit()
            return {"accepted": True, "duplicate": False, "id": signal.id}
        except sqlite3.IntegrityError:
            return {"accepted": True, "duplicate": True, "id": signal.id}


def _row_to_json(row: sqlite3.Row) -> dict:
    out = {
        "id": row["id"],
        "symbol": row["symbol"],
        "side": row["side"],
        "signal_type": row["signal_type"],
        "signal_price": row["signal_price"],
        "sl": row["sl"],
        "value": row["value"],
        "value2": row["value2"],
        "signal_time_ms": row["signal_time_ms"],
    }
    for name, _ in V06_COLUMNS:
        if name in ("mt5_ticket", "ack_detail"):
            continue
        val = row[name]
        out[name] = bool(val) if name in _BOOL_FIELDS else val
    return out


@app.get("/mt5/next")
def mt5_next(authorization: Optional[str] = Header(default=None)):
    require_mt5_token(authorization)

    now = int(time.time())
    cutoff = now - MAX_SIGNAL_AGE_SECONDS

    with closing(db()) as conn:
        conn.execute(
            """
            UPDATE signals
            SET status='expired', ack_status='expired', ack_at=?
            WHERE status='pending' AND received_at < ?
            """,
            (now, cutoff),
        )
        conn.commit()

        row = conn.execute(
            """
            SELECT *
            FROM signals
            WHERE status='pending'
            ORDER BY received_at ASC
            LIMIT 1
            """
        ).fetchone()

    if row is None:
        return Response(status_code=204)

    return _row_to_json(row)


@app.post("/mt5/ack")
def mt5_ack(
    id: str,
    status: str,
    mt5_ticket: Optional[int] = None,
    detail: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    require_mt5_token(authorization)

    if status not in {"success", "reject", "duplicate"}:
        raise HTTPException(status_code=400, detail="invalid status")

    now = int(time.time())

    with closing(db()) as conn:
        cur = conn.execute(
            """
            UPDATE signals
            SET status='done', ack_status=?, ack_at=?, mt5_ticket=?, ack_detail=?
            WHERE id=? AND status='pending'
            """,
            (status, now, mt5_ticket, detail, id),
        )
        conn.commit()

    return {"ok": True, "updated": cur.rowcount}


# --- Execution-side settings, remotely readable/writable --------------------

def _load_config() -> dict:
    merged = dict(CONFIG_DEFAULTS)
    with closing(db()) as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    for row in rows:
        key = row["key"]
        if key not in CONFIG_DEFAULTS:
            continue  # ignore stale/unknown keys rather than erroring
        caster = CONFIG_TYPES[key]
        raw = row["value"]
        if caster is bool:
            merged[key] = raw == "1"
        elif caster is int:
            merged[key] = int(raw)
        elif caster is float:
            merged[key] = float(raw)
        else:
            merged[key] = raw
    return merged


@app.get("/mt5/config")
def get_config(authorization: Optional[str] = Header(default=None)):
    require_mt5_token(authorization)
    return _load_config()


@app.post("/mt5/config")
def update_config(update: dict, authorization: Optional[str] = Header(default=None)):
    require_mt5_token(authorization)

    unknown = set(update.keys()) - set(CONFIG_DEFAULTS.keys())
    if unknown:
        raise HTTPException(status_code=400, detail=f"unknown setting(s): {sorted(unknown)}")

    if "risk_percent" in update and not (0 < float(update["risk_percent"]) <= 100):
        raise HTTPException(status_code=400, detail="risk_percent must be between 0 and 100")
    if "deviation_points" in update and int(update["deviation_points"]) < 0:
        raise HTTPException(status_code=400, detail="deviation_points cannot be negative")
    for interval_key in ("poll_interval_seconds", "management_interval_seconds"):
        if interval_key in update and int(update[interval_key]) < 1:
            raise HTTPException(status_code=400, detail=f"{interval_key} must be >= 1")

    with closing(db()) as conn:
        for key, value in update.items():
            caster = CONFIG_TYPES[key]
            try:
                typed = caster(value)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail=f"invalid value for {key}: {value!r}")
            stored = "1" if (caster is bool and typed) else ("0" if caster is bool else str(typed))
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, stored),
            )
        conn.commit()

    return _load_config()


# --- Same-origin settings page ---------------------------------------------
# Served from this app itself (not a separate local file), so the settings
# page and the API it calls share an origin -- no CORS, no file:// fetch
# quirks, no base-URL field to fill in. Auth (the MT5 token) is entered once
# and kept in this page's own localStorage.

_SETTINGS_PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Blitzkrieg EA Settings</title>
<style>
  body { font-family: -apple-system, Segoe UI, Arial, sans-serif; max-width: 480px; margin: 40px auto; padding: 0 16px; color: #222; background: #fff; }
  h1 { font-size: 20px; }
  label { display: block; margin-top: 14px; font-size: 13px; color: #555; }
  input[type=text], input[type=number], input[type=password] {
    width: 100%; box-sizing: border-box; padding: 8px; font-size: 14px;
    border: 1px solid #ccc; border-radius: 6px; margin-top: 4px;
  }
  .row { display: flex; align-items: center; gap: 8px; margin-top: 14px; }
  .row label { margin: 0; }
  button {
    margin-top: 20px; padding: 10px 16px; font-size: 14px; border: none;
    border-radius: 6px; cursor: pointer; margin-right: 8px;
  }
  #loadBtn { background: #eee; }
  #saveBtn { background: #2563eb; color: white; }
  #status { margin-top: 14px; font-size: 13px; white-space: pre-wrap; }
  .ok { color: #16a34a; }
  .err { color: #dc2626; }
</style>
</head>
<body>
  <h1>Blitzkrieg EA - remote settings</h1>
  <p style="font-size:13px;color:#666">
    Reads and writes the settings the EA polls on every cycle.
    Changes here take effect on the EA's next timer tick.
  </p>

  <label>MT5 token (same value as InpMT5Token / Render's MT5_TOKEN)
    <input type="password" id="token">
  </label>

  <hr style="margin-top:20px">

  <label>Risk % per trade
    <input type="number" step="0.1" id="risk_percent">
  </label>
  <div class="row">
    <input type="checkbox" id="dry_run">
    <label for="dry_run">Dry run (log only, no real orders)</label>
  </div>
  <label>Magic number
    <input type="number" step="1" id="magic_number">
  </label>
  <label>Broker symbol
    <input type="text" id="broker_symbol">
  </label>
  <label>Deviation (points)
    <input type="number" step="1" id="deviation_points">
  </label>
  <label>Poll interval (seconds)
    <input type="number" step="1" id="poll_interval_seconds">
  </label>
  <label>Management interval (seconds)
    <input type="number" step="1" id="management_interval_seconds">
  </label>
  <label>Rates lookback (bars)
    <input type="number" step="1" id="rates_lookback_bars">
  </label>

  <div>
    <button id="loadBtn">Load current</button>
    <button id="saveBtn">Save changes</button>
  </div>
  <div id="status"></div>

<script>
const FIELDS = ["risk_percent","dry_run","magic_number","broker_symbol",
                "deviation_points","poll_interval_seconds","management_interval_seconds",
                "rates_lookback_bars"];

function setStatus(msg, ok) {
  const el = document.getElementById("status");
  el.textContent = msg;
  el.className = ok ? "ok" : "err";
}

window.addEventListener("load", () => {
  try {
    const savedToken = localStorage.getItem("blitz_token");
    if (savedToken) document.getElementById("token").value = savedToken;
  } catch (e) {}
});

function saveToken() {
  try { localStorage.setItem("blitz_token", document.getElementById("token").value); } catch (e) {}
}

async function loadConfig() {
  saveToken();
  const token = document.getElementById("token").value;
  setStatus("Loading...", true);
  try {
    const resp = await fetch("/mt5/config", { headers: { "Authorization": "Bearer " + token } });
    if (!resp.ok) { setStatus("Load failed: HTTP " + resp.status, false); return; }
    const cfg = await resp.json();
    for (const key of FIELDS) {
      const el = document.getElementById(key);
      if (!el) continue;
      if (el.type === "checkbox") el.checked = !!cfg[key];
      else el.value = cfg[key];
    }
    setStatus("Loaded current settings.", true);
  } catch (e) {
    setStatus("Load failed: " + e, false);
  }
}

async function saveConfig() {
  saveToken();
  const token = document.getElementById("token").value;
  const body = {};
  for (const key of FIELDS) {
    const el = document.getElementById(key);
    if (!el) continue;
    if (el.type === "checkbox") body[key] = el.checked;
    else if (el.type === "number") body[key] = parseFloat(el.value);
    else body[key] = el.value;
  }
  setStatus("Saving...", true);
  try {
    const resp = await fetch("/mt5/config", {
      method: "POST",
      headers: { "Authorization": "Bearer " + token, "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    if (!resp.ok) {
      const text = await resp.text();
      setStatus("Save failed: HTTP " + resp.status + " " + text, false);
      return;
    }
    const cfg = await resp.json();
    setStatus("Saved. EA will pick this up on its next poll.\\n" + JSON.stringify(cfg, null, 2), true);
  } catch (e) {
    setStatus("Save failed: " + e, false);
  }
}

document.getElementById("loadBtn").addEventListener("click", loadConfig);
document.getElementById("saveBtn").addEventListener("click", saveConfig);
</script>
</body>
</html>
"""


@app.get("/ui", response_class=HTMLResponse)
def settings_ui():
    return _SETTINGS_PAGE
