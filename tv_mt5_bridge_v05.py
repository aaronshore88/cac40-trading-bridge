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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

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

    value: float = 0.0
    value2: float = 0.0

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


@app.post("/tv/{token}
