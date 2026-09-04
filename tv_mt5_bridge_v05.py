"""
TradingView -> MT5 bridge v0.5
Supports entries, DEMA/Renko full exits, partial close + break-even,
and DEMA trailing-stop commands.
"""

import os
import re
import sqlite3
import time
from contextlib import closing
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")
MT5_TOKEN = os.environ.get("MT5_TOKEN", "")
MAX_SIGNAL_AGE_SECONDS = int(os.environ.get("MAX_SIGNAL_AGE_SECONDS", "120"))
DB_PATH = os.environ.get("DB_PATH", "./signals.db")

if not INGEST_TOKEN or not MT5_TOKEN:
    raise RuntimeError("INGEST_TOKEN and MT5_TOKEN must both be set.")
if INGEST_TOKEN == MT5_TOKEN:
    raise RuntimeError("Use different tokens for TradingView ingest and MT5 access.")

app = FastAPI(title="Trading Automation Bridge", version="0.5.0")

ALLOWED_SIDES = {
    "BUY", "SELL",
    "CLOSE_BUY", "CLOSE_SELL",
    "MANAGE_BUY", "MANAGE_SELL",
    "TRAIL_BUY", "TRAIL_SELL",
}


class TVSignal(BaseModel):
    id: str = Field(min_length=5, max_length=120)
    source: str
    symbol: str = Field(min_length=1, max_length=64)
    side: str
    signal_type: str = Field(min_length=1, max_length=32)
    signal_price: float
    sl: float
    value: float = 0.0
    value2: float = 0.0
    signal_time_ms: int


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

        # Safe migration from earlier bridge DBs.
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(signals)")}
        if "value" not in cols:
            conn.execute("ALTER TABLE signals ADD COLUMN value REAL NOT NULL DEFAULT 0")
        if "value2" not in cols:
            conn.execute("ALTER TABLE signals ADD COLUMN value2 REAL NOT NULL DEFAULT 0")

        conn.commit()


init_db()


def require_mt5_token(auth: Optional[str]):
    expected = f"Bearer {MT5_TOKEN}"
    if auth != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/health")
def health():
    return {"ok": True, "version": "0.5.0"}


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

    # Management-value sanity.
    if signal.signal_type in {"PARTIAL_BE", "PARTIAL_ONLY"}:
        if not (0.0 < signal.value < 100.0):
            raise HTTPException(status_code=400, detail="partial percentage must be between 0 and 100")
    if signal.value2 < 0:
        raise HTTPException(status_code=400, detail="offset cannot be negative")

    now = int(time.time())

    with closing(db()) as conn:
        try:
            conn.execute(
                """
                INSERT INTO signals
                (id, symbol, side, signal_type, signal_price, sl, value, value2,
                 signal_time_ms, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.id,
                    signal.symbol,
                    side,
                    signal.signal_type,
                    signal.signal_price,
                    signal.sl,
                    signal.value,
                    signal.value2,
                    signal.signal_time_ms,
                    now,
                ),
            )
            conn.commit()
            return {"accepted": True, "duplicate": False, "id": signal.id}
        except sqlite3.IntegrityError:
            return {"accepted": True, "duplicate": True, "id": signal.id}


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

    line = "\t".join(
        [
            row["id"],
            row["symbol"],
            row["side"],
            format(row["sl"], ".12g"),
            format(row["signal_price"], ".12g"),
            row["signal_type"],
            format(row["value"], ".12g"),
            format(row["value2"], ".12g"),
        ]
    )
    return Response(content=line, media_type="text/plain")


@app.post("/mt5/ack")
def mt5_ack(
    id: str,
    status: str,
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
            SET status='done', ack_status=?, ack_at=?
            WHERE id=? AND status='pending'
            """,
            (status, now, id),
        )
        conn.commit()

    return {"ok": True, "updated": cur.rowcount}
