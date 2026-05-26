# -*- coding: utf-8 -*-
"""
NIFTY ORB LIVE TRADING BOT — v9L (Cloud)
Broker  : Fyers API v3
Strategy: Opening Range Breakout — Complete Model

CLOUD DEPLOYABLE — runs unattended on Oracle/AWS/any VPS
────────────────────────────────────────────────────────────
Key feature: Headless Fyers authentication via TOTP.
  No browser, no manual URL paste, no input() prompts.
  Cron starts it every morning → auto-login → trade → exit.

All strategy logic (OR, v8 filters, v9 minute-1 filter) UNCHANGED.
Live additions:
  - Headless TOTP auth for cloud servers
  - MODE = "LIVE" — real orders placed at Fyers
  - Order fill verification after entry and exit
  - Position reconciliation every 2 minutes
  - DRY_RUN toggle for testing without orders
  - Zero-capital mode: full signal logic runs, orders skipped
  - Persistent state in logs/live_state_v9.json
  - Weekend auto-skip

DEPLOYMENT:
  1. pip install fyers-apiv3 pyotp requests pytz
  2. Fill FY_ID, PIN, TOTP_KEY below
  3. Set DRY_RUN=True for first test
  4. crontab -e → add:
     10 9 * * 1-5 cd /home/ubuntu/orb && python3 live_cloud.py >> logs/cron.log 2>&1
────────────────────────────────────────────────────────────
"""

import os, sys, time, json, logging, threading, traceback, hashlib
from datetime import datetime, date, timedelta
from typing import Optional
from urllib.parse import urlparse, parse_qs
import pytz
import requests
import pyotp       # pip install pyotp

# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

APP_ID       = ""
SECRET_KEY   = ""
REDIRECT_URI = ""

# ── FYERS HEADLESS LOGIN CREDENTIALS ─────────────────────────
#    Required for auto-login on cloud server (no browser needed).
#    FY_ID      : Your Fyers client ID (e.g. "XA12345")
#    PIN        : Your 4-digit Fyers login PIN
#    TOTP_KEY   : Base32 secret from Fyers 2FA setup
#                 (the key you got when scanning QR in authenticator app)
FY_ID      = ""       # ← FILL THIS
PIN        = ""       # ← FILL THIS
TOTP_KEY   = ""       # ← FILL THIS

# ── MODE ──────────────────────────────────────────────────────
MODE = "LIVE"

# ── DRY RUN (True = logs everything, NO real orders) ─────────
#    Set True for today's validation run, False when deploying capital.
DRY_RUN = False

# ── STRATEGY PARAMETERS (unchanged from paper v9) ────────────
T1=60; T2=100; SL_BUF=5; MIN_SL=10
OR_START=(9,15); OR_END=(9,29); ENTRY=(9,44); HARD_EXIT=(15,20)
LOT=65; SSTEP=50; MAXLOTS=50
PH2=500_000; TPC=0.30

# ── v8 ADDITIONS ─────────────────────────────────────────────
MIN_OR_RANGE = 35
ITM_OFFSET   = 50

# ── v9 ADDITION ──────────────────────────────────────────────
BREAKOUT_DEADLINE = (9, 45)

# ── POSITION RECONCILIATION INTERVAL (seconds) ───────────────
RECONCILE_INTERVAL  = 120

# ── VIX refresh interval (seconds) ───────────────────────────
VIX_REFRESH_SECS = 300

# ── STATE PERSISTENCE ────────────────────────────────────────
LIVE_STATE_FILE = "logs/live_state_v9.json"

os.makedirs("logs", exist_ok=True)
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(f"logs/orb_live_v9_{date.today()}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("ORB_LIVE")
IST = pytz.timezone("Asia/Kolkata")


def hmt(h, m):
    from datetime import time as dt
    return dt(h, m)


# ═══════════════════════════════════════════════════════════════
#  FYERS CLIENT — REST + WebSocket + Orders
# ═══════════════════════════════════════════════════════════════

class FyersClient:
    NIFTY_SYM  = "NSE:NIFTY50-INDEX"
    VIX_SYM    = "NSE:INDIAVIX-INDEX"
    MONTH_CODE = {1:'1', 2:'2', 3:'3', 4:'4', 5:'5', 6:'6',
                  7:'7', 8:'8', 9:'9', 10:'O', 11:'N', 12:'D'}
    MONTH_ABBR = {1:'JAN', 2:'FEB', 3:'MAR', 4:'APR', 5:'MAY', 6:'JUN',
                  7:'JUL', 8:'AUG', 9:'SEP', 10:'OCT', 11:'NOV', 12:'DEC'}

    def __init__(self, app_id: str, access_token: str):
        from fyers_apiv3 import fyersModel
        self.app_id = app_id
        self.token  = access_token
        self.fyers  = fyersModel.FyersModel(
            client_id=app_id,
            is_async=False,
            token=access_token,
            log_path="logs"
        )
        self._ws          = None
        self._tick_cb     = None
        self._ws_running  = False
        log.info(f"Fyers connected: {app_id}  mode={MODE}  dry_run={DRY_RUN}")

    # ── REST helpers ──────────────────────────────────────────

    def _quote(self, symbol: str) -> dict:
        try:
            resp = self.fyers.quotes(data={"symbols": symbol})
            if resp and resp.get("s") == "ok":
                for item in resp.get("d", []):
                    if item.get("s") == "ok":
                        return item.get("v", {})
        except Exception as e:
            log.warning(f"Quote error [{symbol}]: {e}")
        return {}

    def nifty(self) -> float:
        v = self._quote(self.NIFTY_SYM)
        return float(v.get("lp", 0) or 0)

    def vix(self) -> float:
        v = self._quote(self.VIX_SYM)
        lp = float(v.get("lp", 0) or 0)
        return lp if lp > 0 else 15.0

    def opt_ltp(self, symbol: str) -> float:
        if not symbol:
            return 0.0
        v = self._quote(symbol)
        lp = float(v.get("lp", 0) or 0)
        if lp <= 0:
            log.warning(f"opt_ltp returned 0 for {symbol} — raw: {v}")
        return lp

    def opt_ltp_retry(self, symbol: str, retries: int = 3,
                      delay: float = 0.5) -> float:
        for attempt in range(1, retries + 1):
            lp = self.opt_ltp(symbol)
            if lp > 0:
                return lp
            log.warning(f"opt_ltp attempt {attempt}/{retries} failed "
                        f"for {symbol} — retrying in {delay}s ...")
            time.sleep(delay)
        log.error(f"opt_ltp failed after {retries} attempts for {symbol}.")
        return 0.0

    def prev_close(self) -> float:
        try:
            y = date.today() - timedelta(days=1)
            while y.weekday() > 4:
                y -= timedelta(days=1)
            from_dt = datetime.combine(y, datetime.min.time())
            to_dt   = datetime.combine(y, datetime.max.time())
            resp = self.fyers.history(data={
                "symbol":      self.NIFTY_SYM,
                "resolution":  "D",
                "date_format": "0",
                "range_from":  str(int(from_dt.timestamp())),
                "range_to":    str(int(to_dt.timestamp())),
                "cont_flag":   "1"
            })
            if resp and resp.get("s") == "ok":
                candles = resp.get("candles", [])
                if candles:
                    return float(candles[-1][4])
        except Exception as e:
            log.debug(f"prev_close: {e}")
        return 0.0

    def get_expiry(self) -> date:
        today  = date.today()
        dow    = today.weekday()
        target = 3 if today < date(2025, 9, 4) else 1
        da = (target - dow) % 7
        if da == 0:
            da = 7
        return today + timedelta(days=da + 7)

    def _is_monthly_expiry(self, expiry: date) -> bool:
        return (expiry + timedelta(days=7)).month != expiry.month

    def sec_id(self, strike: int, expiry: date, otype: str) -> str:
        yy = expiry.strftime("%y")
        if self._is_monthly_expiry(expiry):
            mon = self.MONTH_ABBR[expiry.month]
            sym = f"NSE:NIFTY{yy}{mon}{int(strike)}{otype}"
        else:
            m  = self.MONTH_CODE[expiry.month]
            dd = expiry.strftime("%d")
            sym = f"NSE:NIFTY{yy}{m}{dd}{int(strike)}{otype}"
        log.debug(f"sec_id: expiry={expiry}  monthly={self._is_monthly_expiry(expiry)}  sym={sym}")
        return sym

    def candles_1min(self, from_dt: datetime, to_dt: datetime) -> list:
        try:
            resp = self.fyers.history(data={
                "symbol":      self.NIFTY_SYM,
                "resolution":  "1",
                "date_format": "0",
                "range_from":  str(int(from_dt.timestamp())),
                "range_to":    str(int(to_dt.timestamp())),
                "cont_flag":   "1"
            })
            if resp and resp.get("s") == "ok":
                return resp.get("candles", [])
        except Exception as e:
            log.warning(f"candles_1min error: {e}")
        return []

    # ── WebSocket ─────────────────────────────────────────────

    def start_websocket(self, on_tick_callback):
        from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket

        self._tick_cb = on_tick_callback

        def _on_message(msg):
            try:
                if isinstance(msg, dict) and msg.get("type") == "sf":
                    for item in msg.get("data", []):
                        ltp = float(item.get("ltp", 0) or 0)
                        if ltp > 0 and self._tick_cb:
                            self._tick_cb(ltp)
                elif isinstance(msg, dict) and "ltp" in msg:
                    ltp = float(msg.get("ltp", 0) or 0)
                    if ltp > 0 and self._tick_cb:
                        self._tick_cb(ltp)
                elif isinstance(msg, list):
                    for item in msg:
                        ltp = float(item.get("ltp", 0) or 0)
                        if ltp > 0 and self._tick_cb:
                            self._tick_cb(ltp)
            except Exception as exc:
                log.debug("WS parse error: %s", exc)

        def _on_error(msg):
            log.error("WebSocket error: %s", msg)

        def _on_close(msg):
            log.warning("WebSocket closed: %s", msg)
            self._ws_running = False

        def _on_connect():
            log.info("WebSocket connected — subscribing to NIFTY ...")
            self._ws.subscribe(
                symbols=[self.NIFTY_SYM],
                data_type="symbolData"
            )
            self._ws_running = True

        self._ws = FyersDataSocket(
            access_token   = f"{self.app_id}:{self.token}",
            write_to_file  = False,
            log_path       = "logs",
            litemode       = False,
            reconnect      = True,
            reconnect_retry= 10,
            on_message     = _on_message,
            on_error       = _on_error,
            on_connect     = _on_connect,
            on_close       = _on_close,
        )

        log.info("Starting WebSocket feed for NIFTY (tick-by-tick)...")
        self._ws.connect()

    def stop_websocket(self):
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        self._ws_running = False

    # ── Order Management ─────────────────────────────────────

    def place_order(self, params: dict) -> dict:
        """
        Place a MARKET order at Fyers.
        DRY_RUN → logs but does not send.
        """
        if DRY_RUN:
            log.info(f"[DRY RUN] Order would be placed: {params}")
            return {"s": "ok", "id": "DRY_RUN_ORDER"}

        try:
            params.setdefault("limitPrice",   0)
            params.setdefault("stopPrice",    0)
            params.setdefault("validity",     "DAY")
            params.setdefault("disclosedQty", 0)
            params.setdefault("offlineOrder", False)

            log.info(f"PLACING ORDER: {json.dumps(params)}")
            resp = self.fyers.place_order(data=params)

            if resp and resp.get("s") == "ok":
                oid = resp.get("id", "")
                log.info(f"ORDER PLACED ✓  id={oid}  "
                         f"sym={params['symbol']}  qty={params['qty']}  "
                         f"side={params['side']}")
                return resp
            else:
                log.error(f"ORDER FAILED ✗  response: {resp}")
                return {}
        except Exception as e:
            log.error(f"place_order exception: {e}")
            return {}

    def modify_order(self, order_id: str, new_stop: float = 0,
                     new_limit: float = 0, order_type: int = 3) -> dict:
        if DRY_RUN or order_id in ("", "DRY_RUN_ORDER"):
            log.info(f"[DRY RUN] Order modify skipped: {order_id}")
            return {"s": "ok"}
        try:
            resp = self.fyers.modify_order(data={
                "id":         order_id,
                "type":       order_type,
                "limitPrice": new_limit,
                "stopPrice":  new_stop
            })
            if resp and resp.get("s") == "ok":
                log.info(f"Order modified: id={order_id}  "
                         f"newStop={new_stop:.1f}  newLimit={new_limit:.1f}")
            else:
                log.error(f"Order modify failed: {resp}")
            return resp or {}
        except Exception as e:
            log.error(f"modify_order exception: {e}")
            return {}

    def cancel_order(self, order_id: str) -> dict:
        if DRY_RUN or order_id in ("", "DRY_RUN_ORDER"):
            log.info(f"[DRY RUN] Order cancel skipped: {order_id}")
            return {"s": "ok"}
        try:
            resp = self.fyers.cancel_order(data={"id": order_id})
            if resp and resp.get("s") == "ok":
                log.info(f"Order cancelled: id={order_id}")
            else:
                log.error(f"Order cancel failed: {resp}")
            return resp or {}
        except Exception as e:
            log.error(f"cancel_order exception: {e}")
            return {}

    # ── Position & Order Queries ──────────────────────────────

    def get_positions(self) -> list:
        try:
            resp = self.fyers.positions()
            if resp and resp.get("s") == "ok":
                return resp.get("netPositions", [])
        except Exception as e:
            log.warning(f"get_positions error: {e}")
        return []

    def get_orders(self) -> list:
        try:
            resp = self.fyers.orderbook()
            if resp and resp.get("s") == "ok":
                return resp.get("orderBook", [])
        except Exception as e:
            log.warning(f"get_orders error: {e}")
        return []

    def get_funds(self) -> dict:
        try:
            resp = self.fyers.funds()
            if resp and resp.get("s") == "ok":
                for f in resp.get("fund_limit", []):
                    if f.get("title") == "Total Balance":
                        return {"balance": float(f.get("equityAmount", 0))}
        except Exception as e:
            log.warning(f"get_funds error: {e}")
        return {}

    def exit_all_positions(self):
        """
        Emergency exit: Fyers API squares off EVERY open position
        in your account in one call. Called automatically if a normal
        exit order fails to fill — ensures no orphan positions survive.
        """
        log.warning("EMERGENCY EXIT — closing all positions via Fyers API")
        try:
            resp = self.fyers.exit_positions(data={})
            log.info(f"exit_all_positions response: {resp}")
            return resp
        except Exception as e:
            log.error(f"exit_all_positions error: {e}")
            return {}


# ═══════════════════════════════════════════════════════════════
#  STATE — persistent, survives restarts
# ═══════════════════════════════════════════════════════════════

class State:
    def __init__(self):
        self.data = self._load()

    def _load(self):
        if os.path.exists(LIVE_STATE_FILE):
            with open(LIVE_STATE_FILE) as f:
                data = json.load(f)
                log.info(f"Loaded state: capital=Rs.{data.get('capital', 0):,.0f}")
                return data
        log.info("No existing state — starting fresh")
        return {
            "start_date":     str(date.today()),
            "capital":        0,
            "peak_capital":   0,
            "max_dd_pct":     0.0,
            "days_traded":    0,
            "total_trades":   0,
            "winning_trades": 0,
            "losing_trades":  0,
            "daily":          [],
            "trade_log":      []
        }

    def save(self):
        tmp = LIVE_STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)
        os.replace(tmp, LIVE_STATE_FILE)

    @property
    def capital(self):
        return self.data["capital"]

    @capital.setter
    def capital(self, v):
        self.data["capital"] = v
        if v > self.data["peak_capital"]:
            self.data["peak_capital"] = v
        dd = (v - self.data["peak_capital"]) / self.data["peak_capital"] * 100 \
            if self.data["peak_capital"] > 0 else 0
        if dd < self.data["max_dd_pct"]:
            self.data["max_dd_pct"] = dd
        self.save()

    def set_initial_capital(self, amount: float):
        """Set capital from Fyers funds on first run or if currently zero."""
        if self.data["capital"] <= 0:
            self.data["capital"]      = amount
            self.data["peak_capital"] = amount
            self.save()
            log.info(f"Initial capital set from Fyers: Rs.{amount:,.0f}")

    def add_trade(self, t):
        self.data["total_trades"] += 1
        if t.get("pnl", 0) >= 0:
            self.data["winning_trades"] += 1
        else:
            self.data["losing_trades"] += 1
        self.data["trade_log"].append(t)
        self.save()

    def add_daily(self, d):
        self.data["days_traded"] += 1
        self.data["daily"].append(d)
        self.save()

    def summary(self):
        d  = self.data
        n  = d["total_trades"]
        wr = d["winning_trades"] / n * 100 if n else 0
        wins = [t["pnl"] for t in d["trade_log"] if t.get("pnl", 0) > 0]
        loss = [t["pnl"] for t in d["trade_log"] if t.get("pnl", 0) < 0]
        pf   = sum(wins) / abs(sum(loss)) if loss else 0
        log.info("=" * 60)
        log.info("  LIVE TRADING SUMMARY")
        log.info("=" * 60)
        log.info(f"  Capital:  Rs.{d['capital']:>10,.0f}")
        log.info(f"  Peak:     Rs.{d['peak_capital']:>10,.0f}")
        log.info(f"  Max DD:   {d['max_dd_pct']:>9.1f}%")
        log.info(f"  Trades:{n:>4}  WR:{wr:.1f}%  PF:{pf:.3f}")
        log.info("=" * 60)
        for day in d["daily"][-14:]:
            s = "+" if day["pnl"] >= 0 else ""
            log.info(f"    {day['date']}  {s}Rs.{day['pnl']:>9,.0f}  "
                     f"cap=Rs.{day['capital']:>10,.0f}")
        log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════
#  SIZING — unchanged from paper v9
# ═══════════════════════════════════════════════════════════════

def vmult(vix, d, dow, ga):
    if vix > 24:
        if dow == 4 and not ga: return 0.0
        if vix > 30: return 1.0 if ga else 0.5
        if dow == 4: return 1.0
        base = (1.5 if d == "LONG" else 1.0) if ga else 0.5
        if ga and dow == 3: base = min(base * 1.2, 2.0)
        return base
    if vix > 21: return 0.5
    if vix >= 18: return 1.5 if ga else 1.0
    if vix >= 13: return 1.0
    return 1.75 if ga else 1.25

def lcap(c):
    if c < 60_000:    return 1
    if c < 200_000:   return 3
    if c < 500_000:   return 8
    if c < 2_000_000: return 20
    return MAXLOTS

def lots(cap, lc, vix, d, dow, ga):
    vm = vmult(vix, d, dow, ga)
    if vm == 0: return 0
    cm   = lcap(cap)
    base = max(1, min(int(cap / lc), cm)) if cap < PH2 \
           else max(1, min(int(cap * TPC / lc), cm))
    n    = max(1, round(base * vm))
    n    = min(n, cm)
    while n > 0 and n * lc > cap:
        n -= 1
    return max(0, n)


# ═══════════════════════════════════════════════════════════════
#  CHARGES CALCULATOR — unchanged from paper v9
# ═══════════════════════════════════════════════════════════════

def calc_charges(entry_prem: float, exit_prem: float,
                 lots_n: int, lot_size: int) -> dict:
    entry_tv = entry_prem * lots_n * lot_size
    exit_tv  = exit_prem  * lots_n * lot_size

    brok  = 20.0 + 20.0
    stt   = exit_tv * 0.0015
    exc   = (entry_tv + exit_tv) * 0.0003503
    sebi  = (entry_tv + exit_tv) * 0.000001
    stamp = entry_tv * 0.00003
    gst   = (brok + exc + sebi) * 0.18
    total = brok + stt + exc + sebi + stamp + gst

    return {
        "brokerage": round(brok,  2),
        "stt":       round(stt,   2),
        "exchange":  round(exc,   2),
        "sebi":      round(sebi,  2),
        "stamp":     round(stamp, 2),
        "gst":       round(gst,   2),
        "total":     round(total, 2),
    }


# ═══════════════════════════════════════════════════════════════
#  BOT — LIVE version
# ═══════════════════════════════════════════════════════════════

class Bot:
    def __init__(self, p: FyersClient, s: State):
        self.p = p; self.s = s; self.today = date.today()

        # ── Day state ──────────────────────────────────────────
        self.day_pnl = 0.0
        self.vix     = 15.0
        self.pc      = 0.0
        self.exp     = None

        # ── OR state ───────────────────────────────────────────
        self.orH  = None
        self.orL  = None
        self.orR  = 0.0
        self.orOK = False

        # ── Signal / trade state ───────────────────────────────
        self.fired   = False
        self.d       = None
        self.ot      = None
        self.eN      = 0.0
        self.eP      = 0.0
        self.eL      = 0
        self.sid     = ""
        self.sk      = 0
        self.eH      = 0.0

        # ── SL / target state ──────────────────────────────────
        self.sl       = 0.0
        self.t1       = False
        self.t2       = False

        # ── Exchange order IDs ─────────────────────────────────
        self.entry_order_id  = ""

        # ── Tick throttle ──────────────────────────────────────
        self._last_ltp     = 0.0
        self._last_tick_t  = None

        # ── Threading ──────────────────────────────────────────
        self._lock = threading.Lock()
        self._done = False

        # ── Zero capital flag ──────────────────────────────────
        self._no_capital = False

    # ──────────────────────────────────────────────────────────
    #  ORDER FILL VERIFICATION
    # ──────────────────────────────────────────────────────────

    def _verify_order_fill(self, order_id: str, symbol: str,
                           expected_qty: int, timeout: int = 10) -> bool:
        """
        Confirms a market order filled at Fyers.
        Does NOT retry or re-place — just checks status.
        Market orders on liquid NIFTY options fill in milliseconds.
        """
        if DRY_RUN or self._no_capital or order_id in ("", "DRY_RUN_ORDER"):
            return True

        log.info(f"Verifying fill: {order_id} ...")
        for i in range(timeout):
            time.sleep(1)
            orders = self.p.get_orders()
            for o in orders:
                if o.get("id") == order_id:
                    status = o.get("status", 0)
                    if status == 2:   # Filled
                        traded_price = o.get("tradedPrice", 0)
                        log.info(f"Order FILLED ✓  id={order_id}  "
                                 f"price={traded_price}")
                        return True
                    elif status in (5, 6):   # Rejected / Cancelled
                        reason = o.get("message", "unknown")
                        log.error(f"Order REJECTED ✗  id={order_id}  "
                                  f"reason={reason}")
                        return False
            log.debug(f"Fill check {i+1}/{timeout} — pending ...")

        log.error(f"Order fill TIMEOUT after {timeout}s — id={order_id}")
        return False

    # ──────────────────────────────────────────────────────────
    #  POSITION RECONCILIATION
    # ──────────────────────────────────────────────────────────

    def _reconcile_positions(self):
        """
        Background thread: every 2 minutes, checks that Fyers
        positions match bot's internal state. Logs warnings if
        they diverge — visible in your cloud server logs.
        """
        while not self._done:
            time.sleep(RECONCILE_INTERVAL)
            if self._done:
                break
            if DRY_RUN or self._no_capital:
                continue
            try:
                positions = self.p.get_positions()
                nifty_opts = [p for p in positions
                              if "NIFTY" in p.get("symbol", "")
                              and p.get("netQty", 0) != 0]

                bot_has_position = (self.eL > 0 and self.sid)

                if bot_has_position and not nifty_opts:
                    log.warning(f"⚠ RECONCILE MISMATCH: Bot has {self.sid} x{self.eL} "
                                f"but Fyers shows NO position!")
                elif not bot_has_position and nifty_opts:
                    syms = [p["symbol"] for p in nifty_opts]
                    log.warning(f"⚠ RECONCILE MISMATCH: Bot has no position "
                                f"but Fyers shows: {syms}")
                elif bot_has_position and nifty_opts:
                    log.info(f"Position reconciled ✓  {self.sid} x{self.eL}")
            except Exception as e:
                log.debug(f"Reconciliation error: {e}")

    # ──────────────────────────────────────────────────────────
    #  DYNAMIC START HELPERS — unchanged from paper
    # ──────────────────────────────────────────────────────────

    def _backfill_or(self, up_to: Optional[datetime] = None):
        today   = date.today()
        from_dt = datetime.combine(today, hmt(*OR_START))
        if up_to is not None:
            to_dt = up_to.replace(second=0, microsecond=0) - timedelta(seconds=1)
        else:
            to_dt = datetime.combine(today, hmt(*OR_END)).replace(second=59)

        if from_dt >= to_dt:
            log.info("OR backfill: nothing to fetch")
            return

        log.info(f"OR backfill: {from_dt.strftime('%H:%M')}–{to_dt.strftime('%H:%M')} ...")
        candles = self.p.candles_1min(from_dt, to_dt)
        if not candles:
            log.warning("OR backfill: no candles returned")
            return

        new_h = max(c[2] for c in candles)
        new_l = min(c[3] for c in candles)
        self.orH = max(self.orH, new_h) if self.orH is not None else new_h
        self.orL = min(self.orL, new_l) if self.orL is not None else new_l
        log.info(f"OR backfill done: H={self.orH:.1f}  L={self.orL:.1f}  "
                 f"({len(candles)} candles)")

    def _finalize_or(self):
        if self.orH is not None and self.orL is not None:
            self.orR  = self.orH - self.orL
            if self.orR < MIN_OR_RANGE:
                self.orOK = False
                log.info(f"OR RANGE FILTER ✗  range={self.orR:.0f} pts < {MIN_OR_RANGE} pts — "
                         f"trade skipped today")
                self._banner("OR RANGE TOO NARROW — NO TRADE TODAY", [
                    f"OR High       : {self.orH:.1f}",
                    f"OR Low        : {self.orL:.1f}",
                    f"OR Range      : {self.orR:.0f} pts",
                    f"Minimum Req   : {MIN_OR_RANGE} pts",
                ])
                return
            self.orOK = True
            log.info(f"OR COMPLETE ✓  H={self.orH:.1f}  L={self.orL:.1f}  "
                     f"range={self.orR:.0f} pts")
        else:
            log.warning("OR could not be established")

    def _check_opportunity_lost(self) -> bool:
        now      = datetime.now(IST).replace(tzinfo=None)
        today    = date.today()
        entry_dt = datetime.combine(today, hmt(*ENTRY))
        if now <= entry_dt:
            return False
        to_dt = now.replace(second=0, microsecond=0) - timedelta(seconds=1)
        if entry_dt >= to_dt:
            return False
        log.info("Opportunity check: fetching candles from entry to now ...")
        candles = self.p.candles_1min(entry_dt, to_dt)
        if not candles:
            log.warning("Opportunity check: no candles — assuming live")
            return False
        for c in candles:
            if c[2] > self.orH or c[3] < self.orL:
                log.warning("OPPORTUNITY LOST — breakout already occurred.")
                return True
        log.info("Opportunity check: breakout NOT yet occurred — live.")
        return False

    # ──────────────────────────────────────────────────────────
    #  VIX BACKGROUND THREAD
    # ──────────────────────────────────────────────────────────

    def _vix_refresher(self):
        while not self._done:
            time.sleep(VIX_REFRESH_SECS)
            if self._done:
                break
            try:
                v = self.p.vix()
                if v > 0:
                    self.vix = v
                    log.info(f"VIX refreshed: {v:.1f}")
            except Exception as e:
                log.debug(f"VIX refresh error: {e}")

    # ──────────────────────────────────────────────────────────
    #  EOD TIMER
    # ──────────────────────────────────────────────────────────

    def _schedule_eod(self):
        now   = datetime.now(IST)
        eod   = now.replace(hour=15, minute=20, second=0, microsecond=0)
        delta = (eod - now).total_seconds()
        if delta <= 0:
            return
        def _trigger():
            log.info("EOD timer fired — forcing exit ...")
            self._force(self._last_ltp)
            self._eod()
            self._done = True
            self.p.stop_websocket()
        t = threading.Timer(delta, _trigger)
        t.daemon = True
        t.start()
        log.info(f"EOD safety timer set for 15:20 "
                 f"({int(delta//60)}m {int(delta%60)}s from now)")

    # ──────────────────────────────────────────────────────────
    #  MAIN RUN
    # ──────────────────────────────────────────────────────────

    def run(self):
        log.info("=" * 60)
        log.info(f"  NIFTY ORB BOT v9L  [LIVE]  {self.today}")
        log.info(f"  DRY_RUN: {DRY_RUN}")
        log.info(f"  Capital: Rs.{self.s.capital:,.0f}")
        log.info("=" * 60)

        now_ist = datetime.now(IST)
        t_now   = now_ist.time()

        if t_now >= hmt(*HARD_EXIT):
            log.warning("Started after market close — nothing to do today.")
            self._eod()
            return

        # ── Session-wide REST fetches ──────────────────────────
        self.pc  = self.p.prev_close()
        self.vix = self.p.vix()
        self.exp = self.p.get_expiry()

        # ── Fetch capital from Fyers ───────────────────────────
        funds = self.p.get_funds()
        balance = funds.get("balance", 0)
        if balance > 0:
            self.s.set_initial_capital(balance)
            log.info(f"Fyers balance: Rs.{balance:,.0f}")
        else:
            log.warning("═" * 58)
            log.warning("  Fyers balance = Rs.0")
            log.warning("  Bot will run ALL signal logic (OR, breakout,")
            log.warning("  strike selection, premium fetch, lot sizing)")
            log.warning("  but orders will NOT be placed.")
            log.warning("  Deploy capital and set DRY_RUN=False to go live.")
            log.warning("═" * 58)
            self._no_capital = True

        log.info(f"VIX={self.vix:.1f}  prev_close={self.pc:.1f}  "
                 f"expiry={self.exp}  no_capital={self._no_capital}")

        # ── OR backfill based on start time ────────────────────
        if t_now < hmt(*OR_START):
            log.info(f"[START] Before OR. Waiting for {hmt(*OR_START)} ...")

        elif hmt(*OR_START) <= t_now <= hmt(*OR_END):
            log.info("[START] Mid OR window. Backfilling missed candles ...")
            self._backfill_or(up_to=now_ist.replace(tzinfo=None))

        elif hmt(*OR_END) < t_now < hmt(*ENTRY):
            log.info("[START] OR window passed. Backfilling full OR ...")
            self._backfill_or()
            self._finalize_or()
            if not self.orOK:
                log.error("OR not established — cannot trade today.")
                self._eod(); return

        else:
            log.info("[START] Entry window already open. Backfilling OR ...")
            self._backfill_or()
            self._finalize_or()
            if not self.orOK:
                log.error("OR not established — cannot trade today.")
                self._eod(); return
            if self._check_opportunity_lost():
                self.fired = True
                log.info("Monitor-only mode until 15:20.")

        # ── Start background threads ───────────────────────────
        for target in [self._vix_refresher, self._reconcile_positions]:
            t = threading.Thread(target=target, daemon=True)
            t.start()

        self._schedule_eod()

        # ── Hand control to WebSocket ──────────────────────────
        log.info("Handing control to WebSocket tick feed ...")
        self.p.start_websocket(self.on_tick)

        while not self._done:
            time.sleep(1)

        if not self._done:
            self._eod()

    # ──────────────────────────────────────────────────────────
    #  TICK HANDLER
    # ──────────────────────────────────────────────────────────

    def on_tick(self, ltp: float):
        if self._done:
            return
        if ltp == self._last_ltp:
            return
        self._last_ltp = ltp

        with self._lock:
            try:
                self._process_tick(ltp)
            except Exception as e:
                log.error(f"TICK ERROR: {e}\n{traceback.format_exc()}")

    def _process_tick(self, ltp: float):
        now = datetime.now(IST)
        t   = now.time()

        # ── Hard exit guard ───────────────────────────────────
        if t >= hmt(15, 20):
            if not self._done:
                self._force(ltp)
                self._eod()
                self._done = True
                self.p.stop_websocket()
            return

        # ── Live OR building ──────────────────────────────────
        if hmt(*OR_START) <= t <= hmt(*OR_END):
            if self.orH is None:
                self.orH = ltp; self.orL = ltp
            else:
                if ltp > self.orH: self.orH = ltp
                if ltp < self.orL: self.orL = ltp

        # ── Seal OR ───────────────────────────────────────────
        if not self.orOK and t > hmt(*OR_END) and self.orH is not None:
            self._finalize_or()

        # ── v9: Breakout deadline ─────────────────────────────
        if (self.orOK and not self.fired
                and t >= hmt(*BREAKOUT_DEADLINE)
                and t < hmt(*HARD_EXIT)):
            self.fired = True
            log.info(f"BREAKOUT DEADLINE PASSED ✗  No breakout by "
                     f"{BREAKOUT_DEADLINE[0]}:{BREAKOUT_DEADLINE[1]:02d}")
            self._banner("NO MINUTE-1 BREAKOUT — SKIPPING TODAY", [
                f"OR High       : {self.orH:.1f}",
                f"OR Low        : {self.orL:.1f}",
                f"OR Range      : {self.orR:.0f} pts",
                f"Nifty at 9:45 : {ltp:.1f}",
            ])
            return

        # ── Signal detection ──────────────────────────────────
        if (self.orOK and not self.fired
                and t >= hmt(*ENTRY)
                and t < hmt(*HARD_EXIT)):
            self._sig(ltp, now.replace(tzinfo=None))

        # ── Trade management ──────────────────────────────────
        if self.fired:
            self._mgr(ltp, now.replace(tzinfo=None))

        # ── Periodic log (every 60 ticks ≈ ~30 sec) ──────────
        self._tick_count = getattr(self, "_tick_count", 0) + 1
        if self._tick_count % 60 == 0:
            log.info(
                f"Nifty={ltp:.1f}  {t.strftime('%H:%M:%S')}  "
                f"OR={'done' if self.orOK else 'building'}  "
                f"signal={'FIRED' if self.fired else 'watching'}  "
                f"pos={'YES' if self.eL > 0 else 'NO'}  "
                f"VIX={self.vix:.1f}  dayPnL=Rs.{self.day_pnl:,.0f}"
            )

    # ──────────────────────────────────────────────────────────
    #  TERMINAL DISPLAY
    # ──────────────────────────────────────────────────────────

    def _banner(self, title: str, lines: list):
        width = 58
        bar   = "═" * width
        print(f"\n╔{bar}╗")
        print(f"║  {title:<{width-2}}║")
        print(f"╠{bar}╣")
        for line in lines:
            print(f"║  {line:<{width-2}}║")
        print(f"╚{bar}╝\n")

    # ──────────────────────────────────────────────────────────
    #  SIGNAL
    # ──────────────────────────────────────────────────────────

    def _sig(self, n, ts):
        if n > self.orH:
            d, o = "LONG", "CE"
        elif n < self.orL:
            d, o = "SHORT", "PE"
        else:
            return

        ga  = True
        if self.pc > 0:
            gp  = (n - self.pc) / self.pc * 100
            gu  = gp > 0
            ga  = (d == "LONG" and gu) or (d == "SHORT" and not gu)
            dow = self.today.weekday()
            if not ga and ((self.vix > 24 and dow == 4) or self.orR > 200):
                d = "SHORT" if d == "LONG" else "LONG"
                o = "PE"    if o == "CE"   else "CE"
                log.info("Gap fill reversal applied")

        log.info(f"*** SIGNAL: {d} {o}  OR={self.orR:.0f}pts  "
                 f"VIX={self.vix:.1f}  Nifty={n:.1f} ***")

        # ── v8: 1-ITM strike selection ────────────────────────
        atm = round(n / SSTEP) * SSTEP
        if o == "CE":
            sk = atm - ITM_OFFSET
        else:
            sk = atm + ITM_OFFSET
        sid = self.p.sec_id(sk, self.exp, o)
        log.info(f"  ATM={atm}  ITM strike={sk}  (offset={ITM_OFFSET})")
        eh  = ts.hour + ts.minute / 60

        lp = self.p.opt_ltp_retry(sid) if sid else 0.0
        if lp <= 0:
            log.error(f"Cannot get live premium for {sid} — trade skipped.")
            self.fired = False
            return
        log.info(f"  Live premium: Rs.{lp:.1f}")

        lc  = lp * LOT
        dow = self.today.weekday()
        nl  = lots(self.s.capital, lc, self.vix, d, dow, ga)

        self.fired = True

        if nl <= 0:
            if self._no_capital:
                log.info(f"  [NO CAPITAL] Signal {d} {o}  strike={sk}  "
                         f"prem=Rs.{lp:.1f} — order skipped (zero funds)")
                self._banner(f"SIGNAL DETECTED — {d} {o}  (NO CAPITAL)", [
                    f"Time          : {ts.strftime('%H:%M:%S')}",
                    f"Symbol        : {sid}",
                    f"Strike        : {sk}",
                    f"Nifty Spot    : {n:.1f}",
                    f"Option Prem   : Rs.{lp:.2f}",
                    f"OR Range      : {self.orR:.0f} pts",
                    f"VIX           : {self.vix:.1f}",
                    f"Lots computed : 0  (no capital deployed)",
                    f"─" * 48,
                    f"All logic validated ✓  No order placed.",
                ])
            else:
                log.info("Lots=0 after VIX filter — signal skipped")
            return

        self.d = d; self.ot = o; self.eN = n; self.eP = lp
        self.eL = nl; self.sid = sid or ""; self.sk = sk; self.eH = eh

        # SL calculation
        slp = max(n - self.orH + SL_BUF, MIN_SL) if d == "LONG" \
              else max(self.orL - n + SL_BUF, MIN_SL)
        self.sl   = (n - slp) if d == "LONG" else (n + slp)
        self.t1   = False; self.t2 = False

        log.info(f"ENTRY {d} {o}  strike={sk}  lots={nl}  "
                 f"prem=Rs.{lp:.1f}  nifty_SL={self.sl:.1f}")
        self._banner(f"TRADE ENTRY — {d} {o}", [
            f"Time          : {ts.strftime('%H:%M:%S')}",
            f"Symbol        : {sid}",
            f"Strike        : {sk}",
            f"Lots          : {nl}  ({nl * LOT} qty)",
            f"Nifty Spot    : {n:.1f}",
            f"Option Prem   : Rs.{lp:.2f}",
            f"Capital used  : Rs.{nl * lp * LOT:,.0f}",
            f"SL (Nifty)    : {self.sl:.1f}  ({abs(n - self.sl):.1f} pts away)",
            f"T1 target     : {(n + T1) if d == 'LONG' else (n - T1):.1f}  (+{T1} pts)",
            f"T2 target     : {(n + T2) if d == 'LONG' else (n - T2):.1f}  (+{T2} pts)",
            f"Capital       : Rs.{self.s.capital:,.0f}",
        ])

        # ── Place ENTRY order (market) ─────────────────────────
        if self._no_capital:
            log.info("[NO CAPITAL] Order not placed — zero funds.")
            return

        entry_resp = self.p.place_order({
            "symbol":      sid,
            "qty":         nl * LOT,
            "type":        2,           # Market order — fills at best available
            "side":        1,           # Buy
            "productType": "INTRADAY",
        })
        self.entry_order_id = entry_resp.get("id", "")

        # ── Verify fill (confirms only, no retry) ─────────────
        if self.entry_order_id:
            filled = self._verify_order_fill(
                self.entry_order_id, sid, nl * LOT
            )
            if not filled:
                log.error("Entry order NOT filled — resetting state. "
                          "Check Fyers order book manually.")
                self.eL = 0
                self.sid = ""
                return

        self.s.add_trade({
            "date": str(self.today), "action": "ENTRY",
            "direction": d, "opt_type": o, "strike": sk,
            "lots": nl, "prem": round(lp, 2),
            "nifty": round(n, 1), "nifty_sl": round(self.sl, 1),
            "pnl": 0, "capital": round(self.s.capital, 2)
        })

    # ──────────────────────────────────────────────────────────
    #  TRADE MANAGEMENT
    # ──────────────────────────────────────────────────────────

    def _mgr(self, n, ts):
        d = self.d
        if not d: return

        en = self.eN

        if not self.t2 and self.eL > 0:
            if d == "LONG":
                if not self.t1 and n <= self.sl:
                    self._cl1(n, ts, "SL_HIT"); return

                if not self.t1 and n >= en + T1:
                    self.t1 = True
                    self.sl = en
                    log.info(f"T1 HIT (+{T1}pts) — SL moved to breakeven {en:.1f}")
                    self._banner("T1 HIT — SL MOVED TO BREAKEVEN", [
                        f"Time          : {ts.strftime('%H:%M:%S')}",
                        f"Nifty Spot    : {n:.1f}",
                        f"Entry was     : {en:.1f}",
                        f"New SL (BE)   : {en:.1f}",
                        f"T2 target     : {en + T2:.1f}",
                    ])

                if self.t1 and n <= self.sl:
                    self._cl1(n, ts, "BE_EXIT"); return

                if n >= en + T2:
                    self._cl1(n, ts, "T2_HIT")
                    return

            else:  # SHORT
                if not self.t1 and n >= self.sl:
                    self._cl1(n, ts, "SL_HIT"); return

                if not self.t1 and n <= en - T1:
                    self.t1 = True
                    self.sl = en
                    log.info(f"T1 HIT (-{T1}pts) — SL moved to breakeven {en:.1f}")
                    self._banner("T1 HIT — SL MOVED TO BREAKEVEN", [
                        f"Time          : {ts.strftime('%H:%M:%S')}",
                        f"Nifty Spot    : {n:.1f}",
                        f"Entry was     : {en:.1f}",
                        f"New SL (BE)   : {en:.1f}",
                        f"T2 target     : {en - T2:.1f}",
                    ])

                if self.t1 and n >= self.sl:
                    self._cl1(n, ts, "BE_EXIT"); return

                if n <= en - T2:
                    self._cl1(n, ts, "T2_HIT")
                    return

    # ──────────────────────────────────────────────────────────
    #  EXIT HELPERS
    # ──────────────────────────────────────────────────────────

    def _xp(self, n, ts):
        if self.sid:
            p = self.p.opt_ltp_retry(self.sid, retries=3, delay=0.3)
            if p > 0:
                return p
        log.warning(f"opt_ltp unavailable at exit for {self.sid} — "
                    f"using entry premium Rs.{self.eP:.1f} as fallback")
        return self.eP

    def _cl1(self, n, ts, reason):
        xp       = self._xp(n, ts)
        gross_pnl = (xp - self.eP) * self.eL * LOT
        ch  = calc_charges(self.eP, xp, self.eL, LOT)
        pnl = gross_pnl - ch["total"]

        self.s.capital += pnl; self.day_pnl += pnl
        outcome = "WIN ✓" if pnl >= 0 else "LOSS ✗"
        s  = "+" if pnl >= 0 else ""
        sg = "+" if gross_pnl >= 0 else ""

        log.info(f"  {outcome} [{reason}] "
                 f"nifty={n:.1f}  prem=Rs.{xp:.1f}  "
                 f"gross={sg}Rs.{gross_pnl:,.0f}  "
                 f"charges=Rs.{ch['total']:,.0f}  "
                 f"net={s}Rs.{pnl:,.0f}  cap=Rs.{self.s.capital:,.0f}")

        self._banner(f"TRADE EXIT [{reason}] — {outcome}", [
            f"Time          : {ts.strftime('%H:%M:%S')}",
            f"Nifty Spot    : {n:.1f}",
            f"Entry Prem    : Rs.{self.eP:.2f}",
            f"Exit Prem     : Rs.{xp:.2f}",
            f"Lots          : {self.eL}  ({self.eL * LOT} qty)",
            f"─" * 48,
            f"Gross P&L     : {sg}Rs.{gross_pnl:,.0f}",
            f"─" * 48,
            f"  Brokerage   : Rs.{ch['brokerage']:.2f}",
            f"  STT (0.15%) : Rs.{ch['stt']:.2f}",
            f"  NSE Txn     : Rs.{ch['exchange']:.2f}",
            f"  SEBI Fee    : Rs.{ch['sebi']:.2f}",
            f"  Stamp Duty  : Rs.{ch['stamp']:.2f}",
            f"  GST (18%)   : Rs.{ch['gst']:.2f}",
            f"  Total Chrgs : Rs.{ch['total']:.2f}",
            f"─" * 48,
            f"Net P&L       : {s}Rs.{pnl:,.0f}",
            f"Capital       : Rs.{self.s.capital:,.0f}",
            f"Day P&L       : {'+' if self.day_pnl >= 0 else ''}Rs.{self.day_pnl:,.0f}",
        ])

        # ── Place exit order (market) ──────────────────────────
        if self.eL > 0 and not self._no_capital:
            exit_resp = self.p.place_order({
                "symbol":      self.sid,
                "qty":         self.eL * LOT,
                "type":        2,    # Market
                "side":        -1,   # Sell
                "productType": "INTRADAY",
            })
            exit_id = exit_resp.get("id", "")
            if exit_id:
                filled = self._verify_order_fill(exit_id, self.sid, self.eL * LOT)
                if not filled:
                    log.error("EXIT NOT FILLED — triggering emergency exit")
                    self.p.exit_all_positions()

        self.s.add_trade({
            "date": str(self.today), "action": f"LOT1_EXIT_{reason}",
            "lots": self.eL, "entry_prem": round(self.eP, 2),
            "exit_prem": round(xp, 2), "nifty": round(n, 1),
            "gross_pnl":  round(gross_pnl, 2),
            "charges":    ch,
            "pnl": round(pnl, 2), "capital": round(self.s.capital, 2)
        })
        self.eL = 0
        if reason == "T2_HIT": self.t2 = True

    def _force(self, n=None):
        if n is None or n <= 0:
            n = self.p.nifty() or self.eN
        ts = datetime.now(IST).replace(tzinfo=None)
        if self.eL > 0: self._cl1(n, ts, "EOD")

    def _eod(self):
        s = "+" if self.day_pnl >= 0 else ""
        log.info(f"\nDay complete: {s}Rs.{self.day_pnl:,.0f}  "
                 f"cap=Rs.{self.s.capital:,.0f}")
        self.s.add_daily({
            "date":    str(self.today),
            "pnl":     round(self.day_pnl, 2),
            "capital": round(self.s.capital, 2),
            "signal":  self.fired
        })
        self.s.summary()


# ═══════════════════════════════════════════════════════════════
#  FYERS HEADLESS AUTH — fully automatic, no browser needed
#
#  Flow: generate auth URL → login via API with TOTP → extract
#        auth_code → exchange for access_token.
#  Runs unattended on cloud server via cron every morning.
#
#  ONE-TIME SETUP:
#    1. Go to Fyers → My Account → Security → Enable TOTP
#    2. When shown the QR code, click "Can't scan?" to see the
#       base32 secret key (looks like ABCDEFGHIJ234567)
#    3. Paste that key as TOTP_KEY above
#    4. pip install pyotp requests
# ═══════════════════════════════════════════════════════════════

def fyers_auto_auth(app_id: str, secret_key: str, redirect_uri: str,
                    fy_id: str, pin: str, totp_key: str) -> str:
    """
    Fully headless Fyers authentication. No browser, no input().
    Uses legacy vagator endpoints to bypass AES encryption requirements.
    """
    from fyers_apiv3 import fyersModel
    import requests
    import pyotp
    import sys

    req_session = requests.Session()
    req_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json"
    })

    log.info("Starting headless Fyers authentication ...")

    # ── Step 1: Create session
    fyers_sess = fyersModel.SessionModel(
        client_id=app_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type="code",
        grant_type="authorization_code"
    )

    # ── Step 2: Send login OTP request (Legacy Endpoint: NO _v2)
    log.info("  Step 1/4: Requesting login OTP ...")
    r1 = req_session.post(
        "https://api-t2.fyers.in/vagator/v2/send_login_otp",
        json={"fy_id": fy_id, "app_id": "2"}
    )
    if r1.status_code != 200:
        log.error(f"  OTP request failed: HTTP {r1.status_code} - {r1.text}")
        sys.exit(1)
    request_key = r1.json().get("request_key", "")
    if not request_key:
        log.error(f"  OTP request failed: {r1.json()}")
        sys.exit(1)
    log.info("  Step 1/4: OTP requested ✓")

    # ── Step 3: Verify with TOTP (Legacy Endpoint: NO _v2)
    log.info("  Step 2/4: Verifying TOTP ...")
    totp = pyotp.TOTP(totp_key).now()
    r2 = req_session.post(
        "https://api-t2.fyers.in/vagator/v2/verify_otp",
        json={"request_key": request_key, "otp": totp}
    )
    if r2.status_code != 200:
        log.error(f"  TOTP verify failed: HTTP {r2.status_code} - {r2.text}")
        sys.exit(1)
    request_key2 = r2.json().get("request_key", "")
    if not request_key2:
        log.error(f"  TOTP verify failed: {r2.json()}")
        sys.exit(1)
    log.info("  Step 2/4: TOTP verified ✓")

    # ── Step 4: Verify PIN (Legacy Endpoint: NO _v2)
    log.info("  Step 3/4: Verifying PIN ...")
    r3 = req_session.post(
        "https://api-t2.fyers.in/vagator/v2/verify_pin",
        json={
            "request_key":   request_key2,
            "identity_type": "pin",
            "identifier":    str(pin)
        }
    )
    if r3.status_code != 200:
        log.error(f"  PIN verify failed: HTTP {r3.status_code} - {r3.text}")
        sys.exit(1)
    r3_data = r3.json().get("data", {})
    access_token_temp = r3_data.get("access_token", "")
    if not access_token_temp:
        log.error(f"  PIN verify failed: {r3.json()}")
        sys.exit(1)
    log.info("  Step 3/4: PIN verified ✓")

    # ── Step 5: Get auth_code
    log.info("  Step 4/4: Exchanging for auth code ...")
    step4_headers = req_session.headers.copy()
    step4_headers["Authorization"] = f"Bearer {access_token_temp}"
    
    r4 = req_session.post(
        "https://api-t1.fyers.in/api/v3/token",
        json={
            "fyers_id":       fy_id,
            "app_id":         app_id.split("-")[0],
            "redirect_uri":   redirect_uri,
            "appType":        "200",
            "code_challenge":  "",
            "state":          "None",
            "scope":          "",
            "nonce":          "",
            "response_type":  "code",
            "create_cookie":  True
        },
        headers=step4_headers
    )
    if r4.status_code != 308:
        pass
    r4_json = r4.json()
    auth_url = r4_json.get("Url", "") or r4_json.get("url", "")
    if not auth_url:
        log.error(f"  Auth code exchange failed: {r4_json}")
        sys.exit(1)

    from urllib.parse import urlparse, parse_qs
    auth_code = parse_qs(urlparse(auth_url).query).get("auth_code", [None])[0]
    if not auth_code:
        log.error(f"  Could not extract auth_code from URL: {auth_url}")
        sys.exit(1)
    log.info("  Step 4/4: Auth code obtained ✓")

    # ── Step 6: Generate final access token
    fyers_sess.set_token(auth_code)
    resp = fyers_sess.generate_token()
    if resp.get("s") != "ok":
        log.error(f"  Token generation failed: {resp}")
        sys.exit(1)

    log.info("Fyers access token obtained ✓ (headless)")
    return resp["access_token"]


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT — cloud-ready, no human interaction needed
# ═══════════════════════════════════════════════════════════════
#
#  CRON SETUP (run every weekday at 9:10 AM IST):
#    10 9 * * 1-5 cd /home/ubuntu/orb && /usr/bin/python3 live_cloud.py >> logs/cron.log 2>&1
#
#  Or run manually via SSH:
#    nohup python3 live_cloud.py > logs/nohup_$(date +\%F).log 2>&1 &
#
#  FIRST-TIME SETUP:
#    pip install fyers-apiv3 pyotp requests pytz
#    Fill in FY_ID, PIN, TOTP_KEY at the top of this file
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print(f"  NIFTY ORB BOT v9L  —  LIVE TRADING (CLOUD)")
    print(f"  DRY_RUN: {DRY_RUN}")
    print(f"  v9: ITM strike (offset={ITM_OFFSET}), OR filter (>={MIN_OR_RANGE}), "
          f"Min-1 breakout (deadline={BREAKOUT_DEADLINE[0]}:{BREAKOUT_DEADLINE[1]:02d})")
    print(f"  Auth: Headless (TOTP)")
    print("=" * 60)

    # ── Validate credentials before anything else ──────────────
    if not FY_ID or not PIN or not TOTP_KEY:
        print("\n  *** ERROR: Fill in FY_ID, PIN, and TOTP_KEY at the top ***")
        print("  These are required for headless auth on cloud server.")
        print("  FY_ID   = Your Fyers client ID (e.g. 'XA12345')")
        print("  PIN     = Your 4-digit Fyers login PIN")
        print("  TOTP_KEY= Base32 secret from Fyers 2FA setup")
        sys.exit(1)

    # ── Skip weekends ──────────────────────────────────────────
    if date.today().weekday() > 4:
        print(f"  Weekend ({date.today().strftime('%A')}) — no trading.")
        sys.exit(0)

    if DRY_RUN:
        print("\n  DRY RUN — no real orders. All signal logic will execute.\n")

    # ── Headless auth (no browser, no input) ───────────────────
    token  = fyers_auto_auth(APP_ID, SECRET_KEY, REDIRECT_URI,
                             FY_ID, PIN, TOTP_KEY)
    state  = State()
    poller = FyersClient(APP_ID, token)
    bot    = Bot(poller, state)
    bot.run()
