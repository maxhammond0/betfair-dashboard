"""Read-only Betfair bet monitor.

A background thread polls the Betfair API and caches a snapshot; the web page
reads that snapshot from /api/data, so page refreshes never hit Betfair.

Only read calls are made (account funds, current orders, cleared orders, market
catalogue). Nothing here can place, cancel or change a bet.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import betfairlightweight as bflw
from betfairlightweight.filters import market_filter, time_range
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

log = logging.getLogger("dashboard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("betfairlightweight").setLevel(logging.WARNING)

TZ = ZoneInfo(os.getenv("DASH_TZ", "Australia/Sydney"))
HISTORY_DAYS = int(os.getenv("DASH_HISTORY_DAYS", "30"))      # Betfair keeps 90 days
CURRENT_POLL_S = int(os.getenv("DASH_CURRENT_POLL_S", "15"))   # funds + open orders
CLEARED_POLL_S = int(os.getenv("DASH_CLEARED_POLL_S", "120"))  # settled bets
KEEP_ALIVE_S = 15 * 60
STATIC = Path(__file__).parent / "static"


def _utc(dt: datetime | None) -> datetime | None:
    # betfairlightweight returns naive UTC datetimes
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _iso(dt: datetime | None) -> str | None:
    dt = _utc(dt)
    return dt.astimezone(TZ).isoformat() if dt else None


class Poller:
    def __init__(self) -> None:
        app_key = os.getenv("BF_APP_KEY") or os.getenv("BF_DELAYED_APP_KEY")
        user, pw = os.getenv("BF_USERNAME"), os.getenv("BF_PASSWORD")
        if not (user and pw and app_key):
            raise SystemExit("Set BF_USERNAME, BF_PASSWORD and BF_APP_KEY (or BF_DELAYED_APP_KEY).")
        self.trading = bflw.APIClient(user, pw, app_key=app_key)
        self.lock = threading.Lock()
        self.snapshot: dict = {"status": "starting"}
        self._catalogue: dict[str, dict] = {}   # market_id -> names, cached forever
        self._cleared: dict = {}
        self._last_cleared = 0.0
        self._last_login = 0.0
        self._last_keep_alive = 0.0

    # --- session -----------------------------------------------------------
    def _ensure_session(self) -> None:
        now = time.time()
        if not self.trading.session_token:
            self.trading.login_interactive()
            self._last_login = self._last_keep_alive = now
            log.info("Logged in to Betfair")
        elif now - self._last_keep_alive > KEEP_ALIVE_S:
            try:
                self.trading.keep_alive()
            except Exception:
                log.warning("keep_alive failed, logging in again")
                self.trading.login_interactive()
            self._last_keep_alive = now

    # --- fetchers ----------------------------------------------------------
    def _funds(self) -> dict:
        f = self.trading.account.get_account_funds()
        return {
            "available": f.available_to_bet_balance,
            "exposure": f.exposure,
            "retained_commission": f.retained_commission,
            "exposure_limit": f.exposure_limit,
        }

    def _current_orders(self) -> list:
        orders, start = [], 0
        while True:
            res = self.trading.betting.list_current_orders(from_record=start, record_count=1000)
            orders += res.orders
            if not res.more_available:
                return orders
            start += len(res.orders)

    def _names(self, market_ids: set[str]) -> None:
        missing = [m for m in market_ids if m not in self._catalogue]
        for i in range(0, len(missing), 50):
            chunk = missing[i:i + 50]
            cats = self.trading.betting.list_market_catalogue(
                filter=market_filter(market_ids=chunk),
                market_projection=["EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION"],
                max_results=len(chunk),
            )
            for c in cats:
                self._catalogue[c.market_id] = {
                    "event": c.event.name if c.event else "",
                    "market": c.market_name,
                    "start": _iso(c.market_start_time),
                    "runners": {r.selection_id: r.runner_name for r in c.runners},
                }

    def _cleared_orders(self, group_by: str) -> list:
        since = datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS)
        orders, start = [], 0
        while True:
            res = self.trading.betting.list_cleared_orders(
                bet_status="SETTLED",
                settled_date_range=time_range(from_=since),
                group_by=group_by,
                include_item_description=True,
                from_record=start,
                record_count=1000,
            )
            orders += res.orders
            if not res.more_available:
                return orders
            start += len(res.orders)

    # --- shaping -----------------------------------------------------------
    def _shape_current(self, orders: list) -> list[dict]:
        self._names({o.market_id for o in orders})
        rows = []
        for o in orders:
            cat = self._catalogue.get(o.market_id, {})
            price = o.price_size.price if o.price_size else None
            size = o.price_size.size if o.price_size else None
            rows.append({
                "bet_id": o.bet_id,
                "placed": _iso(o.placed_date),
                "event": cat.get("event", ""),
                "market": cat.get("market", o.market_id),
                "start": cat.get("start"),
                "runner": cat.get("runners", {}).get(o.selection_id, str(o.selection_id)),
                "side": o.side,
                "order_type": o.order_type,
                "persistence": o.persistence_type,
                "status": o.status,
                "price": price,
                "size": size,
                "bsp_liability": o.bsp_liability,
                "avg_matched": o.average_price_matched,
                "matched": o.size_matched,
                "remaining": o.size_remaining,
                "strategy": o.customer_strategy_ref,
            })
        rows.sort(key=lambda r: r["start"] or "", reverse=False)
        return rows

    def _shape_cleared(self, bets: list, markets: list) -> dict:
        bet_rows = []
        for b in bets:
            d = b.item_description
            bet_rows.append({
                "bet_id": b.bet_id,
                "settled": _iso(b.settled_date),
                "placed": _iso(b.placed_date),
                "event": d.event_desc if d else "",
                "market": d.market_desc if d else b.market_id,
                "runner": d.runner_desc if d else str(b.selection_id),
                "side": b.side,
                "order_type": b.order_type,
                "price": b.price_matched,
                "requested": b.price_requested,
                "size": b.size_settled,
                "outcome": b.bet_outcome,
                "profit": b.profit,
                "strategy": b.customer_strategy_ref,
            })
        bet_rows.sort(key=lambda r: r["settled"] or "", reverse=True)

        # Daily P&L from market-level rows, which carry the commission paid.
        daily: dict[str, dict] = defaultdict(lambda: {"gross": 0.0, "commission": 0.0, "markets": 0})
        for m in markets:
            day = _utc(m.settled_date).astimezone(TZ).date().isoformat()
            daily[day]["gross"] += m.profit or 0.0
            daily[day]["commission"] += m.commission or 0.0
            daily[day]["markets"] += 1
        days = [{"date": k, **v, "net": v["gross"] - v["commission"]} for k, v in sorted(daily.items())]

        by_strategy: dict[str, dict] = defaultdict(lambda: {"bets": 0, "won": 0, "staked": 0.0, "profit": 0.0})
        for r in bet_rows:
            s = by_strategy[r["strategy"] or "(none)"]
            s["bets"] += 1
            s["won"] += r["outcome"] == "WON"
            s["staked"] += r["size"] or 0.0
            s["profit"] += r["profit"] or 0.0
        strategies = [{"strategy": k, **v} for k, v in sorted(by_strategy.items())]

        return {"bets": bet_rows, "daily": days, "strategies": strategies}

    # --- loop --------------------------------------------------------------
    def refresh(self) -> None:
        self._ensure_session()
        funds = self._funds()
        current = self._shape_current(self._current_orders())
        if time.time() - self._last_cleared > CLEARED_POLL_S:
            self._cleared = self._shape_cleared(
                self._cleared_orders("BET"), self._cleared_orders("MARKET"))
            self._last_cleared = time.time()
        with self.lock:
            self.snapshot = {
                "status": "ok",
                "updated": datetime.now(TZ).isoformat(),
                "history_days": HISTORY_DAYS,
                "funds": funds,
                "current": current,
                **self._cleared,
            }

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                self.refresh()
            except Exception as e:
                log.exception("refresh failed")
                self.trading.session_token = None   # force a fresh login next time
                with self.lock:
                    self.snapshot = {**self.snapshot, "status": "error", "error": str(e)}
            stop.wait(CURRENT_POLL_S)


poller: Poller | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global poller
    poller = Poller()
    stop = threading.Event()
    t = threading.Thread(target=poller.run, args=(stop,), daemon=True)
    t.start()
    yield
    stop.set()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/data")
def data():
    with poller.lock:
        return JSONResponse(poller.snapshot)


@app.get("/healthz")
def healthz():
    return {"status": poller.snapshot.get("status")}
