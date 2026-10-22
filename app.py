"""Read-only Betfair bet monitor.

A background thread polls the Betfair API and caches a snapshot; the web page
reads that snapshot from /api/data, so page refreshes never hit Betfair.

Only read calls are made (account funds, current orders, cleared orders, market
catalogue). Nothing here can place, cancel or change a bet.
"""
from __future__ import annotations

import csv
import logging
import os
import re
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

HERE = Path(__file__).parent
TZ = ZoneInfo(os.getenv("DASH_TZ", "Australia/Sydney"))
HISTORY_DAYS = min(int(os.getenv("DASH_HISTORY_DAYS", "90")), 90)  # Betfair keeps 90 days
CURRENT_POLL_S = int(os.getenv("DASH_CURRENT_POLL_S", "15"))       # funds + open orders
CLEARED_POLL_S = int(os.getenv("DASH_CLEARED_POLL_S", "120"))      # newly settled bets
FULL_RELOAD_S = 6 * 3600   # re-pull the whole window now and then, to pick up corrections
KEEP_ALIVE_S = 15 * 60
RETENTION = timedelta(days=90)

# ── NSW thoroughbred turnover charge (estimate) ─────────────────────────────────
# Same rule as thoroughbred-model/scripts/price_movement_model.py. Per Mon–Sun GMT
# week, on NSW thoroughbred markets only, charge 3% of NSW back turnover when that
# turnover is >= $1,000 AND (commission paid + implied commission) / 2 is under
# 1.25% of it; implied commission = 3% of each losing market's net loss.
NSW_RATE = 0.03
NSW_IMPLIED_RATE = 0.03
NSW_GENERATED_MIN = 0.0125
NSW_MIN_TURNOVER = 1000.0
HORSE_RACING = "7"
HARNESS = re.compile(r"\b(pace|trot)\b", re.I)
AU_EVENT = re.compile(r"^(.*?)\s*\(AUS\)", re.I)


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.casefold())


def _load_venue_states() -> dict[str, str]:
    path = Path(os.getenv("DASH_VENUE_STATES", HERE / "venue_states.csv"))
    with open(path, newline="", encoding="utf-8") as f:
        return {_norm(r["venue"]): r["state"].upper() for r in csv.DictReader(f)}


def _utc(dt: datetime | None) -> datetime | None:
    # betfairlightweight returns naive UTC datetimes
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _iso(dt: datetime | None) -> str | None:
    dt = _utc(dt)
    return dt.astimezone(TZ).isoformat() if dt else None


def _week(dt: datetime) -> str:
    """Monday (GMT) of the billing week containing dt."""
    d = dt.astimezone(timezone.utc).date()
    return (d - timedelta(days=d.weekday())).isoformat()


class Poller:
    def __init__(self) -> None:
        app_key = os.getenv("BF_APP_KEY") or os.getenv("BF_DELAYED_APP_KEY")
        user, pw = os.getenv("BF_USERNAME"), os.getenv("BF_PASSWORD")
        if not (user and pw and app_key):
            raise SystemExit("Set BF_USERNAME, BF_PASSWORD and BF_APP_KEY (or BF_DELAYED_APP_KEY).")
        self.trading = bflw.APIClient(user, pw, app_key=app_key)
        self.venue_states = _load_venue_states()
        self.lock = threading.Lock()
        self.snapshot: dict = {"status": "starting"}
        self.bets: list[dict] = []              # settled bets, newest first (served by /api/bets)
        self._catalogue: dict[str, dict] = {}   # market_id -> names, cached forever
        self._bets: dict[str, dict] = {}        # bet_id -> settled bet
        self._markets: dict[str, dict] = {}     # market_id -> market-level settlement
        self._cleared: dict = {}
        self._last_cleared = 0.0
        self._last_full = 0.0
        self._last_keep_alive = 0.0

    # --- session -----------------------------------------------------------
    def _ensure_session(self) -> None:
        now = time.time()
        if not self.trading.session_token:
            self.trading.login_interactive()
            self._last_keep_alive = now
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
                    "venue": c.event.venue if c.event else None,
                    "market": c.market_name,
                    "start": _iso(c.market_start_time),
                    "runners": {r.selection_id: r.runner_name for r in c.runners},
                }

    def _cleared_orders(self, group_by: str, since: datetime) -> list:
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
        rows.sort(key=lambda r: r["start"] or "")
        return rows

    def _state(self, market_id: str, event_desc: str) -> str | None:
        """AU state of a market's venue, 'UNKNOWN' for an unmapped AU venue, None if not AU."""
        venue = self._catalogue.get(market_id, {}).get("venue")
        m = AU_EVENT.match(event_desc or "")
        if not m:
            return None
        return self.venue_states.get(_norm(venue or m.group(1)), "UNKNOWN")

    def _is_nsw_thoroughbred(self, mkt: dict) -> bool:
        if mkt["event_type_id"] != HORSE_RACING or HARNESS.search(mkt["market"] or ""):
            return False
        # Unmapped AU venues count as NSW, as the backtests do: overstating beats missing it.
        return mkt["state"] in ("NSW", "UNKNOWN")

    def _update_cleared(self) -> None:
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(days=HISTORY_DAYS)
        # start on the Monday so the first billing week is complete
        keep_from = datetime.fromisoformat(_week(window_start)).replace(tzinfo=timezone.utc)
        fetch_from = keep_from
        full = time.time() - self._last_full > FULL_RELOAD_S or not self._markets
        if not full:
            latest = max(m["settled_utc"] for m in self._markets.values())
            fetch_from = max(keep_from, latest - timedelta(hours=2))

        for b in self._cleared_orders("BET", fetch_from):
            d = b.item_description
            self._bets[b.bet_id] = {
                "bet_id": b.bet_id,
                "market_id": b.market_id,
                "settled_utc": _utc(b.settled_date),
                "settled": _iso(b.settled_date),
                "event": d.event_desc if d else "",
                "market": d.market_desc if d else b.market_id,
                "runner": d.runner_desc if d else str(b.selection_id),
                "side": b.side,
                "order_type": b.order_type,
                "price": b.price_matched,
                "size": b.size_settled,
                "outcome": b.bet_outcome,
                "profit": b.profit,
                "strategy": b.customer_strategy_ref,
            }
        for m in self._cleared_orders("MARKET", fetch_from):
            d = m.item_description
            event = d.event_desc if d else ""
            self._markets[m.market_id] = {
                "settled_utc": _utc(m.settled_date),
                "profit": m.profit or 0.0,
                "commission": m.commission or 0.0,
                "event_type_id": str(m.event_type_id),
                "event": event,
                "market": d.market_desc if d else "",
                "state": self._state(m.market_id, event),
            }

        # drop anything that has aged out of the window
        self._bets = {k: v for k, v in self._bets.items() if v["settled_utc"] >= keep_from}
        self._markets = {k: v for k, v in self._markets.items() if v["settled_utc"] >= keep_from}
        if full:
            self._last_full = time.time()
        self._cleared = self._shape_cleared(now)

    def _shape_cleared(self, now: datetime) -> dict:
        markets, bets = self._markets, list(self._bets.values())
        nsw = {k for k, m in markets.items() if self._is_nsw_thoroughbred(m)}

        # weekly NSW charge
        weeks: dict[str, dict] = defaultdict(lambda: {"turnover": 0.0, "paid": 0.0, "implied": 0.0, "markets": 0})
        for k in nsw:
            m = markets[k]
            w = weeks[_week(m["settled_utc"])]
            w["paid"] += m["commission"]
            w["implied"] += max(0.0, -m["profit"]) * NSW_IMPLIED_RATE
            w["markets"] += 1
        for b in bets:
            if b["market_id"] in nsw and b["side"] == "BACK":
                weeks[_week(b["settled_utc"])]["turnover"] += b["size"] or 0.0
        current_week = _week(now)
        week_rows = []
        for wk, w in sorted(weeks.items()):
            generated = (w["paid"] + w["implied"]) / 2
            pct = generated / w["turnover"] if w["turnover"] else None
            charged = w["turnover"] >= NSW_MIN_TURNOVER and pct is not None and pct < NSW_GENERATED_MIN
            week_rows.append({
                "week": wk, **w, "generated": generated, "generated_pct": pct,
                "charged": charged, "charge": w["turnover"] * NSW_RATE if charged else 0.0,
                "in_progress": wk == current_week,
                # the first week can start before Betfair's 90-day retention
                "partial": datetime.fromisoformat(wk).replace(tzinfo=timezone.utc) < now - RETENTION,
            })
        charged_weeks = {w["week"] for w in week_rows if w["charged"]}

        def nsw_charge(b: dict) -> float:
            if b["market_id"] in nsw and b["side"] == "BACK" and _week(b["settled_utc"]) in charged_weeks:
                return (b["size"] or 0.0) * NSW_RATE
            return 0.0

        # daily P&L: market profit - commission - NSW charge (allocated by back stake)
        daily: dict[str, dict] = defaultdict(lambda: {"gross": 0.0, "commission": 0.0, "nsw_charge": 0.0, "markets": 0})
        for m in markets.values():
            day = daily[m["settled_utc"].astimezone(TZ).date().isoformat()]
            day["gross"] += m["profit"]
            day["commission"] += m["commission"]
            day["markets"] += 1
        for b in bets:
            if c := nsw_charge(b):
                daily[b["settled_utc"].astimezone(TZ).date().isoformat()]["nsw_charge"] += c
        days = [{"date": k, **v, "net": v["gross"] - v["commission"] - v["nsw_charge"]}
                for k, v in sorted(daily.items())]

        by_strategy: dict[str, dict] = defaultdict(
            lambda: {"bets": 0, "won": 0, "staked": 0.0, "profit": 0.0, "nsw_charge": 0.0})
        for b in bets:
            s = by_strategy[b["strategy"] or "(none)"]
            s["bets"] += 1
            s["won"] += b["outcome"] == "WON"
            s["staked"] += b["size"] or 0.0
            s["profit"] += b["profit"] or 0.0
            s["nsw_charge"] += nsw_charge(b)
        strategies = [{"strategy": k, **v} for k, v in sorted(by_strategy.items())]

        unknown = sorted({AU_EVENT.match(m["event"]).group(1) for k, m in markets.items()
                          if k in nsw and m["state"] == "UNKNOWN"})
        self.bets = sorted(
            ({k: v for k, v in b.items() if k not in ("settled_utc", "market_id")} for b in bets),
            key=lambda r: r["settled"] or "", reverse=True)
        won = sum(b["outcome"] == "WON" for b in bets)
        return {
            "since": days[0]["date"] if days else None,
            "daily": days,
            "strategies": strategies,
            "nsw": {"weeks": week_rows, "unknown_venues": unknown},
            "bet_count": len(bets),
            "won_count": won,
        }

    # --- loop --------------------------------------------------------------
    def refresh(self) -> None:
        self._ensure_session()
        funds = self._funds()
        current = self._shape_current(self._current_orders())
        if time.time() - self._last_cleared > CLEARED_POLL_S:
            self._update_cleared()
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
    return FileResponse(HERE / "static" / "index.html")


@app.get("/api/data")
def data():
    with poller.lock:
        return JSONResponse(poller.snapshot)


@app.get("/api/bets")
def bets(q: str = "", side: str = "", limit: int = 500):
    q = q.strip().lower()
    rows = [b for b in poller.bets
            if (not side or b["side"] == side)
            and (not q or any(q in str(b[k] or "").lower() for k in ("runner", "event", "market", "strategy")))]
    return {"count": len(rows), "profit": sum(b["profit"] or 0.0 for b in rows), "bets": rows[:limit]}


@app.get("/healthz")
def healthz():
    status = poller.snapshot.get("status")
    return JSONResponse({"status": status}, status_code=503 if status == "error" else 200)
