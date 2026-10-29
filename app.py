"""Read-only Betfair bet monitor.

A background thread polls the Betfair API and caches a snapshot; the web page
reads that snapshot from /api/data, so page refreshes never hit Betfair.
Settled bets are kept in a SQLite file, so a restart only fetches what settled
since the last poll instead of the whole history.

Only read calls are made (account funds, current orders, cleared orders, market
catalogue). Nothing here can place, cancel or change a bet.
"""
from __future__ import annotations

import csv
import logging
import os
import re
import sqlite3
import threading
import time
from bisect import bisect_right
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import betfairlightweight as bflw
from betfairlightweight.filters import market_filter, time_range
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse

log = logging.getLogger("dashboard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("betfairlightweight").setLevel(logging.WARNING)

HERE = Path(__file__).parent
TZ = ZoneInfo(os.getenv("DASH_TZ", "Australia/Sydney"))
CURRENT_POLL_S = int(os.getenv("DASH_CURRENT_POLL_S", "15"))       # funds + open orders
CLEARED_POLL_S = int(os.getenv("DASH_CLEARED_POLL_S", "120"))      # newly settled bets
DB_PATH = Path(os.getenv("DASH_DB", HERE / "data" / "dashboard.db"))
RECHECK_S = 6 * 3600              # now and then re-pull recent days, to pick up corrections
RECHECK_SPAN = timedelta(days=3)
KEEP_ALIVE_S = 15 * 60
RETENTION = timedelta(days=90)   # how far back Betfair keeps settled bets

# ── Racing NSW turnover charge (estimate) ───────────────────────────────────────
# Betfair Charges §9.3. Per Mon–Sun GMT week, on Racing NSW turnover charge markets,
# charge 3% of back turnover when that turnover is >= $1,000 AND (commission paid +
# implied commission) / 2 is under 1.25% of it; implied commission = 3% of each
# losing market's net loss.
NSW_RATE = 0.03
NSW_IMPLIED_RATE = 0.03
NSW_GENERATED_MIN = 0.0125
NSW_MIN_TURNOVER = 1000.0
HORSE_RACING = "7"
# Betfair marks charged markets in their rules text. It can only be read while a market
# is open, so upcoming AU horse markets are scanned and the flag recorded.
NSW_FLAG = "Racing NSW Turnover Charge"
SCAN_S = 30 * 60
# price bands for the P&L-by-odds chart: upper edges, the last band is open
ODDS_EDGES = (1.5, 2.0, 3.0, 5.0, 10.0, 20.0)
ODDS_LABELS = ("<1.5", "1.5–2", "2–3", "3–5", "5–10", "10–20", "20+")
SCAN_AHEAD = timedelta(hours=24)
# Fallback for markets settled without a recorded flag: settled orders describe the event
# only as "12:35 Ballarat", so look the venue up in venue_states.csv, and require an AU
# racing start time (21:00–11:00 UTC) to tell e.g. NSW Newcastle from UK Newcastle. The
# "12:35" is the start in the account's timezone (DASH_TZ), used when the start time is missing.
EVENT_TIME = re.compile(r"^(\d{1,2}):(\d{2})\s+")
AU_HOURS_UTC = (21, 11)
# The charge actually debited: an account statement item like "AUS Turnover Chg ADJ77 - wk
# start 12-Oct-26", posted a day or two after the week. It only comes with include_item=ALL,
# among every bet settlement, so just the few days after each week are read.
CHARGE_TEXT = "Turnover Chg"
CHARGE_WEEK = re.compile(r"wk start (\d{1,2}-[A-Za-z]{3}-\d{2})")
CHARGE_WINDOW = timedelta(days=5)
CHARGE_CHECK_S = 3600


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


SCHEMA = """
CREATE TABLE IF NOT EXISTS bets (
    bet_id TEXT PRIMARY KEY, market_id TEXT NOT NULL, settled_utc TEXT NOT NULL,
    event TEXT, market TEXT, runner TEXT, side TEXT, order_type TEXT,
    price REAL, size REAL, outcome TEXT, profit REAL, strategy TEXT
);
CREATE INDEX IF NOT EXISTS bets_settled ON bets (settled_utc);
CREATE TABLE IF NOT EXISTS markets (
    market_id TEXT PRIMARY KEY, settled_utc TEXT NOT NULL, profit REAL, commission REAL,
    event_type_id TEXT, event TEXT, market TEXT, state TEXT, venue TEXT, start_utc TEXT
);
CREATE INDEX IF NOT EXISTS markets_settled ON markets (settled_utc);
CREATE TABLE IF NOT EXISTS market_info (
    market_id TEXT PRIMARY KEY, venue TEXT, country TEXT, race_type TEXT, rnsw INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transfers (
    ref_id TEXT PRIMARY KEY, item_utc TEXT NOT NULL, amount REAL NOT NULL, description TEXT
);
-- NSW turnover charge debited per billing week; amount 0 with no item = checked, none posted
CREATE TABLE IF NOT EXISTS nsw_charges (
    week TEXT PRIMARY KEY, item_utc TEXT, amount REAL NOT NULL, description TEXT
);
"""
BET_COLS = ("bet_id", "market_id", "settled_utc", "event", "market", "runner", "side",
            "order_type", "price", "size", "outcome", "profit", "strategy")
MARKET_COLS = ("market_id", "settled_utc", "profit", "commission", "event_type_id",
               "event", "market", "state", "venue", "start_utc")
INFO_COLS = ("market_id", "venue", "country", "race_type", "rnsw")
TIME_COLS = ("settled_utc", "start_utc")


def _per_market(bets: list[dict]) -> dict[str, dict[str, list]]:
    """market_id -> strategy -> [profit, stake]"""
    per: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
    for b in bets:
        pm = per[b["market_id"]][b["strategy"]]
        pm[0] += b["profit"] or 0.0
        pm[1] += b["size"] or 0.0
    return per


def _commission_weights(per: dict[str, list]) -> dict[str, float]:
    # commission is charged on a market's net winnings: split it between the strategies that
    # won in that market, by how much each won (by stake if none did)
    return {s: p for s, (p, _) in per.items() if p > 0} or {s: st for s, (_, st) in per.items()}


def _only(bets: list[dict], markets: dict[str, dict], keep) -> tuple[list[dict], dict[str, dict]]:
    """Keep the bets whose strategy passes keep(), and cut each market's profit and commission
    down to those bets' share, split as in the strategy table. Profit from listClearedOrders is
    gross at both BET and MARKET level; commission is separate."""
    per = _per_market(bets)
    kept = {}
    for k, by_s in per.items():
        mine = {s: v for s, v in by_s.items() if keep(s)}
        if k not in markets or not mine:
            continue
        m = markets[k]
        if len(mine) < len(by_s):
            weights = _commission_weights(by_s)
            share = sum(w for s, w in weights.items() if keep(s)) / (sum(weights.values()) or 1.0)
            m = {**m, "profit": sum(p for p, _ in mine.values()), "commission": m["commission"] * share}
        kept[k] = m
    return [b for b in bets if keep(b["strategy"])], kept


def _strategy_only(bets: list[dict], markets: dict[str, dict]) -> tuple[list[dict], dict[str, dict]]:
    """Drop bets without a customer_strategy_ref (manual bets, other tools), so every figure
    covers the strategies only."""
    return _only(bets, markets, bool)


class Store:
    """Settled bets and markets on disk. Rows are never pruned, so history older
    than Betfair's 90-day retention survives here."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)  # only the poller thread uses it
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        cols = {r["name"] for r in self.db.execute("PRAGMA table_info(markets)")}
        with self.db:
            if "venue" not in cols:
                self.db.execute("ALTER TABLE markets ADD COLUMN venue TEXT")
            if "start_utc" not in cols:
                self.db.execute("ALTER TABLE markets ADD COLUMN start_utc TEXT")
                # re-fetch the 90 days Betfair has, to fill in start times
                self.db.execute("DELETE FROM meta WHERE key = 'covered_from'")

    def _load(self, table: str) -> list[dict]:
        rows = self.db.execute(f"SELECT * FROM {table}")
        return [{**r, **{c: datetime.fromisoformat(r[c]) for c in TIME_COLS if r.get(c)}} for r in map(dict, rows)]

    def bets(self) -> dict[str, dict]:
        return {b["bet_id"]: {**b, "settled": _iso(b["settled_utc"])} for b in self._load("bets")}

    def markets(self) -> dict[str, dict]:
        return {m.pop("market_id"): m for m in self._load("markets")}

    def market_info(self) -> dict[str, dict]:
        return {i.pop("market_id"): i for i in map(dict, self.db.execute("SELECT * FROM market_info"))}

    def transfers(self) -> list[dict]:
        rows = map(dict, self.db.execute("SELECT * FROM transfers"))
        return [{**r, "item_utc": datetime.fromisoformat(r["item_utc"])} for r in rows]

    def save_transfers(self, rows: list[dict]) -> None:
        with self.db:
            self.db.executemany("INSERT OR REPLACE INTO transfers VALUES (?, ?, ?, ?)",
                                [(r["ref_id"], r["item_utc"].isoformat(), r["amount"], r["description"]) for r in rows])

    def charges(self) -> dict[str, dict]:
        rows = map(dict, self.db.execute("SELECT * FROM nsw_charges"))
        return {r.pop("week"): {**r, "item_utc": r["item_utc"] and datetime.fromisoformat(r["item_utc"])}
                for r in rows}

    def save_charges(self, charges: dict[str, dict]) -> None:
        with self.db:
            self.db.executemany("INSERT OR REPLACE INTO nsw_charges VALUES (?, ?, ?, ?)",
                                [(wk, c["item_utc"] and c["item_utc"].isoformat(), c["amount"], c["description"])
                                 for wk, c in charges.items()])

    def save_market_info(self, info: dict[str, dict]) -> None:
        with self.db:
            self.db.executemany(f"INSERT OR REPLACE INTO market_info VALUES ({','.join('?' * len(INFO_COLS))})",
                                [tuple({"market_id": k, **i}[c] for c in INFO_COLS) for k, i in info.items()])

    def meta(self, key: str) -> datetime | None:
        r = self.db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return datetime.fromisoformat(r[0]) if r else None

    def save(self, bets: list[dict], markets: dict[str, dict], meta: dict[str, datetime]) -> None:
        def row(d: dict, cols: tuple) -> tuple:
            return tuple(d[c].isoformat() if c in TIME_COLS and d[c] else d[c] for c in cols)

        with self.db:
            self.db.executemany(
                f"INSERT OR REPLACE INTO bets VALUES ({','.join('?' * len(BET_COLS))})",
                [row(b, BET_COLS) for b in bets])
            self.db.executemany(
                f"INSERT OR REPLACE INTO markets VALUES ({','.join('?' * len(MARKET_COLS))})",
                [row({"market_id": k, **m}, MARKET_COLS) for k, m in markets.items()])
            self.db.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                                [(k, v.isoformat()) for k, v in meta.items()])


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
        self._catalogue: dict[str, dict] = {}   # market_id -> names, cached forever
        self.store = Store(DB_PATH)
        self._bets: dict[str, dict] = self.store.bets()        # bet_id -> settled bet
        self._markets: dict[str, dict] = self.store.markets()  # market_id -> market-level settlement
        self._market_info: dict[str, dict] = self.store.market_info()  # market_id -> NSW flag etc.
        for k, m in self._markets.items():   # re-derive, in case venue_states.csv has changed since
            self._classify(k, m)
        self._transfers: dict[str, dict] = {t["ref_id"]: t for t in self.store.transfers()}  # deposits/withdrawals
        self._charges: dict[str, dict] = self.store.charges()   # week -> NSW charge debited
        log.info("Loaded %d bets, %d markets from %s", len(self._bets), len(self._markets), DB_PATH)
        # what requests read: (bets, markets, covered_from, transfers, charges), replaced whole after each fetch
        self._history: tuple = ()
        self._views: dict[tuple, tuple[dict, list[dict]]] = {}   # (days, strategies) -> shaped history, until the next fetch
        self._publish(self.store.meta("covered_from"))
        self._last_cleared = 0.0
        self._last_recheck = 0.0
        self._last_scan = 0.0
        self._last_charges = 0.0
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
                market_projection=["EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION", "MARKET_DESCRIPTION"],
                max_results=len(chunk),
            )
            self._record_info(cats)
            for c in cats:
                self._catalogue[c.market_id] = {
                    "event": c.event.name if c.event else "",
                    "venue": c.event.venue if c.event else None,
                    "market": c.market_name,
                    "start": _iso(c.market_start_time),
                    "runners": {r.selection_id: r.runner_name for r in c.runners},
                }

    def _record_info(self, cats: list) -> None:
        info = {c.market_id: {
            "venue": c.event.venue if c.event else None,
            "country": c.event.country_code if c.event else None,
            "race_type": c.description.race_type if c.description else None,
            "rnsw": NSW_FLAG in ((c.description.rules or "") if c.description else ""),
        } for c in cats}
        self._market_info.update(info)
        self.store.save_market_info(info)

    def _scan_upcoming(self) -> None:
        """Record the NSW charge flag on AU horse markets before they close."""
        now = datetime.now(timezone.utc)
        ids = [c.market_id for c in self.trading.betting.list_market_catalogue(
            filter=market_filter(event_type_ids=[HORSE_RACING], market_countries=["AU"],
                                 market_start_time=time_range(from_=now - timedelta(hours=2), to=now + SCAN_AHEAD)),
            max_results=1000)]
        missing = [m for m in ids if m not in self._market_info]
        for i in range(0, len(missing), 100):
            self._record_info(self.trading.betting.list_market_catalogue(
                filter=market_filter(market_ids=missing[i:i + 100]),
                market_projection=["EVENT", "MARKET_DESCRIPTION"], max_results=100))
        if missing:
            log.info("Recorded NSW charge flag on %d upcoming markets", len(missing))

    def _update_transfers(self) -> None:
        """Deposits and withdrawals from the account statement (Betfair keeps 90 days of it)."""
        rows, start = [], 0
        while True:
            res = self.trading.account.get_account_statement(
                include_item="DEPOSITS_WITHDRAWALS", from_record=start, record_count=100,
                item_date_range=time_range(from_=datetime.now(timezone.utc) - RETENTION))
            # ref_id is "0" on every deposit/withdrawal, so key on time and amount too
            rows += [{"ref_id": f"{i.ref_id}:{_utc(i.item_date).isoformat()}:{i.amount}",
                      "item_utc": _utc(i.item_date), "amount": i.amount,
                      "description": i.legacy_data.full_market_name if i.legacy_data else None}
                     for i in res.account_statement]
            if not res.more_available:
                break
            start += len(res.account_statement)
        self.store.save_transfers(rows)
        self._transfers.update((r["ref_id"], r) for r in rows)

    def _update_charges(self) -> None:
        """NSW turnover charges debited for finished weeks that had NSW markets."""
        now = datetime.now(timezone.utc)
        weeks = {_week(m["settled_utc"]) for m in self._markets.values() if m["nsw"]}
        for wk in sorted(weeks - self._charges.keys()):
            end = datetime.fromisoformat(wk).replace(tzinfo=timezone.utc) + timedelta(days=7)
            if end > now or end + CHARGE_WINDOW < now - RETENTION:
                continue   # week not over, or its posting is older than Betfair's statement
            found = self._statement_charges(end, min(now, end + CHARGE_WINDOW))
            if wk not in found and end + CHARGE_WINDOW <= now:
                found[wk] = {"item_utc": None, "amount": 0.0, "description": None}
            self.store.save_charges(found)
            self._charges.update(found)

    def _statement_charges(self, since: datetime, until: datetime) -> dict[str, dict]:
        start = 0
        while True:
            res = self.trading.account.get_account_statement(
                include_item="ALL", from_record=start, record_count=100,
                item_date_range=time_range(from_=since, to=until))
            for i in res.account_statement:
                name = (i.legacy_data.full_market_name if i.legacy_data else None) or ""
                if CHARGE_TEXT in name:
                    t = CHARGE_WEEK.search(name)
                    wk = (datetime.strptime(t[1], "%d-%b-%y").date().isoformat() if t
                          else _week(_utc(i.item_date) - timedelta(days=7)))
                    log.info("NSW turnover charge for week of %s: %.2f", wk, -i.amount)
                    return {wk: {"item_utc": _utc(i.item_date), "amount": -i.amount, "description": name}}
            if not res.more_available:
                return {}
            start += len(res.account_statement)

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
                "market_id": o.market_id,
                "selection_id": o.selection_id,
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

    def _classify(self, market_id: str, m: dict) -> None:
        """Set m["nsw"] (a Racing NSW turnover charge market?) and m["nsw_exact"] (from Betfair's
        flag, or else guessed from the venue). m["state"] is the guessed AU state, 'UNKNOWN' for
        an unmapped venue raced in AU hours, None if not guessed."""
        info = self._market_info.get(market_id)
        m["venue"] = (info and info["venue"]) or EVENT_TIME.sub("", m["event"] or "")
        m["state"] = None
        if info is not None:
            m["nsw"], m["nsw_exact"] = bool(info["rnsw"]), True
            return
        m["nsw"] = m["nsw_exact"] = False
        start = m.get("start_utc")
        if start is None and (t := EVENT_TIME.match(m["event"] or "")):
            local = datetime.combine(m["settled_utc"].astimezone(TZ).date(), dtime(int(t[1]), int(t[2])), TZ)
            start = local.astimezone(timezone.utc)   # the date may be off by one; only the hour is used
        if m["event_type_id"] != HORSE_RACING or start is None or AU_HOURS_UTC[1] <= start.hour < AU_HOURS_UTC[0]:
            return   # not horse racing, or not raced in AU hours (or can't tell)
        # can't exclude NSW harness here: settled orders don't say Pace/Trot
        m["state"] = self.venue_states.get(_norm(m["venue"]), "UNKNOWN")
        m["nsw"] = m["state"] == "NSW"

    def _update_cleared(self) -> None:
        now = datetime.now(timezone.utc)
        earliest = now - RETENTION
        covered_from, fetched_at = self.store.meta("covered_from"), self.store.meta("fetched_at")
        recheck = time.time() - self._last_recheck > RECHECK_S
        if covered_from is None or fetched_at is None or covered_from > earliest:
            fetch_from = earliest   # first run, or Betfair has older bets than we hold: backfill
            log.info("Backfilling settled bets from %s", earliest.date())
        else:
            fetch_from = fetched_at - timedelta(hours=2)
            if recheck:
                fetch_from = min(fetch_from, now - RECHECK_SPAN)
            fetch_from = max(earliest, fetch_from)

        new_bets, new_markets = [], {}
        for b in self._cleared_orders("BET", fetch_from):
            d = b.item_description
            new_bets.append({
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
            })
        for m in self._cleared_orders("MARKET", fetch_from):
            d = m.item_description
            new_markets[m.market_id] = row = {
                "settled_utc": _utc(m.settled_date),
                "start_utc": _utc(d.market_start_time) if d else None,
                "profit": m.profit or 0.0,
                "commission": m.commission or 0.0,
                "event_type_id": str(m.event_type_id),
                "event": d.event_desc if d else "",
                "market": d.market_desc if d else "",
            }
            self._classify(m.market_id, row)
        covered_from = min(fetch_from, covered_from or fetch_from)
        self.store.save(new_bets, new_markets, {"covered_from": covered_from, "fetched_at": now})
        self._bets.update((b["bet_id"], b) for b in new_bets)
        self._markets.update(new_markets)
        if recheck:
            self._last_recheck = time.time()
        self._update_transfers()
        if time.time() - self._last_charges > CHARGE_CHECK_S:
            self._update_charges()
            self._last_charges = time.time()
        self._publish(covered_from)

    def _publish(self, covered_from: datetime | None) -> None:
        with self.lock:
            self._history = (list(self._bets.values()), dict(self._markets), covered_from,
                             list(self._transfers.values()), dict(self._charges))
            self._views = {}

    def view(self, days: int, strategies: tuple[str, ...] = ()) -> tuple[dict, list[dict]]:
        """Settled history for the last `days` days (0 = everything held), for some strategies or
        (empty) all of them: summary and bets."""
        strategies = tuple(sorted(set(strategies)))
        with self.lock:
            if (days, strategies) not in self._views:
                self._views[days, strategies] = self._shape_cleared(
                    *self._history, days, strategies, datetime.now(timezone.utc))
            return self._views[days, strategies]

    def _shape_cleared(self, bets: list[dict], markets: dict[str, dict], covered_from: datetime | None,
                       transfers: list[dict], charges: dict[str, dict], days: int, strategies: tuple[str, ...],
                       now: datetime) -> tuple[dict, list[dict]]:
        bets, markets = _strategy_only(bets, markets)
        names = sorted({b["strategy"] for b in bets})
        nsw = {k for k, m in markets.items() if m["nsw"]}
        # range starts at local midnight; 7 days = today and the 6 before
        start = (datetime.combine(now.astimezone(TZ).date() - timedelta(days=days - 1), dtime.min, TZ)
                 if days > 0 else None)

        # weekly NSW charge, decided on whole weeks whatever the range, then shown for weeks it overlaps
        weeks: dict[str, dict] = defaultdict(
            lambda: {"turnover": 0.0, "paid": 0.0, "implied": 0.0, "markets": 0, "estimated": 0})
        # the same split per strategy, so each one's part adds up to the week's figures: commission
        # paid split as in the strategy table, implied commission between the strategies that lost
        # in the market, by how much each lost
        week_strategies: dict[str, dict[str, dict]] = defaultdict(
            lambda: defaultdict(lambda: {"turnover": 0.0, "paid": 0.0, "implied": 0.0}))
        per_market = _per_market(bets)
        for k in nsw:
            m = markets[k]
            wk = _week(m["settled_utc"])
            w = weeks[wk]
            w["paid"] += m["commission"]
            w["implied"] += (implied := max(0.0, -m["profit"]) * NSW_IMPLIED_RATE)
            w["markets"] += 1
            w["estimated"] += not m["nsw_exact"]
            per = per_market.get(k, {})
            weights = _commission_weights(per)
            total = sum(weights.values()) or 1.0
            for s, wt in weights.items():
                week_strategies[wk][s]["paid"] += m["commission"] * wt / total
            losses = {s: -p for s, (p, _) in per.items() if p < 0}
            for s, loss in losses.items():
                week_strategies[wk][s]["implied"] += implied * loss / sum(losses.values())
        for b in bets:
            if b["market_id"] in nsw and b["side"] == "BACK":
                weeks[wk := _week(b["settled_utc"])]["turnover"] += b["size"] or 0.0
                week_strategies[wk][b["strategy"]]["turnover"] += b["size"] or 0.0
        current_week = _week(now)
        week_rows = []
        week_start = datetime.fromisoformat(current_week).replace(tzinfo=timezone.utc)
        elapsed = max((now - week_start) / timedelta(days=7), 1e-6)
        for wk, w in sorted(weeks.items()):
            generated = (w["paid"] + w["implied"]) / 2
            pct = generated / w["turnover"] if w["turnover"] else None
            charged = w["turnover"] >= NSW_MIN_TURNOVER and pct is not None and pct < NSW_GENERATED_MIN
            estimate = w["turnover"] * NSW_RATE if charged else 0.0
            actual = charges.get(wk)
            in_progress = wk == current_week
            projected = None
            if in_progress:   # the whole week at this week's pace so far; the generated % doesn't change
                full = w["turnover"] / elapsed
                projected = full * NSW_RATE if full >= NSW_MIN_TURNOVER and pct is not None and pct < NSW_GENERATED_MIN else 0.0
            week_rows.append({
                "week": wk, **w, "generated": generated, "generated_pct": pct,
                "charged": actual["amount"] > 0 if actual else charged,
                "charge": actual["amount"] if actual else estimate,
                "charge_est": estimate,
                "actual": actual is not None,
                "projected": projected,
                "in_progress": in_progress,
                # the first week can start before the earliest bets we were able to fetch
                "partial": covered_from is not None
                           and datetime.fromisoformat(wk).replace(tzinfo=timezone.utc) < covered_from,
                # each strategy's part; buffer = generated - 1.25% of turnover, so they add up to the week's
                "strategies": {s: {**v, "generated": (g := (v["paid"] + v["implied"]) / 2),
                                   "buffer": g - NSW_GENERATED_MIN * v["turnover"]}
                               for s, v in sorted(week_strategies[wk].items())},
            })
        # each week's charge spread over its NSW back stake
        charge_rate = {w["week"]: w["charge"] / w["turnover"] for w in week_rows if w["turnover"]}
        # the charge is decided on the whole account's week, so narrow to the chosen strategies only
        # now: they then carry their share of the charge, by their NSW back stake
        if strategies:
            bets, markets = _only(bets, markets, lambda s: s in strategies)
        if start:
            week_rows = [w for w in week_rows
                         if datetime.fromisoformat(w["week"]).replace(tzinfo=timezone.utc) + timedelta(days=7) > start]
            markets = {k: m for k, m in markets.items() if m["settled_utc"] >= start}
            bets = [b for b in bets if b["settled_utc"] >= start]
            transfers = [t for t in transfers if t["item_utc"] >= start]

        def nsw_charge(b: dict) -> float:
            if b["market_id"] in nsw and b["side"] == "BACK":
                return (b["size"] or 0.0) * charge_rate.get(_week(b["settled_utc"]), 0.0)
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
        daily_rows = [{"date": k, **v, "net": v["gross"] - v["commission"] - v["nsw_charge"]}
                      for k, v in sorted(daily.items())]

        by_strategy: dict[str, dict] = defaultdict(
            lambda: {"bets": 0, "won": 0, "staked": 0.0, "profit": 0.0, "commission": 0.0, "nsw_charge": 0.0})
        # strategy -> day -> net, for the cumulative-by-strategy chart
        strategy_days: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for k, per in _per_market(bets).items():
            if not (m := markets.get(k)) or not m["commission"]:
                continue
            day = m["settled_utc"].astimezone(TZ).date().isoformat()
            weights = _commission_weights(per)
            total = sum(weights.values()) or 1.0
            for s, w in weights.items():
                c = m["commission"] * w / total
                by_strategy[s]["commission"] += c
                strategy_days[s][day] -= c
        for b in bets:
            s = by_strategy[b["strategy"]]
            s["bets"] += 1
            s["won"] += b["outcome"] == "WON"
            s["staked"] += b["size"] or 0.0
            s["profit"] += b["profit"] or 0.0
            s["nsw_charge"] += (c := nsw_charge(b))
            strategy_days[b["strategy"]][b["settled_utc"].astimezone(TZ).date().isoformat()] += (b["profit"] or 0.0) - c
        strategies = [{"strategy": k, **v} for k, v in sorted(by_strategy.items())]

        # gross P&L by matched price and side (commission is per market, so can't be split by price)
        bands: dict[tuple, dict] = defaultdict(
            lambda: {"bets": 0, "won": 0, "staked": 0.0, "liability": 0.0, "profit": 0.0, "implied": 0.0})
        for b in bets:
            if not (p := b["price"]) or p <= 1:
                continue
            r = bands[bisect_right(ODDS_EDGES, p), b["side"]]
            r["bets"] += 1
            r["won"] += b["outcome"] == "WON"
            r["staked"] += b["size"] or 0.0
            r["liability"] += (b["size"] or 0.0) * ((p - 1) if b["side"] == "LAY" else 1)
            r["profit"] += b["profit"] or 0.0
            # the strike rate the price implies: a back wins 1/p of the time, a lay 1 - 1/p
            r["implied"] += 1 / p if b["side"] == "BACK" else 1 - 1 / p
        odds = [{"band": label, "side": sd, **bands[i, sd]}
                for i, label in enumerate(ODDS_LABELS) for sd in ("BACK", "LAY") if (i, sd) in bands]

        unknown = sorted({m["venue"] for m in markets.values() if m["state"] == "UNKNOWN"})
        rows = sorted(
            ({k: v for k, v in b.items() if k != "settled_utc"} for b in bets),
            key=lambda r: r["settled"] or "", reverse=True)
        won = sum(b["outcome"] == "WON" for b in bets)
        return {
            "range_days": days,
            "strategy": list(strategies),
            "strategy_names": names,
            "since": start.date().isoformat() if start else daily_rows[0]["date"] if daily_rows else None,
            "daily": daily_rows,
            "strategies": strategies,
            "strategy_daily": {s: [{"date": k, "net": v} for k, v in sorted(days_.items())]
                               for s, days_ in sorted(strategy_days.items())},
            "odds": odds,
            "nsw": {"weeks": week_rows, "unknown_venues": unknown},
            "bet_count": len(bets),
            "won_count": won,
            "transfers": {
                "deposits": sum(t["amount"] for t in transfers if t["amount"] > 0),
                "withdrawals": -sum(t["amount"] for t in transfers if t["amount"] < 0),
                "items": [{"date": _iso(t["item_utc"]), "amount": t["amount"], "description": t["description"]}
                          for t in sorted(transfers, key=lambda t: t["item_utc"], reverse=True)],
            },
        }, rows

    # --- loop --------------------------------------------------------------
    def refresh(self) -> None:
        self._ensure_session()
        funds = self._funds()
        current = self._shape_current(self._current_orders())
        # publish before fetching settled bets, so the page isn't held up by it after a restart;
        # settled history is served from the database in the meantime
        with self.lock:
            self.snapshot = {
                "status": "ok",
                "updated": datetime.now(TZ).isoformat(),
                "funds": funds,
                "current": current,
            }
        if time.time() - self._last_scan > SCAN_S:
            self._scan_upcoming()
            self._last_scan = time.time()
        if time.time() - self._last_cleared > CLEARED_POLL_S:
            self._update_cleared()
            self._last_cleared = time.time()

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
def data(days: int = Query(60, ge=0), strategy: list[str] = Query([])):
    """days: settled history to cover, counted back from today; 0 = everything held.
    strategy: customer_strategy_refs to narrow settled history to (repeat for several);
    none = all strategies."""
    with poller.lock:
        snapshot = poller.snapshot
    return JSONResponse({**snapshot, **poller.view(days, tuple(strategy))[0]})


@app.get("/api/bets")
def bets(q: str = "", side: str = "", limit: int = 500, days: int = Query(60, ge=0),
         strategy: list[str] = Query([])):
    q = q.strip().lower()
    rows = [b for b in poller.view(days, tuple(strategy))[1]
            if (not side or b["side"] == side)
            and (not q or any(q in str(b[k] or "").lower() for k in ("runner", "event", "market", "strategy")))]
    sels = _by_selection(rows)
    return {"count": len(sels), "bet_count": len(rows), "profit": sum(b["profit"] or 0.0 for b in rows),
            "selections": sels[:limit]}


def _by_selection(bets: list[dict]) -> list[dict]:
    """Settled bets grouped by market and runner, latest first: stake and weighted average
    price per side, and gross profit (commission is charged per market, not per runner)."""
    sels: dict[tuple, dict] = {}
    for b in bets:   # already latest first
        s = sels.get(k := (b["market_id"], b["runner"]))
        if s is None:
            s = sels[k] = {"settled": b["settled"], "event": b["event"], "market": b["market"], "runner": b["runner"],
                           "bets": 0, "profit": 0.0, "strategies": [], "result": None,
                           "BACK": {"stake": 0.0, "price": 0.0}, "LAY": {"stake": 0.0, "price": 0.0}}
        side = s[b["side"]]
        size = b["size"] or 0.0
        side["price"] += size * (b["price"] or 0.0)   # weighted sum until divided below
        side["stake"] += size
        s["bets"] += 1
        s["profit"] += b["profit"] or 0.0
        if b["strategy"] and b["strategy"] not in s["strategies"]:
            s["strategies"].append(b["strategy"])
        if s["result"] is None and b["outcome"] in ("WON", "LOST"):
            # how the runner itself did: a lay that won means the runner lost
            s["result"] = "WON" if (b["outcome"] == "WON") == (b["side"] == "BACK") else "LOST"
    for s in sels.values():
        for side in (s["BACK"], s["LAY"]):
            side["price"] = side["price"] / side["stake"] if side["stake"] else None
    return list(sels.values())


@app.get("/healthz")
def healthz():
    status = poller.snapshot.get("status")
    return JSONResponse({"status": status}, status_code=503 if status == "error" else 200)
