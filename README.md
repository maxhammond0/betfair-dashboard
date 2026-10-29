# betfair-dashboard

Read-only web dashboard for the Betfair account: balance and exposure, open
orders, settled bets, daily and cumulative P&L (net and gross), drawdown, P&L by
`customer_strategy_ref` (table and cumulative chart), gross P&L and strike rate by
matched price band, and an estimate of the NSW turnover charge with weekly charts of
commission generated, each strategy's part of the 1.25% buffer, and the buffer with
and without a chosen strategy.

It only calls read endpoints (account funds, current orders, cleared orders,
market catalogue), so it cannot place, cancel or change bets. It logs in with
its own session, separate from the live trader's.

## Run

```sh
cp .env.example .env        # fill in BF_USERNAME, BF_PASSWORD, BF_APP_KEY
docker build -t betfair-dashboard .
docker run -d --name betfair-dashboard --restart unless-stopped \
  --env-file .env -p 8000:8000 -v betfair-dashboard-data:/app/data betfair-dashboard
```

Open http://<homelab-host>:8000.

Settled bets are stored in SQLite at `/app/data/dashboard.db`. Keep that
volume across container rebuilds: the first start backfills the history window
from Betfair (90 days, all it keeps), and later starts fetch only what has
settled since. Rows are never deleted, so the database keeps building history
past Betfair's 90 days.

The buttons at the top of the page pick how much settled history the P&L,
strategy, NSW and settled-bet sections cover (today, 7/30/60/90 days, or
everything stored). The choice is remembered per browser.

The strategy menu next to them narrows the page to one or more
`customer_strategy_ref`s (tick any number; none ticked means all): P&L tiles and
charts, strike rate, open orders and settled selections then cover only those
strategies' bets, with each market's commission cut to their share (as in the
strategy table). Balance, exposure, transfers and the NSW weekly table stay
account-wide, since the NSW charge is decided on all strategies together; the
chosen strategies' P&L carries their share of the charge, by NSW back stake.

The delayed app key works: account and order data aren't delayed.

There is no authentication. The page shows your balance and bets, so keep it on
the LAN or put it behind your reverse proxy's auth.

## Settings (env vars)

| Var | Default | |
|---|---|---|
| `DASH_CURRENT_POLL_S` | 15 | funds and open-orders refresh |
| `DASH_CLEARED_POLL_S` | 120 | fetch newly settled bets (the last 3 days are re-pulled every 6 hours, for corrections) |
| `DASH_DB` | `data/dashboard.db` | SQLite file holding settled bets |
| `DASH_TZ` | Australia/Sydney | timezone for daily P&L buckets |
| `DASH_VENUE_STATES` | `venue_states.csv` | venue → state map used to find NSW markets |

## Notes

- The Withdrawals tile totals withdrawals and deposits in the selected range, from
  the account statement (hover it for the list). Betfair's statement only goes
  back 90 days; the database keeps older ones.
- Only bets with a `customer_strategy_ref` count. Untagged bets (manual bets, other
  tools) are left out of every figure: P&L, commission, strike rate, and NSW turnover
  and charge. A market's commission is cut to the tagged bets' share, split as in the
  strategy table below. They are still stored, so this can be undone.
- Cleared-order `profit` is gross at both bet and market level; commission is a
  separate field on market-level orders, and is subtracted once.
- Daily P&L is net of commission (from market-level cleared orders). Per-bet
  profit is gross, since Betfair charges commission per market. The strategy
  table splits each market's commission between the strategies that won in it,
  in proportion to what each won, so its Net column adds up to the daily P&L.
- The NSW turnover charge is estimated per Mon–Sun GMT week: on Racing NSW
  turnover charge markets, 3% of back turnover when turnover is at least $1,000
  and (commission paid + 3% of each losing market's loss) / 2 is under 1.25% of
  it (Betfair Charges §9.3). Once Betfair debits a week's charge (an account
  statement item like "AUS Turnover Chg ADJ77 - wk start 12-Oct-26", usually the
  Tuesday after) that amount replaces the estimate. It only comes with the full
  statement, so the dashboard reads just the 5 days after each finished week
  (hourly until found; about 200 calls the first time, for 90 days). Daily P&L
  subtracts the charge, spread over the week's NSW back bets. The current week
  is provisional: the tile shows the charge if the week ended now, and at the
  week's pace so far.
- Charged markets are identified by Betfair's own flag: their rules text says
  "This is a Racing NSW Turnover Charge market" (NSW thoroughbred meetings; not
  NSW harness, not Canberra). The flag can only be read while a market is open,
  so every 30 minutes the dashboard records it for the next 24 hours of AU horse
  markets, and for any market it sees an open order on. Settled orders carry
  neither the flag nor the country (the event is just "12:35 Ballarat").
- Markets that settled without a recorded flag (history from before the dashboard
  was running) fall back to the venue: it is looked up in `venue_states.csv`, and
  only races starting 21:00–11:00 UTC count, to tell NSW Newcastle from UK
  Newcastle. These are marked "est." in the NSW table; the fallback can't exclude
  NSW harness races. AU-hours venues missing from the CSV are listed under the
  table and not counted. The CSV was built from
  `betfair-playground/data/track_states.csv` plus the venue map in
  `thoroughbred-model/scripts/engineer_features.py`; rebuild it when new venues
  appear, or mount a newer copy and set `DASH_VENUE_STATES`.
