# betfair-dashboard

Read-only web dashboard for the Betfair account: balance and exposure, open
orders, settled bets, daily and cumulative P&L, P&L by `customer_strategy_ref`,
and an estimate of the NSW turnover charge.

It only calls read endpoints (account funds, current orders, cleared orders,
market catalogue), so it cannot place, cancel or change bets. It logs in with
its own session, separate from the live trader's.

## Run

```sh
cp .env.example .env        # fill in BF_USERNAME, BF_PASSWORD, BF_APP_KEY
docker build -t betfair-dashboard .
docker run -d --name betfair-dashboard --restart unless-stopped \
  --env-file .env -p 8000:8000 betfair-dashboard
```

Open http://<homelab-host>:8000.

The delayed app key works: account and order data aren't delayed.

There is no authentication. The page shows your balance and bets, so keep it on
the LAN or put it behind your reverse proxy's auth.

## Settings (env vars)

| Var | Default | |
|---|---|---|
| `DASH_HISTORY_DAYS` | 90 | days of settled bets to load (Betfair keeps 90; capped there) |
| `DASH_CURRENT_POLL_S` | 15 | funds and open-orders refresh |
| `DASH_CLEARED_POLL_S` | 120 | fetch newly settled bets (the full window is re-pulled every 6 hours) |
| `DASH_TZ` | Australia/Sydney | timezone for daily P&L buckets |
| `DASH_VENUE_STATES` | `venue_states.csv` | venue → state map used to find NSW markets |

## Notes

- Daily P&L is net of commission (from market-level cleared orders). Per-bet and
  per-strategy profit is gross, since Betfair charges commission per market.
- The NSW turnover charge is an **estimate**, not Betfair's invoice. Per Mon–Sun
  GMT week, on NSW thoroughbred markets (harness excluded), it charges 3% of back
  turnover when turnover is at least $1,000 and (commission paid + 3% of each
  losing market's loss) / 2 is under 1.25% of it. That is the same rule as
  `thoroughbred-model/scripts/price_movement_model.py`. Daily P&L subtracts it,
  spread over the week's NSW back bets. The current week is provisional.
- NSW markets are found by venue: the venue name in Betfair's event name
  ("Randwick (AUS) 1st Mar") is looked up in `venue_states.csv`. Unmapped AU
  venues count as NSW and are listed under the NSW table. The CSV was built from
  `betfair-playground/data/track_states.csv` plus the venue map in
  `thoroughbred-model/scripts/engineer_features.py`; rebuild it when new venues
  appear, or mount a newer copy and set `DASH_VENUE_STATES`.
- ACT (Canberra) is not counted as NSW.
