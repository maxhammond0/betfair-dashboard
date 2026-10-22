# betfair-dashboard

Read-only web dashboard for the Betfair account: balance and exposure, open
orders, settled bets, daily and cumulative P&L, and P&L by `customer_strategy_ref`.

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
| `DASH_HISTORY_DAYS` | 30 | days of settled bets to load (Betfair keeps 90) |
| `DASH_CURRENT_POLL_S` | 15 | funds and open-orders refresh |
| `DASH_CLEARED_POLL_S` | 120 | settled-bets refresh |
| `DASH_TZ` | Australia/Sydney | timezone for daily P&L buckets |

## Notes

- Daily P&L is net of commission (from market-level cleared orders). Per-bet and
  per-strategy profit is gross, since Betfair charges commission per market.
- The NSW turnover charge isn't included; it's billed weekly and isn't exposed by
  these endpoints.
